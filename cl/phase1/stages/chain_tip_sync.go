package stages

import (
	"context"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// waitForExecutionEngineToBeFinished checks if the execution engine is ready within a specified timeout.
// It periodically checks the readiness of the execution client and returns true if the client is ready before
// the timeout occurs. If the context is canceled or a timeout occurs, it returns false with the corresponding error.
func waitForExecutionEngineToBeFinished(ctx context.Context, cfg *Cfg) (ready bool, err error) {
	// If no execution client is set, then we can skip this step
	if cfg.executionClient == nil {
		return true, nil
	}

	// Setup the timers
	readyTimeout := time.NewTimer(10 * time.Second)
	readyInterval := time.NewTimer(50 * time.Millisecond)

	// Ensure the timers are stopped to release resources
	defer readyTimeout.Stop()
	defer readyInterval.Stop()

	// Loop to check the readiness status
	for {
		select {
		case <-ctx.Done():
			// Context canceled or timed out
			return false, ctx.Err()
		case <-readyTimeout.C:
			// Timeout reached without the execution engine being ready
			return false, nil
		case <-readyInterval.C:
			// Check the readiness of the execution engine
			ready, err := cfg.executionClient.Ready(ctx)
			if err != nil {
				return false, err
			}
			if !ready {
				// If not ready, continue checking in the next interval
				continue
			}
			// Execution engine is ready
			return true, nil
		}
	}
}

// fetchBlocksFromReqResp retrieves blocks starting from a specified block number and continues for a given count.
// It sends a request to fetch the blocks, verifies the associated blobs, and inserts them into the blob store.
// It returns a PeeredObject containing the blocks and the peer ID, or an error if something goes wrong.
func fetchBlocksFromReqResp(ctx context.Context, cfg *Cfg, from uint64, count uint64) (*peers.PeeredObject[[]*cltypes.SignedBeaconBlock], error) {
	// spam requests to fetch blocks by range from the execution client
	const shardSize = 16
	// 计算需要多少个分片
	shards := (count + shardSize - 1) / shardSize

	// 收集结果
	type shardRes struct {
		idx    int // 分片序号，用于最后排序
		blocks []*cltypes.SignedBeaconBlock
		err    error
		pid    string
	}
	results := make([]shardRes, shards)

	eg0, eg0Ctx := errgroup.WithContext(ctx)
	eg0.SetLimit(3)
	// 为每个分片启动一个 goroutine
	for i := uint64(0); i < shards; i++ {
		i := i
		eg0.Go(func() error {
			nopeersErrChan := make(chan error, 5)
			respChan := make(chan shardRes, 3)
			reqCtx, cancel := context.WithCancel(eg0Ctx)

			var fetchBlocksfn func(idx int, loopCount int)
			fetchBlocksfn = func(idx int, loopCount int) {
				if loopCount > 3 {
					return
				}
				select {
				case <-reqCtx.Done():
					return
				default:
				}
				start := from + uint64(idx)*shardSize
				remain := count - uint64(idx)*shardSize
				if remain > shardSize {
					remain = shardSize
				}

				var (
					blocks []*cltypes.SignedBeaconBlock
					err    error
					pid    string
				)

				blocks, pid, err = cfg.rpc.SendBeaconBlocksByRangeReq(ctx, start, remain)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					needRetry := true
					if strings.Contains(err.Error(), "no addresses") {
						cfg.rpc.BanPeer(pid)
					}
					if errors.Is(err, peers.ErrNoPeers) {
						log.Warn("No peers available for beacon blocks by range request/chaintip", "err", err, "peer", pid)
						nopeersErrChan <- err
						needRetry = false
					}
					log.Warn("Failed to send beacon blocks by range request/chaintip", "err", err, "peer", pid, "loopCount", loopCount)

					if needRetry {
						fetchBlocksfn(idx, loopCount+1)
					}
					return
				}
				if len(blocks) == 0 {
					cfg.rpc.BanPeer(pid)
					return
				}
				obj := shardRes{idx: idx, blocks: blocks, pid: pid}
				select {
				case respChan <- obj:
					results[idx] = obj
					cancel() // 立刻通知退出 for-select
				default:
				}
			}

			for {
				select {
				case <-reqCtx.Done():
					return nil
				case <-nopeersErrChan:
					for len(nopeersErrChan) > 0 {
						<-nopeersErrChan
					}
					fetchBlocksfn(int(i), 0) //sync request
				case <-time.After(time.Duration(1500 * time.Millisecond)):
					go fetchBlocksfn(int(i), 0)
				}
			}
		})
	}

	if err := eg0.Wait(); err != nil {
		return nil, err
	}

	blocks := make([]*cltypes.SignedBeaconBlock, 0)
	var pid string
	for _, r := range results {
		blocks = append(blocks, r.blocks...)
		if pid == "" && r.pid != "" { // 任意取一个 peer ID 即可
			pid = r.pid
		}
	}

	// If no blocks are returned, return nil without error
	if len(blocks) == 0 {
		return nil, nil
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	denebBlocks := []*cltypes.SignedBeaconBlock{}
	fuluBlocks := []*cltypes.SignedBlindedBeaconBlock{}
	for _, block := range blocks {
		blindedBlock, err := block.Blinded()
		if err != nil {
			return nil, err
		}
		if block.Version() >= clparams.FuluVersion {
			fuluBlocks = append(fuluBlocks, blindedBlock)
		} else if block.Version() >= clparams.DenebVersion {
			denebBlocks = append(denebBlocks, block)
		}
	}

	if len(fuluBlocks) > 0 {
		// download missing column data for the fulu blocks
		if cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.ImmediateBlobsBackfilling {
			if err := cfg.peerDas.DownloadColumnsAndRecoverBlobs(ctx, fuluBlocks); err != nil {
				log.Warn("[chainTipSync] failed to download columns and recover blobs", "err", err)
			}
		} else {
			if err := cfg.peerDas.DownloadOnlyCustodyColumns(ctx, fuluBlocks); err != nil {
				log.Warn("[chainTipSync] failed to download only custody columns", "err", err)
			}
		}
	}

	if len(denebBlocks) > 0 {
		// Generate blob identifiers from the retrieved blocks
		allblocks := denebBlocks

		type blobsWithBlocks struct {
			ids    *solid.ListSSZ[*cltypes.BlobIdentifier]
			blocks []*cltypes.SignedBeaconBlock
		}

		var allEntries []blobsWithBlocks
		const chunkSize = 9
		const maxChunkSize = 15
		bindex := 0
		for bindex < len(allblocks)-1 {
			end := bindex + 1
			for end <= len(allblocks)-1 {
				ids, err2 := network2.BlobsIdentifiersFromBlocks(allblocks[bindex:end], cfg.beaconCfg)
				if err2 != nil {
					return nil, err2
				}
				if ids.Len() == 0 {
					end++
					continue
				}
				if ids.Len() > maxChunkSize {
					end--
					break
				}
				if ids.Len() >= chunkSize {
					break
				}
				end++
			}
			ids, err2 := network2.BlobsIdentifiersFromBlocks(allblocks[bindex:end], cfg.beaconCfg)
			if err2 != nil {
				return nil, err2
			}
			if ids.Len() > 0 {
				allEntries = append(allEntries, blobsWithBlocks{
					ids:    ids,
					blocks: allblocks[bindex:end],
				})
			}
			bindex = end
		}

		blocksToProcess := allblocks[bindex:]
		for _, block := range blocksToProcess {
			blocks1 := []*cltypes.SignedBeaconBlock{block}
			subids, err2 := network2.BlobsIdentifiersFromBlocks(blocks1, cfg.beaconCfg)
			if err2 != nil {
				return nil, err2
			}
			allEntries = append(allEntries, blobsWithBlocks{
				ids:    subids,
				blocks: blocks1,
			})
		}

		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(5)
		networkUnstableFlag := atomic.Bool{}

		// Loop until all blobs are inserted into the blob store
		for _, entity := range allEntries {
			select {
			case <-ctx.Done():
				// Context canceled or timed out
				return nil, ctx.Err()
			default:
			}
			if entity.ids.Len() == 0 {
				continue
			}

			var processIDs func(entity blobsWithBlocks, nestcall bool) error
			processIDs = func(entity blobsWithBlocks, nestcall bool) error {
				ids := entity.ids
				retryProcess := func(entity blobsWithBlocks) error {
					eg1, _ := errgroup.WithContext(egCtx)
					eg1.SetLimit(3)
					for _, block := range entity.blocks {
						subids, err2 := network2.BlobsIdentifiersFromBlocks([]*cltypes.SignedBeaconBlock{block}, cfg.beaconCfg)
						if err2 != nil {
							panic(err2)
						}
						if subids.Len() == 0 {
							continue
						}
						entitySmall := blobsWithBlocks{
							ids:    subids,
							blocks: []*cltypes.SignedBeaconBlock{block},
						}
						eg1.Go(func() error {
							return processIDs(entitySmall, true)
						})
					}
					if err1 := eg1.Wait(); err1 != nil {
						return nil
					}
					return nil
				}
				respErrCount := 0
				if !nestcall && networkUnstableFlag.Load() {
					if err1 := retryProcess(entity); err1 != nil {
						return err1
					}
					return nil
				}
				respChan, respErrChan, cancel := network2.RequestBlobsFrantically(egCtx, cfg.rpc, ids)
			Loop:
				for i := 0; ; {
					select {
					case <-egCtx.Done():
						cancel()
						return egCtx.Err()
					case blobs, ok := <-respChan:
						if !ok {
							cancel()
							break Loop
						}

						i += 1
						// Verify the blobs against identifiers and insert them into the blob store
						_, inserted, errverify := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil)
						log.Warn("inserted equal check", "ids.len", ids.Len(), "inserted", inserted)
						if errverify == nil && inserted == uint64(ids.Len()) {
							cancel()
							break Loop
						} else {
							if i > 2 && !nestcall {
								cancel()
								networkUnstableFlag.Store(true)
								if err1 := retryProcess(entity); err1 != nil {
									return err1
								}
								break Loop
							}
							if nestcall && errverify != nil && i > 3 {
								cancel()
								log.Warn("inserted equal check cancel", "err", errverify.Error())
								return errors.Wrap(errverify, "failed to verify blobs against identifiers and insert into the blob store")
							}
						}
					case <-respErrChan:
						respErrCount += 1
						if respErrCount > 7 {
							cancel()
							networkUnstableFlag.Store(true)
							if err1 := retryProcess(entity); err1 != nil {
								return err1
							}
							break Loop
						}
					}
				}
				return nil
			}

			normalEntity := entity
			eg.Go(func() error {
				return processIDs(normalEntity, false)
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
	}

	// Return the blocks and the peer ID wrapped in a PeeredObject
	return &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{
		Data: blocks,
		Peer: pid,
	}, nil
}

// startFetchingBlocksMissedByGossipAfterSomeTime starts fetching blocks that might have been missed by gossip after a delay.
// It periodically fetches blocks from the highest seen block up to the current slot and sends the results or errors to the provided channels.
func startFetchingBlocksMissedByGossipAfterSomeTime(ctx context.Context, cfg *Cfg, args Args, respCh chan<- *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], errCh chan error) {
	// Wait for half the duration of SecondsPerSlot or until the context is done
	if cfg.forkChoice.HighestSeen()-2 >= cfg.ethClock.GetCurrentSlot() {
		select {
		case <-time.After((time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second) / 2):
		case <-ctx.Done():
			return
		}
	}

	// Continuously fetch and process blocks
	for {
		// Calculate the range of blocks to fetch
		from := cfg.forkChoice.HighestSeen() - 2
		currentSlot := cfg.ethClock.GetCurrentSlot()
		count := (currentSlot - from) + 4

		// Stop fetching if the highest seen block is greater than or equal to the target slot
		if cfg.forkChoice.HighestSeen() >= args.targetSlot {
			return
		}

		// Fetch blocks from the specified range
		blocks, err := fetchBlocksFromReqResp(ctx, cfg, from, count)
		if err != nil {
			// Send error to the error channel and return
			errCh <- err
			return
		}
		if blocks == nil {
			continue
		}

		// Send fetched blocks to the response channel or handle context cancellation
		select {
		case respCh <- blocks:
		case <-ctx.Done():
			return
		case <-time.After(time.Second): // Take a short pause before the next iteration
		}
	}
}

// listenToIncomingBlocksUntilANewBlockIsReceived listens for incoming blocks until a new block with a slot greater than or equal to the target slot is received.
// It processes blocks, checks their validity, and publishes them. It also handles context cancellation and logs progress periodically.
func listenToIncomingBlocksUntilANewBlockIsReceived(ctx context.Context, logger log.Logger, cfg *Cfg, args Args, respCh <-chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], errCh chan error) error {
	// Timer to log progress every 30 seconds
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	// Timer to check block presence every 20 milliseconds
	presenceTicker := time.NewTicker(20 * time.Millisecond)
	defer presenceTicker.Stop()

	// Map to keep track of seen block roots
	seenBlockRoots := make(map[common.Hash]struct{})
MainLoop:
	for {
		select {
		case <-presenceTicker.C:
			// Check if the highest seen block is greater than or equal to the target slot
			if cfg.forkChoice.HighestSeen() >= args.targetSlot {
				break MainLoop
			}
		case <-ctx.Done():
			// Handle context cancellation
			return ctx.Err()
		case err := <-errCh:
			// Handle errors received on the error channel
			return err
		case blocks := <-respCh:
			// Handle blocks received on the response channel
			for _, block := range blocks.Data {
				// Check if the parent block is known
				if _, ok := cfg.forkChoice.GetHeader(block.Block.ParentRoot); !ok {
					time.Sleep(time.Millisecond)
					continue
				}

				// Calculate the block root and check if the block is already known
				blockRoot, _ := block.Block.HashSSZ() // Ignoring error as block would not process if HashSSZ failed
				if _, ok := cfg.forkChoice.GetHeader(blockRoot); ok {
					// Check if the block slot is greater than or equal to the target slot
					if block.Block.Slot >= args.targetSlot {
						break MainLoop
					}
					continue
				}

				// Check if the block root has already been seen
				if _, ok := seenBlockRoots[blockRoot]; ok {
					continue
				}

				// Process the block
				if err := processBlock(ctx, cfg, cfg.indiciesDB, block, true, true, true); err != nil {
					log.Debug("bad blocks segment received", "err", err, "blockSlot", block.Block.Slot)
					continue
				}

				// Mark the block root as seen
				seenBlockRoots[blockRoot] = struct{}{}

				// Check if the block slot is greater than or equal to the target slot
				if block.Block.Slot >= args.targetSlot {
					break MainLoop
				}
			}
		case <-logTicker.C:
			// Log progress periodically
			logger.Info("[Caplin] Progress", "progress", cfg.forkChoice.HighestSeen(), "from", args.seenSlot, "to", args.targetSlot)
		}
	}
	return nil
}

// chainTipSync synchronizes the chain tip by fetching blocks from the highest seen block up to the target slot by listening to incoming blocks.
// or by fetching blocks that might have been missed by gossip after a delay.
func chainTipSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	totalRequest := args.targetSlot - args.seenSlot
	log.Debug("[chainTipSync] totalRequest", "totalRequest", totalRequest, "seenSlot", args.seenSlot, "targetSlot", args.targetSlot)
	// If the execution engine is not ready, wait for it to be ready.
	ready, err := waitForExecutionEngineToBeFinished(ctx, cfg)
	if err != nil {
		log.Warn("[chainTipSync] error waiting for execution engine to be ready", "err", err)
		return err
	}
	if !ready {
		log.Debug("[chainTipSync] execution engine is not ready yet")
		return nil
	}

	log.Debug("[chainTipSync] execution engine is ready")

	if cfg.executionClient != nil && cfg.executionClient.SupportInsertion() {
		if err := cfg.blockCollector.Flush(context.Background()); err != nil {
			return err
		}
	}

	logger.Debug("waiting for blocks...",
		"seenSlot", args.seenSlot,
		"targetSlot", args.targetSlot,
		"requestedSlots", totalRequest,
	)
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1024)
	errCh := make(chan error)

	// 25 seconds is a good timeout for this
	ctx, cn := context.WithTimeout(ctx, 210*time.Second)
	defer cn()

	go startFetchingBlocksMissedByGossipAfterSomeTime(ctx, cfg, args, respCh, errCh)

	return listenToIncomingBlocksUntilANewBlockIsReceived(ctx, logger, cfg, args, respCh, errCh)
}
