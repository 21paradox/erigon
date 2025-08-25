package stages

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/db/kv"
	"golang.org/x/sync/errgroup"
)

// shouldProcessBlobs checks if any block in the given list of blocks
// has a version greater than or equal to DenebVersion and contains BlobKzgCommitments.
func shouldProcessBlobs(blocks []*cltypes.SignedBeaconBlock, cfg *Cfg) bool {
	if !cfg.caplinConfig.ArchiveBlobs && !cfg.caplinConfig.ImmediateBlobsBackfilling {
		return false
	}
	blobsExist := false
	highestSlot := blocks[0].Block.Slot
	for _, block := range blocks {
		// Check if block version is greater than or equal to DenebVersion and contains BlobKzgCommitments
		if block.Version() >= clparams.DenebVersion && block.Block.Body.BlobKzgCommitments.Len() > 0 {
			blobsExist = true
		}
		if block.Block.Slot > highestSlot {
			highestSlot = block.Block.Slot
		}
	}
	// Check if the requested blocks are too old to request blobs
	// https://github.com/ethereum/consensus-specs/blob/dev/specs/deneb/p2p-interface.md#the-reqresp-domain

	// this is bad
	// highestEpoch := highestSlot / cfg.beaconCfg.SlotsPerEpoch
	// currentEpoch := cfg.ethClock.GetCurrentEpoch()
	// minEpochDist := uint64(0)
	// if currentEpoch > cfg.beaconCfg.MinEpochsForBlobSidecarsRequests {
	// 	minEpochDist = currentEpoch - cfg.beaconCfg.MinEpochsForBlobSidecarsRequests
	// }
	// finalizedEpoch := currentEpoch - 2
	// if highestEpoch < max(cfg.beaconCfg.DenebForkEpoch, minEpochDist, finalizedEpoch) {
	// 	return false
	// }

	return blobsExist
}

// downloadAndProcessEip4844DA handles downloading and processing of EIP-4844 data availability blobs.
// It takes highest slot processed, and a list of signed beacon blocks as input.
// It returns the highest blob slot processed and an error if any.
func downloadAndProcessEip4844DA(ctx context.Context, logger log.Logger, cfg *Cfg, highestSlotProcessed uint64, blocks []*cltypes.SignedBeaconBlock) (highestBlobSlotProcessed uint64, err error) {

	const chunkSize = 9
	const maxChunkSize = 15
	bindex := 0
	type blobsWithBlocks struct {
		ids    *solid.ListSSZ[*cltypes.BlobIdentifier]
		blocks []*cltypes.SignedBeaconBlock
	}

	var allEntries []blobsWithBlocks

	for bindex < len(blocks)-1 {
		end := bindex + 1
		for end <= len(blocks)-1 {
			ids, err1 := network2.BlobsIdentifiersFromBlocks(blocks[bindex:end], cfg.beaconCfg)
			if err1 != nil {
				err = fmt.Errorf("failed to get blob identifiers: %w", err1)
				return
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
		ids, err1 := network2.BlobsIdentifiersFromBlocks(blocks[bindex:end], cfg.beaconCfg)
		if err1 != nil {
			err = fmt.Errorf("failed to get blob identifiers: %w", err1)
			return
		}
		if ids.Len() > 0 {
			allEntries = append(allEntries, blobsWithBlocks{
				ids:    ids,
				blocks: blocks[bindex:end],
			})
		}
		bindex = end
	}

	blocksToProcess := blocks[bindex:]
	for _, block := range blocksToProcess {
		blocks1 := []*cltypes.SignedBeaconBlock{block}
		subids, err2 := network2.BlobsIdentifiersFromBlocks(blocks1, cfg.beaconCfg)
		if err2 != nil {
			err = fmt.Errorf("failed to get blob identifiers: %w", err2)
			return
		}
		allEntries = append(allEntries, blobsWithBlocks{
			ids:    subids,
			blocks: blocks1,
		})
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(3)
	networkUnstableFlag := atomic.Bool{}
	hprocessedChan := make(chan uint64)

	highestSlotProcessedNow := highestSlotProcessed
	go func() {
		for hprocess := range hprocessedChan {
			if hprocess > highestSlotProcessedNow {
				highestSlotProcessedNow = hprocess
			}
		}
	}()

	for loopIdx, entity := range allEntries {
		select {
		case <-ctx.Done():
			// Context canceled or timed out
			return highestSlotProcessedNow, ctx.Err()
		default:
		}

		if entity.ids.Len() == 0 {
			continue
		}
		var processIDs func(entity blobsWithBlocks, nestcall bool, lastLoop bool) error
		processIDs = func(entity blobsWithBlocks, nestcall bool, lastLoop bool) error {
			ids := entity.ids
			var errRet error

			retryProcess := func(entity blobsWithBlocks) error {
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
					if err1 := processIDs(entitySmall, true, lastLoop); err1 != nil {
						return err1
					}
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
					i += 1
					if !ok {
						cancel()
						break Loop
					}

					// Verify the blobs against identifiers and insert them into the blob store
					hprocessed, inserted, errverify := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil)
					log.Warn("inserted equal check in forward", "ids.len", ids.Len(), "inserted", inserted)

					if errverify == nil && inserted == uint64(ids.Len()) {
						errRet = nil
						hprocessedChan <- hprocessed
						cancel()
						break Loop
					} else {
						if hprocessed > 0 {
							hprocessedChan <- hprocessed - 1
						}
						if errverify != nil {
							log.Warn("inserted equal check forward err", "err", errverify.Error())
							errRet = fmt.Errorf("failed to verify blobs: %w", errverify)
						}
						if lastLoop && i > 3 {
							cancel()
							break Loop
						}
						if nestcall {
							if i > 10 {
								cancel()
								break Loop
							}
						}
						if !lastLoop && i > 3 && !nestcall {
							cancel()
							networkUnstableFlag.Store(true)
							errRet = nil
							if err1 := retryProcess(entity); err1 != nil {
								return err1
							}
							break Loop
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
			return errRet
		}

		normalEntity := entity
		isLastLoopIds := loopIdx == len(allEntries)-1
		eg.Go(func() error {
			return processIDs(normalEntity, false, isLastLoopIds)
		})
	}

	errinGroup := eg.Wait()
	close(hprocessedChan)

	if errinGroup != nil {
		return highestSlotProcessedNow, errinGroup
	}

	return highestSlotProcessedNow, nil
}

func downloadBlobs(ctx context.Context, logger log.Logger, cfg *Cfg, highestBlockProcessed uint64, blocks []*cltypes.SignedBeaconBlock) (err error) {
	fuluBlocks := []*cltypes.SignedBlindedBeaconBlock{}
	denebBlocks := []*cltypes.SignedBeaconBlock{}
	for _, block := range blocks {
		blindedBlock, err := block.Blinded()
		if err != nil {
			return err
		}
		if blindedBlock.Version() >= clparams.FuluVersion {
			fuluBlocks = append(fuluBlocks, blindedBlock)
		} else if block.Version() >= clparams.DenebVersion {
			denebBlocks = append(denebBlocks, block)
		}
	}

	if len(denebBlocks) > 0 && shouldProcessBlobs(denebBlocks, cfg) {
		_, err = downloadAndProcessEip4844DA(ctx, logger, cfg, highestBlockProcessed, denebBlocks)
		if err != nil {
			logger.Trace("[Caplin] Failed to process blobs", "err", err)
			return err
		}
	}

	if len(fuluBlocks) > 0 && canDownloadColumnData(fuluBlocks, cfg) {
		if cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.ImmediateBlobsBackfilling {
			if err = cfg.peerDas.DownloadColumnsAndRecoverBlobs(ctx, fuluBlocks); err != nil {
				logger.Warn("[Caplin] Failed to download columns and recover blobs", "err", err)
			}
		} else {
			if err = cfg.peerDas.DownloadOnlyCustodyColumns(ctx, fuluBlocks); err != nil {
				logger.Warn("[Caplin] Failed to download custody columns", "err", err)
			}
		}
	}

	return nil
}

func canDownloadColumnData(blocks []*cltypes.SignedBlindedBeaconBlock, cfg *Cfg) bool {
	return cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.ImmediateBlobsBackfilling

	// todo: comment out for now
	/*
		// check if data is too far behind
		// minimum_request_epoch = max(finalized_epoch, current_epoch - MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS, FULU_FORK_EPOCH)
		// Get the current epoch from the first block
		if len(blocks) == 0 {
			return false
		}
		currentEpoch := cfg.ethClock.GetCurrentEpoch()

		// Get finalized epoch from forkchoice store
		//finalizedEpoch := cfg.forkChoice.FinalizedCheckpoint().Epoch

		// Calculate minimum request epoch
		minimumRequestEpoch := uint64(0)
		if currentEpoch > cfg.beaconCfg.MinEpochsForDataColumnSidecarsRequests {
			minEpoch := currentEpoch - cfg.beaconCfg.MinEpochsForDataColumnSidecarsRequests
			if minEpoch > minimumRequestEpoch {
				minimumRequestEpoch = minEpoch
			}
		}
		if cfg.beaconCfg.FuluForkEpoch > minimumRequestEpoch {
			minimumRequestEpoch = cfg.beaconCfg.FuluForkEpoch
		}

		// Check if any blocks are before minimum request epoch
		for _, block := range blocks {
			blockEpoch := block.Block.Slot / cfg.beaconCfg.SlotsPerEpoch
			if blockEpoch < minimumRequestEpoch {
				return false
			}
		}

		return true*/
}

// processDownloadedBlockBatches processes a batch of downloaded blocks.
// It takes the highest block processed, a flag to determine if insertion is needed, and a list of signed beacon blocks as input.
// It returns the new highest block processed and an error if any.
func processDownloadedBlockBatches(ctx context.Context, logger log.Logger, cfg *Cfg, highestBlockProcessed uint64, shouldInsert bool, blocks []*cltypes.SignedBeaconBlock) (newHighestBlockProcessed uint64, err error) {
	// Pre-process the block batch to ensure that the blocks are sorted by slot in ascending order
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	if err = downloadBlobs(ctx, logger, cfg, highestBlockProcessed, blocks); err != nil {
		return
	}

	var blockRoot common.Hash
	newHighestBlockProcessed = highestBlockProcessed
	// Iterate over each block in the sorted list
	for _, block := range blocks {
		// Compute the hash of the current block
		blockRoot, err = block.Block.HashSSZ()
		if err != nil {
			// Return an error if block hashing fails
			err = fmt.Errorf("failed to hash block: %w", err)
			return
		}

		var hasSignedHeaderInDB bool

		if err = cfg.indiciesDB.View(ctx, func(tx kv.Tx) error {
			_, hasSignedHeaderInDB, err = beacon_indicies.ReadSignedHeaderByBlockRoot(ctx, tx, blockRoot)
			return err
		}); err != nil {
			err = fmt.Errorf("failed to read signed header: %w", err)
			return
		}

		checkDataAvaiability := cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.ImmediateBlobsBackfilling
		// Process the block
		if err = processBlock(ctx, cfg, cfg.indiciesDB, block, false, true, checkDataAvaiability); err != nil {
			if errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
				// Return an error if EIP-4844 data is not available
				logger.Trace("[Caplin] forward sync EIP-4844 data not available", "blockSlot", block.Block.Slot)
				if newHighestBlockProcessed == 0 {
					return 0, nil
				}
				return newHighestBlockProcessed - 1, nil
			}
			// Return an error if block processing fails
			err = fmt.Errorf("bad blocks segment received: %w", err)
			return
		}

		if !hasSignedHeaderInDB && block.Block.Slot%(cfg.beaconCfg.SlotsPerEpoch*2) == 0 {
			var st *state.CachingBeaconState
			// Perform post-processing on the block
			st, err = cfg.forkChoice.GetStateAtBlockRoot(blockRoot, false)
			if err == nil && st != nil {
				// Dump the beacon state on disk if conditions are met
				if err = cfg.forkChoice.DumpBeaconStateOnDisk(st); err != nil {
					// Return an error if dumping the state fails
					err = fmt.Errorf("failed to dump state: %w", err)
					return
				}
				if err = saveHeadStateOnDiskIfNeeded(cfg, st); err != nil {
					// Return an error if saving the head state fails
					err = fmt.Errorf("failed to save head state: %w", err)
					return
				}
			}
		}

		// Update the highest block processed if the current block's slot is higher
		if newHighestBlockProcessed < block.Block.Slot {
			newHighestBlockProcessed = block.Block.Slot
		}

		// If block version is less than BellatrixVersion or shouldInsert is false, skip insertion
		if block.Version() < clparams.BellatrixVersion || !shouldInsert {
			continue
		}
		// Add the block to the block collector
		if err = cfg.blockCollector.AddBlock(block.Block); err != nil {
			// Return an error if adding the block to the collector fails
			err = fmt.Errorf("failed to add block to collector: %w", err)
			return
		}
	}
	return
}

// forwardSync (MAIN ROUTINE FOR ForwardSync) performs the forward synchronization of beacon blocks.
func forwardSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	var (
		shouldInsert  = cfg.executionClient != nil && cfg.executionClient.SupportInsertion() // Check if the execution client supports insertion
		secsPerLog    = 30                                                                   // Interval in seconds for logging progress
		logTicker     = time.NewTicker(time.Duration(secsPerLog) * time.Second)              // Ticker for logging progress
		downloader    = network2.NewForwardBeaconDownloader(ctx, cfg.rpc)                    // Initialize a new forward beacon downloader
		currentSlot   atomic.Uint64                                                          // Atomic variable to track the current slot
		startSlot     = cfg.forkChoice.HighestSeen()
		maxReorgRange = uint64(300) // if node falls too much out of sync, we allow a maximum reorg range of 300 slots
	)
	// Start forwardsync a little bit behind the highest seen slot (account for potential reorgs)
	if startSlot < maxReorgRange {
		startSlot = 0
	} else {
		startSlot = startSlot - maxReorgRange
	}

	finalizedSlot := cfg.forkChoice.FinalizedCheckpoint().Epoch * cfg.beaconCfg.SlotsPerEpoch
	startSlot = max(startSlot, finalizedSlot, cfg.forkChoice.AnchorSlot()) // we cap how low we go with the finalized slot and anchor slot

	// Initialize the slot to download from the finalized checkpoint
	currentSlot.Store(startSlot)

	// Always start from the current finalized checkpoint
	downloader.SetHighestProcessedSlot(currentSlot.Load())

	// Set the function to process downloaded blocks
	downloader.SetProcessFunction(func(initialHighestSlotProcessed uint64, blocks []*cltypes.SignedBeaconBlock) (newHighestSlotProcessed uint64, err error) {
		highestSlotProcessed, err := processDownloadedBlockBatches(ctx, logger, cfg, initialHighestSlotProcessed, shouldInsert, blocks)
		if err != nil {
			logger.Warn("[Caplin] Failed to process block batch", "err", err)
			return initialHighestSlotProcessed, err
		}
		currentSlot.Store(highestSlotProcessed)
		return highestSlotProcessed, nil
	})

	// Get the current slot of the chain tip
	chainTipSlot := cfg.ethClock.GetCurrentSlot()
	logger.Info("[Caplin] Forward Sync", "from", currentSlot.Load(), "to", chainTipSlot)
	prevProgress := currentSlot.Load()

	// Run the log loop until the highest processed slot reaches the chain tip slot
	for downloader.GetHighestProcessedSlot() < chainTipSlot {
		downloader.RequestMore(ctx)
		select {
		case <-ctx.Done():
			// Return if the context is done
			return ctx.Err()
		case <-logTicker.C:
			// Log progress at regular intervals
			progressMade := chainTipSlot - currentSlot.Load()
			distFromChainTip := time.Duration(progressMade*cfg.beaconCfg.SecondsPerSlot) * time.Second
			timeProgress := currentSlot.Load() - prevProgress
			estimatedTimeRemaining := 999 * time.Hour
			if timeProgress > 0 {
				estimatedTimeRemaining = time.Duration(float64(progressMade)/(float64(currentSlot.Load()-prevProgress)/float64(secsPerLog))) * time.Second
			}
			if distFromChainTip < 0 || estimatedTimeRemaining < 0 {
				continue
			}
			prevProgress = currentSlot.Load()
			logger.Info("[Caplin] Forward Sync", "progress", currentSlot.Load(), "distance-from-chain-tip", distFromChainTip, "estimated-time-remaining", estimatedTimeRemaining)
		default:
		}
	}

	return nil
}
