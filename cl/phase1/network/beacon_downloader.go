// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package network

import (
	"errors"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
)

// Input: the currently highest slot processed and the list of blocks we want to know process
// Output: the new last new highest slot processed and an error possibly?
type ProcessFn func(
	highestSlotProcessed uint64,
	blocks []*cltypes.SignedBeaconBlock) (
	newHighestSlotProcessed uint64,
	err error)

type ForwardBeaconDownloader struct {
	ctx                   context.Context
	highestSlotProcessed  uint64
	highestSlotUpdateTime time.Time
	rpc                   *rpc.BeaconRpcP2P
	process               ProcessFn

	mu sync.Mutex
}

func NewForwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P) *ForwardBeaconDownloader {
	return &ForwardBeaconDownloader{
		ctx: ctx,
		rpc: rpc,
	}
}

// SetProcessFunction sets the function used to process segments.
func (f *ForwardBeaconDownloader) SetProcessFunction(fn ProcessFn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.process = fn
}

// SetHighestProcessedSlot sets the highest processed slot so far.
func (f *ForwardBeaconDownloader) SetHighestProcessedSlot(highestSlotProcessed uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if highestSlotProcessed > f.highestSlotProcessed {
		f.highestSlotProcessed = highestSlotProcessed
		f.highestSlotUpdateTime = time.Now()
	}
}

type peerAndBlocks struct {
	peerId string
	blocks []*cltypes.SignedBeaconBlock
}

func (f *ForwardBeaconDownloader) RequestMore(ctx context.Context) {
	count := uint64(16)
	respChan := make(chan peerAndBlocks)
	nopeersErrChan := make(chan error, 5)
	var requestFn func(ctx context.Context, loopCount int)
	requestFn = func(ctx context.Context, loopCount int) {
		if loopCount > 3 {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		var reqSlot uint64
		if f.highestSlotProcessed > 2 {
			reqSlot = f.highestSlotProcessed - 2
		}
		// double the request count every 10 seconds. This is inspired by the mekong network, which has many consecutive missing blocks.
		reqCount := count
		// NEED TO COMMENT THIS BC IT CAUSES ISSUES ON MAINNET

		// if !f.highestSlotUpdateTime.IsZero() {
		// 	multiplier := int(time.Since(f.highestSlotUpdateTime).Seconds()) / 10
		// 	multiplier = min(multiplier, 6)
		// 	reqCount *= uint64(1 << uint(multiplier))
		// }

		// leave a warning if we are stuck for more than 90 seconds
		if time.Since(f.highestSlotUpdateTime) > 90*time.Second {
			log.Trace("Forward beacon downloader gets stuck", "time", time.Since(f.highestSlotUpdateTime).Seconds(), "highestSlotProcessed", f.highestSlotProcessed)
		}
		// this is so we do not get stuck on a side-fork
		responses, peerId, err := f.rpc.SendBeaconBlocksByRangeReq(ctx, reqSlot, reqCount)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			needRetry := true
			if strings.Contains(err.Error(), "no addresses") {
				f.rpc.BanPeer(peerId)
			}
			if errors.Is(err, peers.ErrNoPeers) {
				log.Trace("No peers available for beacon blocks by range request", "err", err, "peer", peerId, "slot", reqSlot, "reqCount", reqCount)
				nopeersErrChan <- err
				needRetry = false
			}
			log.Warn("Failed to send beacon blocks by range request", "err", err, "peer", peerId, "slot", reqSlot, "reqCount", reqCount, "loopCount", loopCount)
			if needRetry {
				requestFn(ctx, loopCount+1)
			}
			return
		}
		if responses == nil {
			return
		}
		if len(responses) == 0 {
			f.rpc.BanPeer(peerId)
			return
		}
		respChan <- peerAndBlocks{peerId, responses}
	}

	reqCtx, cancel := context.WithCancel(ctx)

	go func() {
		go requestFn(reqCtx, 0)
		for {
			select {
			case <-reqCtx.Done():
				return
			case <-nopeersErrChan:
				for len(nopeersErrChan) > 0 {
					<-nopeersErrChan
				}
				requestFn(reqCtx, 0) //sync request
			case <-time.After(3 * time.Second):
				go requestFn(reqCtx, 0)
			}
		}
	}()

	select {
	case <-reqCtx.Done():
		return
	case atomicResp, ok := <-respChan:
		if ok {
			cancel()

			var highestSlotProcessed uint64
			var err error
			blocks := atomicResp.blocks
			if highestSlotProcessed, err = f.process(f.highestSlotProcessed, blocks); err != nil {
				return
			}
			if highestSlotProcessed > f.highestSlotProcessed {
				f.highestSlotProcessed = highestSlotProcessed
				f.highestSlotUpdateTime = time.Now()
			}
			return
		}
	}
}

// GetHighestProcessedSlot retrieve the highest processed slot we accumulated.
func (f *ForwardBeaconDownloader) GetHighestProcessedSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestSlotProcessed
}

func (f *ForwardBeaconDownloader) Peers() (uint64, error) {
	return f.rpc.Peers()
}
