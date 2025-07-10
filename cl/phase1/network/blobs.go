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
	"time"

	"golang.org/x/net/context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
)

var requestBlobBatchExpiration = 45 * time.Second

// This is just a bunch of functions to handle blobs

// BlobsIdentifiersFromBlocks returns a list of blob identifiers from a list of blocks, which should then be forwarded to the network.
func BlobsIdentifiersFromBlocks(blocks []*cltypes.SignedBeaconBlock, cfg *clparams.BeaconChainConfig) (*solid.ListSSZ[*cltypes.BlobIdentifier], error) {
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	for _, block := range blocks {
		if block.Version() < clparams.DenebVersion {
			continue
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		kzgCommitments := block.Block.Body.BlobKzgCommitments.Len()
		if ids.Len()+kzgCommitments > cfg.MaxRequestBlobSidecarsByVersion(block.Version()) {
			break
		}
		for i := 0; i < kzgCommitments; i++ {
			ids.Append(&cltypes.BlobIdentifier{
				BlockRoot: blockRoot,
				Index:     uint64(i),
			})
		}
	}
	return ids, nil
}

func BlobsIdentifiersFromBlindedBlocks(blocks []*cltypes.SignedBlindedBeaconBlock, cfg *clparams.BeaconChainConfig) (*solid.ListSSZ[*cltypes.BlobIdentifier], error) {
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	for _, block := range blocks {
		if block.Version() < clparams.DenebVersion {
			continue
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		kzgCommitments := block.Block.Body.BlobKzgCommitments.Len()
		if ids.Len()+kzgCommitments > cfg.MaxRequestBlobSidecarsByVersion(block.Version()) {
			break
		}
		for i := 0; i < kzgCommitments; i++ {
			ids.Append(&cltypes.BlobIdentifier{
				BlockRoot: blockRoot,
				Index:     uint64(i),
			})
		}
	}
	return ids, nil
}

type PeerAndSidecars struct {
	Peer      string
	Responses []*cltypes.BlobSidecar
}

func RequestBlobsFrantically(ctx context.Context, r *rpc.BeaconRpcP2P, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (chan PeerAndSidecars, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)

	respChan := make(chan PeerAndSidecars, 50)
	nopeersErrChan := make(chan error, 50)

	var requestFn = func(ctx context.Context, loopCount int, countStart int) {}
	requestFn = func(ctx context.Context, loopCount int, countStart int) {
		if loopCount > 2 {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
		responses, pid, err := r.SendBlobsSidecarByIdentifierReq(ctx, req)

		if err != nil {
			log.Warn("LoopingRequestBlobs: error", "err", err, "peer", pid, "size", req.Len(), "loopCount", loopCount, "countstart", countStart)
			needRetry := true
			if strings.Contains(err.Error(), "deadline exceeded") {
				r.BanPeer(pid)
			}
			if strings.Contains(err.Error(), "no addresses") {
				// needRetry = false
				r.BanPeer(pid)
			}
			if strings.Contains(err.Error(), "peer error code: 1") {
				r.BanPeer(pid)
			}
			if errors.Is(err, peers.ErrNoPeers) {
				log.Warn("LoopingRequestBlobs: No peers available")
				nopeersErrChan <- err
				needRetry = false
			}
			if needRetry {
				go requestFn(ctx, loopCount+1, countStart)
			}
			return
		}
		if responses == nil {
			log.Warn("LoopingRequestBlobs: response is nil", "peer", pid)
			return
		}
		if len(responses) == 0 {
			return
		}
		resp := PeerAndSidecars{
			Peer:      pid,
			Responses: responses,
		}
		respChan <- resp
	}

	go func() {
		go requestFn(ctx, 0, 0)
		delay := time.Duration(300 * time.Millisecond)
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-nopeersErrChan:
				for len(nopeersErrChan) > 0 {
					<-nopeersErrChan
				}
				delay = time.Duration(5000 * time.Millisecond)
			case <-time.After(delay):
				go requestFn(ctx, 0, i)
				delay = time.Duration(300 * time.Millisecond)
			}
		}
	}()

	return respChan, cancel
}
