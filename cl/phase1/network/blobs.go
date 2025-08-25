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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common/log/v3"
)

var ErrTimeout = errors.New("timeout")

var requestBlobBatchExpiration = 15 * time.Second

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

func RequestBlobsFrantically(ctx context.Context, r *rpc.BeaconRpcP2P, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (chan PeerAndSidecars, chan error, context.CancelFunc) {
	ctx1, cancel := context.WithCancel(ctx)

	respChan := make(chan PeerAndSidecars, 50)
	nopeersErrChan := make(chan error, 50)
	respErrChan := make(chan error, 50)

	var requestFn func(loopCount int, countStart int)
	requestFn = func(loopCount int, countStart int) {
		if loopCount > 2 {
			return
		}

		select {
		case <-ctx1.Done():
			return
		default:
		}
		responses, pid, err := r.SendBlobsSidecarByIdentifierReq(ctx1, req)

		if err != nil {
			select {
			case <-ctx1.Done():
				return
			default:
			}
			log.Warn("LoopingRequestBlobs: error", "err", err, "peer", pid, "size", req.Len(), "loopCount", loopCount, "countstart", countStart)
			needRetry := true
			if strings.Contains(err.Error(), "no addresses") {
				// needRetry = false
				r.BanPeer(pid)
			}
			if strings.Contains(err.Error(), "peer error code: 1") {
				respErrChan <- err
				r.BanPeer(pid)
			}
			if errors.Is(err, peers.ErrNoPeers) {
				log.Warn("LoopingRequestBlobs: No peers available")
				nopeersErrChan <- err
				needRetry = false
			}
			if needRetry {
				requestFn(loopCount+1, countStart)
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

	var delay time.Duration
	if req.Len() <= 3 {
		delay = 500 * time.Millisecond
	} else if req.Len() <= 6 {
		delay = 1000 * time.Millisecond
	} else if req.Len() <= 9 {
		delay = 1500 * time.Millisecond
	} else if req.Len() <= 12 {
		delay = 2000 * time.Millisecond
	} else if req.Len() <= 15 {
		delay = 2500 * time.Millisecond
	} else {
		delay = 3000 * time.Millisecond
	}

	go func() {
		go requestFn(0, 0)
		for i := 1; ; i++ {
			select {
			case <-ctx1.Done():
				return
			case <-nopeersErrChan:
				for len(nopeersErrChan) > 0 {
					<-nopeersErrChan
				}
				requestFn(0, i) //sync request
			case <-time.After(delay):
				go requestFn(0, i)
			}
		}
	}()

	return respChan, respErrChan, cancel
}
