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
package peers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PeeredObject[T any] struct {
	Peer string
	Data T
}

var (
	ErrNoPeers  = errors.New("no peers")
	PeerRecover = errors.New("recover peers")
)

// Item is an item in the pool
type Item struct {
	id      peer.ID
	score   atomic.Int64
	uses    int
	banned  atomic.Bool
	inuse   atomic.Bool
	inqueue atomic.Bool
}

func (i *Item) Id() peer.ID {
	return i.id
}

func (i *Item) String() string {
	return i.id.String()
}

func (i *Item) Score() int {
	return int(i.score.Load())
}

func (i *Item) Add(n int) int {
	return int(i.score.Add(int64(n)))
}

// PeerPool is a pool of peers
type Pool struct {
	host host.Host
	peerData sync.Map

	queue           chan *Item
	peerRecoverCond *sync.Cond
}

func NewPool(h host.Host) *Pool {
	p := &Pool{
		host:            h,
		peerData:        sync.Map{},
		queue:           make(chan *Item, 1024),
		peerRecoverCond: sync.NewCond(&sync.Mutex{}),
	}
	return p
}

func (p *Pool) pushNoDup(it *Item) {
	_, ok := p.peerData.Load(it.id)
	if !ok {
		return
	}
	if !it.inqueue.CompareAndSwap(false, true) {
		return
	}

	select {
	case p.queue <- it:
		p.peerRecoverCond.Broadcast()
	default:
		it.inqueue.Store(false)
	}
}

func (p *Pool) BanStatus(pid peer.ID) bool {
	if v, ok := p.peerData.Load(pid); ok {
		item := v.(*Item)
		if item.banned.Load() {
			return true
		}
	}

	return false
}

func (p *Pool) LenBannedPeers() int {
	count := 0
	p.peerData.Range(func(key, v any) bool {
		if v.(*Item).banned.Load() {
			count += 1
		}
		return true
	})
	return count
}

func (p *Pool) AddPeer(pid peer.ID) {
	if _, ok := p.peerData.Load(pid); ok {
		return
	}
	newItem := &Item{
		id: pid,
	}
	p.peerData.Store(pid, newItem)
	// add it to our queue as a new item
	p.pushNoDup(newItem)
}

func (p *Pool) SetBanStatus(pid peer.ID, banned bool) {
	v, ok := p.peerData.Load(pid)
	if !ok {
		return
	}

	item := v.(*Item)
	if banned {
		item.banned.Store(true)
		time.AfterFunc(30*time.Minute, func() {
			item.banned.Store(false)
			p.pushNoDup(item)
		})
	} else {
		item.banned.Store(false)
		p.pushNoDup(item)
	}
}

func (p *Pool) RemovePeer(pid peer.ID) {
	p.peerData.Delete(pid)

}

// Request a peer from the pool
// caller MUST call the done function when done with peer IFF err != nil
func (p *Pool) Request(ctx context.Context) (item *Item, done func(), err error) {
	empty := true
	p.peerData.Range(func(_, v any) bool {
		it := v.(*Item)
		if !it.banned.Load() {
			empty = false
			return false
		}
		return true
	})
	checkItem := func(item *Item) (done func()) {
		item.inqueue.Store(false)
		if _, ok := p.peerData.Load(item.id); ok {
			if !item.banned.Load() {
				if item.inuse.CompareAndSwap(false, true) {
					done := func() {
						_, ok := p.peerData.Load(item.id)
						if !ok {
							return
						}
						item.inuse.Store(false)

						if item.banned.Load() {
							return
						}
						p.pushNoDup(item)
					}
					return done
				}
			}
		}
		return nil
	}
	if empty {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case item := <-p.queue:
			done1 := checkItem(item)
			if done1 != nil {
				return item, done1, nil
			}
		}
		return nil, nil, PeerRecover
	}

	for {
		select {
		case item := <-p.queue:
			done1 := checkItem(item)
			if done1 != nil {
				return item, done1, nil
			}
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}
