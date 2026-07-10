// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reservations

import (
	"context"
	"sync"
	"time"

	"google.golang.org/api/compute/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/klog/v2"
)

const (
	blocksFetchInterval = time.Minute
)

type blockPullerCloudProvider interface {
	// GetReservationBlocksInReservation returns the reservation blocks for a particular reservation, in specfied project and zone.
	GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error)
	// GetReservationSubBlocksInReservationBlock returns the reservation subBlocks for a particular reservation block, in specfied reservation, project, and zone.
	GetReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error)
}

// BlocksPuller pulls GCE reservation blocks for GCE reservations. Stores reservation blocks in an in-memory cache
type BlocksPuller struct {
	sync.RWMutex
	provider           blockPullerCloudProvider
	reservationsPuller *gceclient.ReservationsPuller
	reservationBlocks  map[gceclient.ReservationRef][]*gceclient.GceReservationBlock
}

// NewBlocksPuller builds a new BlocksPuller.
func NewBlocksPuller(provider blockPullerCloudProvider, reservationsPuller *gceclient.ReservationsPuller) *BlocksPuller {
	r := &BlocksPuller{
		provider:           provider,
		reservationsPuller: reservationsPuller,
		reservationBlocks:  make(map[gceclient.ReservationRef][]*gceclient.GceReservationBlock),
	}
	return r
}

// GetReservationBlocksInReservation returns the cached blocks for the provided reservation, in a specific project and zone.
func (p *BlocksPuller) GetReservationBlocksInReservation(ref gceclient.ReservationRef) []*gceclient.GceReservationBlock {
	p.RLock()
	defer p.RUnlock()

	if blocks, ok := p.reservationBlocks[ref]; ok {
		return blocks
	}
	return nil
}

// setReservationBlocksInReservation sets the blocks for the provided reservation, in a specific project and zone.
func (p *BlocksPuller) setReservationBlocksInReservation(blocks map[gceclient.ReservationRef][]*gceclient.GceReservationBlock) {
	p.Lock()
	defer p.Unlock()
	p.reservationBlocks = blocks
}

// Run pulls reservation blocks for all reservations periodically.
func (p *BlocksPuller) Run(ctx context.Context) {
	klog.V(0).Info("Enabling Reservation Blocks Puller")

	p.Loop()

	ticker := time.NewTicker(blocksFetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.Loop()
		}
	}
}

// Loop runs a single loop of reservation blocks pulling,
// fetching them from local and shared reservations.
func (p *BlocksPuller) Loop() {
	if p.reservationsPuller == nil {
		return
	}
	reservations := p.reservationsPuller.GetReservations()
	if len(reservations) == 0 {
		return
	}

	rbsMap := p.blocksPullerLoop(reservations)
	rbsMap = p.subblocksPullerLoop(rbsMap)
	p.setReservationBlocksInReservation(rbsMap)
}

func (p *BlocksPuller) blocksPullerLoop(reservations []*compute.Reservation) map[gceclient.ReservationRef][]*gceclient.GceReservationBlock {
	rbsMap := make(map[gceclient.ReservationRef][]*gceclient.GceReservationBlock)

	startTime := time.Now()
	blocksCount := 0
	klog.V(4).Info("Starting blocksPuller loop")
	for _, reservation := range reservations {
		// filter reservations and only list blocks for reservations that do have blocks
		if reservation.ResourceStatus != nil && reservation.ResourceStatus.ReservationBlockCount == 0 {
			continue
		}
		key := gceclient.GetReservationRefFromReservation(*reservation)
		rbs, err := p.provider.GetReservationBlocksInReservation(key)
		if err != nil {
			klog.Errorf("Error when getting reservation blocks: %v", err)
			continue
		}
		if len(rbs) == 0 {
			if reservation.ResourceStatus != nil {
				klog.Warningf("Reservation has non zero block count but no reservation blocks were fetched: reservation=%s, blockCount=%d", reservation.Name, reservation.ResourceStatus.ReservationBlockCount)
			}
			continue
		}
		var filteredRbs []*gceclient.GceReservationBlock
		for _, rb := range rbs {
			if rb.Status == "READY" {
				filteredRbs = append(filteredRbs, rb)
			}
		}
		blocksCount += len(filteredRbs)
		rbsMap[key] = filteredRbs
	}
	klog.V(4).Infof("blocksPuller loop: duration=%v, reservations=%d, reservationBlocks=%d", time.Since(startTime), len(reservations), blocksCount)
	return rbsMap
}

func (p *BlocksPuller) subblocksPullerLoop(reservationBlocks map[gceclient.ReservationRef][]*gceclient.GceReservationBlock) map[gceclient.ReservationRef][]*gceclient.GceReservationBlock {
	rbsMap := make(map[gceclient.ReservationRef][]*gceclient.GceReservationBlock)

	startTime := time.Now()
	klog.V(4).Info("Starting subBlocksPuller loop")
	sbCount := 0

	for ref, rbs := range reservationBlocks {
		var blocks []*gceclient.GceReservationBlock
		for _, rb := range rbs {
			// copy block value
			newRb := *rb
			blocks = append(blocks, &newRb)

			// create new reference with the block name
			r := ref
			r.BlockName = rb.Name

			// get subBlocks
			subBlocks, err := p.provider.GetReservationSubBlocksInReservationBlock(r)
			if err != nil {
				klog.Errorf("Error when getting reservation subblocks: %v", err)
				continue
			}
			if len(subBlocks) == 0 {
				continue

			}

			sbCount += len(subBlocks)
			newRb.SubBlockCount = int64(len(subBlocks))
			newRb.SubBlocks = make([]*gceclient.GceReservationSubBlock, 0, len(subBlocks))
			// filter subBlocks
			for _, subBlock := range subBlocks {
				if subBlock.Status == "READY" {
					newRb.SubBlocks = append(newRb.SubBlocks, subBlock)
				}
			}
		}
		rbsMap[ref] = blocks
	}
	klog.V(4).Infof("subBlocksPuller loop: duration=%v, reservationBlocks=%d, subBlocks=%d", time.Since(startTime), len(reservationBlocks), sbCount)
	return rbsMap
}
