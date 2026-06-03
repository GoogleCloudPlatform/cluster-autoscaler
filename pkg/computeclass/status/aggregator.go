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

package status

import (
	"context"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
	ctrClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Aggregator is responsible for aggregating status updates for CRDs and periodically applying them.
type Aggregator struct {
	client    client.Client
	lister    lister.Lister
	dirtySet  map[CRDId]bool
	statusMap map[CRDId]crd.CRDStatus
	inputCh   chan UpdateMessage
	ctrClient ctrClient.Client
}

// NewAggregator creates a new Aggregator.
func NewAggregator(client client.Client, lister lister.Lister, inputCh chan UpdateMessage, ctrClient ctrClient.Client) *Aggregator {
	return &Aggregator{
		client:    client,
		lister:    lister,
		dirtySet:  make(map[CRDId]bool),
		statusMap: make(map[CRDId]crd.CRDStatus),
		inputCh:   inputCh,
		ctrClient: ctrClient,
	}
}

// Start starts the aggregation loop.
func (a *Aggregator) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case msg := <-a.inputCh:
			// 1. Fetch or Init State
			crd, err := a.lister.Crd(msg.Id.CRDLabel, msg.Id.CRDName)
			if err != nil {
				klog.Errorf("Failed to fetch CRD %s/%s: %v", msg.Id.CRDLabel, msg.Id.CRDName, err)
				continue
			}

			// 2. Apply the Functional Mutator
			// This merges the partial update into the master state safely.
			status := a.getOrCreateStatus(crd)
			msg.Mutate(status)

			// 3. Mark as Dirty
			a.dirtySet[msg.Id] = true

		case <-ticker.C:
			// 4. Batch Flush
			a.makeUpdates(ctx)

		case <-cleanupTicker.C:
			a.cleanup()

		case <-ctx.Done():
			return
		}
	}
}

func (a *Aggregator) cleanup() {
	count := 0
	for id := range a.statusMap {
		if _, err := a.lister.Crd(id.CRDLabel, id.CRDName); err != nil {
			delete(a.statusMap, id)
			delete(a.dirtySet, id)
			count++
		}
	}
	klog.V(4).Infof("Aggregator cleanup removed metadata for %d CRDs.", count)
}

func (a *Aggregator) makeUpdates(ctx context.Context) {
	klog.V(4).Infof("Aggregator flushing updates for %d CRDs.", len(a.dirtySet))
	for crdId := range a.dirtySet {
		crd, err := a.lister.Crd(crdId.CRDLabel, crdId.CRDName)
		if err != nil {
			klog.Errorf("Failed to fetch CRD %s/%s for update: %v", crdId.CRDLabel, crdId.CRDName, err)
			continue
		}
		status := a.getOrCreateStatus(crd)
		err = a.ctrClient.Status().Patch(ctx, status.GetCRDStatusPatch(), ctrClient.Apply, ctrClient.FieldOwner("cluster-autoscaler"), ctrClient.ForceOwnership)
		if err != nil {
			klog.Errorf("Failed to patch status for CRD %s/%s: %v", crdId.CRDLabel, crdId.CRDName, err)
		}
	}
	// reset the dirty set
	a.dirtySet = make(map[CRDId]bool)
}

func (a *Aggregator) getOrCreateStatus(crdObj crd.CRD) crd.CRDStatus {
	id := CRDId{
		CRDName:  crdObj.Name(),
		CRDLabel: crdObj.Label(),
	}

	if status, ok := a.statusMap[id]; ok {
		return status
	}

	status := ccc.NewCccCRDStatus(crdObj.Name())

	a.statusMap[id] = status
	return status
}
