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

package capacitybuffers

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
)

const (
	// defaultInFlightTTL defines the safety backstop duration for in-flight create records in case watch events or informers stall.
	defaultInFlightTTL = 30 * time.Second

	// defaultDeleteTTL defines the long memory safety window for recorded deletions before sweeping them from RAM map.
	defaultDeleteTTL = 10 * time.Minute
)

// cccInFlightCache maintains partitioned, per-ComputeClass transient tracking for in-flight buffer creates and deletes.
//
// Concurrency Note:
// This cache is intentionally designed without internal mutexes because it is accessed strictly non-concurrently.
// controller-runtime workqueues partition reconciliation events by ComputeClass name and process them serially
// (at most one worker reconciles a given ComputeClass at any point in time). Since cache instances are partitioned
// 1:1 per ComputeClass (via Reconciler.getOrCreateCache), all methods on a given *cccInFlightCache instance execute sequentially.
type cccInFlightCache struct {
	// bufferCreateRequests maps CapacityBuffer resource name (e.g., "cb-my-ccc-active-abcde") to its in-flight representation.
	bufferCreateRequests map[string]internalCapacityBuffer
	// bufferDeleteRequests maps deleted CapacityBuffer UID to the deletion record timestamp.
	bufferDeleteRequests map[types.UID]time.Time
	createTTL            time.Duration
	deleteTTL            time.Duration
}

func newCCCCache() *cccInFlightCache {
	return &cccInFlightCache{
		bufferCreateRequests: make(map[string]internalCapacityBuffer),
		bufferDeleteRequests: make(map[types.UID]time.Time),
		createTTL:            defaultInFlightTTL,
		deleteTTL:            defaultDeleteTTL,
	}
}

// RecordBufferCreate stores the in-memory buffer specification right after client.Create succeeds.
func (c *cccInFlightCache) RecordBufferCreate(cb *cbv1beta1.CapacityBuffer) {
	if cb == nil || cb.Name == "" {
		return
	}

	buf := buildInternalCapacityBuffer(cb)
	buf.PendingCreation = true
	buf.CreationTimestamp = time.Now()
	c.bufferCreateRequests[cb.Name] = buf
}

// DropBufferCreate purges an in-flight buffer record by its assigned name once observed via informer watch or upon error.
func (c *cccInFlightCache) DropBufferCreate(name string) {
	if name == "" {
		return
	}
	delete(c.bufferCreateRequests, name)
}

// GetPendingBuffers returns all non-expired in-flight records for capacity buffers.
func (c *cccInFlightCache) GetPendingBuffers() []internalCapacityBuffer {
	var results []internalCapacityBuffer
	now := time.Now()
	for _, rec := range c.bufferCreateRequests {
		if now.Sub(rec.CreationTimestamp) <= c.createTTL {
			results = append(results, rec)
		}
	}
	return results
}

// RecordBufferDelete marks a UID as deleted and instantly evicts any matching create record by name in O(1) time.
func (c *cccInFlightCache) RecordBufferDelete(uid types.UID, name string) {
	if uid != "" {
		c.bufferDeleteRequests[uid] = time.Now()
	}
	if name != "" {
		delete(c.bufferCreateRequests, name)
	}
}

// HasRecentBufferDelete checks if a capacity buffer UID was recently marked for deletion.
func (c *cccInFlightCache) HasRecentBufferDelete(uid types.UID) bool {
	if uid == "" {
		return false
	}
	_, exists := c.bufferDeleteRequests[uid]
	return exists
}

// PruneExpired sweeps and purges expired in-flight entries.
func (c *cccInFlightCache) PruneExpired() {
	now := time.Now()
	for name, rec := range c.bufferCreateRequests {
		if now.Sub(rec.CreationTimestamp) > c.createTTL {
			delete(c.bufferCreateRequests, name)
		}
	}
	for uid, ts := range c.bufferDeleteRequests {
		if now.Sub(ts) > c.deleteTTL {
			delete(c.bufferDeleteRequests, uid)
		}
	}
}
