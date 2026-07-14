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

package daemonsetmutation

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	podMutationCacheTTL    = 5 * time.Minute
	podMutationCacheJitter = 30 * time.Second
)

type cacheEntry struct {
	updatedPod *apiv1.Pod
	generation int64
	expiresAt  time.Time
}

// MutationCache stores cached pod mutations for DaemonSets.
type MutationCache struct {
	mu    sync.RWMutex
	items map[types.UID]cacheEntry
}

// NewMutationCache creates and returns a new MutationCache instance.
func NewMutationCache() *MutationCache {
	return &MutationCache{
		items: make(map[types.UID]cacheEntry),
	}
}

// Get returns the cached mutated pod and a boolean (stale) indicating whether the cache entry
// is missing, modified, or TTL-expired.
func (c *MutationCache) Get(uid types.UID, gen int64) (*apiv1.Pod, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.items[uid]
	// Invalidate the cache entry if the DaemonSet spec has been modified since caching.
	if !ok || entry.generation != gen {
		return nil, true
	}

	return entry.updatedPod.DeepCopy(), time.Now().After(entry.expiresAt)
}

// Set stores a mutated pod and generation in the cache for the given UID.
func (c *MutationCache) Set(uid types.UID, gen int64, pod *apiv1.Pod) error {
	if pod == nil {
		return fmt.Errorf("pod cannot be nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.items[uid]; ok && entry.generation > gen {
		return nil
	}

	c.items[uid] = cacheEntry{
		updatedPod: pod.DeepCopy(),
		generation: gen,
		expiresAt:  time.Now().Add(podMutationCacheTTL + randomJitter(podMutationCacheJitter)),
	}
	return nil
}

// Remove purges the cache entry for the given UID.
func (c *MutationCache) Remove(uid types.UID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, uid)
}

// randomJitter returns a random duration in the range [-maxJitter, maxJitter).
func randomJitter(maxJitter time.Duration) time.Duration {
	if maxJitter <= 0 {
		return 0
	}
	ns := rand.N(2 * maxJitter)
	return ns - maxJitter
}
