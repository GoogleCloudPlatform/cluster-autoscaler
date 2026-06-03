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

package nodetracker

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type Interface interface {
	// AddNode adds a node with the given ID and expiration time to the tracker.
	// The expiration time determines when the node will be automatically removed from the tracker
	// if it hasn't been renewed.
	AddNode(nodeId string, expirationTime time.Time)
	// IsTracked returns true if a node with the given ID is tracked.
	IsTracked(nodeId string) bool
	// DeleteNode removes a node with the given ID from the tracker.
	DeleteNode(nodeId string)
	// Count returns the number of nodes currently being tracked and the earliest expiration time.
	Count() (int, time.Time)
}

type nodeTracker struct {
	mu    sync.Mutex
	nodes map[string]time.Time
	clock clock.PassiveClock
}

func New(clock clock.PassiveClock) *nodeTracker {
	return &nodeTracker{
		nodes: map[string]time.Time{},
		clock: clock,
	}
}

func (t *nodeTracker) AddNode(nodeId string, expirationTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if existingExpirationTime, found := t.nodes[nodeId]; !found || existingExpirationTime.Before(expirationTime) {
		t.nodes[nodeId] = expirationTime
	}
}

func (t *nodeTracker) IsTracked(nodeId string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	expirationTime, found := t.nodes[nodeId]
	if !found {
		return false
	}

	if t.clock.Now().Before(expirationTime) {
		return true
	}

	delete(t.nodes, nodeId)
	return false
}

func (t *nodeTracker) Count() (int, time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := 0
	now := t.clock.Now()
	var earliestExpiration time.Time
	for _, expirationTime := range t.nodes {
		if expirationTime.After(now) {
			count++
			if earliestExpiration.IsZero() || expirationTime.Before(earliestExpiration) {
				earliestExpiration = expirationTime
			}
		}
	}
	return count, earliestExpiration
}

func (t *nodeTracker) DeleteNode(nodeId string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.nodes, nodeId)
}
