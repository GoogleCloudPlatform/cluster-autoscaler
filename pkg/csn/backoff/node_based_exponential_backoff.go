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

package backoff

import (
	"math/rand"
	"sync"
	"time"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

type nodeBasedExponentialBackoff struct {
	maxBackoffDuration     time.Duration
	initialBackoffDuration time.Duration
	backoffResetTimeout    time.Duration
	useJitter              bool

	mu          sync.Mutex
	backoffInfo map[string]nodeBackoffInfo
}

type nodeBackoffInfo struct {
	duration            time.Duration
	backoffUntil        time.Time
	lastFailedExecution time.Time
	errorInfo           cloudprovider.InstanceErrorInfo
}

// NewNodeBasedExponentialBackoff creates an instance of exponential backoff with node name used as a key.
func NewNodeBasedExponentialBackoff(initialBackoffDuration, maxBackoffDuration, backoffResetTimeout time.Duration, useJitter bool) *nodeBasedExponentialBackoff {
	return &nodeBasedExponentialBackoff{
		maxBackoffDuration:     maxBackoffDuration,
		initialBackoffDuration: initialBackoffDuration,
		backoffResetTimeout:    backoffResetTimeout,
		backoffInfo:            make(map[string]nodeBackoffInfo),
		useJitter:              useJitter,
	}
}

func (b *nodeBasedExponentialBackoff) getNodeKey(nodeInfo *framework.NodeInfo) string {
	if nodeInfo != nil && nodeInfo.Node() != nil && nodeInfo.Node().Name != "" {
		return nodeInfo.Node().Name
	}
	return ""
}

// Backoff execution for the given node group. Returns time till execution is backed off.
func (b *nodeBasedExponentialBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	duration := b.initialBackoffDuration
	key := b.getNodeKey(nodeInfo)
	if key == "" {
		return currentTime
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if backoffInfo, found := b.backoffInfo[key]; found {
		// Multiple concurrent scale-ups failing shouldn't cause
		// backoff duration to increase exponentially
		duration = backoffInfo.duration
		if backoffInfo.backoffUntil.Before(currentTime) {
			// Node is not currently in backoff, but was recently
			// Increase backoff duration exponentially
			duration = b.nextBackoffDuration(backoffInfo.duration)
			if duration > b.maxBackoffDuration {
				duration = b.maxBackoffDuration
			}
		}
	}
	backoffUntil := currentTime.Add(duration)
	b.backoffInfo[key] = nodeBackoffInfo{
		duration:            duration,
		backoffUntil:        backoffUntil,
		lastFailedExecution: currentTime,
		errorInfo:           errorInfo,
	}
	return backoffUntil
}

func (b *nodeBasedExponentialBackoff) nextBackoffDuration(duration time.Duration) time.Duration {
	if !b.useJitter {
		return 2 * duration
	}
	return duration + time.Duration(rand.Int63n(int64(duration)))
}

// BackoffStatus returns whether the execution is backed off for the given node group and error info when the node group is backed off.
func (b *nodeBasedExponentialBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	key := b.getNodeKey(nodeInfo)
	if key == "" {
		return base_backoff.Status{IsBackedOff: false}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	backoffInfo, found := b.backoffInfo[key]
	if !found || backoffInfo.backoffUntil.Before(currentTime) {
		return base_backoff.Status{IsBackedOff: false}
	}
	return base_backoff.Status{
		IsBackedOff: true,
		ErrorInfo:   backoffInfo.errorInfo,
	}
}

// RemoveBackoff removes backoff data for the given node group.
func (b *nodeBasedExponentialBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
	key := b.getNodeKey(nodeInfo)
	if key == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.backoffInfo, key)
}

// RemoveStaleBackoffData removes stale backoff data.
func (b *nodeBasedExponentialBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, backoffInfo := range b.backoffInfo {
		if backoffInfo.lastFailedExecution.Add(b.backoffResetTimeout).Before(currentTime) {
			delete(b.backoffInfo, key)
		}
	}
}
