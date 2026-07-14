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

package retry

import (
	"context"
	"maps"
	"math/rand"
	"sync"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/retry/internal"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/set"
)

const logPrefix = "CSN BackoffManager:"

// Config holds configuration parameters for BackoffManager.
type Config struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// Event is emitted by the BackoffManager on each execution of the internal
// loop that is responsible for queuing operations that are ready to be retried.
// This can be used for observability purposes.
type Event struct {
	// Number of operations that have been queued.
	RetriedOps int
}

// Key determines the fields that are used to track unique retry counts.
type Key struct {
	NodeName string
	OpType   ops.OperationType
}

// BackoffManager handles retrying failed operations with an exponential backoff.
type BackoffManager struct {
	mu     sync.Mutex
	clock  clock.Clock
	config Config

	enqueue       func(ops.Operation) error
	eventHandlers []func(Event)

	retryCounts map[Key]int
	retryQueue  *internal.RetryQueue
	notifyCh    chan bool
}

// NewBackoffManager creates a new BackoffManager.
// It uses enqueue to retry operations for nodes.
// Event structs are emitted by calling handleEvent, which allows
// for observability of the internal BackoffManager loop.
func NewBackoffManager(
	cl clock.Clock,
	config Config,
	enqueue func(ops.Operation) error,
	eventHandlers ...func(Event),
) *BackoffManager {
	return &BackoffManager{
		enqueue:       enqueue,
		eventHandlers: eventHandlers,
		clock:         cl,
		config:        config,
		retryCounts:   make(map[Key]int),
		retryQueue:    internal.NewRetryQueue(),
		notifyCh:      make(chan bool, 1),
	}
}

// BackoffResult is returned when operation backoff is attempted for
// a set of nodes.
type BackoffResult struct {
	// BackedOffNodes are nodes for which the operation will be retried later.
	BackedOffNodes set.Set[string]
	// FailedNodes are nodes for which the operation should be considered a
	// permanent failure.
	FailedNodes set.Set[string]
}

// AddFailedNodes registers failed nodes for a specific operation to be retried.
func (m *BackoffManager) AddFailedNodes(op ops.Operation, failedNodes set.Set[string]) BackoffResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := BackoffResult{FailedNodes: set.New[string](), BackedOffNodes: set.New[string]()}
	for nodeName := range failedNodes {
		k := Key{NodeName: nodeName, OpType: op.Type}
		count := m.retryCounts[k]
		count++
		if count > m.config.MaxRetries {
			klog.Errorf("%s max retries reached for node %q, operation %v. Dropping.", logPrefix, nodeName, op.Type)
			delete(m.retryCounts, k)
			result.FailedNodes.Insert(nodeName)
			continue
		}
		m.retryCounts[k] = count
		delay := m.calculateDelay(count)
		m.retryQueue.Push(internal.DelayedOp{
			Op: ops.Operation{
				MIG:       op.MIG,
				Type:      op.Type,
				NodeNames: set.New(nodeName),
			},
			ExecuteAfter: m.clock.Now().Add(delay),
		})
		result.BackedOffNodes.Insert(nodeName)
	}
	if len(result.BackedOffNodes) > 0 {
		m.notify()
	}
	return result
}

// ClearRetryCount resets the retry count for successfully completed operations.
// The dispatcher or handler might not need to call this if we just let old
// counts linger, but it's cleaner to clear them.
func (m *BackoffManager) ClearRetryCount(opType ops.OperationType, nodeNames set.Set[string]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for nodeName := range nodeNames {
		delete(m.retryCounts, Key{NodeName: nodeName, OpType: opType})
	}
}

// RetryCounts allows callers to observe the number of retries
// attempted for currently tracked nodes.
func (m *BackoffManager) RetryCounts() map[Key]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.retryCounts)
}

// make sure retryNum is  >= 1
func (m *BackoffManager) calculateDelay(retryNum int) time.Duration {
	// Simple exponential backoff: initial * 2^(retryNum-1)
	multiplier := 1 << (retryNum - 1)
	delay := time.Duration(multiplier) * m.config.InitialDelay
	if delay > m.config.MaxDelay || delay <= 0 /* overflow */ {
		return m.config.MaxDelay
	}
	halfDelay := delay / 2
	// Randomize the other half
	jitter := time.Duration(rand.Int63n(int64(halfDelay)))
	// Combine the fixed half and the randomized half
	return halfDelay + jitter
}

// Run starts the retry loop that periodically checks for operations
// ready to be re-enqueued.
func (m *BackoffManager) Run(ctx context.Context) {
	timer := m.clock.NewTimer(m.config.MaxDelay)
	// Periodic check if there are no events.
	// The timer is set to the next execution time.
	for {
		m.mu.Lock()
		now := m.clock.Now()
		readyToRetry := m.retryQueue.PopReadyToRun(now)
		m.setNextExecutionTime(timer, now)
		m.mu.Unlock()

		runEvent := Event{}
		for _, delayedOp := range readyToRetry {
			if err := m.enqueue(delayedOp.Op); err != nil {
				klog.Errorf("%s failed to re-enqueue retry for op %q: %v", logPrefix, delayedOp, err)
				continue
			}
			runEvent.RetriedOps += 1
		}
		m.emitEvent(runEvent)

		select {
		case <-timer.C():
		case <-m.notifyCh:
			safeStop(timer)
		case <-ctx.Done():
			safeStop(timer)
			return
		}
	}
}

// notify signals to the BackoffManager that the retry loop should
// be executed
func (m *BackoffManager) notify() {
	select {
	case m.notifyCh <- true:
	default:
		// do not notify if already notified
	}
}

func (m *BackoffManager) setNextExecutionTime(timer clock.Timer, now time.Time) {
	firstToRun, ok := m.retryQueue.FirstToRun()
	if !ok {
		// next execution to be triggered by queue entry
		return
	}
	timer.Reset(firstToRun.ExecuteAfter.Sub(now))
}

func (m *BackoffManager) emitEvent(e Event) {
	for _, f := range m.eventHandlers {
		f(e)
	}
}

func safeStop(timer clock.Timer) {
	timer.Stop()
	// Some timer implementations do not automatically
	// clean up the channel after calling Stop.
	select {
	case <-timer.C():
	default:
	}
}
