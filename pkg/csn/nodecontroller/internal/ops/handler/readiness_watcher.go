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

package handler

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

const (
	// nodeReadinessTimeout is how long a resumed node is given to report ready
	// to Kubernetes before the operation gives up on it and retries.
	nodeReadinessTimeout = 5 * time.Minute
	// nodeReadinessPollInterval is how often the watched nodes are looked up. The
	// lookups hit an informer cache rather than the API server, so the interval
	// can stay short.
	nodeReadinessPollInterval = 2 * time.Second

	nodeReadinessPrefix = "Node Readiness Watcher:"
)

// readinessWatcher waits for resumed nodes to report ready to Kubernetes.
//
// It polls every node from a single goroutine rather than one goroutine per
// node: they all read the same informer cache on the same interval, so a
// goroutine apiece would buy nothing and would cost a batch of a thousand nodes
// a thousand stacks and five hundred wakeups a second for as long as its slowest
// node takes.
type readinessWatcher struct {
	nodeLister   NodeLister
	timeout      time.Duration
	pollInterval time.Duration
	logPrefix    string

	// mu guards the maps below, which the polling goroutine and the goroutine
	// handing over nodes both touch.
	mu sync.Mutex
	// waitingSince maps each node still being waited for to the moment its wait
	// began, which is both what the timeout is counted from and what the wait
	// is measured against once it ends.
	waitingSince map[string]time.Time
	// notReady maps each node given up on to the reason. It holds the outcome of
	// the watch, and is only read once the watcher has stopped.
	notReady map[string]error
}

func newReadinessWatcher(nodeLister NodeLister, maxNodes int, logPrefix string) *readinessWatcher {
	if logPrefix != "" {
		logPrefix += " "
	}
	logPrefix += nodeReadinessPrefix
	return &readinessWatcher{
		nodeLister:   nodeLister,
		timeout:      nodeReadinessTimeout,
		pollInterval: nodeReadinessPollInterval,
		waitingSince: make(map[string]time.Time, maxNodes),
		notReady:     make(map[string]error),
		logPrefix:    logPrefix,
	}
}

// watch starts waiting for a node, timing it from now, the moment its instance
// came back up.
func (w *readinessWatcher) watch(nodeName string) {
	if status, done := w.readyOrDeleted(nodeName); done {
		// The node was already back by the time it was handed over, so it never
		// waited at all.
		observeReadinessWait(status, 0)
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waitingSince[nodeName] = time.Now()
}

// keepOnly stops waiting for every node outside consumed.
//
// Those waits are cut short rather than seen through, so they say nothing about
// how long a node takes to come back and go unmeasured.
func (w *readinessWatcher) keepOnly(consumed set.Set[string]) {
	w.mu.Lock()
	defer w.mu.Unlock()
	maps.DeleteFunc(w.waitingSince, func(nodeName string, _ time.Time) bool {
		return !consumed.Has(nodeName)
	})
}

// poll looks up every watched node, letting go of the ones that are back and
// giving up on the ones that have run out of time, and reports how many are
// left.
//
// The lookups are made under the lock. They read an informer cache, so lookups
// are fast.
func (w *readinessWatcher) poll(now time.Time) (left int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for nodeName, startedAt := range w.waitingSince {
		status, done := w.readyOrDeleted(nodeName)
		switch {
		case done:
			w.stopWaiting(nodeName, startedAt, now, status)
		case !now.Before(startedAt.Add(w.timeout)):
			w.notReady[nodeName] = w.notReadyError(nodeName, context.DeadlineExceeded)
			w.stopWaiting(nodeName, startedAt, now, readinessTimedOut)
		}
	}
	return len(w.waitingSince)
}

// giveUpOnRemaining gives up on every node still being waited for, recording
// cause as the reason.
func (w *readinessWatcher) giveUpOnRemaining(cause error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	for nodeName, startedAt := range w.waitingSince {
		w.notReady[nodeName] = w.notReadyError(nodeName, cause)
		w.stopWaiting(nodeName, startedAt, now, readinessAborted)
	}
}

// stopWaiting stops waiting for a node and records how long its wait lasted
// under status. It must be called with mu held.
func (w *readinessWatcher) stopWaiting(nodeName string, startedAt, now time.Time, status string) {
	delete(w.waitingSince, nodeName)
	observeReadinessWait(status, now.Sub(startedAt))
}

// observeReadinessWait records a finished wait for a node's readiness.
func observeReadinessWait(status string, waited time.Duration) {
	nodeReadinessWaitSeconds.WithLabelValues(status).Observe(waited.Seconds())
}

// run polls until every node is either back or given up on, which it can only
// know once noMoreNodes says that the caller has handed over its last node. A
// cancelled context stops it early, giving up on whatever is left.
func (w *readinessWatcher) run(ctx context.Context, noMoreNodes <-chan struct{}) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	allNodesIn := false
	for {
		select {
		case <-ctx.Done():
			// Nothing showed that the nodes still being waited for came back, so they
			// are given up on rather than passed off as consumed.
			w.giveUpOnRemaining(ctx.Err())
			return
		case <-noMoreNodes:
			// From here an empty watcher is a finished one. Nilling the channel stops
			// the select from picking this case again, as a nil channel is never
			// ready.
			allNodesIn, noMoreNodes = true, nil
		case <-ticker.C:
		}
		if left := w.poll(time.Now()); allNodesIn && left == 0 {
			return
		}
	}
}

// notReadyError returns the error recorded for a node that never became ready
// in time.
func (w *readinessWatcher) notReadyError(nodeName string, cause error) error {
	return fmt.Errorf("gave up waiting for node %q to become ready within %v of its instance being resumed: %w", nodeName, w.timeout, cause)
}

// readyOrDeleted reports whether there is nothing left to wait for: either
// Kubernetes sees the node as ready, or the node was deleted, in which case
// waiting would only hold up the operation. It reads an informer cache, so it
// costs no API call. The status it returns tells the two apart, so that the time
// a node that was deleted spent waiting is not passed off as the time it takes a
// node to come back.
//
// Any other lookup failure leaves the node waiting: the next poll looks again,
// and the timeout caps the wait.
func (w *readinessWatcher) readyOrDeleted(nodeName string) (status string, done bool) {
	node, err := w.nodeLister.Get(nodeName)
	if apierrors.IsNotFound(err) {
		klog.V(4).Infof("%s node %q was deleted, so there is no readiness left to wait for", w.logPrefix, nodeName)
		return readinessDeleted, true
	}
	if err != nil {
		klog.Warningf("%s Failed to look up node %q while waiting for it to become ready: %v", w.logPrefix, nodeName, err)
		return "", false
	}
	readiness, err := kube_util.GetNodeReadiness(node)
	if err != nil {
		// A node that does not report a readiness condition yet is not ready.
		return "", false
	}
	if !readiness.Ready {
		return "", false
	}
	return readinessReady, true
}
