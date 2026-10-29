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

// Package scaledownpriority reads scale-down priority from a node taint.
package scaledownpriority

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// InvalidPriorityEventReason is the reason of the Warning event recorded on a
// node whose scale-down priority taint has no valid value.
const InvalidPriorityEventReason = "InvalidScaleDownPriority"

// reportTTL is how long a node's invalid-value report is remembered after the
// node was last seen. Entries of nodes that left the cluster expire after it.
const reportTTL = time.Hour

type report struct {
	values   string
	lastSeen time.Time
}

// TaintParser resolves node scale-down priorities from a node taint. A nil
// *TaintParser is disabled and returns priority 0 for every node.
type TaintParser struct {
	taintKey string
	recorder record.EventRecorder
	clock    clock.PassiveClock

	// reported holds, per node name, the invalid taint values for which a
	// Warning event was last recorded. An event is emitted when a node's set
	// of invalid values changes, not on every autoscaler loop. An entry is
	// dropped when the node has a valid value again, and expires reportTTL
	// after the node was last seen, so the map does not grow with node churn.
	mu        sync.Mutex
	reported  map[string]report
	lastSweep time.Time
}

// NewTaintParser returns a TaintParser reading priority from taintKey. An
// empty key disables scale-down priority. recorder may be nil, in which case
// invalid values are only logged.
func NewTaintParser(taintKey string, recorder record.EventRecorder) *TaintParser {
	return newTaintParser(taintKey, recorder, clock.RealClock{})
}

func newTaintParser(taintKey string, recorder record.EventRecorder, clk clock.PassiveClock) *TaintParser {
	return &TaintParser{taintKey: taintKey, recorder: recorder, clock: clk, reported: map[string]report{}, lastSweep: clk.Now()}
}

// Enabled reports whether a non-empty taint key is configured.
func (p *TaintParser) Enabled() bool {
	return p != nil && p.taintKey != ""
}

// NodePriority returns the scale-down priority of node: the highest value of
// the priority taint that parses as a positive integer, across all effects,
// or 0 if the taint is absent or no value is valid.
//
// A node whose taint carries only invalid values is reported as a Warning
// event (InvalidPriorityEventReason) and a log line. The event is emitted
// once per node and set of invalid values; it is repeated if the values
// change, if the node had a valid value in between, or after a Cluster
// Autoscaler restart (the bookkeeping is in memory). A valid value under any
// effect suppresses the report.
func (p *TaintParser) NodePriority(node *apiv1.Node) int {
	if !p.Enabled() || node == nil {
		return 0
	}
	priority := 0
	invalid := ""
	for _, t := range node.Spec.Taints {
		if t.Key != p.taintKey {
			continue
		}
		if n, err := strconv.Atoi(t.Value); err == nil && n > 0 {
			priority = max(priority, n)
		} else {
			invalid += strconv.Quote(t.Value) + " "
		}
	}
	if priority > 0 || invalid == "" {
		p.forget(node.Name)
	} else {
		p.reportInvalid(node, invalid[:len(invalid)-1])
	}
	return priority
}

func (p *TaintParser) forget(nodeName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.reported, nodeName)
}

func (p *TaintParser) reportInvalid(node *apiv1.Node, values string) {
	now := p.clock.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sweepLocked(now)
	if r, ok := p.reported[node.Name]; ok && r.values == values {
		r.lastSeen = now
		p.reported[node.Name] = r
		return
	}
	p.reported[node.Name] = report{values: values, lastSeen: now}
	msg := fmt.Sprintf("scale-down priority taint %s has value(s) %s, expected a positive integer; the node is not prioritized for scale-down", p.taintKey, values)
	klog.Warningf("Node %s: %s", node.Name, msg)
	if p.recorder != nil {
		p.recorder.Event(node, apiv1.EventTypeWarning, InvalidPriorityEventReason, msg)
	}
}

// sweepLocked drops reports of nodes not seen for reportTTL. It runs at most
// once per reportTTL. Must be called with p.mu held.
func (p *TaintParser) sweepLocked(now time.Time) {
	if now.Sub(p.lastSweep) < reportTTL {
		return
	}
	p.lastSweep = now
	for name, r := range p.reported {
		if now.Sub(r.lastSeen) >= reportTTL {
			delete(p.reported, name)
		}
	}
}
