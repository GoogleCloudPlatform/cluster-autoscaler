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
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	klog "k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
)

type backoffInfo struct {
	until     time.Time
	rules     map[int]bool
	errorInfo cloudprovider.InstanceErrorInfo
}

// npcCrdBackoff offers a very aggressive, but relatively short-lived backoff strategy.
// The goal is to backoff an entire NodeConfig rule to allow a quick
// fallback to other rules.
type npcCrdBackoff struct {
	mu        sync.RWMutex
	npcLister npc_lister.Lister
	matcher   computeclass.Matcher
	backoffs  map[string]backoffInfo
	duration  time.Duration
	observers []BackoffObserver
}

// NewNpcCrdBackoff returns a new npcCrdBackoff.
func NewNpcCrdBackoff(backoffDuration time.Duration, npcLister npc_lister.Lister, provider backoffCloudProvider, observers ...BackoffObserver) *npcCrdBackoff {
	return &npcCrdBackoff{
		npcLister: npcLister,
		matcher:   computeclass.NewMatcher(npcLister, provider),
		backoffs:  make(map[string]backoffInfo),
		duration:  backoffDuration,
		observers: observers,
	}
}

// Backoff execution for npc crd. Returns time till execution is backed off.
func (b *npcCrdBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !IsStockout(nodeGroup, nodeInfo, errorInfo) {
		return currentTime
	}

	npcCrd, npcCrdName, err := b.npcLister.NodeGroupCrd(nodeGroup)
	if err != nil {
		klog.Errorf("Failed to fetch npc crd when trying to apply npc crd backoff: %v", err)
		return currentTime
	}

	if npcCrd == nil || npcCrdName == "" {
		return currentTime
	}

	if npcCrd.Rules() == nil {
		return currentTime
	}

	ruleFound, ruleIdx, rule := b.matcher.FirstMatchedRule(nodeGroup, npcCrd)
	if !ruleFound {
		return currentTime
	}

	// Backoff does not apply to node pool rules.
	if len(rule.NodePoolNames()) > 0 {
		return currentTime
	}

	if ruleIdx >= getRuleCount(npcCrd)-1 {
		// Don't backoff last rule (counting in ScaleUpAnyway implicit rule).
		// The goal of this backoff is to prioritize latency by quickly falling back
		// through rules. Backing off last rule guarantees we won't scale-up, which is counterproductive.
		return currentTime
	}

	if len(rule.Reservations()) > 1 {
		// Do not apply rule scoped backoff in case multiple specific reservations
		// are used, so that individual reservations could get backed off individually
		return currentTime
	}

	bi, found := b.backoffs[npcCrd.Name()]
	if !found {
		bi.rules = make(map[int]bool)
	}
	bi.rules[ruleIdx] = true
	until := currentTime.Add(b.duration)
	bi.until = until
	bi.errorInfo = errorInfo
	klog.V(4).Infof("Backing off rule %d of npc crd %s:%s until %v. All backed off rules: %v", ruleIdx, npcCrd.Label(), npcCrd.Name(), until, bi.rules)
	b.backoffs[npcCrd.Name()] = bi

	for k := range bi.rules {
		for _, obs := range b.observers {
			obs.OnNpcBackoff(npcCrd, k, errorInfo, until)
		}
	}

	return until
}

// BackoffStatus returns whether the execution is backed off for the given node group and error info when the node group is backed off.
func (b *npcCrdBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, _ *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	b.mu.RLock()
	defer b.mu.RUnlock()

	npcCrd, npcCrdName, err := b.npcLister.NodeGroupCrd(nodeGroup)
	if err != nil {
		klog.Errorf("Failed to fetch npc crd when trying to check npc crd backoff: %v", err)
		return base_backoff.Status{IsBackedOff: false}
	}

	if npcCrd == nil || npcCrdName == "" {
		return base_backoff.Status{IsBackedOff: false}
	}

	if npcCrd.Rules() == nil {
		return base_backoff.Status{IsBackedOff: false}
	}

	bi, found := b.backoffs[npcCrd.Name()]
	if !found || bi.until.Before(currentTime) {
		return base_backoff.Status{IsBackedOff: false}
	}

	ruleFound, ruleIdx, rule := b.matcher.FirstMatchedRule(nodeGroup, npcCrd)
	if !ruleFound || len(rule.NodePoolNames()) > 0 || !bi.rules[ruleIdx] {
		return base_backoff.Status{IsBackedOff: false}
	}

	return base_backoff.Status{IsBackedOff: true, ErrorInfo: bi.errorInfo}
}

// RuleBackoffStatus returns whether the execution is backed off for the given rule and error info when rule is backed off.
func (b *npcCrdBackoff) RuleBackoffStatus(npcCrd crd.CRD, ruleIdx int, currentTime time.Time) base_backoff.Status {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if npcCrd == nil || npcCrd.Name() == "" {
		return base_backoff.Status{IsBackedOff: false}
	}

	if ruleIdx < 0 || ruleIdx >= len(npcCrd.Rules()) {
		return base_backoff.Status{IsBackedOff: false}
	}

	bi, found := b.backoffs[npcCrd.Name()]
	if !found || bi.until.Before(currentTime) {
		return base_backoff.Status{IsBackedOff: false}
	}

	rule := npcCrd.Rules()[ruleIdx]
	if len(rule.NodePoolNames()) > 0 || !bi.rules[ruleIdx] {
		return base_backoff.Status{IsBackedOff: false}
	}

	return base_backoff.Status{IsBackedOff: true, ErrorInfo: bi.errorInfo}
}

// RemoveBackoff is not implemented for npcCrdBackoff.
func (b *npcCrdBackoff) RemoveBackoff(_ cloudprovider.NodeGroup, _ *framework.NodeInfo) {}

// RemoveStaleBackoffData removes stale backoff data.
func (b *npcCrdBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for npcName, bi := range b.backoffs {
		if bi.until.Before(currentTime) {
			delete(b.backoffs, npcName)
			klog.V(4).Infof("NPC backoff expired for npc %s", npcName)
		}
	}
}

func getRuleCount(npcCrd crd.CRD) int {
	ruleCount := len(npcCrd.Rules())
	if npcCrd.ScaleUpAnyway() {
		ruleCount++
	}
	return ruleCount
}

// GetNpcCrdBackoff returns the NpcCrdBackoff from the given composite backoff.
func GetNpcCrdBackoff(compositeBackoff CompositeBackoff) *npcCrdBackoff {
	for _, backoff := range compositeBackoff.GetBackoffs() {
		if npcCrdBackoff, ok := backoff.(*npcCrdBackoff); ok {
			return npcCrdBackoff
		}
	}
	return nil
}
