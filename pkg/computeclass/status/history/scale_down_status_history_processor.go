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

package history

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	scaledownstatus "k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/klog/v2"
)

const (
	pendingDeleteExpirationTimeout = 15 * time.Minute
)

type pendingDeleteInfo struct {
	crdId        status.CRDId
	priority     string
	registeredAt time.Time
}

// ScaleDownStatusHistoryProcessor collects information related to ScalingEventsHistory - ConsolidatedNodesCount.
type ScaleDownStatusHistoryProcessor struct {
	lister         lister.Lister
	matcher        computeclass.Matcher
	updatesCh      chan<- status.UpdateMessage
	pendingDeletes map[string]pendingDeleteInfo
	now            func() time.Time
}

// NewScaleDownStatusHistoryProcessor creates a new ScaleDownStatusHistoryProcessor.
func NewScaleDownStatusHistoryProcessor(lister lister.Lister, provider machineConfigProvider, updatesCh chan<- status.UpdateMessage) *ScaleDownStatusHistoryProcessor {
	return &ScaleDownStatusHistoryProcessor{
		lister:         lister,
		matcher:        computeclass.NewMatcher(lister, provider),
		updatesCh:      updatesCh,
		pendingDeletes: make(map[string]pendingDeleteInfo),
		now:            time.Now,
	}
}

// Process analyses the scale down status and updates ConsolidatedNodesCount.
func (p *ScaleDownStatusHistoryProcessor) Process(context *context.AutoscalingContext, scaleDownStatus *scaledownstatus.ScaleDownStatus) {
	if scaleDownStatus == nil || p.updatesCh == nil {
		return
	}

	p.cleanupStalePendingDeletes()

	// 1. Map node groups from ScaledDownNodes to CCC and priority, storing in pendingDeletes.
	for _, node := range scaleDownStatus.ScaledDownNodes {
		nodeGroup := node.NodeGroup
		if nodeGroup == nil {
			continue
		}

		ruleIndex, c, err := getRuleIndex(nodeGroup, p.lister, p.matcher)
		if err != nil {
			klog.Errorf("Failed to retrieve rule index for node group %v: %v", nodeGroup.Id(), err)
			continue
		}
		if c == nil {
			continue
		}

		crdId := status.CRDId{CRDName: c.Name(), CRDLabel: c.Label()}
		ruleIdxStr := fmt.Sprintf("%d", ruleIndex)

		if node.Node == nil || node.Node.Name == "" {
			continue
		}
		p.pendingDeletes[node.Node.Name] = pendingDeleteInfo{
			crdId:        crdId,
			priority:     ruleIdxStr,
			registeredAt: p.now(),
		}
	}

	// 2. Process NodeDeleteResults for successful deletions, grouping by crdId and priority (rule index).
	deltas := make(map[status.CRDId]map[string]int)
	for nodeName, result := range scaleDownStatus.NodeDeleteResults {
		if result.ResultType == scaledownstatus.NodeDeleteOk {
			if info, found := p.pendingDeletes[nodeName]; found {
				crdId := info.crdId
				rIdx := info.priority

				if _, ok := deltas[crdId]; !ok {
					deltas[crdId] = make(map[string]int)
				}
				deltas[crdId][rIdx]++
			}
		}
		// Always clean up pending deletes for nodes that have results reported.
		delete(p.pendingDeletes, nodeName)
	}

	// 3. Send updates with the collected deltas.
	for crdId, ruleDeltas := range deltas {
		for rIdx, delta := range ruleDeltas {
			rIdxVal := rIdx
			deltaVal := delta
			crdIdVal := crdId

			p.updatesCh <- status.UpdateMessage{
				Id: crdIdVal,
				Mutate: func(s crd.CRDStatus) {
					current := s.GetRuleScalingHistory(rIdxVal)
					updated := crd.ScalingEventsHistory{}
					if current != nil {
						updated = *current
					}
					updated.ConsolidatedNodesCount += deltaVal
					updated.MeasuredAt = metav1.NewTime(p.now())
					s.UpdateRuleScalingHistory(rIdxVal, updated)
				},
			}
		}
	}
}

// CleanUp implements status.ScaleDownStatusProcessor.
func (p *ScaleDownStatusHistoryProcessor) CleanUp() {
}

func (p *ScaleDownStatusHistoryProcessor) cleanupStalePendingDeletes() {
	now := p.now()
	for nodeName, info := range p.pendingDeletes {
		if now.Sub(info.registeredAt) > pendingDeleteExpirationTimeout {
			delete(p.pendingDeletes, nodeName)
			klog.V(4).Infof("Removed stale pending delete for node %s", nodeName)
		}
	}
}
