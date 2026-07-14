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
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/scaleupfailures"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
)

// historyClusterStateRegistry is an interface mapping to clusterstate.ClusterStateRegistry methods
// needed by processor, to allow simple mocking in unit tests.
type historyClusterStateRegistry interface {
	IsNodeGroupRegistered(nodeGroupName string) bool
	IsNodeGroupAtTargetSize(nodeGroupName string) bool
	GetScaleUpFailures() map[string][]scaleupfailures.Record
}

// AutoscalingStatusHistoryProcessor implements status.AutoscalingStatusProcessor.
// It verifies whether previously initiated scaleups reported by the ScaleUpStatusHistoryProcessor
// have successfully completed according to ClusterStateRegistry.
type AutoscalingStatusHistoryProcessor struct {
	sharedData *scaleUpData
	updatesCh  chan<- npc_status.UpdateMessage
	observer   npc_processors.MinCapacityObserver
}

// NewAutoscalingStatusHistoryProcessor creates a new processor.
func NewAutoscalingStatusHistoryProcessor(sharedData *scaleUpData, updatesCh chan<- npc_status.UpdateMessage, observer npc_processors.MinCapacityObserver) *AutoscalingStatusHistoryProcessor {
	return &AutoscalingStatusHistoryProcessor{
		sharedData: sharedData,
		updatesCh:  updatesCh,
		observer:   observer,
	}
}

// Process evaluates unfinished scaleups and updates provisioned nodes counts if target size is reached.
func (p *AutoscalingStatusHistoryProcessor) Process(context *context.AutoscalingContext, csr *clusterstate.ClusterStateRegistry, now time.Time) error {
	return p.process(context, csr, now)
}

func (p *AutoscalingStatusHistoryProcessor) process(context *context.AutoscalingContext, csr historyClusterStateRegistry, now time.Time) error {
	if p.updatesCh == nil {
		return nil
	}

	p.sharedData.periodicCleanup()

	for nodeGroupId, failures := range csr.GetScaleUpFailures() {
		if len(failures) > 0 {
			p.sharedData.markScaleUpFailed(nodeGroupId)
			p.observeMinCapacityFailures(nodeGroupId, failures, now)
		}
	}

	deltas := make(map[npc_status.CRDId]map[string]*crd.ScalingEventsHistory)

	getDelta := func(crdId npc_status.CRDId, ruleIdx string) *crd.ScalingEventsHistory {
		if _, ok := deltas[crdId]; !ok {
			deltas[crdId] = make(map[string]*crd.ScalingEventsHistory)
		}
		if _, ok := deltas[crdId][ruleIdx]; !ok {
			deltas[crdId][ruleIdx] = &crd.ScalingEventsHistory{
				MeasuredSince: metav1.NewTime(now),
			}
		}
		return deltas[crdId][ruleIdx]
	}

	// Iterate through all unfinished scale-up operations to check if they have completed.
	unfinished := p.sharedData.getUnfinishedNodeGroups()
	for nodeGroupId, scaleUpInfo := range unfinished {
		// As per HTNAP Async Node Groups requirements, skip `-async` suffixed node groups to prevent double counting.
		// These are part of a larger logical scale-up event.
		if strings.Contains(nodeGroupId, "-async") {
			continue
		}

		// Check if the node group has reached its target size. We only report metrics once the scaling operation is finished.
		isAtTarget := csr.IsNodeGroupAtTargetSize(nodeGroupId)
		isFailed := scaleUpInfo.failed

		if isAtTarget {
			// If the target size is reached (or the operation failed), calculate the final metrics.
			var currentSize int
			for _, delta := range scaleUpInfo.deltas {
				// Determine the actual size reached. If the scale-up was marked as failed, we fetch the current
				// size from the registry. Otherwise, we assume success and use the expected target size.
				if isFailed {
					currentSize = getNodeGroupTargetSize(context, nodeGroupId)
				} else {
					currentSize = delta.targetSize
				}

				// Calculate how many nodes were actually added to the cluster during this operation.
				added := currentSize - delta.initialSize
				// Ensure we don't report negative node additions if the group size decreased for other reasons.
				if added < 0 {
					added = 0
				}
				// Clamp the added nodes to the maximum requested delta. We don't want to credit this scale-up
				// with nodes that were already there or added by other processes.
				maxAdded := delta.targetSize - delta.initialSize
				if added > maxAdded {
					added = maxAdded
				}

				// Accumulate the provisioned nodes count into the CRD's status history.
				d := getDelta(delta.crdId, delta.ruleIndex)
				d.ProvisionedNodesCount += added
				d.MeasuredAt = metav1.NewTime(now)
			}

			// Mark the scale-up as finished so it's removed from the tracking buffer.
			p.sharedData.finishScaleUp(nodeGroupId)
		}
	}

	for crdId, ruleDeltas := range deltas {
		for ruleIdx, delta := range ruleDeltas {
			// Capture variables for closure
			rIdx := ruleIdx
			d := delta
			crdId := crdId

			p.updatesCh <- npc_status.UpdateMessage{
				Id: crdId,
				Mutate: func(s crd.CRDStatus) {
					current := s.GetRuleScalingHistory(rIdx)
					updated := *d // Start with delta copy

					if current != nil {
						updated.ProvisionedNodesCount += current.ProvisionedNodesCount
						updated.ConsolidatedNodesCount += current.ConsolidatedNodesCount
						updated.MigratedNodesCount += current.MigratedNodesCount
						// Keep earliest MeasuredSince
						if !current.MeasuredSince.IsZero() && (updated.MeasuredSince.IsZero() || current.MeasuredSince.Before(&updated.MeasuredSince)) {
							updated.MeasuredSince = current.MeasuredSince
						}
					}
					s.UpdateRuleScalingHistory(rIdx, updated)

					existingConditions := s.GetRuleConditions(rIdx)
					// we want to overwrite the NodeProvisioningInProgress condition if it exists to the newest one.
					var overwritten []metav1.Condition
					for _, existing := range existingConditions {
						if existing.Type != ConditionTypeNodeProvisioningInProgress {
							overwritten = append(overwritten, existing)
						}
					}
					s.UpdateRuleConditions(rIdx, overwritten)
				},
			}
		}
	}

	return nil
}

func getNodeGroupTargetSize(context *context.AutoscalingContext, nodeGroupId string) int {
	for _, ng := range context.CloudProvider.NodeGroups() {
		if ng.Id() == nodeGroupId {
			targetSize, err := ng.TargetSize()
			if err != nil {
				klog.Warningf("Failed to get TargetSize for node group %s: %v", nodeGroupId, err)
				return 0
			}
			return targetSize
		}
	}
	return 0
}

// CleanUp implements status.AutoscalingStatusProcessor.
func (p *AutoscalingStatusHistoryProcessor) CleanUp() {
	// Cleanup is handled by Aggregator in standard flow.
}

func (p *AutoscalingStatusHistoryProcessor) observeMinCapacityFailures(nodeGroupId string, failures []scaleupfailures.Record, now time.Time) {
	if p.observer == nil || len(failures) == 0 {
		return
	}

	unfinished := p.sharedData.getUnfinishedNodeGroups()
	if info, ok := unfinished[nodeGroupId]; ok {
		for _, delta := range info.deltas {
			if delta.isMinCapacity {
				failure := failures[0]
				reason := failure.ErrorInfo.ErrorCode
				errType := string(reason)
				p.observer.OnProvisioningError(delta.crdId.CRDName, errType, false, now)
			}
		}
	}
}
