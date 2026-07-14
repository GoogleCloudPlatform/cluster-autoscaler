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

package flexadvisor

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/klog/v2"
)

const (
	// ConditionTypeRuleFilteredOut is the condition type emitted when GCE Flex Advisor filters out all machines for a rule.
	ConditionTypeRuleFilteredOut = "ProvisioningSuspended"
	// ConditionReasonFilteredOut is the reason for RuleFilteredOut condition.
	ConditionReasonFilteredOut = "CapacityConstrained"

	// ConditionTypeRulePartiallyFiltered is the condition type emitted when GCE Flex Advisor filters out some but not all machines for a rule.
	ConditionTypeRulePartiallyFiltered = "ProvisioningConstrained"

	// ConditionMessageFilteredOutFormat is the format for RuleFilteredOut condition message.
	ConditionMessageFilteredOutFormat = "No matching configuration is available (0/%d available) due to active capacity cooldowns across requested zones."
	// ConditionMessagePartiallyFilteredFormat is the format for RulePartiallyFiltered condition message.
	ConditionMessagePartiallyFilteredFormat = "A subset (%d/%d) of configurations are excluded due to active capacity cooldowns across requested zones."
)

// FilteringState represents the filtering state of a rule.
type FilteringState string

const (
	// FilteringStateNone means no machines are filtered out.
	FilteringStateNone FilteringState = "None"
	// FilteringStatePartial means some but not all machines are filtered out.
	FilteringStatePartial FilteringState = "Partial"
	// FilteringStateFull means all machines are filtered out.
	FilteringStateFull FilteringState = "Full"
)

type filteredOutInstancesState struct {
	state         FilteringState
	totalCount    int
	filteredCount int
}

func (w *scopeWorker) updateRuleFilteringConditions(results map[string]*api.InstanceAvailability) {
	if w.statusUpdatesCh == nil || w.cccLister == nil {
		return
	}
	cc, _ := w.instanceConfigGenerator.matchingCrd(w.scope.flexibilityScopeKey) // errors are logged upstream

	if cc == nil {
		return
	}

	for idx, rule := range cc.Rules() {
		ruleIdxStr := fmt.Sprintf("%d", idx)
		res := w.countFilteredOutInstances(results, rule, idx)

		msg := status.UpdateMessage{
			Id: status.CRDId{
				CRDName:  cc.Name(),
				CRDLabel: cc.Label(),
			},
			Mutate: func(s crd.CRDStatus) {
				existingConditions := s.GetRuleConditions(ruleIdxStr)
				var deduplicated []metav1.Condition
				for _, existing := range existingConditions {
					if existing.Type != ConditionTypeRuleFilteredOut && existing.Type != ConditionTypeRulePartiallyFiltered {
						deduplicated = append(deduplicated, existing)
					}
				}
				switch res.state {
				case FilteringStateFull:
					deduplicated = append(deduplicated, metav1.Condition{
						Type:               ConditionTypeRuleFilteredOut,
						Status:             metav1.ConditionTrue,
						Reason:             ConditionReasonFilteredOut,
						Message:            fmt.Sprintf(ConditionMessageFilteredOutFormat, res.totalCount),
						LastTransitionTime: metav1.Now(),
					})
				case FilteringStatePartial:
					deduplicated = append(deduplicated, metav1.Condition{
						Type:               ConditionTypeRulePartiallyFiltered,
						Status:             metav1.ConditionTrue,
						Reason:             ConditionReasonFilteredOut,
						Message:            fmt.Sprintf(ConditionMessagePartiallyFilteredFormat, res.filteredCount, res.totalCount),
						LastTransitionTime: metav1.Now(),
					})
				}
				s.UpdateRuleConditions(ruleIdxStr, deduplicated)
			},
		}
		if w.statusUpdatesCh != nil {
			select {
			case w.statusUpdatesCh <- msg:
			default:
				klog.Warningf("FlexAdvisor: status channel is full, dropping status update for ComputeClass %s, rule %d", cc.Name(), idx)
			}
		}
	}
}

func (w *scopeWorker) countFilteredOutInstances(results map[string]*api.InstanceAvailability, rule rules.Rule, ruleIdx int) filteredOutInstancesState {
	configs, _ := w.instanceConfigGenerator.generateInstanceConfigsForRule(rule, -1)

	if len(configs) == 0 {
		return filteredOutInstancesState{state: FilteringStateNone}
	}

	totalCount := 0
	filteredCount := 0

	for _, config := range configs {
		snapshotAvailability, found := results[config.Signature()]
		if !found {
			continue
		}
		snapshot := snapshotAvailability.NewSnapshot()

		for zone := range config.Zones() {

			maxAvailable, ok := snapshot.MaxAvailableInstances(zone)
			if !ok {
				continue
			}

			totalCount++
			if maxAvailable <= 0 {
				filteredCount++
			}
		}
	}

	if totalCount == 0 {
		return filteredOutInstancesState{state: FilteringStateNone}
	}

	if filteredCount == totalCount {
		return filteredOutInstancesState{
			state:         FilteringStateFull,
			totalCount:    totalCount,
			filteredCount: filteredCount,
		}
	}
	if filteredCount > 0 {
		return filteredOutInstancesState{
			state:         FilteringStatePartial,
			totalCount:    totalCount,
			filteredCount: filteredCount,
		}
	}
	return filteredOutInstancesState{state: FilteringStateNone}
}
