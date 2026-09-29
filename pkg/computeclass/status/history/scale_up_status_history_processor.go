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
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	npc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podkind"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

const (
	ConditionTypeNodeProvisioningInProgress = "NodeProvisioningInProgress"
	ConditionReasonPodPending               = "PodPending"
	ConditionReasonMinimumCapacity          = "MinimumCapacity"
	ConditionReasonActiveMigration          = "ActiveMigration"
	ConditionReasonCapacityBuffer           = "CapacityBuffer"
	ConditionReasonProvisioningRequest      = "ProvisioningRequest"
	ConditionReasonCapacityRequest          = "CapacityRequest"
	ConditionReasonNodePoolMinSize          = "NodePoolMinSize"
)

type machineConfigProvider interface {
	IsAutopilotEnabled() bool
}

// ScaleUpStatusHistoryProcessor is responsible for recording scale-up events to the shared scaleUpData,
// which is later used to report autoscaling status in the Node Provisioning Config.
type ScaleUpStatusHistoryProcessor struct {
	lister             lister.Lister
	matcher            computeclass.Matcher
	provider           machineConfigProvider
	experimentsManager experiments.Manager
	// sharedData is the buffer where scale-up events are recorded.
	sharedData *scaleUpData
	updatesCh  chan<- npc_status.UpdateMessage
	now        func() time.Time
	observer   npc_processors.MinCapacityObserver
}

// NewScaleUpStatusHistoryProcessor creates a new ScaleUpStatusHistoryProcessor.
func NewScaleUpStatusHistoryProcessor(lister lister.Lister, provider machineConfigProvider, sharedData *scaleUpData, updatesCh chan<- npc_status.UpdateMessage, observer npc_processors.MinCapacityObserver, experimentsManager experiments.Manager) *ScaleUpStatusHistoryProcessor {
	return &ScaleUpStatusHistoryProcessor{
		lister:             lister,
		matcher:            computeclass.NewMatcher(lister, provider),
		provider:           provider,
		sharedData:         sharedData,
		updatesCh:          updatesCh,
		now:                time.Now,
		observer:           observer,
		experimentsManager: experimentsManager,
	}
}

func (p *ScaleUpStatusHistoryProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	if p.sharedData == nil || scaleUpStatus == nil {
		return
	}
	if !computeclass.IsComputeClassEnhancedObservabilityEnabled(p.experimentsManager) {
		return
	}

	deltasByRule := p.collectDeltas(scaleUpStatus)

	if p.updatesCh == nil {
		return
	}

	reason, ok := determineScaleUpReason(scaleUpStatus.PodsTriggeredScaleUp)
	if !ok {
		return
	}
	p.emitConditions(deltasByRule, reason, p.now())
}

func (p *ScaleUpStatusHistoryProcessor) collectDeltas(scaleUpStatus *status.ScaleUpStatus) map[npc_status.CRDId]map[string][]ScaleUpDelta {
	deltasByRule := make(map[npc_status.CRDId]map[string][]ScaleUpDelta)

	for _, scaleUpInfo := range scaleUpStatus.ScaleUpInfos {
		if scaleUpInfo.NewSize <= scaleUpInfo.CurrentSize {
			continue
		}
		added := scaleUpInfo.NewSize - scaleUpInfo.CurrentSize

		nodeGroup := scaleUpInfo.Group
		if isAsyncNodeGroup(nodeGroup) {
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

		crdId := npc_status.CRDId{CRDName: c.Name(), CRDLabel: c.Label()}
		ruleIdxStr := fmt.Sprintf("%d", ruleIndex)
		nodeConfig, zone := getNodeGroupConfigAndZone(nodeGroup)

		isMinCapacity := false
		actualRuleIdx := -1
		if ruleIndex < len(c.Rules()) {
			actualRuleIdx = ruleIndex
			if c.Rules()[ruleIndex].TargetNodeCount() != nil {
				isMinCapacity = true
			}
		} else {
			if c.TargetNodeCount() != nil {
				isMinCapacity = true
			}
		}

		if p.observer != nil && isMinCapacity && scaleUpStatus.Result == status.ScaleUpSuccessful {
			klog.V(5).Infof("ScaleUpStatusHistoryProcessor (MinCapacity): Scale up observed for ComputeClass %s, rule %d", c.Name(), actualRuleIdx)
			p.observer.OnScaleUpDecision(c.Name(), actualRuleIdx, p.now())
		}

		delta := ScaleUpDelta{
			crdId:         crdId,
			ruleIndex:     ruleIdxStr,
			addedNodes:    added,
			initialSize:   scaleUpInfo.CurrentSize,
			targetSize:    scaleUpInfo.NewSize,
			config:        nodeConfig,
			zone:          zone,
			isMinCapacity: isMinCapacity,
		}
		p.sharedData.registerScaleUp(nodeGroup.Id(), delta)

		if _, ok := deltasByRule[crdId]; !ok {
			deltasByRule[crdId] = make(map[string][]ScaleUpDelta)
		}
		deltasByRule[crdId][ruleIdxStr] = append(deltasByRule[crdId][ruleIdxStr], delta)
	}
	return deltasByRule
}

func (p *ScaleUpStatusHistoryProcessor) emitConditions(deltasByRule map[npc_status.CRDId]map[string][]ScaleUpDelta, reason string, now time.Time) {
	for crdId, ruleDeltas := range deltasByRule {
		for ruleIdx, deltas := range ruleDeltas {
			var addedTotal int
			configs := make(map[nodeGroupConfig]map[string]struct{})
			for _, d := range deltas {
				addedTotal += d.addedNodes

				if _, ok := configs[d.config]; !ok {
					configs[d.config] = make(map[string]struct{})
				}
				configs[d.config][d.zone] = struct{}{}
			}

			var configStrs []string
			for cfg, zonesMap := range configs {
				var zones []string
				for z := range zonesMap {
					zones = append(zones, z)
				}
				sort.Strings(zones)
				configStrs = append(configStrs, fmt.Sprintf("{%s, Zones: %s}", cfg.String(), strings.Join(zones, ", ")))
			}
			sort.Strings(configStrs)

			cond := metav1.Condition{
				Type:               ConditionTypeNodeProvisioningInProgress,
				Status:             metav1.ConditionTrue,
				Reason:             reason,
				Message:            fmt.Sprintf("NodeProvisioning associated with this priority triggered due to %s. %d new nodes will be added with config: %s", reasonDescription(reason), addedTotal, strings.Join(configStrs, ", ")),
				LastTransitionTime: metav1.NewTime(now),
			}

			rIdx := ruleIdx

			npc_status.TrySendRuleUpdate(p.updatesCh, npc_status.UpdateMessage{
				Id: crdId,
				Mutate: func(s crd.CRDStatus) {
					existingConditions := s.GetRuleConditions(rIdx)
					var deduplicated []metav1.Condition
					for _, existing := range existingConditions {
						if existing.Type != ConditionTypeNodeProvisioningInProgress {
							deduplicated = append(deduplicated, existing)
						}
					}
					deduplicated = append(deduplicated, cond)
					s.UpdateRuleConditions(rIdx, deduplicated)
				},
			}, rIdx)
		}
	}
}

func (p *ScaleUpStatusHistoryProcessor) CleanUp() {
	// Processor doesn't keep state, so nothing to clean up locally.
}

// getRuleIndex returns the rule index for the given node group.
// Copied from pkg/computeclass/processors/utils.go to avoid dependency cycle/visibility issues.
func getRuleIndex(nodeGroup cloudprovider.NodeGroup, lister lister.Lister, matcher computeclass.Matcher) (int, crd.CRD, error) {
	c, cName, err := lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return -1, nil, err
	}

	if c == nil || cName == "" {
		return 0, nil, nil
	}

	ruleFound, ruleIndex, _ := matcher.FirstMatchedRule(nodeGroup, c)

	// Check for no rule matching.
	if !ruleFound {
		if len(c.Rules()) > 0 && !c.ScaleUpAnyway() {
			return -1, nil, fmt.Errorf("nodepool: %v shouldn't scale scale up. crd: %v:%v", nodeGroup, c.Label(), c.Name())
		}
		return len(c.Rules()), c, nil
	}
	return ruleIndex, c, nil
}

func isAsyncNodeGroup(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return false
	}
	return mig.IsUpcoming() && !mig.Exist(context.TODO())
}

// reasonDescriptions maps a NodeProvisioningInProgress condition reason to the
// phrase used in its message.
var reasonDescriptions = map[string]string{
	ConditionReasonPodPending:          "pending pods",
	ConditionReasonActiveMigration:     "active migration",
	ConditionReasonMinimumCapacity:     "minimum capacity",
	ConditionReasonCapacityBuffer:      "capacity buffers",
	ConditionReasonProvisioningRequest: "provisioning request",
	ConditionReasonCapacityRequest:     "capacity request",
	ConditionReasonNodePoolMinSize:     "node pool minimum size",
}

// reasonDescription returns the phrase used in the condition message for the
// given reason. It should never fall back, but an empty phrase would produce a
// broken message ("triggered due to ."), so guard against it explicitly.
func reasonDescription(reason string) string {
	if description, ok := reasonDescriptions[reason]; ok {
		return description
	}
	klog.Errorf("No description registered for NodeProvisioningInProgress reason %q, falling back to a generic one", reason)
	return "autoscaling"
}

// determineScaleUpReason returns the reason to report for a scale-up triggered
// by the given pods. The second return value is false when the scale-up should
// not be surfaced to the user at all, which is the case when it was caused
// solely by Cluster Autoscaler internal pods.
func determineScaleUpReason(pods []*apiv1.Pod) (string, bool) {
	if len(pods) == 0 {
		// The scale-up was not triggered by any pod. Today the only path
		// producing a successful scale-up without triggering pods is
		// ScaleUpToNodeGroupMinSize (--enforce-node-group-min-size), which grows
		// node pools that are below their minimum size.
		return ConditionReasonNodePoolMinSize, true
	}

	kinds := make(map[podkind.Kind]bool, len(pods))
	for _, pod := range pods {
		kinds[podkind.Of(pod)] = true
	}

	// Today all of those should be exclusive, however this is not guaranteed in the future.
	switch {
	case kinds[podkind.PodPending] || kinds[podkind.UnrecognizedFake]:
		// UnrecognizedFake pods come from the OSS proactive scale-up injector,
		// which copies pods of controllers that are missing replicas. Those are
		// real user replicas, so they are reported as pending pods.
		return ConditionReasonPodPending, true
	case kinds[podkind.ProvisioningRequest]:
		return ConditionReasonProvisioningRequest, true
	case kinds[podkind.CapacityRequest]:
		return ConditionReasonCapacityRequest, true
	case kinds[podkind.ActiveMigration]:
		return ConditionReasonActiveMigration, true
	case kinds[podkind.MinCapacity]:
		return ConditionReasonMinimumCapacity, true
	case kinds[podkind.CapacityBuffer]:
		return ConditionReasonCapacityBuffer, true
	}

	// Only internal pods (e.g. EK VM lookahead buffer) triggered the scale-up.
	// There is nothing actionable for the user here, so skip the condition
	// rather than blaming one of the user-facing reasons.
	klog.V(4).Infof("Skipping %s condition: scale-up triggered only by internal pods", ConditionTypeNodeProvisioningInProgress)
	return "", false
}
