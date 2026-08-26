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

package safeguard

import (
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	cc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	podutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	cb "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

const (
	filteredFalsePositiveThreshold = 10 * time.Minute
	cleanupThreshold               = 30 * time.Minute
)

// Preprocessor is an interface for processors that support preprocessing pods.
type Preprocessor interface {
	Preprocess(unschedulablePods []*apiv1.Pod)
}

// SafeguardedPodListProcessor wraps a PodListProcessor to monitor "false positive schedulable" pods, that is
// pods that a given processor consistently filters out with the assumption that they can schedule on existing nodes (specifically tracking ready nodes),
// yet remain unscheduled in the cluster, which indicates a disconnect between CA assumptions and scheduler.
// In case a pod remains pending in the cluster for longer than filteredFalsePositiveThreshold (10 minutes),
// the safeguard reports metrics.
//
// Restoration Option (restore bool): When restore=true, pods identified as false positives are injected back into
// unschedulablePods to force CA to provision new capacity. Processors currently run in metrics-only mode (restore=false).
type SafeguardedPodListProcessor struct {
	// processor is the underlying PodListProcessor being monitored and wrapped.
	processor pods.PodListProcessor
	// reason identifies the specific metric label value under which false positives are reported.
	reason metrics.FilteredFalsePositiveReason
	// restore indicates whether pods identified as false positives should be restored to the unschedulablePods list.
	restore bool
	// falsePositiveFirstSeen maps pod keys to the timestamp when they were first observed dropped by the processor while placed on a ready node in the cluster snapshot.
	falsePositiveFirstSeen map[podutil.PodKey]time.Time
	// lastSeen maps keys of tracked pods to the timestamp when they were last seen in unschedulablePods.
	lastSeen map[podutil.PodKey]time.Time
}

// NewSafeguardedPodListProcessor creates a new SafeguardedPodListProcessor.
func NewSafeguardedPodListProcessor(processor pods.PodListProcessor, reason metrics.FilteredFalsePositiveReason, restore bool) *SafeguardedPodListProcessor {
	if processor == nil {
		return nil
	}
	return &SafeguardedPodListProcessor{
		processor:              processor,
		reason:                 reason,
		restore:                restore,
		falsePositiveFirstSeen: make(map[podutil.PodKey]time.Time),
		lastSeen:               make(map[podutil.PodKey]time.Time),
	}
}

// Process passes pods to the underlying processor and restores dropped pods if needed.
func (p *SafeguardedPodListProcessor) Process(ctx *context.AutoscalingContext, beforePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	now := time.Now()

	p.refreshLastSeenForTrackedPods(beforePods, now)
	p.cleanupExpiredPods(now)

	afterPods, err := p.processor.Process(ctx, beforePods)
	if err != nil {
		return nil, err
	}

	droppedPods := podutil.GetMissingPods(beforePods, afterPods)
	if len(droppedPods) == 0 {
		return afterPods, nil
	}

	scheduledOnReadyNode, err := getPodsScheduledOnReadyNodes(ctx.ClusterSnapshot)
	if err != nil {
		klog.Warningf("Failed to list NodeInfos from ClusterSnapshot: %v", err)
	}

	falsePositiveEventsCount := 0
	for _, pod := range droppedPods {
		if isFakeOrSyntheticPod(pod) {
			continue
		}
		if !scheduledOnReadyNode[pod.UID] {
			continue
		}

		key := podutil.PodKey{UID: pod.UID, Name: pod.Name}
		firstSeenTime := p.trackPod(key, now)

		if now.Sub(firstSeenTime) <= filteredFalsePositiveThreshold {
			continue
		}

		falsePositiveEventsCount++

		if p.restore {
			// The processor assumed this pod was schedulable and dropped it, but it has been pending for a long time.
			// This is a false positive. We restore the original unmutated pod to force CA to provision new capacity.
			afterPods = append(afterPods, pod)
		}
	}

	if falsePositiveEventsCount > 0 {
		metrics.IncreaseFilteredFalsePositiveSchedulablePodsEvents(p.reason, falsePositiveEventsCount)
	}
	return afterPods, nil
}

func getPodsScheduledOnReadyNodes(snapshot clustersnapshot.ClusterSnapshot) (map[types.UID]bool, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("cluster snapshot is nil")
	}
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		return nil, err
	}
	scheduled := make(map[types.UID]bool)
	for _, nodeInfo := range nodeInfos {
		if nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		node := nodeInfo.Node()
		if !kube_util.IsNodeReadyAndSchedulable(node) {
			continue
		}
		if _, isUpcoming := node.Annotations[annotations.NodeUpcomingAnnotation]; isUpcoming {
			continue
		}
		for _, podInfo := range nodeInfo.GetPods() {
			scheduled[podInfo.GetPod().UID] = true
		}
	}
	return scheduled, nil
}

// CleanUp cleans up the underlying processor.
func (p *SafeguardedPodListProcessor) CleanUp() {
	if p.processor != nil {
		p.processor.CleanUp()
	}
}

// Preprocess pre-processes pods if the underlying processor supports it.
func (p *SafeguardedPodListProcessor) Preprocess(unschedulablePods []*apiv1.Pod) {
	if preprocessor, ok := p.processor.(Preprocessor); ok {
		preprocessor.Preprocess(unschedulablePods)
	}
}

// trackPod registers a pod that was dropped by the wrapped processor on a ready node.
// It initializes falsePositiveFirstSeen if the pod is newly dropped, updates lastSeen to now, and returns the firstSeen timestamp.
func (p *SafeguardedPodListProcessor) trackPod(key podutil.PodKey, now time.Time) time.Time {
	p.lastSeen[key] = now
	if firstSeen, ok := p.falsePositiveFirstSeen[key]; ok {
		return firstSeen
	}
	p.falsePositiveFirstSeen[key] = now
	return now
}

// refreshLastSeenForTrackedPods updates the lastSeen timestamp for pods we are already tracking.
// This ensures that as long as a tracked pod is present in unschedulablePods, it isn't cleaned up.
func (p *SafeguardedPodListProcessor) refreshLastSeenForTrackedPods(beforePods []*apiv1.Pod, now time.Time) {
	for _, pod := range beforePods {
		key := podutil.PodKey{UID: pod.UID, Name: pod.Name}
		if _, ok := p.lastSeen[key]; ok {
			p.lastSeen[key] = now
		}
	}
}

// cleanupExpiredPods purges entries for pods that have been absent from unschedulablePods for longer than cleanupThreshold.
// This automatically cleans up tracking state for pods that were scheduled or deleted.
func (p *SafeguardedPodListProcessor) cleanupExpiredPods(now time.Time) {
	for key, lastSeenTime := range p.lastSeen {
		if now.Sub(lastSeenTime) > cleanupThreshold {
			delete(p.falsePositiveFirstSeen, key)
			delete(p.lastSeen, key)
		}
	}
}

func isFakeOrSyntheticPod(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	if fake.IsFake(pod) {
		return true
	}
	if cb.IsFakeCapacityBuffersPod(pod) {
		return true
	}
	if _, isInjectedPR := pr_pods.InjectedPodProvReqRef(pod); isInjectedPR {
		return true
	}
	if pod.Annotations != nil && pod.Annotations[cc_processors.MinCapacityFakePodAnnotation] == "true" {
		return true
	}
	return false
}
