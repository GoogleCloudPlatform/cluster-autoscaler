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

package metrics_processors

import (
	"cmp"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	crutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/preemption"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	provreq_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/klog/v2"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"
)

const (
	longUnschedulableThreshold        = 60 * time.Minute
	unhelpableGracePeriod             = 10 * time.Minute
	gpuMetricLabel                    = "gpu"
	tpuMetricLabel                    = "tpu"
	outOfResourceMetricLabel          = "out_of_resource"
	placementMetricLabel              = "placement_type"
	consumingProvisioningRequestLabel = "consuming_provisioning_request"
	computeClassLabel                 = "cloud.google.com/compute-class"
	deviceAllocationModeMetricLabel   = "device_allocation_mode"
)

var supportedSchedulers = map[string]bool{
	// We treat empty scheduler as the default.
	"":                                      true,
	"default-scheduler":                     true,
	"gke.io/default-scheduler":              true,
	"gke.io/optimize-utilization-scheduler": true,
}

type metricLabels map[string]string

type unschedulablePodInfo struct {
	// unschedulableSince is the timestamp when the pod still became unschedulable.
	// If it was before autoscaler started we set it to CA start time instead
	// (since CA could not help it if it was not running).
	unschedulableSince time.Time
	// unhelpableUntil is the last time CA couldn't help the pod (for example
	// due to hitting stockout or GCP quota). We only count SLI for the time
	// where autoscaler could have helped the pod.
	unhelpableUntil time.Time
}

// ScaleUpStatusMetricsProcessor gathers gke specific metrics.
type ScaleUpStatusMetricsProcessor struct {
	autoscalerStartTime time.Time
	podInfos            map[types.UID]unschedulablePodInfo
	labelCounter        labelCounter
	aggregator          *PodStatusAggregator
	observer            podSchedulableObserver
	metricsFilter       filter.MetricsFilter
	npcCrdLister        npc_lister.Lister
}

// NewScaleUpStatusMetricsProcessor returns a new ScaleUpStatusMetricsProcessor.
func NewScaleUpStatusMetricsProcessor(aggregator *PodStatusAggregator, metricsFilter filter.MetricsFilter, npcCrdLister npc_lister.Lister) *ScaleUpStatusMetricsProcessor {
	return &ScaleUpStatusMetricsProcessor{
		autoscalerStartTime: time.Now(),
		podInfos:            make(map[types.UID]unschedulablePodInfo),
		labelCounter:        labelCounter{},
		aggregator:          aggregator,
		observer:            &metricsPodSchedulableObserver{},
		metricsFilter:       metricsFilter,
		npcCrdLister:        npcCrdLister,
	}
}

func (p *ScaleUpStatusMetricsProcessor) podUnschedulableSince(pod *apiv1.Pod) time.Time {
	// We use the latest time among pod creation time, autoscaler start time,
	// or the time the pod was marked as unschedulable (if applicable).
	// This ensures that pods pending before the autoscaler started,
	// Only impacts the SLO from the time the autoscaler started.
	// And pods that are not processed by the scheduler yet impact the SLO from the time they got created.
	startTimeCandidates := []time.Time{pod.CreationTimestamp.Time, p.autoscalerStartTime}

	_, condition := podv1.GetPodCondition(&pod.Status, apiv1.PodScheduled)
	// This should always be true for a normal pod. It may not be true for
	// some in-memory pod like CapacityRequest pod or similar, which we should
	// never consider here anyway. Still better safe than sorry with nil ptrs.
	if condition != nil && condition.Status == apiv1.ConditionFalse && condition.Reason == apiv1.PodReasonUnschedulable {
		startTimeCandidates = append(startTimeCandidates, condition.LastTransitionTime.Time)
	}
	return slices.MaxFunc(startTimeCandidates, time.Time.Compare)
}

func (p *ScaleUpStatusMetricsProcessor) observePendingPod(pod *apiv1.Pod) {
	if crutils.IsPodCapacityRequest(pod) {
		klog.Warningf("Trying to observe CR pod when counting SLI: %s", pod.Name)
		return
	}
	if _, found := p.podInfos[pod.UID]; !found {
		p.podInfos[pod.UID] = unschedulablePodInfo{
			unschedulableSince: p.podUnschedulableSince(pod),
		}
	}
}

func (p *ScaleUpStatusMetricsProcessor) processScheduledPod(pod *apiv1.Pod, info unschedulablePodInfo, labels map[string]string, now time.Time) podScheduling {
	scheduleTime := now
	_, condition := podv1.GetPodCondition(&pod.Status, apiv1.PodScheduled)
	if condition != nil && condition.Status == apiv1.ConditionTrue {
		scheduleTime = condition.LastTransitionTime.Time
	}
	schedulingDuration := scheduleTime.Sub(info.unschedulableSince)
	if schedulingDuration < 0 {
		schedulingDuration = 0
	}
	unschedulableDuration := getUnschedulableDuration(info, scheduleTime)

	stockout := labels[outOfResourceMetricLabel]
	allocationMode := labels[deviceAllocationModeMetricLabel]
	_, cccName, err := p.npcCrdLister.PodCrd(pod)
	if err != nil {
		klog.Warningf("Failed to get CCC for pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
	p.observer.observePodSchedulingDuration(schedulingDuration, stockout, cccName, allocationMode)
	p.observer.observePodUnschedulableDuration(unschedulableDuration, labels)

	return podScheduling{namespace: pod.Namespace, name: pod.Name, schedulingDuration: schedulingDuration, unschedulableDuration: unschedulableDuration}
}

func (p *ScaleUpStatusMetricsProcessor) forgetPod(podId types.UID) {
	p.metricsFilter.ForgetPod(podId)
	delete(p.podInfos, podId)
}

// Process analyses the scale up status and emits appropriate visibility events.
func (p *ScaleUpStatusMetricsProcessor) Process(context *context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	now := time.Now()
	p.processImpl(context, scaleUpStatus, now)
}

// This version allows passing timestamp for easier testing.
func (p *ScaleUpStatusMetricsProcessor) processImpl(context *context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus, now time.Time) {
	seen := make(map[types.UID]bool)

	// Inform Metrics Filter of the scale up
	var scaleUpNodeGroupIds []string
	for _, scaleUp := range scaleUpStatus.ScaleUpInfos {
		scaleUpNodeGroupIds = append(scaleUpNodeGroupIds, scaleUp.Group.Id())
	}
	p.metricsFilter.ObserveScaleUp(scaleUpStatus.PodsTriggeredScaleUp, scaleUpNodeGroupIds, now)

	// STEP1 - Filter out pods that can't be helped for objective reasons - they wouldn't
	// fit any NodeGroup or NodeGroups they would fit are already max size / backed off.
	// There is nothing CA can do, so we don't want to count them towards SLI.
	for _, noScaleUp := range scaleUpStatus.PodsRemainUnschedulable {
		pod := noScaleUp.Pod
		p.observePendingPod(pod)
		info := p.podInfos[pod.UID]
		info.unhelpableUntil = now
		p.podInfos[pod.UID] = info
		seen[pod.UID] = true
	}

	// STEP2 - Add any pod not reported in scale-up status
	// This will generally include pods removed by FilterOutSchedulable, but
	// also cover scenarios where scale-up failed and we got nothing from status
	// We also filter out pods that had faced
	//	1. Quota problems in the past
	//	2. Had an hard preference for preemption VMs
	// and pods with custom schedulers.
	// We add specific metricLabels to pods which:
	//  1. Need GPU
	//	2. Encountered a stockout.
	unschedulablePods := make(map[types.UID]bool)
	unscheduledPodsLabels := p.filterPodsAndAddLabels(p.aggregator.Unschedulable)
	for _, pod := range p.aggregator.Unschedulable {
		if crutils.IsPodCapacityRequest(pod) {
			continue
		}
		unschedulablePods[pod.UID] = true
		// unschedulable pod that hasn't been handled yet
		if _, found := seen[pod.UID]; !found {
			p.observePendingPod(pod)
		}
	}

	// STEP3 - Remove any pod that was successfully scheduled and update metrics

	// It's safe to grab a fresh list of scheduled pods. In particulare nothing bad
	// will happen if we process a pod that we observed in this loop. We will look at
	// exactly the same stored state whether we process it now or next loop. And doing
	// it faster reduces the risk of pod being deleted before we process it.
	// We also filter out pods that had faced
	// 1. Quota problems in the past
	// 2. Had an hard preference for preemption VMs
	// We add specific metricLabels to pods which:
	// 1. Need GPU
	// 2. Encountered a stockout.
	pods, err := context.AllPodLister().List()
	if err != nil {
		klog.Warningf("Failed to list scheduled pods when calculating SLI metrics: %v", err)
		return
	}
	// We only process pods present in p.podInfos further, filter others out so that
	// we don't run expensive processing for the entire cluster each loop
	pods = slices.DeleteFunc(
		pods,
		func(pod *apiv1.Pod) bool {
			_, found := p.podInfos[pod.UID]
			return !found
		},
	)
	scheduledPods := kube_util.ScheduledPods(pods)
	filteredOutPods := p.filterPodsAndAddLabels(scheduledPods)
	var podSchedulings []podScheduling
	for _, pod := range scheduledPods {
		if info, found := p.podInfos[pod.UID]; found {
			// Only record metrics if we found the appropriate labels (for eg. pods aren't filtered out)
			if labels, found := filteredOutPods[pod.UID]; found {
				scheduling := p.processScheduledPod(pod, info, labels, now)
				podSchedulings = append(podSchedulings, scheduling)
			}
			p.forgetPod(pod.UID)
		}
	}
	logPodSchedulings(podSchedulings)

	// STEP4 - Mark pods as unhelpable until now if they belong to parts of the cluster that were scaled down to zero
	for podId, podInfo := range p.podInfos {
		if p.metricsFilter.IsPodScaledToZero(podId) {
			podInfo.unhelpableUntil = now
			p.podInfos[podId] = podInfo
		}
	}

	p.labelCounter.reset()

	// STEP5 - Process pods in our data structure. Remove any pods that no longer exist.
	for podId, podInfo := range p.podInfos {
		if _, found := unschedulablePods[podId]; found {
			if podInfo.unschedulableSince.Add(longUnschedulableThreshold).Before(now) {
				// We only add pod to histogram once it is no longer pending,
				// a pod pending forever will never show up in histogram.
				// So we count pods that has been pending for a really long time to
				// avoid missing the completely.

				// Except if we recently found a pod was 'unhelpable' (ie. there
				// was no action available to CA that would help it) we don't.
				// The reason is that there are many cases where the pod can't be
				// helped due to things like hitting GCE quota or stockout, but
				// we will periodically try scale-up for this pod. Every time
				// we do it would be reported as longUnschedulable creating noise
				// in metrics.
				// If the scale-up fails due to one of the reasons described above
				// it's not related to CA and we don't count it. If it succeeds
				// the pod should schedule soon and it will be added to histogram,
				// so no need for a workaround for forever-pending pods.
				// We increment only if the pod isn't filtered out as having some scale
				// up issues(quotas, needing preemption VMs)
				if podInfo.unhelpableUntil.IsZero() || podInfo.unhelpableUntil.Add(unhelpableGracePeriod).Before(now) {
					// Add metrics only if the pod didn't face issues(quota, needs preemption VM)
					if labels, found := unscheduledPodsLabels[podId]; found {
						p.labelCounter.increment(labels)
					}
				}
			}
		} else {
			// Pod is no longer returned by unschedulable pod lister.
			// It may have been scheduled and we missed it, it may
			// have been deleted. Either way it's not pending anymore,
			// let's update metrics.
			// We process only if the pod isn't filtered out as having some
			// issues(quotas, needing preemption VMs)
			//
			// Only record metrics if we found the appropriate labels (for eg. pods aren't filtered out)
			if labels, found := unscheduledPodsLabels[podId]; found {
				p.observer.observePodUnschedulableDuration(getUnschedulableDuration(podInfo, now), labels)
			}
			p.forgetPod(podId)
		}
	}

	p.labelCounter.process(p.observer.setLongUnschedulablePodCount)
	p.metricsFilter.CleanCache(p.aggregator.Unschedulable, now)
}

// filterPodsAndAddLabels filters out pods that have quota issues and have a
// hard preference for preemption VMs. For the pods that remain, it adds gpu and
// out_of_resource labels if they need a GPU or need have encountered a stockout
// It returns a map of UIDs of pods(which have no issues) and labels
func (p *ScaleUpStatusMetricsProcessor) filterPodsAndAddLabels(pods []*apiv1.Pod) map[types.UID]metricLabels {
	// 1a. filter out pods that are owned by Daemon Sets.
	var nonDSPods []*apiv1.Pod
	for _, pod := range pods {
		if !isOwnedByDaemonSet(pod) {
			nonDSPods = append(nonDSPods, pod)
		}
	}
	var podsWithoutVmPreemption []*apiv1.Pod

	// 1b. filter out pods that need preemption VMs
	for _, pod := range nonDSPods {
		if !preemption.PodRequiresPreemption(pod) {
			podsWithoutVmPreemption = append(podsWithoutVmPreemption, pod)
		}
	}

	// 1c. filter out pods using custom scheduler
	var podsWithSupportedScheduler []*apiv1.Pod
	for _, pod := range podsWithoutVmPreemption {
		if usesSupportedScheduler(pod) {
			podsWithSupportedScheduler = append(podsWithSupportedScheduler, pod)
		}
	}

	// 1d. filter out pods going through/went through quota issues.
	finalFilteredOutPods := p.metricsFilter.FilterOutPods(podsWithSupportedScheduler)

	podLabels := make(map[types.UID]metricLabels, len(finalFilteredOutPods))
	// 2a. add request labels onto remaining pods
	for _, pod := range finalFilteredOutPods {
		labels := make(metricLabels, len([]string{gpuMetricLabel, tpuMetricLabel, placementMetricLabel, consumingProvisioningRequestLabel}))
		if needsGPU(pod) {
			labels[gpuMetricLabel] = "true"
		} else {
			labels[gpuMetricLabel] = "false"
		}
		if needsTPU(pod) {
			labels[tpuMetricLabel] = "true"
		} else {
			labels[tpuMetricLabel] = "false"
		}
		req := podrequirements.GetRequirements(pod)
		labels[placementMetricLabel] = placement.FromRequirements(req.LabelReq).Type()
		consumingProvisioningRequest, _ := provreq_pods.ProvisioningRequestName(pod)
		labels[consumingProvisioningRequestLabel] = consumingProvisioningRequest
		labels[deviceAllocationModeMetricLabel] = podutils.DeviceAllocationModeForPod(pod).String()
		podLabels[pod.UID] = labels
	}

	// 2b. add out_of_resource labels to pods
	stockoutCheck := p.metricsFilter.GetsPodsEncounteringStockOut(finalFilteredOutPods)
	for _, pod := range finalFilteredOutPods {
		var labels metricLabels
		var found bool
		// This condition should never happen
		if labels, found = podLabels[pod.UID]; !found {
			labels = map[string]string{}
		}
		if stockoutCheck[pod.UID] {
			labels[outOfResourceMetricLabel] = "true"
		} else {
			labels[outOfResourceMetricLabel] = "false"
		}
		podLabels[pod.UID] = labels
	}

	return podLabels
}

// CleanUp cleans up
func (p *ScaleUpStatusMetricsProcessor) CleanUp() {}

func getUnschedulableDuration(info unschedulablePodInfo, now time.Time) time.Duration {
	var duration time.Duration
	if !info.unhelpableUntil.IsZero() {
		duration = now.Sub(info.unhelpableUntil)
	} else {
		duration = now.Sub(info.unschedulableSince)
	}
	if duration < 0 {
		return 0
	}
	return duration
}

// Used for logging.
type podScheduling struct {
	namespace             string
	name                  string
	schedulingDuration    time.Duration
	unschedulableDuration time.Duration
}

func logPodSchedulings(schedulings []podScheduling) {
	if len(schedulings) == 0 {
		return
	}

	klog.Infof("Observed pods scheduled: %d", len(schedulings))

	longest := slices.MaxFunc(schedulings, func(a podScheduling, b podScheduling) int {
		return cmp.Compare(a.schedulingDuration, b.schedulingDuration)
	})
	if longest.schedulingDuration == longest.unschedulableDuration {
		klog.Infof("Longest pod scheduling took %s in pod %s/%s", longest.schedulingDuration, longest.namespace, longest.name)
	} else {
		klog.Infof("Longest pod scheduling took in total %s (unschedulable for %s) in pod %s/%s", longest.schedulingDuration, longest.unschedulableDuration, longest.namespace, longest.name)
	}
}

func needsGPU(pod *apiv1.Pod) bool {
	podRequests := podutils.PodRequests(pod)
	_, found := podRequests[gpu.ResourceNvidiaGPU]
	return found
}

func needsTPU(pod *apiv1.Pod) bool {
	podRequests := podutils.PodRequests(pod)
	_, found := podRequests[tpu.ResourceGoogleTPU]
	return found
}

func usesSupportedScheduler(pod *apiv1.Pod) bool {
	return supportedSchedulers[pod.Spec.SchedulerName]
}

func isOwnedByDaemonSet(pod *apiv1.Pod) bool {
	for _, ownerRef := range pod.OwnerReferences {
		if ownerRef.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}
