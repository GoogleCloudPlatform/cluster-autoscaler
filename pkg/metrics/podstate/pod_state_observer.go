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

package podstate

import (
	"context"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctx "k8s.io/autoscaler/cluster-autoscaler/context"
	coreutils "k8s.io/autoscaler/cluster-autoscaler/core/utils"
	cb "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/fake"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	osspodutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/ccc"
	podstatetypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate/types"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	"k8s.io/klog/v2"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/utils/clock"
)

const (
	defaultPeriodicCheckFrequency = 30 * time.Second
	loggingThreshold              = 1 * time.Minute
	logLimit                      = 100
	logWindow                     = 1 * time.Minute
)

var (
	RegisterPendingPodsCollectorFunc       = RegisterPendingPodsCollector
	RegisterPendingPodsPerCccCollectorFunc = ccc.RegisterPendingPodsPerCccCollector
)

type reactionTimeMetrics interface {
	ObserveFirstReactionTime(duration time.Duration, systemPod bool, hasPVC bool, hasCSI bool, reactionType metrics.ReactionType, deviceAllocationMode string)
}

type podStateData struct {
	systemPod          bool
	hasPVC             bool
	hasCSI             bool
	reactionType       metrics.ReactionType
	unschedulableSince *time.Time
	cccName            string
	allocationMode     podutils.DeviceAllocationMode
}

type PodStateObserver struct {
	autoscalerStartTime          time.Time
	informerFactory              informers.SharedInformerFactory
	reactionTimeMetrics          reactionTimeMetrics
	systemPodsClassifier         systempods.Classifier
	clock                        clock.WithTicker
	periodicCheckFrequency       time.Duration
	logLimiter                   *logging.LogRateLimiter
	npcCrdLister                 npc_lister.Lister
	expendablePodsPriorityCutoff int

	mux                 sync.Mutex
	unreportedPodStates map[types.UID]*podStateData
	reportedPodStates   map[types.UID]*podStateData
}

// NewPodStateObserver returns a new PodStateObserver.
func NewPodStateObserver(informerFactory informers.SharedInformerFactory, reactionTimeMetrics reactionTimeMetrics, podsClassifier systempods.Classifier, npc_crd_lister npc_lister.Lister, enablePendingPodsMetric, enablePendingPodsPerCccMetric bool, expendablePodsPriorityCutoff int) (*PodStateObserver, error) {
	return newPodStateObserver(informerFactory, reactionTimeMetrics, podsClassifier, npc_crd_lister, defaultPeriodicCheckFrequency, clock.RealClock{}, enablePendingPodsMetric, enablePendingPodsPerCccMetric, expendablePodsPriorityCutoff)
}

func newPodStateObserver(informerFactory informers.SharedInformerFactory, reactionTimeMetrics reactionTimeMetrics, podsClassifier systempods.Classifier, npc_crd_lister npc_lister.Lister, periodicCheckFrequency time.Duration, clock clock.WithTicker, enablePendingPodsMetric, enablePendingPodsPerCccMetric bool, expendablePodsPriorityCutoff int) (*PodStateObserver, error) {
	o := &PodStateObserver{
		autoscalerStartTime:    clock.Now(),
		informerFactory:        informerFactory,
		reactionTimeMetrics:    reactionTimeMetrics,
		systemPodsClassifier:   podsClassifier,
		clock:                  clock,
		periodicCheckFrequency: periodicCheckFrequency,

		npcCrdLister:                 npc_crd_lister,
		expendablePodsPriorityCutoff: expendablePodsPriorityCutoff,
		unreportedPodStates:          make(map[types.UID]*podStateData),
		reportedPodStates:            make(map[types.UID]*podStateData),
	}

	podInformer := o.informerFactory.Core().V1().Pods()
	if _, err := podInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    o.onAdd,
			UpdateFunc: o.onUpdate,
			DeleteFunc: o.onDelete,
		},
	); err != nil {
		klog.Errorf("Initialization of PodStateObserver failed with: %v", err)
		return nil, err
	}
	if enablePendingPodsMetric {
		RegisterPendingPodsCollectorFunc(o.calculatePendingPods)
	}
	if enablePendingPodsPerCccMetric {
		RegisterPendingPodsPerCccCollectorFunc(o.calculatePendingPodsPerCcc)
	}
	o.logLimiter = logging.NewLogRateLimiter(logLimit, logWindow)

	return o, nil
}

// Run starts the periodic checks and waits until the context is canceled.
func (o *PodStateObserver) Run(ctx context.Context) {
	ticker := o.clock.NewTicker(o.periodicCheckFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C():
			o.reportTimeoutPods()
			o.logLimiter.ReportDrops()
		case <-ctx.Done():
			klog.Infof("PodStateObserver received stop signal. Status: %v. Stopping.", ctx.Err())
			return
		}
	}
}

// Process analyses the scale up status and emits appropriate visibility events.
func (o *PodStateObserver) Process(context *ctx.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	o.ObserveReaction(scaleUpStatus.PodsTriggeredScaleUp, metrics.ScaleUp)

	pods := make([]*v1.Pod, len(scaleUpStatus.PodsRemainUnschedulable))
	for i, ns := range scaleUpStatus.PodsRemainUnschedulable {
		pods[i] = ns.Pod
	}
	o.ObserveReaction(pods, metrics.Unhelpable)
}

func (o *PodStateObserver) ObserveReaction(pods []*v1.Pod, reactionType metrics.ReactionType) {
	loggingQuota := logging.OverdueReactionsLoggingQuota()
	now := o.clock.Now()
	filteredPods := make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		if !fake.IsFake(pod) && !cb.IsFakeCapacityBuffersPod(pod) && !coreutils.IsExpendablePod(pod, o.expendablePodsPriorityCutoff) {
			filteredPods = append(filteredPods, pod)
		}
	}
	reactionDataToReportReactionTime := o.updateAndGetReactionDataToReport(filteredPods, reactionType)
	for pod, sd := range reactionDataToReportReactionTime {
		t := getTimePassedFromBeingUnschedulable(now, sd)
		o.reactionTimeMetrics.ObserveFirstReactionTime(t, sd.systemPod, sd.hasPVC, sd.hasCSI, reactionType, sd.allocationMode.String())
		if t > loggingThreshold {
			klogx.V(1).UpTo(loggingQuota).Infof("PodStateObserver: overdue reaction for pod %v (UID=%v) - %v after %v", pod.Name, pod.UID, reactionType, t)
		}
	}
	klogx.V(1).Over(loggingQuota).Infof("PodStateObserver: There are also %d other pods experiencing overdue reaction %v", -loggingQuota.Left(), reactionType)
}

// CleanUp cleans up
func (o *PodStateObserver) CleanUp() {
	o.reportTimeoutPods()
}

// isPodAlreadyTrackedNoLock checks if the pod is present in either the unreported or reported map.
func (o *PodStateObserver) isPodAlreadyTrackedNoLock(pod *v1.Pod) bool {
	return o.getPodStateDataNoLock(pod) != nil
}

// getPodStateDataNoLock returns the podStateData from either reported or unreported state map.
func (o *PodStateObserver) getPodStateDataNoLock(pod *v1.Pod) *podStateData {
	if podStateData, ok := o.unreportedPodStates[pod.UID]; ok {
		return podStateData
	}
	if podStateData, ok := o.reportedPodStates[pod.UID]; ok {
		return podStateData
	}
	return nil
}

func (o *PodStateObserver) updatePodUnschedulableTime(pod *v1.Pod) {
	transitionTime := o.getPodUnschedulableTime(pod)
	if transitionTime == nil {
		return
	}
	o.mux.Lock()
	defer o.mux.Unlock()
	// set unschedulableSince time only if it was not set before
	if unscheduledPodStateData := o.getPodStateDataNoLock(pod); unscheduledPodStateData != nil && unscheduledPodStateData.unschedulableSince == nil {
		unscheduledPodStateData.unschedulableSince = transitionTime
	}
}

func (o *PodStateObserver) onAdd(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Warningf("Informer has incorrect object type: %v", obj)
		return
	}
	if pod.Spec.NodeName != "" {
		return
	}
	if osspodutils.IsDaemonSetPod(pod) {
		return
	}
	if coreutils.IsExpendablePod(pod, o.expendablePodsPriorityCutoff) {
		return
	}

	o.mux.Lock()
	defer o.mux.Unlock()

	if o.isPodAlreadyTrackedNoLock(pod) {
		klog.Warningf("PodStateObserver.OnAdd: %v (UID=%v) unscheduled already existing", pod.Name, pod.UID)
		return
	}
	o.logLimiter.Logf(5, "PodStateObserver.OnAdd: %v (UID=%v) unscheduled", pod.Name, pod.UID)

	// if pod is marked as unscheduled we want to store the last transition timestamp
	o.unreportedPodStates[pod.UID] = &podStateData{
		systemPod:          o.systemPodsClassifier.IsSystemPod(pod),
		hasPVC:             HasPVC(pod),
		hasCSI:             HasCSI(pod),
		reactionType:       metrics.NoReaction,
		unschedulableSince: o.getPodUnschedulableTime(pod),
		cccName:            o.getPodCccName(pod),
		allocationMode:     podutils.DeviceAllocationModeForPod(pod),
	}
}

func (o *PodStateObserver) getPodCccName(pod *v1.Pod) string {
	_, name, err := o.npcCrdLister.PodCrd(pod)
	if err != nil {
		klog.Warningf("Failed to get CCC for pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
	return name
}

func (o *PodStateObserver) onUpdate(oldObj, newObj interface{}) {
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		klog.Warningf("Informer has incorrect object type: %v", newObj)
		return
	}
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		klog.Warningf("Informer has incorrect object type: %v", newObj)
		return
	}
	// if pod is marked as unscheduled we want to store the last transition timestamp
	o.updatePodUnschedulableTime(newPod)
	if oldPod.Spec.NodeName == "" && newPod.Spec.NodeName != "" {
		// Pods are only scheduled once in their lifetime, so we can safely delete it now.
		o.observeReactionAndForgetPod(newPod, metrics.Scheduled)
	}
}

func (o *PodStateObserver) onDelete(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Warningf("Informer has incorrect object type: %v", obj)
		return
	}
	if pod.Spec.NodeName != "" {
		return
	}
	if coreutils.IsExpendablePod(pod, o.expendablePodsPriorityCutoff) {
		return
	}
	o.observeReactionAndForgetPod(pod, metrics.Deleted)
}

func (o *PodStateObserver) observeReactionAndForgetPod(pod *v1.Pod, reactionType metrics.ReactionType) {
	info := o.getReactionDataAndRemove(pod)
	if info == nil {
		// No data - skip.
		return
	}
	reactionTime := getTimePassedFromBeingUnschedulable(o.clock.Now(), *info)
	var logMessage string
	var logLevel klog.Level
	o.reactionTimeMetrics.ObserveFirstReactionTime(reactionTime, info.systemPod, info.hasPVC, info.hasCSI, reactionType, info.allocationMode.String())
	if reactionTime > loggingThreshold {
		logMessage = "PodStateObserver: overdue reaction for pod %v (UID=%v) - %v after %v"
		logLevel = klog.Level(1)
	} else {
		logMessage = "PodStateObserver: Pod %v (UID=%v) %v after %v"
		logLevel = klog.Level(5)
	}

	o.logLimiter.Logf(logLevel, logMessage, pod.Name, pod.UID, reactionType, reactionTime)
}

func (o *PodStateObserver) getReactionDataAndRemove(pod *v1.Pod) *podStateData {
	o.mux.Lock()
	defer o.mux.Unlock()

	if info, found := o.unreportedPodStates[pod.UID]; found {
		delete(o.unreportedPodStates, pod.UID)
		return info
	}

	delete(o.reportedPodStates, pod.UID)
	return nil
}

func (o *PodStateObserver) updateAndGetReactionDataToReport(pods []*v1.Pod, reactionType metrics.ReactionType) map[*v1.Pod]podStateData {
	o.mux.Lock()
	defer o.mux.Unlock()

	res := make(map[*v1.Pod]podStateData, len(pods))
	for _, pod := range pods {
		if info, found := o.unreportedPodStates[pod.UID]; found {
			delete(o.unreportedPodStates, pod.UID)
			o.reportedPodStates[pod.UID] = info

			info.reactionType = reactionType
			res[pod] = *info
		} else if info, found := o.reportedPodStates[pod.UID]; found {
			info.reactionType = reactionType
		} else {
			listerPod, err := o.informerFactory.Core().V1().Pods().Lister().Pods(pod.Namespace).Get(pod.Name)
			if err == nil && listerPod != nil && listerPod.UID == pod.UID && listerPod.Spec.NodeName == "" {
				// An active, unscheduled pod that still exists in the cluster but is missing
				// from the tracking maps represents an anomaly (e.g. due to informer lag,
				// missed OnAdd events, or startup sync timing issues).
				// We log this warning to catch cases where the observer loses track of active
				// unscheduled pods, which helps identify metric tracking gaps or race conditions.
				klog.Warningf("PodStateObserver.updateAndGetReactionDataToReport: pod %v (UID=%v) still exists in cluster but is missing from tracking in reaction %v",
					pod.Name, pod.UID, reactionType)
			}
		}
	}
	return res
}

func (o *PodStateObserver) processTimedOutPods() map[types.UID]podStateData {
	o.mux.Lock()
	defer o.mux.Unlock()

	timeoutPods := make(map[types.UID]podStateData)

	for uid, podStateData := range o.unreportedPodStates {
		// we want to avoid reporting timeouts for pods that are not marked as unschedulable
		if podStateData.unschedulableSince == nil {
			continue
		}
		if o.clock.Since(*podStateData.unschedulableSince) >= metrics.MaxReactionTime {
			podStateData.reactionType = metrics.Timeout
			timeoutPods[uid] = *podStateData
			delete(o.unreportedPodStates, uid)
			o.reportedPodStates[uid] = podStateData
		}
	}
	return timeoutPods
}

func (o *PodStateObserver) reportTimeoutPods() {
	loggingQuota := logging.OverdueReactionsLoggingQuota()
	timeoutPods := o.processTimedOutPods()
	for uid, info := range timeoutPods {
		o.reactionTimeMetrics.ObserveFirstReactionTime(metrics.MaxReactionTime, info.systemPod, info.hasPVC, info.hasCSI, metrics.Timeout, info.allocationMode.String())
		klogx.V(1).UpTo(loggingQuota).Infof("PodStateObserver: timeout for pod (systemPod=%v, hasPVC=%v, hasCSI=%v) with UID=%v", info.systemPod, info.hasPVC, info.hasCSI, uid)
	}
	klogx.V(1).Over(loggingQuota).Infof("PodStateObserver: There are also %d other pods experiencing timeout for reaction", -loggingQuota.Left())
}

// Creating podStates snapshot is expected to minimize the time that the lock is held on the mutex.
// The temporal additional memory consumption is an accepted tradeoff (this memory will quickly be garbage collected)
func (o *PodStateObserver) getPodStatesSnapshot() []podStateData {
	o.mux.Lock()
	defer o.mux.Unlock()
	totalPods := len(o.unreportedPodStates) + len(o.reportedPodStates)
	podData := make([]podStateData, 0, totalPods)

	for _, data := range o.unreportedPodStates {
		podData = append(podData, *data)
	}
	for _, data := range o.reportedPodStates {
		podData = append(podData, *data)
	}

	return podData
}

func (o *PodStateObserver) calculatePendingPods() []podstatetypes.PendingPodsMetric {
	podData := o.getPodStatesSnapshot()
	return calculatePendingPodsMetricFromPodData(podData)
}

func (o *PodStateObserver) calculatePendingPodsPerCcc() []podstatetypes.PendingPodsPerCccMetric {
	podData := o.getPodStatesSnapshot()
	podsPerCcc := make(map[string][]podStateData)

	for _, pod := range podData {
		podsPerCcc[pod.cccName] = append(podsPerCcc[pod.cccName], pod)
	}
	cccNames := o.getCCCNames()
	for cccName := range cccNames {
		if _, ok := podsPerCcc[cccName]; !ok {
			podsPerCcc[cccName] = nil
		}
	}

	var result []podstatetypes.PendingPodsPerCccMetric
	for cccName, podData := range podsPerCcc {
		podsMetrics := calculatePendingPodsMetricFromPodData(podData)
		for _, metric := range podsMetrics {
			result = append(result, podstatetypes.PendingPodsPerCccMetric{PendingPodsMetric: metric, CccName: cccName})
		}
	}

	return result
}

func (o *PodStateObserver) getCCCNames() map[string]bool {
	cccNames := make(map[string]bool)
	crds, err := o.npcCrdLister.ListCrds()
	if err != nil {
		klog.Warningf("PodStateObserver.getCCCNames: failed to list CRDs: %v", err)
	} else {
		for _, crd := range crds {
			cccNames[crd.Name()] = true
		}
	}

	if defaultCrd, _, found := o.npcCrdLister.Default(); found {
		cccNames[defaultCrd] = true
	}
	return cccNames
}

func (o *PodStateObserver) getPodUnschedulableTime(pod *v1.Pod) *time.Time {
	// if pod is marked as unscheduled we want to store the last transition timestamp
	_, condition := podv1.GetPodCondition(&pod.Status, v1.PodScheduled)
	if condition != nil && condition.Status == v1.ConditionFalse && condition.Reason == v1.PodReasonUnschedulable {
		res := condition.LastTransitionTime.Time
		if o.autoscalerStartTime.After(res) {
			res = o.autoscalerStartTime
		}
		return &res
	}
	return nil
}

type pendingPodCounts struct {
	provisioningInProgress int
	unableToProvision      int
	unprocessed            int
	noActionTaken          int
}

func calculatePendingPodsMetricFromPodData(podData []podStateData) []podstatetypes.PendingPodsMetric {
	systemPodCounts := pendingPodCounts{}
	nonSystemPodCounts := pendingPodCounts{}
	for _, data := range podData {
		// Skip pods that are no longer considered pending for this metric.
		if data.reactionType == metrics.Scheduled || data.reactionType == metrics.Deleted {
			continue
		}
		counts := selectCounts(data.systemPod, &systemPodCounts, &nonSystemPodCounts)
		switch data.reactionType {
		case metrics.ScaleUp, metrics.EkUpsize:
			counts.provisioningInProgress++
		case metrics.Unhelpable:
			counts.unableToProvision++
		case metrics.NoActionNeeded:
			counts.noActionTaken++
		case metrics.NoReaction, metrics.Timeout:
			counts.unprocessed++
		}
	}
	var result []podstatetypes.PendingPodsMetric
	for _, systemPod := range []bool{true, false} {
		counts := selectCounts(systemPod, &systemPodCounts, &nonSystemPodCounts)
		result = append(result, podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: systemPod, Count: counts.provisioningInProgress})
		result = append(result, podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: systemPod, Count: counts.unableToProvision})
		result = append(result, podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: systemPod, Count: counts.unprocessed})
		result = append(result, podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: systemPod, Count: counts.noActionTaken})
	}
	return result
}

func selectCounts(systemPod bool, systemCounts, nonSystemCounts *pendingPodCounts) *pendingPodCounts {
	if systemPod {
		return systemCounts
	}
	return nonSystemCounts
}

func HasPVC(pod *v1.Pod) bool {
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			return true
		}
	}
	return false
}

func HasCSI(pod *v1.Pod) bool {
	for _, vol := range pod.Spec.Volumes {
		if vol.CSI != nil {
			return true
		}
	}
	return false
}

// getTimePassedFromBeingUnschedulable returns the time that passed since pod was marked as unschedulable
// otherwise if pod was not marked so, 0 is returned
func getTimePassedFromBeingUnschedulable(currentTime time.Time, podStateData podStateData) time.Duration {
	var timePassed time.Duration
	// if unschedulableSince is equal to the default value, it means that pod is not marked as unschedulable
	if podStateData.unschedulableSince != nil {
		timePassed = currentTime.Sub(*podStateData.unschedulableSince)
	}
	return timePassed
}
