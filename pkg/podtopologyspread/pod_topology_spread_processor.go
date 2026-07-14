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

package podtopologyspread

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	metricLabel                 = "scaleUp:processPodTopologySpread"
	scheduleAnywayProcessWindow = 5 * time.Minute
)

type labelKeyValue struct {
	key   string
	value string
}

// Processor is a PodListProcessor that enforces pod topology spread constraints for ComputeClass.
type Processor struct {
	simulator         *scheduling.HintingSimulator
	domainDiscoveries []PTSDomainDiscovery
	clock             clock.PassiveClock
	backoffs          map[types.UID]time.Time
}

// NewPodTopologySpreadProcessor return an instance of PodTopologySpreadProcessor
func NewPodTopologySpreadProcessor(domainDiscoveries []PTSDomainDiscovery) *Processor {
	return &Processor{
		simulator:         scheduling.NewHintingSimulator(),
		domainDiscoveries: domainDiscoveries,
		clock:             clock.RealClock{},
		backoffs:          make(map[types.UID]time.Time),
	}
}

type PTSDomainDiscovery interface {
	// EligiblePTSPods returns pods eligible for PodTopologySpreadProcessor.
	// This function returns the corresponding topologySpreadConstraints and available domains as well when it is eligible, and returns nil otherwise.
	EligiblePTSPods(pods []*apiv1.Pod) []PTSConfig
}

type PTSConfig struct {
	pod         *apiv1.Pod
	domainNames []string
	constraint  *apiv1.TopologySpreadConstraint

	domainDiscoveryName string
}

// Preprocess computes backed off controllers based on the unfiltered list of unschedulable pods.
// This is done so that we can correctly track original unhelpable pods that might be removed by other processors.
func (p *Processor) Preprocess(unschedulablePods []*apiv1.Pod) {
	p.updateBackoffs(unschedulablePods)
}

func (p *Processor) Process(ctx *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	defer metrics.UpdateDurationFromStart(metricLabel, time.Now())

	var configs []PTSConfig
	for _, dd := range p.domainDiscoveries {
		configs = append(configs, dd.EligiblePTSPods(unschedulablePods)...)
	}

	// Apply common filters
	configs = p.filterPTSConfigs(configs)

	// We want this check early to avoid the heavy computations when there is no need for it.
	if len(configs) == 0 {
		return unschedulablePods, nil
	}

	// For eligible pods, we need to copy them as we mutate them as surprisingly they persist across CA loops which causes problems.
	replacedPods := map[types.UID]*apiv1.Pod{}
	for i := range configs {
		currentPod := configs[i].pod
		newPod, found := replacedPods[currentPod.UID]
		if !found {
			newPod = currentPod.DeepCopy()
			replacedPods[currentPod.UID] = newPod
		}

		configs[i].pod = newPod
	}
	unschedulablePods = replacePods(unschedulablePods, replacedPods)

	// We use stable sort to the preserve order of domains discoveries (important to be deterministic for the hinting simulator + prioritize the config from earlier domain discovery in case of conflict).
	slices.SortStableFunc(configs, func(a, b PTSConfig) int {
		if a.pod.Namespace != b.pod.Namespace {
			return cmp.Compare(a.pod.Namespace, b.pod.Namespace)
		}
		return cmp.Compare(a.pod.Name, b.pod.Name)
	})

	allPods, podsToNodes, err := allScheduledPods(ctx)
	if err != nil {
		return nil, err
	}
	allPods = append(allPods, unschedulablePods...)

	// Indexing pods by label keys to enable optimizations.
	allPodsByLabel := indexPodsByLabel(allPods)

	loggingQuota := logging.PTSPodAssignmentLoggingQuota()
	matchedPodsCache := make(map[string][]*apiv1.Pod)
	for _, config := range configs {
		pod := config.pod
		pts := config.constraint
		targetDomainKey := pts.TopologyKey
		domains := config.domainNames

		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = make(map[string]string)
		}

		if v := pod.Spec.NodeSelector[targetDomainKey]; v != "" {
			klog.Warningf("Pod %q already has node selector with key %q and value %q. Skipping config from domain discovery processor %q.", podId(pod), targetDomainKey, v, config.domainDiscoveryName)
			continue
		}

		matchingPods := getMatchingPodsWithCache(allPodsByLabel, matchedPodsCache, allPods, pts, pod)
		// Scheduler scopes PTS per namespace, for each pod we need to calculate only pods from the same namespace.
		// TODO(b/513145089): We should take this into consideration when building the cache, not only filter at the end.
		matchingPods = filterPodsByNamespace(matchingPods, pod.Namespace)
		matchedPodsCountPerDomain := make(map[string]int, len(domains))
		for _, matchedPod := range matchingPods {
			node := podsToNodes[matchedPod]
			domain := ""
			if node == nil { // Unscheduled pod.
				domain = matchedPod.Spec.NodeSelector[targetDomainKey]
			} else { // Scheduled pod.
				domain = node.Labels[targetDomainKey]
			}
			if domain == "" {
				continue
			}
			matchedPodsCountPerDomain[domain]++
		}

		minDomain := domains[0]
		for i := 1; i < len(domains); i++ {
			if matchedPodsCountPerDomain[domains[i]] < matchedPodsCountPerDomain[minDomain] {
				minDomain = domains[i]
			}
		}

		// We can make modifications to the pod as we copied pods in ptsPods array earlier in this function.)
		for constraintIdx, constraint := range pod.Spec.TopologySpreadConstraints {
			if constraint.TopologyKey == targetDomainKey {
				pod.Spec.TopologySpreadConstraints[constraintIdx].WhenUnsatisfiable = apiv1.ScheduleAnyway
				break
			}
		}
		pod.Spec.NodeSelector[targetDomainKey] = minDomain
		// Setting specific annotation so the nodeSelector without toleration doesn't get rejected by workload separation check in pod requirements.
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[gkelabels.PTSDomainKeyAnnotation] = targetDomainKey
		klogx.V(4).UpTo(loggingQuota).Infof("PodTopologySpreadProcessor: Pod %q got assigned to domain %q for domain key %q initiated by domain discovery processor %q", podId(pod), minDomain, targetDomainKey, config.domainDiscoveryName)
	}
	klogx.V(4).Over(loggingQuota).Infof("PodTopologySpreadProcessor also assigned domains for %d other pods", -loggingQuota.Left())

	var ptsPods []*apiv1.Pod
	for _, pod := range replacedPods {
		ptsPods = append(ptsPods, pod)
	}

	// We need to schedule pods after replacing PTS with nodeSelector.
	// Otherwise, pods that can be scheduled after PTS processor but not scheduled in the existing FilterOutSchedulable processor will result in an unwanted scale up.
	// We also need this code to happen after FilterOutSchedulable bec in case we have maxSkew=2, we will scale-up a node while Scheduler will pick existing domain if there is space there, making the scale-up useless and unwanted.
	statuses, _, err := p.simulator.TrySchedulePods(ctx.ClusterSnapshot, ptsPods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: func(nodeInfo *framework.NodeInfo) bool {
			return nodeNotBeingRemoved(nodeInfo.Node())
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to schedule pods in PodTopologySpreadProcessor: %v", err)
	}
	unschedulablePods = removeScheduledPods(unschedulablePods, statuses)

	return unschedulablePods, nil
}

func (p *Processor) updateBackoffs(unschedulablePods []*apiv1.Pod) {
	hasUnhelpable := make(map[types.UID]bool)
	oldest := make(map[types.UID]time.Time)

	for _, pod := range unschedulablePods {
		targetUID := pod.UID
		if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
			targetUID = controllerRef.UID
		}

		if annotator.IsUnhelpablePod(pod) {
			hasUnhelpable[targetUID] = true
		}

		if !pod.CreationTimestamp.IsZero() {
			if t, found := oldest[targetUID]; !found || pod.CreationTimestamp.Time.Before(t) {
				oldest[targetUID] = pod.CreationTimestamp.Time
			}
		}
	}

	// Handle expiration and updates for existing backoffs.
	// The backoff applies to all pods for a given parent controller and expires once all the pods
	// for that parent controller that were created before this backoff was introduced are gone or scheduled.
	// We don't want to extend the backoff every time we see unhelpable pods, we re-evaluate when all older pods are handled.
	for controllerUID, cutoff := range p.backoffs {
		t, found := oldest[controllerUID]
		if found && t.Before(cutoff) {
			continue // We keep the backoff as is.
		}

		if hasUnhelpable[controllerUID] {
			p.backoffs[controllerUID] = p.clock.Now()
		} else {
			delete(p.backoffs, controllerUID)
		}
	}

	// Add new backoffs for controllers that are not already backed off.
	for controllerUID := range hasUnhelpable {
		if _, found := p.backoffs[controllerUID]; !found {
			p.backoffs[controllerUID] = p.clock.Now()
		}
	}

	if len(p.backoffs) > 0 {
		var backedOffUIDs []string
		for controllerUID := range p.backoffs {
			backedOffUIDs = append(backedOffUIDs, string(controllerUID))
		}
		slices.Sort(backedOffUIDs)

		limit := 20
		if len(backedOffUIDs) > limit {
			klog.V(4).Infof("PTS backoff active for controllers: %v and %d others", backedOffUIDs[:limit], len(backedOffUIDs)-limit)
		} else {
			klog.V(4).Infof("PTS backoff active for controllers: %v", backedOffUIDs)
		}
	}
}

// filterPTSConfigs applies common filtering to the PTSConfigs returned by Domain Discoveries
func (p *Processor) filterPTSConfigs(configs []PTSConfig) []PTSConfig {
	var result []PTSConfig
	for _, c := range configs {
		if p.isInBackoff(c.pod) {
			continue
		}
		if c.constraint.WhenUnsatisfiable == apiv1.ScheduleAnyway && p.clock.Since(c.pod.CreationTimestamp.Time) > scheduleAnywayProcessWindow {
			continue
		}
		if v := c.pod.Spec.NodeSelector[c.constraint.TopologyKey]; v != "" {
			continue
		}
		result = append(result, c)
	}
	return result
}

func podId(pod *apiv1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

func (p *Processor) isInBackoff(pod *apiv1.Pod) bool {
	targetUID := pod.UID
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		targetUID = controllerRef.UID
	}
	_, inBackoff := p.backoffs[targetUID]
	return inBackoff
}

func removeScheduledPods(unscheduledPods []*apiv1.Pod, statuses []scheduling.Status) []*apiv1.Pod {
	scheduledPods := make(map[*apiv1.Pod]bool)
	for _, status := range statuses {
		scheduledPods[status.Pod] = true
	}

	var finalPods []*apiv1.Pod
	for _, pod := range unscheduledPods {
		if scheduledPods[pod] {
			continue
		}
		finalPods = append(finalPods, pod)
	}
	return finalPods
}

func replacePods(pods []*apiv1.Pod, replacedPods map[types.UID]*apiv1.Pod) []*apiv1.Pod {
	for idx, pod := range pods {
		if newPod, found := replacedPods[pod.UID]; found {
			pods[idx] = newPod
		}
	}
	return pods
}

func getCacheKey(sel *metav1.LabelSelector) string {
	return sel.String() // Deterministic implementation.
}

func getMatchingPodsWithCache(allPodsByLabel map[labelKeyValue][]*apiv1.Pod, matchedPodsCache map[string][]*apiv1.Pod, allPods []*apiv1.Pod, pts *apiv1.TopologySpreadConstraint, pod *apiv1.Pod) []*apiv1.Pod {
	if pts.LabelSelector == nil {
		return allPods
	}

	labelSelector := getNormalizedLabelSelector(pts, pod)

	cacheKey := getCacheKey(labelSelector)
	if matched, found := matchedPodsCache[cacheKey]; found {
		return matched
	}

	matchedPods := getMatchingPods(allPodsByLabel, allPods, labelSelector)
	matchedPodsCache[cacheKey] = matchedPods
	return matchedPods
}

func getMatchingPods(allPodsByLabel map[labelKeyValue][]*apiv1.Pod, allPods []*apiv1.Pod, labelSelector *metav1.LabelSelector) []*apiv1.Pod {
	matched := getMatchingPodsForRequiredLabels(allPodsByLabel, allPods, labelSelector.MatchLabels)
	if labelSelector == nil || len(labelSelector.MatchExpressions) == 0 {
		return matched
	}

	expressionSelector := labelSelector.DeepCopy()
	expressionSelector.MatchLabels = nil // We already matched it earlier. Removing it to avoid duplicate work.
	selector, err := metav1.LabelSelectorAsSelector(expressionSelector)
	if err != nil {
		klog.Errorf("failed to create label selector from pod topology spread constraint: %v", err)
		return nil
	}

	var matchingPods []*apiv1.Pod
	for _, pod := range matched {
		if selector.Matches(labels.Set(pod.Labels)) {
			matchingPods = append(matchingPods, pod)
		}
	}

	return matchingPods
}

// getNormalizedLabelSelector returns a label selector from topology spread constraint, that is pod agnostic.
// To achieve this, all MatchLabelKeys have to be replaced based on the label values present in the pod object.
func getNormalizedLabelSelector(pts *apiv1.TopologySpreadConstraint, pod *apiv1.Pod) *metav1.LabelSelector {

	if len(pts.MatchLabelKeys) == 0 && len(pts.LabelSelector.MatchExpressions) == 0 {
		return pts.LabelSelector
	}

	labelSelector := pts.LabelSelector.DeepCopy()

	if labelSelector.MatchLabels == nil {
		labelSelector.MatchLabels = make(map[string]string)
	}

	for _, k := range pts.MatchLabelKeys {
		v, found := pod.Labels[k]
		if !found {
			continue
		}
		labelSelector.MatchLabels[k] = v
	}

	// We optimize the most used case in MatchExpressions by moving them to MatchLabels as we did many optimizations for the MatchLabels case.
	var newMatchExpressions []metav1.LabelSelectorRequirement
	for _, expr := range labelSelector.MatchExpressions {
		if expr.Operator != metav1.LabelSelectorOpIn || len(expr.Values) != 1 {
			newMatchExpressions = append(newMatchExpressions, expr)
			continue
		}
		labelSelector.MatchLabels[expr.Key] = expr.Values[0]
	}
	labelSelector.MatchExpressions = newMatchExpressions

	return labelSelector
}

// getMatchingPodsForRequiredLabels returns a list of pods that match the given required pod labels.
func getMatchingPodsForRequiredLabels(allPodsByLabel map[labelKeyValue][]*apiv1.Pod, allPods []*apiv1.Pod, requiredPodLabels map[string]string) []*apiv1.Pod {
	if len(requiredPodLabels) == 0 {
		return allPods
	}

	rulesNum := len(requiredPodLabels)

	matchedRulesCountPerPod := make(map[*apiv1.Pod]int)
	for k, v := range requiredPodLabels {
		label := labelKeyValue{key: k, value: v}
		for _, pod := range allPodsByLabel[label] {
			matchedRulesCountPerPod[pod]++
		}
	}

	var matchingPods []*apiv1.Pod
	for pod, matchedRulesCount := range matchedRulesCountPerPod {
		if matchedRulesCount == rulesNum {
			matchingPods = append(matchingPods, pod)
		}
	}
	return matchingPods
}

func filterPodsByNamespace(pods []*apiv1.Pod, namespace string) []*apiv1.Pod {
	var result []*apiv1.Pod
	for _, p := range pods {
		if p.Namespace == namespace {
			result = append(result, p)
		}
	}
	return result
}

func allScheduledPods(ctx *context.AutoscalingContext) ([]*apiv1.Pod, map[*apiv1.Pod]*apiv1.Node, error) {
	nodeInfos, err := ctx.ClusterSnapshot.NodeInfos().List()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get node infos: %v", err)
	}
	pods := []*apiv1.Pod{}
	podsToNodes := make(map[*apiv1.Pod]*apiv1.Node)
	for _, nodeInfo := range nodeInfos {
		if !nodeNotBeingRemoved(nodeInfo.Node()) {
			continue
		}
		for _, podInfo := range nodeInfo.GetPods() {
			pods = append(pods, podInfo.GetPod())
			podsToNodes[podInfo.GetPod()] = nodeInfo.Node()
		}
	}
	return pods, podsToNodes, nil
}

func indexPodsByLabel(pods []*apiv1.Pod) map[labelKeyValue][]*apiv1.Pod {
	podsByLabel := make(map[labelKeyValue][]*apiv1.Pod)
	for _, pod := range pods {
		for k, v := range pod.Labels {
			label := labelKeyValue{key: k, value: v}
			podsByLabel[label] = append(podsByLabel[label], pod)
		}
	}
	return podsByLabel
}

func nodeNotBeingRemoved(node *apiv1.Node) bool {
	if node == nil {
		return false
	}
	if taints.HasTaint(node, taints.ToBeDeletedTaint) || taints.HasTaint(node, defrag.HardTaint) {
		return false
	}
	return true
}

// CleanUp is called at CA termination
func (p *Processor) CleanUp() {
}
