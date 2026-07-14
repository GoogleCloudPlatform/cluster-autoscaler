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

package filter

import (
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// MetricsFilter notes stockout and filterable issues and maps it to pods to let
// the metrics processor know which pods are going through specific issues.
type MetricsFilter interface {
	// ObserveNodeGroupStockOut observes a nodegroup growing through stockout
	ObserveNodeGroupStockOut(nodeGroupId string)
	// ObserveNodeGroupFilterableIssue observes a nodegroup growing through
	// an issue that can be filtered out like quota/ip exhaustion or service
	// account deletion
	ObserveNodeGroupFilterableIssue(nodeGroupId string)
	// ObserveScaleUp observes a scale up with pods that triggered it
	// and the node groups scaling up
	ObserveScaleUp(pods []*apiv1.Pod, nodeGroups []string, now time.Time)
	// ObserveScaleToZero observes a scale to zero triggered in the cluster for the given pods
	// and the pods on the nodes. Instead of reconstructing copies of nodes array, ignoreNodeFn
	// is used to ignore nodes whoses pods should not be marked with the given scaledToZero value.
	// It is used MT clusters to ignore nodes of other tenants.
	ObserveScaleToZero(pods []*apiv1.Pod, nodes []*framework.NodeInfo, ignoreNodeFn IgnoreNodeFilter, scaledToZero bool)
	// GetsPodsEncounteringStockOut returns the pods that are going through or
	// have gone through stockout (all the node groups in the scale up encountered
	// stockouts)
	GetsPodsEncounteringStockOut(pods []*apiv1.Pod) map[types.UID]bool
	// IsPodScaledToZero returns true if pod was part of a scale to zero event
	IsPodScaledToZero(pod types.UID) bool
	// FilterOutPods filters out pods that are going through filterable issues.
	// Only one of the node groups for the pod's scale up needs to go through
	// filterable issues.
	FilterOutPods(pods []*apiv1.Pod) []*apiv1.Pod
	// ForgetPod indicates to the filter to forget a pod after it
	// has been scheduled.
	ForgetPod(podId types.UID)
	// CleanCache forgets pods that we haven't seen for greater than podTTL,
	// unless the pod is still unschedulable.
	CleanCache(unschedulablePods []*apiv1.Pod, now time.Time)
}

type nodeGroup struct {
	Id              nodeGroupId
	FilterableIssue bool
	Stockout        bool
}

type scaleUp struct {
	Id         string
	NodeGroups []*nodeGroup
	Pods       map[types.UID]bool
}

type nodeGroupId string

type metricsFilterImpl struct {
	sync.Mutex

	podToScaleUp map[types.UID][]*scaleUp
	ngToScaleUp  map[nodeGroupId][]*scaleUp

	podWentThroughFilterableIssue map[types.UID]bool
	podWentThroughStockout        map[types.UID]bool

	podSeenAt     map[types.UID]time.Time
	scaleUpSeenAt map[*scaleUp]time.Time

	scaleDownToZeroPodFilter scaleToZeroPodsFilter
}

const podTTL = 70 * time.Minute
const scaleUpTTL = 2 * time.Hour

func (f *metricsFilterImpl) ObserveNodeGroupStockOut(ngId string) {
	f.Lock()
	defer f.Unlock()

	id := nodeGroupId(ngId)

	var scaleUps []*scaleUp
	var found bool
	if scaleUps, found = f.ngToScaleUp[id]; !found || len(scaleUps) == 0 {
		klog.Warningf("MetricsFilter did not find node group %s in its cache.", id)
		return
	}

	allNodegroupsInStockout := true

	for _, su := range scaleUps {
		for _, group := range su.NodeGroups {
			if group.Id == id {
				group.Stockout = true
			} else if !group.Stockout {
				allNodegroupsInStockout = false
			}
		}

		// If all node groups in scale up have gone through stockout, mark the pods
		// that triggered the scale up as going through stockout issues.
		if allNodegroupsInStockout {
			for pod := range su.Pods {
				f.podWentThroughStockout[pod] = true
			}
		}
	}
}

func (f *metricsFilterImpl) ObserveNodeGroupFilterableIssue(ngId string) {
	f.Lock()
	defer f.Unlock()

	id := nodeGroupId(ngId)

	var scaleUps []*scaleUp
	var found bool
	if scaleUps, found = f.ngToScaleUp[id]; !found || len(scaleUps) == 0 {
		klog.Warningf("MetricsFilter did not find node group %s in its cache.", id)
		return
	}

	for _, su := range scaleUps {
		for _, group := range su.NodeGroups {
			if group.Id == id {
				group.FilterableIssue = true
			}
		}

		// Unlike stockout issues, if one node group in scale up goes through
		// filterable issues, we mark the pods that triggered the scale up as going
		// through filterable issues.
		for pod := range su.Pods {
			f.podWentThroughFilterableIssue[pod] = true
		}
	}
}

func (f *metricsFilterImpl) ObserveScaleUp(podTriggeringScaleUp []*apiv1.Pod, nodeGroups []string, now time.Time) {
	f.Lock()
	defer f.Unlock()

	podMap := make(map[types.UID]bool)
	for _, pod := range podTriggeringScaleUp {
		f.podSeenAt[pod.UID] = now
		podMap[pod.UID] = true
	}

	var groups []*nodeGroup

	for _, ng := range nodeGroups {
		groups = append(groups, &nodeGroup{
			Id: nodeGroupId(ng),
		})
	}

	su := &scaleUp{
		NodeGroups: groups,
		Pods:       podMap,
	}

	f.scaleUpSeenAt[su] = now

	for _, pod := range podTriggeringScaleUp {
		cur := f.podToScaleUp[pod.UID]
		cur = append(cur, su)
		f.podToScaleUp[pod.UID] = cur
	}

	for _, ng := range nodeGroups {
		cur := f.ngToScaleUp[nodeGroupId(ng)]
		cur = append(cur, su)
		f.ngToScaleUp[nodeGroupId(ng)] = cur
	}
}

func (f *metricsFilterImpl) ObserveScaleToZero(pods []*apiv1.Pod, nodes []*framework.NodeInfo, ignoreNodeFn IgnoreNodeFilter, scaledToZero bool) {
	f.Lock()
	defer f.Unlock()

	f.scaleDownToZeroPodFilter.ObserveScaleToZero(pods, nodes, ignoreNodeFn, scaledToZero)
}

func (f *metricsFilterImpl) GetsPodsEncounteringStockOut(pods []*apiv1.Pod) map[types.UID]bool {
	f.Lock()
	defer f.Unlock()

	stockoutPods := map[types.UID]bool{}

	for _, pod := range pods {
		if f.podWentThroughStockout[pod.UID] {
			stockoutPods[pod.UID] = true
		}
	}

	return stockoutPods
}

func (f *metricsFilterImpl) FilterOutPods(pods []*apiv1.Pod) []*apiv1.Pod {
	f.Lock()
	defer f.Unlock()

	var podsWithoutQuotaIssues []*apiv1.Pod

	for _, pod := range pods {
		if f.scaleDownToZeroPodFilter.IsPodScaledToZero(pod.UID) {
			continue
		}
		if !f.podWentThroughFilterableIssue[pod.UID] {
			podsWithoutQuotaIssues = append(podsWithoutQuotaIssues, pod)
		}
	}

	return podsWithoutQuotaIssues
}

func (f *metricsFilterImpl) IsPodScaledToZero(pod types.UID) bool {
	f.Lock()
	defer f.Unlock()

	return f.scaleDownToZeroPodFilter.IsPodScaledToZero(pod)
}

func (f *metricsFilterImpl) ForgetPod(podId types.UID) {
	f.Lock()
	defer f.Unlock()

	f.deletePodWithoutLock(podId)
}

// CleanCache forgets pods that are that have not been accessed for the last
// ~one hour and also forgets scale ups after 2 hours.
func (f *metricsFilterImpl) CleanCache(unschedulablePods []*apiv1.Pod, now time.Time) {
	f.Lock()
	defer f.Unlock()

	for _, pod := range unschedulablePods {
		f.podSeenAt[pod.UID] = now
	}

	for pod, accessedAt := range f.podSeenAt {
		if now.Sub(accessedAt) > podTTL {
			f.deletePodWithoutLock(pod)
		}
	}

	for su, accessedAt := range f.scaleUpSeenAt {
		if now.Sub(accessedAt) > scaleUpTTL {
			f.deleteScaleUpsWithoutLock(su)
		}
	}
}

func (f *metricsFilterImpl) deleteScaleUpsWithoutLock(scaleUpToDelete *scaleUp) {
	for pod := range scaleUpToDelete.Pods {
		var finalScaleUps []*scaleUp
		for _, su := range f.podToScaleUp[pod] {
			if su != scaleUpToDelete {
				finalScaleUps = append(finalScaleUps, su)
			}
		}
		if len(finalScaleUps) > 0 {
			f.podToScaleUp[pod] = finalScaleUps
		} else {
			delete(f.podToScaleUp, pod)
		}
	}

	for _, ng := range scaleUpToDelete.NodeGroups {
		id := ng.Id
		var finalScaleUps []*scaleUp
		for _, su := range f.ngToScaleUp[id] {
			if su != scaleUpToDelete {
				finalScaleUps = append(finalScaleUps, su)
			}
		}
		if len(finalScaleUps) > 0 {
			f.ngToScaleUp[id] = finalScaleUps
		} else {
			delete(f.ngToScaleUp, id)
		}
	}

	delete(f.scaleUpSeenAt, scaleUpToDelete)
}

func (f *metricsFilterImpl) deletePodWithoutLock(uid types.UID) {
	var ngsToTrim []nodeGroupId
	for _, su := range f.podToScaleUp[uid] {
		for _, ng := range su.NodeGroups {
			ngsToTrim = append(ngsToTrim, ng.Id)
		}
	}

	for _, ng := range ngsToTrim {
		var scaleUps []*scaleUp
		var found bool
		if scaleUps, found = f.ngToScaleUp[ng]; !found {
			continue
		}
		var finalScaleUps []*scaleUp
		for _, su := range scaleUps {
			delete(su.Pods, uid)
			if len(su.Pods) != 0 {
				finalScaleUps = append(finalScaleUps, su)
			}
		}
		// Only maintain the map for the pod having some scaleups
		if len(finalScaleUps) > 0 {
			f.ngToScaleUp[ng] = finalScaleUps
		} else {
			delete(f.ngToScaleUp, ng)
		}
	}

	delete(f.podToScaleUp, uid)
	delete(f.podSeenAt, uid)

	// Delete pod from pods going through stockouts and filterable issues
	delete(f.podWentThroughFilterableIssue, uid)
	delete(f.podWentThroughStockout, uid)

	f.scaleDownToZeroPodFilter.ForgetPod(uid)
}

// NewMetricsFilter returns a new metricsFilterImpl, a MetricsFilter
func NewMetricsFilter() *metricsFilterImpl {
	return newMetricsFilterWithScaleDownToZeroPodFilter(newScaleToZeroPodsFilter())
}

func NewMultitenantMetricsFilter(experimentsManager experiments.Manager) *metricsFilterImpl {
	return newMetricsFilterWithScaleDownToZeroPodFilter(newMultitenatScaleToZeroPodsFilter(experimentsManager))
}

func newMetricsFilterWithScaleDownToZeroPodFilter(scaleDownToZeroPodsFilter scaleToZeroPodsFilter) *metricsFilterImpl {
	return &metricsFilterImpl{
		podToScaleUp:                  map[types.UID][]*scaleUp{},
		podSeenAt:                     map[types.UID]time.Time{},
		ngToScaleUp:                   map[nodeGroupId][]*scaleUp{},
		podWentThroughStockout:        map[types.UID]bool{},
		podWentThroughFilterableIssue: map[types.UID]bool{},
		scaleUpSeenAt:                 map[*scaleUp]time.Time{},
		scaleDownToZeroPodFilter:      scaleDownToZeroPodsFilter,
	}
}
