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

package impostor

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	kubeutil "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	listersv1 "k8s.io/client-go/listers/apps/v1"
	v1batchlister "k8s.io/client-go/listers/batch/v1"
	v1lister "k8s.io/client-go/listers/core/v1"
)

type listerRegistry struct {
	nodeLister                  *nodeLister
	podLister                   *podLister
	statefulSetLister           *statefulSetLister
	pdbLister                   *pdbLister
	replicaSetLister            *replicaSetLister
	daemonSetLister             *daemonSetLister
	replicationControllerLister v1lister.ReplicationControllerLister
	jobLister                   v1batchlister.JobLister
}

func newListerRegistry(scheduledPods *sync.Map, pdbs []*policyv1.PodDisruptionBudget, clusterSnapshot clustersnapshot.ClusterSnapshot, clusterSnapshotMutex *sync.Mutex) *listerRegistry {
	rcLister, _ := kubeutil.NewTestReplicationControllerLister([]*apiv1.ReplicationController{})
	jobLister, _ := kubeutil.NewTestJobLister([]*batchv1.Job{})
	pLister := &podLister{scheduledPods}
	return &listerRegistry{
		nodeLister:                  &nodeLister{clusterSnapshot: clusterSnapshot, clusterSnapshotMutex: clusterSnapshotMutex},
		podLister:                   pLister,
		statefulSetLister:           newStatefulSetLister(),
		pdbLister:                   &pdbLister{pdbs: pdbs, podLister: pLister},
		daemonSetLister:             newDaemonSetLister(),
		replicaSetLister:            newReplicaSetLister(),
		jobLister:                   jobLister,
		replicationControllerLister: rcLister,
	}
}

func (mlr *listerRegistry) newKubernetesRegistry() kubernetes.ListerRegistry {
	return kubernetes.NewListerRegistry(
		mlr.nodeLister,
		mlr.nodeLister,
		mlr.podLister,
		mlr.pdbLister,
		mlr.daemonSetLister,
		mlr.replicationControllerLister,
		mlr.jobLister,
		mlr.replicaSetLister,
		mlr.statefulSetLister,
	)
}

type nodeLister struct {
	clusterSnapshot      clustersnapshot.ClusterSnapshot
	clusterSnapshotMutex *sync.Mutex
}

// List mockNodes - follows NodeLister interface
func (nl *nodeLister) List() ([]*apiv1.Node, error) {
	nl.clusterSnapshotMutex.Lock()
	defer nl.clusterSnapshotMutex.Unlock()

	var result []*apiv1.Node
	nodes, err := nl.clusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %v", err)
	}
	for _, node := range nodes {
		result = append(result, node.Node())
	}
	return result, nil
}

// Get node by name
func (nl *nodeLister) Get(name string) (*apiv1.Node, error) {
	nl.clusterSnapshotMutex.Lock()
	defer nl.clusterSnapshotMutex.Unlock()

	nodeInfo, err := nl.clusterSnapshot.GetNodeInfo(name)
	if err != nil {
		return nil, err
	}
	return nodeInfo.Node(), nil
}

type podLister struct {
	pods *sync.Map
}

// List pods - follows PodLister interface
func (pl *podLister) List() ([]*apiv1.Pod, error) {
	var result []*apiv1.Pod
	pl.pods.Range(func(key, value any) bool {
		result = append(result, value.(*apiv1.Pod))
		return true
	})
	return result, nil
}

type replicaSetLister struct {
	rSets map[string]*replicaSetNamespaceLister
	listersv1.ReplicaSetListerExpansion
}

// newReplicaSetLister returns a new instance of replicaSetLister
func newReplicaSetLister() *replicaSetLister {
	return &replicaSetLister{rSets: make(map[string]*replicaSetNamespaceLister)}
}

// Add adds a new ReplicaSet.
func (rl *replicaSetLister) Add(rSet *appsv1.ReplicaSet) {
	if rl.rSets[rSet.Namespace] == nil {
		rl.rSets[rSet.Namespace] = &replicaSetNamespaceLister{
			rSets: make(map[string]*appsv1.ReplicaSet),
		}
	}
	rl.rSets[rSet.Namespace].rSets[rSet.Name] = rSet
}

// List lists all existing replica sets.
func (rl *replicaSetLister) List(_ labels.Selector) ([]*appsv1.ReplicaSet, error) {
	var result []*appsv1.ReplicaSet
	for _, sSet := range rl.rSets {
		el, _ := sSet.List(nil)
		result = append(result, el...)
	}
	return result, nil
}

// ReplicaSets returns ReplicaSetNamespaceLister for provided namespace.
func (rl *replicaSetLister) ReplicaSets(namespace string) listersv1.ReplicaSetNamespaceLister {
	if rl.rSets[namespace] == nil {
		rl.rSets[namespace] = &replicaSetNamespaceLister{
			rSets: make(map[string]*appsv1.ReplicaSet),
		}
	}
	return rl.rSets[namespace]
}

// replicaSetNamespaceLister implements ReplicaSetNamespaceLister interface.
type replicaSetNamespaceLister struct {
	rSets map[string]*appsv1.ReplicaSet
}

// List lists existing replica sets in a given namespace.
func (rNL *replicaSetNamespaceLister) List(_ labels.Selector) ([]*appsv1.ReplicaSet, error) {
	var result []*appsv1.ReplicaSet
	for _, rSet := range rNL.rSets {
		result = append(result, rSet)
	}
	return result, nil
}

// Get returns ReplicaSet corresponding to provided name.
func (rNL *replicaSetNamespaceLister) Get(name string) (*appsv1.ReplicaSet, error) {
	return rNL.rSets[name], nil
}

// statefulSetLister is a test implementation of StatefulSetLister.
type statefulSetLister struct {
	sSets map[string]*statefulSetNamespaceLister
	listersv1.StatefulSetListerExpansion
}

// newStatefulSetLister returns a new instance of statefulSetLister
func newStatefulSetLister() *statefulSetLister {
	return &statefulSetLister{sSets: make(map[string]*statefulSetNamespaceLister)}
}

// Add adds a new StatefulSet
func (sl *statefulSetLister) Add(sSet *appsv1.StatefulSet) {
	if sl.sSets[sSet.Namespace] == nil {
		sl.sSets[sSet.Namespace] = &statefulSetNamespaceLister{
			sSets: make(map[string]*appsv1.StatefulSet),
		}
	}
	sl.sSets[sSet.Namespace].sSets[sSet.Name] = sSet
}

// List lists all existing stateful sets.
func (sl *statefulSetLister) List(_ labels.Selector) ([]*appsv1.StatefulSet, error) {
	var result []*appsv1.StatefulSet
	for _, sSet := range sl.sSets {
		el, _ := sSet.List(nil)
		result = append(result, el...)
	}
	return result, nil
}

// StatefulSets returns StatefulSetNamespaceLister for provided namespace.
func (sl *statefulSetLister) StatefulSets(namespace string) listersv1.StatefulSetNamespaceLister {
	if sl.sSets[namespace] == nil {
		sl.sSets[namespace] = &statefulSetNamespaceLister{
			sSets: make(map[string]*appsv1.StatefulSet),
		}
	}
	return sl.sSets[namespace]
}

// statefulSetNamespaceLister implements StatefulSetNamespaceLister interface.
type statefulSetNamespaceLister struct {
	sSets map[string]*appsv1.StatefulSet
}

// List lists existing stateful sets in a given namespace.
func (sNL *statefulSetNamespaceLister) List(_ labels.Selector) (ret []*appsv1.StatefulSet, err error) {
	var result []*appsv1.StatefulSet
	for _, sSet := range sNL.sSets {
		result = append(result, sSet)
	}
	return result, nil
}

// Get returns StatefulSet corresponding to provided name.
func (sNL *statefulSetNamespaceLister) Get(name string) (*appsv1.StatefulSet, error) {
	return sNL.sSets[name], nil
}

// daemonSetLister is a test implementation of StatefulSetLister.
type daemonSetLister struct {
	dSets map[string]*daemonSetNamespaceLister
	listersv1.DaemonSetListerExpansion
}

// newDaemonSetLister returns a new instance of statefulSetLister
func newDaemonSetLister() *daemonSetLister {
	return &daemonSetLister{dSets: make(map[string]*daemonSetNamespaceLister)}
}

// Add adds a new DaemonSet.
func (dl *daemonSetLister) Add(dSet *appsv1.DaemonSet) {
	if dl.dSets[dSet.Namespace] == nil {
		dl.dSets[dSet.Namespace] = &daemonSetNamespaceLister{
			dSets: make(map[string]*appsv1.DaemonSet),
		}
	}
	dl.dSets[dSet.Namespace].dSets[dSet.Name] = dSet
}

// List lists all existing daemon sets.
func (dl *daemonSetLister) List(_ labels.Selector) ([]*appsv1.DaemonSet, error) {
	var result []*appsv1.DaemonSet
	for _, dSet := range dl.dSets {
		el, _ := dSet.List(nil)
		result = append(result, el...)
	}
	return result, nil
}

// DaemonSets returns DaemonSetNamespaceLister for provided namespace.
func (dl *daemonSetLister) DaemonSets(namespace string) listersv1.DaemonSetNamespaceLister {
	if dl.dSets[namespace] == nil {
		dl.dSets[namespace] = &daemonSetNamespaceLister{
			dSets: make(map[string]*appsv1.DaemonSet),
		}
	}
	return dl.dSets[namespace]
}

type daemonSetNamespaceLister struct {
	dSets map[string]*appsv1.DaemonSet
}

func (dNL *daemonSetNamespaceLister) Get(name string) (*appsv1.DaemonSet, error) {
	if _, ok := dNL.dSets[name]; !ok {
		return nil, fmt.Errorf("daemonSet %v does not exist", name)
	}
	return dNL.dSets[name], nil
}

func (dNL *daemonSetNamespaceLister) List(_ labels.Selector) ([]*appsv1.DaemonSet, error) {
	var result []*appsv1.DaemonSet
	for _, ds := range dNL.dSets {
		result = append(result, ds)
	}
	return result, nil
}

type pdbLister struct {
	sync.Mutex
	pdbs      []*policyv1.PodDisruptionBudget
	podLister *podLister
}

// Add adds new pdb.
func (pdl *pdbLister) Add(pdb *policyv1.PodDisruptionBudget) error {
	pdl.pdbs = append(pdl.pdbs, pdb)
	pods, err := pdl.podLister.List()
	if err != nil {
		return err
	}
	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return err
	}
	for _, p := range pods {
		if p.Namespace == pdb.Namespace && selector.Matches(labels.Set(p.Labels)) {
			pdb.Status.ExpectedPods++
			pdb.Status.CurrentHealthy++
		}
	}
	if minStr := pdb.Spec.MinAvailable.StrVal; minStr != "" {
		if _, err = getNumberFromString(minStr); err != nil {
			return err
		}
	}
	if maxStr := pdb.Spec.MinAvailable.StrVal; maxStr != "" {
		if _, err = getNumberFromString(maxStr); err != nil {
			return err
		}
	}
	updateAllowedDisruptions(pdb)
	return nil
}

func (pdl *pdbLister) PodEvicted(pod *apiv1.Pod) {
	if pod == nil {
		return
	}
	for _, pdb := range pdl.pdbs {
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			return
		}
		if pod.Namespace == pdb.Namespace && selector.Matches(labels.Set(pod.Labels)) {
			pdl.Lock()
			pdb.Status.CurrentHealthy--
			updateAllowedDisruptions(pdb)
			pdl.Unlock()
		}
	}
}

func (pdl *pdbLister) PodScheduled(pod *apiv1.Pod) {
	for _, pdb := range pdl.pdbs {
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			return
		}
		if pod.Namespace == pdb.Namespace && selector.Matches(labels.Set(pod.Labels)) {
			pdl.Lock()
			pdb.Status.CurrentHealthy++
			updateAllowedDisruptions(pdb)
			pdl.Unlock()
		}
	}
}

// List lists existing pdbs.
func (pdl *pdbLister) List() ([]*policyv1.PodDisruptionBudget, error) {
	return pdl.pdbs, nil
}

func updateAllowedDisruptions(pdb *policyv1.PodDisruptionBudget) {
	minAvail := 0
	switch pdb.Spec.MinAvailable.Type {
	case intstr.String:
		minAvail = getAvailabilityFromPercentage(mustGetNumberFromString(pdb.Spec.MinAvailable.StrVal), pdb.Status.ExpectedPods)
	case intstr.Int:
		minAvail = pdb.Spec.MinAvailable.IntValue()
	}
	if pdb.Status.CurrentHealthy <= int32(minAvail) {
		pdb.Status.DisruptionsAllowed = 0
	} else {
		pdb.Status.DisruptionsAllowed = pdb.Status.CurrentHealthy - int32(minAvail)
	}

	maxUnavail := 0
	switch pdb.Spec.MaxUnavailable.Type {
	case intstr.String:
		maxUnavail = getAvailabilityFromPercentage(mustGetNumberFromString(pdb.Spec.MaxUnavailable.StrVal), pdb.Status.ExpectedPods)
	case intstr.Int:
		maxUnavail = pdb.Spec.MaxUnavailable.IntValue()
	}
	currentUnavail := pdb.Status.ExpectedPods - pdb.Status.CurrentHealthy
	if currentUnavail >= int32(maxUnavail) {
		pdb.Status.DisruptionsAllowed = 0
	} else {
		pdb.Status.DisruptionsAllowed = int32(math.Min(float64(pdb.Status.DisruptionsAllowed), float64(maxUnavail)-float64(currentUnavail)))
	}
}

func getAvailabilityFromPercentage(val int, expected int32) int {
	return int(float64(val) / 100.0 * float64(expected))
}

func mustGetNumberFromString(strVal string) int {
	val, err := getNumberFromString(strVal)
	if err != nil {
		panic(err)
	}
	return val
}

func getNumberFromString(strVal string) (int, error) {
	maxAvailPerc := strings.TrimSuffix(strVal, "%")
	return strconv.Atoi(maxAvailPerc)
}
