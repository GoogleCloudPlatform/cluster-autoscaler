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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	kube_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/klog/v2"
)

var (
	errNotEvictedYet = errors.New("pod not evicted yet")
	podSuffixLen     = 4
)

type podCallback interface {
	PodEvicted(*corev1.Pod)
	PodScheduled(*corev1.Pod)
}

// coreV1 is a wrapper over CoreV1Interface.
type coreV1 struct {
	v1.CoreV1Interface
	podInterfaces        map[string]*pods
	nodeInterfaces       *mockNodes
	pods                 *sync.Map
	clusterSnapshot      clustersnapshot.ClusterSnapshot
	clusterSnapshotMutex *sync.Mutex
	podEvictionMutex     *sync.Mutex
	podCallbacks         []podCallback
}

// newCoreV1 returns a new instance of coreV1.
func newCoreV1(coreInterface v1.CoreV1Interface, podsMap *sync.Map, clusterSnapshot clustersnapshot.ClusterSnapshot, clusterSnapshotMutex *sync.Mutex, podCallbacks []podCallback) *coreV1 {
	return &coreV1{
		CoreV1Interface:      coreInterface,
		pods:                 podsMap,
		clusterSnapshot:      clusterSnapshot,
		clusterSnapshotMutex: clusterSnapshotMutex,
		podInterfaces:        make(map[string]*pods),
		podEvictionMutex:     &sync.Mutex{},
		podCallbacks:         podCallbacks,
		nodeInterfaces: &mockNodes{
			NodeInterface:        coreInterface.Nodes(),
			clusterSnapshot:      clusterSnapshot,
			clusterSnapshotMutex: clusterSnapshotMutex,
		},
	}
}

func (c *coreV1) addPodCallback(callback podCallback) {
	c.podCallbacks = append(c.podCallbacks, callback)
}

// AddPods adds provided pods.
func (c *coreV1) AddPods(evictionTime time.Duration, newPods []*corev1.Pod) {
	for _, pod := range newPods {
		namespace := pod.Namespace
		if _, nsExists := c.podInterfaces[namespace]; !nsExists {
			c.podInterfaces[namespace] = &pods{
				PodInterface:         c.CoreV1Interface.Pods(namespace),
				clusterSnapshot:      c.clusterSnapshot,
				clusterSnapshotMutex: c.clusterSnapshotMutex,
				podEvictions:         &sync.Map{},
				pods:                 c.pods,
				namespace:            namespace,
				podEvictionMutex:     c.podEvictionMutex,
				podCallbacks:         c.podCallbacks,
			}
		}
		c.podInterfaces[namespace].AddPod(pod, evictionTime, true)
	}
}

// AddNode adds provided node.
func (c *coreV1) AddNode(node *corev1.Node) {
	c.nodeInterfaces.AddNode(node)
}

// Pods returns pod interface for provided namespace.
func (c *coreV1) Pods(namespace string) v1.PodInterface {
	return c.podInterfaces[namespace]
}

// Nodes returns node interface.
func (c *coreV1) Nodes() v1.NodeInterface {
	return c.nodeInterfaces
}

// pods is a wrapper over PodInterface.
type pods struct {
	v1.PodInterface
	namespace            string
	podEvictions         *sync.Map
	pods                 *sync.Map
	clusterSnapshot      clustersnapshot.ClusterSnapshot
	clusterSnapshotMutex *sync.Mutex
	podEvictionMutex     *sync.Mutex
	podCallbacks         []podCallback
}

// Evict removes pod provided in eviction and tries to reschedule it to a different node.
func (p *pods) Evict(_ context.Context, eviction *policyv1beta1.Eviction) error {
	podID := fmt.Sprintf("%s/%s", eviction.Namespace, eviction.Name)
	pE, err := p.startEviction(podID, eviction.Name)
	if err != nil {
		return err
	}
	if pE == nil {
		return nil
	}
	pod, ok := p.pods.Load(podID)
	if !ok || pod == nil {
		return kube_errors.NewNotFound(corev1.Resource("pod"), eviction.Name)
	}
	go func() {
		time.Sleep(pE.evictionTime)
		p.podEvictionMutex.Lock()
		pE.evicted = true
		p.podEvictionMutex.Unlock()

		p.reschedulePod(pod.(*corev1.Pod), pE)
	}()
	return errNotEvictedYet
}

func (p *pods) startEviction(podID, podName string) (*podEvictionStatus, error) {
	p.podEvictionMutex.Lock()
	defer p.podEvictionMutex.Unlock()
	pELoaded, ok := p.podEvictions.Load(podID)
	if !ok || pELoaded == nil {
		return nil, kube_errors.NewNotFound(corev1.Resource("pod"), podName)
	}
	pE := pELoaded.(*podEvictionStatus)
	if pE.evicted {
		return nil, nil
	}
	if pE.evictionStarted {
		return nil, errNotEvictedYet
	}
	pE.evictionStarted = true
	pod, ok := p.pods.Load(podID)
	if !ok {
		klog.Warningf("Couldn't load pod %q", podID)
	}
	for _, c := range p.podCallbacks {
		c.PodEvicted(pod.(*corev1.Pod))
	}
	return pE, nil
}

// Get returns pod given by a provided name.
func (p *pods) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Pod, error) {
	pod, ok := p.pods.Load(fmt.Sprintf("%s/%s", p.namespace, name))
	if !ok {
		return nil, kube_errors.NewNotFound(corev1.Resource("pod"), name)
	}
	return pod.(*corev1.Pod), nil
}

// AddPod adds a new pod.
func (p *pods) AddPod(pod *corev1.Pod, evictionTime time.Duration, addToSnapshot bool) {
	podID := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	p.podEvictions.Store(podID, &podEvictionStatus{evictionTime: evictionTime})
	p.pods.Store(podID, pod)
	if addToSnapshot {
		p.clusterSnapshotMutex.Lock()
		defer p.clusterSnapshotMutex.Unlock()
		if err := p.clusterSnapshot.ForceAddPod(pod, pod.Spec.NodeName); err != nil {
			klog.Errorf("error while adding pod: %v", err)
		}
	}
	for _, c := range p.podCallbacks {
		c.PodScheduled(pod)
	}
}

func (p *pods) reschedulePod(pod *corev1.Pod, pE *podEvictionStatus) {
	p.removeFromOldNode(pod)
	podID := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	p.pods.Delete(podID)
	p.podEvictions.Delete(podID)
	if pod.OwnerReferences[0].Kind == "DaemonSet" {
		return
	}
	p.setUpReplacementPod(pod, pE)
}

// removeFromOldNode removes a pod from it's current node.
func (p *pods) removeFromOldNode(pod *corev1.Pod) {
	p.clusterSnapshotMutex.Lock()
	err := p.clusterSnapshot.ForceRemovePod(pod.Namespace, pod.Name, pod.Spec.NodeName)
	p.clusterSnapshotMutex.Unlock()
	if err != nil {
		klog.Errorf("error while removing pod %s: %v", pod.Name, err)
	}
}

// setUpReplacementPod creates a new pod in place of the one that got evicted
func (p *pods) setUpReplacementPod(pod *corev1.Pod, pE *podEvictionStatus) {
	pod = pod.DeepCopy()
	newPodName := p.generateNewNameForPod(pod.Name)
	if newPodName != "" {
		pod.Name = newPodName
	}
	previousNodeName := pod.Spec.NodeName
	pod.Spec.NodeName = ""
	newNode := p.scheduleOnNewNode(pod, previousNodeName)
	pod.Spec.NodeName = newNode
	// Pass addToSnapshot=false since scheduleOnNewNode already adds the pod to the snapshot.
	p.AddPod(pod, pE.evictionTime, false)
}

func (p *pods) generateNewNameForPod(previousName string) string {
	coreNamePart := previousName[:len(previousName)-podSuffixLen]
	for i := 0; i < 20; i++ {
		newName := coreNamePart + rand.String(podSuffixLen)
		if _, ok := p.pods.Load(newName); !ok {
			return newName
		}
	}
	// log warning in an unlikely case the new name cannot be generated
	klog.Warningf("couldn't create new name for replacement pod")
	return ""
}

// scheduleOnNewNode tries to schedule the provided pod on a new node.
func (p *pods) scheduleOnNewNode(pod *corev1.Pod, previousNodeName string) string {
	p.clusterSnapshotMutex.Lock()
	defer p.clusterSnapshotMutex.Unlock()
	newNode, err := p.clusterSnapshot.SchedulePodOnAnyNodeMatching(pod, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: func(info *framework.NodeInfo) bool {
			return info.Node().Name != previousNodeName
		},
	})
	if err != nil {
		klog.Errorf("Unexpected error while trying to find a new node for a pod: %v", err)
	}
	return newNode
}

type podEvictionStatus struct {
	evictionTime    time.Duration
	evicted         bool
	evictionStarted bool
}

// mockNodes is a wrapper over NodeInterface.
type mockNodes struct {
	v1.NodeInterface
	clusterSnapshot      clustersnapshot.ClusterSnapshot
	clusterSnapshotMutex *sync.Mutex
}

// AddNode adds a new node.
func (n *mockNodes) AddNode(node *corev1.Node) {
	n.clusterSnapshotMutex.Lock()
	defer n.clusterSnapshotMutex.Unlock()
	if err := n.clusterSnapshot.AddNodeInfo(framework.NewNodeInfo(node, nil)); err != nil {
		klog.Errorf("%v", err)
	}
}

// Update updates provided node in internal cache and cluster snapshot.
func (n *mockNodes) Update(_ context.Context, node *corev1.Node, _ metav1.UpdateOptions) (*corev1.Node, error) {
	// updating in cluster snapshot
	n.clusterSnapshotMutex.Lock()
	defer n.clusterSnapshotMutex.Unlock()
	existing, err := n.clusterSnapshot.GetNodeInfo(node.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to update node %v: %v", node.Name, err)
	}
	if err := n.clusterSnapshot.RemoveNodeInfo(node.Name); err != nil {
		return nil, err
	}
	var pods []*corev1.Pod
	for _, pod := range existing.Pods() {
		pods = append(pods, pod.Pod)
	}
	if err := n.clusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)); err != nil {
		return nil, err
	}
	return node, nil
}

// Delete deletes provided node.
func (n *mockNodes) Delete(_ context.Context, name string, _ metav1.DeleteOptions) error {
	// deleting from cluster snapshot
	n.clusterSnapshotMutex.Lock()
	defer n.clusterSnapshotMutex.Unlock()
	if err := n.clusterSnapshot.RemoveNodeInfo(name); err != nil {
		return err
	}
	return nil
}

// List lists existing nodes.
func (n *mockNodes) List(_ context.Context, _ metav1.ListOptions) (*corev1.NodeList, error) {
	result := &corev1.NodeList{Items: []corev1.Node{}}
	n.clusterSnapshotMutex.Lock()
	nodes, err := n.clusterSnapshot.ListNodeInfos()
	n.clusterSnapshotMutex.Unlock()
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %v", err)
	}
	for _, node := range nodes {
		result.Items = append(result.Items, *node.Node())
	}
	return result, nil
}

// Get returns node given by provided name.
func (n *mockNodes) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Node, error) {
	n.clusterSnapshotMutex.Lock()
	nodeInfo, err := n.clusterSnapshot.GetNodeInfo(name)
	n.clusterSnapshotMutex.Unlock()
	if err != nil {
		return nil, err
	}
	return nodeInfo.Node(), nil
}
