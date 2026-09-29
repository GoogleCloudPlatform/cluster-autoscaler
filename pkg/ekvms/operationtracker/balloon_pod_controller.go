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

package operationtracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/kubernetes/pkg/controller"
)

const (
	podDeletionTimeout       = 15 * time.Second
	podCreationTimeout       = 15 * time.Second
	ipprKubeletResizeTimeout = 20 * time.Second
	ipprPatchTimeout         = 7 * time.Second
	ipprPatchVerifyTimeout   = 3 * time.Second

	balloonPodControllerLogPrefix = "Balloon Pod Controller: "
)

type resizeWaiter struct {
	targetCpu resource.Quantity
	targetMem resource.Quantity
	ch        chan error
}

type podStatus struct {
	pod             *v1.Pod
	state           BalloonPodState
	waitForRunning  chan struct{}
	waitForDeletion chan struct{}
	resizeWaiter    *resizeWaiter
}

type balloonPodControllerImpl struct {
	clientSet          clientset.Interface
	informerFactory    informers.SharedInformerFactory
	podDeletionTimeout time.Duration
	podCreationTimeout time.Duration
	podResizeTimeout   time.Duration

	mux  sync.Mutex
	pods map[string]map[types.UID]*podStatus

	onAdd    func(obj interface{})
	onDelete func(obj interface{})
}

func newBalloonPodController(clientSet clientset.Interface, informerFactory informers.SharedInformerFactory) *balloonPodControllerImpl {
	b := &balloonPodControllerImpl{
		clientSet:          clientSet,
		informerFactory:    informerFactory,
		podDeletionTimeout: podDeletionTimeout,
		podCreationTimeout: podCreationTimeout,
		podResizeTimeout:   ipprKubeletResizeTimeout,
		pods:               make(map[string]map[types.UID]*podStatus),
	}
	b.onAdd = b.defaultOnAdd
	b.onDelete = b.defaultOnDelete
	return b
}

func (c *balloonPodControllerImpl) Init() error {
	klog.V(4).Infof("%sInitializing balloon pod controller", balloonPodControllerLogPrefix)
	podInformer := c.informerFactory.Core().V1().Pods()
	if _, err := podInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: c.onAdd,
			UpdateFunc: func(_, new interface{}) {
				c.onAdd(new)
			},
			DeleteFunc: c.onDelete,
		},
	); err != nil {
		return err
	}
	labelSelector := fmt.Sprintf("%s=%s", componentLabel, balloonPodComponentLabelValue)
	podList, err := c.clientSet.CoreV1().Pods("kube-system").List(context.TODO(), metav1.ListOptions{LabelSelector: labelSelector, ResourceVersion: "0"})
	if err != nil {
		klog.Errorf("Cannot list pods")
		return err
	}
	klog.V(4).Infof("%sFound %d balloon pods in initialization", balloonPodControllerLogPrefix, len(podList.Items))
	for i := range podList.Items {
		// Never pass interator reference.
		c.onAdd(&podList.Items[i])
	}
	klog.V(4).Infof("%sInitialized balloon pod controller", balloonPodControllerLogPrefix)
	return nil
}

func (c *balloonPodControllerImpl) CreateBalloonPod(node *v1.Node, cpu, mem resource.Quantity) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), c.podCreationTimeout)
	defer cancelFunc()
	if node == nil {
		return fmt.Errorf("nil Node")
	}
	if entries := c.getPodEntriesByNode(node.Name); len(entries) != 0 {
		var podNames []string
		for _, p := range entries {
			podNames = append(podNames, p.pod.Name)
		}
		return fmt.Errorf("balloon pod(s) %q already exists on node %s", strings.Join(podNames, ", "), node.Name)
	}

	klog.V(4).Infof("%sCreating balloon Pod for node %q: cpuMilli: %d, mem: %d", balloonPodControllerLogPrefix, node.Name, cpu.MilliValue(), mem.Value())
	bp, err := GenerateBalloonPod(node, cpu, mem, false)
	if err != nil {
		return fmt.Errorf("balloon pod generation error: %v", err)
	}
	pod, err := c.clientSet.CoreV1().Pods(bp.Namespace).Create(ctx, bp, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("balloon pod creation error: %v", err)
	}
	// Race-condition with onAdd.
	c.mux.Lock()
	podEntry, exists := c.getPodEntry(pod)
	if !exists {
		podEntry = newPodStatus(pod)
		if err := c.addOrUpdatePodEntry(podEntry); err != nil {
			c.mux.Unlock()
			return fmt.Errorf("failure while adding pod entry %+v: %v", podEntry, err)
		}
	}
	c.mux.Unlock()

	select {
	case <-podEntry.waitForRunning:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("balloon pod not running: %v", ctx.Err())
	}
}

func (c *balloonPodControllerImpl) ResizeBalloonPodInPlace(node *v1.Node, cpu, mem resource.Quantity) error {
	if node == nil {
		return fmt.Errorf("nil Node")
	}
	pod, ch, err := c.reservePodForResize(node, cpu, mem)
	if err != nil {
		return err
	}
	if pod == nil {
		return nil
	}
	defer func() {
		c.mux.Lock()
		defer c.mux.Unlock()
		if entry, exists := c.getPodEntry(pod); exists && entry.resizeWaiter != nil && entry.resizeWaiter.ch == ch {
			entry.resizeWaiter = nil
		}
	}()

	if !hasMatchingSpec(pod, cpu, mem) {
		if err := c.patchPod(pod, cpu, mem); err != nil {
			// Under apiserver load, a patch may succeed on the server while the client times
			// out reading the response. Verify whether the write landed before falling back.
			if !c.patchLanded(pod, cpu, mem) {
				return err
			}
			klog.Warningf("%sResize patch for balloon pod %q (node: %q) reported %v, but pod spec matches target - continuing with Kubelet wait",
				balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.podResizeTimeout)
	defer cancel()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%w on node %q", ek_errors.ResizeTimeoutError, pod.Spec.NodeName)
	}
}

// patchLanded checks if an in-place resize patch was applied despite a patch error.
func (c *balloonPodControllerImpl) patchLanded(pod *v1.Pod, cpu, mem resource.Quantity) bool {
	ctx, cancel := context.WithTimeout(context.Background(), ipprPatchVerifyTimeout)
	defer cancel()

	fresh, err := c.clientSet.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		klog.V(4).Infof("%sCould not re-read balloon pod %q: %v", balloonPodControllerLogPrefix, pod.Name, err)
		return false
	}
	if fresh.UID != pod.UID {
		return false
	}
	return hasMatchingSpec(fresh, cpu, mem)
}

func (c *balloonPodControllerImpl) reservePodForResize(node *v1.Node, cpu, mem resource.Quantity) (*v1.Pod, chan error, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	nodePods, exists := c.pods[node.Name]
	if !exists || len(nodePods) == 0 {
		return nil, nil, fmt.Errorf("%w for node %q", ek_errors.NoActiveBalloonPodError, node.Name)
	}
	if len(nodePods) > 1 {
		return nil, nil, fmt.Errorf("multiple balloon pods (%d) found for node %q", len(nodePods), node.Name)
	}

	var podEntry *podStatus
	for _, entry := range nodePods {
		podEntry = entry
		break
	}

	if podEntry.resizeWaiter != nil {
		return nil, nil, fmt.Errorf("%w for balloon pod %q", ek_errors.ConcurrentResizeError, podEntry.pod.Name)
	}

	pod := podEntry.pod
	if !controller.IsPodActive(pod) {
		return nil, nil, fmt.Errorf("%w for node %q", ek_errors.NoActiveBalloonPodError, node.Name)
	}

	if isGuaranteedQoS(pod) {
		klog.V(4).Infof("%sBalloon pod %q for node %q is in the Guaranteed QoS class, skipping in-place resize in favor of recreation",
			balloonPodControllerLogPrefix, pod.Name, node.Name)
		return nil, nil, fmt.Errorf("%w: balloon pod %q is in the Guaranteed QoS class", ek_errors.IncompatibleQoSError, pod.Name)
	}

	currentState := getResizeState(pod)
	if currentState.shouldRecreate() {
		return nil, nil, fmt.Errorf("%w as %s", ek_errors.ResizeRejectedError, currentState)
	}

	if hasMatchingSpec(pod, cpu, mem) && hasMatchingAlloc(pod, cpu, mem) && currentState != resizeStateInProgress {
		return nil, nil, nil
	}

	ch := make(chan error, 1)
	podEntry.resizeWaiter = &resizeWaiter{
		targetCpu: cpu,
		targetMem: mem,
		ch:        ch,
	}

	return pod, ch, nil
}

func (c *balloonPodControllerImpl) patchPod(pod *v1.Pod, cpu, mem resource.Quantity) error {
	containerSpec := getBalloonContainerSpec(pod)
	if containerSpec == nil {
		return fmt.Errorf("%w: balloon pod %q has no %q container", ek_errors.ResizePatchError, pod.Name, balloonContainerName)
	}

	klog.V(4).Infof("%sIn-place resizing balloon Pod %q (node: %q): new cpuRequests: %s, memoryRequests: %s",
		balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName, cpu.String(), mem.String())

	patchPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: containerSpec.Name,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    cpu,
							v1.ResourceMemory: mem,
						},
					},
				},
			},
		},
	}

	patchData, err := json.Marshal(patchPod)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data: %w: %w", ek_errors.ResizePatchError, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipprPatchTimeout)
	defer cancel()

	_, err = c.clientSet.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, patchData, metav1.PatchOptions{}, "resize")
	if err != nil {
		return fmt.Errorf("failed to in-place patch balloon pod %q: %w: %w", pod.Name, ek_errors.ResizePatchError, err)
	}
	return nil
}

func (c *balloonPodControllerImpl) DeleteAllBalloonPods(node *v1.Node) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), c.podDeletionTimeout)
	defer cancelFunc()
	if node == nil {
		return fmt.Errorf("nil Node")
	}

	podEntries := c.getPodEntriesByNode(node.Name)
	if len(podEntries) == 0 {
		// Nothing to do.
		return nil
	}

	errCh := make(chan error, len(podEntries))
	for i := range podEntries {
		podEntry := podEntries[i] // Needed for go < 1.22
		go func() {
			errCh <- c.deletePod(ctx, podEntry)
		}()
	}

	var errs []error
	for range podEntries {
		err := <-errCh
		if err != nil {
			errs = append(errs, err)
		}
	}
	err := errors.Join(errs...)
	if err != nil {
		return fmt.Errorf("error while deleting balloon pod(s) for node %s: %v", node.Name, err)
	}
	return nil
}

func (c *balloonPodControllerImpl) deletePod(ctx context.Context, podEntry *podStatus) error {
	pod := podEntry.pod
	klog.V(4).Infof("%sDeleting Pod %q of node %q", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName)
	deletePolicy := metav1.DeletePropagationBackground
	err := c.clientSet.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: int64Ptr(0),
		PropagationPolicy:  &deletePolicy,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Pod is already gone (for some reason), we need to clean up the state manually.
			klog.Warningf("%sDeleting balloon pod %q entry: %v", balloonPodControllerLogPrefix, pod.Name, err)
			c.defaultOnDelete(pod)
			return nil
		}
		return fmt.Errorf("deleting balloon pod %q failed: %v", pod.Name, err)
	}
	select {
	case <-podEntry.waitForDeletion:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("balloon pod %q not deleted: %v", pod.Name, ctx.Err())
	}
}

func int64Ptr(i int64) *int64 { return &i }

func (c *balloonPodControllerImpl) List() []*v1.Pod {
	c.mux.Lock()
	defer c.mux.Unlock()

	podList := make([]*v1.Pod, 0, len(c.pods))
	for _, nodePods := range c.pods {
		for _, entry := range nodePods {
			podList = append(podList, entry.pod)
		}
	}
	return podList
}

func (c *balloonPodControllerImpl) GetPodsForNode(node *v1.Node) []*v1.Pod {
	if node == nil {
		return nil
	}
	c.mux.Lock()
	defer c.mux.Unlock()

	var pods []*v1.Pod
	for _, entry := range c.pods[node.Name] {
		pods = append(pods, entry.pod)
	}

	return pods
}

func (c *balloonPodControllerImpl) defaultOnAdd(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Warningf("Informer has incorrect object type: %v", obj)
		return
	}
	if !IsBalloonPod(pod) {
		return
	}
	if pod.Spec.NodeName == "" {
		// Should never happen.
		klog.Errorf("%sBalloon pod %q is not bound to a Node", balloonPodControllerLogPrefix, pod.Name)
		return
	}
	podState := getBalloonPodState(pod)
	c.mux.Lock()
	defer c.mux.Unlock()

	podEntry, exists := c.getPodEntry(pod)
	if !exists {
		podEntry = newPodStatus(pod)
		klog.V(5).Infof("%sBalloon pod %q for node %q is being added to balloon pod controller internal state", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName)
	}

	if exists && podState < podEntry.state {
		// Return if the pod is already in a later state in its lifecycle
		klog.V(5).Infof("%sBalloon pod %q for node %q addition/update is skipped in balloon pod controller internal state: podState state %s is lower than the internal podState %s", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName, podState, podEntry.state)
		return
	} else {
		// Same-state updates are processed too, so that the cached pod keeps up with Kubelet updating the pod status during an in-place resize.
		klog.V(5).Infof("%sBalloon pod %q for node %q is being updated in balloon pod controller internal state: podState state %s, internal podState %s", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName, podState, podEntry.state)
	}

	if podEntry.resizeWaiter != nil {
		resizeState := getResizeState(pod)
		if resizeState.shouldRecreate() {
			podEntry.resizeWaiter.ch <- fmt.Errorf("%w as %s", ek_errors.ResizeRejectedError, resizeState)
			podEntry.resizeWaiter = nil
		} else if hasMatchingSpec(pod, podEntry.resizeWaiter.targetCpu, podEntry.resizeWaiter.targetMem) &&
			hasMatchingAlloc(pod, podEntry.resizeWaiter.targetCpu, podEntry.resizeWaiter.targetMem) &&
			resizeState != resizeStateInProgress {
			podEntry.resizeWaiter.ch <- nil
			podEntry.resizeWaiter = nil
		}
	}

	// On transition to running or when first-spotted pod is running.
	if podState > BalloonPodTemplate && (!exists || podEntry.state == BalloonPodTemplate) {
		close(podEntry.waitForRunning)
	}
	podEntry.pod = pod
	podEntry.state = podState
	err := c.addOrUpdatePodEntry(podEntry)
	if err != nil {
		klog.Errorf("%sFailure while adding or updating pod entry %+v: %v", balloonPodControllerLogPrefix, podEntry, err)
	} else {
		klog.V(5).Infof("%sBalloon pod %q for node %q is added/updated in balloon pod controller internal state", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName)
	}
}

func (c *balloonPodControllerImpl) defaultOnDelete(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Warningf("Informer has incorrect object type: %v", obj)
		return
	}
	if !IsBalloonPod(pod) || pod.Spec.NodeName == "" {
		return
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	klog.V(5).Infof("%sBalloon pod %q for node %q is being deleted from balloon pod controller internal state", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName)

	podEntry, exists := c.getPodEntry(pod)
	if !exists {
		klog.V(5).Infof("%sBalloon pod %q for node %q deletion is skipped in balloon pod controller internal state as podEntry does not exist", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName)
		return
	}

	if podEntry.resizeWaiter != nil {
		podEntry.resizeWaiter.ch <- fmt.Errorf("balloon pod %q on node %q was deleted during resize", pod.Name, pod.Spec.NodeName)
		podEntry.resizeWaiter = nil
	}

	c.deletePodEntry(podEntry)
	close(podEntry.waitForDeletion)
	klog.V(5).Infof("%sBalloon pod %q for node %q is deleted from balloon pod controller internal state", balloonPodControllerLogPrefix, pod.Name, pod.Spec.NodeName)
}

// Should be called under a mutex.
func (c *balloonPodControllerImpl) getPodEntry(pod *v1.Pod) (*podStatus, bool) {
	nodeName := pod.Spec.NodeName
	if len(nodeName) == 0 {
		klog.Errorf("Couldn't find node name for balloon pod %s", pod.Name)
		return nil, false
	}
	podUid := pod.UID
	nodePods, exists := c.pods[nodeName]
	if !exists {
		return nil, false
	}
	entry, exists := nodePods[podUid]
	return entry, exists
}

// Should be called under a mutex.
func (c *balloonPodControllerImpl) deletePodEntry(entry *podStatus) {
	nodeName := entry.pod.Spec.NodeName
	podUid := entry.pod.UID
	nodePods, exists := c.pods[nodeName]
	if !exists {
		klog.V(5).Infof("%sBalloon pod %s on node %q is not found in balloon pod controller internal state (node entry missing), deletion skipped", balloonPodControllerLogPrefix, entry.pod.Name, entry.pod.Spec.NodeName)
		return
	}
	if _, found := nodePods[podUid]; !found {
		klog.V(5).Infof("%sBalloon pod %s on node %q is not found in balloon pod controller internal state (pod UID entry missing), deletion skipped", balloonPodControllerLogPrefix, entry.pod.Name, entry.pod.Spec.NodeName)
		return
	}
	delete(nodePods, podUid)
	if len(nodePods) == 0 {
		delete(c.pods, nodeName)
	}
}

// Should be called under a mutex.
func (c *balloonPodControllerImpl) addOrUpdatePodEntry(entry *podStatus) error {
	pod := entry.pod
	if pod == nil {
		return fmt.Errorf("pod is empty in podStatus %+v", entry)
	}
	nodeName := pod.Spec.NodeName
	if len(nodeName) == 0 {
		return fmt.Errorf("couldn't find node name for balloon pod %s", pod.Name)
	}
	podUid := pod.UID
	if c.pods[nodeName] == nil {
		c.pods[nodeName] = make(map[types.UID]*podStatus)
	}
	c.pods[nodeName][podUid] = entry
	return nil
}

func (c *balloonPodControllerImpl) getPodEntriesByNode(nodeName string) []*podStatus {
	c.mux.Lock()
	defer c.mux.Unlock()
	nodePods := c.pods[nodeName]
	podEntries := make([]*podStatus, 0, len(nodePods))
	for _, entry := range nodePods {
		podEntries = append(podEntries, entry)
	}
	return podEntries
}

func newPodStatus(pod *v1.Pod) *podStatus {
	return &podStatus{
		pod:             pod,
		state:           BalloonPodTemplate,
		waitForRunning:  make(chan struct{}),
		waitForDeletion: make(chan struct{}),
	}
}
