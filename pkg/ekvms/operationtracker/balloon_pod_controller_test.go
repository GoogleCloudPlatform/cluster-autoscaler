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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	client_testing "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestBalloonPodControllerInit(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024*1024)

	bPod1Node1NotRunning, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		true)
	assert.NoError(t, err)
	bPod1Node1Running := bPod1Node1NotRunning.DeepCopy()
	bPod1Node1Running.Status.Phase = v1.PodRunning
	bPod1Node1Terminated := bPod1Node1NotRunning.DeepCopy()
	bPod1Node1Terminated.Status.Phase = v1.PodSucceeded

	bPod2Node1NotRunning, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		true)
	assert.NoError(t, err)
	bPod2Node1Running := bPod2Node1NotRunning.DeepCopy()
	bPod2Node1Running.Status.Phase = v1.PodRunning

	notBPod := test.BuildTestPod("random", 0, 0, func(pod *v1.Pod) {
		pod.Namespace = "kube-system"
		pod.Spec.NodeName = "node1"
	})
	node2 := test.BuildTestNode("node2", 1000, 1024*1024)
	bPod1Node2Running, err := GenerateBalloonPod(node2,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		true)
	bPod1Node2Running.Status.Phase = v1.PodRunning
	assert.NoError(t, err)
	testCases := []struct {
		desc          string
		initPods      []*v1.Pod
		afterInitPods []*v1.Pod
		expectedPods  []*v1.Pod
	}{
		{
			desc:          "Not running pod added in init",
			initPods:      []*v1.Pod{bPod1Node1NotRunning},
			afterInitPods: []*v1.Pod{},
			expectedPods:  []*v1.Pod{bPod1Node1NotRunning},
		},
		{
			desc:          "Multiple not running pods added in init",
			initPods:      []*v1.Pod{bPod1Node1NotRunning, bPod2Node1NotRunning},
			afterInitPods: []*v1.Pod{},
			expectedPods:  []*v1.Pod{bPod1Node1NotRunning, bPod2Node1NotRunning},
		},
		{
			desc:          "Running pod added in init",
			initPods:      []*v1.Pod{bPod1Node1Running},
			afterInitPods: []*v1.Pod{bPod1Node1Running},
			expectedPods:  []*v1.Pod{bPod1Node1Running},
		},
		{
			desc:          "Not running pod added in init, becomes running after init",
			initPods:      []*v1.Pod{bPod1Node1NotRunning},
			afterInitPods: []*v1.Pod{bPod1Node1Running},
			expectedPods:  []*v1.Pod{bPod1Node1Running},
		},
		{
			desc:          "Multiple not running pod added in init, becomes running after init",
			initPods:      []*v1.Pod{bPod1Node1NotRunning, bPod2Node1NotRunning},
			afterInitPods: []*v1.Pod{bPod1Node1Running, bPod2Node1Running},
			expectedPods:  []*v1.Pod{bPod1Node1Running, bPod2Node1Running},
		},
		{
			desc:          "Running pod added in init and another one added in after init, both of them exist",
			initPods:      []*v1.Pod{bPod1Node1Running},
			afterInitPods: []*v1.Pod{bPod2Node1Running},
			expectedPods:  []*v1.Pod{bPod1Node1Running, bPod2Node1Running},
		},
		{
			desc:          "Not running pod added in init and another running one added in after init, both of them exist with correct states",
			initPods:      []*v1.Pod{bPod1Node1NotRunning},
			afterInitPods: []*v1.Pod{bPod2Node1Running},
			expectedPods:  []*v1.Pod{bPod1Node1NotRunning, bPod2Node1Running},
		},
		{
			desc:          "Running pod added in init, not running update received after init",
			initPods:      []*v1.Pod{bPod1Node1Running},
			afterInitPods: []*v1.Pod{bPod1Node1NotRunning},
			expectedPods:  []*v1.Pod{bPod1Node1Running},
		},
		{
			desc:          "Running pod added in init, becomes terminated after init",
			initPods:      []*v1.Pod{bPod1Node1Running},
			afterInitPods: []*v1.Pod{bPod1Node1Terminated},
			expectedPods:  []*v1.Pod{bPod1Node1Terminated},
		},
		{
			desc:          "Not running pod added in init, becomes running after init and new running pod added after init",
			initPods:      []*v1.Pod{bPod1Node1NotRunning},
			afterInitPods: []*v1.Pod{bPod1Node1Running, bPod1Node2Running},
			expectedPods:  []*v1.Pod{bPod1Node1Running, bPod1Node2Running},
		},
		{
			desc:          "Running pod added in init, not running update received after init and new running pod added after init",
			initPods:      []*v1.Pod{bPod1Node1Running},
			afterInitPods: []*v1.Pod{bPod1Node1NotRunning, bPod1Node2Running},
			expectedPods:  []*v1.Pod{bPod1Node1Running, bPod1Node2Running},
		},
		{
			desc:          "Not BPods are omitted",
			initPods:      []*v1.Pod{bPod1Node1NotRunning, notBPod},
			afterInitPods: []*v1.Pod{},
			expectedPods:  []*v1.Pod{bPod1Node1NotRunning},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			onAddWG := sync.WaitGroup{}
			clientSet := fake.NewSimpleClientset()
			informerClientSet := fake.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(informerClientSet, 0)

			for _, pod := range tc.initPods {
				if pod.Labels[componentLabel] == balloonPodComponentLabelValue {
					onAddWG.Add(1)
				}
				_, err := clientSet.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			bPController := newBalloonPodController(clientSet, informerFactory)
			defaultOnAdd := bPController.onAdd
			bPController.onAdd = func(obj interface{}) {
				defaultOnAdd(obj)
				onAddWG.Done()
			}
			err = bPController.Init()
			assert.NoError(t, err)
			onAddWG.Wait()

			stopCh := make(chan struct{})
			defer close(stopCh)
			informerFactory.Start(stopCh)
			_ = informerFactory.WaitForCacheSync(stopCh)
			for _, pod := range tc.afterInitPods {
				onAddWG.Add(1)
				_, err := informerClientSet.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
				assert.NoError(t, err)
				onAddWG.Wait()
			}

			bPController.mux.Lock()
			defer bPController.mux.Unlock()

			expectedPodsGroupedByNode := make(map[string][]*v1.Pod)
			for _, p := range tc.expectedPods {
				expectedPodsGroupedByNode[p.Spec.NodeName] = append(expectedPodsGroupedByNode[p.Spec.NodeName], p)
			}

			for nodeName, podList := range bPController.pods {
				var actualPods []*v1.Pod
				for _, p := range podList {
					actualPods = append(actualPods, p.pod)
				}
				assert.ElementsMatch(t, expectedPodsGroupedByNode[nodeName], actualPods)
			}
		})
	}
}

func TestCreateBalloonPod(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024*1024)

	bPod1NotRunning, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		false)
	assert.NoError(t, err)
	bPod1Running := bPod1NotRunning.DeepCopy()
	bPod1Running.Status = v1.PodStatus{
		Phase: v1.PodRunning,
	}

	bPod2NotRunning, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		false)
	assert.NoError(t, err)
	bPod2Running := bPod2NotRunning.DeepCopy()
	bPod2Running.Status = v1.PodStatus{
		Phase: v1.PodRunning,
	}

	testCases := []struct {
		desc     string
		node     *v1.Node
		initPods []*v1.Pod
		timeout  bool
		wantErr  bool
		wantPods []*v1.Pod
	}{
		{
			desc:     "success",
			node:     node,
			wantErr:  false,
			wantPods: []*v1.Pod{bPod1Running},
		},
		{
			desc:     "nil node",
			node:     nil,
			wantErr:  true,
			wantPods: []*v1.Pod{},
		},
		{
			desc:     "pod already exists",
			node:     nil,
			initPods: []*v1.Pod{bPod1Running},
			wantErr:  true,
			wantPods: []*v1.Pod{bPod1Running},
		},
		{
			desc:     "different balloon pod exist on same node",
			node:     nil,
			initPods: []*v1.Pod{bPod2Running},
			wantErr:  true,
			wantPods: []*v1.Pod{bPod1Running},
		},
		{
			desc:     "timeout",
			node:     node,
			timeout:  true,
			wantErr:  true,
			wantPods: []*v1.Pod{bPod1NotRunning},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stopCh := make(chan struct{})
				defer close(stopCh)
				clientSet := fake.NewSimpleClientset()
				informerFactory := informers.NewSharedInformerFactory(clientSet, 0)
				podInformer := informerFactory.Core().V1().Pods()

				if !tc.timeout {
					_, err := podInformer.Informer().AddEventHandler(
						cache.ResourceEventHandlerFuncs{
							AddFunc: podRunner(t, clientSet),
							UpdateFunc: func(_, new interface{}) {
								podRunner(t, clientSet)(new)
							},
						},
					)
					assert.NoError(t, err)
				}
				for _, pod := range tc.initPods {
					_, err := clientSet.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
					assert.NoError(t, err)
				}

				bPController := newBalloonPodController(clientSet, informerFactory)
				if tc.timeout {
					bPController.podCreationTimeout = 1 * time.Millisecond
				}
				err := bPController.Init()
				assert.NoError(t, err)
				informerFactory.Start(stopCh)
				_ = informerFactory.WaitForCacheSync(stopCh)
				synctest.Wait()
				err = bPController.CreateBalloonPod(tc.node,
					*resource.NewMilliQuantity(1000, resource.DecimalSI),
					*resource.NewQuantity(1024*1024, resource.DecimalSI))
				if tc.wantErr {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				pods := bPController.List()
				assert.Equal(t, tc.wantPods, pods)
			})
		})
	}
}

func TestDeleteAllBalloonPods(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024*1024)

	bPod1NotRunning, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		true)
	assert.NoError(t, err)
	bPod1Running := bPod1NotRunning.DeepCopy()
	bPod1Running.Status = v1.PodStatus{
		Phase: v1.PodRunning,
	}

	bPod2NotRunning, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		true)
	assert.NoError(t, err)
	bPod2Running := bPod2NotRunning.DeepCopy()
	bPod2Running.Status = v1.PodStatus{
		Phase: v1.PodRunning,
	}

	testCases := []struct {
		desc     string
		node     *v1.Node
		initPods []*v1.Pod
		timeout  bool
		wantErr  bool
	}{
		{
			desc:     "1 initial pod - success",
			node:     node,
			initPods: []*v1.Pod{bPod1Running},
			wantErr:  false,
		},
		{
			desc:     "2 initial pods - success",
			node:     node,
			initPods: []*v1.Pod{bPod1Running, bPod2Running},
			wantErr:  false,
		},
		{
			desc:     "nil node",
			node:     nil,
			initPods: []*v1.Pod{bPod1Running},
			wantErr:  true,
		},
		{
			desc:     "timeout",
			node:     node,
			initPods: []*v1.Pod{bPod1Running},
			timeout:  true,
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				clientSet := fake.NewSimpleClientset()
				informerFactory := informers.NewSharedInformerFactory(clientSet, 0)
				podInformer := informerFactory.Core().V1().Pods()
				_, err := podInformer.Informer().AddEventHandler(
					cache.ResourceEventHandlerFuncs{
						AddFunc: podRunner(t, clientSet),
						UpdateFunc: func(_, new interface{}) {
							podRunner(t, clientSet)(new)
						},
					},
				)
				assert.NoError(t, err)

				bPController := newBalloonPodController(clientSet, informerFactory)
				if tc.timeout {
					bPController.podDeletionTimeout = 0 * time.Millisecond
					defaultOnDelete := bPController.onDelete
					bPController.onDelete = func(obj interface{}) {
						<-ctx.Done() // Block until the test completes and ctx is canceled
						defaultOnDelete(obj)
					}
				}
				for _, pod := range tc.initPods {
					_, err = clientSet.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
					assert.NoError(t, err)
				}
				err = bPController.Init()
				assert.NoError(t, err)
				informerFactory.Start(ctx.Done())
				_ = informerFactory.WaitForCacheSync(ctx.Done())
				synctest.Wait()
				pods := bPController.List()
				assert.Equal(t, len(tc.initPods), len(pods))

				err = bPController.DeleteAllBalloonPods(tc.node)
				if tc.wantErr {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				pods = bPController.List()
				assert.Equal(t, []*v1.Pod{}, pods)
			})
		})
	}
}

func TestDeleteAllBalloonPods_NotFound(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024*1024)
	bPod, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		false)
	assert.NoError(t, err)

	clientSet := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(clientSet, 0)

	bPController := newBalloonPodController(clientSet, informerFactory)
	// Inject pod manually - we need to desync internal state from clientSet.
	bPController.defaultOnAdd(bPod)
	pods := bPController.List()
	assert.Equal(t, []*v1.Pod{bPod}, pods)

	err = bPController.DeleteAllBalloonPods(node)
	assert.NoError(t, err)
	pods = bPController.List()
	assert.Equal(t, []*v1.Pod{}, pods)
}

func podRunner(t *testing.T, clientSet clientset.Interface) func(interface{}) {
	return func(obj interface{}) {
		pod := obj.(*v1.Pod)
		// Updates here are fed to the informer which is runs this function again in an infinite fashion until error occurs, breaking it by phase checking.
		if pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			return
		}
		updatedPod := pod.DeepCopy()
		updatedPod.Status = v1.PodStatus{
			Phase: v1.PodRunning,
		}
		go func() {
			_, err := clientSet.CoreV1().Pods(pod.Namespace).Update(context.TODO(), updatedPod, metav1.UpdateOptions{})
			assert.NoError(t, err)
		}()
	}
}

func TestDefaultOnAddDoesNotPanic(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(clientSet, 0)
	bPController := newBalloonPodController(clientSet, informerFactory)
	bPController.defaultOnAdd(struct{}{})
}

func TestDefaultOnDeleteDoesNotPanic(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(clientSet, 0)
	bPController := newBalloonPodController(clientSet, informerFactory)
	bPController.defaultOnDelete(struct{}{})
}

// podEvent describes a single informer event for a balloon pod as data rather
// than as a mutation closure, so the test table below stays declarative.
// Annotations are used purely to identify which event produced a given pod, so
// the tests can assert *which* pod ended up cached.
type podEvent struct {
	phase       v1.PodPhase
	unassigned  bool
	annotations map[string]string
}

func (e podEvent) apply(base *v1.Pod, nodeName string) *v1.Pod {
	p := base.DeepCopy()
	p.Status.Phase = e.phase
	p.Annotations = e.annotations
	if e.unassigned {
		p.Spec.NodeName = ""
	} else {
		p.Spec.NodeName = nodeName
	}
	return p
}

// getPodEntryLocked reads a pod entry under the controller lock, which
// getPodEntry expects its callers to hold.
func getPodEntryLocked(c *balloonPodControllerImpl, pod *v1.Pod) (*podStatus, bool) {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.getPodEntry(pod)
}

func TestBalloonPodController_OnAdd(t *testing.T) {
	t.Parallel()

	node := test.BuildTestNode("node", 4000, 4000*1024)
	basePod, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		true)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	tests := []struct {
		name     string
		prior    []podEvent
		incoming podEvent

		wantEntryExists   bool
		wantState         BalloonPodState
		wantRunningClosed bool
		// wantCachedPod identifies, by annotations, which event's pod the entry
		// should be holding once the incoming event has been processed. For
		// ignored events this is the previously cached pod, not the incoming one.
		wantCachedPod map[string]string
	}{
		{
			name:            "unassigned pod without NodeName is ignored",
			incoming:        podEvent{unassigned: true},
			wantEntryExists: false,
		},
		{
			name:              "initial add is cached in Template state",
			incoming:          podEvent{annotations: map[string]string{"event": "1"}},
			wantEntryExists:   true,
			wantState:         BalloonPodTemplate,
			wantRunningClosed: false,
			wantCachedPod:     map[string]string{"event": "1"},
		},
		{
			name:              "first-spotted pod already Running closes the channel",
			incoming:          podEvent{phase: v1.PodRunning, annotations: map[string]string{"event": "1"}},
			wantEntryExists:   true,
			wantState:         BalloonPodRunning,
			wantRunningClosed: true,
			wantCachedPod:     map[string]string{"event": "1"},
		},
		{
			name:              "transition from Template to Running closes the channel",
			prior:             []podEvent{{annotations: map[string]string{"event": "1"}}},
			incoming:          podEvent{phase: v1.PodRunning, annotations: map[string]string{"event": "2"}},
			wantEntryExists:   true,
			wantState:         BalloonPodRunning,
			wantRunningClosed: true,
			wantCachedPod:     map[string]string{"event": "2"},
		},
		{
			// This is the case the same-state handling exists for: Kubelet reports
			// in-place resize progress through pod status while the pod stays
			// Running, so the cached pod has to be refreshed even though the state
			// does not change.
			name: "same-state Running update refreshes the cached pod",
			prior: []podEvent{
				{annotations: map[string]string{"event": "1"}},
				{phase: v1.PodRunning, annotations: map[string]string{"event": "2"}},
			},
			incoming:          podEvent{phase: v1.PodRunning, annotations: map[string]string{"event": "3"}},
			wantEntryExists:   true,
			wantState:         BalloonPodRunning,
			wantRunningClosed: true,
			wantCachedPod:     map[string]string{"event": "3"},
		},
		{
			name: "transition from Running to Terminated updates state",
			prior: []podEvent{
				{annotations: map[string]string{"event": "1"}},
				{phase: v1.PodRunning, annotations: map[string]string{"event": "2"}},
			},
			incoming:          podEvent{phase: v1.PodSucceeded, annotations: map[string]string{"event": "3"}},
			wantEntryExists:   true,
			wantState:         BalloonPodTerminated,
			wantRunningClosed: true,
			wantCachedPod:     map[string]string{"event": "3"},
		},
		{
			name: "out-of-order Running event after termination is ignored",
			prior: []podEvent{
				{annotations: map[string]string{"event": "1"}},
				{phase: v1.PodRunning, annotations: map[string]string{"event": "2"}},
				{phase: v1.PodSucceeded, annotations: map[string]string{"event": "3"}},
			},
			incoming:          podEvent{phase: v1.PodRunning, annotations: map[string]string{"event": "4"}},
			wantEntryExists:   true,
			wantState:         BalloonPodTerminated,
			wantRunningClosed: true,
			wantCachedPod:     map[string]string{"event": "3"},
		},
		{
			name: "out-of-order Template event after Running is ignored",
			prior: []podEvent{
				{annotations: map[string]string{"event": "1"}},
				{phase: v1.PodRunning, annotations: map[string]string{"event": "2"}},
			},
			incoming:          podEvent{annotations: map[string]string{"event": "3"}},
			wantEntryExists:   true,
			wantState:         BalloonPodRunning,
			wantRunningClosed: true,
			wantCachedPod:     map[string]string{"event": "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clientSet := fake.NewSimpleClientset()
			c := newBalloonPodController(clientSet, informers.NewSharedInformerFactory(clientSet, 0))

			for _, e := range tt.prior {
				c.defaultOnAdd(e.apply(basePod, node.Name))
			}
			incoming := tt.incoming.apply(basePod, node.Name)
			c.defaultOnAdd(incoming)

			entry, exists := getPodEntryLocked(c, incoming)
			if !assert.Equal(t, tt.wantEntryExists, exists) {
				t.FailNow()
			}
			if !exists {
				return
			}

			assert.Equal(t, tt.wantState, entry.state)
			assert.Equal(t, tt.wantRunningClosed, isChanClosed(entry.waitForRunning), "waitForRunning closed status mismatch")
			assert.Equal(t, tt.wantCachedPod, entry.pod.Annotations, "entry holds the pod from the wrong event")
		})
	}
}

func isChanClosed(ch <-chan struct{}) bool {
	select {
	case _, ok := <-ch:
		return !ok
	default:
		return false
	}
}

func setBalloonSpecRequests(t *testing.T, p *v1.Pod, cpu, mem resource.Quantity) {
	t.Helper()

	found := false
	for i := range p.Spec.Containers {
		if p.Spec.Containers[i].Name == balloonContainerName {
			if p.Spec.Containers[i].Resources.Requests == nil {
				p.Spec.Containers[i].Resources.Requests = make(v1.ResourceList)
			}
			p.Spec.Containers[i].Resources.Requests[v1.ResourceCPU] = cpu
			p.Spec.Containers[i].Resources.Requests[v1.ResourceMemory] = mem
			found = true
			break
		}
	}
	assert.True(t, found, "balloon container %q not found in pod spec", balloonContainerName)
}

func verifyResizePatchAction(t *testing.T, actions []client_testing.Action, expectPatch bool, targetCpu, targetMem resource.Quantity) {
	t.Helper()

	var patchActions []client_testing.PatchAction
	for _, a := range actions {
		if pa, ok := a.(client_testing.PatchAction); ok {
			patchActions = append(patchActions, pa)
		}
	}

	if !expectPatch {
		assert.Empty(t, patchActions, "expected no patch actions")
		return
	}

	if !assert.Len(t, patchActions, 1, "expected exactly one patch action") {
		return
	}
	patchAction := patchActions[0]

	assert.Equal(t, "resize", patchAction.GetSubresource(), "expected patch on resize subresource")

	var patchPayload struct {
		Spec struct {
			Containers []struct {
				Name      string                  `json:"name"`
				Resources v1.ResourceRequirements `json:"resources"`
			} `json:"containers"`
		} `json:"spec"`
	}

	if !assert.NoError(t, json.Unmarshal(patchAction.GetPatch(), &patchPayload), "failed to unmarshal patch payload") {
		return
	}

	var balloonFound bool
	for _, container := range patchPayload.Spec.Containers {
		if container.Name != balloonContainerName {
			continue
		}

		cpuReq, hasCpu := container.Resources.Requests[v1.ResourceCPU]
		if !assert.True(t, hasCpu, "cpu request missing in patch") {
			return
		}
		assert.True(t, targetCpu.Equal(cpuReq), "expected CPU %s, got %s", targetCpu.String(), cpuReq.String())

		memReq, hasMem := container.Resources.Requests[v1.ResourceMemory]
		if !assert.True(t, hasMem, "memory request missing in patch") {
			return
		}
		assert.True(t, targetMem.Equal(memReq), "expected Memory %s, got %s", targetMem.String(), memReq.String())

		balloonFound = true
		break
	}
	assert.True(t, balloonFound, "balloon container %q not found in patch payload", balloonContainerName)
}

func buildPodWithAllocated(t *testing.T, base *v1.Pod, cpu, mem resource.Quantity) *v1.Pod {
	t.Helper()

	p := base.DeepCopy()
	setBalloonSpecRequests(t, p, cpu, mem)
	p.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name: balloonContainerName,
		AllocatedResources: v1.ResourceList{
			v1.ResourceCPU:    cpu,
			v1.ResourceMemory: mem,
		},
	}}
	return p
}

// withResizeCondition reports the resize state the way Kubelet does: through the
// PodResizePending and PodResizeInProgress conditions.
func withResizeCondition(p *v1.Pod, condType v1.PodConditionType, reason string) *v1.Pod {
	cp := p.DeepCopy()
	cp.Status.Conditions = append(cp.Status.Conditions, v1.PodCondition{
		Type:   condType,
		Status: v1.ConditionTrue,
		Reason: reason,
	})
	return cp
}

func withQoSClass(p *v1.Pod, qos v1.PodQOSClass) *v1.Pod {
	cp := p.DeepCopy()
	cp.Status.QOSClass = qos
	return cp
}

// withResourceLimits mimics a legacy balloon pod created before balloon pods moved to the Burstable QoS class.
func withResourceLimits(t *testing.T, p *v1.Pod, cpu, mem resource.Quantity) *v1.Pod {
	t.Helper()

	cp := p.DeepCopy()
	c := getBalloonContainerSpec(cp)
	if !assert.NotNil(t, c) {
		t.FailNow()
	}
	c.Resources.Limits = v1.ResourceList{
		v1.ResourceCPU:    cpu,
		v1.ResourceMemory: mem,
	}
	return cp
}

func setupTestPod(t *testing.T, node *v1.Node, cpu, mem resource.Quantity) *v1.Pod {
	t.Helper()
	pod, err := GenerateBalloonPod(node, cpu, mem, true)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	pod.Name = "balloon-pod-node1"
	pod.Namespace = "kube-system"
	pod.UID = "pod-uid-node1"
	pod.ResourceVersion = "1"
	pod.Spec.NodeName = node.Name
	pod.Status.Phase = v1.PodRunning
	return pod
}

func (c *balloonPodControllerImpl) defaultOnUpdate(_, newObj interface{}) {
	c.defaultOnAdd(newObj)
}

func TestBalloonPodController_ResizeBalloonPodInPlace_Immediate(t *testing.T) {
	t.Parallel()

	node := test.BuildTestNode("node1", 4000, 4000*1024)
	targetCpu := resource.MustParse("2000m")
	targetMem := resource.MustParse("200Mi")

	basePod := setupTestPod(t, node, resource.MustParse("1000m"), resource.MustParse("100Mi"))
	podAlreadyResized := buildPodWithAllocated(t, basePod, targetCpu, targetMem)

	now := metav1.NewTime(time.Now())
	terminatingPod := basePod.DeepCopy()
	terminatingPod.DeletionTimestamp = &now

	failedPod := basePod.DeepCopy()
	failedPod.Status.Phase = v1.PodFailed

	secondPod := setupTestPod(t, node, resource.MustParse("1000m"), resource.MustParse("100Mi"))
	secondPod.Name = "balloon-pod-node1-second"
	secondPod.UID = "pod-uid-node1-second"

	tests := []struct {
		desc        string
		targetNode  *v1.Node
		pod         *v1.Pod
		extraPods   []*v1.Pod
		seedCache   bool
		patchErr    error
		wantErr     string
		expectPatch bool
	}{
		{
			desc:        "nil node returns error",
			targetNode:  nil,
			pod:         nil,
			wantErr:     "nil Node",
			expectPatch: false,
		},
		{
			desc:        "node not in controller cache returns NoActiveBalloonPodError",
			targetNode:  node,
			pod:         basePod,
			seedCache:   false,
			wantErr:     ek_errors.NoActiveBalloonPodError.Error(),
			expectPatch: false,
		},
		{
			desc:        "multiple balloon pods on node returns error",
			targetNode:  node,
			pod:         basePod,
			extraPods:   []*v1.Pod{secondPod},
			seedCache:   true,
			wantErr:     "multiple balloon pods (2) found for node",
			expectPatch: false,
		},
		{
			desc:        "terminating balloon pod returns NoActiveBalloonPodError",
			targetNode:  node,
			pod:         terminatingPod,
			seedCache:   true,
			wantErr:     ek_errors.NoActiveBalloonPodError.Error(),
			expectPatch: false,
		},
		{
			desc:        "failed balloon pod returns NoActiveBalloonPodError",
			targetNode:  node,
			pod:         failedPod,
			seedCache:   true,
			wantErr:     ek_errors.NoActiveBalloonPodError.Error(),
			expectPatch: false,
		},
		{
			desc:        "patch api failure returns error",
			targetNode:  node,
			pod:         basePod,
			seedCache:   true,
			patchErr:    errors.New("apiserver unavailable"),
			wantErr:     "failed to in-place patch balloon pod",
			expectPatch: true,
		},
		{
			desc:       "patch api 409 conflict returns error",
			targetNode: node,
			pod:        basePod,
			seedCache:  true,
			patchErr: apierrors.NewConflict(
				schema.GroupResource{Resource: "pods"},
				basePod.Name,
				errors.New("object has been modified"),
			),
			wantErr:     "failed to in-place patch balloon pod",
			expectPatch: true,
		},
		{
			desc:        "already at desired size returns immediately without patch",
			targetNode:  node,
			pod:         podAlreadyResized,
			seedCache:   true,
			expectPatch: false,
		},
		{
			desc:        "pod already marked Infeasible returns error without patch",
			targetNode:  node,
			pod:         withResizeCondition(podAlreadyResized, v1.PodResizePending, v1.PodReasonInfeasible),
			seedCache:   true,
			wantErr:     "as Infeasible",
			expectPatch: false,
		},
		{
			desc:        "pod already marked Deferred returns error without patch",
			targetNode:  node,
			pod:         withResizeCondition(podAlreadyResized, v1.PodResizePending, v1.PodReasonDeferred),
			seedCache:   true,
			wantErr:     "as Deferred",
			expectPatch: false,
		},
		{
			desc:        "Guaranteed pod returns error without patch",
			targetNode:  node,
			pod:         withQoSClass(basePod, v1.PodQOSGuaranteed),
			seedCache:   true,
			wantErr:     "not eligible for in-place resize",
			expectPatch: false,
		},
		{
			desc:        "pod with limits but no QoS class set returns error without patch",
			targetNode:  node,
			pod:         withResourceLimits(t, basePod, resource.MustParse("1000m"), resource.MustParse("100Mi")),
			seedCache:   true,
			wantErr:     "not eligible for in-place resize",
			expectPatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			var initialObjects []runtime.Object
			if tt.pod != nil {
				initialObjects = append(initialObjects, tt.pod.DeepCopy())
			}
			for _, p := range tt.extraPods {
				initialObjects = append(initialObjects, p.DeepCopy())
			}
			fakeClient := fake.NewSimpleClientset(initialObjects...)

			if tt.patchErr != nil {
				fakeClient.PrependReactor("patch", "pods", func(_ client_testing.Action) (bool, runtime.Object, error) {
					return true, nil, tt.patchErr
				})
			}

			informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
			controller := newBalloonPodController(fakeClient, informerFactory)

			if tt.seedCache {
				if tt.pod != nil {
					controller.defaultOnAdd(tt.pod.DeepCopy())
				}
				for _, p := range tt.extraPods {
					controller.defaultOnAdd(p.DeepCopy())
				}
			}

			err := controller.ResizeBalloonPodInPlace(tt.targetNode, targetCpu, targetMem)

			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}

			verifyResizePatchAction(t, fakeClient.Actions(), tt.expectPatch, targetCpu, targetMem)
		})
	}
}

func TestBalloonPodController_ResizeBalloonPodInPlace_Events(t *testing.T) {
	t.Parallel()

	node := test.BuildTestNode("node1", 4000, 4000*1024)
	targetCpu := resource.MustParse("2000m")
	targetMem := resource.MustParse("200Mi")

	initialCpu := resource.MustParse("1000m")
	initialMem := resource.MustParse("100Mi")

	basePod := setupTestPod(t, node, initialCpu, initialMem)

	tests := []struct {
		desc       string
		targetCpu  resource.Quantity
		targetMem  resource.Quantity
		emitEvent  func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod)
		wantErr    error
		wantErrSub string
	}{
		{
			desc:      "kubelet allocates resources (scale up) -> resize succeeds",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				updated := buildPodWithAllocated(t, currentPod, targetCpu, targetMem)
				c.defaultOnUpdate(currentPod, updated)
			},
		},
		{
			desc:      "kubelet allocates resources (scale down) -> resize succeeds",
			targetCpu: resource.MustParse("500m"),
			targetMem: resource.MustParse("50Mi"),
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				downCpu := resource.MustParse("500m")
				downMem := resource.MustParse("50Mi")
				updated := buildPodWithAllocated(t, currentPod, downCpu, downMem)
				c.defaultOnUpdate(currentPod, updated)
			},
		},
		{
			desc:      "kubelet updates allocated resources while InProgress, then clears InProgress -> resize succeeds",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				inProgress := withResizeCondition(buildPodWithAllocated(t, currentPod, targetCpu, targetMem),
					v1.PodResizeInProgress, "")
				c.defaultOnUpdate(currentPod, inProgress)

				// Kubelet clears the condition once the resize is acknowledged.
				completed := buildPodWithAllocated(t, currentPod, targetCpu, targetMem)
				c.defaultOnUpdate(inProgress, completed)
			},
		},
		{
			desc:      "spurious update (e.g. annotations/labels) ignored until target resources allocated",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				spurious := currentPod.DeepCopy()
				if spurious.Annotations == nil {
					spurious.Annotations = make(map[string]string)
				}
				spurious.Annotations["cluster-autoscaler"] = "touched"
				c.defaultOnUpdate(currentPod, spurious)

				completed := buildPodWithAllocated(t, spurious, targetCpu, targetMem)
				c.defaultOnUpdate(spurious, completed)
			},
		},
		{
			desc:      "partial intermediate allocation does not resolve until full target reached",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				partialCpu := resource.MustParse("1500m")
				partial := withResizeCondition(buildPodWithAllocated(t, currentPod, partialCpu, initialMem),
					v1.PodResizeInProgress, "")
				c.defaultOnUpdate(currentPod, partial)

				completed := buildPodWithAllocated(t, currentPod, targetCpu, targetMem)
				c.defaultOnUpdate(partial, completed)
			},
		},
		{
			desc:      "kubelet updates allocated resources but remains InProgress -> times out",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				// Allocation already matches the target, so only the lingering
				// PodResizeInProgress condition keeps the resize unresolved.
				inProgress := withResizeCondition(buildPodWithAllocated(t, currentPod, targetCpu, targetMem),
					v1.PodResizeInProgress, "")
				c.defaultOnUpdate(currentPod, inProgress)
			},
			wantErr: ek_errors.ResizeTimeoutError,
		},
		{
			desc:      "kubelet sets PodResizePending/Deferred condition -> fails with rejection error",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				deferred := withResizeCondition(buildPodWithAllocated(t, currentPod, initialCpu, initialMem),
					v1.PodResizePending, v1.PodReasonDeferred)
				c.defaultOnUpdate(currentPod, deferred)
			},
			wantErrSub: "as Deferred",
		},
		{
			desc:      "kubelet sets PodResizePending/Infeasible condition -> fails with rejection error",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				infeasible := withResizeCondition(buildPodWithAllocated(t, currentPod, initialCpu, initialMem),
					v1.PodResizePending, v1.PodReasonInfeasible)
				c.defaultOnUpdate(currentPod, infeasible)
			},
			wantErrSub: "as Infeasible",
		},
		{
			desc:      "kubelet sets PodResizeInProgress/Error condition -> fails fast instead of timing out",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				errored := withResizeCondition(buildPodWithAllocated(t, currentPod, initialCpu, initialMem),
					v1.PodResizeInProgress, v1.PodReasonError)
				c.defaultOnUpdate(currentPod, errored)
			},
			wantErrSub: "as Error",
		},
		{
			desc:      "pod deleted while waiting -> fails with deletion error",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: func(t *testing.T, c *balloonPodControllerImpl, currentPod *v1.Pod) {
				c.defaultOnDelete(currentPod)
			},
			wantErrSub: "was deleted during resize",
		},
		{
			desc:      "no event arrives -> times out",
			targetCpu: targetCpu,
			targetMem: targetMem,
			emitEvent: nil,
			wantErr:   ek_errors.ResizeTimeoutError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				fakeClient := fake.NewSimpleClientset(basePod.DeepCopy())
				informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

				controller := newBalloonPodController(fakeClient, informerFactory)
				controller.defaultOnAdd(basePod.DeepCopy())
				controller.podResizeTimeout = 5 * time.Second

				errCh := make(chan error, 1)
				go func() {
					errCh <- controller.ResizeBalloonPodInPlace(node, tt.targetCpu, tt.targetMem)
				}()

				synctest.Wait()

				if tt.emitEvent != nil {
					tt.emitEvent(t, controller, basePod.DeepCopy())
					synctest.Wait()
				}

				resizeErr := <-errCh

				switch {
				case tt.wantErr != nil:
					assert.ErrorIs(t, resizeErr, tt.wantErr)
				case tt.wantErrSub != "":
					assert.ErrorContains(t, resizeErr, tt.wantErrSub)
				default:
					assert.NoError(t, resizeErr)
				}
			})
		})
	}
}

func TestBalloonPodController_ResizeBalloonPodInPlace_ConcurrentCallsFail(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		node := test.BuildTestNode("node1", 4000, 4000*1024)
		targetCpu := resource.MustParse("2000m")
		targetMem := resource.MustParse("200Mi")

		basePod := setupTestPod(t, node, resource.MustParse("1000m"), resource.MustParse("100Mi"))

		fakeClient := fake.NewSimpleClientset(basePod.DeepCopy())
		informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

		bPController := newBalloonPodController(fakeClient, informerFactory)
		bPController.podResizeTimeout = 5 * time.Second
		bPController.defaultOnAdd(basePod.DeepCopy())

		firstErrCh := make(chan error, 1)
		go func() {
			firstErrCh <- bPController.ResizeBalloonPodInPlace(node, targetCpu, targetMem)
		}()

		synctest.Wait()

		secondErr := bPController.ResizeBalloonPodInPlace(node, targetCpu, targetMem)
		assert.ErrorIs(t, secondErr, ek_errors.ConcurrentResizeError)

		updatedPod := buildPodWithAllocated(t, basePod, targetCpu, targetMem)
		bPController.defaultOnUpdate(basePod.DeepCopy(), updatedPod)
		synctest.Wait()

		assert.NoError(t, <-firstErrCh)
	})
}

// TestBalloonPodController_ResizeBalloonPodInPlace_PreservesQoS pins the invariant behind the production failure: the
// resize patch only sets Requests, so issuing it against a balloon pod that still carries Limits would move the pod from
// Guaranteed to Burstable and the API server rejects it with "Pod QOS Class may not change as a result of resizing".
//
// The reactor stands in for that API server validation, which the fake client set does not perform. If the Guaranteed
// pre-check in ResizeBalloonPodInPlace is ever removed, the Guaranteed case below starts issuing the patch and fails
// with the real rejection instead of silently passing.
func TestBalloonPodController_ResizeBalloonPodInPlace_PreservesQoS(t *testing.T) {
	t.Parallel()

	node := test.BuildTestNode("node1", 4000, 4000*1024)
	initialCpu := resource.MustParse("1000m")
	initialMem := resource.MustParse("100Mi")
	targetCpu := resource.MustParse("2000m")
	targetMem := resource.MustParse("200Mi")

	burstablePod := setupTestPod(t, node, initialCpu, initialMem)
	guaranteedPod := withQoSClass(withResourceLimits(t, burstablePod, initialCpu, initialMem), v1.PodQOSGuaranteed)

	// Returned once the patch has cleared QoS validation, so the call fails fast instead of waiting for a Kubelet
	// acknowledgement that no informer is going to deliver in this test.
	errPatchAccepted := errors.New("patch accepted by fake apiserver")

	tests := []struct {
		desc      string
		pod       *v1.Pod
		wantPatch bool
		wantErr   string
	}{
		{
			desc:      "burstable balloon pod is patched and keeps its QoS class",
			pod:       burstablePod,
			wantPatch: true,
			wantErr:   errPatchAccepted.Error(),
		},
		{
			desc:      "guaranteed balloon pod is never patched",
			pod:       guaranteedPod,
			wantPatch: false,
			wantErr:   ek_errors.IncompatibleQoSError.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			livePod := tt.pod.DeepCopy()
			fakeClient := fake.NewSimpleClientset(livePod)

			var patched bool
			fakeClient.PrependReactor("patch", "pods", func(action client_testing.Action) (bool, runtime.Object, error) {
				patched = true
				if action.GetSubresource() != "resize" {
					return true, nil, fmt.Errorf("unexpected patch on subresource %q", action.GetSubresource())
				}
				// The patch only ever sets Requests, so a pod that carries Limits would change QoS class.
				if c := getBalloonContainerSpec(livePod); c != nil && len(c.Resources.Limits) > 0 {
					return true, nil, fmt.Errorf("Pod %q is invalid: spec: Invalid value: %q: Pod QOS Class may not change as a result of resizing",
						livePod.Name, v1.PodQOSGuaranteed)
				}
				return true, nil, errPatchAccepted
			})

			informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
			controller := newBalloonPodController(fakeClient, informerFactory)
			controller.defaultOnAdd(livePod.DeepCopy())

			err := controller.ResizeBalloonPodInPlace(node, targetCpu, targetMem)

			assert.ErrorContains(t, err, tt.wantErr)
			assert.NotContains(t, fmt.Sprint(err), "QOS Class may not change",
				"resize patch must never be issued in a way that changes the pod QoS class")
			assert.Equal(t, tt.wantPatch, patched, "unexpected patch behaviour")
		})
	}
}

// TestBalloonPodController_ResizeBalloonPodInPlace_VerifiesFailedPatch tests that a failed
// patch whose write actually landed does not trigger unnecessary recreation fallback.
func TestBalloonPodController_ResizeBalloonPodInPlace_VerifiesFailedPatch(t *testing.T) {
	t.Parallel()

	node := test.BuildTestNode("node1", 4000, 4000*1024)
	initialCpu := resource.MustParse("1000m")
	initialMem := resource.MustParse("100Mi")
	targetCpu := resource.MustParse("2000m")
	targetMem := resource.MustParse("200Mi")

	cachedPod := setupTestPod(t, node, initialCpu, initialMem)
	resizedPod := setupTestPod(t, node, targetCpu, targetMem)
	// Same name, different identity: the pod was recreated by something else while we patched.
	replacedPod := setupTestPod(t, node, targetCpu, targetMem)
	replacedPod.UID = "pod-uid-node1-replacement"

	tests := []struct {
		desc string
		// livePod is what reading the pod back from the apiserver returns after the failed patch.
		livePod *v1.Pod
		getErr  error
		// wantFallback is true when the controller must surface the patch error so that the
		// caller falls back to recreating the balloon pod.
		wantFallback bool
	}{
		{
			desc:         "patch landed despite the error, so keep waiting for Kubelet",
			livePod:      resizedPod,
			wantFallback: false,
		},
		{
			desc:         "patch did not land, so fall back to recreation",
			livePod:      cachedPod,
			wantFallback: true,
		},
		{
			desc:         "pod was replaced while patching, so fall back to recreation",
			livePod:      replacedPod,
			wantFallback: true,
		},
		{
			desc:         "verification read failed, so fall back to recreation",
			livePod:      cachedPod,
			getErr:       errors.New("apiserver unavailable"),
			wantFallback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			fakeClient := fake.NewSimpleClientset(tt.livePod.DeepCopy())
			fakeClient.PrependReactor("patch", "pods", func(_ client_testing.Action) (bool, runtime.Object, error) {
				return true, nil, context.DeadlineExceeded
			})
			gets := 0
			fakeClient.PrependReactor("get", "pods", func(_ client_testing.Action) (bool, runtime.Object, error) {
				gets++
				if tt.getErr != nil {
					return true, nil, tt.getErr
				}
				return true, tt.livePod.DeepCopy(), nil
			})

			controller := newBalloonPodController(fakeClient, informers.NewSharedInformerFactory(fakeClient, 0))
			// No informer runs in this test, so a resize that proceeds to the wait can only
			// end in a timeout. Keep that short so the assertions distinguish "kept waiting"
			// from "fell back" without sitting out the full ipprKubeletResizeTimeout.
			controller.podResizeTimeout = 50 * time.Millisecond
			controller.defaultOnAdd(cachedPod.DeepCopy())

			err := controller.ResizeBalloonPodInPlace(node, targetCpu, targetMem)

			assert.Equal(t, 1, gets, "a failed patch must be verified with exactly one read")
			if tt.wantFallback {
				assert.ErrorContains(t, err, ek_errors.ResizePatchError.Error(),
					"caller must see a patch error so that it recreates the pod")
				return
			}
			assert.ErrorContains(t, err, ek_errors.ResizeTimeoutError.Error(),
				"controller must go on to wait for Kubelet instead of reporting a patch failure")
			assert.NotContains(t, fmt.Sprint(err), ek_errors.ResizePatchError.Error(),
				"a patch that landed must never be reported as a patch failure")
		})
	}
}

// TestBalloonPodController_ResizeBalloonPodInPlace_SucceedingPatchIsNotVerified guards the cost of
// the verification read: it exists only for the failure path and must not add an apiserver round
// trip to every in-place resize.
func TestBalloonPodController_ResizeBalloonPodInPlace_SucceedingPatchIsNotVerified(t *testing.T) {
	t.Parallel()

	node := test.BuildTestNode("node1", 4000, 4000*1024)
	targetCpu := resource.MustParse("2000m")
	targetMem := resource.MustParse("200Mi")

	cachedPod := setupTestPod(t, node, resource.MustParse("1000m"), resource.MustParse("100Mi"))

	fakeClient := fake.NewSimpleClientset(cachedPod.DeepCopy())
	gets := 0
	fakeClient.PrependReactor("get", "pods", func(_ client_testing.Action) (bool, runtime.Object, error) {
		gets++
		return true, cachedPod.DeepCopy(), nil
	})

	controller := newBalloonPodController(fakeClient, informers.NewSharedInformerFactory(fakeClient, 0))
	controller.podResizeTimeout = 50 * time.Millisecond
	controller.defaultOnAdd(cachedPod.DeepCopy())

	err := controller.ResizeBalloonPodInPlace(node, targetCpu, targetMem)

	assert.ErrorContains(t, err, ek_errors.ResizeTimeoutError.Error())
	assert.Zero(t, gets, "a successful patch must not trigger a verification read")
}
