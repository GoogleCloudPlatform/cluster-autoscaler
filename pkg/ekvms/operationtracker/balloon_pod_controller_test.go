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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
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
