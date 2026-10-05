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

package capacitybuffers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	bufferslisters "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/listers/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	cbclient "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/client"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	capacitybufferpodlister "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	gpuutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	tpuutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/tpu"
)

type MockMetrics struct {
	mock.Mock
}

func (m *MockMetrics) UpdateCapacityBufferPods(counts map[metrics.CapacityBufferPodsKey]int) {
	m.Called(counts)
}

func (m *MockMetrics) UpdateCapacityBuffersNumber(countsByType map[string]int) {
	m.Called(countsByType)
}

func (m *MockMetrics) UpdateCapacityBufferResources(compute map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources, accelerators map[metrics.CapacityBufferAcceleratorKey]int64) {
	m.Called(compute, accelerators)
}

type MockCapacityBufferLister struct {
	mock.Mock
}

func (m *MockCapacityBufferLister) List(selector labels.Selector) (ret []*v1beta1.CapacityBuffer, err error) {
	args := m.Called(selector)
	return args.Get(0).([]*v1beta1.CapacityBuffer), args.Error(1)
}

func (m *MockCapacityBufferLister) CapacityBuffers(namespace string) bufferslisters.CapacityBufferNamespaceLister {
	return nil
}

func TestMetricProcessor_Process(t *testing.T) {
	strategy1 := "strategy1"
	strategy2 := "strategy2"

	cb1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb1", Namespace: "ns1", Name: "buf1"},
		Status:     v1beta1.CapacityBufferStatus{ProvisioningStrategy: &strategy1},
	}
	cb2 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb2", Namespace: "ns2", Name: "buf2"},
		Status:     v1beta1.CapacityBufferStatus{ProvisioningStrategy: &strategy2},
	}
	cb3 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb3", Namespace: "ns1", Name: "buf3"},
		Status:     v1beta1.CapacityBufferStatus{ProvisioningStrategy: &strategy1},
	}
	capacityBuffers := []*v1beta1.CapacityBuffer{cb1, cb2, cb3}

	registry := fakepods.NewRegistry(nil)
	newBufferPod := func(cb *v1beta1.CapacityBuffer, name string, cpu, mem, storage int64, opts ...func(*apiv1.Pod)) *apiv1.Pod {
		pod := createCapacityBufferPodWithResources(name, cpu, mem, storage, opts...)
		registry.SetCapacityBuffer(pod.UID, cb)
		return pod
	}

	nodeGpuConfigs := map[string]*cloudprovider.GpuConfig{
		"node-ready":    {Type: "nvidia-tesla-t4", ExtendedResourceName: gpuutils.ResourceNvidiaGPU},
		"node-upcoming": {Type: "tpu-v4-podslice", ExtendedResourceName: tpu.ResourceGoogleTPU},
	}
	nodeInfos := []*framework.NodeInfo{
		framework.NewTestNodeInfo(
			test.BuildTestNode("node-ready", 1000, 1000),
			newBufferPod(cb1, "pod1-buf1", 100, 100, 200, withResourceRequest(gpuutils.ResourceNvidiaGPU, 2)),
			newBufferPod(cb3, "pod1-buf3", 300, 400, 500, withResourceRequest(gpuutils.ResourceNvidiaGPU, 4)),
			test.BuildTestPod("regular-pod", 100, 100),
			createCapacityBufferPod("unregistered-scheduled-pod"),
		),
		framework.NewTestNodeInfo(
			test.BuildTestNode("node-upcoming", 1000, 1000, asUpcoming),
			newBufferPod(cb1, "pod2-buf1", 100, 100, 200, withResourceRequest(tpu.ResourceGoogleTPU, 4)),
			newBufferPod(cb1, "pod3-buf1", 100, 100, 200),
		),
	}
	unschedulablePods := []*apiv1.Pod{
		newBufferPod(cb2, "pod1-buf2", 500, 1000, 2000, withResourceRequest(gpuutils.ResourceNvidiaGPU, 1)),
		createCapacityBufferPod("unregistered-unschedulable-pod"),
	}

	expectedPodCounts := map[metrics.CapacityBufferPodsKey]int{
		{ProvisioningStrategy: strategy1, State: metrics.CapacityBufferPodStateReady}:        2,
		{ProvisioningStrategy: strategy1, State: metrics.CapacityBufferPodStateProvisioning}: 2,
		{ProvisioningStrategy: strategy2, State: metrics.CapacityBufferPodStateNotReady}:     1,
	}
	expectedCbCounts := map[string]int{
		strategy1: 2,
		strategy2: 1,
	}

	strategyKey := func(strategy string, state metrics.CapacityBufferPodState) metrics.CapacityBufferResourceKey {
		return metrics.CapacityBufferResourceKey{
			ProvisioningStrategy: strategy,
			State:                state,
		}
	}
	strategyAccelKey := func(strategy string, state metrics.CapacityBufferPodState, resName, model string) metrics.CapacityBufferAcceleratorKey {
		return metrics.CapacityBufferAcceleratorKey{
			CapacityBufferResourceKey: strategyKey(strategy, state),
			ResourceName:              resName,
			Model:                     model,
		}
	}
	perBufferKey := func(cb *v1beta1.CapacityBuffer, state metrics.CapacityBufferPodState) metrics.CapacityBufferResourceKey {
		return metrics.CapacityBufferResourceKey{
			EntityNamespace:      cb.Namespace,
			EntityName:           cb.Name,
			EntityUID:            string(cb.UID),
			ProvisioningStrategy: *cb.Status.ProvisioningStrategy,
			State:                state,
		}
	}
	perBufferAccelKey := func(cb *v1beta1.CapacityBuffer, state metrics.CapacityBufferPodState, resName, model string) metrics.CapacityBufferAcceleratorKey {
		return metrics.CapacityBufferAcceleratorKey{
			CapacityBufferResourceKey: perBufferKey(cb, state),
			ResourceName:              resName,
			Model:                     model,
		}
	}
	computeResources := func(cpuCores float64, memBytes, storageBytes int64) metrics.CapacityBufferResources {
		return metrics.CapacityBufferResources{
			CpuCores:              cpuCores,
			MemoryBytes:           memBytes,
			EphemeralStorageBytes: storageBytes,
		}
	}

	testCases := []struct {
		name                       string
		perBufferMetrics           bool
		perAcceleratorModelMetrics bool
		expectedCompute            map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources
		expectedAccelerators       map[metrics.CapacityBufferAcceleratorKey]int64
	}{
		{
			name: "per-buffer and per-accelerator-model metrics disabled",
			expectedCompute: map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources{
				strategyKey(strategy1, metrics.CapacityBufferPodStateReady):        computeResources(0.4, 500, 700),
				strategyKey(strategy1, metrics.CapacityBufferPodStateProvisioning): computeResources(0.2, 200, 400),
				strategyKey(strategy2, metrics.CapacityBufferPodStateNotReady):     computeResources(0.5, 1000, 2000),
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{
				strategyAccelKey(strategy1, metrics.CapacityBufferPodStateReady, gpuutils.ResourceNvidiaGPU, ""):    6,
				strategyAccelKey(strategy1, metrics.CapacityBufferPodStateProvisioning, tpu.ResourceGoogleTPU, ""):  4,
				strategyAccelKey(strategy2, metrics.CapacityBufferPodStateNotReady, gpuutils.ResourceNvidiaGPU, ""): 1,
			},
		},
		{
			name:             "per-buffer metrics enabled",
			perBufferMetrics: true,
			expectedCompute: map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources{
				perBufferKey(cb1, metrics.CapacityBufferPodStateReady):        computeResources(0.1, 100, 200),
				perBufferKey(cb3, metrics.CapacityBufferPodStateReady):        computeResources(0.3, 400, 500),
				perBufferKey(cb1, metrics.CapacityBufferPodStateProvisioning): computeResources(0.2, 200, 400),
				perBufferKey(cb2, metrics.CapacityBufferPodStateNotReady):     computeResources(0.5, 1000, 2000),
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{
				perBufferAccelKey(cb1, metrics.CapacityBufferPodStateReady, gpuutils.ResourceNvidiaGPU, ""):    2,
				perBufferAccelKey(cb3, metrics.CapacityBufferPodStateReady, gpuutils.ResourceNvidiaGPU, ""):    4,
				perBufferAccelKey(cb1, metrics.CapacityBufferPodStateProvisioning, tpu.ResourceGoogleTPU, ""):  4,
				perBufferAccelKey(cb2, metrics.CapacityBufferPodStateNotReady, gpuutils.ResourceNvidiaGPU, ""): 1,
			},
		},
		{
			name:                       "per-accelerator-model metrics enabled",
			perAcceleratorModelMetrics: true,
			expectedCompute: map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources{
				strategyKey(strategy1, metrics.CapacityBufferPodStateReady):        computeResources(0.4, 500, 700),
				strategyKey(strategy1, metrics.CapacityBufferPodStateProvisioning): computeResources(0.2, 200, 400),
				strategyKey(strategy2, metrics.CapacityBufferPodStateNotReady):     computeResources(0.5, 1000, 2000),
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{
				strategyAccelKey(strategy1, metrics.CapacityBufferPodStateReady, gpuutils.ResourceNvidiaGPU, "nvidia-tesla-t4"):   6,
				strategyAccelKey(strategy1, metrics.CapacityBufferPodStateProvisioning, tpu.ResourceGoogleTPU, "tpu-v4-podslice"): 4,
				// Unschedulable pods aren't placed on any node, so their accelerator model is unknown.
				strategyAccelKey(strategy2, metrics.CapacityBufferPodStateNotReady, gpuutils.ResourceNvidiaGPU, ""): 1,
			},
		},
		{
			name:                       "per-buffer and per-accelerator-model metrics enabled",
			perBufferMetrics:           true,
			perAcceleratorModelMetrics: true,
			expectedCompute: map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources{
				perBufferKey(cb1, metrics.CapacityBufferPodStateReady):        computeResources(0.1, 100, 200),
				perBufferKey(cb3, metrics.CapacityBufferPodStateReady):        computeResources(0.3, 400, 500),
				perBufferKey(cb1, metrics.CapacityBufferPodStateProvisioning): computeResources(0.2, 200, 400),
				perBufferKey(cb2, metrics.CapacityBufferPodStateNotReady):     computeResources(0.5, 1000, 2000),
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{
				perBufferAccelKey(cb1, metrics.CapacityBufferPodStateReady, gpuutils.ResourceNvidiaGPU, "nvidia-tesla-t4"):   2,
				perBufferAccelKey(cb3, metrics.CapacityBufferPodStateReady, gpuutils.ResourceNvidiaGPU, "nvidia-tesla-t4"):   4,
				perBufferAccelKey(cb1, metrics.CapacityBufferPodStateProvisioning, tpu.ResourceGoogleTPU, "tpu-v4-podslice"): 4,
				// Unschedulable pods aren't placed on any node, so their accelerator model is unknown.
				perBufferAccelKey(cb2, metrics.CapacityBufferPodStateNotReady, gpuutils.ResourceNvidiaGPU, ""): 1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, nodeInfo := range nodeInfos {
				assert.NoError(t, snapshot.AddNodeInfo(nodeInfo))
			}

			ctx := &ca_context.AutoscalingContext{
				CloudProvider: testprovider.NewTestCloudProviderBuilder().WithNodeGpuConfig(func(node *apiv1.Node) *cloudprovider.GpuConfig {
					return nodeGpuConfigs[node.Name]
				}).Build(),
				ClusterSnapshot: snapshot,
			}

			mockMetrics := &MockMetrics{}
			mockMetrics.On("UpdateCapacityBufferPods", expectedPodCounts).Return()
			mockMetrics.On("UpdateCapacityBuffersNumber", expectedCbCounts).Return()
			mockMetrics.On("UpdateCapacityBufferResources", tc.expectedCompute, tc.expectedAccelerators).Return()

			mockLister := &MockCapacityBufferLister{}
			mockLister.On("List", labels.Everything()).Return(capacityBuffers, nil)

			client, err := cbclient.NewCapacityBufferClient(nil, nil, mockLister, nil, nil, nil, nil, nil, nil, nil, nil)
			assert.NoError(t, err)

			boolFlags := map[string]bool{
				experiments.CapacityBuffersPerBufferMetrics:           tc.perBufferMetrics,
				experiments.CapacityBuffersPerAcceleratorModelMetrics: tc.perAcceleratorModelMetrics,
			}
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{})

			processor := NewMetricProcessor(client, registry, mockMetrics, experimentsManager)
			err = processor.ProcessMetrics(ctx, unschedulablePods)
			assert.NoError(t, err)

			mockMetrics.AssertExpectations(t)
			mockLister.AssertExpectations(t)
		})
	}
}

func createCapacityBufferPod(name string, opts ...func(*apiv1.Pod)) *apiv1.Pod {
	return createCapacityBufferPodWithResources(name, 100, 100, 0, opts...)
}

func createCapacityBufferPodWithResources(name string, cpu, mem, ephemeralStorage int64, opts ...func(*apiv1.Pod)) *apiv1.Pod {
	p := test.BuildTestPodWithEphemeralStorage(name, cpu, mem, ephemeralStorage)
	if p.Annotations == nil {
		p.Annotations = make(map[string]string)
	}
	p.Annotations[capacitybufferpodlister.CapacityBufferFakePodAnnotationKey] = capacitybufferpodlister.CapacityBufferFakePodAnnotationValue
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func withResourceRequest(resName apiv1.ResourceName, qty int64) func(*apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.Containers[0].Resources.Requests[resName] = *resource.NewQuantity(qty, resource.DecimalSI)
	}
}

func buildContainer(name string, cpu, mem, ephemeralStorage int64, extraRequests map[apiv1.ResourceName]int64) apiv1.Container {
	requests := apiv1.ResourceList{
		apiv1.ResourceCPU:              *resource.NewMilliQuantity(cpu, resource.DecimalSI),
		apiv1.ResourceMemory:           *resource.NewQuantity(mem, resource.DecimalSI),
		apiv1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStorage, resource.DecimalSI),
	}
	for resName, qty := range extraRequests {
		requests[resName] = *resource.NewQuantity(qty, resource.DecimalSI)
	}
	return apiv1.Container{
		Name:      name,
		Resources: apiv1.ResourceRequirements{Requests: requests},
	}
}

func withContainer(name string, cpu, mem, ephemeralStorage int64, extraRequests map[apiv1.ResourceName]int64) func(*apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.Containers = append(p.Spec.Containers, buildContainer(name, cpu, mem, ephemeralStorage, extraRequests))
	}
}

func withInitContainer(name string, cpu, mem, ephemeralStorage int64, extraRequests map[apiv1.ResourceName]int64) func(*apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.InitContainers = append(p.Spec.InitContainers, buildContainer(name, cpu, mem, ephemeralStorage, extraRequests))
	}
}

func asUpcoming(node *apiv1.Node) {
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[annotations.NodeUpcomingAnnotation] = "true"
}

func TestBufferKey(t *testing.T) {
	strategy := "test-strategy"
	cb := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb1"},
		Status: v1beta1.CapacityBufferStatus{
			ProvisioningStrategy: &strategy,
		},
	}
	cbNoStrategy := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb2"},
	}

	pod := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod-with-strategy"}}
	podNoStrategy := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod-no-strategy"}}
	podUnknown := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "unknown"}}

	registry := fakepods.NewRegistry(nil)
	registry.SetCapacityBuffer(pod.UID, cb)
	registry.SetCapacityBuffer(podNoStrategy.UID, cbNoStrategy)

	nodeReady := test.BuildTestNode("node-ready", 1000, 1000)
	nodeUpcoming := test.BuildTestNode("node-upcoming", 1000, 1000, asUpcoming)

	testCases := []struct {
		name     string
		pod      *apiv1.Pod
		node     *apiv1.Node
		expected *metrics.CapacityBufferPodsKey
	}{
		{
			name:     "pod not in registry",
			pod:      podUnknown,
			node:     nodeReady,
			expected: nil,
		},
		{
			name: "node is nil (NotReady state)",
			pod:  pod,
			node: nil, // Represents an unschedulable pod.
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateNotReady,
			},
		},
		{
			name: "node is upcoming (Provisioning state)",
			pod:  pod,
			node: nodeUpcoming,
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateProvisioning,
			},
		},
		{
			name: "node is ready (Ready state)",
			pod:  pod,
			node: nodeReady,
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateReady,
			},
		},
		{
			name: "no provisioning strategy (unknown strategy)",
			pod:  podNoStrategy,
			node: nodeReady,
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: unknownProvisioningStrategy,
				State:                metrics.CapacityBufferPodStateReady,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := bufferKey(tc.pod, tc.node, registry)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestAccumulatePodResources(t *testing.T) {
	baseKey := metrics.CapacityBufferResourceKey{
		EntityNamespace:      "ns1",
		EntityName:           "buf1",
		ProvisioningStrategy: "strategy1",
		State:                metrics.CapacityBufferPodStateReady,
	}

	acceleratorKey := func(resName, model string) metrics.CapacityBufferAcceleratorKey {
		return metrics.CapacityBufferAcceleratorKey{
			CapacityBufferResourceKey: baseKey,
			ResourceName:              resName,
			Model:                     model,
		}
	}

	testCases := []struct {
		name                 string
		pods                 []*apiv1.Pod
		accelModel           string
		expectedCompute      map[metrics.CapacityBufferResourceKey]resourceAccumulator
		expectedAccelerators map[metrics.CapacityBufferAcceleratorKey]int64
	}{
		{
			name: "accumulates CPU, memory, and ephemeral storage across multiple pods",
			pods: []*apiv1.Pod{
				createCapacityBufferPodWithResources("pod1", 250, 1024, 2048),
				createCapacityBufferPodWithResources("pod2", 750, 3072, 4096),
			},
			accelModel: "nvidia-tesla-t4",
			expectedCompute: map[metrics.CapacityBufferResourceKey]resourceAccumulator{
				baseKey: {
					cpuMilli:              1000,
					memoryBytes:           4096,
					ephemeralStorageBytes: 6144,
				},
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{},
		},
		{
			name: "accumulates all supported GPU vendor resource types and TPUs",
			pods: []*apiv1.Pod{
				createCapacityBufferPodWithResources("pod-gpu-1", 100, 100, 0,
					withResourceRequest(gpuutils.ResourceNvidiaGPU, 2),
					withResourceRequest(gpuutils.ResourceAMDGPU, 4),
					withResourceRequest(gpuutils.ResourceIntelGaudi, 1),
				),
				createCapacityBufferPodWithResources("pod-gpu-2", 200, 200, 0,
					withResourceRequest(gpuutils.ResourceNvidiaGPU, 3),
					withResourceRequest(gpuutils.ResourceIntelGPU, 2),
					withResourceRequest(gpuutils.ResourceDirectX, 1),
				),
				createCapacityBufferPodWithResources("pod-tpu", 300, 300, 0,
					withResourceRequest(tpu.ResourceGoogleTPU, 4),
					withResourceRequest(tpuutils.ResourceTPUPrefix+"v3-8", 8),
				),
			},
			accelModel: "",
			expectedCompute: map[metrics.CapacityBufferResourceKey]resourceAccumulator{
				baseKey: {
					cpuMilli:              600,
					memoryBytes:           600,
					ephemeralStorageBytes: 0,
				},
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{
				acceleratorKey(gpuutils.ResourceNvidiaGPU, ""):        5,
				acceleratorKey(gpuutils.ResourceAMDGPU, ""):           4,
				acceleratorKey(gpuutils.ResourceIntelGaudi, ""):       1,
				acceleratorKey(gpuutils.ResourceIntelGPU, ""):         2,
				acceleratorKey(gpuutils.ResourceDirectX, ""):          1,
				acceleratorKey(tpu.ResourceGoogleTPU, ""):             4,
				acceleratorKey(tpuutils.ResourceTPUPrefix+"v3-8", ""): 8,
			},
		},
		{
			name: "ignores non-accelerator extended resources and zero-quantity accelerator requests",
			pods: []*apiv1.Pod{
				createCapacityBufferPodWithResources("pod1", 100, 200, 300,
					withResourceRequest(gpuutils.ResourceNvidiaGPU, 0),
					withResourceRequest(tpu.ResourceGoogleTPU, 0),
					withResourceRequest("example.com/dongle", 5),
					withResourceRequest("hugepages-2Mi", 1024),
				),
			},
			accelModel: "nvidia-tesla-t4",
			expectedCompute: map[metrics.CapacityBufferResourceKey]resourceAccumulator{
				baseKey: {
					cpuMilli:              100,
					memoryBytes:           200,
					ephemeralStorageBytes: 300,
				},
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{},
		},
		{
			name:       "accumulates across multiple containers and init containers in a pod",
			accelModel: "nvidia-tesla-a100",
			pods: []*apiv1.Pod{
				createCapacityBufferPodWithResources("multi-container-pod", 200, 500, 300,
					withResourceRequest(gpuutils.ResourceNvidiaGPU, 1),
					withContainer("c2", 200, 500, 400, map[apiv1.ResourceName]int64{gpuutils.ResourceNvidiaGPU: 2}),
					withInitContainer("init-c1", 500, 2000, 100, map[apiv1.ResourceName]int64{gpuutils.ResourceNvidiaGPU: 4}),
				),
			},
			expectedCompute: map[metrics.CapacityBufferResourceKey]resourceAccumulator{
				baseKey: {
					// Init container CPU (500m) > sum of app containers (400m); init memory (2000) > sum of app containers (1000); app ephemeral storage sum (700) > init (100).
					cpuMilli:              500,
					memoryBytes:           2000,
					ephemeralStorageBytes: 700,
				},
			},
			expectedAccelerators: map[metrics.CapacityBufferAcceleratorKey]int64{
				// Init container GPU (4) > sum of app containers (3).
				acceleratorKey(gpuutils.ResourceNvidiaGPU, "nvidia-tesla-a100"): 4,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			computeTotals := map[metrics.CapacityBufferResourceKey]resourceAccumulator{}
			acceleratorTotals := map[metrics.CapacityBufferAcceleratorKey]int64{}
			for _, pod := range tc.pods {
				accumulatePodResources(pod, baseKey, tc.accelModel, computeTotals, acceleratorTotals)
			}
			assert.Equal(t, tc.expectedCompute, computeTotals)
			assert.Equal(t, tc.expectedAccelerators, acceleratorTotals)
		})
	}
}

func TestBufferResourceKey(t *testing.T) {
	strategy := "test-strategy"
	cb := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb1", Namespace: "ns1", Name: "buf1"},
		Status: v1beta1.CapacityBufferStatus{
			ProvisioningStrategy: &strategy,
		},
	}

	pod := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod-with-strategy"}}
	podUnknown := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "unknown"}}

	registry := fakepods.NewRegistry(nil)
	registry.SetCapacityBuffer(pod.UID, cb)

	podKey := metrics.CapacityBufferPodsKey{
		ProvisioningStrategy: strategy,
		State:                metrics.CapacityBufferPodStateReady,
	}

	testCases := []struct {
		name             string
		pod              *apiv1.Pod
		podKey           metrics.CapacityBufferPodsKey
		perBufferMetrics bool
		expected         metrics.CapacityBufferResourceKey
	}{
		{
			name:             "per-buffer metrics enabled",
			pod:              pod,
			podKey:           podKey,
			perBufferMetrics: true,
			expected: metrics.CapacityBufferResourceKey{
				EntityNamespace:      "ns1",
				EntityName:           "buf1",
				EntityUID:            "cb1",
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateReady,
			},
		},
		{
			name:             "per-buffer metrics disabled",
			pod:              pod,
			podKey:           podKey,
			perBufferMetrics: false,
			expected: metrics.CapacityBufferResourceKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateReady,
			},
		},
		{
			name:             "pod not in registry with per-buffer metrics enabled",
			pod:              podUnknown,
			podKey:           podKey,
			perBufferMetrics: true,
			expected: metrics.CapacityBufferResourceKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateReady,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := bufferResourceKey(tc.pod, tc.podKey, registry, tc.perBufferMetrics)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestNodeAcceleratorModel(t *testing.T) {
	gpuNode := test.BuildTestNode("gpu-node", 1000, 1000)
	tpuNode := test.BuildTestNode("tpu-node", 1000, 1000)
	cpuNode := test.BuildTestNode("cpu-node", 1000, 1000)

	provider := testprovider.NewTestCloudProviderBuilder().WithNodeGpuConfig(func(node *apiv1.Node) *cloudprovider.GpuConfig {
		switch node.Name {
		case gpuNode.Name:
			return &cloudprovider.GpuConfig{Type: "nvidia-tesla-t4", ExtendedResourceName: gpuutils.ResourceNvidiaGPU}
		case tpuNode.Name:
			return &cloudprovider.GpuConfig{Type: "tpu-v4-podslice", ExtendedResourceName: tpu.ResourceGoogleTPU}
		default:
			return nil
		}
	}).Build()

	testCases := []struct {
		name     string
		provider cloudprovider.CloudProvider
		node     *apiv1.Node
		expected string
	}{
		{
			name:     "node with GPU",
			provider: provider,
			node:     gpuNode,
			expected: "nvidia-tesla-t4",
		},
		{
			name:     "node with TPU",
			provider: provider,
			node:     tpuNode,
			expected: "tpu-v4-podslice",
		},
		{
			name:     "node without accelerator",
			provider: provider,
			node:     cpuNode,
			expected: "",
		},
		{
			name:     "nil node",
			provider: provider,
			node:     nil,
			expected: "",
		},
		{
			name:     "nil cloud provider",
			provider: nil,
			node:     gpuNode,
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, nodeAcceleratorModel(tc.provider, tc.node))
		})
	}
}

func TestIsUpcomingNode(t *testing.T) {
	testCases := []struct {
		name     string
		node     *apiv1.Node
		expected bool
	}{
		{
			name:     "upcoming node",
			node:     test.BuildTestNode("node-upcoming", 1000, 1000, asUpcoming),
			expected: true,
		},
		{
			name:     "ready node",
			node:     test.BuildTestNode("node-ready", 1000, 1000),
			expected: false,
		},
		{
			name:     "node with nil annotations",
			node:     &apiv1.Node{},
			expected: false,
		},
		{
			name:     "nil node",
			node:     nil,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isUpcomingNode(tc.node))
		})
	}
}
