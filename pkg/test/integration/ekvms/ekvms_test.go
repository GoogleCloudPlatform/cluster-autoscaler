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

package ekvms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	kube_test "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type resizeCall struct {
	instance             gce.GceRef
	desiredCpuMillicores int64
	desiredMemoryKb      int64
}

type fakeResizableVmClient struct {
	mu          sync.Mutex
	defaultSize ekvmsize.VmSize
	states      map[gce.GceRef]ekvmtypes.ResizableVmState
	bulkCalls   int
	getCalls    int
	resizeCalls []resizeCall
	bulkErr     error
	getErr      error
	resizeErr   error
}

func newFakeResizableVmClient(defaultSize ekvmsize.VmSize) *fakeResizableVmClient {
	return &fakeResizableVmClient{
		defaultSize: defaultSize,
		states:      make(map[gce.GceRef]ekvmtypes.ResizableVmState),
	}
}

func (f *fakeResizableVmClient) ResizeVm(_ context.Context, instance gce.GceRef, desiredCpuMillicores, desiredMemoryKb int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resizeCalls = append(f.resizeCalls, resizeCall{
		instance:             instance,
		desiredCpuMillicores: desiredCpuMillicores,
		desiredMemoryKb:      desiredMemoryKb,
	})
	if f.resizeErr != nil {
		return f.resizeErr
	}
	f.states[instance] = ekvmtypes.ResizableVmState{
		Size: ekvmsize.VmSize{
			MilliCpus: desiredCpuMillicores,
			KBytes:    desiredMemoryKb,
		},
		Status: ekvmtypes.ResizeStatusAtIntent,
	}
	return nil
}

func (f *fakeResizableVmClient) GetCurrentResizableVmState(_ resizablevms.MaxResizableVmSizeProvider, instance gce.GceRef) (ekvmtypes.ResizableVmState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getErr != nil {
		return ekvmtypes.ResizableVmState{}, f.getErr
	}
	if state, ok := f.states[instance]; ok {
		return state, nil
	}
	return ekvmtypes.ResizableVmState{
		Size:   f.defaultSize,
		Status: ekvmtypes.ResizeStatusAtIntent,
	}, nil
}

func (f *fakeResizableVmClient) BulkFetchCurrentResizableVmStates(_ resizablevms.MaxResizableVmSizeProvider, _, _ string) (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bulkCalls++
	if f.bulkErr != nil {
		return nil, f.bulkErr
	}
	out := make(map[gce.GceRef]ekvmtypes.ResizableVmState, len(f.states))
	for k, v := range f.states {
		out[k] = v
	}
	return out, nil
}

func (f *fakeResizableVmClient) getResizeCalls() []resizeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]resizeCall, len(f.resizeCalls))
	copy(out, f.resizeCalls)
	return out
}

func (f *fakeResizableVmClient) getCounts() (bulkCalls, getCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bulkCalls, f.getCalls
}

type staticNodeSizeRecommender struct {
	mu        sync.Mutex
	maxSize   ekvmsize.VmSize
	seenNodes map[string]bool
}

func newStaticNodeSizeRecommender(maxSize ekvmsize.VmSize) *staticNodeSizeRecommender {
	return &staticNodeSizeRecommender{
		maxSize:   maxSize,
		seenNodes: make(map[string]bool),
	}
}

func (s *staticNodeSizeRecommender) SetLocations(_ []string) {}

func (s *staticNodeSizeRecommender) MaxSize(node *apiv1.Node) *nodesizerecommender.MaxSizeRecommendation {
	if node != nil {
		s.mu.Lock()
		if s.seenNodes == nil {
			s.seenNodes = make(map[string]bool)
		}
		s.seenNodes[node.Name] = true
		s.mu.Unlock()
	}
	return &nodesizerecommender.MaxSizeRecommendation{
		VmSize:       s.maxSize,
		CreationTime: time.Now(),
	}
}

func (s *staticNodeSizeRecommender) hasSeenNode(nodeName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seenNodes[nodeName]
}

func (s *staticNodeSizeRecommender) factory() nodesizerecommender.RecommenderFactory {
	return func(internalopts.AutoscalingOptions, nodesizerecommender.InstanceFetcher, config.StringFlagEvaluator) (nodesizerecommender.NodeSizeRecommender, error) {
		return s, nil
	}
}

func staticRecommenderFactory(maxSize ekvmsize.VmSize) nodesizerecommender.RecommenderFactory {
	return newStaticNodeSizeRecommender(maxSize).factory()
}

// installBalloonPodRunningReactor simulates Kubelet starting and in-place resizing balloon pods
// in kube-system. On 1.36.* nodes, in-place resize leaves pod.Status.Resources.Requests[memory]
// at its initial PodRunning value, matching the 1.36 Kubelet bug (b/583763422).
func installBalloonPodRunningReactor(infra *integration.TestInfrastructure) {
	var uidCounter int64
	podsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	nodesGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}

	populateBalloonStatus := func(pod *apiv1.Pod, podLevelMem resource.Quantity) {
		c := pod.Spec.Containers[0]
		reqs := c.Resources.Requests.DeepCopy()
		pod.Status.Phase = apiv1.PodRunning
		pod.Status.ContainerStatuses = []apiv1.ContainerStatus{{
			Name:               c.Name,
			Ready:              true,
			State:              apiv1.ContainerState{Running: &apiv1.ContainerStateRunning{}},
			AllocatedResources: reqs.DeepCopy(),
		}}
		pod.Status.AllocatedResources = reqs.DeepCopy()
		pod.Status.Resources = &apiv1.ResourceRequirements{
			Requests: apiv1.ResourceList{
				apiv1.ResourceCPU:    reqs[apiv1.ResourceCPU],
				apiv1.ResourceMemory: podLevelMem,
			},
		}
	}

	infra.Fakes.KubeClient.Fake.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := createAction.GetObject().(*apiv1.Pod)
		if !ok || !operationtracker.IsBalloonPod(pod) {
			return false, nil, nil
		}
		// balloonPodChecker performs a DryRunAll Create with an unbound node (Spec.NodeName == "").
		// Return without persisting it in the fake tracker, matching real kube-apiserver DryRunAll behavior.
		if pod.Spec.NodeName == "" {
			return true, pod, nil
		}
		seq := atomic.AddInt64(&uidCounter, 1)
		if pod.UID == "" {
			pod.UID = types.UID(fmt.Sprintf("balloon-uid-%d", seq))
		}
		if pod.Name == "" {
			pod.Name = fmt.Sprintf("%s%d", pod.GenerateName, seq)
		}
		populateBalloonStatus(pod, pod.Spec.Containers[0].Resources.Requests[apiv1.ResourceMemory])
		return false, nil, nil
	})

	infra.Fakes.KubeClient.Fake.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patchAction, ok := action.(k8stesting.PatchAction)
		if !ok || patchAction.GetSubresource() != "resize" {
			return false, nil, nil
		}
		obj, err := infra.Fakes.KubeClient.Tracker().Get(podsGVR, patchAction.GetNamespace(), patchAction.GetName())
		if err != nil {
			return true, nil, err
		}
		updated := obj.(*apiv1.Pod).DeepCopy()
		var patchPayload apiv1.Pod
		if err := json.Unmarshal(patchAction.GetPatch(), &patchPayload); err != nil {
			return true, nil, err
		}
		updated.Spec.Containers[0].Resources = patchPayload.Spec.Containers[0].Resources

		podLevelMem := updated.Spec.Containers[0].Resources.Requests[apiv1.ResourceMemory]
		if nodeObj, err := infra.Fakes.KubeClient.Tracker().Get(nodesGVR, "", updated.Spec.NodeName); err == nil {
			if strings.HasPrefix(strings.TrimPrefix(nodeObj.(*apiv1.Node).Status.NodeInfo.KubeletVersion, "v"), "1.36.") && updated.Status.Resources != nil {
				podLevelMem = updated.Status.Resources.Requests[apiv1.ResourceMemory]
			}
		}
		populateBalloonStatus(updated, podLevelMem)

		if err := infra.Fakes.KubeClient.Tracker().Update(podsGVR, updated, updated.Namespace); err != nil {
			return true, nil, err
		}
		return true, updated, nil
	})
}

func ekNodePool(name string, initialCount, maxCount int, extraLabels map[string]string) *gke_api_beta.NodePool {
	lbls := map[string]string{
		apiv1.LabelInstanceTypeStable: "ek-standard-8",
		gkelabels.MachineFamilyLabel:  "ek",
	}
	for k, v := range extraLabels {
		lbls[k] = v
	}
	return integration.DefaultNodePoolBuilder(name).
		WithMachineType("ek-standard-8").
		WithSize(int64(initialCount)).
		WithMin(1).
		WithMax(int64(maxCount)).
		WithLocations("us-central1-a").
		WithLabels(lbls).
		Build()
}

func waitForEkNodeReady(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, autoscaler core.Autoscaler, recommender *staticNodeSizeRecommender) string {
	t.Helper()
	var balloonNodeName string
	var nodeReady bool
	for range 10 {
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		pods, err := infra.Fakes.KubeClient.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}
		for i := range pods.Items {
			if operationtracker.IsBalloonPod(&pods.Items[i]) && pods.Items[i].Spec.NodeName != "" && pods.Items[i].Spec.NodeName != "unmanaged-ek-node" {
				balloonNodeName = pods.Items[i].Spec.NodeName
				break
			}
		}
		if balloonNodeName != "" {
			node, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, balloonNodeName, metav1.GetOptions{})
			if err == nil && !taints.TaintExists(node.Spec.Taints, ekvmtypes.BPResizeTaint) {
				nodeReady = true
				break
			}
		}
	}
	if !nodeReady {
		t.Fatalf("OperationTracker did not initialize EK node and remove BPResizeTaint")
	}
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
	if !recommender.hasSeenNode(balloonNodeName) {
		t.Fatalf("ScaleUpNodeProcessor did not register EK node %s with recommender", balloonNodeName)
	}
	return balloonNodeName
}

func findNodeBalloonPod(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodeName string) *apiv1.Pod {
	t.Helper()
	pods, err := infra.Fakes.KubeClient.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list kube-system pods: %v", err)
	}
	for i := range pods.Items {
		if operationtracker.IsBalloonPod(&pods.Items[i]) && pods.Items[i].Spec.NodeName == nodeName {
			return &pods.Items[i]
		}
	}
	return nil
}

func TestEkvmsUpsizeAndBalloonPodIppr(t *testing.T) {
	for _, tc := range []struct {
		name              string
		nodeVersion       string
		wantInPlaceResize bool
	}{
		{
			name:              "1.37_kubelet_resizes_in_place",
			nodeVersion:       "1.37.1-gke.3020000",
			wantInPlaceResize: true,
		},
		{
			name:              "1.36_kubelet_recreates_balloon_pod",
			nodeVersion:       "1.36.4-gke.2377000",
			wantInPlaceResize: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)

				infra := integration.SetupInfrastructure(ctx, t)
				installBalloonPodRunningReactor(infra)

				// Start the ek-standard-8 VM at a downsized state (2 vCPU, 8 GiB) with max recommended size 8 vCPU, 32 GiB.
				initialVmSize := ekvmsize.VmSize{MilliCpus: 2000, KBytes: 8 * ekvmsize.GiBToKiB}
				maxVmSize := ekvmsize.VmSize{MilliCpus: 8000, KBytes: 32 * ekvmsize.GiBToKiB}
				rvClient := newFakeResizableVmClient(initialVmSize)
				recommender := newStaticNodeSizeRecommender(maxVmSize)

				np := ekNodePool("ek-pool", 1, 3, nil)
				np.Version = tc.nodeVersion

				cfg := integration.NewTestConfig().
					WithClusterOverrides(integration.WithCurrentMasterVersion("1.37.1-gke.3020000")).
					WithNodePools(np).
					WithResizableVmClient(rvClient).
					WithNodeSizeRecommenderFactory(recommender.factory()).
					WithOverrides(integration.WithEkvmsEnabled(true))

				autoscaler := integration.MustSetupAutoscaler(ctx, t, cfg, infra)
				ekNodeName := waitForEkNodeReady(ctx, t, infra, autoscaler, recommender)
				initialBP := findNodeBalloonPod(ctx, t, infra, ekNodeName)
				if initialBP == nil {
					t.Fatalf("expected initial balloon pod on node %q", ekNodeName)
				}

				// 16 GiB workload exceeds current 2 vCPU / 8 GiB size and only fits on the 32 GiB node once the
				// ~24 GiB balloon pod shrinks without leaving stale pod.Status.Resources.Requests[memory].
				pod := kube_test.BuildTestPod("ek-workload-pod", 3000, 16*1024*1024*1024, kube_test.MarkUnschedulable())
				pod.Namespace = "default"
				infra.Fakes.K8s.AddPod(pod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
				time.Sleep(150 * time.Millisecond)
				synctest.Wait()

				calls := rvClient.getResizeCalls()
				if assert.GreaterOrEqual(t, len(calls), 1, "expected ResizableVmClient.ResizeVm to be called to upsize the EK node") {
					assert.Greater(t, calls[0].desiredCpuMillicores, initialVmSize.MilliCpus)
				}

				afterBP := findNodeBalloonPod(ctx, t, infra, ekNodeName)
				if afterBP == nil {
					t.Fatalf("expected balloon pod on node %q after upsize", ekNodeName)
				}
				assert.Equal(t, tc.wantInPlaceResize, afterBP.UID == initialBP.UID, "unexpected balloon pod resize mechanism (initial UID %q, after UID %q)", initialBP.UID, afterBP.UID)

				cluster := integration.GetTestCluster(t, infra, cfg)
				if assert.Len(t, cluster.NodePools, 1) {
					assert.Equal(t, int64(1), infra.Fakes.GkeService.MustGetTargetSize(t, cluster.NodePools[0]), "expected ek-pool not to add a new node when upsizing existing EK VM suffices")
				}

				infra.Fakes.RunScheduler(ctx, t)
				scheduled, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
				if assert.NoError(t, err) {
					assert.Equal(t, ekNodeName, scheduled.Spec.NodeName)
				}
			})
		})
	}
}

func TestEkvmsResizableVmClientErrorsHandledGracefully(t *testing.T) {
	t.Run("BulkAndGetStateErrors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)

			infra := integration.SetupInfrastructure(ctx, t)
			installBalloonPodRunningReactor(infra)

			maxVmSize := ekvmsize.VmSize{MilliCpus: 8000, KBytes: 32 * ekvmsize.GiBToKiB}
			rvClient := newFakeResizableVmClient(maxVmSize)
			rvClient.bulkErr = errors.New("simulated BulkFetchCurrentResizableVmStates error")
			rvClient.getErr = errors.New("simulated GetCurrentResizableVmState error")

			pod := kube_test.BuildTestPod("pending-pod", 1000, 1*1024*1024*1024, kube_test.MarkUnschedulable())
			pod.Namespace = "default"
			infra.Fakes.K8s.AddPod(pod)

			cfg := integration.NewTestConfig().
				WithNodePools(ekNodePool("ek-pool", 1, 3, nil)).
				WithResizableVmClient(rvClient).
				WithNodeSizeRecommenderFactory(staticRecommenderFactory(maxVmSize)).
				WithOverrides(integration.WithEkvmsEnabled(true))

			autoscaler := integration.MustSetupAutoscaler(ctx, t, cfg, infra)

			for range 5 {
				time.Sleep(50 * time.Millisecond)
				synctest.Wait()
				bulkCalls, getCalls := rvClient.getCounts()
				if bulkCalls >= 1 && getCalls >= 1 {
					break
				}
			}
			bulkCalls, getCalls := rvClient.getCounts()
			assert.GreaterOrEqual(t, bulkCalls, 1)
			assert.GreaterOrEqual(t, getCalls, 1)

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
		})
	})

	t.Run("ResizeVmErrorOnUpsize", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)

			infra := integration.SetupInfrastructure(ctx, t)
			installBalloonPodRunningReactor(infra)

			initialVmSize := ekvmsize.VmSize{MilliCpus: 2000, KBytes: 8 * ekvmsize.GiBToKiB}
			maxVmSize := ekvmsize.VmSize{MilliCpus: 8000, KBytes: 32 * ekvmsize.GiBToKiB}
			rvClient := newFakeResizableVmClient(initialVmSize)
			rvClient.resizeErr = errors.New("simulated GCE ResizeVm failure")
			recommender := newStaticNodeSizeRecommender(maxVmSize)

			cfg := integration.NewTestConfig().
				WithNodePools(ekNodePool("ek-pool", 1, 3, nil)).
				WithResizableVmClient(rvClient).
				WithNodeSizeRecommenderFactory(recommender.factory()).
				WithOverrides(integration.WithEkvmsEnabled(true))

			autoscaler := integration.MustSetupAutoscaler(ctx, t, cfg, infra)

			waitForEkNodeReady(ctx, t, infra, autoscaler, recommender)

			pod := kube_test.BuildTestPod("ek-upsize-fail-pod", 3000, 4*1024*1024*1024, kube_test.MarkUnschedulable())
			pod.Namespace = "default"
			infra.Fakes.K8s.AddPod(pod)

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
			for range 5 {
				time.Sleep(50 * time.Millisecond)
				synctest.Wait()
				if len(rvClient.getResizeCalls()) >= 1 {
					break
				}
			}
			assert.GreaterOrEqual(t, len(rvClient.getResizeCalls()), 1, "expected ResizeVm to be attempted")

			// Subsequent RunOnce after ResizeVm failure and backoff/reconciliation must not panic.
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
			time.Sleep(50 * time.Millisecond)
			synctest.Wait()
		})
	})
}

func TestEkvmsWithUnmanagedNodeAndComputeClassDoesNotPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		installBalloonPodRunningReactor(infra)

		initialVmSize := ekvmsize.VmSize{MilliCpus: 2000, KBytes: 8 * ekvmsize.GiBToKiB}
		maxVmSize := ekvmsize.VmSize{MilliCpus: 8000, KBytes: 32 * ekvmsize.GiBToKiB}
		rvClient := newFakeResizableVmClient(initialVmSize)
		recommender := newStaticNodeSizeRecommender(maxVmSize)

		ccc := &cccv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "ek-ccc",
			},
			Spec: cccv1.ComputeClassSpec{
				Priorities: []cccv1.Priority{
					{
						MachineFamily: ptr.To("ek"),
					},
				},
				NodePoolAutoCreation: &cccv1.NodePoolAutoCreation{
					Enabled: true,
				},
				WhenUnsatisfiable: "ScaleUpAnyway",
			},
		}

		cfg := integration.NewTestConfig().
			WithNodePools(ekNodePool("ek-ccc-pool", 1, 3, map[string]string{
				gkelabels.ComputeClassLabel: "ek-ccc",
			})).
			WithCccCrds(ccc).
			WithResizableVmClient(rvClient).
			WithNodeSizeRecommenderFactory(recommender.factory()).
			WithOverrides(integration.WithEkvmsEnabled(true))

		// Add an unmanaged EK node (with a ProviderID not belonging to any MIG, so GkeMigForNode returns (nil, nil))
		// and a pre-existing running balloon pod so OperationTracker adds it to nodeStateManager without needing
		// a MIG lookup. During RunOnce, ScaleUpNodeProcessor's calculateNodeRuleIndex will be invoked for this node.
		unmanagedNode := kube_test.BuildTestNode("unmanaged-ek-node", 8000, 32*1024*1024*1024)
		unmanagedNode.Spec.ProviderID = "gce://test-project/us-central1-a/unmanaged-ek-node"
		unmanagedNode.Labels = map[string]string{
			apiv1.LabelInstanceTypeStable: "ek-standard-8",
			gkelabels.MachineFamilyLabel:  "ek",
			gkelabels.ComputeClassLabel:   "ek-ccc",
		}
		kube_test.SetNodeReadyState(unmanagedNode, true, time.Now().Add(-10*time.Minute))

		bPod, err := operationtracker.GenerateBalloonPod(
			unmanagedNode,
			*resource.NewMilliQuantity(6000, resource.DecimalSI),
			*resource.NewQuantity(24*1024*1024*1024, resource.DecimalSI),
			true,
		)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		bPod.Status.Phase = apiv1.PodRunning
		infra.Fakes.K8s.AddPod(bPod)
		infra.Fakes.K8s.AddNode(unmanagedNode)

		autoscaler := integration.MustSetupAutoscaler(ctx, t, cfg, infra)

		waitForEkNodeReady(ctx, t, infra, autoscaler, recommender)

		// Add an unschedulable pod targeting ek-ccc to trigger ScaleUpNodeProcessor's CCC rule splitting.
		pod := kube_test.BuildTestPod("ek-ccc-pod", 3000, 4*1024*1024*1024, kube_test.MarkUnschedulable())
		pod.Namespace = "default"
		pod.Spec.NodeSelector = map[string]string{
			gkelabels.ComputeClassLabel: "ek-ccc",
		}
		infra.Fakes.K8s.AddPod(pod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
		for range 5 {
			time.Sleep(50 * time.Millisecond)
			synctest.Wait()
			if len(rvClient.getResizeCalls()) >= 1 {
				break
			}
		}

		assert.True(t, recommender.hasSeenNode(unmanagedNode.Name), "expected unmanaged-ek-node to be processed without panic")
		assert.GreaterOrEqual(t, len(rvClient.getResizeCalls()), 1, "expected managed ek-ccc-pool node to be upsized")
	})
}
