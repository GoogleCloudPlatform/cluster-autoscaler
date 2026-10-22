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

// installBalloonPodRunningReactor simulates Kubelet starting balloon pods created in kube-system
// by assigning a generated Name, UID, and PodRunning phase upon creation.
func installBalloonPodRunningReactor(infra *integration.TestInfrastructure) {
	var uidCounter int64
	infra.Fakes.KubeClient.Fake.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := createAction.GetObject().(*apiv1.Pod)
		if !ok {
			return false, nil, nil
		}
		if pod.Namespace == "kube-system" && (strings.HasPrefix(pod.Name, "gke-system-balloon-pod-") || pod.GenerateName == "gke-system-balloon-pod-") {
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
			pod.Status.Phase = apiv1.PodRunning
		}
		return false, nil, nil
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

func TestEkvmsUpsizeOnPendingPod(t *testing.T) {
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

		cfg := integration.NewTestConfig().
			WithNodePools(ekNodePool("ek-pool", 1, 3, nil)).
			WithResizableVmClient(rvClient).
			WithNodeSizeRecommenderFactory(recommender.factory()).
			WithOverrides(integration.WithEkvmsEnabled(true))

		autoscaler := integration.MustSetupAutoscaler(ctx, t, cfg, infra)

		waitForEkNodeReady(ctx, t, infra, autoscaler, recommender)

		// Add an unschedulable pod requesting 3 CPUs and 4 GiB memory, which exceeds the current 2 CPU
		// allocatable (with the 6 CPU balloon pod) but fits within the 8 CPU max size of the EK node.
		pod := kube_test.BuildTestPod("ek-workload-pod", 3000, 4*1024*1024*1024, kube_test.MarkUnschedulable())
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

		calls := rvClient.getResizeCalls()
		if assert.GreaterOrEqual(t, len(calls), 1, "expected ResizableVmClient.ResizeVm to be called to upsize the EK node") {
			assert.Greater(t, calls[0].desiredCpuMillicores, initialVmSize.MilliCpus)
		}

		// Allow operationTracker.upsize background polling (5ms interval) to complete finishInPlaceResize.
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()

		cluster := integration.GetTestCluster(t, infra, cfg)
		if !assert.Len(t, cluster.NodePools, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(1), infra.Fakes.GkeService.MustGetTargetSize(t, cluster.NodePools[0]), "expected ek-pool not to add a new node when upsizing existing EK VM suffices")
	})
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
