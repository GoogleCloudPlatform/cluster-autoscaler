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

package dualca

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestDualCA_ScaleUpOnlyForOwnedPendingPods checks that a CA instance only
// scales up for the pending pods it owns, and doesn't report the pods of the
// other instance as not triggering a scale-up. PodShardingProcessor processes
// one shard of pending pods per loop, so the test runs a loop, schedules the
// pods that got capacity and runs another loop, in which only the pods of the
// other ComputeClasses are left pending.
func TestDualCA_ScaleUpOnlyForOwnedPendingPods(t *testing.T) {
	testCases := []struct {
		name               string
		autoscalerNames    []string
		pendingPods        []*apiv1.Pod
		wantStdPoolSize    int64
		wantSolverPoolSize int64
	}{
		{
			name:            "solver CA scales up only for solver ComputeClass pods",
			autoscalerNames: solverCANames,
			pendingPods: []*apiv1.Pod{
				pendingPod("solver-pod-1", solverComputeClass),
				pendingPod("solver-pod-2", solverComputeClass),
				pendingPod("std-pod-1", stdComputeClass),
				pendingPod("unannotated-pod-1", ""),
			},
			wantSolverPoolSize: 2,
		},
		{
			name:            "standard CA scales up only for standard ComputeClass pods",
			autoscalerNames: standardCANames,
			pendingPods: []*apiv1.Pod{
				pendingPod("std-pod-1", stdComputeClass),
				pendingPod("std-pod-2", stdComputeClass),
				pendingPod("solver-pod-1", solverComputeClass),
				pendingPod("solver-pod-2", solverComputeClass),
			},
			wantStdPoolSize: 2,
		},
		{
			name:            "standard CA scales up for pods without a ComputeClass but not for solver ComputeClass pods",
			autoscalerNames: standardCANames,
			pendingPods: []*apiv1.Pod{
				pendingPod("unannotated-pod-1", ""),
				pendingPod("solver-pod-1", solverComputeClass),
				pendingPod("solver-pod-2", solverComputeClass),
			},
			wantStdPoolSize: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stdPool := newComputeClassPool(stdPoolName, stdComputeClass, 0, "us-central1-a")
			solverPool := newComputeClassPool(solverPoolName, solverComputeClass, 0, "us-central1-a")
			testConfig := newDualCATestConfig(tc.autoscalerNames, stdPool, solverPool).
				WithOverrides(
					integration.WithAutoscalerVisibility(true),
					integration.WithEmitNoScaleUpCAVizEvents(true),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Given: pending pods of both CA instances.
				for _, p := range tc.pendingPods {
					infra.Fakes.K8s.AddPod(p)
				}

				// When: the CA instance runs, the scheduler schedules the pods
				// that got capacity, and the CA instance runs again.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Then: the CA instance scaled up only for its own pods, and
				// didn't report the other instance's pods as not triggering a
				// scale-up.
				assert.Equal(t, tc.wantStdPoolSize, infra.Fakes.GkeService.MustGetTargetSize(t, stdPool), "std-pool size")
				assert.Equal(t, tc.wantSolverPoolSize, infra.Fakes.GkeService.MustGetTargetSize(t, solverPool), "solver-pool size")
				assert.Empty(t, noScaleUpPodNames(infra), "pods reported in NoScaleUp visibility events")
			})
		})
	}
}

// TestDualCA_ScaleUpAccountsForOtherCANodesAndPods checks that the nodes and
// pods of the other CA instance, while not owned, still constrain where the
// owned pods can run: they are part of the cluster snapshot. The owned pods
// require pod affinity to a pod of the other instance, so they can only run in
// its zone, and can't run at all if the CA instance doesn't see it.
func TestDualCA_ScaleUpAccountsForOtherCANodesAndPods(t *testing.T) {
	testCases := []struct {
		name              string
		autoscalerNames   []string
		ownedComputeClass string
		ownedPoolName     string
		otherComputeClass string
		otherPoolName     string
	}{
		{
			name:              "solver CA puts solver pods in the zone of a standard pod",
			autoscalerNames:   solverCANames,
			ownedComputeClass: solverComputeClass,
			ownedPoolName:     solverPoolName,
			otherComputeClass: stdComputeClass,
			otherPoolName:     stdPoolName,
		},
		{
			name:              "standard CA puts standard pods in the zone of a solver pod",
			autoscalerNames:   standardCANames,
			ownedComputeClass: stdComputeClass,
			ownedPoolName:     stdPoolName,
			otherComputeClass: solverComputeClass,
			otherPoolName:     solverPoolName,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ownedPool := newComputeClassPool(tc.ownedPoolName, tc.ownedComputeClass, 0, "us-central1-a", "us-central1-b")
			otherPool := newComputeClassPool(tc.otherPoolName, tc.otherComputeClass, 1, "us-central1-b")
			pools := map[string]*gke_api_beta.NodePool{ownedPool.Name: ownedPool, otherPool.Name: otherPool}
			testConfig := newDualCATestConfig(tc.autoscalerNames, pools[stdPoolName], pools[solverPoolName])

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Given: a pod of the other CA instance running on its node in
				// us-central1-b.
				otherNode := mustGetPoolNode(t, infra, tc.otherPoolName)
				infra.Fakes.K8s.AddPod(tu.BuildTestPod("critical-other-pod", 100, 100,
					pod.WithCCC(tc.otherComputeClass),
					tu.WithLabels(map[string]string{"app": "critical"}),
					tu.WithNodeName(otherNode.Name),
				))

				// Given: 2 pending pods of the CA instance that must run in the
				// same zone as that pod.
				for _, name := range []string{"owned-pod-1", "owned-pod-2"} {
					infra.Fakes.K8s.AddPod(tu.BuildTestPod(name, podCPU, podMem,
						tu.MarkUnschedulable(),
						pod.WithCCC(tc.ownedComputeClass),
						pod.WithPodAffinity(map[string]string{"app": "critical"}, apiv1.LabelTopologyZone),
					))
				}

				// When: the CA instance runs.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Then: the CA instance scaled up its node pool only in
				// us-central1-b.
				assert.Equal(t, int64(0), infra.Fakes.GkeService.MustGetTargetSizeInZone(t, ownedPool, "us-central1-a"), "owned pool size in us-central1-a")
				assert.Equal(t, int64(2), infra.Fakes.GkeService.MustGetTargetSizeInZone(t, ownedPool, "us-central1-b"), "owned pool size in us-central1-b")
			})
		})
	}
}

// TestDualCA_SingleCAScalesUpEveryComputeClass checks that a single CA instance
// scales up for the pods of every
// ComputeClass, regardless of their autoscaler-name annotations: both when it
// owns every autoscaler name, e.g. while the solver CA is not deployed, and
// when it has no autoscaler names, i.e. ownership filtering is disabled.
func TestDualCA_SingleCAScalesUpEveryComputeClass(t *testing.T) {
	testCases := []struct {
		name            string
		autoscalerNames []string
	}{
		{
			name:            "owning every autoscaler name",
			autoscalerNames: append(slices.Clone(standardCANames), solverCANames...),
		},
		{
			name:            "without autoscaler names",
			autoscalerNames: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stdPool := newComputeClassPool(stdPoolName, stdComputeClass, 0, "us-central1-a")
			solverPool := newComputeClassPool(solverPoolName, solverComputeClass, 0, "us-central1-a")
			testConfig := newDualCATestConfig(tc.autoscalerNames, stdPool, solverPool)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Given: a pending pod of each ComputeClass.
				infra.Fakes.K8s.AddPod(pendingPod("std-pod-1", stdComputeClass))
				infra.Fakes.K8s.AddPod(pendingPod("solver-pod-1", solverComputeClass))

				// When: the CA instance runs twice: the pods of each
				// ComputeClass are a separate pod shard, processed in a
				// separate loop.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Then: the CA instance scaled up both node pools.
				assert.Equal(t, int64(1), infra.Fakes.GkeService.MustGetTargetSize(t, stdPool), "std-pool size")
				assert.Equal(t, int64(1), infra.Fakes.GkeService.MustGetTargetSize(t, solverPool), "solver-pool size")
			})
		})
	}
}
