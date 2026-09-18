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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// scaleDownUnneededTime is lowered so that scale-down doesn't need long waits.
const scaleDownUnneededTime = time.Second

// TestDualCA_ScaleDownOnlyOwnedNodes checks that a CA instance only scales down
// the nodes it owns.
func TestDualCA_ScaleDownOnlyOwnedNodes(t *testing.T) {
	testCases := []struct {
		name               string
		autoscalerNames    []string
		wantStdPoolSize    int64
		wantSolverPoolSize int64
	}{
		{
			name:               "solver CA scales down only the empty solver node",
			autoscalerNames:    solverCANames,
			wantStdPoolSize:    1,
			wantSolverPoolSize: 0,
		},
		{
			name:               "standard CA scales down only the empty standard node",
			autoscalerNames:    standardCANames,
			wantStdPoolSize:    0,
			wantSolverPoolSize: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stdPool := newComputeClassPool(stdPoolName, stdComputeClass, 1, "us-central1-a")
			solverPool := newComputeClassPool(solverPoolName, solverComputeClass, 1, "us-central1-a")
			testConfig := newDualCATestConfig(tc.autoscalerNames, stdPool, solverPool).
				WithOverrides(integration.WithScaleDownUnneededTime(scaleDownUnneededTime))

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Given: an empty standard and an empty solver node. When: the
				// first loop finds the empty nodes unneeded, the second one,
				// once they've been unneeded for longer than
				// ScaleDownUnneededTime, removes them.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*scaleDownUnneededTime)

				// Then: only the node of the CA instance was removed.
				assert.Equal(t, tc.wantStdPoolSize, infra.Fakes.GkeService.MustGetTargetSize(t, stdPool), "std-pool size")
				assert.Equal(t, tc.wantSolverPoolSize, infra.Fakes.GkeService.MustGetTargetSize(t, solverPool), "solver-pool size")
			})
		})
	}
}

// TestDualCA_ScaleToZeroBlockedByOtherCAPendingPods checks that cluster
// scale-to-0, a cluster-wide decision, also accounts for the pods pending for
// the other CA instance.
func TestDualCA_ScaleToZeroBlockedByOtherCAPendingPods(t *testing.T) {
	const scaleToZeroGracePeriod = 0
	testCases := []struct {
		name            string
		pendingPods     []*apiv1.Pod
		wantStdPoolSize int64
	}{
		{
			name:            "no pending pods: standard CA scales the cluster to zero",
			wantStdPoolSize: 0,
		},
		{
			name:            "pending solver pod: standard CA does not scale the cluster to zero",
			pendingPods:     []*apiv1.Pod{pendingPod("solver-pod-1", solverComputeClass)},
			wantStdPoolSize: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stdPool := newComputeClassPool(stdPoolName, stdComputeClass, 1, "us-central1-a")
			solverPool := newComputeClassPool(solverPoolName, solverComputeClass, 0, "us-central1-a")
			testConfig := newDualCATestConfig(standardCANames, stdPool, solverPool).
				WithOverrides(
					integration.WithClusterScaleToZeroEnabled(scaleToZeroGracePeriod),
					integration.WithScaleDownUnneededTime(scaleDownUnneededTime),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Given: the standard node runs only a system pod, which blocks
				// regular scale-down of the node.
				stdNode := mustGetPoolNode(t, infra, stdPoolName)
				infra.Fakes.K8s.AddPod(tu.BuildTestPod("system-pod", 100, 100, tu.WithNamespace("kube-system"), tu.WithNodeName(stdNode.Name)))
				for _, p := range tc.pendingPods {
					infra.Fakes.K8s.AddPod(p)
				}

				// When: the standard CA runs three times. The first loop
				// observes the cluster empty, i.e. without user pods. The
				// second one, once the cluster has been empty for longer than
				// the scale-to-0 grace period, removes the system pods from the
				// cluster snapshot, so the node becomes unneeded. The third
				// one, once the node has been unneeded for longer than
				// ScaleDownUnneededTime, removes it.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, scaleToZeroGracePeriod+time.Second)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*scaleDownUnneededTime)

				// Then: the standard node was removed only if no pods were
				// pending for either CA instance.
				assert.Equal(t, tc.wantStdPoolSize, infra.Fakes.GkeService.MustGetTargetSize(t, stdPool), "std-pool size")
			})
		})
	}
}
