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

package flexadvisor

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	experimentfake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestFlexAdvisorThrottlingEndToEndCapEnforcement verifies that when the FlexAdvisorMaxActiveScopes cap is hit,
// the scope creation is rejected and CA falls back to standard scale up logic.
func TestFlexAdvisorThrottlingEndToEndCapEnforcement(t *testing.T) {

	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").WithMachineType(AvailableMachineType).WithCCCLabel("ccc-1").Build(),
		integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel("ccc-2").Build(),
		integration.EmptyNodePool("pool-3").WithMachineType(AvailableMachineType).WithCCCLabel("ccc-3").Build(),
	}

	ccc1 := ccc.NewComputeClassBuilder("ccc-1").WithNodePoolsRules("pool-1").Build()
	ccc2 := ccc.NewComputeClassBuilder("ccc-2").WithNodePoolsRules("pool-2").Build()
	ccc3 := ccc.NewComputeClassBuilder("ccc-3").WithNodePoolsRules("pool-3").Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc1, ccc2, ccc3).
		WithOverrides(
			integration.WithFlexAdvisorEnabled(),
		)

	testConfig.ExperimentEvaluator = experimentfake.NewEvaluator(nil, map[string]string{experiments.FlexAdvisorMaxActiveScopes: "1"})

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(AvailableMachineType).WithCapacity(10),
		)

		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("ccc-1"))
		infra.Fakes.K8s.AddPod(pod1)

		pod2 := tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("ccc-2"))
		infra.Fakes.K8s.AddPod(pod2)

		pod3 := tu.BuildTestPod("pod-3", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("ccc-3"))
		infra.Fakes.K8s.AddPod(pod3)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)
		infra.Fakes.RunScheduler(ctx, t)
		t.Logf("Nodes: %d", len(infra.Fakes.K8s.Nodes().Items))
		for _, p := range []string{"pod-1", "pod-2", "pod-3"} {
			pod, _ := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, p, metav1.GetOptions{})
			t.Logf("Pod %s NodeName: %q", p, pod.Spec.NodeName)
		}

		expectedMetric := `
# HELP cluster_autoscaler_flexadvisor_rejected_scopes [ALPHA] Number of Flex Advisor scopes rejected in the current loop due to throttling.
# TYPE cluster_autoscaler_flexadvisor_rejected_scopes gauge
cluster_autoscaler_flexadvisor_rejected_scopes 2
`
		// We only check the metric after the first run because throttling is applied during the initial scope creation process.
		// In subsequent runs, the set of unscheduled pods and thus the scopes might change, and the metric's value depends on
		// whether new scopes are being proposed and rejected, which isn't the primary focus of this test after the initial cap enforcement.
		err = testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedMetric), "cluster_autoscaler_flexadvisor_rejected_scopes")
		assert.NoError(t, err)

		// After 1st run, 1 pod should be scheduled and 2 unscheduled (because pod sharding restricts standard CA to 1 shard per loop)
		scheduledCount := 0
		unscheduledCount := 0
		for _, p := range []string{"pod-1", "pod-2", "pod-3"} {
			pod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, p, metav1.GetOptions{})
			assert.NoError(t, err)
			if pod.Spec.NodeName != "" {
				scheduledCount++
			} else {
				unscheduledCount++
			}
		}
		assert.Equal(t, 1, scheduledCount, "expected exactly 1 pod to be scheduled after 1st run")
		assert.Equal(t, 2, unscheduledCount, "expected exactly 2 pods to remain unscheduled after 1st run")

		// Run 2 more times
		for i := 0; i < 2; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)
			infra.Fakes.RunScheduler(ctx, t)
		}

		// Verify that ultimately all 3 pods got scheduled meaning FA was not consulted for the third pod.
		scheduledCount = 0
		unscheduledCount = 0
		for _, p := range []string{"pod-1", "pod-2", "pod-3"} {
			pod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, p, metav1.GetOptions{})
			assert.NoError(t, err)
			if pod.Spec.NodeName != "" {
				scheduledCount++
			} else {
				unscheduledCount++
			}
		}

		assert.Equal(t, 3, scheduledCount, "expected all 3 pods to be scheduled after 3 runs")
		assert.Equal(t, 0, unscheduledCount, "expected 0 pods to remain unscheduled")
	})
}
