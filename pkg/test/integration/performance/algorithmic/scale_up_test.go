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

package algorithmic

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integrationsynctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/testutils"
	"math"
	"testing"
	"testing/synctest"
	"time"
)

const targetNodeCount = 5000

// TestLargeScaleUp verifies the autoscaler's ability to handle a massive scale-up event.
// It ensures that the autoscaler correctly scales up to meet demand within performance bounds.
func TestLargeScaleUp(t *testing.T) {
	// TODO(b/527753792): investigate test failures.
	testutils.MarkTestManual(t)

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithMaxNodesTotal(math.MaxInt32),
			integration.WithMaxCoresTotal(math.MaxInt64),
			integration.WithMaxMemoryTotal(math.MaxInt64),
			integration.WithMaxNodesPerScaleUp(targetNodeCount),
		).
		WithNodePools(
			integration.DefaultNodePoolBuilder("pool-1").WithMax(2000).Build(),
			integration.EmptyNodePool("pool-2").WithMax(2000).Build(),
			integration.EmptyNodePool("pool-3").WithMax(2000).Build(),
		)

	defer integrationsynctest.EnableLargeWatchChannel()()

	// To measure the real-world CPU time it takes for CA to execute
	// within the synctest bubble, we use a RealTimeClock.
	rtc := integrationsynctest.NewRealTimeClock()
	defer rtc.Stop()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Add targetNodeCount unschedulable pods to the Tracker before setting up the autoscaler (and informers).
		// This causes the initial Informer List to fetch all targetNodeCount pods at once,
		// bypassing targetNodeCount individual watch events and significantly expediting the test.
		var pods []*apiv1.Pod
		for i := 0; i < targetNodeCount; i++ {
			// n1-standard-2 has 2 CPUs, so requesting 1500m guarantees 1 pod per node
			pod := tu.BuildTestPod(fmt.Sprintf("my-pod-%d", i), 1500, 4000, tu.MarkUnschedulable())
			pods = append(pods, pod)
			assert.NoError(t, infra.Fakes.KubeClient.Tracker().Add(pod))
		}

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integrationsynctest.TearDown(cancel)

		testConfig.ResolveOptions()

		startScaleUp := rtc.Now()
		// We explicitly capped the node pools at 2000 nodes each via integration.WithNodePoolMax(2000).
		// Since we need 5000 nodes total, we iterate exactly until the required nodes are provisioned.
		for i := 0; i < 10; i++ { // Safe upper bound to prevent infinite hang if logic fails
			nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			if len(nodes.Items) >= targetNodeCount {
				break
			}
			integrationsynctest.MustRunOnceAfter(t, autoscaler, time.Second)
		}
		endScaleUp := rtc.Now()
		scaleUpDuration := endScaleUp.Sub(startScaleUp)
		t.Logf("Scale up real execution time: %v", scaleUpDuration)
		assert.Less(t, scaleUpDuration, 90*time.Second, "Scale-up took too long")

		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes.Items), targetNodeCount, "Expected at least %d nodes after scale up", targetNodeCount)
	})
}
