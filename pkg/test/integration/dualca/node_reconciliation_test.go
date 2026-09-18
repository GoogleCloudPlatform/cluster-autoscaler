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

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/failednodes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestDualCA_DefragCleansUpOnlyOwnedNodes checks that a CA instance removes
// stale defrag taints from its own nodes, but not the taints of the other
// instance's defrag candidates.
func TestDualCA_DefragCleansUpOnlyOwnedNodes(t *testing.T) {
	allDefragTaints := []string{defrag.HardTaint, defrag.SoftTaint}
	testCases := []struct {
		name                 string
		autoscalerNames      []string
		wantStdNodeTaints    []string
		wantSolverNodeTaints []string
	}{
		{
			name:                 "standard CA cleans up only the standard node",
			autoscalerNames:      standardCANames,
			wantStdNodeTaints:    nil,
			wantSolverNodeTaints: allDefragTaints,
		},
		{
			name:                 "solver CA cleans up only the solver node",
			autoscalerNames:      solverCANames,
			wantStdNodeTaints:    allDefragTaints,
			wantSolverNodeTaints: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stdPool := newComputeClassPool(stdPoolName, stdComputeClass, 1, "us-central1-a")
			solverPool := newComputeClassPool(solverPoolName, solverComputeClass, 1, "us-central1-a")
			// Defrag has to be enabled for the stale defrag taint cleanup to
			// run, but neither node may be a defrag candidate of the CA
			// instance: the failed-nodes plugin only picks failed nodes, and
			// the test nodes are healthy.
			testConfig := newDualCATestConfig(tc.autoscalerNames, stdPool, solverPool).
				WithOverrides(integration.WithDefragEnabled(failednodes.PluginName))

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Given: both nodes carry defrag taints, e.g. as defrag
				// candidates of the instance owning them.
				for _, poolName := range []string{stdPoolName, solverPoolName} {
					node := mustGetPoolNode(t, infra, poolName)
					node.Spec.Taints = append(node.Spec.Taints,
						apiv1.Taint{Key: defrag.HardTaint, Value: "1", Effect: apiv1.TaintEffectNoSchedule},
						apiv1.Taint{Key: defrag.SoftTaint, Value: "1", Effect: apiv1.TaintEffectPreferNoSchedule},
					)
					infra.Fakes.K8s.UpdateNode(node)
				}

				// When: the CA instance runs, and none of the nodes is its
				// defrag candidate.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Then: the CA instance removed the taints from its own node
				// only.
				assert.ElementsMatch(t, tc.wantStdNodeTaints, defragTaintKeys(mustGetPoolNode(t, infra, stdPoolName)), "defrag taints on the standard node")
				assert.ElementsMatch(t, tc.wantSolverNodeTaints, defragTaintKeys(mustGetPoolNode(t, infra, solverPoolName)), "defrag taints on the solver node")
			})
		})
	}
}

func defragTaintKeys(node *apiv1.Node) []string {
	var keys []string
	for _, taint := range node.Spec.Taints {
		if taint.Key == defrag.HardTaint || taint.Key == defrag.SoftTaint {
			keys = append(keys, taint.Key)
		}
	}
	return keys
}

// TestDualCA_NodeAnnotatorAnnotatesOnlyOwnedNodes checks that a CA instance
// only sets the ComputeClass priority annotation on its own nodes. The other
// instance's ComputeClasses are not listed by its lister, so it would otherwise
// annotate the other instance's nodes as having a deleted ComputeClass.
func TestDualCA_NodeAnnotatorAnnotatesOnlyOwnedNodes(t *testing.T) {
	testCases := []struct {
		name                string
		autoscalerNames     []string
		wantStdAnnotated    bool
		wantSolverAnnotated bool
	}{
		{
			name:             "standard CA annotates only the standard node",
			autoscalerNames:  standardCANames,
			wantStdAnnotated: true,
		},
		{
			name:                "solver CA annotates only the solver node",
			autoscalerNames:     solverCANames,
			wantSolverAnnotated: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stdPool := newComputeClassPool(stdPoolName, stdComputeClass, 1, "us-central1-a")
			solverPool := newComputeClassPool(solverPoolName, solverComputeClass, 1, "us-central1-a")
			testConfig := newDualCATestConfig(tc.autoscalerNames, stdPool, solverPool)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Given: a standard and a solver node, neither annotated yet.
				// When: the node annotator, which runs in the background once a
				// minute, runs.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

				// Then: only the node of the CA instance got the priority
				// annotation.
				assertPriorityAnnotated(t, mustGetPoolNode(t, infra, stdPoolName), tc.wantStdAnnotated)
				assertPriorityAnnotated(t, mustGetPoolNode(t, infra, solverPoolName), tc.wantSolverAnnotated)
			})
		})
	}
}

// assertPriorityAnnotated asserts that the node has the priority annotation of
// the first ComputeClass priority rule, the only rule of the test
// ComputeClasses, if want is set, or no priority annotation otherwise.
func assertPriorityAnnotated(t *testing.T, node *apiv1.Node, want bool) {
	t.Helper()
	if want {
		assert.Equal(t, "0", node.Annotations[gkelabels.CCCPriorityIndexAnnotationKey], "priority annotation on node %s", node.Name)
	} else {
		assert.NotContains(t, node.Annotations, gkelabels.CCCPriorityIndexAnnotationKey, "priority annotation on node %s", node.Name)
	}
}
