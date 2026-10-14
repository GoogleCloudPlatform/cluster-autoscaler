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

package ccc_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

const (
	// scaleDownDisabledAnnotation opts a node out of being removed, which makes
	// it impossible to migrate.
	scaleDownDisabledAnnotation = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
	// blockReasonDebounceLoops is the number of consecutive loops a block reason
	// has to hold before it is reported.
	blockReasonDebounceLoops = 3
	// statusFlushDelay is long enough for the status aggregator to flush what
	// the loop produced to the ComputeClass.
	statusFlushDelay = 60 * time.Second
)

// driftCounts is the part of the reported config drift progress a test pins
// down exactly. currentNodes is left out on purpose: how many nodes the
// autoscaler adds while replacing the drifted ones is not something these tests
// control, so it is verified through the partition invariant instead.
type driftCounts struct {
	drifted   int
	migrating int
	// blocked maps a ComputeClass API block reason to the number of nodes
	// reported under it.
	blocked map[string]int
}

// TestCCCNodeConfigDriftStatusBuckets tests that every node of a ComputeClass
// with config drift migration enabled is reported in the bucket describing what
// is actually happening to it.
//
// The block reasons covered here are the ones that hold steadily for as long as
// whatever causes them is in place. Reasons that describe the state of the
// migration itself, such as an exhausted disruption budget, come and go with the
// defrag pipeline: the node holding the budget is picked up and released every
// other loop, so the reason never holds long enough to be reported. That is the
// debouncing doing its job, and is covered by
// TestCCCNodeConfigDriftStatusBlockReasonDebounce instead.
func TestCCCNodeConfigDriftStatusBuckets(t *testing.T) {
	testCases := []struct {
		name string
		// computeClass defaults to a ComputeClass with config drift migration
		// enabled that the drifted node pool does not match.
		computeClass   *cccv1.ComputeClass
		compliantNodes int
		driftedNodes   int
		// prepare makes the drifted nodes blocked before the autoscaler gets to
		// look at them.
		prepare func(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, driftedNodes []*apiv1.Node)
		loops   int
		want    driftCounts
		// wantNoReport expects the ComputeClass status to say nothing about
		// config drift.
		wantNoReport bool
	}{
		{
			name:           "nothing drifted",
			compliantNodes: 2,
			loops:          2,
			want:           driftCounts{},
		},
		{
			name:           "drifted node being migrated",
			compliantNodes: 1,
			driftedNodes:   1,
			loops:          2,
			want:           driftCounts{migrating: 1},
		},
		{
			name:           "drifted node opted out of removal",
			compliantNodes: 1,
			driftedNodes:   1,
			prepare:        annotateNodes(scaleDownDisabledAnnotation, "true"),
			loops:          blockReasonDebounceLoops,
			want:           driftCounts{blocked: map[string]int{"NodeConsolidationDisabled": 1}},
		},
		{
			name:           "drifted node cordoned by hand",
			compliantNodes: 1,
			driftedNodes:   1,
			prepare:        cordonNodes(),
			loops:          blockReasonDebounceLoops,
			want:           driftCounts{blocked: map[string]int{"Cordoned": 1}},
		},
		{
			name:           "drifted node not ready",
			compliantNodes: 1,
			driftedNodes:   1,
			prepare:        makeNodesNotReady(),
			loops:          blockReasonDebounceLoops,
			want:           driftCounts{blocked: map[string]int{"NodeNotReady": 1}},
		},
		{
			name:           "drifted node running a pod that cannot be drained",
			compliantNodes: 1,
			driftedNodes:   1,
			prepare:        scheduleUndrainablePod(),
			loops:          blockReasonDebounceLoops,
			want:           driftCounts{blocked: map[string]int{"BlockingPods": 1}},
		},
		{
			// Neither drifted node has the label, so both share the empty
			// value and form one atomic group.
			name:           "atomic group with one member running a pod that cannot be drained",
			computeClass:   driftStatusComputeClass("drift-status-ccc").WithAtomicGroupLabels("partition").Build(),
			compliantNodes: 1,
			driftedNodes:   2,
			prepare: func(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodes []*apiv1.Node) {
				scheduleUndrainablePod()(ctx, t, infra, nodes[:1])
			},
			loops: blockReasonDebounceLoops,
			want:  driftCounts{blocked: map[string]int{"BlockingPods": 1, "AtomicGroupBlocked": 1}},
		},
		{
			name: "config drift migration disabled",
			computeClass: ccc.NewComputeClassBuilder("drift-status-ccc").
				WithNodePoolConfig(&cccv1.NodePoolConfig{NodeLabels: map[string]string{"tier": "frontend"}}).
				AddPriority(cccv1.Priority{MachineFamily: ptr.To("n2")}).
				WithConfigDrift(false).
				Build(),
			compliantNodes: 1,
			driftedNodes:   1,
			loops:          2,
			wantNoReport:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := tc.computeClass
			if cc == nil {
				cc = driftStatusComputeClass("drift-status-ccc").Build()
			}
			testConfig := driftStatusTestConfig(cc, tc.compliantNodes, tc.driftedNodes).
				WithOverrides(integration.WithComputeClassConfigDriftReportingEnabled())

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				if !assert.NoError(t, err) {
					return
				}

				nodes := listNodes(ctx, t, infra)
				assert.Len(t, nodes, tc.compliantNodes+tc.driftedNodes, "The cluster should start with the configured nodes")
				scheduleWorkloadOnEveryNode(ctx, t, infra, nodes)
				if tc.prepare != nil {
					tc.prepare(ctx, t, infra, driftedNodes(nodes))
				}

				runLoops(ctx, t, autoscaler, tc.loops)

				configDrift := readConfigDriftStatus(ctx, t, infra, cc.Name)
				if tc.wantNoReport {
					assert.Nil(t, configDrift, "Nothing should be reported for a ComputeClass that does not migrate drifted nodes")
					return
				}
				assertConfigDriftCounts(ctx, t, infra, configDrift, tc.want)
			})
		})
	}
}

// TestCCCNodeConfigDriftStatusReportingDisabled tests that no config drift
// progress reaches the ComputeClass status unless the feature is turned on.
func TestCCCNodeConfigDriftStatusReportingDisabled(t *testing.T) {
	testCases := []struct {
		name           string
		disableFlag    bool
		disableExpFlag bool
	}{
		{
			name:        "flag disabled",
			disableFlag: true,
		},
		{
			name:           "experiment disabled",
			disableExpFlag: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := driftStatusComputeClass("drift-status-ccc").Build()
			testConfig := driftStatusTestConfig(cc, 1, 1)
			if !tc.disableFlag {
				testConfig = testConfig.WithOverrides(integration.WithComputeClassConfigDriftReportingEnabled())
			}
			if tc.disableExpFlag {
				testConfig = testConfig.WithExperimentOverrides(
					map[string]bool{experiments.ComputeClassConfigDriftStatusEnabledFlag: false}, nil)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				if !assert.NoError(t, err) {
					return
				}

				scheduleWorkloadOnEveryNode(ctx, t, infra, listNodes(ctx, t, infra))
				runLoops(ctx, t, autoscaler, 2)

				assert.Nil(t, readConfigDriftStatus(ctx, t, infra, cc.Name),
					"Nothing should be reported while config drift reporting is disabled")
			})
		})
	}
}

// TestCCCNodeConfigDriftStatusBlockReasonDebounce tests that a node is reported
// as blocked only once the reason blocking it has held for several loops, and
// is reported as merely drifted until then.
func TestCCCNodeConfigDriftStatusBlockReasonDebounce(t *testing.T) {
	cc := driftStatusComputeClass("drift-status-ccc").Build()
	testConfig := driftStatusTestConfig(cc, 1, 1).
		WithOverrides(integration.WithComputeClassConfigDriftReportingEnabled())

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if !assert.NoError(t, err) {
			return
		}

		nodes := listNodes(ctx, t, infra)
		scheduleWorkloadOnEveryNode(ctx, t, infra, nodes)
		cordonNodes()(ctx, t, infra, driftedNodes(nodes))

		// One loop short of the debounce: the node is known to be drifted, but
		// the reason it is not moving is not trusted yet.
		runLoops(ctx, t, autoscaler, blockReasonDebounceLoops-1)
		assertConfigDriftCounts(ctx, t, infra, readConfigDriftStatus(ctx, t, infra, cc.Name),
			driftCounts{drifted: 1})

		// The reason held, so it is reported.
		runLoops(ctx, t, autoscaler, 1)
		assertConfigDriftCounts(ctx, t, infra, readConfigDriftStatus(ctx, t, infra, cc.Name),
			driftCounts{blocked: map[string]int{"Cordoned": 1}})
	})
}

// TestCCCNodeConfigDriftStatusClearedOnComputeClassChange tests that editing a
// ComputeClass clears the progress reported for it, which was measured against
// the configuration that no longer applies, and that the next loop reports
// progress against the new one.
func TestCCCNodeConfigDriftStatusClearedOnComputeClassChange(t *testing.T) {
	cc := driftStatusComputeClass("drift-status-ccc").Build()
	testConfig := driftStatusTestConfig(cc, 1, 1).
		WithOverrides(integration.WithComputeClassConfigDriftReportingEnabled())

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if !assert.NoError(t, err) {
			return
		}

		scheduleWorkloadOnEveryNode(ctx, t, infra, listNodes(ctx, t, infra))
		runLoops(ctx, t, autoscaler, 2)
		assert.NotNil(t, readConfigDriftStatus(ctx, t, infra, cc.Name),
			"Config drift progress should be reported before the ComputeClass is edited")

		// Requiring a different node label redefines which nodes are drifted,
		// so everything reported so far describes a question nobody is asking
		// any more.
		updateComputeClass(ctx, t, infra, cc.Name, func(updated *cccv1.ComputeClass) {
			updated.Spec.NodePoolConfig.NodeLabels = map[string]string{"tier": "backend"}
		})
		waitForStatusFlush()
		assert.Nil(t, readConfigDriftStatus(ctx, t, infra, cc.Name),
			"The progress measured against the previous configuration should be cleared")

		runLoops(ctx, t, autoscaler, 1)
		assert.NotNil(t, readConfigDriftStatus(ctx, t, infra, cc.Name),
			"The next loop should report progress against the new configuration")
	})
}

// TestCCCNodeConfigDriftStatusClearedWhenMigrationDisabled tests that turning
// config drift migration off clears the progress reported for the ComputeClass
// and that nothing is reported for it afterwards.
func TestCCCNodeConfigDriftStatusClearedWhenMigrationDisabled(t *testing.T) {
	cc := driftStatusComputeClass("drift-status-ccc").Build()
	testConfig := driftStatusTestConfig(cc, 1, 1).
		WithOverrides(integration.WithComputeClassConfigDriftReportingEnabled())

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if !assert.NoError(t, err) {
			return
		}

		scheduleWorkloadOnEveryNode(ctx, t, infra, listNodes(ctx, t, infra))
		runLoops(ctx, t, autoscaler, 2)
		assert.NotNil(t, readConfigDriftStatus(ctx, t, infra, cc.Name),
			"Config drift progress should be reported while the migration is enabled")

		updateComputeClass(ctx, t, infra, cc.Name, func(updated *cccv1.ComputeClass) {
			updated.Spec.ActiveMigration.ConfigDrift = ptr.To(false)
		})

		runLoops(ctx, t, autoscaler, 2)
		assert.Nil(t, readConfigDriftStatus(ctx, t, infra, cc.Name),
			"Nothing should be reported once the ComputeClass stops migrating drifted nodes")
	})
}

// driftStatusComputeClass returns a ComputeClass that migrates drifted nodes and
// requires both the "tier: frontend" node label and the n2 machine family, which
// the drifted node pool of driftStatusTestConfig has neither of.
func driftStatusComputeClass(name string) *ccc.ComputeClassBuilder {
	return ccc.NewComputeClassBuilder(name).
		WithNodePoolConfig(&cccv1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(cccv1.Priority{
			MachineFamily: ptr.To("n2"),
		}).
		WithConfigDrift(true)
}

// driftStatusTestConfig returns an autoscaler that migrates drifted nodes and
// reports what it is doing in the ComputeClass status, running against a cluster
// of nodes that match the given ComputeClass and nodes that do not.
func driftStatusTestConfig(cc *cccv1.ComputeClass, compliantNodes, driftedNodes int) *integration.TestConfig {
	var nodePools []*gke_api_beta.NodePool
	if compliantNodes > 0 {
		nodePools = append(nodePools, integration.DefaultNodePool(
			integration.WithNodePoolName("ng-compliant"),
			integration.WithNodePoolMachineType("n2-standard-2"),
			integration.WithNodePoolSize(int64(compliantNodes)),
			integration.WithNodePoolLocations("us-central1-a"),
			integration.WithNodePoolLabels(map[string]string{
				labels.ComputeClassLabel:  cc.Name,
				labels.MachineFamilyLabel: "n2",
				"tier":                    "frontend",
			}),
			integration.WithNodePoolMin(0),
			integration.WithNodePoolMax(10),
			integration.WithNodePoolAutoscalingEnabled(true),
		))
	}
	if driftedNodes > 0 {
		nodePools = append(nodePools, integration.DefaultNodePool(
			integration.WithNodePoolName("ng-drifted"),
			integration.WithNodePoolMachineType("e2-standard-2"),
			integration.WithNodePoolSize(int64(driftedNodes)),
			integration.WithNodePoolLocations("us-central1-a"),
			integration.WithNodePoolLabels(map[string]string{
				labels.ComputeClassLabel:  cc.Name,
				labels.MachineFamilyLabel: "e2",
			}),
			integration.WithNodePoolMin(0),
			integration.WithNodePoolMax(10),
			integration.WithNodePoolAutoscalingEnabled(true),
		))
	}

	return integration.NewTestConfig().
		WithCccCrds(cc).
		WithNodePools(nodePools...).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
			integration.WithEnhancedCrdStatusReportingEnabled(),
		)
}

// runLoops runs the given number of autoscaler loops and waits for the status
// aggregator to flush what they produced.
func runLoops(ctx context.Context, t *testing.T, autoscaler core.Autoscaler, loops int) {
	t.Helper()
	for range loops {
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
	}
	waitForStatusFlush()
}

// waitForStatusFlush waits for the status aggregator, which patches the
// ComputeClass on a schedule of its own rather than as part of the loop.
func waitForStatusFlush() {
	time.Sleep(statusFlushDelay)
}

func listNodes(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure) []*apiv1.Node {
	t.Helper()
	nodeList, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	nodes := make([]*apiv1.Node, 0, len(nodeList.Items))
	for i := range nodeList.Items {
		nodes = append(nodes, &nodeList.Items[i])
	}
	return nodes
}

// driftedNodes returns the nodes that do not match the ComputeClass of
// driftStatusComputeClass.
func driftedNodes(nodes []*apiv1.Node) []*apiv1.Node {
	var drifted []*apiv1.Node
	for _, node := range nodes {
		if node.Labels[labels.MachineFamilyLabel] == "e2" {
			drifted = append(drifted, node)
		}
	}
	return drifted
}

// scheduleWorkloadOnEveryNode puts a pod using most of the node on every node,
// so that regular scale-down leaves them alone and the config drift migration is
// the only thing that can move them.
//
// The pods are never evicted, since these tests install no eviction mock, which
// holds every migration at the point where the drifted node has been tainted and
// is waiting to be drained.
func scheduleWorkloadOnEveryNode(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodes []*apiv1.Node) {
	t.Helper()
	for i, node := range nodes {
		pod := tu.BuildTestPod(fmt.Sprintf("workload-pod-%d", i), 1200, 100)
		tu.SetRSPodSpec(pod, fmt.Sprintf("rs-%d", i))
		pod.Spec.NodeName = node.Name
		_, err := infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		assert.NoError(t, err)
	}
}

// annotateNodes returns a preparation step annotating every given node.
func annotateNodes(key, value string) func(context.Context, *testing.T, *integration.TestInfrastructure, []*apiv1.Node) {
	return func(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodes []*apiv1.Node) {
		t.Helper()
		updateNodes(ctx, t, infra, nodes, func(node *apiv1.Node) {
			if node.Annotations == nil {
				node.Annotations = map[string]string{}
			}
			node.Annotations[key] = value
		})
	}
}

// cordonNodes returns a preparation step marking every given node unschedulable.
func cordonNodes() func(context.Context, *testing.T, *integration.TestInfrastructure, []*apiv1.Node) {
	return func(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodes []*apiv1.Node) {
		t.Helper()
		updateNodes(ctx, t, infra, nodes, func(node *apiv1.Node) {
			node.Spec.Unschedulable = true
		})
	}
}

// makeNodesNotReady returns a preparation step reporting every given node as not
// ready.
func makeNodesNotReady() func(context.Context, *testing.T, *integration.TestInfrastructure, []*apiv1.Node) {
	return func(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodes []*apiv1.Node) {
		t.Helper()
		updateNodes(ctx, t, infra, nodes, func(node *apiv1.Node) {
			for i := range node.Status.Conditions {
				if node.Status.Conditions[i].Type == apiv1.NodeReady {
					node.Status.Conditions[i].Status = apiv1.ConditionFalse
					node.Status.Conditions[i].LastTransitionTime = metav1.Now()
				}
			}
		})
	}
}

// scheduleUndrainablePod returns a preparation step putting a pod that refuses
// to be evicted on every given node.
func scheduleUndrainablePod() func(context.Context, *testing.T, *integration.TestInfrastructure, []*apiv1.Node) {
	return func(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodes []*apiv1.Node) {
		t.Helper()
		for i, node := range nodes {
			pod := tu.BuildTestPod(fmt.Sprintf("undrainable-pod-%d", i), 10, 10)
			tu.SetRSPodSpec(pod, fmt.Sprintf("undrainable-rs-%d", i))
			pod.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}
			pod.Spec.NodeName = node.Name
			_, err := infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			assert.NoError(t, err)
		}
	}
}

func updateNodes(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodes []*apiv1.Node, mutate func(*apiv1.Node)) {
	t.Helper()
	for _, node := range nodes {
		updated := node.DeepCopy()
		mutate(updated)
		_, err := infra.Fakes.KubeClient.CoreV1().Nodes().Update(ctx, updated, metav1.UpdateOptions{})
		assert.NoError(t, err)
	}
}

// updateComputeClass edits the ComputeClass the way a user would.
func updateComputeClass(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, name string, mutate func(*cccv1.ComputeClass)) {
	t.Helper()
	cccObj, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, name, metav1.GetOptions{})
	if !assert.NoError(t, err) {
		return
	}
	updated := cccObj.DeepCopy()
	mutate(updated)
	// The fake client tracks neither of these, but the autoscaler relies on the
	// API server bumping the generation whenever the spec changes.
	updated.Generation = cccObj.Generation + 1
	updated.ResourceVersion = fmt.Sprintf("%d", updated.Generation+1)
	_, err = infra.Fakes.CccClient.CloudV1().ComputeClasses().Update(ctx, updated, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

// readConfigDriftStatus returns the config drift progress reported for the
// ComputeClass, or nil if none is reported.
func readConfigDriftStatus(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, name string) *cccv1.ConfigDriftStatus {
	t.Helper()
	cccObj, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, name, metav1.GetOptions{})
	if !assert.NoError(t, err) || cccObj.Status.Migration == nil {
		return nil
	}
	return cccObj.Status.Migration.ConfigDrift
}

// assertConfigDriftCounts asserts the reported progress against what the test
// expects, and against the invariant that every node of the ComputeClass is
// counted exactly once.
func assertConfigDriftCounts(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, got *cccv1.ConfigDriftStatus, want driftCounts) {
	t.Helper()
	if !assert.NotNil(t, got, "Config drift progress should be reported") {
		return
	}

	blocked := map[string]int{}
	blockedNodes := 0
	for _, info := range got.BlockedNodes {
		blocked[info.Reason] = info.Count
		blockedNodes += info.Count
	}
	wantBlocked := want.blocked
	if wantBlocked == nil {
		wantBlocked = map[string]int{}
	}
	assert.Equal(t, want.drifted, ptr.Deref(got.DriftedNodes, 0), "Nodes waiting for their migration to start")
	assert.Equal(t, want.migrating, ptr.Deref(got.MigratingNodes, 0), "Nodes being migrated")
	assert.Equal(t, wantBlocked, blocked, "Nodes whose migration cannot proceed")

	// Every node of the cluster belongs to the ComputeClass, and the buckets
	// partition its nodes, so they have to add up to the size of the cluster.
	// This is also what pins down currentNodes, which the test does not predict
	// on its own: how many nodes the autoscaler adds to replace the drifted ones
	// is up to it.
	reported := ptr.Deref(got.CurrentNodes, 0) + ptr.Deref(got.DriftedNodes, 0) + ptr.Deref(got.MigratingNodes, 0) + blockedNodes
	assert.Equal(t, len(listNodes(ctx, t, infra)), reported, "The reported counts should account for every node of the ComputeClass")
	assert.NotNil(t, got.MeasuredAt, "The reported counts should be timestamped")
}
