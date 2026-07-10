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

package nodecontroller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/cfg"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestList(t *testing.T) {
	csnNode := test.CreateNode("csn-Node", test.StateOpt(csn.NodeStateChilling))
	regularNode := test.CreateNode("regular-Node")
	otherCSNNode := test.CreateNode("other-csn-Node", test.StateOpt(csn.NodeStateSuspended))
	csnNodeWithState := CSNNode{
		Name:         csnNode.Name,
		DesiredState: csn.NodeStateChilling,
	}
	otherCSNNodeWithState := CSNNode{
		Name:         otherCSNNode.Name,
		DesiredState: csn.NodeStateSuspended,
	}

	testCases := []struct {
		desc                     string
		initNodes                []*v1.Node
		additionalNodes          []*v1.Node
		deletedNodes             []*v1.Node
		expectedNodesAfterInit   []CSNNode
		expectedNodesAfterAdd    []CSNNode
		expectedNodesAfterDelete []CSNNode
	}{
		{
			desc: "no_nodes",
		},
		{
			desc:                     "init_with_csn_node",
			initNodes:                []*v1.Node{csnNode},
			expectedNodesAfterInit:   []CSNNode{csnNodeWithState},
			expectedNodesAfterAdd:    []CSNNode{csnNodeWithState},
			expectedNodesAfterDelete: []CSNNode{csnNodeWithState},
		},
		{
			desc:      "init_with_regular_node",
			initNodes: []*v1.Node{regularNode},
		},
		{
			desc:                     "add_csn_node_after_init",
			additionalNodes:          []*v1.Node{csnNode},
			expectedNodesAfterAdd:    []CSNNode{csnNodeWithState},
			expectedNodesAfterDelete: []CSNNode{csnNodeWithState},
		},
		{
			desc:                     "delete_csn_node",
			initNodes:                []*v1.Node{csnNode},
			deletedNodes:             []*v1.Node{csnNode},
			expectedNodesAfterInit:   []CSNNode{csnNodeWithState},
			expectedNodesAfterAdd:    []CSNNode{csnNodeWithState},
			expectedNodesAfterDelete: []CSNNode{},
		},
		{
			desc:                     "mixed_nodes",
			initNodes:                []*v1.Node{regularNode},
			additionalNodes:          []*v1.Node{csnNode},
			expectedNodesAfterAdd:    []CSNNode{csnNodeWithState},
			expectedNodesAfterDelete: []CSNNode{csnNodeWithState},
		},
		{
			desc:                     "add_and_delete_different_csn_nodes",
			initNodes:                []*v1.Node{csnNode},
			additionalNodes:          []*v1.Node{otherCSNNode},
			deletedNodes:             []*v1.Node{csnNode},
			expectedNodesAfterInit:   []CSNNode{csnNodeWithState},
			expectedNodesAfterAdd:    []CSNNode{csnNodeWithState, otherCSNNodeWithState},
			expectedNodesAfterDelete: []CSNNode{otherCSNNodeWithState},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, suite := createSuite(t, withInitialNodes(tc.initNodes...), withSkipCacheSync())
				synctest.Wait()

				// Verify after init
				actualNodes, err := c.List()
				assert.NoError(t, err)
				assert.True(t, nodesMatch(actualNodes, tc.expectedNodesAfterInit), "Nodes after init did not match expected nodes")

				// Add nodes after init
				for _, node := range tc.additionalNodes {
					_, err := suite.ClientSet.CoreV1().Nodes().Create(t.Context(), node, metav1.CreateOptions{})
					assert.NoError(t, err)
				}
				synctest.Wait()
				actualNodes, err = c.List()
				assert.NoError(t, err)
				assert.True(t, nodesMatch(actualNodes, tc.expectedNodesAfterAdd), "Nodes after add did not match expected nodes")

				// Delete nodes
				for _, node := range tc.deletedNodes {
					err := suite.ClientSet.CoreV1().Nodes().Delete(t.Context(), node.Name, metav1.DeleteOptions{})
					assert.NoError(t, err)
				}
				synctest.Wait()
				actualNodes, err = c.List()
				assert.NoError(t, err)
				assert.True(t, nodesMatch(actualNodes, tc.expectedNodesAfterDelete), "Nodes after delete did not match expected nodes")
			})
		})
	}
}

func TestReconciliation(t *testing.T) {
	chillingNode := test.CreateNode("chilling-node", test.StateOpt(csn.NodeStateChilling))
	chillingRef, err := gce.GceRefFromProviderId(chillingNode.Spec.ProviderID)
	assert.NoError(t, err)
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	suspendedRef, err := gce.GceRefFromProviderId(suspendedNode.Spec.ProviderID)
	assert.NoError(t, err)
	mig := gke.NewTestGkeMigBuilder().
		SetGceRef(gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}).
		Build()

	synctest.Test(t, func(t *testing.T) {
		var resumeNode atomic.Bool
		instanceForRef := func(migRef gce.GceRef) *gce.GceInstance {
			instances := map[gce.GceRef]*gce.GceInstance{
				chillingRef:  {GCEStatus: "RUNNING"},
				suspendedRef: {GCEStatus: "SUSPENDED"},
			}
			if resumeNode.Load() {
				instances[suspendedRef].GCEStatus = "RUNNING"
			}
			return instances[migRef]
		}
		c, suite := createSuite(t,
			withInitialNodes(chillingNode.DeepCopy(), suspendedNode.DeepCopy()),
			withCloudProvider(&test.MockCloudProvider{
				NodeNameToMIG: map[string]*gke.GkeMig{chillingNode.Name: mig, suspendedNode.Name: mig},
				Instances:     instanceForRef,
			}),
			withSkipCacheSync(),
		)
		synctest.Wait()

		// Verify after init
		gotNodes, err := c.List()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []CSNNode{
			{Name: suspendedNode.Name, DesiredState: csn.NodeStateSuspended},
			{Name: chillingNode.Name, DesiredState: csn.NodeStateChilling},
		}, gotNodes)

		patchChan := mustGetPatchWaitChannel(t, suite.ClientSet, 1)
		resumeNode.Store(true)
		// The node controller should notice that the suspended instance
		// had a change of status. This should trigger consumption, which will
		// remove CSN labels and taints.
		c.Reconcile()

		mustWaitForPatches(t, patchChan)
		synctest.Wait()

		// The state for the previously suspended node should be consumed.
		gotNodes, err = c.List()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []CSNNode{
			{Name: suspendedNode.Name, DesiredState: csn.NodeStateConsumed},
			{Name: chillingNode.Name, DesiredState: csn.NodeStateChilling},
		}, gotNodes)

		// Node should not be recognizable as a CSN node.
		n, err := suite.ClientSet.CoreV1().Nodes().Get(t.Context(), suspendedNode.Name, metav1.GetOptions{})
		assert.Equal(t, csn.NodeStateConsumed, csn.ClassifyNode(n))
	})

}

func TestLogNSoftTaintsPresent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		csnNodes := make([]*v1.Node, 8)
		nodeNames := make([]string, len(csnNodes))
		// Split nodes into two node pools.
		for idx := 0; idx < len(csnNodes); idx++ {
			np := "pool-1"
			if idx >= len(csnNodes)/2 {
				np = "pool-2"
			}
			nodeNames[idx] = fmt.Sprintf("node-%d", idx)
			csnNodes[idx] = test.CreateNode(nodeNames[idx], test.StateOpt(csn.NodeStateChilling), func(node *v1.Node) {
				node.Labels[labels.GkeNodePoolLabel] = np
			})
		}

		var patchChan <-chan bool
		c, suite := createSuite(t,
			withInitialNodes(csnNodes...),
			func(s *controllerTestSuite) {
				patchChan = mustGetPatchWaitChannel(t, s.ClientSet, len(csnNodes)-2)
			},
			withSkipCacheSync(),
		)
		synctest.Wait()

		// Verify after init
		gotNodes, err := c.List()
		assert.NoError(t, err)
		assert.Len(t, gotNodes, len(csnNodes))

		mustWaitForPatches(t, patchChan)

		taintCounts := make(map[int]int)
		for _, n := range nodeNames {
			n, err := suite.ClientSet.CoreV1().Nodes().Get(t.Context(), n, metav1.GetOptions{})
			assert.NoError(t, err)
			c := csn.GetSoftTaintCount(n)
			if _, ok := taintCounts[c]; !ok {
				taintCounts[c] = 0
			}
			taintCounts[c] += 1
		}

		// 1 node in each node pool has 0 taints
		// 2 nodes in each node pool have 1 taint
		// 1 remaining node in each node pools has 2 taints
		assert.Equal(t, map[int]int{
			0: 2,
			1: 4,
			2: 2,
		}, taintCounts)
	})
}

func nodesMatch(actual, expected []CSNNode) bool {
	if len(actual) != len(expected) {
		return false
	}
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].Name < actual[j].Name
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].Name < expected[j].Name
	})
	for i := range actual {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func TestList_PendingOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Node ready for suspension
		node := test.CreateNode("node-to-suspend", test.StateOpt(csn.NodeStateChilling), func(n *v1.Node) {
			n.CreationTimestamp = metav1.Time{Time: time.Now().Add(-15 * time.Minute)}
		})
		mig := gke.NewTestGkeMigBuilder().
			SetGceRef(gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}).
			Build()
		blockSuspend := make(chan struct{})

		c, _ := createSuite(t,
			withInitialNodes(node),
			withCloudProvider(&test.MockCloudProvider{
				BlockSuspend:  blockSuspend,
				NodeNameToMIG: map[string]*gke.GkeMig{node.Name: mig},
				Instances: func(gce.GceRef) *gce.GceInstance {
					return &gce.GceInstance{GCEStatus: "RUNNING"}
				},
			}),
			withSkipCacheSync(),
		)
		synctest.Wait()
		// Initially, node should be visible
		list, err := c.List(WithoutPendingOperationsFilter)
		assert.NoError(t, err)
		assert.Len(t, list, 1)

		// Trigger suspension
		nodes := []*framework.NodeInfo{framework.NewTestNodeInfo(node)}
		_, err = c.MarkAsSuspendable(nodes)
		assert.NoError(t, err)

		// Node should be hidden now because operation is pending (and blocked)
		list, err = c.List(WithoutPendingOperationsFilter)
		assert.NoError(t, err)
		assert.Len(t, list, 0)

		// Node should still be visible without filter
		list, err = c.List()
		assert.NoError(t, err)
		assert.Len(t, list, 1)

		// Unblock
		<-blockSuspend
		synctest.Wait()
		list, err = c.List(WithoutPendingOperationsFilter)
		assert.NoError(t, err)
		assert.Len(t, list, 1)
	})
}

func TestConsume(t *testing.T) {
	testCases := []struct {
		desc         string
		expectResume bool
		node         *v1.Node
	}{
		{
			desc:         "suspended_node_consumption",
			node:         test.CreateNode("node-name", test.StateOpt(csn.NodeStateSuspended)),
			expectResume: true,
		},
		{
			desc: "chilling_node_consumption",
			node: test.CreateNode("node-name", test.StateOpt(csn.NodeStateChilling)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mig := gke.NewTestGkeMigBuilder().
					SetGceRef(gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}).
					Build()
				c, suite := createSuite(t,
					withInitialNodes(tc.node),
					withCloudProvider(&test.MockCloudProvider{
						NodeNameToMIG: map[string]*gke.GkeMig{tc.node.Name: mig},
						Instances: func(gce.GceRef) *gce.GceInstance {
							status := "RUNNING"
							if tc.expectResume {
								status = "SUSPENDED"
							}
							return &gce.GceInstance{GCEStatus: status}
						},
					}),
					withSkipCacheSync(),
				)
				synctest.Wait()
				// Verify node is tracked
				nodes, err := c.List()
				assert.NoError(t, err)
				if len(nodes) != 1 {
					t.Fatalf("Expected to find 1 node in `List` output")
				}
				assert.Equal(t, nodes[0].Name, tc.node.Name)

				// Consume the node
				patchChan := mustGetPatchWaitChannel(t, suite.ClientSet, 1)
				err = c.Consume([]string{tc.node.Name})
				assert.NoError(t, err)

				nodes, err = c.List()
				assert.NoError(t, err)
				if len(nodes) != 1 {
					t.Fatalf("Expected to find 1 node in `List` output")
				}
				assert.Equal(t, nodes[0].Name, tc.node.Name)
				assert.Equal(t, nodes[0].DesiredState, csn.NodeStateConsumed)

				mustWaitForPatches(t, patchChan)

				// Verify the patch effect happened.
				updatedNode, err := suite.ClientSet.CoreV1().Nodes().Get(t.Context(), tc.node.Name, metav1.GetOptions{})
				assert.NoError(t, err)

				assert.Equal(t, csn.NodeStateConsumed, csn.ClassifyNode(updatedNode))

				// Verify resume happened if expected
				if tc.expectResume {
					iRef, err := gce.GceRefFromProviderId(tc.node.Spec.ProviderID)
					assert.NoError(t, err, "Error computing GCE instance reference")
					assert.ElementsMatch(t,
						[]test.ResumeCall{{MIG: mig.GceRef(), Instances: []gce.GceRef{iRef}}},
						suite.CloudProvider.GetResumeCalls())
				} else {
					assert.Empty(t, suite.CloudProvider.GetResumeCalls())
				}
			})
		})
	}
}

func TestMarkAsSuspendable(t *testing.T) {
	buffer := func(initTime string) *v1beta1.CapacityBuffer {
		b := &v1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "buffer-1",
				Namespace:   "default",
				Annotations: map[string]string{},
			},
		}
		if initTime != "" {
			b.Annotations[csn.ColdCapacityInitTimeAnnotationKey] = initTime
		}
		return b
	}

	testCases := []struct {
		desc                string
		node                *v1.Node
		assignBuffer        *v1beta1.CapacityBuffer
		expectedSuspended   []string
		expectedState       csn.NodeState
		nodeTimestampOffset time.Duration
		migBGI              *gke.MigBlueGreenInfo
	}{
		{
			desc: "ready_node_suspension",
			// Node has label, is old enough.
			node:                test.CreateNode("old-node", test.StateOpt(csn.NodeStateChilling)),
			nodeTimestampOffset: -15 * time.Minute,
			expectedSuspended:   []string{"old-node"},
			expectedState:       csn.NodeStateSuspended,
		},
		{
			desc: "young_node_no_suspension",
			// Node has label, is young. Starts without taint.
			node:                test.CreateNode("young-node", test.StateOpt(csn.NodeStateChilling)),
			nodeTimestampOffset: -5 * time.Minute,
			expectedSuspended:   nil,
			expectedState:       csn.NodeStateChilling,
		},
		{
			desc: "untracked_node_ignored",
			// Node has NO label. Old enough.
			node:                test.CreateNode("untracked-node"),
			nodeTimestampOffset: -15 * time.Minute,
			expectedSuspended:   nil,
			expectedState:       "", // Untracked
		},
		{
			desc: "suspended_node_should_not_be_resuspended",
			node: test.CreateNode(
				"suspended-node",
				test.StateOpt(csn.NodeStateSuspended),
			),
			nodeTimestampOffset: -15 * time.Minute,
			expectedSuspended:   nil,
			expectedState:       csn.NodeStateSuspended,
		},
		{
			desc: "young_node_suspended_with_buffer_annotation",
			// Node has label, is young. Starts without taint.
			node:                test.CreateNode("young-node", test.StateOpt(csn.NodeStateChilling)),
			nodeTimestampOffset: -5 * time.Minute,
			expectedSuspended:   []string{"young-node"},
			assignBuffer:        buffer("1m"),
			expectedState:       csn.NodeStateSuspended,
		},
		{
			desc: "young_node_not_suspended_when_annotation_incorrect",
			// Node has label, is young. Starts without taint.
			node:                test.CreateNode("young-node", test.StateOpt(csn.NodeStateChilling)),
			nodeTimestampOffset: -5 * time.Minute,
			assignBuffer:        buffer("1xd"),
			expectedState:       csn.NodeStateChilling,
		},
		{
			// fallback to default
			desc: "old_node_is_suspended_when_annotation_incorrect",
			// Node has label, is young. Starts without taint.
			node:                test.CreateNode("old-node", test.StateOpt(csn.NodeStateChilling)),
			nodeTimestampOffset: -15 * time.Minute,
			expectedSuspended:   []string{"old-node"},
			assignBuffer:        buffer("1xd"),
			expectedState:       csn.NodeStateSuspended,
		},
		{
			desc: "young_node_not_suspended_when_annotation_not_present",
			// Node has label, is young. Starts without taint.
			node:                test.CreateNode("young-node", test.StateOpt(csn.NodeStateChilling)),
			nodeTimestampOffset: -5 * time.Minute,
			assignBuffer:        buffer(""),
			expectedState:       csn.NodeStateChilling,
		},
		{
			desc:                "node_not_suspended_during_blue_green_upgrade",
			node:                test.CreateNode("bg-node", test.StateOpt(csn.NodeStateChilling)),
			nodeTimestampOffset: -15 * time.Minute,
			migBGI: &gke.MigBlueGreenInfo{
				Color:        gke.BlueMig,
				Phase:        gkeclient.PhaseCordoningBluePool,
				IsAutoScaled: true,
			},
			expectedSuspended: nil,
			expectedState:     csn.NodeStateChilling,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// time needs to be set inside the synctest bubble
				tc.node.CreationTimestamp.Time = time.Now().Add(tc.nodeTimestampOffset)
				blockSuspend := make(chan struct{})
				mig := gke.NewTestGkeMigBuilder().
					SetGceRef(gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}).
					SetBlueGreenInfo(tc.migBGI).
					Build()
				c, suite := createSuite(t,
					withInitialNodes(tc.node),
					withCloudProvider(&test.MockCloudProvider{
						BlockSuspend:  blockSuspend,
						NodeNameToMIG: map[string]*gke.GkeMig{tc.node.Name: mig},
						Instances: func(gce.GceRef) *gce.GceInstance {
							return &gce.GceInstance{GCEStatus: "RUNNING"}
						},
					}),
				)
				synctest.Wait()

				if tc.assignBuffer != nil {
					patchChan := mustGetPatchWaitChannel(t, suite.ClientSet, 1)
					c.ProcessBufferAssignment(map[string]*v1beta1.CapacityBuffer{tc.node.Name: tc.assignBuffer})
					mustWaitForPatches(t, patchChan)
				}

				nodes := []*framework.NodeInfo{framework.NewTestNodeInfo(tc.node)}
				suspended, err := c.MarkAsSuspendable(nodes)
				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.expectedSuspended, suspended)

				list, err := c.List()
				assert.NoError(t, err)
				// If we expect it to be untracked, verify it's not in list
				if tc.expectedState == "" {
					assert.Len(t, list, 0, "No nodes should be returned if we don't expect them to be tracked")
					return
				}
				if len(list) != 1 {
					t.Fatalf("Expected to find 1 node in `List` output")
				}

				gotCSNNode := list[0]
				assert.Equal(t, tc.node.Name, gotCSNNode.Name)
				assert.Equal(t, tc.expectedState, gotCSNNode.DesiredState)

				if len(tc.expectedSuspended) > 0 {
					// one mig should result in one suspension call
					<-blockSuspend
				}
				synctest.Wait()

				n, err := suite.ClientSet.CoreV1().Nodes().Get(t.Context(), tc.node.Name, metav1.GetOptions{})
				assert.NoError(t, err, "Error received after getting node")

				gotState := csn.ClassifyNode(n)
				assert.Equal(t, tc.expectedState, gotState)

				if slices.Contains(tc.expectedSuspended, tc.node.Name) {
					suspendCalls := suite.CloudProvider.GetSuspendCalls()
					if l := len(suspendCalls); l != 1 {
						t.Fatalf("1 instance should have been suspended, found %d", l)
					}
					iRef, err := gce.GceRefFromProviderId(n.Spec.ProviderID)
					assert.NoError(t, err, "Error computing GCE instance reference")
					assert.Equal(t, test.SuspendCall{MIG: mig.GceRef(), Instances: []gce.GceRef{iRef}, Force: false}, suspendCalls[0])
				}
			})
		})
	}
}

func TestProcessBufferAssignment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		buffer1 := &v1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "buffer-1",
				Namespace: "default",
			},
		}
		buffer2 := &v1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "buffer-2",
				Namespace: "ns2",
			},
		}
		node1 := test.CreateNode("node1", test.StateOpt(csn.NodeStateChilling))
		node2 := test.CreateNode("node2", test.StateOpt(csn.NodeStateSuspended))
		node3, err := csn.AssignNodeToBufferId(
			test.CreateNode("node3", test.StateOpt(csn.NodeStateSuspended)),
			(&BufferInfo{
				Name:      buffer2.Name,
				Namespace: buffer2.Namespace,
			}).Id(),
		)
		assert.NoError(t, err)

		c, suite := createSuite(t, withInitialNodes(node1.DeepCopy(), node2.DeepCopy(), node3.DeepCopy()), withSkipCacheSync())
		synctest.Wait()

		assignments := map[string]*v1beta1.CapacityBuffer{
			node1.Name: buffer1,
			node2.Name: buffer2,
			node3.Name: buffer2,
		}
		// node3 is already assigned, so it shouldn't trigger a patch
		patchChan := mustGetPatchWaitChannel(t, suite.ClientSet, len(assignments)-1)
		c.ProcessBufferAssignment(assignments)
		mustWaitForPatches(t, patchChan)

		for nodeName, buffer := range assignments {
			n, err := suite.ClientSet.CoreV1().Nodes().Get(t.Context(), nodeName, metav1.GetOptions{})
			assert.NoError(t, err, "Error received after getting node")

			assignedNode, err := csn.AssignNodeToBufferId(n.DeepCopy(), fmt.Sprintf("%s/%s", buffer.Namespace, buffer.Name))
			assert.NoError(t, err)
			assert.Equal(t, assignedNode, n)
		}

		nodes, err := c.List()
		assert.NoError(t, err)
		assert.Len(t, nodes, len(assignments))
		for _, n := range nodes {
			expectedBuffer, ok := assignments[n.Name]
			if !assert.True(t, ok) || !assert.NotNil(t, expectedBuffer) {
				continue
			}
			assert.Equal(t, &BufferInfo{Name: expectedBuffer.Name, Namespace: expectedBuffer.Namespace}, n.Buffer)
		}
	})
}

type controllerTestSuite struct {
	ClientSet     kubernetes.Interface
	CloudProvider *test.MockCloudProvider
	skipCacheSync bool
}

type suiteOpt func(*controllerTestSuite)

func withInitialNodes(nodes ...*v1.Node) suiteOpt {
	objs := make([]runtime.Object, 0, len(nodes))
	for _, n := range nodes {
		objs = append(objs, n)
	}
	return func(suite *controllerTestSuite) {
		suite.ClientSet = fake.NewSimpleClientset(objs...)
	}
}

func withCloudProvider(m *test.MockCloudProvider) suiteOpt {
	return func(suite *controllerTestSuite) {
		suite.CloudProvider = m
	}
}

func withSkipCacheSync() suiteOpt {
	return func(suite *controllerTestSuite) {
		suite.skipCacheSync = true
	}
}

// createSuite configures and runs a CSN Node Controller. The controller
// makes use of dependencies destined to be used in unit tests.
func createSuite(t *testing.T, opts ...suiteOpt) (*csnNodeController, controllerTestSuite) {
	suite := controllerTestSuite{
		ClientSet:     fake.NewSimpleClientset(),
		CloudProvider: &test.MockCloudProvider{},
	}
	for _, opt := range opts {
		opt(&suite)
	}

	experimentsManager := experiments.NewMockManagerWithOptions(
		version.Version{},
		nil,
		map[string]string{
			experiments.ColdStandbyNodesControllerConfigV1Flag: cfg.ExampleControllerJSON,
		},
	)
	factory := informers.NewSharedInformerFactory(suite.ClientSet, 0)
	c := NewCSNNodeController(factory, suite.ClientSet, suite.CloudProvider, experimentsManager)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})
	c.Run(ctx)
	factory.Start(ctx.Done())
	if !suite.skipCacheSync {
		factory.WaitForCacheSync(ctx.Done())
	}
	return c, suite
}

func mustGetPatchWaitChannel(t *testing.T, client kubernetes.Interface, patchCount int) <-chan bool {
	patchChan := make(chan bool, 1)
	watcher, err := client.CoreV1().Nodes().Watch(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to setup watch: %v", err)
	}

	go func() {
		defer close(patchChan)
		defer watcher.Stop()
		for patches := 0; patches < patchCount; {
			select {
			case event, ok := <-watcher.ResultChan():
				if !ok {
					return
				}
				// We care about MODIFIED events (Updates/Patches)
				if event.Type == watch.Modified {
					patches++
				}
			case <-t.Context().Done():
				return
			}
		}
		patchChan <- true
	}()
	return patchChan
}

func mustWaitForPatches(t *testing.T, patchChan <-chan bool) {
	t.Helper()
	if !<-patchChan {
		t.Fatalf("Patches did not happen as expected")
	}
}

func TestBufferInfo_Id(t *testing.T) {
	testCases := []struct {
		desc     string
		bi       *BufferInfo
		expected string
	}{
		{
			desc:     "nil_buffer_info",
			bi:       nil,
			expected: "",
		},
		{
			desc: "simple_buffer_info",
			bi: &BufferInfo{
				Namespace: "default",
				Name:      "buffer-1",
			},
			expected: "default/buffer-1",
		},
		{
			desc: "another namespace",
			bi: &BufferInfo{
				Namespace: "my-ns",
				Name:      "my-buffer",
			},
			expected: "my-ns/my-buffer",
		},
		{
			desc:     "empty buffer info",
			bi:       &BufferInfo{},
			expected: "/",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.bi.Id())
		})
	}
}
