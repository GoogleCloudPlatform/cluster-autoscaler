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

package reconciliation

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

type mockStateManager struct {
	trackedNodes []state.TrackedNode
}

func (m *mockStateManager) List(filters ...state.NodeFilter) []state.TrackedNode {
	// let's make sure filter is used.
	if !slices.Equal(filters, []state.NodeFilter{state.WithoutPendingOperationsFilter}) {
		return nil
	}
	return m.trackedNodes
}

func (m *mockStateManager) Get(nodeName string) (state.TrackedNode, bool) {
	for _, tn := range m.trackedNodes {
		if tn.Node.Name == nodeName {
			return tn, true
		}
	}
	return state.TrackedNode{}, false
}

type mockCloudProvider struct {
	nodeToMig map[string]*gke.GkeMig
	instances map[gce.GceRef]*gce.GceInstance
}

func (m *mockCloudProvider) GkeMigForNode(node *v1.Node) (*gke.GkeMig, error) {
	if mig, ok := m.nodeToMig[node.Name]; ok {
		return mig, nil
	}
	return nil, errors.New("not found")
}

func (m *mockCloudProvider) InstanceByRef(ref gce.GceRef) *gce.GceInstance {
	if m.instances == nil {
		return nil
	}
	return m.instances[ref]
}

type mockWorkQueue struct {
	enqueuedOps []ops.Operation
	err         error
}

func (m *mockWorkQueue) EnqueueWithOpts(o ops.Operation, opts ...state.PendingOperationOpt) error {
	if !slices.Equal(opts, []state.PendingOperationOpt{state.ExclusiveOp}) {
		return errors.New("EXCLUSIVE_OP should be used")
	}
	m.enqueuedOps = append(m.enqueuedOps, o)
	return m.err
}

func TestReconcile(t *testing.T) {
	mig1 := gce.GceRef{Project: "p1", Zone: "z1", Name: "mig1"}
	mig2 := gce.GceRef{Project: "p2", Zone: "z2", Name: "mig2"}

	gkeMig1 := gke.NewTestGkeMigBuilder().SetGceRef(mig1).Build()
	gkeMig2 := gke.NewTestGkeMigBuilder().SetGceRef(mig2).Build()

	node1 := test.CreateNode("node1", test.StateOpt(csn.NodeStateChilling))
	node2 := test.CreateNode("node2", test.StateOpt(csn.NodeStateSuspended))
	node3 := test.CreateNode("node3", test.StateOpt(csn.NodeStateChilling))
	node4 := test.CreateNode("node4", test.StateOpt(csn.NodeStateSuspended))
	node5 := test.CreateNode("node5", test.StateOpt(csn.NodeStateChilling))

	instance := func(status string) *gce.GceInstance {
		return &gce.GceInstance{
			GCEStatus: status,
		}
	}

	instances := func(nodeToStatus map[*v1.Node]string) map[gce.GceRef]*gce.GceInstance {
		result := make(map[gce.GceRef]*gce.GceInstance, len(nodeToStatus))
		for n, status := range nodeToStatus {
			ref, err := gce.GceRefFromProviderId(n.Spec.ProviderID)
			assert.NoError(t, err)
			result[ref] = instance(status)
		}
		return result
	}

	testCases := []struct {
		name            string
		trackedNodes    []state.TrackedNode
		cpNodeToMig     map[string]*gke.GkeMig
		instances       map[gce.GceRef]*gce.GceInstance
		reconcileCalls  int
		maxInvalidCount int
		expectedOps     []ops.Operation
	}{
		{
			name: "no_drift",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateSuspended},
				{Node: node3, State: csn.NodeStateChilling},
				{Node: node4, State: csn.NodeStateSuspended},
				{Node: node5, State: csn.NodeStateChilling},
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node3.Name: gkeMig1,
				node4.Name: gkeMig1,
				node5.Name: gkeMig1,
			},
			instances: instances(map[*v1.Node]string{
				node1: "RUNNING",
				node2: "SUSPENDED",
				node3: "PENDING_STOP",
				node4: "STOPPING",
				node5: "TERMINATED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
		},
		{
			name: "drift_detected_single_mig",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateSuspended},
				{Node: node3, State: csn.NodeStateChilling}, // Drifted (is SUSPENDED)
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node3.Name: gkeMig1,
			},
			instances: instances(map[*v1.Node]string{
				node1: "RUNNING",
				node2: "SUSPENDING",
				node3: "SUSPENDED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
			expectedOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node3.Name),
				},
			},
		},
		{
			name: "drift_not_detected_when_invalid_count_not_reached",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateSuspended},
				{Node: node3, State: csn.NodeStateChilling}, // Drifted (is SUSPENDED)
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node3.Name: gkeMig1,
			},
			instances: instances(map[*v1.Node]string{
				node1: "RUNNING",
				node2: "SUSPENDING",
				node3: "SUSPENDED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 2,
		},
		{
			name: "drift_detected_when_invalid_count_reached",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateSuspended},
				{Node: node3, State: csn.NodeStateChilling}, // Drifted (is SUSPENDED)
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node3.Name: gkeMig1,
			},
			instances: instances(map[*v1.Node]string{
				node1: "RUNNING",
				node2: "SUSPENDING",
				node3: "SUSPENDED",
			}),
			reconcileCalls:  2,
			maxInvalidCount: 2,
			expectedOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node3.Name),
				},
			},
		},
		{
			name: "drift_detected_single_mig",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateSuspended},
				// Drifted - desired state takes precedence
				{Node: node3, State: csn.NodeStateSuspended, DesiredState: csn.NodeStateConsumed},
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node3.Name: gkeMig1,
			},
			instances: instances(map[*v1.Node]string{
				node1: "RUNNING",
				node2: "SUSPENDING",
				node3: "SUSPENDED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
			expectedOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node3.Name),
				},
			},
		},
		{
			name: "drift_detected_multiple_migs",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling}, // Drifted
				{Node: node2, State: csn.NodeStateSuspended},
				{Node: node4, State: csn.NodeStateSuspended}, // Drifted
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node4.Name: gkeMig2,
			},
			instances: instances(map[*v1.Node]string{
				node1: "SUSPENDED",
				node2: "SUSPENDED",
				node4: "RUNNING",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
			expectedOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node1.Name),
				},
				{
					MIG:       mig2,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node4.Name),
				},
			},
		},
		{
			name: "instance_without_mig_ignored",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateChilling},
				{Node: node3, State: csn.NodeStateChilling},
				{Node: node4, State: csn.NodeStateChilling},
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node3.Name: gkeMig2,
				// node 4 doesn't have a MIG.
			},
			instances: instances(map[*v1.Node]string{
				node2: "SUSPENDED",
				node3: "SUSPENDED",
				node4: "SUSPENDED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
			expectedOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node2.Name),
				},
				{
					MIG:       mig2,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node3.Name),
				},
			},
		},
		{
			name: "instance_with_nil_mig_ignored",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateChilling},
				{Node: node3, State: csn.NodeStateChilling},
				{Node: node4, State: csn.NodeStateChilling},
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
				node3.Name: gkeMig2,
				node4.Name: nil,
			},
			instances: instances(map[*v1.Node]string{
				node2: "SUSPENDED",
				node3: "SUSPENDED",
				node4: "SUSPENDED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
			expectedOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node2.Name),
				},
				{
					MIG:       mig2,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node3.Name),
				},
			},
		},
		{
			name: "skip_nodes_with_pending_op",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling, PendingOperations: ops.SuspendOp},
				{Node: node2, State: csn.NodeStateChilling},
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig1,
			},
			instances: instances(map[*v1.Node]string{
				node1: "SUSPENDED",
				node2: "SUSPENDED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
			expectedOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node2.Name),
				},
			},
		},
		{
			name: "instance_not_found_ignored",
			trackedNodes: []state.TrackedNode{
				{Node: node1, State: csn.NodeStateChilling},
				{Node: node2, State: csn.NodeStateChilling},
			},
			cpNodeToMig: map[string]*gke.GkeMig{
				node1.Name: gkeMig1,
				node2.Name: gkeMig2,
			},
			instances: instances(map[*v1.Node]string{
				node2: "SUSPENDED",
			}),
			reconcileCalls:  1,
			maxInvalidCount: 1,
			expectedOps: []ops.Operation{
				{
					MIG:       mig2,
					Type:      ops.ConsumeOp,
					NodeNames: set.New(node2.Name),
				},
			},
		},
		{
			name:            "no_op_without_tracked_nodes",
			reconcileCalls:  1,
			maxInvalidCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				MaxInvalidCount: tc.maxInvalidCount,
			}
			sm := &mockStateManager{trackedNodes: tc.trackedNodes}
			cp := &mockCloudProvider{nodeToMig: tc.cpNodeToMig, instances: tc.instances}
			wq := &mockWorkQueue{}

			r := NewReconciler(sm, cp, wq, cfg)
			for range tc.reconcileCalls {
				r.Reconcile()
			}

			assert.ElementsMatch(t, tc.expectedOps, wq.enqueuedOps)
		})
	}
}
