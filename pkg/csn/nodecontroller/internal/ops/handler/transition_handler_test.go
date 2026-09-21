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

package handler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

// setupHandler builds the handler of the given variant over nodes that all need
// the transition, and returns it along with its mocks.
func setupHandler(t *testing.T, v transitionVariant, nodeNames ...string) (ops.OperationHandler, *test.MockCloudProvider, *test.MockK8sClient) {
	t.Helper()
	nodes := make(map[string]state.TrackedNode, len(nodeNames))
	managed := make([]*gceclient.ManagedInstance, 0, len(nodeNames))
	for _, name := range nodeNames {
		nodes[name] = state.TrackedNode{Node: test.CreateNode(name, test.StateOpt(v.nodeState)), State: v.nodeState}
		managed = append(managed, test.ManagedInstance(name, v.initialStatus))
	}
	cp := &test.MockCloudProvider{
		ManagedInstances: map[gce.GceRef][]*gceclient.ManagedInstance{testMIG: managed},
	}
	kc := &test.MockK8sClient{}
	return v.newHandler(&statetest.MockStateManager{Nodes: nodes}, cp, kc), cp, kc
}

// TestHandlers_StopOnCancelledContext checks that the operation's context
// reaches the poll. Without it a shutdown would leave the worker waiting for the
// instances for as long as the action timeout allows.
func TestHandlers_StopOnCancelledContext(t *testing.T) {
	for _, v := range transitionVariants() {
		t.Run(v.handlerName, func(t *testing.T) {
			handle, _, _ := setupHandler(t, v, "node-1")
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			res, err := handle(ctx, ops.Operation{MIG: testMIG, Type: v.opType, NodeNames: set.New("node-1")})

			// The cancellation is reported per node rather than failing the whole
			// operation, so that the nodes that did make it in time still count as
			// successes.
			assert.NoError(t, err)
			assert.Empty(t, res.Success.UnsortedList())
			assert.ErrorIs(t, res.Errs["node-1"], context.Canceled)
		})
	}
}

// TestHandlers_LargeOperation runs an operation over more nodes than GCE accepts
// in a single call, which is where the handlers have to keep their per-node
// bookkeeping straight across several start calls and one poll. The batching
// itself is covered by TestInstanceTransitioner_Start.
func TestHandlers_LargeOperation(t *testing.T) {
	nodeCount := maxBatchSize*2 + 1
	nodeNames := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeNames = append(nodeNames, fmt.Sprintf("batching-node-%d", i))
	}

	for _, v := range transitionVariants() {
		t.Run(v.handlerName, func(t *testing.T) {
			handle, cp, kc := setupHandler(t, v, nodeNames...)

			res, err := handle(t.Context(), ops.Operation{MIG: testMIG, Type: v.opType, NodeNames: set.New(nodeNames...)})

			assert.NoError(t, err)
			assert.ElementsMatch(t, nodeNames, res.Success.UnsortedList())
			assert.Empty(t, res.Errs)
			assert.Len(t, kc.GetPatchCalls(), nodeCount)

			// Every batch is dispatched before the polling starts, so a single poll
			// covers all the accepted instances. Their order is not stable, since
			// categorization walks the operation's node name set.
			pollCalls := cp.GetPollUntilCalls()
			assert.Len(t, pollCalls, 1)
			assert.Equal(t, v.action, pollCalls[0].Action)
			assert.Equal(t, testMIG, pollCalls[0].MIG)
			assert.ElementsMatch(t, instanceRefs(nodeNames...), pollCalls[0].Instances)
		})
	}
}
