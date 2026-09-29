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
	"cmp"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

// testMIG is the MIG every test in this package operates on. It matches the
// provider IDs test.CreateNode builds, so that instanceRef can derive an
// instance ref from a node name.
var testMIG = gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}

// instanceRef is the ref of the instance backing the node named name.
func instanceRef(name string) gce.GceRef {
	return gce.GceRef{Project: testMIG.Project, Zone: testMIG.Zone, Name: name}
}

func instanceRefs(names ...string) []gce.GceRef {
	refs := make([]gce.GceRef, 0, len(names))
	for _, name := range names {
		refs = append(refs, instanceRef(name))
	}
	return refs
}

func mustGetRef(t *testing.T, n *v1.Node) gce.GceRef {
	t.Helper()
	ref, err := gce.GceRefFromProviderId(n.Spec.ProviderID)
	assert.NoError(t, err, "Failed to extract GceRef from node %q", n.Name)
	return ref
}

// startCall is the shape test.ResumeCall and test.SuspendCall have in common, so
// that a case can say what it expects without knowing which of the two it gets.
type startCall struct {
	MIG       gce.GceRef
	Instances []gce.GceRef
	// Force is only ever set by suspend, and only if the handler asks for it,
	// which it never does today.
	Force bool
}

// transitionVariant describes one of the two directions CSN moves a node in:
// the GCE transition itself, and the handler that drives it. The two directions
// are the same choreography with different statuses and calls, so every test in
// this file runs its whole table against both rather than being written twice.
type transitionVariant struct {
	// name is the transition's name. instanceTransitioner uses it in its error
	// messages, so it is not just a subtest label.
	name string
	// handlerName names the handler driving the transition. It differs from name
	// because a node is consumed by resuming the instance behind it.
	handlerName string
	opType      ops.OperationType
	// nodeState is the CSN state the nodes are in when the operation starts.
	nodeState csn.NodeState
	// action is the CurrentAction GCE reports while the transition is in flight.
	action gceclient.InstanceAction
	// initialStatus and finalStatus mirror the transitioner's own statuses.
	initialStatus string
	finalStatus   string
	// callType is the opGceBatchSize label for the start call.
	callType string

	newTransitioner func(StateManager, CloudProvider) instanceTransitionerImpl
	newHandler      func(StateManager, CloudProvider, K8sClient) ops.OperationHandler
	// setStartErr makes the start call of this variant fail.
	setStartErr func(cp *test.MockCloudProvider, err error)
	// startCalls normalizes the calls the mock recorded for this variant, since
	// test.ResumeCall and test.SuspendCall are distinct types.
	startCalls func(cp *test.MockCloudProvider) []startCall
}

func transitionVariants() []transitionVariant {
	return []transitionVariant{
		{
			name:            "resume",
			handlerName:     "consume",
			opType:          ops.ConsumeOp,
			nodeState:       csn.NodeStateSuspended,
			action:          gceclient.ActionResuming,
			initialStatus:   instanceStatusSuspended,
			finalStatus:     instanceStatusRunning,
			callType:        resumeCall,
			newTransitioner: newResumeTransitioner,
			newHandler: func(sm StateManager, cp CloudProvider, kc K8sClient) ops.OperationHandler {
				return NewConsumeHandler(sm, cp, kc, test.ReadyNodeLister(), nil /* backoff */, readinessWaitExperiment(true)).Handle
			},
			setStartErr: func(cp *test.MockCloudProvider, err error) { cp.ResumeErr = err },
			startCalls: func(cp *test.MockCloudProvider) []startCall {
				var calls []startCall
				for _, c := range cp.GetResumeCalls() {
					calls = append(calls, startCall{MIG: c.MIG, Instances: c.Instances})
				}
				return calls
			},
		},
		{
			name:            "suspend",
			handlerName:     "suspend",
			opType:          ops.SuspendOp,
			nodeState:       csn.NodeStateChilling,
			action:          gceclient.ActionSuspending,
			initialStatus:   instanceStatusRunning,
			finalStatus:     instanceStatusSuspended,
			callType:        suspendCall,
			newTransitioner: newSuspendTransitioner,
			newHandler: func(sm StateManager, cp CloudProvider, kc K8sClient) ops.OperationHandler {
				noopEnqueue := func(ops.Operation) error { return nil }
				return NewSuspendHandler(sm, cp, kc, noopEnqueue, 0 /* beforeSuspend */).Handle
			},
			setStartErr: func(cp *test.MockCloudProvider, err error) { cp.SuspendErr = err },
			startCalls: func(cp *test.MockCloudProvider) []startCall {
				var calls []startCall
				for _, c := range cp.GetSuspendCalls() {
					calls = append(calls, startCall{MIG: c.MIG, Instances: c.Instances, Force: c.Force})
				}
				return calls
			},
		},
	}
}

// instanceKind names where an instance stands relative to the transition under
// test, so that a case can describe it once and have it rendered into the
// concrete GCE statuses of whichever variant is running.
type instanceKind int

const (
	// needsStart has not been asked to transition yet.
	needsStart instanceKind = iota
	// alreadyStarted is already being transitioned by GCE.
	alreadyStarted
	// alreadyDone reached the final status and settled there.
	alreadyDone
	// terminal converges to a status the transition can never reach.
	terminal
	// unsupportedState converges to the final status without actively transitioning towards it.
	unsupportedState
)

func (k instanceKind) managedInstance(v transitionVariant, name string) *gceclient.ManagedInstance {
	switch k {
	case needsStart:
		return &gceclient.ManagedInstance{Name: name, InstanceStatus: v.initialStatus, TargetStatus: v.initialStatus, CurrentAction: "NONE"}
	case alreadyStarted:
		return &gceclient.ManagedInstance{Name: name, InstanceStatus: v.initialStatus, TargetStatus: v.finalStatus, CurrentAction: string(v.action)}
	case alreadyDone:
		return &gceclient.ManagedInstance{Name: name, InstanceStatus: v.finalStatus, TargetStatus: v.finalStatus, CurrentAction: "NONE"}
	case terminal:
		return &gceclient.ManagedInstance{Name: name, InstanceStatus: "TERMINATED", TargetStatus: "STOPPED", CurrentAction: "NONE"}
	case unsupportedState:
		return &gceclient.ManagedInstance{Name: name, InstanceStatus: v.initialStatus, TargetStatus: v.finalStatus, CurrentAction: "NONE"}
	default:
		panic(fmt.Sprintf("unknown instance kind %d", k))
	}
}

func TestInstanceTransitioner_CategorizeInstances(t *testing.T) {
	tests := []struct {
		name string
		// instances is the state GCE reports for each node of the operation.
		instances map[string]instanceKind
		// The nodes below are part of the operation but cannot be placed in a
		// bucket, each for its own reason.
		untracked         []string
		invalidProviderID []string
		missingFromMIG    []string
		fetchErr          error

		expectedToStart    []string
		expectedInProgress []string
		expectedCompleted  []string
		expectedSuccess    []string
		expectedErrs       []string
		expectedErrSubstr  map[string]string
	}{
		{
			name: "splits_instances_by_the_work_they_still_need",
			instances: map[string]instanceKind{
				"to-start":       needsStart,
				"in-progress":    alreadyStarted,
				"already-done":   alreadyDone,
				"on-its-way-out": terminal,
			},
			expectedToStart:    []string{"to-start"},
			expectedInProgress: []string{"in-progress"},
			expectedCompleted:  []string{"already-done"},
			// A terminal instance will never reach the final status, so it is failed
			// right away instead of being polled until the timeout.
			expectedErrs: []string{"on-its-way-out"},
		},
		{
			// Nothing is left to do for a node the state manager dropped while the
			// operation was queued.
			name:            "untracked_node_is_an_immediate_success",
			untracked:       []string{"forgotten"},
			expectedSuccess: []string{"forgotten"},
		},
		{
			name:              "node_without_a_usable_provider_id_fails",
			invalidProviderID: []string{"malformed"},
			expectedErrs:      []string{"malformed"},
		},
		{
			name:           "node_whose_instance_the_mig_does_not_report_fails",
			missingFromMIG: []string{"ghost"},
			expectedErrs:   []string{"ghost"},
		},
		{
			// TargetStatus is finalStatus, but the instance has not reached it
			// yet and is not actively transitioning either.
			name: "node_with_unsupported_instance_state_fails",
			instances: map[string]instanceKind{
				"unsupported": unsupportedState,
			},
			expectedErrs: []string{"unsupported"},
			expectedErrSubstr: map[string]string{
				"unsupported": "unsupported instance state",
			},
		},
		{
			// Without the MIG's view there is no way to tell what any instance needs.
			name:         "fetch_error_fails_every_node",
			instances:    map[string]instanceKind{"to-start": needsStart},
			fetchErr:     errors.New("gce api error"),
			expectedErrs: []string{"to-start"},
		},
	}

	for _, v := range transitionVariants() {
		t.Run(v.name, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					nodeNames := set.New[string]()
					nodes := map[string]state.TrackedNode{}
					var managed []*gceclient.ManagedInstance
					for name, kind := range tc.instances {
						nodeNames.Insert(name)
						nodes[name] = state.TrackedNode{Node: test.CreateNode(name)}
						managed = append(managed, kind.managedInstance(v, name))
					}
					for _, name := range tc.untracked {
						nodeNames.Insert(name)
					}
					for _, name := range tc.invalidProviderID {
						nodeNames.Insert(name)
						nodes[name] = state.TrackedNode{Node: test.CreateNode(name, func(n *v1.Node) { n.Spec.ProviderID = "" })}
					}
					for _, name := range tc.missingFromMIG {
						nodeNames.Insert(name)
						nodes[name] = state.TrackedNode{Node: test.CreateNode(name)}
					}

					cp := &test.MockCloudProvider{
						ManagedInstances:         map[gce.GceRef][]*gceclient.ManagedInstance{testMIG: managed},
						FetchManagedInstancesErr: tc.fetchErr,
					}
					tr := v.newTransitioner(&statetest.MockStateManager{Nodes: nodes}, cp)

					res := ops.NewResult()
					got := tr.categorizeInstances(testMIG, nodeNames, &res)

					assert.ElementsMatch(t, instanceRefs(tc.expectedToStart...), got.toStart)
					assert.ElementsMatch(t, instanceRefs(tc.expectedInProgress...), got.inProgress)
					assert.ElementsMatch(t, instanceRefs(tc.expectedCompleted...), got.completed)
					assert.ElementsMatch(t, tc.expectedSuccess, res.Success.UnsortedList())
					assert.ElementsMatch(t, tc.expectedErrs, keysOf(res.Errs))
					for name, substr := range tc.expectedErrSubstr {
						assert.ErrorContains(t, res.Errs[name], substr)
					}
				})
			}
		})
	}
}

func TestInstanceTransitioner_Start(t *testing.T) {
	tests := []struct {
		name string
		// toStart and inProgress are counts, since only the number of instances
		// matters here.
		toStart    int
		inProgress int
		startErr   error

		expectedBatchSizes []int
		// expectedPolled is how many instances start hands back for polling.
		expectedPolled int
		expectedErrs   int
		// The metric observes the size of each batch, so these are sums.
		expectedSuccessObserved float64
		expectedFailureObserved float64
	}{
		{
			// Instances GCE is already transitioning need no call, only polling.
			name:                    "starts_the_pending_instances_and_polls_them_with_the_in_flight_ones",
			toStart:                 3,
			inProgress:              2,
			expectedBatchSizes:      []int{3},
			expectedPolled:          5,
			expectedSuccessObserved: 3,
		},
		{
			name:                    "splits_calls_larger_than_the_gce_limit",
			toStart:                 maxBatchSize*2 + 1,
			expectedBatchSizes:      []int{maxBatchSize, maxBatchSize, 1},
			expectedPolled:          maxBatchSize*2 + 1,
			expectedSuccessObserved: maxBatchSize*2 + 1,
		},
		{
			// GCE never took the instances, so there is nothing to wait for.
			name:                    "rejected_batch_fails_its_instances_and_is_not_polled",
			toStart:                 3,
			inProgress:              2,
			startErr:                errors.New("gce rejected the call"),
			expectedBatchSizes:      []int{3},
			expectedPolled:          2,
			expectedErrs:            3,
			expectedFailureObserved: 3,
		},
	}

	for _, v := range transitionVariants() {
		t.Run(v.name, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					var categorized categorizedInstances
					for i := 0; i < tc.toStart; i++ {
						categorized.toStart = append(categorized.toStart, instanceRef(fmt.Sprintf("to-start-%d", i)))
					}
					for i := 0; i < tc.inProgress; i++ {
						categorized.inProgress = append(categorized.inProgress, instanceRef(fmt.Sprintf("in-progress-%d", i)))
					}

					cp := &test.MockCloudProvider{}
					if tc.startErr != nil {
						v.setStartErr(cp, tc.startErr)
					}
					tr := v.newTransitioner(&statetest.MockStateManager{}, cp)

					deltas := []*test.MetricDelta{
						test.NewMetricDelta(test.ExpectedValue(tc.expectedSuccessObserved), opGceBatchSize, []string{v.callType, gceSuccess}),
						test.NewMetricDelta(test.ExpectedValue(tc.expectedFailureObserved), opGceBatchSize, []string{v.callType, gceFailure}),
					}
					for _, d := range deltas {
						d.Init(t)
					}

					res := ops.NewResult()
					toPoll := tr.start(testMIG, categorized, &res)

					for _, d := range deltas {
						assert.NoError(t, d.Verify(t))
					}
					assert.Len(t, toPoll, tc.expectedPolled)
					assert.Len(t, res.Errs, tc.expectedErrs)

					calls := v.startCalls(cp)
					var gotBatchSizes []int
					var calledRefs []gce.GceRef
					for _, c := range calls {
						assert.Equal(t, testMIG, c.MIG)
						assert.False(t, c.Force, "the handlers never force the transition")
						gotBatchSizes = append(gotBatchSizes, len(c.Instances))
						calledRefs = append(calledRefs, c.Instances...)
					}
					assert.Equal(t, tc.expectedBatchSizes, gotBatchSizes)
					assert.ElementsMatch(t, categorized.toStart, calledRefs)
				})
			}
		})
	}
}

func TestInstanceTransitioner_PollUntilDone(t *testing.T) {
	tests := []struct {
		name string
		// instances are polled in this order, which the mock preserves.
		instances []string
		pollErr   error
		// readyBeforeErr complete before pollErr ends the poll.
		readyBeforeErr []string
		// nonBlockingErr, when set, is what GCE reports for every instance that does
		// not complete.
		nonBlockingErr *gceclient.NonBlockingInstanceError
		cancelCtx      bool
		// stopAfterFirstYield breaks out of the iterator, which must stop the poll.
		stopAfterFirstYield bool

		expectedCompleted []string
		expectedRetrying  []string
		expectedErrs      []string
		// expectedErrCode is the error code every failed instance is reported
		// with. Defaults to ops.ErrorCodeGKEInternal.
		expectedErrCode string
	}{
		{
			name:              "every_instance_completes",
			instances:         []string{"a", "b"},
			expectedCompleted: []string{"a", "b"},
		},
		{
			// A poll that made progress before giving up must keep it.
			name:              "timeout_fails_only_the_instances_still_pending",
			instances:         []string{"a", "b"},
			pollErr:           errors.New("timeout waiting for instances"),
			readyBeforeErr:    []string{"a"},
			expectedCompleted: []string{"a"},
			expectedErrs:      []string{"b"},
		},
		{
			// GCE keeps retrying, so a non-blocking error is surfaced to the caller
			// but does not fail the instance by itself; only the timeout does. The
			// instance is then failed with the code of that error, so that a
			// stockout is not retried like a misconfiguration. Which code maps to
			// which canonical code is ops.ErrorCodeFromGCEError's own business.
			name:             "non_blocking_error_is_yielded_before_the_timeout",
			instances:        []string{"a"},
			pollErr:          errors.New("timeout waiting for instances"),
			nonBlockingErr:   &gceclient.NonBlockingInstanceError{Code: "ZONE_RESOURCE_POOL_EXHAUSTED", Message: "zone out of capacity"},
			expectedRetrying: []string{"a"},
			expectedErrs:     []string{"a"},
			expectedErrCode:  gce.ErrorCodeResourcePoolExhausted,
		},
		{
			// Between two updates there is no loop body to break out of, so
			// cancelling is the only way to stop waiting.
			name:         "cancelled_context_fails_the_instances_still_pending",
			instances:    []string{"a", "b"},
			cancelCtx:    true,
			expectedErrs: []string{"a", "b"},
		},
		{
			name:                "breaking_out_of_the_loop_stops_the_poll",
			instances:           []string{"a", "b"},
			stopAfterFirstYield: true,
			expectedCompleted:   []string{"a"},
		},
	}

	for _, v := range transitionVariants() {
		t.Run(v.name, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					cp := &test.MockCloudProvider{
						PollUntilErr:            tc.pollErr,
						PollUntilReadyBeforeErr: set.New(tc.readyBeforeErr...),
						NonBlockingError:        tc.nonBlockingErr,
					}
					tr := v.newTransitioner(&statetest.MockStateManager{}, cp)

					ctx := t.Context()
					if tc.cancelCtx {
						cancellable, cancel := context.WithCancel(ctx)
						cancel()
						ctx = cancellable
					}

					var completed, retrying []string
					res := ops.NewResult()
					for ref, nonBlockingErr := range tr.pollUntilDone(ctx, testMIG, instanceRefs(tc.instances...), &res) {
						if nonBlockingErr != nil {
							retrying = append(retrying, ref.Name)
						} else {
							completed = append(completed, ref.Name)
						}
						if tc.stopAfterFirstYield {
							break
						}
					}

					assert.Equal(t, tc.expectedCompleted, completed)
					assert.Equal(t, tc.expectedRetrying, retrying)
					assert.ElementsMatch(t, tc.expectedErrs, keysOf(res.Errs))
					assert.ElementsMatch(t, []test.PollUntilCall{
						{Action: v.action, MIG: testMIG, Instances: instanceRefs(tc.instances...)},
					}, cp.GetPollUntilCalls())
					expectedCode := cmp.Or(tc.expectedErrCode, ops.ErrorCodeGKEInternal)
					for _, name := range tc.expectedErrs {
						assert.ErrorContains(t, res.Errs[name], fmt.Sprintf("failed to wait for instance to %s", v.name))
						assert.Equal(t, expectedCode, ops.ExtractErrorCode(res.Errs[name]))
						if tc.cancelCtx {
							assert.ErrorIs(t, res.Errs[name], context.Canceled)
						}
					}
				})
			}
		})
	}
}

func keysOf(errs map[string]error) []string {
	names := make([]string, 0, len(errs))
	for name := range errs {
		names = append(names, name)
	}
	return names
}
