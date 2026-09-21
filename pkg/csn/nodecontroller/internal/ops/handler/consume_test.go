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
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/utils/set"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

type reportResumptionErrorCall struct {
	nodeName       string
	errorCode      string
	errorMessage   string
	instanceStatus string
}

type mockCSNCompositeBackoff struct {
	CSNCompositeBackoff
	reportResumptionErrorCalls []reportResumptionErrorCall
}

func (m *mockCSNCompositeBackoff) ReportResumptionError(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorCode, errorMessage, instanceStatus string) {
	var nodeName string
	if nodeInfo != nil && nodeInfo.Node() != nil {
		nodeName = nodeInfo.Node().Name
	}
	m.reportResumptionErrorCalls = append(m.reportResumptionErrorCalls, reportResumptionErrorCall{
		nodeName:       nodeName,
		errorCode:      errorCode,
		errorMessage:   errorMessage,
		instanceStatus: instanceStatus,
	})
}

// TestConsumeHandler_Handle covers what the handler adds on top of the GCE
// choreography it shares with the suspend handler: which nodes get patched to
// Consumed, and when. The shared part is covered by transition_test.go.
func TestConsumeHandler_Handle(t *testing.T) {
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	suspendedNodeRef := mustGetRef(t, suspendedNode)
	consumedNode := test.CreateNode("consumed-node", test.StateOpt(csn.NodeStateConsumed))
	consumedNodeRef := mustGetRef(t, consumedNode)
	resumingNode := test.CreateNode("resuming-node", test.StateOpt(csn.NodeStateSuspended))
	resumingNodeRef := mustGetRef(t, resumingNode)
	terminatedNode := test.CreateNode("terminated-node", test.StateOpt(csn.NodeStateSuspended))
	terminatedNodeRef := mustGetRef(t, terminatedNode)

	defaultManagedInstances := map[gce.GceRef]*gceclient.ManagedInstance{
		suspendedNodeRef:  {Name: suspendedNode.Name, InstanceStatus: "SUSPENDED", TargetStatus: "SUSPENDED", CurrentAction: "NONE"},
		consumedNodeRef:   {Name: consumedNode.Name, InstanceStatus: "RUNNING", TargetStatus: "RUNNING", CurrentAction: "NONE"},
		resumingNodeRef:   {Name: resumingNode.Name, InstanceStatus: "SUSPENDED", TargetStatus: "RUNNING", CurrentAction: "RESUMING"},
		terminatedNodeRef: {Name: terminatedNode.Name, InstanceStatus: "TERMINATED", TargetStatus: "STOPPED", CurrentAction: "NONE"},
	}

	tests := []struct {
		name                    string
		op                      ops.Operation
		stateManager            *statetest.MockStateManager
		cloudProvider           *test.MockCloudProvider
		k8sClientErr            error
		expectError             bool
		expectedSuccessfulNodes set.Set[string]
		expectedFailedNodes     set.Set[string]
		expectedResumed         []test.ResumeCall
		expectedPolled          []test.PollUntilCall
		expectedPatched         []test.PatchCall
	}{
		{
			name: "wrong_operation_type",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			expectError: true,
		},
		{
			// Nothing left to patch for a node the state manager dropped.
			name: "node_not_found_in_state_manager",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New("unknown-node"),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{},
			},
			expectedSuccessfulNodes: set.New("unknown-node"),
			expectedResumed:         nil,
			expectedPatched:         nil,
		},
		{
			// One node of each category, to check that the handler patches exactly
			// the ones whose instance made it and fails the others: a suspended
			// instance is resumed then patched, one already resuming is only waited
			// on, one already running is patched without touching GCE, and a
			// terminated one cannot be brought back at all.
			name: "mixed_batch_all_four_groups",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name, resumingNode.Name, consumedNode.Name, terminatedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name:  {Node: suspendedNode, State: csn.NodeStateSuspended},
					resumingNode.Name:   {Node: resumingNode, State: csn.NodeStateSuspended},
					consumedNode.Name:   {Node: consumedNode, State: csn.NodeStateConsumed},
					terminatedNode.Name: {Node: terminatedNode, State: csn.NodeStateSuspended},
				},
			},
			expectedSuccessfulNodes: set.New(suspendedNode.Name, resumingNode.Name, consumedNode.Name),
			expectedFailedNodes:     set.New(terminatedNode.Name),
			expectedResumed:         []test.ResumeCall{{MIG: testMIG, Instances: []gce.GceRef{suspendedNodeRef}}},
			expectedPolled:          []test.PollUntilCall{{Action: gceclient.ActionResuming, MIG: testMIG, Instances: []gce.GceRef{suspendedNodeRef, resumingNodeRef}}},
			expectedPatched: []test.PatchCall{
				{Node: consumedNode, State: csn.NodeStateConsumed},
				{Node: suspendedNode, State: csn.NodeStateConsumed},
				{Node: resumingNode, State: csn.NodeStateConsumed},
			},
		},
		{
			// A poll that reports some instances ready before failing must keep the
			// progress it made: only the nodes still in flight are failed, and the
			// ones that made it are patched rather than held back by the batch.
			name: "poll_until_error_after_partial_progress",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name, resumingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
					resumingNode.Name:  {Node: resumingNode, State: csn.NodeStateSuspended},
				},
			},
			cloudProvider: &test.MockCloudProvider{
				ManagedInstances: map[gce.GceRef][]*gceclient.ManagedInstance{
					testMIG: {defaultManagedInstances[suspendedNodeRef], defaultManagedInstances[resumingNodeRef]},
				},
				PollUntilErr:            errors.New("poll error"),
				PollUntilReadyBeforeErr: set.New(suspendedNode.Name),
			},
			expectedSuccessfulNodes: set.New(suspendedNode.Name),
			expectedFailedNodes:     set.New(resumingNode.Name),
			expectedResumed:         []test.ResumeCall{{MIG: testMIG, Instances: []gce.GceRef{suspendedNodeRef}}},
			expectedPolled:          []test.PollUntilCall{{Action: gceclient.ActionResuming, MIG: testMIG, Instances: []gce.GceRef{suspendedNodeRef, resumingNodeRef}}},
			expectedPatched:         []test.PatchCall{{Node: suspendedNode, State: csn.NodeStateConsumed}},
		},
		{
			// The instance is up but the node is not usable until the patch lands.
			name: "patch_node_error",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
				},
			},
			k8sClientErr:        errors.New("patch error"),
			expectedFailedNodes: set.New(suspendedNode.Name),
			expectedResumed:     []test.ResumeCall{{MIG: testMIG, Instances: []gce.GceRef{suspendedNodeRef}}},
			expectedPolled:      []test.PollUntilCall{{Action: gceclient.ActionResuming, MIG: testMIG, Instances: []gce.GceRef{suspendedNodeRef}}},
			expectedPatched:     []test.PatchCall{{Node: suspendedNode, State: csn.NodeStateConsumed}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sClient := &test.MockK8sClient{PatchErr: tc.k8sClientErr}
			cloudProvider := tc.cloudProvider
			if cloudProvider == nil {
				cloudProvider = &test.MockCloudProvider{
					ManagedInstances: map[gce.GceRef][]*gceclient.ManagedInstance{
						testMIG: slices.Collect(maps.Values(defaultManagedInstances)),
					},
				}
			}
			stateManager := tc.stateManager
			if stateManager == nil {
				stateManager = &statetest.MockStateManager{}
			}

			// These cases cover which nodes get patched and when, so every node
			// reports ready right away. The wait itself is covered by
			// TestConsumeHandler_WaitsForResumedNodesToBecomeReady.
			h := NewConsumeHandler(stateManager, cloudProvider, k8sClient, test.ReadyNodeLister(), &mockCSNCompositeBackoff{}, readinessWaitExperiment(true))

			res, err := h.Handle(t.Context(), tc.op)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.expectedSuccessfulNodes.UnsortedList(), res.Success.UnsortedList())
			assert.ElementsMatch(t, tc.expectedFailedNodes.UnsortedList(), keysOf(res.Errs))
			assert.ElementsMatch(t, tc.expectedResumed, cloudProvider.GetResumeCalls())
			assert.ElementsMatch(t, tc.expectedPolled, cloudProvider.GetPollUntilCalls())
			assert.ElementsMatch(t, tc.expectedPatched, k8sClient.GetPatchCalls())
		})
	}
}

// TestConsumeHandler_ReportsResumptionErrors covers what the handler does with
// the non-blocking errors the poll surfaces. GCE keeps retrying the instance, so
// the node is not failed on the spot; the error only feeds the global backoff,
// and only when there is something to back off.
func TestConsumeHandler_ReportsResumptionErrors(t *testing.T) {
	node := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	tracked := state.TrackedNode{Node: node, State: csn.NodeStateSuspended}

	tests := []struct {
		name           string
		backoffEnabled bool
		migForNode     *gke.GkeMig
		getFunc        func(string) (state.TrackedNode, bool)
		expectedCalls  []reportResumptionErrorCall
	}{
		{
			name:           "reports_to_the_backoff",
			backoffEnabled: true,
			migForNode:     &gke.GkeMig{},
			expectedCalls: []reportResumptionErrorCall{{
				nodeName:       node.Name,
				errorCode:      "RESOURCE_NOT_FOUND",
				errorMessage:   "instance not found",
				instanceStatus: "SUSPENDED",
			}},
		},
		{
			name:           "stays_quiet_when_backoff_is_disabled",
			backoffEnabled: false,
			migForNode:     &gke.GkeMig{},
		},
		{
			// Without a node group there is nothing to back off.
			name:           "stays_quiet_when_the_node_has_no_mig",
			backoffEnabled: true,
			migForNode:     nil,
		},
		{
			// The node stopped being tracked while its instance was resuming.
			name:           "stays_quiet_when_the_node_is_no_longer_tracked",
			backoffEnabled: true,
			migForNode:     &gke.GkeMig{},
			getFunc:        trackedThenDropped(node),
		},
		{
			name:           "stays_quiet_when_the_tracked_node_lost_its_node_object",
			backoffEnabled: true,
			migForNode:     &gke.GkeMig{},
			getFunc:        trackedThenWithoutNode(node),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm := &statetest.MockStateManager{
				BackoffEnabled: tc.backoffEnabled,
				Nodes:          map[string]state.TrackedNode{node.Name: tracked},
				GetFunc:        tc.getFunc,
			}
			cp := &test.MockCloudProvider{
				ManagedInstances: map[gce.GceRef][]*gceclient.ManagedInstance{
					testMIG: {test.ManagedInstance(node.Name, "SUSPENDED")},
				},
				Instances:     func(gce.GceRef) *gce.GceInstance { return &gce.GceInstance{GCEStatus: "SUSPENDED"} },
				NodeNameToMIG: map[string]*gke.GkeMig{node.Name: tc.migForNode},
				NonBlockingError: &gceclient.NonBlockingInstanceError{
					Code:    "RESOURCE_NOT_FOUND",
					Message: "instance not found",
				},
				PollUntilErr: errors.New("timeout waiting for instances"),
			}
			backoff := &mockCSNCompositeBackoff{}
			h := NewConsumeHandler(sm, cp, &test.MockK8sClient{}, test.ReadyNodeLister(), backoff, readinessWaitExperiment(true))

			res, err := h.Handle(t.Context(), ops.Operation{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(node.Name),
			})

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedCalls, backoff.reportResumptionErrorCalls)
			// The non-blocking error never fails the node on its own; the timeout that
			// follows it does.
			assert.Contains(t, res.Errs, node.Name)
		})
	}
}

func TestConsumeHandler_WaitsForResumedNodesToBecomeReady(t *testing.T) {
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	runningNode := test.CreateNode("running-node", test.StateOpt(csn.NodeStateChilling))

	tests := []struct {
		name       string
		nodes      []*v1.Node
		nodeLister *test.MockNodeLister
		patchErr   error
		// experimentDisabled turns the readiness wait off, which is what the handler
		// looks like before the experiment rolls out.
		experimentDisabled bool
		// stateManagerGet, when set, replaces the lookups of the state manager.
		stateManagerGet func(nodeName string) (state.TrackedNode, bool)
		expectedSuccess []string
		expectedFailed  []string
		// expectedPatched names the nodes the handler patched. The patch does not
		// wait for the node to be ready, so a node whose patch went out stays
		// patched however its watch turns out.
		expectedPatched []string
		// expectedLookedUp names the nodes the handler looked up in the lister. A
		// node is looked up the moment it is handed to the watcher, so a lookup only
		// says that the wait began, not that it was seen through.
		expectedLookedUp []string
		// expectedWait is how long the handler spends waiting for readiness, read off
		// the fake clock of the bubble, where waiting is the only thing that costs
		// time. It is what tells a wait that was cut short, which costs nothing, from
		// one that ran its course, which costs the whole timeout.
		expectedWait time.Duration
		// expectedObservedWait is the wait the readiness metric is expected to
		// record per status. A status left out is expected to record nothing,
		// which reads the same as a wait of no time at all, as both add nothing to
		// the sum the status holds.
		expectedObservedWait map[string]time.Duration
		// expectedErr is looked for in the error of every failed node.
		expectedErr string
	}{
		{
			// The node is looked up once as it is handed over and once per poll, so it
			// is only ready on the poll after the first tick.
			name:             "succeeds_once_the_node_reports_ready",
			nodes:            []*v1.Node{suspendedNode},
			nodeLister:       readyAfterLookups(2),
			expectedSuccess:  []string{suspendedNode.Name},
			expectedPatched:  []string{suspendedNode.Name},
			expectedLookedUp: []string{suspendedNode.Name},
			expectedWait:     nodeReadinessPollInterval,
			expectedObservedWait: map[string]time.Duration{
				readinessReady: nodeReadinessPollInterval,
			},
		},
		{
			name:             "fails_the_node_that_never_reports_ready",
			nodes:            []*v1.Node{suspendedNode},
			nodeLister:       neverReadyLister(),
			expectedFailed:   []string{suspendedNode.Name},
			expectedPatched:  []string{suspendedNode.Name},
			expectedLookedUp: []string{suspendedNode.Name},
			expectedWait:     nodeReadinessTimeout,
			expectedObservedWait: map[string]time.Duration{
				readinessTimedOut: nodeReadinessTimeout,
			},
			expectedErr: "gave up waiting for node",
		},
		{
			// Nothing is waited on or even looked up with the experiment off: the node
			// is reported as consumed as soon as its patch goes out, however far it is
			// from being ready.
			name:               "does_not_wait_when_the_experiment_is_off",
			nodes:              []*v1.Node{suspendedNode},
			nodeLister:         neverReadyLister(),
			experimentDisabled: true,
			expectedSuccess:    []string{suspendedNode.Name},
			expectedPatched:    []string{suspendedNode.Name},
			expectedLookedUp:   nil,
		},
		{
			name:             "does_not_watch_the_node_whose_instance_was_already_up",
			nodes:            []*v1.Node{runningNode},
			nodeLister:       neverReadyLister(),
			expectedSuccess:  []string{runningNode.Name},
			expectedPatched:  []string{runningNode.Name},
			expectedLookedUp: nil,
		},
		{
			// An empty lister answers the way the informer cache does for a node that
			// was deleted. There is no readiness left to wait for in a node that is no
			// longer there, so the very first lookup ends the wait rather than the
			// timeout ending it five minutes later. The patch went out while the node
			// was still around, so it is reported as consumed.
			name:             "does_not_wait_for_the_node_that_was_deleted",
			nodes:            []*v1.Node{suspendedNode},
			nodeLister:       &test.MockNodeLister{},
			expectedSuccess:  []string{suspendedNode.Name},
			expectedPatched:  []string{suspendedNode.Name},
			expectedLookedUp: []string{suspendedNode.Name},
		},
		{
			name:             "stops_waiting_for_the_node_that_is_deleted_mid_wait",
			nodes:            []*v1.Node{suspendedNode},
			nodeLister:       deletedAfterLookups(2),
			expectedSuccess:  []string{suspendedNode.Name},
			expectedPatched:  []string{suspendedNode.Name},
			expectedLookedUp: []string{suspendedNode.Name},
			expectedWait:     nodeReadinessPollInterval,
			expectedObservedWait: map[string]time.Duration{
				readinessDeleted: nodeReadinessPollInterval,
			},
		},
		{
			// The patch never went out, so the node is not being consumed and its
			// readiness is beside the point: the wait is dropped and the patch error is
			// what the node is failed with.
			name:             "fails_the_node_whose_patch_failed_without_waiting_for_it",
			nodes:            []*v1.Node{suspendedNode},
			nodeLister:       neverReadyLister(),
			patchErr:         errors.New("patch error"),
			expectedFailed:   []string{suspendedNode.Name},
			expectedPatched:  []string{suspendedNode.Name},
			expectedLookedUp: []string{suspendedNode.Name},
			expectedErr:      "failed to patch",
		},
		{
			// A node the state manager let go of is not patched, so nothing is waiting
			// on it becoming ready.
			name:             "succeeds_without_waiting_for_the_node_that_stopped_being_tracked",
			nodes:            []*v1.Node{suspendedNode},
			nodeLister:       neverReadyLister(),
			stateManagerGet:  trackedThenDropped(suspendedNode),
			expectedSuccess:  []string{suspendedNode.Name},
			expectedPatched:  nil,
			expectedLookedUp: []string{suspendedNode.Name},
		},
		{
			// A tracked node missing its node object returned from the state manager is not patched, so nothing is waiting
			// on it becoming ready.
			name:             "succeeds_without_waiting_for_the_tracked_node_that_lost_its_node_object",
			nodes:            []*v1.Node{suspendedNode},
			nodeLister:       neverReadyLister(),
			stateManagerGet:  trackedThenWithoutNode(suspendedNode),
			expectedSuccess:  []string{suspendedNode.Name},
			expectedPatched:  nil,
			expectedLookedUp: []string{suspendedNode.Name},
		},
		{
			name:             "fails_the_node_that_never_reports_ready_without_touching_its_peers",
			nodes:            []*v1.Node{suspendedNode, runningNode},
			nodeLister:       neverReadyLister(),
			expectedSuccess:  []string{runningNode.Name},
			expectedFailed:   []string{suspendedNode.Name},
			expectedPatched:  []string{suspendedNode.Name, runningNode.Name},
			expectedLookedUp: []string{suspendedNode.Name},
			expectedWait:     nodeReadinessTimeout,
			expectedObservedWait: map[string]time.Duration{
				readinessTimedOut: nodeReadinessTimeout,
			},
			expectedErr: "gave up waiting for node",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sm, cp, op := consumeOperation(tc.nodes...)
				sm.GetFunc = tc.stateManagerGet
				k8sClient := &test.MockK8sClient{PatchErr: tc.patchErr}
				h := NewConsumeHandler(sm, cp, k8sClient, tc.nodeLister, &mockCSNCompositeBackoff{}, readinessWaitExperiment(!tc.experimentDisabled))
				deltas := readinessWaitDeltas(t, tc.expectedObservedWait)

				start := time.Now()
				res, err := h.Handle(t.Context(), op)
				waited := time.Since(start)

				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.expectedSuccess, res.Success.UnsortedList())
				assert.ElementsMatch(t, tc.expectedFailed, keysOf(res.Errs))
				assert.ElementsMatch(t, tc.expectedPatched, patchedNodeNames(k8sClient.GetPatchCalls()))
				// A node is looked up as many times as it takes to make up its mind, so
				// only the distinct names say which nodes the handler asked about.
				assert.ElementsMatch(t, tc.expectedLookedUp, set.New(tc.nodeLister.GetCalls()...).UnsortedList())
				// Nothing but the wait for readiness takes time here, so the fake clock
				// says how much of the wait actually happened.
				assert.Equal(t, tc.expectedWait, waited, "unexpected time spent waiting for readiness")
				for status, d := range deltas {
					assert.NoError(t, d.Verify(t), "unexpected wait recorded for status %q", status)
				}
				for nodeName, nodeErr := range res.Errs {
					assert.ErrorContains(t, nodeErr, tc.expectedErr, "unexpected error for node %q", nodeName)
				}
			})
		})
	}
}

// TestReadinessWatcher_KeepsEarlierError covers the precedence between the two
// things that can go wrong with a node: the first failure, a failed patch above
// all, says more about the node than a readiness it was then never given a fair
// chance to show.
//
// The watcher is driven directly here, as the handler has no path that both
// patches a node and hands it back; the rule is there so that one cannot appear
// without the first error being noticed.
func TestReadinessWatcher_KeepsEarlierError(t *testing.T) {
	const nodeName = "suspended-node"

	synctest.Test(t, func(t *testing.T) {
		h := NewConsumeHandler(&statetest.MockStateManager{}, &test.MockCloudProvider{}, &test.MockK8sClient{}, neverReadyLister(), &mockCSNCompositeBackoff{}, readinessWaitExperiment(true))
		watchReadiness, waitForReadiness := h.startReadinessWatcher(t.Context(), 1)
		watchReadiness(nodeName)

		result := ops.NewResult()
		patchErr := errors.New("patch error")
		result.Errs[nodeName] = patchErr
		// The watch runs the node out of time, which the bubble clock sees to as
		// soon as there is nothing else left to do.
		waitForReadiness(&result, set.New(nodeName))

		assert.Equal(t, patchErr, result.Errs[nodeName])
	})
}

func TestReadinessWatcher_GivesUpWhenTheContextIsCancelled(t *testing.T) {
	const nodeName = "suspended-node"
	// waitedBeforeCancelling is short of the poll interval, so that the
	// cancellation rather than a poll is plainly what ends the wait.
	const waitedBeforeCancelling = nodeReadinessPollInterval / 2

	synctest.Test(t, func(t *testing.T) {
		h := NewConsumeHandler(&statetest.MockStateManager{}, &test.MockCloudProvider{}, &test.MockK8sClient{}, neverReadyLister(), &mockCSNCompositeBackoff{}, readinessWaitExperiment(true))
		deltas := readinessWaitDeltas(t, map[string]time.Duration{readinessAborted: waitedBeforeCancelling})

		ctx, cancel := context.WithCancel(t.Context())
		watchReadiness, waitForReadiness := h.startReadinessWatcher(ctx, 1)
		watchReadiness(nodeName)
		// Letting some of the wait happen first is what makes the recorded wait a
		// length worth checking rather than no time at all.
		time.Sleep(waitedBeforeCancelling)
		cancel()

		result := ops.NewResult()
		// The patch went out, so the node was on its way to being reported as
		// consumed until the wait was cut short.
		result.Success.Insert(nodeName)
		waitForReadiness(&result, set.New(nodeName))

		assert.Empty(t, result.Success.UnsortedList(), "a node that was never seen ready is not consumed")
		assert.ErrorIs(t, result.Errs[nodeName], context.Canceled)
		assert.ErrorContains(t, result.Errs[nodeName], "gave up waiting for node")
		for status, d := range deltas {
			assert.NoError(t, d.Verify(t), "unexpected wait recorded for status %q", status)
		}
	})
}

func TestReadinessWatcher_LogPrefix(t *testing.T) {
	tests := []struct {
		name           string
		logPrefix      string
		expectedPrefix string
	}{
		{
			name:           "empty_prefix",
			logPrefix:      "",
			expectedPrefix: nodeReadinessPrefix,
		},
		{
			name:           "with_caller_prefix",
			logPrefix:      consumeHandlerLogPrefix,
			expectedPrefix: consumeHandlerLogPrefix + " " + nodeReadinessPrefix,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newReadinessWatcher(&test.MockNodeLister{}, 1, tc.logPrefix)
			assert.Equal(t, tc.expectedPrefix, w.logPrefix)
		})
	}
}

// readinessWaitExperiment returns an experiments manager that turns the wait for
// readiness on or off.
func readinessWaitExperiment(enabled bool) experiments.Manager {
	return experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.ColdStandbyNodesWaitForNodeReadiness: enabled,
	}, nil)
}

// consumeOperation builds the state manager and the cloud provider of a consume
// operation over nodes, and the operation itself. Each node is backed by an
// instance in the status its state implies: the instance of a suspended node is
// suspended, which is what makes the handler resume it and then wait for the
// node to report ready.
func consumeOperation(nodes ...*v1.Node) (*statetest.MockStateManager, *test.MockCloudProvider, ops.Operation) {
	sm := &statetest.MockStateManager{Nodes: make(map[string]state.TrackedNode, len(nodes))}
	instances := make([]*gceclient.ManagedInstance, 0, len(nodes))
	nodeNames := set.New[string]()
	for _, node := range nodes {
		nodeState := csn.ClassifyNode(node)
		instanceStatus := instanceStatusRunning
		if nodeState == csn.NodeStateSuspended {
			instanceStatus = instanceStatusSuspended
		}
		sm.Nodes[node.Name] = state.TrackedNode{Node: node, State: nodeState}
		instances = append(instances, test.ManagedInstance(node.Name, instanceStatus))
		nodeNames.Insert(node.Name)
	}
	cp := &test.MockCloudProvider{
		ManagedInstances: map[gce.GceRef][]*gceclient.ManagedInstance{testMIG: instances},
	}
	return sm, cp, ops.Operation{MIG: testMIG, Type: ops.ConsumeOp, NodeNames: nodeNames}
}

// answersAfterFirstLookup builds a state manager Get that reports node as
// tracked for the first lookup of each node, the one the operation makes when it
// picks the node up, and answers with tn and ok from then on. Prefer the wrappers
// below, which name the case being set up rather than spelling out the answer.
//
// Lookups are counted per node so that the promise holds for every node of an
// operation rather than only whichever one is looked up first, and under a lock
// because the patch worker and the poll loop both reach the state manager.
func answersAfterFirstLookup(node *v1.Node, tn state.TrackedNode, ok bool) func(string) (state.TrackedNode, bool) {
	var mu sync.Mutex
	lookups := map[string]int{}
	return func(nodeName string) (state.TrackedNode, bool) {
		mu.Lock()
		defer mu.Unlock()
		lookups[nodeName]++
		if lookups[nodeName] > 1 {
			return tn, ok
		}
		return state.TrackedNode{Node: node, State: csn.ClassifyNode(node)}, true
	}
}

// trackedThenDropped reports node as tracked for its first lookup and as no
// longer tracked from then on, which is what a node the state manager lets go of
// mid-operation looks like.
func trackedThenDropped(node *v1.Node) func(string) (state.TrackedNode, bool) {
	return answersAfterFirstLookup(node, state.TrackedNode{}, false)
}

// trackedThenWithoutNode reports node as tracked for its first lookup and as
// still tracked but without a node object from then on, which is what a node the
// state manager is following but can no longer hand out looks like.
func trackedThenWithoutNode(node *v1.Node) func(string) (state.TrackedNode, bool) {
	return answersAfterFirstLookup(node, state.TrackedNode{State: csn.ClassifyNode(node)}, true)
}

// readyAfterLookups returns a lister that reports the node as not ready for the
// first notReadyLookups lookups and as ready from then on, which is what the
// node is like right after its instance is resumed.
func readyAfterLookups(notReadyLookups int) *test.MockNodeLister {
	lookups := 0
	return &test.MockNodeLister{
		GetFunc: func(nodeName string) (*v1.Node, error) {
			lookups++
			return test.CreateNode(nodeName, test.ReadyOpt(lookups > notReadyLookups)), nil
		},
	}
}

// neverReadyLister returns a lister that reports every node as not ready.
func neverReadyLister() *test.MockNodeLister {
	return &test.MockNodeLister{
		GetFunc: func(nodeName string) (*v1.Node, error) {
			return test.CreateNode(nodeName, test.ReadyOpt(false)), nil
		},
	}
}

// deletedAfterLookups returns a lister that reports the node as not ready for
// the first presentLookups lookups and as deleted from then on, which is what a
// node removed while it is being waited on looks like.
func deletedAfterLookups(presentLookups int) *test.MockNodeLister {
	lookups := 0
	return &test.MockNodeLister{
		GetFunc: func(nodeName string) (*v1.Node, error) {
			lookups++
			if lookups > presentLookups {
				return nil, apierrors.NewNotFound(v1.Resource("node"), nodeName)
			}
			return test.CreateNode(nodeName, test.ReadyOpt(false)), nil
		},
	}
}

// readinessWaitDeltas starts watching what the readiness wait metric records,
// and returns what to check once the handler is done. Every status is watched,
// including the ones expected to record nothing, so that a wait recorded under
// the wrong status is caught rather than merely missed.
//
// The metric holds the total time recorded under each status, so the check is
// on the seconds added to a status rather than on the waits behind them.
func readinessWaitDeltas(t *testing.T, expected map[string]time.Duration) map[string]*test.MetricDelta {
	t.Helper()
	deltas := map[string]*test.MetricDelta{}
	for _, status := range []string{readinessReady, readinessDeleted, readinessTimedOut, readinessAborted} {
		d := test.NewMetricDelta(test.ExpectedValue(expected[status].Seconds()), nodeReadinessWaitSeconds, []string{status})
		d.Init(t)
		deltas[status] = d
	}
	return deltas
}

func patchedNodeNames(calls []test.PatchCall) []string {
	names := make([]string, 0, len(calls))
	for _, call := range calls {
		names = append(names, call.Node.Name)
	}
	return names
}
