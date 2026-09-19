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

package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	cc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	scaledownstatus "sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
)

const (
	testCrdLabel = "test-crd-label"
	testCrdName  = "default-crd-test"
	// testPriority is the identifier of the only priority of the test ComputeClass.
	testPriority = "0"
)

var testCrdID = status.CRDId{CRDName: testCrdName, CRDLabel: testCrdLabel}

func TestScaleDownBlockedStatusProcessorMapReason(t *testing.T) {
	sliceNode := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "slice-node",
		Labels: map[string]string{gke_labels.TPUSliceLabel: "my-slice"},
	}}
	plainNode := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "plain-node"}}
	fakePod := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:        "min-nodes-fake-ccc-pod-my-ccc-0",
		Annotations: map[string]string{cc_processors.MinCapacityFakePodAnnotation: "true"},
	}}
	realPod := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "real-pod"}}

	testCases := []struct {
		name        string
		unremovable *scaledownstatus.UnremovableNode
		nodeInfo    *framework.NodeInfo
		want        string
	}{
		{
			name:        "not underutilized is reported as above utilization threshold",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NotUnderutilized},
			want:        crd.ConsolidationReasonAboveUtilizationThreshold,
		},
		{
			name:        "unneeded but not for long enough",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NotUnneededLongEnough},
			want:        crd.ConsolidationReasonNotUnneededLongEnough,
		},
		{
			name:        "scale down of unready nodes disabled",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.ScaleDownUnreadyDisabled},
			want:        crd.ConsolidationReasonNodeNotReady,
		},
		{
			name:        "not unready long enough",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NotUnreadyLongEnough},
			want:        crd.ConsolidationReasonNodeNotReady,
		},
		{
			name:        "no place to move pods is its own reason",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NoPlaceToMovePods},
			want:        crd.ConsolidationReasonNoPlaceToMovePods,
		},
		{
			// The fake pods holding a compute class floor fill their node, so the drain
			// simulation reports NoPlaceToMovePods.
			name:        "no place to move pods with only min capacity fake pods is reported as min capacity",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NoPlaceToMovePods},
			nodeInfo:    framework.NewTestNodeInfo(plainNode, fakePod),
			want:        crd.ConsolidationReasonMinCapacityReached,
		},
		{
			name:        "no place to move pods with a real pod next to the fake pod stays no place to move pods",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NoPlaceToMovePods},
			nodeInfo:    framework.NewTestNodeInfo(plainNode, fakePod, realPod),
			want:        crd.ConsolidationReasonNoPlaceToMovePods,
		},
		{
			name: "pod blocked by pdb is reported as pod disruption budget",
			unremovable: &scaledownstatus.UnremovableNode{
				Node:        plainNode,
				Reason:      simulator.BlockedByPod,
				BlockingPod: &drain.BlockingPod{Reason: drain.NotEnoughPdb},
			},
			want: crd.ConsolidationReasonPodDisruptionBudget,
		},
		{
			name: "pod blocked for any other reason is reported as blocking pods",
			unremovable: &scaledownstatus.UnremovableNode{
				Node:        plainNode,
				Reason:      simulator.BlockedByPod,
				BlockingPod: &drain.BlockingPod{Reason: drain.UnmovableKubeSystemPod},
			},
			want: crd.ConsolidationReasonBlockingPods,
		},
		{
			name:        "blocked by pod without blocking pod details",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.BlockedByPod},
			want:        crd.ConsolidationReasonBlockingPods,
		},
		{
			name:        "on completion pod",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.BlockedByOnCompletionPod},
			want:        crd.ConsolidationReasonBlockingPods,
		},
		{
			name:        "atomic group",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.AtomicScaleDownFailed},
			want:        crd.ConsolidationReasonAtomicGroupBlocked,
		},
		{
			name:        "node group min size",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NodeGroupMinSizeReached},
			want:        crd.ConsolidationReasonMinCapacityReached,
		},
		{
			name:        "cluster wide resource limits",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.MinimalResourceLimitExceeded},
			want:        crd.ConsolidationReasonMinCapacityReached,
		},
		{
			name:        "scale down disabled annotation on a slice bound node",
			unremovable: &scaledownstatus.UnremovableNode{Node: sliceNode, Reason: simulator.ScaleDownDisabledAnnotation},
			want:        crd.ConsolidationReasonUsedByFormedSlice,
		},
		{
			// The blocking label is configured, not hardcoded: a node carrying a label that is
			// not configured as blocking is just a node that opted out of scale-down.
			name: "scale down disabled annotation on a node whose label is not configured as blocking",
			unremovable: &scaledownstatus.UnremovableNode{
				Node: &apiv1.Node{ObjectMeta: metav1.ObjectMeta{
					Name:   "other-label-node",
					Labels: map[string]string{"example.com/some-other-label": "x"},
				}},
				Reason: simulator.ScaleDownDisabledAnnotation,
			},
			want: crd.ConsolidationReasonNodeConsolidationDisabled,
		},
		{
			name:        "scale down disabled annotation without a slice label",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.ScaleDownDisabledAnnotation},
			want:        crd.ConsolidationReasonNodeConsolidationDisabled,
		},
		{
			name:        "per loop removal cap reached is not a blocked reason",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NodeGroupMaxDeletionCountReached},
			want:        "",
		},
		{
			name:        "never inspected is not a blocked reason",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NotUnneededOtherReason},
			want:        "",
		},
		{
			name:        "unexpected error falls back to the catch all",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.UnexpectedError},
			want:        crd.ConsolidationReasonConsolidationBlocked,
		},
		{
			name:        "missing node info falls back to the catch all",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NoNodeInfo},
			want:        crd.ConsolidationReasonConsolidationBlocked,
		},
		{
			name:        "not autoscaled falls back to the catch all",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NotAutoscaled},
			want:        crd.ConsolidationReasonConsolidationBlocked,
		},
	}

	processor := &ScaleDownBlockedStatusProcessor{blockingLabels: []string{gke_labels.TPUSliceLabel}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := processor.mapReason(tc.unremovable, tc.nodeInfo)
			assert.Equal(t, tc.want, got)
			if got != "" {
				assert.Contains(t, crd.ConsolidationReasons, got, "every reported reason must be a member of the API enum")
			}
		})
	}
}

func TestScaleDownBlockedStatusProcessor_CountsActuatedNodeOnce(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		ScaledDownNodes: []*scaledownstatus.ScaleDownNode{
			{NodeGroup: mig, Node: node("deleting-1")},
		},
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			// Already counted via ScaledDownNodes - must not be double counted.
			{NodeGroup: mig, Node: node("deleting-1"), Reason: simulator.CurrentlyBeingDeleted},
			// Being deleted, but not reported in ScaledDownNodes this pass.
			{NodeGroup: mig, Node: node("deleting-2"), Reason: simulator.CurrentlyBeingDeleted},
		},
	})

	consolidation := getConsolidationStatus(t, updatesCh)
	assert.Equal(t, 2, consolidation.ActuationInProgress, "each node being deleted must be counted exactly once")
	assert.Empty(t, consolidation.BlockedNodes)
}

func TestScaleDownBlockedStatusProcessor_GroupsBlockedNodesByReasonInEvaluationOrder(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: node("busy-1"), Reason: simulator.NotUnderutilized},
			{NodeGroup: mig, Node: sliceNode("slice-1"), Reason: simulator.ScaleDownDisabledAnnotation},
			{NodeGroup: mig, Node: sliceNode("slice-2"), Reason: simulator.ScaleDownDisabledAnnotation},
			{NodeGroup: mig, Node: node("pinned-1"), Reason: simulator.NoPlaceToMovePods},
		},
	})

	consolidation := getConsolidationStatus(t, updatesCh)
	// Blocking labels are checked before utilization, which is checked before the drain
	// simulation: the status lists the reasons in that order, not alphabetically.
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonUsedByFormedSlice, Count: 2},
		{Reason: crd.ConsolidationReasonAboveUtilizationThreshold, Count: 1},
		{Reason: crd.ConsolidationReasonNoPlaceToMovePods, Count: 1},
	}, consolidation.BlockedNodes)
}

func TestScaleDownBlockedStatusProcessor_LeavesNodesWithoutBlockedReasonOut(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: node("uninspected-1"), Reason: simulator.NotUnneededOtherReason},
			{NodeGroup: mig, Node: node("capped-1"), Reason: simulator.NodeGroupMaxDeletionCountReached},
		},
	})

	// Neither node counts as blocked, and without a cluster snapshot there is no node total to
	// derive notProcessed from, so there is nothing to report for the priority at all.
	assert.Empty(t, getConsolidationStatuses(t, updatesCh))
}

func TestScaleDownBlockedStatusProcessor_DerivesNotProcessedFromSnapshot(t *testing.T) {
	processor, updatesCh, _ := newTestProcessor(t)
	mig := sizedMig(t, 3, 1)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("real-1"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("real-2"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("real-3"), nil)))
	// Generated from the node group template for simulation purposes: not a node the user
	// has, so it must not inflate notProcessed.
	templateNode := node("template-1")
	templateNode.Annotations = map[string]string{gke_labels.NodeGeneratedFromTemplateAnnotation: "true"}
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(templateNode, nil)))
	// Not in any node group CA manages: not attributed to the priority either.
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("unmanaged-1"), nil)))

	autoscalingCtx := &ca_context.AutoscalingContext{
		ClusterSnapshot: snapshot,
		CloudProvider: &nodeGroupForNodeStub{nodeGroups: map[string]cloudprovider.NodeGroup{
			"real-1": mig, "real-2": mig, "real-3": mig, "template-1": mig,
		}},
	}
	processor.Process(t.Context(), autoscalingCtx, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: node("real-1"), Reason: simulator.NotUnderutilized},
		},
	})

	consolidation := getConsolidationStatus(t, updatesCh)
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonAboveUtilizationThreshold, Count: 1},
	}, consolidation.BlockedNodes)
	assert.Equal(t, 2, consolidation.NotProcessed, "3 real nodes in the priority, 1 with a blocked reason")
}

func TestScaleDownBlockedStatusProcessor_AttributesMinSizeNodesToFloor(t *testing.T) {
	processor, updatesCh, _ := newTestProcessor(t)
	mig := sizedMig(t, 4, 4)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("idle-1"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("uninspected-1"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("pinned-1"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("deleting-1"), nil)))
	autoscalingCtx := &ca_context.AutoscalingContext{
		ClusterSnapshot: snapshot,
		CloudProvider: &nodeGroupForNodeStub{nodeGroups: map[string]cloudprovider.NodeGroup{
			"idle-1": mig, "uninspected-1": mig, "pinned-1": mig, "deleting-1": mig,
		}},
	}
	processor.Process(t.Context(), autoscalingCtx, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			// No blocked reason: the pool minimum is what keeps the node, so the user must see
			// that rather than a node CA never got to.
			{NodeGroup: mig, Node: node("uninspected-1"), Reason: simulator.NotUnneededOtherReason},
			// The user can act on a pod pinning the node, not on the pool minimum, so the
			// reason CA gave is the more useful one to report.
			{NodeGroup: mig, Node: node("pinned-1"), Reason: simulator.BlockedByPod},
			// The node is going away whatever the pool size, so it is not held by the minimum.
			{NodeGroup: mig, Node: node("deleting-1"), Reason: simulator.CurrentlyBeingDeleted},
		},
	})

	consolidation := getConsolidationStatus(t, updatesCh)
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonBlockingPods, Count: 1},
		{Reason: crd.ConsolidationReasonMinCapacityReached, Count: 2},
	}, consolidation.BlockedNodes)
	assert.Equal(t, 1, consolidation.ActuationInProgress)
	assert.Equal(t, 0, consolidation.NotProcessed, "every node must be accounted for")
}

func TestScaleDownBlockedStatusProcessor_RecentlyUnremovable(t *testing.T) {
	pdbBlocked := func(name string) *scaledownstatus.UnremovableNode {
		return &scaledownstatus.UnremovableNode{
			Node:        node(name),
			Reason:      simulator.BlockedByPod,
			BlockingPod: &drain.BlockingPod{Reason: drain.NotEnoughPdb},
		}
	}
	withReason := func(name string, reason simulator.UnremovableReason) *scaledownstatus.UnremovableNode {
		return &scaledownstatus.UnremovableNode{Node: node(name), Reason: reason}
	}

	// CA re-runs the drain simulation of a node that failed it only every
	// UnremovableNodeRecheckTimeout and reports RecentlyUnremovable in between. Nothing changed
	// for the node in the meantime, so the user must keep seeing what blocked it.
	testCases := []struct {
		name  string
		loops [][]*scaledownstatus.UnremovableNode
		want  []crd.BlockedNodesByReason
	}{
		{
			name: "continues reporting previous reason when current reason is RecentlyUnremovable",
			loops: [][]*scaledownstatus.UnremovableNode{
				{pdbBlocked("node-1")},
				{withReason("node-1", simulator.RecentlyUnremovable)},
			},
			want: []crd.BlockedNodesByReason{{Reason: crd.ConsolidationReasonPodDisruptionBudget, Count: 1}},
		},
		{
			name: "updates reported reason when CA reports a different reason",
			loops: [][]*scaledownstatus.UnremovableNode{
				{pdbBlocked("node-1")},
				{withReason("node-1", simulator.NotUnderutilized)},
				{withReason("node-1", simulator.RecentlyUnremovable)},
			},
			want: []crd.BlockedNodesByReason{{Reason: crd.ConsolidationReasonAboveUtilizationThreshold, Count: 1}},
		},
		{
			// The previous reason may no longer hold once the node left the unremovable set,
			// so it must not come back with a later RecentlyUnremovable.
			name: "stops reporting previous reason when there is no scale-down status at all",
			loops: [][]*scaledownstatus.UnremovableNode{
				{pdbBlocked("node-1")},
				{},
				{withReason("node-1", simulator.RecentlyUnremovable)},
			},
			want: []crd.BlockedNodesByReason{{Reason: crd.ConsolidationReasonConsolidationBlocked, Count: 1}},
		},
		{
			name: "falls back to the catch all for a node never seen with a blocked reason",
			loops: [][]*scaledownstatus.UnremovableNode{
				{withReason("node-1", simulator.RecentlyUnremovable)},
			},
			want: []crd.BlockedNodesByReason{{Reason: crd.ConsolidationReasonConsolidationBlocked, Count: 1}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processor, updatesCh, mig := newTestProcessor(t)
			for _, unremovable := range tc.loops {
				for _, un := range unremovable {
					un.NodeGroup = mig
				}
				getConsolidationStatuses(t, updatesCh)
				processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{UnremovableNodes: unremovable})
			}
			assert.Equal(t, tc.want, getConsolidationStatus(t, updatesCh).BlockedNodes)
		})
	}
}

func TestScaleDownBlockedStatusProcessor_KeepsRememberedReasonDuringCooldown(t *testing.T) {
	processor, updatesCh, _ := newTestProcessor(t)
	mig := sizedMig(t, 4, 1)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("atomic-1"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("young-1"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("busy-1"), nil)))
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(node("fresh-1"), nil)))
	autoscalingCtx := &ca_context.AutoscalingContext{
		ClusterSnapshot: snapshot,
		CloudProvider: &nodeGroupForNodeStub{nodeGroups: map[string]cloudprovider.NodeGroup{
			"atomic-1": mig, "young-1": mig, "busy-1": mig, "fresh-1": mig,
		}},
	}

	// A full pass: CA gives every node a reason.
	processor.Process(t.Context(), autoscalingCtx, &scaledownstatus.ScaleDownStatus{
		Result: scaledownstatus.ScaleDownNoNodeDeleted,
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: node("atomic-1"), Reason: simulator.AtomicScaleDownFailed},
			{NodeGroup: mig, Node: node("young-1"), Reason: simulator.NotUnneededLongEnough},
			{NodeGroup: mig, Node: node("busy-1"), Reason: simulator.NotUnderutilized},
			{NodeGroup: mig, Node: node("fresh-1"), Reason: simulator.NotUnneededLongEnough},
		},
	})
	getConsolidationStatuses(t, updatesCh)

	// In cooldown CA skips the step that picks the nodes to delete, so the reasons given there
	// are missing although nothing changed for those nodes: the user must keep seeing them. A
	// reason CA does give is current and wins, and a node being deleted is not blocked anymore.
	processor.Process(t.Context(), autoscalingCtx, &scaledownstatus.ScaleDownStatus{
		Result: scaledownstatus.ScaleDownInCooldown,
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: node("busy-1"), Reason: simulator.NotUnderutilized},
			{NodeGroup: mig, Node: node("fresh-1"), Reason: simulator.CurrentlyBeingDeleted},
		},
	})
	cooldown := getConsolidationStatus(t, updatesCh)
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonAboveUtilizationThreshold, Count: 1},
		{Reason: crd.ConsolidationReasonNotUnneededLongEnough, Count: 1},
		{Reason: crd.ConsolidationReasonAtomicGroupBlocked, Count: 1},
	}, cooldown.BlockedNodes)
	assert.Equal(t, 1, cooldown.ActuationInProgress)
	assert.Equal(t, 0, cooldown.NotProcessed, "nodes without a reason keep their previous one")

	// Once the cooldown is over, a pass that gives a node no reason is authoritative: nothing
	// is carried over anymore.
	processor.Process(t.Context(), autoscalingCtx, &scaledownstatus.ScaleDownStatus{
		Result: scaledownstatus.ScaleDownNoNodeDeleted,
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: node("busy-1"), Reason: simulator.NotUnderutilized},
		},
	})
	after := getConsolidationStatus(t, updatesCh)
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonAboveUtilizationThreshold, Count: 1},
	}, after.BlockedNodes)
	assert.Equal(t, 3, after.NotProcessed)
}

func TestScaleDownBlockedStatusProcessor_ClearsPriorityNoLongerBlocked(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: sliceNode("slice-1"), Reason: simulator.ScaleDownDisabledAnnotation},
		},
	})
	first := getConsolidationStatus(t, updatesCh)
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonUsedByFormedSlice, Count: 1},
	}, first.BlockedNodes)

	// The slice is gone and nothing is blocked anymore. The status is cumulative, so the
	// processor must report the priority as clear instead of leaving the previous breakdown -
	// and its stale measuredAt - on the CRD.
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})

	second := getConsolidationStatus(t, updatesCh)
	assert.Empty(t, second.BlockedNodes)
	assert.Equal(t, 0, second.ActuationInProgress)
}

func TestScaleDownBlockedStatusProcessor_StopsReportingOnceCleared(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: sliceNode("slice-1"), Reason: simulator.ScaleDownDisabledAnnotation},
		},
	})
	getConsolidationStatuses(t, updatesCh)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	getConsolidationStatuses(t, updatesCh)

	// Nothing changed and nothing is left to clear, so no further updates are produced.
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	assert.Empty(t, getConsolidationStatuses(t, updatesCh), "a quiet cluster must not keep generating status patches")
}

func TestScaleDownBlockedStatusProcessor_RetriesDroppedClearingUpdate(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: sliceNode("slice-1"), Reason: simulator.ScaleDownDisabledAnnotation},
		},
	})
	getConsolidationStatuses(t, updatesCh)

	// The channel is full, so the clearing update is dropped. The priority must stay due.
	processor.updatesCh = make(chan status.UpdateMessage)
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	assert.Contains(t, processor.lastReported, priorityKey{crdID: testCrdID, index: testPriority}, "a dropped clear must be retried")

	// With room in the channel again the clear goes through, and only then is it done.
	processor.updatesCh = updatesCh
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	cleared := getConsolidationStatus(t, updatesCh)
	assert.Empty(t, cleared.BlockedNodes)
	assert.Empty(t, processor.lastReported)
}

func TestScaleDownBlockedStatusProcessor_DoesNothingWhenExperimentOff(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)
	processor.experimentsManager = experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.ComputeClassScaleDownStatusEnabledFlag: false,
	}, map[string]string{})

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: sliceNode("slice-1"), Reason: simulator.ScaleDownDisabledAnnotation},
		},
	})
	assert.Empty(t, getConsolidationStatuses(t, updatesCh))
}

// sliceNode returns a node bound to a TPU slice.
func sliceNode(name string) *apiv1.Node {
	return &apiv1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{gke_labels.TPUSliceLabel: "my-slice"},
	}}
}

// node returns a plain node.
func node(name string) *apiv1.Node {
	return &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// newTestProcessor returns a processor wired to a single-priority test ComputeClass, the channel
// it reports on, and the MIG that belongs to that priority.
func newTestProcessor(t *testing.T) (*ScaleDownBlockedStatusProcessor, chan status.UpdateMessage, *gke.GkeMig) {
	t.Helper()
	testCrd := crd.NewTestCrd(
		crd.WithLabel(testCrdLabel),
		crd.WithName(testCrdName),
		crd.WithCrdType("TEST"),
		crd.WithRules([]rules.Rule{
			rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
		}))
	mockLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
	mockLister.SetCrdLabel(testCrdLabel)
	mockLister.SetDefaultCrdName(testCrdName)

	mig := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefName("nodepool-1-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels: map[string]string{testCrdLabel: testCrdName},
		}).Build()

	updatesCh := make(chan status.UpdateMessage, 10)
	mockManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.ComputeClassScaleDownStatusEnabledFlag: true,
	}, map[string]string{})

	processor := NewScaleDownBlockedStatusProcessor(mockLister, standardClusterProvider{}, updatesCh, mockManager, []string{gke_labels.TPUSliceLabel})
	processor.now = func() time.Time { return time.Unix(0, 0) }
	return processor, updatesCh, mig
}

// sizedMig returns a MIG of the test priority with the given target and minimum size.
func sizedMig(t *testing.T, targetSize int64, minSize int) *gke.GkeMig {
	t.Helper()
	manager := gke.NewFakeGkeManagerBuilder().WithMigSize(targetSize).Build()
	return gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefName("nodepool-1-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels: map[string]string{testCrdLabel: testCrdName},
		}).
		SetGkeManager(manager).
		SetExist(true).
		SetMinSize(minSize).
		Build()
}

// getConsolidationStatuses applies every buffered update to a fake status and returns what was reported,
// keyed by priority.
func getConsolidationStatuses(t *testing.T, updatesCh chan status.UpdateMessage) map[string]crd.ConsolidationStatus {
	t.Helper()
	reported := make(map[string]crd.ConsolidationStatus)
	recorder := &consolidationRecorder{reported: reported}
	for {
		select {
		case msg := <-updatesCh:
			assert.Equal(t, testCrdID, msg.Id)
			msg.Mutate(recorder)
		default:
			return reported
		}
	}
}

// getConsolidationStatus drains the buffered updates and returns the status reported for the
// test priority, failing the test if there is none.
func getConsolidationStatus(t *testing.T, updatesCh chan status.UpdateMessage) crd.ConsolidationStatus {
	t.Helper()
	consolidation, ok := getConsolidationStatuses(t, updatesCh)[testPriority]
	if !ok {
		t.Fatalf("expected a consolidation status for priority %s", testPriority)
	}
	return consolidation
}

// standardClusterProvider is a machineConfigProvider of a Standard cluster.
type standardClusterProvider struct{}

func (standardClusterProvider) IsAutopilotEnabled() bool {
	return false
}

// nodeGroupForNodeStub is a cloudprovider.CloudProvider that only knows which node group each
// node belongs to. Every other method panics, which is what the processor must never need.
type nodeGroupForNodeStub struct {
	cloudprovider.CloudProvider
	nodeGroups map[string]cloudprovider.NodeGroup
}

func (s *nodeGroupForNodeStub) NodeGroupForNode(_ context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	return s.nodeGroups[node.Name], nil
}

// consolidationRecorder is a crd.CRDStatus that records the consolidation statuses written to it.
type consolidationRecorder struct {
	crd.MockCRDStatus
	reported map[string]crd.ConsolidationStatus
}

func (r *consolidationRecorder) UpdateRuleConsolidationStatus(priority string, consolidation crd.ConsolidationStatus) {
	r.reported[priority] = consolidation
}
