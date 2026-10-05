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

	testCases := []struct {
		name        string
		unremovable *scaledownstatus.UnremovableNode
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
			// CA caches a failed drain simulation for a few minutes and reports the node as
			// RecentlyUnremovable in between. That is not a failed removal attempt.
			name:        "recently unremovable is a cached verdict, not a failure",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.RecentlyUnremovable},
			want:        crd.ConsolidationReasonConsolidationBlocked,
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
			name:        "per loop removal cap reached is not a verdict",
			unremovable: &scaledownstatus.UnremovableNode{Node: plainNode, Reason: simulator.NodeGroupMaxDeletionCountReached},
			want:        "",
		},
		{
			name:        "never inspected is not a verdict",
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
			got := processor.mapReason(tc.unremovable)
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

	consolidation := reportedConsolidation(t, updatesCh)
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

	consolidation := reportedConsolidation(t, updatesCh)
	// Blocking labels are checked before utilization, which is checked before the drain
	// simulation: the status lists the reasons in that order, not alphabetically.
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonUsedByFormedSlice, Count: 2},
		{Reason: crd.ConsolidationReasonAboveUtilizationThreshold, Count: 1},
		{Reason: crd.ConsolidationReasonNoPlaceToMovePods, Count: 1},
	}, consolidation.BlockedNodes)
}

func TestScaleDownBlockedStatusProcessor_LeavesNodesWithoutVerdictOut(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: node("uninspected-1"), Reason: simulator.NotUnneededOtherReason},
			{NodeGroup: mig, Node: node("capped-1"), Reason: simulator.NodeGroupMaxDeletionCountReached},
		},
	})

	// Neither node counts as blocked, and without a cluster snapshot there is no node total to
	// derive notProcessed from, so there is nothing to report for the priority at all.
	assert.Empty(t, drainUpdates(t, updatesCh))
}

func TestScaleDownBlockedStatusProcessor_DerivesNotProcessedFromSnapshot(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, n := range []*apiv1.Node{node("real-1"), node("real-2"), node("real-3")} {
		assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(n, nil)))
	}
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

	consolidation := reportedConsolidation(t, updatesCh)
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonAboveUtilizationThreshold, Count: 1},
	}, consolidation.BlockedNodes)
	assert.Equal(t, 2, consolidation.NotProcessed, "3 real nodes in the priority, 1 with a verdict")
}

func TestScaleDownBlockedStatusProcessor_ClearsPriorityNoLongerBlocked(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: sliceNode("slice-1"), Reason: simulator.ScaleDownDisabledAnnotation},
		},
	})
	first := reportedConsolidation(t, updatesCh)
	assert.Equal(t, []crd.BlockedNodesByReason{
		{Reason: crd.ConsolidationReasonUsedByFormedSlice, Count: 1},
	}, first.BlockedNodes)

	// The slice is gone and nothing is blocked anymore. The status is cumulative, so the
	// processor must report the priority as clear instead of leaving the previous breakdown -
	// and its stale measuredAt - on the CRD.
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})

	second := reportedConsolidation(t, updatesCh)
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
	drainUpdates(t, updatesCh)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	drainUpdates(t, updatesCh)

	// Nothing changed and nothing is left to clear, so no further updates are produced.
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	assert.Empty(t, drainUpdates(t, updatesCh), "a quiet cluster must not keep generating status patches")
}

func TestScaleDownBlockedStatusProcessor_RetriesDroppedClearingUpdate(t *testing.T) {
	processor, updatesCh, mig := newTestProcessor(t)

	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{
		UnremovableNodes: []*scaledownstatus.UnremovableNode{
			{NodeGroup: mig, Node: sliceNode("slice-1"), Reason: simulator.ScaleDownDisabledAnnotation},
		},
	})
	drainUpdates(t, updatesCh)

	// The channel is full, so the clearing update is dropped. The priority must stay due.
	processor.updatesCh = make(chan status.UpdateMessage)
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	assert.Contains(t, processor.lastReported, priorityKey{crdID: testCrdID, index: testPriority}, "a dropped clear must be retried")

	// With room in the channel again the clear goes through, and only then is it done.
	processor.updatesCh = updatesCh
	processor.Process(t.Context(), nil, &scaledownstatus.ScaleDownStatus{})
	cleared := reportedConsolidation(t, updatesCh)
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
	assert.Empty(t, drainUpdates(t, updatesCh))
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

	mockProvider := NewMockCloudProvider()
	mockProvider.On("IsAutopilotEnabled").Return(false)

	updatesCh := make(chan status.UpdateMessage, 10)
	mockManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.ComputeClassScaleDownStatusEnabledFlag: true,
	}, map[string]string{})

	processor := NewScaleDownBlockedStatusProcessor(mockLister, mockProvider, updatesCh, mockManager, []string{gke_labels.TPUSliceLabel})
	processor.now = func() time.Time { return time.Unix(0, 0) }
	return processor, updatesCh, mig
}

// drainUpdates applies every buffered update to a fake status and returns what was reported,
// keyed by priority.
func drainUpdates(t *testing.T, updatesCh chan status.UpdateMessage) map[string]crd.ConsolidationStatus {
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

// reportedConsolidation drains the buffered updates and returns the status reported for the
// test priority, failing the test if there is none.
func reportedConsolidation(t *testing.T, updatesCh chan status.UpdateMessage) crd.ConsolidationStatus {
	t.Helper()
	consolidation, ok := drainUpdates(t, updatesCh)[testPriority]
	if !ok {
		t.Fatalf("expected a consolidation status for priority %s", testPriority)
	}
	return consolidation
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
