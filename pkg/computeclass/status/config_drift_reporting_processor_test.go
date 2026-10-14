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

package status

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

// ignoreMeasuredAt drops the timestamp from comparisons. It is asserted on
// separately, in the one test that pins the clock.
var ignoreMeasuredAt = cmpopts.IgnoreFields(crd.ConfigDriftInfo{}, "MeasuredAt")

func TestConfigDriftReportingProcessor_ReportsProgressPerComputeClass(t *testing.T) {
	cluster := newFakeCluster(
		nodeGroupDrift{
			id:      "drifted-ng",
			crdName: driftedCrdName,
			drifted: true,
			nodes: []*apiv1.Node{
				newTestNode("being-replaced", defragCandidate()),
				newTestNode("waiting-its-turn"),
			},
		},
		nodeGroupDrift{
			id:      "up-to-date-ng",
			crdName: otherCrdName,
			nodes:   []*apiv1.Node{newTestNode("up-to-date")},
		},
		// Outside any ComputeClass: there is nothing to report it against.
		nodeGroupDrift{
			id:    "unmanaged-ng",
			nodes: []*apiv1.Node{newTestNode("unmanaged")},
		},
	)
	processor, updatesCh := newTestReportingProcessor(cluster, nil)
	// Built outside the bubble below: a cluster snapshot starts background
	// goroutines that outlive the loop, which a bubble reports as a deadlock.
	autoscalingCtx := cluster.autoscalingContext(t)

	// The clock is pinned so that the reported measurement time can be
	// compared against the time the loop ran at.
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		processMustSucceed(t, processor, autoscalingCtx)

		got := drainConfigDriftUpdates(t, updatesCh)
		want := map[string]reportedStatus{
			driftedCrdName: {Reported: []crd.ConfigDriftInfo{{DriftedNodes: 1, MigratingNodes: 1}}},
			otherCrdName:   {Reported: []crd.ConfigDriftInfo{{CurrentNodes: 1}}},
		}
		if diff := cmp.Diff(want, got, ignoreMeasuredAt); diff != "" {
			t.Errorf("reported statuses mismatch (-want +got):\n%s", diff)
		}
		for crdName, status := range got {
			for _, info := range status.Reported {
				if !info.MeasuredAt.Time.Equal(now) {
					t.Errorf("ComputeClass %q was reported as measured at %v, want %v", crdName, info.MeasuredAt, now)
				}
			}
		}
	})
}

func TestConfigDriftReportingProcessor_ClearsComputeClassesWithNothingToReport(t *testing.T) {
	cluster := newFakeCluster(
		// Turning migration off must take the counters with it, rather than
		// leaving the last reported progress frozen in the status forever.
		nodeGroupDrift{
			id:                "opted-out-ng",
			crdName:           driftedCrdName,
			migrationDisabled: true,
			drifted:           true,
			nodes:             []*apiv1.Node{newTestNode("opted-out")},
		},
		nodeGroupDrift{
			id:    "unmanaged-ng",
			nodes: []*apiv1.Node{newTestNode("unmanaged")},
		},
	)
	processor, updatesCh := newTestReportingProcessor(cluster, nil)

	processMustSucceed(t, processor, cluster.autoscalingContext(t))

	want := map[string]reportedStatus{driftedCrdName: {Resets: 1}}
	if diff := cmp.Diff(want, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Errorf("reported statuses mismatch (-want +got):\n%s", diff)
	}
}

func TestConfigDriftReportingProcessor_ClearsComputeClassesItStopsOwning(t *testing.T) {
	drifted := nodeGroupDrift{
		id:      "drifted-ng",
		crdName: driftedCrdName,
		drifted: true,
		nodes:   []*apiv1.Node{newTestNode("n1")},
	}
	cluster := newFakeCluster(drifted)
	processor, updatesCh := newTestReportingProcessor(cluster, nil)

	processMustSucceed(t, processor, cluster.autoscalingContext(t))
	want := map[string]reportedStatus{driftedCrdName: {Reported: []crd.ConfigDriftInfo{{DriftedNodes: 1}}}}
	if diff := cmp.Diff(want, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Fatalf("first loop mismatch (-want +got):\n%s", diff)
	}

	// The last node group of the ComputeClass is gone, so the counters
	// describing it have to go too, even though the ComputeClass itself is no
	// longer this autoscaler's business.
	cluster.setGroups()
	processMustSucceed(t, processor, cluster.autoscalingContext(t))
	want = map[string]reportedStatus{driftedCrdName: {Resets: 1}}
	if diff := cmp.Diff(want, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Errorf("second loop mismatch (-want +got):\n%s", diff)
	}

	// And it is cleared once, not on every loop from now on: a ComputeClass
	// this autoscaler has nothing to do with may well be owned by another
	// shard, which must be left to report on it.
	processMustSucceed(t, processor, cluster.autoscalingContext(t))
	if diff := cmp.Diff(map[string]reportedStatus{}, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Errorf("third loop mismatch (-want +got):\n%s", diff)
	}
}

func TestConfigDriftReportingProcessor_InconclusiveLoopReportsNothing(t *testing.T) {
	cluster := newFakeCluster(nodeGroupDrift{
		id:      "drifted-ng",
		crdName: driftedCrdName,
		drifted: true,
		nodes:   []*apiv1.Node{newTestNode("n1")},
	})
	// Defrag did not evaluate nodes this loop, so nothing is known about what
	// is blocked and the previously reported counters must be left alone.
	processor, updatesCh := newTestReportingProcessor(cluster, &staticBlockReasons{})

	processMustSucceed(t, processor, cluster.autoscalingContext(t))

	if diff := cmp.Diff(map[string]reportedStatus{}, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Errorf("reported statuses mismatch (-want +got):\n%s", diff)
	}
}

func TestConfigDriftReportingProcessor_DisabledReportsNothing(t *testing.T) {
	cluster := newFakeCluster(nodeGroupDrift{
		id:      "drifted-ng",
		crdName: driftedCrdName,
		drifted: true,
		nodes:   []*apiv1.Node{newTestNode("n1")},
	})
	updatesCh := make(chan UpdateMessage, updatesChannelSize)
	processor := NewConfigDriftReportingProcessor(cluster, updatesCh, cluster, &staticBlockReasons{registry: observability.NewRegistry()},
		experiments.NewMockManagerWithOptions(
			version.Version{},
			map[string]bool{experiments.ComputeClassConfigDriftStatusEnabledFlag: false},
			map[string]string{},
		))

	processMustSucceed(t, processor, cluster.autoscalingContext(t))

	if diff := cmp.Diff(map[string]reportedStatus{}, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Errorf("reported statuses mismatch (-want +got):\n%s", diff)
	}
}

func TestConfigDriftReportingProcessor_ReportsBlockedNodesOnceTheyPersist(t *testing.T) {
	cluster := newFakeCluster(nodeGroupDrift{
		id:      "drifted-ng",
		crdName: driftedCrdName,
		drifted: true,
		nodes:   []*apiv1.Node{newTestNode("n1", notReady())},
	})
	processor, updatesCh := newTestReportingProcessor(cluster, nil)

	// Debouncing state lives in the processor, so a reason only surfaces after
	// the same node has reported it for several consecutive loops.
	for loop := 1; loop < defaultBlockReasonDebounceLoops; loop++ {
		processMustSucceed(t, processor, cluster.autoscalingContext(t))
		want := map[string]reportedStatus{driftedCrdName: {Reported: []crd.ConfigDriftInfo{{DriftedNodes: 1}}}}
		if diff := cmp.Diff(want, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
			t.Fatalf("loop %d mismatch (-want +got):\n%s", loop, diff)
		}
	}

	processMustSucceed(t, processor, cluster.autoscalingContext(t))
	want := map[string]reportedStatus{driftedCrdName: {Reported: []crd.ConfigDriftInfo{{
		BlockedNodes: []crd.BlockedNodesInfo{{Reason: string(observability.NodeNotReady), Count: 1}},
	}}}}
	if diff := cmp.Diff(want, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Errorf("loop %d mismatch (-want +got):\n%s", defaultBlockReasonDebounceLoops, diff)
	}
}

// updatesChannelSize is large enough that no test fills the channel, so that a
// dropped update is a bug rather than a full buffer.
const updatesChannelSize = 20

// newTestReportingProcessor returns a processor reporting on the given cluster,
// with the feature enabled. A nil blockReasons stands for a loop in which
// defrag evaluated the cluster and found nothing blocked.
func newTestReportingProcessor(cluster *fakeCluster, blockReasons BlockReasonSource) (*ConfigDriftReportingProcessor, chan UpdateMessage) {
	if blockReasons == nil {
		blockReasons = &staticBlockReasons{registry: observability.NewRegistry()}
	}
	updatesCh := make(chan UpdateMessage, updatesChannelSize)
	processor := NewConfigDriftReportingProcessor(cluster, updatesCh, cluster, blockReasons,
		experiments.NewMockManager(experiments.ComputeClassConfigDriftStatusEnabledFlag))
	return processor, updatesCh
}

func processMustSucceed(t *testing.T, processor *ConfigDriftReportingProcessor, autoscalingCtx *ca_context.AutoscalingContext) {
	t.Helper()
	if err := processor.Process(context.TODO(), autoscalingCtx, &clusterstate.ClusterStateRegistry{}, time.Now()); err != nil {
		t.Fatalf("Process() returned an error: %v", err)
	}
}

// staticBlockReasons hands the processor a fixed registry, standing in for what
// the defrag pipeline recorded during the loop. A nil registry means defrag did
// not evaluate the cluster at all.
type staticBlockReasons struct{ registry *observability.Registry }

func (s *staticBlockReasons) BlockReasons() *observability.Registry { return s.registry }

// nodeGroupDrift is one node group of a fake cluster.
type nodeGroupDrift struct {
	id string
	// crdName is the ComputeClass the node group belongs to, or "" if it
	// belongs to none.
	crdName string
	// migrationDisabled makes the ComputeClass one that did not opt into config
	// drift migration.
	migrationDisabled bool
	// drifted makes the node group stop matching its ComputeClass config.
	drifted bool
	nodes   []*apiv1.Node
}

// fakeCluster is the world a ConfigDriftReportingProcessor sees.
//
// It plays the cloud provider, the ComputeClass lister and the matcher at once,
// so that a test describes the cluster in one place and can change it between
// loops with setGroups.
type fakeCluster struct {
	cloudprovider.CloudProvider
	groups []nodeGroupDrift
	crds   map[string]crd.CRD
}

func newFakeCluster(groups ...nodeGroupDrift) *fakeCluster {
	return &fakeCluster{groups: groups, crds: map[string]crd.CRD{}}
}

func (c *fakeCluster) setGroups(groups ...nodeGroupDrift) { c.groups = groups }

// autoscalingContext returns the context of a loop running against the cluster
// in its current shape.
func (c *fakeCluster) autoscalingContext(t *testing.T) *ca_context.AutoscalingContext {
	t.Helper()
	var nodes []*apiv1.Node
	for _, group := range c.groups {
		nodes = append(nodes, group.nodes...)
	}
	// The processor reads the nodes from the lister rather than the cluster
	// snapshot, so that nodes already being drained are still reported.
	listers := kube_util.NewListerRegistry(kube_util.NewTestNodeLister(nodes), nil, nil, nil, nil, nil, nil, nil, nil)
	return &ca_context.AutoscalingContext{
		AutoscalingKubeClients: ca_context.AutoscalingKubeClients{ListerRegistry: listers},
		CloudProvider:          c,
	}
}

func (c *fakeCluster) NodeGroups(context.Context) []cloudprovider.NodeGroup {
	nodeGroups := make([]cloudprovider.NodeGroup, 0, len(c.groups))
	for _, group := range c.groups {
		nodeGroups = append(nodeGroups, &fakeNodeGroup{id: group.id})
	}
	return nodeGroups
}

func (c *fakeCluster) NodeGroupForNode(_ context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	for _, group := range c.groups {
		for _, groupNode := range group.nodes {
			if groupNode.Name == node.Name {
				return &fakeNodeGroup{id: group.id}, nil
			}
		}
	}
	return nil, nil
}

func (c *fakeCluster) NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error) {
	group, found := c.group(nodeGroup)
	if !found || group.crdName == "" {
		return nil, "", nil
	}
	if _, built := c.crds[group.crdName]; !built {
		c.crds[group.crdName] = crd.NewTestCrd(
			crd.WithLabel(testCrdLabelKey),
			crd.WithName(group.crdName),
			crd.WithConfigDrift(!group.migrationDisabled),
		)
	}
	return c.crds[group.crdName], group.crdName, nil
}

func (c *fakeCluster) MatchesCrdLabel(cloudprovider.NodeGroup, crd.CRD) bool { return true }

func (c *fakeCluster) MatchesCrdConfig(nodeGroup cloudprovider.NodeGroup, _ crd.CRD) bool {
	group, found := c.group(nodeGroup)
	return found && !group.drifted
}

func (c *fakeCluster) FirstMatchedRule(nodeGroup cloudprovider.NodeGroup, computeClass crd.CRD) (bool, int, rules.Rule) {
	return c.MatchesCrdConfig(nodeGroup, computeClass), 0, nil
}

func (c *fakeCluster) group(nodeGroup cloudprovider.NodeGroup) (nodeGroupDrift, bool) {
	for _, group := range c.groups {
		if group.id == nodeGroup.Id() {
			return group, true
		}
	}
	return nodeGroupDrift{}, false
}

// reportedStatus is what a single ComputeClass was told during one loop.
type reportedStatus struct {
	Reported []crd.ConfigDriftInfo
	Resets   int
}

// recordingStatus records what the processor does to one ComputeClass status.
// Anything other than config drift reporting panics.
type recordingStatus struct {
	crd.CRDStatus
	recorded *reportedStatus
}

func (s *recordingStatus) UpdateConfigDriftInfo(info crd.ConfigDriftInfo) {
	s.recorded.Reported = append(s.recorded.Reported, info)
}

func (s *recordingStatus) ResetConfigDriftInfo() { s.recorded.Resets++ }

// drainConfigDriftUpdates applies everything the processor has sent so far and
// returns what each ComputeClass ended up being told.
func drainConfigDriftUpdates(t *testing.T, updatesCh chan UpdateMessage) map[string]reportedStatus {
	t.Helper()
	statuses := map[string]*reportedStatus{}
	for {
		select {
		case update := <-updatesCh:
			if update.Id.CRDLabel != testCrdLabelKey {
				t.Errorf("update for ComputeClass %q carries label %q, want %q", update.Id.CRDName, update.Id.CRDLabel, testCrdLabelKey)
			}
			status, known := statuses[update.Id.CRDName]
			if !known {
				status = &reportedStatus{}
				statuses[update.Id.CRDName] = status
			}
			update.Mutate(&recordingStatus{recorded: status})
		default:
			drained := make(map[string]reportedStatus, len(statuses))
			for crdName, status := range statuses {
				drained[crdName] = *status
			}
			return drained
		}
	}
}

func TestConfigDriftReportingProcessor_RetriesAResetDroppedByAFullChannel(t *testing.T) {
	cluster := newFakeCluster(nodeGroupDrift{
		id:      "drifted-ng",
		crdName: driftedCrdName,
		drifted: true,
		nodes:   []*apiv1.Node{newTestNode("n1")},
	})
	// One slot, so the first loop's report fills the channel and the reset the
	// second loop wants to send has nowhere to go.
	updatesCh := make(chan UpdateMessage, 1)
	processor := NewConfigDriftReportingProcessor(cluster, updatesCh, cluster,
		&staticBlockReasons{registry: observability.NewRegistry()},
		experiments.NewMockManager(experiments.ComputeClassConfigDriftStatusEnabledFlag))

	processMustSucceed(t, processor, cluster.autoscalingContext(t))

	// The ComputeClass loses its last node group, so its counters have to be
	// cleared. The channel is still full, so that reset is dropped.
	cluster.setGroups()
	processMustSucceed(t, processor, cluster.autoscalingContext(t))

	want := map[string]reportedStatus{driftedCrdName: {Reported: []crd.ConfigDriftInfo{{DriftedNodes: 1}}}}
	if diff := cmp.Diff(want, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Fatalf("first two loops mismatch (-want +got):\n%s", diff)
	}

	// Nothing will re-send it, so the dropped reset has to be retried here.
	// Otherwise the ComputeClass keeps publishing counters for node groups that
	// no longer exist, for as long as the autoscaler runs.
	processMustSucceed(t, processor, cluster.autoscalingContext(t))
	want = map[string]reportedStatus{driftedCrdName: {Resets: 1}}
	if diff := cmp.Diff(want, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Fatalf("retry loop mismatch (-want +got):\n%s", diff)
	}

	// And once it lands it is not repeated, so another shard is left alone.
	processMustSucceed(t, processor, cluster.autoscalingContext(t))
	if diff := cmp.Diff(map[string]reportedStatus{}, drainConfigDriftUpdates(t, updatesCh), ignoreMeasuredAt); diff != "" {
		t.Errorf("loop after the retry landed mismatch (-want +got):\n%s", diff)
	}
}

func TestDeletionsInProgressWithAValueTypedActuator(t *testing.T) {
	// A struct rather than a pointer, which reflect.Value.IsNil panics on.
	autoscalingCtx := &ca_context.AutoscalingContext{ScaleDownActuator: valueActuator{}}

	got := deletionsInProgress(autoscalingCtx)

	want := sets.New("being-emptied", "being-drained")
	if !got.Equal(want) {
		t.Errorf("deletionsInProgress() = %v, want %v", sets.List(got), sets.List(want))
	}
}

// valueActuator implements scaledown.Actuator as a value rather than a pointer.
type valueActuator struct{ scaledown.Actuator }

func (valueActuator) CheckStatus() scaledown.ActuationStatus { return valueActuationStatus{} }

type valueActuationStatus struct{ scaledown.ActuationStatus }

func (valueActuationStatus) DeletionsInProgress() ([]string, []string) {
	return []string{"being-emptied"}, []string{"being-drained"}
}
