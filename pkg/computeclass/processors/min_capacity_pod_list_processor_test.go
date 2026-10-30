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

package processors

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	cloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/eligibility"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	gkeclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/fairness"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

func intPtr(v int) *int { return &v }

type nodeOption func(*apiv1.Node)

func buildNodeWithCCC(name, ccc string, opts ...nodeOption) *apiv1.Node {
	n := test.BuildTestNode(name, 1000, 1000)
	if n.Labels == nil {
		n.Labels = make(map[string]string)
	}
	n.Labels["kubernetes.io/hostname"] = name
	if ccc != "" {
		n.Labels["cloud.google.com/compute-class"] = ccc
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

func withLabel(key, value string) nodeOption {
	return func(node *apiv1.Node) { node.Labels[key] = value }
}

func withAnnotation(key, value string) nodeOption {
	return func(node *apiv1.Node) {
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}
		node.Annotations[key] = value
	}
}

func withTaint(key string) nodeOption {
	return func(node *apiv1.Node) {
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{Key: key, Effect: apiv1.TaintEffectNoSchedule})
	}
}

func withPodCapacity(pods int64) nodeOption {
	return func(node *apiv1.Node) {
		node.Status.Capacity[apiv1.ResourcePods] = *resource.NewQuantity(pods, resource.DecimalSI)
		node.Status.Allocatable[apiv1.ResourcePods] = *resource.NewQuantity(pods, resource.DecimalSI)
	}
}

func withDeletionTimestamp() nodeOption {
	return func(node *apiv1.Node) {
		now := metav1.Now()
		node.DeletionTimestamp = &now
	}
}

// Tests that Process counts node group target sizes for the rule-level min
// capacity and snapshot nodes for the spec-level min capacity.
func TestMinCapacityPodListProcessor_Process(t *testing.T) {
	testCases := []struct {
		name             string
		crds             []crd.CRD
		existingNodes    []*apiv1.Node
		nodeFilter       resourcequotas.NodeFilter
		cloudProvider    cloudprovider.CloudProvider
		expectedFakePods int
	}{
		{
			name: "top-level CCC, zero capacity triggers full capacity injection",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			existingNodes:    []*apiv1.Node{},
			cloudProvider:    &minCapacityMockCloudProvider{},
			expectedFakePods: 5,
		},
		{
			name: "top-level CCC, partial capacity partially triggers injection",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("n2", "my-ccc"),
			},
			cloudProvider:    &minCapacityMockCloudProvider{},
			expectedFakePods: 3,
		},
		{
			name: "top-level CCC, full capacity skips injection",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(3)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("n2", "my-ccc"),
				buildNodeWithCCC("n3", "my-ccc"),
			},
			cloudProvider:    &minCapacityMockCloudProvider{},
			expectedFakePods: 0,
		},
		{
			name: "priority rule injects specific fake pods, but existing node lacks priority label",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("priority-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithRules([]rules.Rule{
						rules.NewRule(
							rules.WithLabelsRule(map[string]string{"cloud.google.com/compute-class-priority": "0"}),
							rules.WithTargetNodeCountRule(intPtr(4)),
						),
					}),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "priority-ccc"),
			},
			cloudProvider: &minCapacityMockCloudProvider{
				nodeGroups: []cloudprovider.NodeGroup{
					&minCapacityMockNodeGroup{
						id:         "ng1",
						targetSize: 1,
						labels: map[string]string{
							"cloud.google.com/compute-class": "priority-ccc",
						},
					},
				},
			},
			expectedFakePods: 4, // 4 target - 0 match
		},
		{
			name: "priority rule injects specific fake pods and finds a matching priority node",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("priority-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithRules([]rules.Rule{
						rules.NewRule(
							rules.WithLabelsRule(map[string]string{"cloud.google.com/compute-class-priority": "0"}),
							rules.WithTargetNodeCountRule(intPtr(4)),
						),
					}),
				),
			},
			existingNodes: []*apiv1.Node{
				func() *apiv1.Node {
					n := buildNodeWithCCC("n1", "priority-ccc")
					n.Labels[labels.ComputeClassPriorityIdxLabel] = "0"
					return n
				}(),
			},
			cloudProvider: &minCapacityMockCloudProvider{
				nodeGroups: []cloudprovider.NodeGroup{
					&minCapacityMockNodeGroup{
						id:         "ng1",
						targetSize: 1,
						labels: map[string]string{
							"cloud.google.com/compute-class":          "priority-ccc",
							"cloud.google.com/compute-class-priority": "0",
						},
					},
				},
			},
			expectedFakePods: 3, // 4 target - 1 match
		},
		{
			name: "top-level CCC, nodes are full, should NOT inject more than target",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			existingNodes: []*apiv1.Node{
				func() *apiv1.Node {
					n := buildNodeWithCCC("n1", "my-ccc")
					n.Status.Allocatable = apiv1.ResourceList{
						apiv1.ResourcePods: *resource.NewQuantity(0, resource.DecimalSI),
					}
					return n
				}(),
				func() *apiv1.Node {
					n := buildNodeWithCCC("n2", "my-ccc")
					n.Status.Allocatable = apiv1.ResourceList{
						apiv1.ResourcePods: *resource.NewQuantity(0, resource.DecimalSI),
					}
					return n
				}(),
			},
			cloudProvider: &minCapacityMockCloudProvider{},
			// Min capacity 5 - 2 existing nodes, however full they are.
			expectedFakePods: 3,
		},
		{
			name: "ScaleUpAnyway priority label counts for top-level but ignores priority rules",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(5)),
					crd.WithRules([]rules.Rule{
						rules.NewRule(
							rules.WithLabelsRule(map[string]string{"cloud.google.com/compute-class-priority": "0"}),
							rules.WithTargetNodeCountRule(intPtr(2)),
						),
					}),
				),
			},
			existingNodes: []*apiv1.Node{
				func() *apiv1.Node {
					n := buildNodeWithCCC("n1", "my-ccc")
					n.Labels[labels.ComputeClassPriorityIdxLabel] = "-1"
					return n
				}(),
			},
			cloudProvider: &minCapacityMockCloudProvider{
				nodeGroups: []cloudprovider.NodeGroup{
					&minCapacityMockNodeGroup{
						id:         "ng1",
						targetSize: 1,
						labels: map[string]string{
							"cloud.google.com/compute-class": "my-ccc",
						},
					},
				},
			},
			// 2 priority pods (rule min capacity 2 - 0 matching nodes) + 2 spec pods
			// (spec min capacity 5 - 2 priority pods - 1 existing node).
			expectedFakePods: 4,
		},
		{
			name: "priority rule, upcoming node groups partially satisfy target, injects shortfall",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("priority-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithRules([]rules.Rule{
						rules.NewRule(
							rules.WithLabelsRule(map[string]string{"cloud.google.com/compute-class-priority": "0"}),
							rules.WithTargetNodeCountRule(intPtr(4)),
						),
					}),
				),
			},
			existingNodes: []*apiv1.Node{},
			cloudProvider: &minCapacityMockCloudProvider{
				nodeGroups: []cloudprovider.NodeGroup{
					&minCapacityMockNodeGroup{
						id:         "ng1",
						targetSize: 2,
						labels: map[string]string{
							"cloud.google.com/compute-class":          "priority-ccc",
							"cloud.google.com/compute-class-priority": "0",
						},
					},
				},
			},
			expectedFakePods: 2, // 4 target - 2 upcoming = 2 fake pods
		},
		{
			name: "top-level CCC, existing node tainted with defrag hard candidate taint counts",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(2)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				func() *apiv1.Node {
					n := buildNodeWithCCC("n2", "my-ccc")
					n.Spec.Taints = append(n.Spec.Taints, apiv1.Taint{
						Key:    defrag.HardTaint,
						Effect: apiv1.TaintEffectNoSchedule,
					})
					return n
				}(),
			},
			cloudProvider: &minCapacityMockCloudProvider{},
			// Min capacity 2 - n1 and n2: a defrag candidate counts until it is
			// being deleted.
			expectedFakePods: 0,
		},
		{
			name: "top-level CCC, existing node tainted with defrag soft candidate taint counts",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(2)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				func() *apiv1.Node {
					n := buildNodeWithCCC("n2", "my-ccc")
					n.Spec.Taints = append(n.Spec.Taints, apiv1.Taint{
						Key:    defrag.SoftTaint,
						Effect: apiv1.TaintEffectPreferNoSchedule,
					})
					return n
				}(),
			},
			cloudProvider: &minCapacityMockCloudProvider{},
			// Min capacity 2 - n1 and n2: the soft taint does not mark a deletion.
			expectedFakePods: 0,
		},
		{
			name: "top-level CCC, existing node tainted with ToBeDeleted taint does not count",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(2)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				func() *apiv1.Node {
					n := buildNodeWithCCC("n2", "my-ccc")
					n.Spec.Taints = append(n.Spec.Taints, apiv1.Taint{
						Key:    taints.ToBeDeletedTaint,
						Effect: apiv1.TaintEffectNoSchedule,
					})
					return n
				}(),
			},
			cloudProvider:    &minCapacityMockCloudProvider{},
			expectedFakePods: 1, // min capacity 2 - n1
		},
		{
			name: "top-level CCC, existing node with DeletionTimestamp does not count",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(2)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				func() *apiv1.Node {
					n := buildNodeWithCCC("n2", "my-ccc")
					now := metav1.Now()
					n.DeletionTimestamp = &now
					return n
				}(),
			},
			cloudProvider:    &minCapacityMockCloudProvider{},
			expectedFakePods: 1, // min capacity 2 - n1
		},
		{
			name: "top-level CCC, upcoming placeholder node counts",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(2)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("n2", "my-ccc", withAnnotation(annotations.NodeUpcomingAnnotation, "true")),
			},
			cloudProvider:    &minCapacityMockCloudProvider{},
			expectedFakePods: 0, // min capacity 2 - n1 - upcoming n2
		},
		{
			name: "top-level CCC, node excluded by the node filter does not count",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(2)),
				),
			},
			existingNodes:    []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("surge", "my-ccc")},
			nodeFilter:       nodeNameFilter{"surge": true},
			cloudProvider:    &minCapacityMockCloudProvider{},
			expectedFakePods: 1, // min capacity 2 - n1
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ccLister := lister.NewMockCrdListerWithLabel(tc.crds, labels.ComputeClassLabel)
			processor := NewMinCapacityPodListProcessor(ccLister, nil, nil, tc.nodeFilter, nil)

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, n := range tc.existingNodes {
				err := snapshot.AddNodeInfo(framework.NewNodeInfo(n, nil))
				assert.NoError(t, err)
			}

			ctx := &ca_context.AutoscalingContext{
				ClusterSnapshot: snapshot,
				CloudProvider:   tc.cloudProvider,
			}

			trulyUnschedulable, err := processor.Process(ctx, nil)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedFakePods, len(trulyUnschedulable))

			for _, p := range trulyUnschedulable {
				assert.Contains(t, p.Annotations, crd.MinCapacityFakePodAnnotation)
				assert.Equal(t, metav1.NamespaceDefault, p.Namespace)
			}
		})
	}
}

type nodeNameFilter map[string]bool

func (f nodeNameFilter) ExcludeFromTracking(node *apiv1.Node) bool {
	return f[node.Name]
}

// Tests that ComputeClasses whose name is not a valid label value get no fake
// pods, while other ComputeClasses still do.
func TestMinCapacityPodListProcessor_SkipsInvalidComputeClassName(t *testing.T) {
	validName := "cc-" + strings.Repeat("a", 60)   // 63 characters
	invalidName := "cc-" + strings.Repeat("b", 61) // 64 characters
	crds := []crd.CRD{
		crd.NewTestCrd(
			crd.WithName(validName),
			crd.WithLabel(labels.ComputeClassLabel),
			crd.WithTargetNodeCount(intPtr(2)),
		),
		crd.NewTestCrd(
			crd.WithName(invalidName),
			crd.WithLabel(labels.ComputeClassLabel),
			crd.WithTargetNodeCount(intPtr(3)),
			crd.WithRules([]rules.Rule{
				rules.NewRule(
					rules.WithLabelsRule(map[string]string{"cloud.google.com/compute-class-priority": "0"}),
					rules.WithTargetNodeCountRule(intPtr(4)),
				),
			}),
		),
	}
	ccLister := lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel)
	processor := NewMinCapacityPodListProcessor(ccLister, nil, nil, nil, nil)
	ctx := &ca_context.AutoscalingContext{
		ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
		CloudProvider:   &minCapacityMockCloudProvider{},
	}

	fakePods, err := processor.Process(ctx, nil)

	assert.NoError(t, err)
	assert.Len(t, fakePods, 2)
	for _, p := range fakePods {
		assert.Equal(t, validName, p.Spec.NodeSelector[labels.ComputeClassLabel])
	}
}

type minCapacityMockNodeGroup struct {
	cloudprovider.NodeGroup
	id         string
	targetSize int
	labels     map[string]string
}

func (m *minCapacityMockNodeGroup) Id() string {
	return m.id
}

func (m *minCapacityMockNodeGroup) TargetSize(ctx context.Context) (int, error) {
	return m.targetSize, nil
}

// GetOptions reports non-atomic scaling by default. Tests exercising atomic
// grouping can embed a node group that overrides this.
func (m *minCapacityMockNodeGroup) GetOptions(ctx context.Context, defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return &defaults, nil
}

func (m *minCapacityMockNodeGroup) Spec() *gkeclient.NodePoolSpec {
	return &gkeclient.NodePoolSpec{
		Labels: m.labels,
	}
}

func (m *minCapacityMockNodeGroup) TemplateNodeInfo(ctx context.Context) (*framework.NodeInfo, error) {
	return nil, nil
}

func (m *minCapacityMockNodeGroup) TemplateNodeLabels() (map[string]string, error) {
	return m.labels, nil
}

func (m *minCapacityMockNodeGroup) GceRef() gce.GceRef {
	return gce.GceRef{}
}

func (m *minCapacityMockNodeGroup) MachineType() string {
	return "n1-standard-2"
}

func (m *minCapacityMockNodeGroup) NodePoolName() string {
	return ""
}

func (m *minCapacityMockNodeGroup) NodeTpuCount() (int64, error) {
	return 0, nil
}

func (m *minCapacityMockNodeGroup) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

func (m *minCapacityMockNodeGroup) IsExtendedFallbacksEnabled() bool {
	return false
}

type minCapacityMockCloudProvider struct {
	cloudprovider.CloudProvider
	nodeGroups []cloudprovider.NodeGroup
}

func (m *minCapacityMockCloudProvider) NodeGroups(ctx context.Context) []cloudprovider.NodeGroup {
	return m.nodeGroups
}

func (m *minCapacityMockCloudProvider) IsAutopilotEnabled() bool {
	return false
}

// NodeGroupForNode returns no node group by default. Tests exercising atomic
// grouping can embed a provider that overrides this.
func (m *minCapacityMockCloudProvider) NodeGroupForNode(ctx context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	return nil, nil
}

// newEnforcers returns the enforcer to pass to the processor under test, the
// one of the next processor sharing the turns with it, and the function that
// starts a fairness loop. The processor's enforcer is created first, so the
// turn starts with it.
func newEnforcers(maxLoopsBeforeAdmission int) (processor, next fairness.FairnessEnforcer, startLoop func()) {
	manager := fairness.NewSharedEnforcerManager(maxLoopsBeforeAdmission, nil)
	return manager.CreateEnforcer("min-capacity"), manager.CreateEnforcer("next"), manager.StartLoop
}

func TestMinCapacityPodListProcessor_Fairness(t *testing.T) {
	pendingPods := []*apiv1.Pod{test.BuildTestPod("pending", 100, 100)}
	testCases := []struct {
		name                    string
		existingNodes           []*apiv1.Node
		maxLoopsBeforeAdmission int
		wantInjectedPods        int
		wantNextAdmitted        bool
	}{
		{
			name:             "no shortfall passes the turn to the next processor",
			existingNodes:    []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("n2", "my-ccc")},
			wantInjectedPods: 0,
			wantNextAdmitted: true,
		},
		{
			name:             "shortfall admitted injects the pods and uses the loop",
			existingNodes:    []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc")},
			wantInjectedPods: 1,
			wantNextAdmitted: false,
		},
		{
			name:                    "shortfall throttled injects nothing and keeps the turn",
			existingNodes:           []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc")},
			maxLoopsBeforeAdmission: 2,
			wantInjectedPods:        0,
			wantNextAdmitted:        false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crds := []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithLabel(labels.ComputeClassLabel),
					crd.WithTargetNodeCount(intPtr(2)),
				),
			}
			ccLister := lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel)
			enforcer, next, startLoop := newEnforcers(tc.maxLoopsBeforeAdmission)
			processor := NewMinCapacityPodListProcessor(ccLister, enforcer, nil, nil, nil)
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, n := range tc.existingNodes {
				if err := snapshot.AddNodeInfo(framework.NewNodeInfo(n, nil)); err != nil {
					t.Fatalf("AddNodeInfo(%s) failed: %v", n.Name, err)
				}
			}
			ctx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, CloudProvider: &minCapacityMockCloudProvider{}}
			startLoop()

			pods, err := processor.Process(ctx, pendingPods)

			assert.NoError(t, err)
			assert.Len(t, pods, len(pendingPods)+tc.wantInjectedPods)
			assert.Equal(t, tc.wantNextAdmitted, next.Admit(nil, true), "next processor admitted")
		})
	}
}

// failingCRDLister fails to list the ComputeClasses.
type failingCRDLister struct {
	*lister.MockCrdLister
}

func (failingCRDLister) ListCrds() ([]crd.CRD, error) {
	return nil, errors.New("ComputeClass lister unavailable")
}

// TestMinCapacityPodListProcessor_PassesTurnWhenItCannotRun checks that the
// processor passes the fairness turn when it cannot compute fake pods: holding
// the turn would starve the other processors sharing the enforcer.
func TestMinCapacityPodListProcessor_PassesTurnWhenItCannotRun(t *testing.T) {
	crds := []crd.CRD{
		crd.NewTestCrd(
			crd.WithName("my-ccc"),
			crd.WithLabel(labels.ComputeClassLabel),
			crd.WithTargetNodeCount(intPtr(2)),
		),
	}
	testCases := []struct {
		name               string
		ccLister           lister.Lister
		experimentsManager experiments.Manager
	}{
		{
			name:     "minimum capacity disabled",
			ccLister: lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel),
			experimentsManager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{experiments.ComputeClassMinCapacityEnabledFlag: false},
				map[string]string{},
			),
		},
		{
			name:     "no ComputeClasses",
			ccLister: lister.NewMockCrdListerWithLabel(nil, labels.ComputeClassLabel),
		},
		{
			name:     "listing ComputeClasses fails",
			ccLister: failingCRDLister{lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel)},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			enforcer, next, startLoop := newEnforcers(0)
			processor := NewMinCapacityPodListProcessor(tc.ccLister, enforcer, tc.experimentsManager, nil, nil)
			ctx := &ca_context.AutoscalingContext{ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t), CloudProvider: &minCapacityMockCloudProvider{}}
			startLoop()

			pods, err := processor.Process(ctx, nil)

			assert.NoError(t, err)
			assert.Empty(t, pods)
			assert.True(t, next.Admit(nil, true), "next processor admitted")
		})
	}
}

// TestMinCapacityPodListProcessor_PlacedPodsDifferFromShortfallPods checks that
// the pods Process places in the snapshot and the shortfall pods it returns in
// the same loop never share a name or a UID.
func TestMinCapacityPodListProcessor_PlacedPodsDifferFromShortfallPods(t *testing.T) {
	crds := []crd.CRD{crd.NewTestCrd(crd.WithName("my-ccc"), crd.WithLabel(labels.ComputeClassLabel), crd.WithTargetNodeCount(intPtr(4)))}
	processor := NewMinCapacityPodListProcessor(lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel), nil, nil, nil, nil)
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, name := range []string{"n1", "n2"} {
		if err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(buildNodeWithCCC(name, "my-ccc"))); err != nil {
			t.Fatalf("AddNodeInfo(%s) failed: %v", name, err)
		}
	}
	ctx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, CloudProvider: &minCapacityMockCloudProvider{}}

	shortfallPods, err := processor.Process(ctx, nil)

	if err != nil {
		t.Fatalf("Process() failed: %v", err)
	}
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		t.Fatalf("ListNodeInfos() failed: %v", err)
	}
	var placedNames, placedUIDs []string
	for _, nodeInfo := range nodeInfos {
		for _, podInfo := range nodeInfo.Pods() {
			placedNames = append(placedNames, podInfo.Pod.Name)
			placedUIDs = append(placedUIDs, string(podInfo.Pod.UID))
		}
	}
	// With a min capacity of 4 and 2 nodes, Process places a pod on each node and
	// returns 2 shortfall pods.
	if len(placedNames) != 2 || len(shortfallPods) != 2 {
		t.Fatalf("Process() placed %d pods and returned %d, want 2 and 2", len(placedNames), len(shortfallPods))
	}
	for _, pod := range shortfallPods {
		assert.NotContains(t, placedNames, pod.Name, "placed pod names")
		assert.NotContains(t, placedUIDs, string(pod.UID), "placed pod UIDs")
	}
}

func TestPlaceMinCapacityPods(t *testing.T) {
	testCases := []struct {
		name       string
		crds       []crd.CRD
		nodes      []*apiv1.Node
		pods       []*apiv1.Pod
		nodeFilter resourcequotas.NodeFilter
		// atomicNodeGroups maps atomic node group IDs to their nodes.
		atomicNodeGroups map[string][]string
		blockingLabels   []string
		// wantMinCapPods maps node names to their min capacity fake pods.
		wantMinCapPods map[string]int
	}{
		{
			name:           "at the min capacity: one pod on every node",
			crds:           []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes:          []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("n2", "my-ccc")},
			wantMinCapPods: map[string]int{"n1": 1, "n2": 1},
		},
		{
			name:           "above the min capacity: the first nodes by name get a pod",
			crds:           []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes:          []*apiv1.Node{buildNodeWithCCC("n3", "my-ccc"), buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("n2", "my-ccc")},
			wantMinCapPods: map[string]int{"n1": 1, "n2": 1, "n3": 0},
		},
		{
			name:           "below the min capacity: every node of the ComputeClass gets a pod",
			crds:           []crd.CRD{minCapacityCRD("my-ccc", 5)},
			nodes:          []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("other", "other-ccc"), buildNodeWithCCC("plain", "")},
			wantMinCapPods: map[string]int{"n1": 1, "other": 0, "plain": 0},
		},
		{
			name: "nodes being deleted and upcoming placeholders are skipped",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 5)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("to-be-deleted", "my-ccc", withTaint(taints.ToBeDeletedTaint)),
				buildNodeWithCCC("deleting", "my-ccc", withDeletionTimestamp()),
				buildNodeWithCCC("upcoming", "my-ccc", withAnnotation(annotations.NodeUpcomingAnnotation, "true")),
			},
			wantMinCapPods: map[string]int{"n1": 1, "to-be-deleted": 0, "deleting": 0, "upcoming": 0},
		},
		{
			name: "a defrag candidate gets a pod when needed for the min capacity",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("defrag-candidate", "my-ccc", withTaint(defrag.HardTaint)),
			},
			wantMinCapPods: map[string]int{"n1": 1, "defrag-candidate": 1},
		},
		{
			name: "a defrag candidate comes last",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("a-defrag-candidate", "my-ccc", withTaint(defrag.HardTaint)),
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("n2", "my-ccc"),
			},
			pods:           []*apiv1.Pod{test.BuildScheduledTestPod("workload", 100, 100, "a-defrag-candidate")},
			wantMinCapPods: map[string]int{"a-defrag-candidate": 0, "n1": 1, "n2": 1},
		},
		{
			name: "an atomic node group with a defrag candidate comes last, even when blocked from scale-down",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("a1", "my-ccc", withTaint(defrag.HardTaint), withLabel(labels.TPUSliceLabel, "slice")), buildNodeWithCCC("a2", "my-ccc"),
				buildNodeWithCCC("b1", "my-ccc"), buildNodeWithCCC("b2", "my-ccc"),
			},
			pods:             []*apiv1.Pod{test.BuildScheduledTestPod("workload", 100, 100, "a2")},
			atomicNodeGroups: map[string][]string{"slice-a": {"a1", "a2"}, "slice-b": {"b1", "b2"}},
			blockingLabels:   []string{labels.TPUSliceLabel},
			wantMinCapPods:   map[string]int{"a1": 0, "a2": 0, "b1": 1, "b2": 1},
		},
		{
			name:           "nodes excluded by the node filter are skipped",
			crds:           []crd.CRD{minCapacityCRD("my-ccc", 5)},
			nodes:          []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("surge", "my-ccc")},
			nodeFilter:     nodeNameFilter{"surge": true},
			wantMinCapPods: map[string]int{"n1": 1, "surge": 0},
		},
		{
			name:           "several ComputeClasses are processed independently",
			crds:           []crd.CRD{minCapacityCRD("a", 1), minCapacityCRD("b", 2), minCapacityCRD("none", 0)},
			nodes:          []*apiv1.Node{buildNodeWithCCC("a1", "a"), buildNodeWithCCC("a2", "a"), buildNodeWithCCC("b1", "b"), buildNodeWithCCC("none1", "none")},
			wantMinCapPods: map[string]int{"a1": 1, "a2": 0, "b1": 1, "none1": 0},
		},
		{
			name:           "nodes running workload pods come first",
			crds:           []crd.CRD{minCapacityCRD("my-ccc", 1)},
			nodes:          []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("n2", "my-ccc")},
			pods:           []*apiv1.Pod{test.BuildScheduledTestPod("workload", 100, 100, "n2")},
			wantMinCapPods: map[string]int{"n1": 0, "n2": 1},
		},
		{
			name:  "a node running workload pods gets a pod even at its pod capacity",
			crds:  []crd.CRD{minCapacityCRD("my-ccc", 1)},
			nodes: []*apiv1.Node{buildNodeWithCCC("a-idle", "my-ccc"), buildNodeWithCCC("b-full", "my-ccc", withPodCapacity(2))},
			pods: []*apiv1.Pod{
				test.BuildScheduledTestPod("workload1", 100, 100, "b-full"),
				test.BuildScheduledTestPod("workload2", 100, 100, "b-full"),
			},
			wantMinCapPods: map[string]int{"a-idle": 0, "b-full": 1},
		},
		{
			name:           "DaemonSet and mirror pods are not workload pods",
			crds:           []crd.CRD{minCapacityCRD("my-ccc", 1)},
			nodes:          []*apiv1.Node{buildNodeWithCCC("n1", "my-ccc"), buildNodeWithCCC("n2", "my-ccc"), buildNodeWithCCC("n3", "my-ccc")},
			pods:           []*apiv1.Pod{test.SetDSPodSpec(test.BuildScheduledTestPod("ds", 100, 100, "n1")), test.SetMirrorPodSpec(test.BuildScheduledTestPod("mirror", 100, 100, "n1")), test.BuildScheduledTestPod("workload", 100, 100, "n3")},
			wantMinCapPods: map[string]int{"n1": 0, "n2": 0, "n3": 1},
		},
		{
			name: "an atomic node group running workload pods is filled before an idle one",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("a1", "my-ccc"), buildNodeWithCCC("a2", "my-ccc"),
				buildNodeWithCCC("b1", "my-ccc"), buildNodeWithCCC("b2", "my-ccc"),
			},
			pods:             []*apiv1.Pod{test.BuildScheduledTestPod("workload", 100, 100, "b2")},
			atomicNodeGroups: map[string][]string{"slice-a": {"a1", "a2"}, "slice-b": {"b1", "b2"}},
			wantMinCapPods:   map[string]int{"a1": 0, "a2": 0, "b1": 1, "b2": 1},
		},
		{
			name: "an atomic node group blocked from scale-down is filled first",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("a1", "my-ccc"), buildNodeWithCCC("a2", "my-ccc"),
				buildNodeWithCCC("b1", "my-ccc", withLabel(labels.TPUSliceLabel, "slice")), buildNodeWithCCC("b2", "my-ccc"),
			},
			pods:             []*apiv1.Pod{test.BuildScheduledTestPod("workload", 100, 100, "a1")},
			atomicNodeGroups: map[string][]string{"slice-a": {"a1", "a2"}, "slice-b": {"b1", "b2"}},
			blockingLabels:   []string{labels.TPUSliceLabel},
			wantMinCapPods:   map[string]int{"a1": 0, "a2": 0, "b1": 1, "b2": 1},
		},
		{
			name: "among atomic node groups blocked from scale-down, one running workload pods is filled first",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("a1", "my-ccc"), buildNodeWithCCC("a2", "my-ccc"),
				buildNodeWithCCC("b1", "my-ccc", withLabel(labels.TPUSliceLabel, "slice-b")), buildNodeWithCCC("b2", "my-ccc"),
				buildNodeWithCCC("c1", "my-ccc", withLabel(labels.TPUSliceLabel, "slice-c")), buildNodeWithCCC("c2", "my-ccc"),
			},
			pods: []*apiv1.Pod{
				test.BuildScheduledTestPod("workload-a1", 100, 100, "a1"),
				test.BuildScheduledTestPod("workload-a2", 100, 100, "a2"),
				test.BuildScheduledTestPod("workload-c", 100, 100, "c2"),
			},
			atomicNodeGroups: map[string][]string{"slice-a": {"a1", "a2"}, "slice-b": {"b1", "b2"}, "slice-c": {"c1", "c2"}},
			blockingLabels:   []string{labels.TPUSliceLabel},
			wantMinCapPods:   map[string]int{"a1": 0, "a2": 0, "b1": 0, "b2": 0, "c1": 1, "c2": 1},
		},
		{
			name: "a larger idle atomic node group is filled before a smaller one",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 3)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("a1", "my-ccc"), buildNodeWithCCC("a2", "my-ccc"),
				buildNodeWithCCC("b1", "my-ccc"), buildNodeWithCCC("b2", "my-ccc"), buildNodeWithCCC("b3", "my-ccc"),
			},
			atomicNodeGroups: map[string][]string{"slice-a": {"a1", "a2"}, "slice-b": {"b1", "b2", "b3"}},
			wantMinCapPods:   map[string]int{"a1": 0, "a2": 0, "b1": 1, "b2": 1, "b3": 1},
		},
		{
			name: "a blocking label with an empty value does not block scale-down",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 1)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc", withLabel(labels.TPUSliceLabel, "")),
				buildNodeWithCCC("n2", "my-ccc"),
			},
			pods:           []*apiv1.Pod{test.BuildScheduledTestPod("workload", 100, 100, "n2")},
			blockingLabels: []string{labels.TPUSliceLabel},
			wantMinCapPods: map[string]int{"n1": 0, "n2": 1},
		},
		{
			name: "a node with the scale-down-disabled annotation comes first",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 1)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("n2", "my-ccc", withAnnotation(eligibility.ScaleDownDisabledKey, "true")),
			},
			pods:           []*apiv1.Pod{test.BuildScheduledTestPod("workload", 100, 100, "n1")},
			wantMinCapPods: map[string]int{"n1": 0, "n2": 1},
		},
		{
			name: "atomic node groups are filled before non-atomic nodes",
			crds: []crd.CRD{minCapacityCRD("my-ccc", 2)},
			nodes: []*apiv1.Node{
				buildNodeWithCCC("n1", "my-ccc"),
				buildNodeWithCCC("s1", "my-ccc"), buildNodeWithCCC("s2", "my-ccc"),
			},
			atomicNodeGroups: map[string][]string{"slice": {"s1", "s2"}},
			wantMinCapPods:   map[string]int{"n1": 0, "s1": 1, "s2": 1},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, n := range tc.nodes {
				var pods []*apiv1.Pod
				for _, p := range tc.pods {
					if p.Spec.NodeName == n.Name {
						pods = append(pods, p)
					}
				}
				if err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(n, pods...)); err != nil {
					t.Fatalf("AddNodeInfo(%s) failed: %v", n.Name, err)
				}
			}
			provider := &atomicNodeGroupsCloudProvider{nodeGroupsByNode: map[string]cloudprovider.NodeGroup{}}
			for id, nodeNames := range tc.atomicNodeGroups {
				nodeGroup := &atomicMockNodeGroup{minCapacityMockNodeGroup{id: id}}
				provider.nodeGroups = append(provider.nodeGroups, nodeGroup)
				for _, nodeName := range nodeNames {
					provider.nodeGroupsByNode[nodeName] = nodeGroup
				}
			}
			autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, CloudProvider: provider}
			processor := NewMinCapacityPodListProcessor(nil, nil, nil, tc.nodeFilter, tc.blockingLabels)

			err := processor.placeMinCapacityPods(autoscalingCtx, tc.crds)

			assert.NoError(t, err)
			for nodeName, want := range tc.wantMinCapPods {
				nodeInfo, err := snapshot.GetNodeInfo(nodeName)
				if err != nil {
					t.Fatalf("GetNodeInfo(%q) failed: %v", nodeName, err)
				}
				assert.Equal(t, want, countMinCapacityPods(nodeInfo), "min capacity pods on %s after placeMinCapacityPods()", nodeName)
			}
		})
	}
}

type atomicMockNodeGroup struct {
	minCapacityMockNodeGroup
}

func (m *atomicMockNodeGroup) GetOptions(ctx context.Context, defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	defaults.ZeroOrMaxNodeScaling = true
	return &defaults, nil
}

type atomicNodeGroupsCloudProvider struct {
	minCapacityMockCloudProvider
	nodeGroupsByNode map[string]cloudprovider.NodeGroup
}

func (m *atomicNodeGroupsCloudProvider) NodeGroupForNode(ctx context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	return m.nodeGroupsByNode[node.Name], nil
}

func countMinCapacityPods(nodeInfo *framework.NodeInfo) int {
	count := 0
	for _, podInfo := range nodeInfo.Pods() {
		if crd.IsMinCapacityFakePod(podInfo.Pod) {
			count++
		}
	}
	return count
}

func minCapacityCRD(name string, minCapacity int) crd.CRD {
	return crd.NewTestCrd(crd.WithName(name), crd.WithLabel(labels.ComputeClassLabel), crd.WithTargetNodeCount(intPtr(minCapacity)))
}
