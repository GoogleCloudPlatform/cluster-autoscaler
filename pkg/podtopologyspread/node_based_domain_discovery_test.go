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

package podtopologyspread

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestNodeBasedDomainDiscoveryEligiblePTSPods(t *testing.T) {
	testCases := []struct {
		name               string
		experimentDisabled bool
		existingNodes      []*apiv1.Node
		nodeGroupInfos     []nodeGroupInfoForTest
		pods               []*apiv1.Pod
		wantConfigs        []ptsEligibilityForTest
	}{
		{
			name: "no pods",
		},
		{
			name: "pod without PTS",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1),
			},
		},
		{
			name:               "pod with custom PTS, experiment disabled",
			experimentDisabled: true,
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{labels.MachineFamilyLabel: "e2"}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: nil,
		},
		{
			name: "pod with custom PTS, domain found on existing node",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{labels.MachineFamilyLabel: "e2"}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: []ptsEligibilityForTest{
				{
					domainNames:         []string{"e2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with custom PTS, domain found on empty node pool template",
			nodeGroupInfos: []nodeGroupInfoForTest{
				{
					id:           "ng1",
					templateNode: buildTestNodeWithLabels("t1", map[string]string{labels.MachineFamilyLabel: "n2"}),
				},
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: []ptsEligibilityForTest{
				{
					domainNames:         []string{"n2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with custom PTS and MinDomains enforcement - not eligible if count is less than min domains",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{labels.MachineFamilyLabel: "e2"}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(2),
					}),
				),
			},
			wantConfigs: nil,
		},
		{
			name: "pod with custom PTS and MinDomains enforcement - eligible if count matches min domains",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{labels.MachineFamilyLabel: "e2"}),
				buildTestNodeWithLabels("n2", map[string]string{labels.MachineFamilyLabel: "n2"}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(2),
					}),
				),
			},
			wantConfigs: []ptsEligibilityForTest{
				{
					domainNames:         []string{"e2", "n2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with custom PTS, domain found only on a single node (not in any node group)",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{labels.MachineFamilyLabel: "e2"}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: []ptsEligibilityForTest{
				{
					domainNames:         []string{"e2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
			},
		},
		{
			name: "multiple pods with custom PTS, domains found on both existing node and empty node pool",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{labels.MachineFamilyLabel: "e2"}),
			},
			nodeGroupInfos: []nodeGroupInfoForTest{
				{
					id:           "ng1",
					templateNode: buildTestNodeWithLabels("ng1-template-node", map[string]string{labels.MachineFamilyLabel: "n2"}),
				},
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
				test.BuildTestPod("p2", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: []ptsEligibilityForTest{
				{
					domainNames:         []string{"e2", "n2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
				{
					domainNames:         []string{"e2", "n2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p2",
				},
			},
		},
		{
			name: "multiple pods with custom PTS",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{labels.MachineFamilyLabel: "e2"}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
				test.BuildTestPod("p2", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: []ptsEligibilityForTest{
				{
					domainNames:         []string{"e2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
				{
					domainNames:         []string{"e2"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p2",
				},
			},
		},
		{
			name: "multiple constraints on single pod - prefer DoNotSchedule over ScheduleAnyway (first soft, second hard)",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{
					labels.MachineFamilyLabel: "e2",
					"custom-topology-label":   "custom-topology-value",
				}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       labels.MachineFamilyLabel,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       "custom-topology-label",
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: []ptsEligibilityForTest{
				{
					domainNames:         []string{"custom-topology-value"},
					domainDiscoveryName: nodeBasedDomainDiscoveryName,
					constraintIdx:       proto.Int32(1),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with hostname topology key - not eligible",
			existingNodes: []*apiv1.Node{
				buildTestNodeWithLabels("n1", map[string]string{apiv1.LabelHostname: "n1"}),
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelHostname,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
					}),
				),
			},
			wantConfigs: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, node := range tc.existingNodes {
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node))
				assert.NoError(t, err)
			}

			var ngs []cloudprovider.NodeGroup
			for _, ngInfo := range tc.nodeGroupInfos {
				ngs = append(ngs, &mockNodeGroup{
					id:           ngInfo.id,
					templateNode: ngInfo.templateNode,
				})
			}

			cp := &mockCloudProvider{nodeGroups: ngs}
			exps := []string{}
			if !tc.experimentDisabled {
				exps = []string{nodeBasedExperimentName}
			}
			experimentsManager := experiments.NewMockManager(exps...)

			dd := NewNodeBasedDomainDiscovery(experimentsManager, snapshot, cp)
			configs := dd.EligiblePTSPods(tc.pods)

			wantConfigs := ptsEligibilityFromTestOnes(t, tc.wantConfigs, tc.pods)

			assert.ElementsMatch(t, wantConfigs, configs)
		})
	}
}

func buildTestNodeWithLabels(name string, labels map[string]string) *apiv1.Node {
	node := test.BuildTestNode(name, 1000, 1000)
	node.Labels = labels
	return node
}

type nodeGroupInfoForTest struct {
	id           string
	templateNode *apiv1.Node
}

type mockNodeGroup struct {
	id           string
	templateNode *apiv1.Node
}

func (m *mockNodeGroup) Id() string                               { return m.id }
func (m *mockNodeGroup) MinSize() int                             { return 0 }
func (m *mockNodeGroup) MaxSize() int                             { return 10 }
func (m *mockNodeGroup) TargetSize() (int, error)                 { return 0, nil }
func (m *mockNodeGroup) IncreaseSize(delta int) error             { return nil }
func (m *mockNodeGroup) DecreaseTargetSize(delta int) error       { return nil }
func (m *mockNodeGroup) DeleteNodes([]*apiv1.Node) error          { return nil }
func (m *mockNodeGroup) DecreaseSize(delta int) error             { return nil }
func (m *mockNodeGroup) Nodes() ([]cloudprovider.Instance, error) { return nil, nil }
func (m *mockNodeGroup) TemplateNodeInfo() (*framework.NodeInfo, error) {
	if m.templateNode == nil {
		return nil, cloudprovider.ErrNotImplemented
	}
	return framework.NewTestNodeInfo(m.templateNode), nil
}
func (m *mockNodeGroup) Exist() bool                              { return true }
func (m *mockNodeGroup) Create() (cloudprovider.NodeGroup, error) { return nil, nil }
func (m *mockNodeGroup) Delete() error                            { return nil }
func (m *mockNodeGroup) Autoprovisioned() bool                    { return false }
func (m *mockNodeGroup) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return nil, cloudprovider.ErrNotImplemented
}
func (m *mockNodeGroup) AtomicIncreaseSize(delta int) error   { return cloudprovider.ErrNotImplemented }
func (m *mockNodeGroup) ForceDeleteNodes([]*apiv1.Node) error { return cloudprovider.ErrNotImplemented }
func (m *mockNodeGroup) Debug() string                        { return "" }

type mockCloudProvider struct {
	cloudprovider.CloudProvider
	nodeGroups []cloudprovider.NodeGroup
}

func (m *mockCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	return m.nodeGroups
}
