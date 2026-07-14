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

package highprioritymigration

import (
	"math/rand"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "k8s.io/api/core/v1"
	testCloudProvider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/testutil"
	"k8s.io/utils/ptr"
)

const (
	maxCandidateNodeCount = 5
	testCrdLabel          = "test-crd"
)

func initTestCase(t *testing.T, nodeGroups []testutil.ExtendedNodeGroup, crds []crd.CRD, defaultCrd bool, defaultCrdName string, crdLabel string) (*context.AutoscalingContext, defrag.Plugin) {

	crdLister := npc_lister.NewMockCrdLister(crds)
	crdLister.SetCrdLabel(crdLabel)
	if defaultCrd {
		crdLister.SetDefaultCrdName(defaultCrdName)
	}

	cp := testCloudProvider.NewTestCloudProviderBuilder().Build()
	var allNodes []*apiv1.Node
	for _, ng := range nodeGroups {
		mig := testutil.CreateMig(ng, makeMockGkeManager())
		cp.InsertNodeGroup(mig)
		for _, node := range ng.Nodes {
			cp.AddNode(mig.Id(), node)
			allNodes = append(allNodes, node)
		}
	}

	cs := testsnapshot.NewTestSnapshotOrDie(t)
	assert.NoError(t, cs.SetClusterState(allNodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))
	ctx := &context.AutoscalingContext{
		ClusterSnapshot: cs,
		CloudProvider:   cp,
	}

	return ctx, buildPlugin(crdLister)
}

func TestHighPriorityMigrationNewCandidate(t *testing.T) {
	testDefaultCrdName := "test-crd1"
	testNoMigrationCrdName := "test-no-migration-crd"
	testNotConfiguredMigrationCrdName := "test-no-reconciliation-crd"
	testDefaultCrd := newCrd(testDefaultCrdName, true)
	testNoMigrationCrd := newCrd(testNoMigrationCrdName, false)
	testNotConfiguredMigrationCrd := newCrd(testNotConfiguredMigrationCrdName, false)

	cccWithPriorityScoreName := "ccc-with-priority-score"
	cccWithPriorityScore := newCccWithPriorityScore(cccWithPriorityScoreName)

	testCases := []struct {
		name                      string
		nodeNames                 []string
		nodeGroups                []testutil.ExtendedNodeGroup
		defaultCrd                bool
		wantCandidateNodeNames    []string
		wantLatestUnfitNodesCount int
		crds                      []crd.CRD
	}{
		{
			name:      "Default is set, finds potential candidate",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						MachineType: "n2-standard-4",
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
			},
			crds:                      []crd.CRD{testDefaultCrd},
			defaultCrd:                true,
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 3,
		},
		{
			name:      "Default is set, but does not exist - no candidate",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						MachineType: "n2-standard-4",
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
			},
			defaultCrd:             true,
			wantCandidateNodeNames: []string{},
		},
		{
			name:      "No potential candidate nodes exists - No crds",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
			},
			wantCandidateNodeNames: []string{},
		},
		{
			name:      "No potential candidate nodes exists - OptimizeRulePriority set to false",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testNoMigrationCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
			},
			crds:                   []crd.CRD{testNoMigrationCrd},
			wantCandidateNodeNames: []string{},
		},
		{
			name:      "No potential candidate nodes exists - no ActiveMigration configured defaults to disabled",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testNotConfiguredMigrationCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
			},
			crds:                   []crd.CRD{testNotConfiguredMigrationCrd},
			wantCandidateNodeNames: []string{},
		},
		{
			name:      "No potential candidate nodes exists - All have highest priority",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						MachineType: "c3-standard-4",
						Spot:        true,
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						MachineType: "c3-standard-4",
						Spot:        true,
					},
				},
			},
			wantCandidateNodeNames: []string{},
		},
		{
			name:      "Only one potential candidate node group exists",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
			},
			crds:                      []crd.CRD{testDefaultCrd},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:      "Multiple candidate nodes group exists - Exceeds maxCandidateNodeCount",
			nodeNames: []string{"n1", "n2", "n3", "n4", "n5", "n6"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
						test.BuildTestNode("n4", 1000, 10),
						test.BuildTestNode("n5", 1000, 10),
						test.BuildTestNode("n6", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			crds:                      []crd.CRD{testDefaultCrd},
			wantCandidateNodeNames:    []string{"n1", "n2", "n3", "n4", "n5"},
			wantLatestUnfitNodesCount: 6,
		},
		{
			name:      "Multiple potential candidate nodes group exists - Multiple Node group",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n4", 1000, 10),
						test.BuildTestNode("n5", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
						Spot:        true,
					},
				},
			},
			crds:                      []crd.CRD{testDefaultCrd},
			wantCandidateNodeNames:    []string{"n1", "n2", "n3"},
			wantLatestUnfitNodesCount: 3,
		},
		{
			name:      "CCC with priorityScore - no potential candiates",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4",
						Spot:        true, // top priority group.
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "e2-standard-4",
						Spot:        true, // top priority group
					},
				},
			},
			crds:       []crd.CRD{cccWithPriorityScore},
			defaultCrd: true,
		},
		{
			name:      "CCC with priorityScore - potential candiates",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4",
						Spot:        true, // top priority group.
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "e2-standard-4",
						Spot:        true, // top priority group
					},
				},
				{
					Name: "group3",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n3", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4", // 2nd priority group
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorityScore},
			defaultCrd:                true,
			wantCandidateNodeNames:    []string{"n3"},
			wantLatestUnfitNodesCount: 1,
		},
	}
	// TODO: test effects of default CCC
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crdLabel := testCrdLabel
			if len(tc.crds) > 0 {
				crdLabel = tc.crds[0].Label() // assumption: all crds have the same label.
			}
			ctx, plugin := initTestCase(t, tc.nodeGroups, tc.crds, tc.defaultCrd, testDefaultCrdName, crdLabel)
			candidate := plugin.NewCandidate(ctx, tc.nodeNames)

			if len(tc.wantCandidateNodeNames) == 0 {
				assert.Nil(t, candidate)
			} else {
				assert.NotNil(t, candidate)
				assert.Equal(t, tc.wantCandidateNodeNames, candidate.Nodes)
				assert.Equal(t, defrag.Partial, candidate.Mode)
			}

			latestUnfitNodesCount := plugin.LatestUnfitNodesCount()
			if latestUnfitNodesCount != tc.wantLatestUnfitNodesCount {
				t.Errorf("plugin.LatestUnfitNodesCount() got latest unfit node count: %d, want latest unfit node count: %d", latestUnfitNodesCount, tc.wantLatestUnfitNodesCount)
			}
		})
	}
}

func TestHighPriorityMigrationValidCandidateNodes(t *testing.T) {
	testDefaultCrdName := "test-crd1"
	testNoMigrationCrdName := "test-no-migration-crd"
	testDefaultCrd := newCrd(testDefaultCrdName, true)
	testNoMigrationCrd := newCrd(testNoMigrationCrdName, false)

	cccWithPriorityScoreName := "ccc-with-priority-score"
	cccWithPriorityScore := newCccWithPriorityScore(cccWithPriorityScoreName)

	testCases := []struct {
		name                    string
		candidateNodes          []string
		nodeGroups              []testutil.ExtendedNodeGroup
		crds                    []crd.CRD
		defaultCrd              bool
		wantValidCandidateNodes []string
	}{
		{
			name:           "Candidate is valid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			crds:                    []crd.CRD{testDefaultCrd},
			wantValidCandidateNodes: []string{"n1"},
		},
		{
			name:           "Candidate using default is valid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						MachineType: "n2-standard-4",
					},
				},
			},
			crds:                    []crd.CRD{testDefaultCrd},
			defaultCrd:              true,
			wantValidCandidateNodes: []string{"n1"},
		},
		{
			name:           "Candidate is invalid - no crd",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Candidate using default which does not exist - invalid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						MachineType: "n2-standard-4",
					},
				},
			},
			defaultCrd:              true,
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Candidate is invalid - no crd label, no default",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						MachineType: "n2-standard-4",
					},
				},
			},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Candidate is invalid - Node has highest priority",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "c3-standard-4",
						Spot:        true,
					},
				},
			},
			crds:                    []crd.CRD{testDefaultCrd},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Candidate is invalid - OptimizeRulePriority is false",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testNoMigrationCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			crds:                    []crd.CRD{testNoMigrationCrd},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Candidate is invalid - NodeInfo doesn't exist",
			candidateNodes: []string{"n2"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testNoMigrationCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			crds:                    []crd.CRD{testNoMigrationCrd},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "CCC with priorityScore - candidate is not valid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4",
						Spot:        true, // top priority group
					},
				},
			},
			crds:                    []crd.CRD{cccWithPriorityScore},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "CCC with priorityScore - candidate is valid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4", // 2nd priority group
					},
				},
			},
			crds:                    []crd.CRD{cccWithPriorityScore},
			wantValidCandidateNodes: []string{"n1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crdLabel := testCrdLabel
			if len(tc.crds) > 0 {
				crdLabel = tc.crds[0].Label() // assumption: all crds have the same label.
			}
			ctx, plugin := initTestCase(t, tc.nodeGroups, tc.crds, tc.defaultCrd, testDefaultCrdName, crdLabel)

			assert.Equal(t, tc.wantValidCandidateNodes, plugin.ValidCandidateNodes(ctx, tc.candidateNodes))
		})
	}
}

func TestHighPriorityMigrationIsExpansionValid(t *testing.T) {
	testDefaultCrdName := "test-crd1"
	testDefaultCrd := newCrd(testDefaultCrdName, true)

	cccWithPriorityScoreName := "ccc-with-priority-score"
	cccWithPriorityScore := newCccWithPriorityScore(cccWithPriorityScoreName)

	testCases := []struct {
		name               string
		candidateNodes     []string
		nodeGroups         []testutil.ExtendedNodeGroup
		expandedNodeGroup  testutil.ExtendedNodeGroup
		defaultCrd         bool
		wantExpansionValid bool
		crds               []crd.CRD
	}{
		{
			name:           "Expansion option is valid, node is migrated to higher priority",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "group1",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: testDefaultCrdName,
					},
					MachineType: "n2-standard-4",
					Spot:        true,
				},
			},
			crds:               []crd.CRD{testDefaultCrd},
			wantExpansionValid: true,
		},
		{
			name:           "Expansion option is invalid, node is migrated to lower priority",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "e2-standard-4",
						Spot:        true,
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "group1",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: testDefaultCrdName,
					},
					MachineType: "n2-standard-4",
					Spot:        true,
				},
			},
			crds:               []crd.CRD{testDefaultCrd},
			wantExpansionValid: false,
		},
		{
			name:           "Expansion option is invalid, nodes are not migrated",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "e2-standard-4",
						Spot:        true,
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "group1",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: testDefaultCrdName,
					},
					MachineType: "e2-standard-4",
					Spot:        true,
				},
			},
			crds:               []crd.CRD{testDefaultCrd},
			wantExpansionValid: false,
		},
		{
			name:           "Expansion option is valid, no crd",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "e2-standard-4",
						Spot:        true,
					},
				},
			},
			wantExpansionValid: false,
		},
		{
			name:           "CCC with priorityScore - expansion option is valid, node is migrated to higher priority group",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4", // 2nd priority group
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "group1",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
					},
					MachineType: "e2-standard-8",
					Spot:        true, // top priority group
				},
			},
			crds:               []crd.CRD{cccWithPriorityScore},
			wantExpansionValid: true,
		},
		{
			name:           "CCC with priorityScore - expansion option is not valid, node is migrated to same priority group",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4", // 2nd priority group
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "group1",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
					},
					MachineType: "e2-standard-8", // 2nd priority group
				},
			},
			crds:               []crd.CRD{cccWithPriorityScore},
			wantExpansionValid: false,
		},
		{
			name:           "CCC with priorityScore - expansion option is not valid, node is migrated to lower priority group",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
						},
						MachineType: "n2-standard-4", // 2nd priority group
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "group1",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: cccWithPriorityScoreName,
					},
					MachineType: "c3-standard-8", // 3rd priority group
				},
			},
			crds:               []crd.CRD{cccWithPriorityScore},
			wantExpansionValid: false,
		},
		{
			name:           "one candidate node no longer exists",
			candidateNodes: []string{"n1", "n2"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: testDefaultCrdName,
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "group1",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: testDefaultCrdName,
					},
					MachineType: "n2-standard-4",
					Spot:        true,
				},
			},
			crds:               []crd.CRD{testDefaultCrd},
			wantExpansionValid: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			expansionOption := expander.Option{
				NodeGroup: testutil.CreateMig(tc.expandedNodeGroup, makeMockGkeManager()),
				NodeCount: len(tc.expandedNodeGroup.Nodes),
			}
			crdLabel := testCrdLabel
			if len(tc.crds) > 0 {
				crdLabel = tc.crds[0].Label() // assumption: all crds have the same label.
			}
			ctx, plugin := initTestCase(t, tc.nodeGroups, tc.crds, tc.defaultCrd, testDefaultCrdName, crdLabel)
			candidate := defrag.NewCandidate(tc.candidateNodes, defrag.CreateBeforeDelete)
			assert.Equal(t, tc.wantExpansionValid, plugin.IsExpansionOptionValid(ctx, candidate, expansionOption))
		})
	}
}

func newCrd(name string, optimizeRulePriority bool) crd.CRD {
	n2 := "n2"
	e2 := "e2"
	c3 := "c3"
	spot := true
	testCrdoptions := []crd.TestCrdOption{
		crd.WithRules([]rules.Rule{
			rules.NewMachineSpecRule(&c3, &spot, nil, nil),
			rules.NewMachineSpecRule(&e2, &spot, nil, nil),
			rules.NewMachineSpecRule(&n2, &spot, nil, nil),
			rules.NewMachineSpecRule(&n2, nil, nil, nil),
		}),
		crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled(),
		crd.WithLabel(testCrdLabel), crd.WithName(name)}
	if optimizeRulePriority {
		testCrdoptions = append(testCrdoptions, crd.WithOptimizeRulePriority())
	}
	crd := crd.NewTestCrd(testCrdoptions...)
	return crd
}

func newCccWithPriorityScore(name string) crd.CRD {
	return ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.ComputeClassSpec{
			NodePoolAutoCreation: &v1.NodePoolAutoCreation{
				Enabled: true,
			},
			WhenUnsatisfiable: "ScaleUpAnyway",
			ActiveMigration: &v1.ActiveMigration{
				OptimizeRulePriority: true,
			},
			Priorities: []v1.Priority{
				{
					MachineFamily: ptr.To("n2"),
					Spot:          ptr.To(true),
					PriorityScore: ptr.To(10),
				},
				{
					MachineFamily: ptr.To("n2"),
					PriorityScore: ptr.To(5),
				},
				{
					MachineFamily: ptr.To("e2"),
					Spot:          ptr.To(true),
					PriorityScore: ptr.To(10),
				},
				{
					MachineFamily: ptr.To("e2"),
					PriorityScore: ptr.To(5),
				},
				{
					MachineFamily: ptr.To("c3"),
					PriorityScore: ptr.To(2),
				},
			},
		},
	}, "test-project", false, crd.TestDefaultDataProvider(), nil)
}

func NewMockHighPriorityMigrationPlugin(config config.PluginsConfig) *plugin {
	p := NewPlugin(config)
	pImpl, _ := p.(*plugin)
	pImpl.randomGenerator = rand.New(&mockRandSource{})
	return pImpl
}

func buildPlugin(lister npc_lister.Lister) *plugin {
	provider := mockPluginProvider{}
	provider.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.E2)
	provider.On("IsAutopilotEnabled").Return(false)
	config := config.New(config.Options{
		MaxCandidateNodeCount: maxCandidateNodeCount,
		NPCLister:             lister,
		Provider:              &provider,
		Autopilot:             false,
	})
	return NewMockHighPriorityMigrationPlugin(config)
}

type mockRandSource struct{}

func (mrs *mockRandSource) Int63() int64 {
	return 0
}
func (mrs *mockRandSource) Seed(seed int64) {}

func makeMockGkeManager() gke.GkeManager {
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil)
	return gkeManager
}

type mockPluginProvider struct {
	mock.Mock
}

func (m *mockPluginProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	args := m.Called()
	return args.Get(0).(machinetypes.MachineFamily)
}

func (m *mockPluginProvider) IsAutopilotEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}
