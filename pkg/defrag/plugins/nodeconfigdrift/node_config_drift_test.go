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

package nodeconfigdrift

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/testutil"
	testCloudProvider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	csisnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/csi/snapshot"
	drasnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/snapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	maxCandidateNodeCount = 5
	testCrdLabel          = "test-crd"
)

func TestNodeConfigDriftNewCandidate(t *testing.T) {
	n2Family := "n2"
	c3Family := "c3"
	spot := true

	cccWithPriorities := newTestCrd(
		"ccc-with-priorities",
		true,
		false,
		map[string]string{"env": "prod"},
		"sa@project.iam.gserviceaccount.com",
		"cos_containerd",
		"1.30.0-gke.1",
		[]rules.Rule{
			rules.NewMachineSpecRule(&c3Family, &spot, nil, nil),
			rules.NewMachineSpecRule(&n2Family, nil, nil, nil),
		},
	)

	cccWithScaleUpAnyway := newTestCrd(
		"ccc-scale-up-anyway",
		true,
		true,
		map[string]string{"env": "prod"},
		"sa@project.iam.gserviceaccount.com",
		"cos_containerd",
		"1.30.0-gke.1",
		[]rules.Rule{
			rules.NewMachineSpecRule(&c3Family, &spot, nil, nil),
			rules.NewMachineSpecRule(&n2Family, nil, nil, nil),
		},
	)

	cccDisabledDrift := newTestCrd(
		"ccc-disabled",
		false,
		false,
		map[string]string{"env": "prod"},
		"",
		"",
		"",
		[]rules.Rule{
			rules.NewMachineSpecRule(&n2Family, nil, nil, nil),
		},
	)

	cccNoPriorities := newTestCrd(
		"ccc-no-priorities",
		true,
		false,
		map[string]string{"env": "prod"},
		"",
		"",
		"",
		nil,
	)

	testCases := []struct {
		name                      string
		nodeNames                 []string
		nodeGroups                []testutil.ExtendedNodeGroup
		crds                      []crd.CRD
		wantCandidateNodeNames    []string
		wantLatestUnfitNodesCount int
	}{
		{
			name:      "Drift: Missing required user label from high-level config",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							// Missing "env": "prod"
						},
						MachineType:    "n2-standard-4",
						ServiceAccount: "sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.30.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorities},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:      "Drift: Mismatched service account in high-level config",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							"env":        "prod",
						},
						MachineType:    "n2-standard-4",
						ServiceAccount: "wrong-sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.30.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorities},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:      "Drift: Mismatched node version in high-level config",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							"env":        "prod",
						},
						MachineType:    "n2-standard-4",
						ServiceAccount: "sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.29.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorities},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:      "Drift: Matches high-level config, but doesn't match ANY priority rule",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							"env":        "prod",
						},
						MachineType:    "e2-standard-4", // e2 is not in c3 or n2 priorities
						ServiceAccount: "sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.30.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorities},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:      "No drift: Matches high-level config and Priority 1 (c3 spot)",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							"env":        "prod",
						},
						MachineType:    "c3-standard-4",
						Spot:           true,
						ServiceAccount: "sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.30.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorities},
			wantCandidateNodeNames:    []string{},
			wantLatestUnfitNodesCount: 0,
		},
		{
			name:      "No drift: Matches high-level config and Priority 2 (n2 on-demand)",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							"env":        "prod",
						},
						MachineType:    "n2-standard-4",
						Spot:           false,
						ServiceAccount: "sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.30.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorities},
			wantCandidateNodeNames:    []string{},
			wantLatestUnfitNodesCount: 0,
		},
		{
			name:      "No drift: CCC without priorities, matches high-level config",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-no-priorities",
							"env":        "prod",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			crds:                      []crd.CRD{cccNoPriorities},
			wantCandidateNodeNames:    []string{},
			wantLatestUnfitNodesCount: 0,
		},
		{
			name:      "Drift: CCC without priorities, mismatched high-level config",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-no-priorities",
							// Missing "env": "prod"
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			crds:                      []crd.CRD{cccNoPriorities},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:      "Config drift disabled: Node is drifted but ConfigDrift is false",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-disabled",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			crds:                      []crd.CRD{cccDisabledDrift},
			wantCandidateNodeNames:    []string{},
			wantLatestUnfitNodesCount: 0,
		},
		{
			name:      "Multiple drifted nodes exceeding maxCandidateNodeCount",
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
							testCrdLabel: "ccc-with-priorities",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			crds:                      []crd.CRD{cccWithPriorities},
			wantCandidateNodeNames:    []string{"n1", "n2", "n3", "n4", "n5"},
			wantLatestUnfitNodesCount: 6,
		},
		{
			name:      "ScaleUpAnyway enabled: Node matches high-level config, but doesn't match priority rule - NOT drifted",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-scale-up-anyway",
							"env":        "prod",
						},
						MachineType:    "e2-standard-4", // e2 is not in c3 or n2 priorities, but ScaleUpAnyway is true
						ServiceAccount: "sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.30.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithScaleUpAnyway},
			wantCandidateNodeNames:    []string{},
			wantLatestUnfitNodesCount: 0,
		},
		{
			name:      "ScaleUpAnyway enabled: Node doesn't match high-level config (missing label) - DRIFTED",
			nodeNames: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-scale-up-anyway",
							// Missing "env": "prod"
						},
						MachineType:    "e2-standard-4",
						ServiceAccount: "sa@project.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeVersion:    "1.30.0-gke.1",
					},
				},
			},
			crds:                      []crd.CRD{cccWithScaleUpAnyway},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, p := initTestCase(t, tc.nodeGroups, tc.crds, testCrdLabel)
			candidate := p.NewCandidate(ctx, tc.nodeNames)

			if len(tc.wantCandidateNodeNames) == 0 {
				assert.Nil(t, candidate)
			} else {
				assert.NotNil(t, candidate)
				assert.Equal(t, tc.wantCandidateNodeNames, candidate.Nodes)
				assert.Equal(t, defrag.Partial, candidate.Mode)
			}

			assert.Equal(t, tc.wantLatestUnfitNodesCount, p.LatestUnfitNodesCount())
		})
	}
}

func TestNodeConfigDriftValidCandidateNodes(t *testing.T) {
	n2Family := "n2"
	cccWithPriorities := newTestCrd(
		"ccc-with-priorities",
		true,
		false,
		map[string]string{"env": "prod"},
		"",
		"",
		"",
		[]rules.Rule{
			rules.NewMachineSpecRule(&n2Family, nil, nil, nil),
		},
	)

	testCases := []struct {
		name                    string
		candidateNodes          []string
		nodeGroups              []testutil.ExtendedNodeGroup
		crds                    []crd.CRD
		wantValidCandidateNodes []string
	}{
		{
			name:           "Candidate nodes are drifted (missing label) - valid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			crds:                    []crd.CRD{cccWithPriorities},
			wantValidCandidateNodes: []string{"n1"},
		},
		{
			name:           "Candidate node is not drifted (compliant) - invalid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							"env":        "prod",
						},
						MachineType: "n2-standard-4",
					},
				},
			},
			crds:                    []crd.CRD{cccWithPriorities},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Candidate node info not found - invalid",
			candidateNodes: []string{"non-existing-node"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			crds:                    []crd.CRD{cccWithPriorities},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Config drift disabled: Node is drifted but ConfigDrift is false - invalid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-disabled",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			crds: []crd.CRD{
				newTestCrd("ccc-disabled", false, false, map[string]string{"env": "prod"}, "", "", "", []rules.Rule{rules.NewMachineSpecRule(&n2Family, nil, nil, nil)}),
			},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Partial candidate validity: one node drifted, one node compliant",
			candidateNodes: []string{"n1", "n2"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
						},
						MachineType: "e2-standard-4", // drifted (missing env: prod)
					},
				},
				{
					Name: "group2",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
							"env":        "prod",
						},
						MachineType: "n2-standard-4", // compliant
					},
				},
			},
			crds:                    []crd.CRD{cccWithPriorities},
			wantValidCandidateNodes: []string{"n1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, p := initTestCase(t, tc.nodeGroups, tc.crds, testCrdLabel)
			assert.Equal(t, tc.wantValidCandidateNodes, p.ValidCandidateNodes(ctx, tc.candidateNodes))
		})
	}
}

func TestNodeConfigDriftIsExpansionOptionValid(t *testing.T) {
	n2Family := "n2"
	cccWithPriorities := newTestCrd(
		"ccc-with-priorities",
		true,
		false,
		map[string]string{"env": "prod"},
		"",
		"",
		"",
		[]rules.Rule{
			rules.NewMachineSpecRule(&n2Family, nil, nil, nil),
		},
	)
	cccWithScaleUpAnyway := newTestCrd(
		"ccc-scale-up-anyway",
		true,
		true,
		map[string]string{"env": "prod"},
		"",
		"",
		"",
		[]rules.Rule{
			rules.NewMachineSpecRule(&n2Family, nil, nil, nil),
		},
	)

	testCases := []struct {
		name               string
		candidateNodes     []string
		nodeGroups         []testutil.ExtendedNodeGroup
		expandedNodeGroup  testutil.ExtendedNodeGroup
		crds               []crd.CRD
		wantExpansionValid bool
	}{
		{
			name:           "Expansion option is valid: compliant node pool matching CCC high-level config and priority",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
						},
						MachineType: "e2-standard-4", // drifted candidate
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "expanded-group",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "ccc-with-priorities",
						"env":        "prod",
					},
					MachineType: "n2-standard-4", // compliant
				},
			},
			crds:               []crd.CRD{cccWithPriorities},
			wantExpansionValid: true,
		},
		{
			name:           "Expansion option is invalid: target node pool is also drifted (wrong machine type)",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "expanded-group",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "ccc-with-priorities",
						"env":        "prod",
					},
					MachineType: "c3-standard-4", // not matching n2 priority
				},
			},
			crds:               []crd.CRD{cccWithPriorities},
			wantExpansionValid: false,
		},
		{
			name:           "Expansion option is invalid: target node pool belongs to a different CCC",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-with-priorities",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "expanded-group",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "other-ccc",
						"env":        "prod",
					},
					MachineType: "n2-standard-4",
				},
			},
			crds:               []crd.CRD{cccWithPriorities},
			wantExpansionValid: false,
		},
		{
			name:           "Empty candidate nodes: expansion invalid",
			candidateNodes: []string{},
			nodeGroups:     []testutil.ExtendedNodeGroup{},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "expanded-group",
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "ccc-with-priorities",
						"env":        "prod",
					},
					MachineType: "n2-standard-4",
				},
			},
			crds:               []crd.CRD{cccWithPriorities},
			wantExpansionValid: false,
		},
		{
			name:           "ScaleUpAnyway enabled: Expansion option is valid when target matches CRD config even if not matching priority",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-scale-up-anyway",
							// missing env: prod (drifted)
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "expanded-group",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "ccc-scale-up-anyway",
						"env":        "prod", // matches CRD config
					},
					MachineType: "e2-standard-4", // does not match n2 priority, but ScaleUpAnyway is true
				},
			},
			crds:               []crd.CRD{cccWithScaleUpAnyway},
			wantExpansionValid: true,
		},
		{
			name:           "ScaleUpAnyway enabled: Expansion option is invalid when target does not match CRD config",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-scale-up-anyway",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "expanded-group",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "ccc-scale-up-anyway",
						// missing env: prod
					},
					MachineType: "e2-standard-4",
				},
			},
			crds:               []crd.CRD{cccWithScaleUpAnyway},
			wantExpansionValid: false,
		},
		{
			name:           "Config drift disabled: Expansion option is invalid",
			candidateNodes: []string{"n1"},
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "group1",
					Nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							testCrdLabel: "ccc-disabled",
						},
						MachineType: "e2-standard-4",
					},
				},
			},
			expandedNodeGroup: testutil.ExtendedNodeGroup{
				Name: "expanded-group",
				Nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
				},
				Spec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "ccc-disabled",
						"env":        "prod",
					},
					MachineType: "n2-standard-4",
				},
			},
			crds: []crd.CRD{
				newTestCrd("ccc-disabled", false, false, map[string]string{"env": "prod"}, "", "", "", []rules.Rule{rules.NewMachineSpecRule(&n2Family, nil, nil, nil)}),
			},
			wantExpansionValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expansionOption := expander.Option{
				NodeGroup: testutil.CreateMig(tc.expandedNodeGroup, testutil.MakeMockGkeManager()),
				NodeCount: len(tc.expandedNodeGroup.Nodes),
			}
			ctx, p := initTestCase(t, tc.nodeGroups, tc.crds, testCrdLabel)
			candidate := defrag.NewCandidate(tc.candidateNodes, defrag.CreateBeforeDelete)
			assert.Equal(t, tc.wantExpansionValid, p.IsExpansionOptionValid(ctx, candidate, expansionOption))
		})
	}
}

func TestNodeConfigDriftIsListerValid(t *testing.T) {
	// Untyped nil should not panic and return false.
	cfgUntypedNil := config.New(config.Options{NPCLister: nil})
	pUntypedNil := NewPlugin(cfgUntypedNil).(*plugin)
	assert.False(t, pUntypedNil.isListerValid())

	// Typed nil should not panic and return false.
	var typedNilLister npc_lister.Lister = (*npc_lister.MockCrdLister)(nil)
	cfgTypedNil := config.New(config.Options{NPCLister: typedNilLister})
	pTypedNil := NewPlugin(cfgTypedNil).(*plugin)
	assert.False(t, pTypedNil.isListerValid())

	// Valid lister should return true.
	cfgValid := config.New(config.Options{NPCLister: npc_lister.NewMockCrdLister(nil)})
	pValid := NewPlugin(cfgValid).(*plugin)
	assert.True(t, pValid.isListerValid())
}

func initTestCase(t *testing.T, nodeGroups []testutil.ExtendedNodeGroup, crds []crd.CRD, crdLabel string) (*ca_context.AutoscalingContext, defrag.Plugin) {
	crdLister := npc_lister.NewMockCrdLister(crds)
	crdLister.SetCrdLabel(crdLabel)

	cp := testCloudProvider.NewTestCloudProviderBuilder().Build()
	var allNodes []*apiv1.Node
	for _, ng := range nodeGroups {
		mig := testutil.CreateMig(ng, testutil.MakeMockGkeManager())
		cp.InsertNodeGroup(mig)
		for _, node := range ng.Nodes {
			cp.AddNode(mig.Id(), node)
			allNodes = append(allNodes, node)
		}
	}

	cs := testsnapshot.NewTestSnapshotOrDie(t)
	assert.NoError(t, cs.SetClusterState(context.TODO(), allNodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))
	ctx := &ca_context.AutoscalingContext{
		ClusterSnapshot: cs,
		CloudProvider:   cp,
	}

	return ctx, buildPlugin(crdLister)
}

func buildPlugin(lister npc_lister.Lister) *plugin {
	provider := testutil.MockPluginProvider{}
	provider.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.E2)
	provider.On("IsAutopilotEnabled").Return(false)
	cfg := config.New(config.Options{
		MaxCandidateNodeCount: maxCandidateNodeCount,
		NPCLister:             lister,
		Provider:              &provider,
		Autopilot:             false,
	})
	p := NewPlugin(cfg)
	pImpl, _ := p.(*plugin)
	return pImpl
}

func newTestCrd(name string, configDrift bool, scaleUpAnyway bool, userLabels map[string]string, serviceAccount string, imageType string, nodeVersion string, priorityRules []rules.Rule) crd.CRD {
	opts := []crd.TestCrdOption{
		crd.WithLabel(testCrdLabel),
		crd.WithName(name),
		crd.WithConfigDrift(configDrift),
	}
	if scaleUpAnyway {
		opts = append(opts, crd.WithScaleUpAnyway())
	}
	if userLabels != nil {
		opts = append(opts, crd.WithUserDefinedLabels(userLabels))
	}
	if serviceAccount != "" {
		opts = append(opts, crd.WithServiceAccount(serviceAccount))
	}
	if imageType != "" {
		opts = append(opts, crd.WithImageType(imageType))
	}
	if nodeVersion != "" {
		opts = append(opts, crd.WithNodeVersion(nodeVersion))
	}
	if len(priorityRules) > 0 {
		opts = append(opts, crd.WithRules(priorityRules))
	}
	return crd.NewTestCrd(opts...)
}
