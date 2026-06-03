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

package crd

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestNodeCrdLabel(t *testing.T) {
	testCrdLabel := "test.io/crd"

	testCases := []struct {
		name        string
		node        *apiv1.Node
		wantCrdName string
		wantFound   bool
		wantErr     bool
	}{
		{
			name: "node without crd label",
			node: buildNodeWithLabels(map[string]string{
				"random-label": "random-value",
			}),
			wantFound: false,
		},
		{
			name: "node has empty crd label",
			node: buildNodeWithLabels(map[string]string{
				testCrdLabel: "",
			}),
			wantCrdName: "",
			wantFound:   true,
		},
		{
			name: "node has non-empty crd label",
			node: buildNodeWithLabels(map[string]string{
				testCrdLabel: "test-crd",
			}),
			wantCrdName: "test-crd",
			wantFound:   true,
		},
		{
			name:      "node is nil",
			node:      nil,
			wantFound: false,
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crd, found, err := NodeCrdLabel(tc.node, testCrdLabel)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tc.wantCrdName, crd)
				assert.Equal(t, tc.wantFound, found)
				assert.NoError(t, err)
			}
		})
	}
}

func TestNodeGroupCrdLabel(t *testing.T) {
	testCrdLabel := "test.io/crd"

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name        string
		nodeGroup   *gke.GkeMig
		node        *apiv1.Node
		nodeErr     error
		wantCrdName string
		wantFound   bool
		wantErr     bool
	}{
		{
			name: "Mig with crd label set to empty string",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Labels: map[string]string{testCrdLabel: ""}}).
				Build(),
			node:        &apiv1.Node{},
			wantCrdName: "",
			wantFound:   true,
		},
		{
			name: "Mig with crd label set to non-empty string",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Labels: map[string]string{testCrdLabel: "test-crd"}}).
				Build(),
			node:        &apiv1.Node{},
			wantCrdName: "test-crd",
			wantFound:   true,
		},
		{
			name: "Mig without crd label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			node:    &apiv1.Node{},
			nodeErr: errors.New("nodeGroup node template error"),
			wantErr: true,
		},
		{
			name: "Mig without crd label, node without crd label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			node:      &apiv1.Node{},
			wantFound: false,
		},
		{
			name: "Mig without crd label, node has empty crd label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetGkeManager(gkeManager).
				Build(),
			node: buildNodeWithLabels(map[string]string{
				testCrdLabel: "",
			}),
			wantCrdName: "",
			wantFound:   true,
		},
		{
			name: "Mig without crd label, node has non-empty crd label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetGkeManager(gkeManager).
				Build(),
			node: buildNodeWithLabels(map[string]string{
				testCrdLabel: "test-crd",
			}),
			wantCrdName: "test-crd",
			wantFound:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManager.On("GetMigTemplateNodeInfo", tc.nodeGroup).Return(framework.NewTestNodeInfo(tc.node), tc.nodeErr).Once()
			crd, found, err := NodeGroupCrdLabel(tc.nodeGroup, testCrdLabel)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tc.wantCrdName, crd)
				assert.Equal(t, tc.wantFound, found)
				assert.NoError(t, err)
			}
		})
	}
}

func TestMigCrdTaint(t *testing.T) {
	testCrdLabel := "test.io/crd"

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name        string
		nodeGroup   cloudprovider.NodeGroup
		node        *apiv1.Node
		nodeErr     error
		wantCrdName string
		wantErr     bool
	}{
		{
			name:      "Nil nodeGroup",
			nodeGroup: nil,
			node:      &apiv1.Node{},
			wantErr:   true,
		},
		{
			name: "Mig without crd taint",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			node:        &apiv1.Node{},
			nodeErr:     errors.New("nodeGroup node template error"),
			wantCrdName: "",
			wantErr:     true,
		},
		{
			name: "Mig with other taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: "non-crd-taint1", Value: "value"},
					{Key: "non-crd-taint2", Value: "value"},
				}}).
				SetGkeManager(gkeManager).
				Build(),
			node:        &apiv1.Node{},
			nodeErr:     errors.New("nodeGroup node template error"),
			wantCrdName: "",
			wantErr:     true,
		},
		{
			name: "Mig with crd taint value set to empty string",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: testCrdLabel, Value: ""},
				}}).
				Build(),
			node:        &apiv1.Node{},
			wantCrdName: "",
			wantErr:     false,
		},
		{
			name: "Mig with crd taint value set to non-empty string",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: testCrdLabel, Value: "test-crd"},
				}}).
				Build(),
			node:        &apiv1.Node{},
			wantCrdName: "test-crd",
			wantErr:     false,
		},
		{
			name: "Mig without crd taint and empty node",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			node:    &apiv1.Node{},
			wantErr: true,
		},
		{
			name: "Mig without crd taint and node with other taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			node: buildNodeWithTaints([]apiv1.Taint{
				{Key: "non-crd-taint", Value: "value"},
			}),
			wantErr: true,
		},
		{
			name: "Mig without crd taint but node contains crd taint",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			node: buildNodeWithTaints([]apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
				{Key: "non-crd-taint", Value: "value"},
			}),
			wantErr:     false,
			wantCrdName: "test-crd",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManager.On("GetMigTemplateNodeInfo", tc.nodeGroup).Return(framework.NewTestNodeInfo(tc.node), tc.nodeErr).Once()
			crdName, err := NodeGroupCrdTaint(tc.nodeGroup, testCrdLabel)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tc.wantCrdName, crdName)
				assert.NoError(t, err)
			}
		})
	}
}

func TestNodeGroupCrdTaints(t *testing.T) {
	testCrdLabel := "test.io/crd"

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name       string
		nodeGroup  cloudprovider.NodeGroup
		wantTaints []apiv1.Taint
		wantErr    bool
		node       apiv1.Node
		nodeErr    error
	}{
		{
			name:      "Nil nodeGroup",
			nodeGroup: nil,
			wantErr:   true,
		},
		{
			name: "Mig without crd taint",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			wantErr: true,
			nodeErr: errors.New("nodeGroup node template error"),
		},
		{
			name: "Mig with other taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: "non-crd-taint1", Value: "value"},
					{Key: "non-crd-taint2", Value: "value"},
				}}).
				SetGkeManager(gkeManager).
				Build(),
			wantErr: true,
			nodeErr: errors.New("nodeGroup node template error"),
		},
		{
			name: "Mig with crd taint value set to empty string",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: testCrdLabel, Value: ""},
				}}).
				Build(),
			wantTaints: []apiv1.Taint{
				{Key: testCrdLabel, Value: ""},
			},
			wantErr: false,
		},
		{
			name: "Mig with crd taint value set to non-empty string",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: testCrdLabel, Value: "test-crd"},
				}}).
				Build(),
			wantErr: false,
			wantTaints: []apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
			},
		},
		{
			name: "Mig with multiple crd taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: testCrdLabel, Value: "test-crd"},
					{Key: testCrdLabel, Value: "test-crd1"},
					{Key: testCrdLabel, Value: "test-crd2"},
				}}).
				Build(),
			wantErr: false,
			wantTaints: []apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
				{Key: testCrdLabel, Value: "test-crd1"},
				{Key: testCrdLabel, Value: "test-crd2"},
			},
		},
		{
			name: "Mig with multiple similar crd taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{Taints: []apiv1.Taint{
					{Key: testCrdLabel, Value: "test-crd"},
					{Key: testCrdLabel, Value: "test-crd"},
					{Key: testCrdLabel, Value: "test-crd"},
				}}).
				Build(),
			wantErr: false,
			wantTaints: []apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
				{Key: testCrdLabel, Value: "test-crd"},
				{Key: testCrdLabel, Value: "test-crd"},
			},
		},
		{
			name: "Mig without crd taint and empty node",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			wantErr: true,
			node:    apiv1.Node{},
		},
		{
			name: "Mig without crd taint and node with other taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			wantErr: true,
			node: *buildNodeWithTaints([]apiv1.Taint{
				{Key: "non-crd-taint", Value: "value"},
			}),
		},
		{
			name: "Mig without crd taint but node contains crd taint",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			wantErr: false,
			node: *buildNodeWithTaints([]apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
				{Key: "non-crd-taint", Value: "value"},
			}),
			wantTaints: []apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
			},
		},
		{
			name: "Mig without crd taint but node contains multiple crd taint",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{}).
				SetGkeManager(gkeManager).
				Build(),
			wantErr: false,
			node: *buildNodeWithTaints([]apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
				{Key: testCrdLabel, Value: "test-crd2"},
				{Key: "non-crd-taint", Value: "value"},
			}),
			wantTaints: []apiv1.Taint{
				{Key: testCrdLabel, Value: "test-crd"},
				{Key: testCrdLabel, Value: "test-crd2"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManager.On("GetMigTemplateNodeInfo", tc.nodeGroup).Return(framework.NewTestNodeInfo(&tc.node), tc.nodeErr).Once()
			taints, err := NodeGroupCrdTaints(tc.nodeGroup, testCrdLabel)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.ElementsMatch(t, tc.wantTaints, taints)
				assert.NoError(t, err)
			}
		})
	}
}

func buildNodeWithLabels(labels map[string]string) *apiv1.Node {
	node := apiv1.Node{}
	node.Labels = labels
	return &node
}

func buildNodeWithTaints(taints []apiv1.Taint) *apiv1.Node {
	node := apiv1.Node{}
	node.Spec.Taints = taints
	return &node
}
