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

package customresources

import (
	stdcontext "context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

func TestMultiNetworkingProcessor_FilterOutNodesWithUnreadyResources(t *testing.T) {
	quantity1 := resource.NewQuantity(1, resource.DecimalSI)
	quantity2 := resource.NewQuantity(2, resource.DecimalSI)

	multiNetSpec := &gkeclient.NodePoolSpec{
		NetworkConfigs: []gkeclient.AdditionalNetworkConfig{
			{NetworkAttachment: "attachment1"},
		},
	}
	standardSpec := &gkeclient.NodePoolSpec{}

	testCases := []struct {
		name              string
		migSpec           *gkeclient.NodePoolSpec
		templateNode      *v1.Node
		templateErr       error
		providerErr       error
		unmanagedNode     bool
		nodeAllocatable   v1.ResourceList
		expectReady       bool
		expectUnreadyCopy bool
	}{
		{
			name:    "Standard L3 node with .IP populated -> ready",
			migSpec: multiNetSpec,
			templateNode: &v1.Node{
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						"networking.gke.io.networks/net.IP": *quantity1,
					},
				},
			},
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU:                      *quantity2,
				"networking.gke.io.networks/net.IP": *quantity1,
			},
			expectReady: true,
		},
		{
			name:    "Standard L3 node missing .IP (netd initializing) -> unready",
			migSpec: multiNetSpec,
			templateNode: &v1.Node{
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						"networking.gke.io.networks/net.IP": *quantity1,
					},
				},
			},
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU: *quantity2,
			},
			expectReady:       false,
			expectUnreadyCopy: true,
		},
		{
			name:    "High-performance NetDevice node with .IP and device populated -> ready",
			migSpec: multiNetSpec,
			templateNode: &v1.Node{
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						"networking.gke.io.networks/net.IP": *quantity1,
						"networking.gke.io.networks/net":    *quantity1,
					},
				},
			},
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU:                      *quantity2,
				"networking.gke.io.networks/net.IP": *quantity1,
				"networking.gke.io.networks/net":    *quantity1,
			},
			expectReady: true,
		},
		{
			name:    "High-performance NetDevice node missing device plugin resource -> unready",
			migSpec: multiNetSpec,
			templateNode: &v1.Node{
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						"networking.gke.io.networks/net.IP": *quantity1,
						"networking.gke.io.networks/net":    *quantity1,
					},
				},
			},
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU:                      *quantity2,
				"networking.gke.io.networks/net.IP": *quantity1,
			},
			expectReady:       false,
			expectUnreadyCopy: true,
		},
		{
			name:    "High-performance NetDevice node uninitialized -> unready",
			migSpec: multiNetSpec,
			templateNode: &v1.Node{
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						"networking.gke.io.networks/net.IP": *quantity1,
						"networking.gke.io.networks/net":    *quantity1,
					},
				},
			},
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU: *quantity2,
			},
			expectReady:       false,
			expectUnreadyCopy: true,
		},
		{
			name:    "Standard non-multi-networking node -> ready",
			migSpec: standardSpec,
			templateNode: &v1.Node{
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						v1.ResourceCPU: *quantity2,
					},
				},
			},
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU: *quantity2,
			},
			expectReady: true,
		},
		{
			name:          "Unmanaged node (no node group) -> ready",
			unmanagedNode: true,
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU: *quantity2,
			},
			expectReady: true,
		},
		{
			name:        "NodeGroupForNode error -> ready",
			providerErr: fmt.Errorf("provider error"),
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU: *quantity2,
			},
			expectReady: true,
		},
		{
			name:        "TemplateNodeInfo error -> ready",
			migSpec:     multiNetSpec,
			templateErr: fmt.Errorf("template error"),
			nodeAllocatable: v1.ResourceList{
				v1.ResourceCPU: *quantity2,
			},
			expectReady: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
				Status: v1.NodeStatus{
					Allocatable: tc.nodeAllocatable,
				},
			}

			cloudProviderMock := &gke.GkeCloudProviderMock{}
			if tc.providerErr != nil {
				cloudProviderMock.On("NodeGroupForNode", node).Return(nil, tc.providerErr)
			} else if tc.unmanagedNode {
				cloudProviderMock.On("NodeGroupForNode", node).Return(nil, nil)
			} else {
				gkeManagerMock := &gke.GkeManagerMock{}
				gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)

				mig := gke.NewTestGkeMigBuilder().
					SetGceRefName("test-pool").
					SetMaxSize(10).
					SetAutoprovisioned(true).
					SetExist(false).
					SetNodePoolName("test-pool").
					SetSpec(tc.migSpec).
					SetGkeManager(gkeManagerMock).
					Build()

				if tc.templateErr != nil {
					gkeManagerMock.On("GetMigTemplateNodeInfo", mig).Return((*framework.NodeInfo)(nil), tc.templateErr)
				} else if tc.templateNode != nil {
					gkeManagerMock.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(tc.templateNode), nil)
				}

				cloudProviderMock.On("NodeGroupForNode", node).Return(mig, nil)
			}

			ctx := &context.AutoscalingContext{CloudProvider: cloudProviderMock}
			processor := MultiNetworkingProcessor{}

			allNodes := []*v1.Node{node}
			readyNodes := []*v1.Node{node}

			gotAll, gotReady := processor.FilterOutNodesWithUnreadyResources(stdcontext.Background(), ctx, allNodes, readyNodes)

			if tc.expectReady {
				assert.Equal(t, []*v1.Node{node}, gotReady)
				assert.Equal(t, []*v1.Node{node}, gotAll)
			} else {
				assert.Empty(t, gotReady)
				assert.Equal(t, 1, len(gotAll))
				assert.Equal(t, "test-node", gotAll[0].Name)

				readiness, _ := kubernetes.GetNodeReadiness(gotAll[0])
				assert.False(t, readiness.Ready)
				assert.Equal(t, kubernetes.ResourceUnready, readiness.Reason)
			}
		})
	}
}
