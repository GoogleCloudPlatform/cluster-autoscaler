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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func intPtr(v int) *int { return &v }

func buildNodeWithLabel(name, ccc string) *apiv1.Node {
	n := test.BuildTestNode(name, 1000, 1000)
	if n.Labels == nil {
		n.Labels = make(map[string]string)
	}
	n.Labels["kubernetes.io/hostname"] = name
	if ccc != "" {
		n.Labels["cloud.google.com/compute-class"] = ccc
	}
	return n
}

// Tests the processor successfully calculates shortfall and routes through simulator logic
func TestMinCapacityPodListProcessor_Process(t *testing.T) {
	testCases := []struct {
		name             string
		crds             []crd.CRD
		existingNodes    []*apiv1.Node
		expectedFakePods int
	}{
		{
			name: "top-level CCC, zero capacity triggers full capacity injection",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			existingNodes:    []*apiv1.Node{},
			expectedFakePods: 5,
		},
		{
			name: "top-level CCC, partial capacity partially triggers injection",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithLabel("n1", "my-ccc"),
				buildNodeWithLabel("n2", "my-ccc"),
			},
			expectedFakePods: 3, // 5 target - 2 existing
		},
		{
			name: "top-level CCC, full capacity skips injection",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithTargetNodeCount(intPtr(3)),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithLabel("n1", "my-ccc"),
				buildNodeWithLabel("n2", "my-ccc"),
				buildNodeWithLabel("n3", "my-ccc"),
			},
			expectedFakePods: 0,
		},
		{
			name: "priority rule injects specific fake pods, but existing node lacks priority label",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("priority-ccc"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(4))),
					}),
				),
			},
			existingNodes: []*apiv1.Node{
				buildNodeWithLabel("n1", "priority-ccc"),
			},
			expectedFakePods: 4, // 4 target - 0 match
		},
		{
			name: "priority rule injects specific fake pods and finds a matching priority node",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("priority-ccc"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(4))),
					}),
				),
			},
			existingNodes: []*apiv1.Node{
				func() *apiv1.Node {
					n := buildNodeWithLabel("n1", "priority-ccc")
					n.Labels[labels.ComputeClassPriorityIdxLabel] = "0"
					return n
				}(),
			},
			expectedFakePods: 3, // 4 target - 1 match
		},
		{
			name: "top-level CCC, nodes are full, should NOT inject more than target",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			existingNodes: []*apiv1.Node{
				func() *apiv1.Node {
					n := buildNodeWithLabel("n1", "my-ccc")
					n.Status.Allocatable = apiv1.ResourceList{
						apiv1.ResourcePods: *resource.NewQuantity(0, resource.DecimalSI),
					}
					return n
				}(),
				func() *apiv1.Node {
					n := buildNodeWithLabel("n2", "my-ccc")
					n.Status.Allocatable = apiv1.ResourceList{
						apiv1.ResourcePods: *resource.NewQuantity(0, resource.DecimalSI),
					}
					return n
				}(),
			},
			expectedFakePods: 3, // 5 target - 2 full = 3 fake pods. 0 schedule on the 2 full nodes. Result is 3 fake pods.
		},
		{
			name: "-1 priority label counts for top-level but ignores priority rules",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithTargetNodeCount(intPtr(5)),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(2))),
					}),
				),
			},
			existingNodes: []*apiv1.Node{
				func() *apiv1.Node {
					n := buildNodeWithLabel("n1", "my-ccc")
					n.Labels[labels.ComputeClassPriorityIdxLabel] = "-1"
					return n
				}(),
			},
			expectedFakePods: 4, // 2 priority pods (target 2 - 0 match) + 2 top-level pods (target 5 - 2 priority pods - 1 existing matcher CCC)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ccLister := lister.NewMockCrdLister(tc.crds)
			processor := NewMinCapacityPodListProcessor(ccLister, nil)

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, n := range tc.existingNodes {
				err := snapshot.AddNodeInfo(framework.NewNodeInfo(n, nil))
				assert.NoError(t, err)
			}

			ctx := &ca_context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}

			trulyUnschedulable, err := processor.Process(ctx, nil)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedFakePods, len(trulyUnschedulable))

			for _, p := range trulyUnschedulable {
				assert.Contains(t, p.Annotations, MinCapacityFakePodAnnotation)
			}
		})
	}
}
