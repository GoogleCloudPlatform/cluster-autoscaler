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

package computeclass

import (
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/utils/ptr"
)

func TestMatchesCrdLabel(t *testing.T) {
	testCrdLabel := "test.io/crd"

	testCases := []struct {
		name      string
		nodeGroup cloudprovider.NodeGroup
		crd       crd.CRD
		wantMatch bool
	}{
		{
			name: "node group matching crd without rules",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "node group matching crd with matching rules",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				SetNodePoolName("np-1").
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"np-1"})),
				}),
			),
			wantMatch: true,
		},
		{
			name: "node group matching crd without matching rules",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				SetNodePoolName("np-1").
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"other-np"})),
				}),
			),
			wantMatch: true,
		},
		{
			name: "node group not matching crd without rules",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("other-crd"),
			),
			wantMatch: false,
		},
		{
			name: "node group not matching crd with matching rules",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				SetNodePoolName("np-1").
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("other-crd"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"np-1"})),
				}),
			),
			wantMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			mockProvider := NewMockGKEProvider(nil, machinetypes.E2)
			matcherObj := NewMatcher(mockCrdLister, mockProvider)
			assert.Equal(t, tc.wantMatch, matcherObj.MatchesCrdLabel(tc.nodeGroup, tc.crd))
		})
	}
}

func TestMatchesCrdConfig(t *testing.T) {
	testCrdLabel := "test.io/crd"

	testCases := []struct {
		name             string
		nodeGroup        cloudprovider.NodeGroup
		crd              crd.CRD
		autopilotEnabled bool
		wantMatch        bool
	}{
		{
			name: "autopilot enabled, autopilot managed CRD, node group without managed nodes",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithAutopilotManaged(),
			),
			autopilotEnabled: true,
			wantMatch:        true,
		},
		{
			name: "autopilot disabled, autopilot managed CRD, node group without managed nodes",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithAutopilotManaged(),
			),
			wantMatch: false,
		},
		{
			name: "autopilot disabled, autopilot managed CRD, node group with managed nodes",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:               "crd-1",
						gkelabels.ManagedNodeLabel: "true",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithAutopilotManaged(),
			),
			wantMatch: true,
		},
		{
			name: "autopilot disabled, standard CRD, node group without managed nodes",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "autopilot disabled, standard CRD, node group with managed nodes",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:               "crd-1",
						gkelabels.ManagedNodeLabel: "true",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: false,
		},
		{
			name: "Node group with Service Account, CRD without",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:         map[string]string{testCrdLabel: "crd-1"},
					ServiceAccount: "test@1234.iam.gserviceaccount.com",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "Node group with Service Account, CRD with Service Account",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:         map[string]string{testCrdLabel: "crd-1"},
					ServiceAccount: "test@1234.iam.gserviceaccount.com",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithServiceAccount("test@1234.iam.gserviceaccount.com"),
			),
			wantMatch: true,
		},
		{
			name: "Node group without Service Account, CRD with Service Account",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{testCrdLabel: "crd-1"},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithServiceAccount("test@1234.iam.gserviceaccount.com"),
			),
			wantMatch: false,
		},
		{
			name: "Node group with Service Account, CRD with another Service Account",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:         map[string]string{testCrdLabel: "crd-1"},
					ServiceAccount: "test@1234.iam.gserviceaccount.com",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithServiceAccount("2test@1234.iam.gserviceaccount.com"),
			),
			wantMatch: false,
		},
		{
			name: "CRD Image Type matches node group",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:    map[string]string{testCrdLabel: "crd-1"},
					ImageType: "ubuntu_containerd",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithImageType("ubuntu_containerd"),
			),
			wantMatch: true,
		},
		{
			name: "CRD Image Type matches node group in GKE API capitalized format",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:    map[string]string{testCrdLabel: "crd-1"},
					ImageType: "UBUNTU_CONTAINERD",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithImageType("ubuntu_containerd"),
			),
			wantMatch: true,
		},
		{
			name: "CRD Image Type does not match node group",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:    map[string]string{testCrdLabel: "crd-1"},
					ImageType: "cos_containerd",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithImageType("ubuntu_containerd"),
			),
			wantMatch: false,
		},
		{
			name: "CRD Image Type not specified matches cos",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:    map[string]string{testCrdLabel: "crd-1"},
					ImageType: "cos_containerd",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "CRD Image Type not specified matches ubuntu",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:    map[string]string{testCrdLabel: "crd-1"},
					ImageType: "ubuntu_containerd",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "CRD self service labels match node group labels",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:      "crd-1",
						"feature-label-1": "feature-value-1",
						"feature-label-2": "feature-value-2",
					},
					SelfServiceMetadata: map[string]string{
						"feature-label-1": "feature-value-1",
						"feature-label-2": "feature-value-2",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithSelfServiceMetadata(map[string]string{
					"feature-label-1": "feature-value-1",
					"feature-label-2": "feature-value-2",
				}),
			),
			wantMatch: true,
		},
		{
			name: "CRD self service labels do not match node group labels",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:      "crd-1",
						"feature-label-1": "feature-value-1",
					},
					SelfServiceMetadata: map[string]string{
						"feature-label-1": "feature-value-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithSelfServiceMetadata(map[string]string{
					"feature-label-1": "feature-value-1",
					"feature-label-2": "feature-value-2",
				}),
			),
			wantMatch: false,
		},
		{
			name: "CRD self service metadata overridden by priority rule and matches node group",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
					SelfServiceMetadata: map[string]string{
						"base-key":     "base-value",
						"override-key": "priority-value",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithSelfServiceMetadata(map[string]string{
					"base-key":     "base-value",
					"override-key": "nodepool-value",
				}),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithSelfServiceRule(map[string]string{
						"override-key": "priority-value",
					})),
				}),
			),
			wantMatch: true,
		},
		{
			name: "CRD self service metadata overridden by priority rule but node group doesn't match",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
					SelfServiceMetadata: map[string]string{
						"base-key":     "base-value",
						"override-key": "wrong-value",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithSelfServiceMetadata(map[string]string{
					"base-key":     "base-value",
					"override-key": "nodepool-value",
				}),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithSelfServiceRule(map[string]string{
						"override-key": "priority-value",
					})),
				}),
			),
			wantMatch: false,
		},
		{
			name: "CRD self service has no labels while node group has some",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:      "crd-1",
						"feature-label-1": "feature-value-1",
					},
					SelfServiceMetadata: map[string]string{
						"feature-label-1": "feature-value-1",
						"feature-label-2": "feature-value-2",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "CRD self service labels is a subset of node group labels",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:      "crd-1",
						"feature-label-1": "feature-value-1",
					},
					SelfServiceMetadata: map[string]string{
						"feature-label-1": "feature-value-1",
						"feature-label-2": "feature-value-2",
						"feature-label-3": "feature-value-3",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithSelfServiceMetadata(map[string]string{
					"feature-label-1": "feature-value-1",
					"feature-label-3": "feature-value-3",
				}),
			),
			wantMatch: true,
		},
		{
			name: "CRD user defined labels does match node group labels",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:   "crd-1",
						"user-label-1": "user-value-1",
						"user-label-2": "user-value-2",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedLabels(map[string]string{
					"user-label-1": "user-value-1",
					"user-label-2": "user-value-2",
				}),
			),
			wantMatch: true,
		},
		{
			name: "CRD user defined labels do not match node group labels by keys",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:   "crd-1",
						"user-label-1": "user-value-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedLabels(map[string]string{
					"user-label-1": "user-value-1",
					"user-label-2": "user-value-2",
				}),
			),
			wantMatch: false,
		},
		{
			name: "CRD user defined labels do not match node group labels by keys values",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:   "crd-1",
						"user-label-1": "user-value-1",
						"user-label-2": "user-value-2",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedLabels(map[string]string{
					"user-label-1": "user-value-1",
					"user-label-2": "user-value-different",
				}),
			),
			wantMatch: false,
		},
		{
			name: "CRD user-defined labels are a subset of node group labels",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:    "crd-1",
						"user-label-1":  "user-value-1",
						"user-label-2":  "user-value-2",
						"extra-label-1": "extra-value-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedLabels(map[string]string{
					"user-label-1": "user-value-1",
					"user-label-2": "user-value-2",
				}),
			),
			wantMatch: true,
		},
		{
			name: "CRD user defined taints do match node group taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
					},
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedTaints([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectNoExecute,
					},
				}),
			),
			wantMatch: true,
		},
		{
			name: "CRD user defined taints do not match node group taints by keys",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
					},
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedTaints([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectNoExecute,
					},
				}),
			),
			wantMatch: false,
		},
		{
			name: "CRD user defined taints do not match node group taints by values",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
					},
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedTaints([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-different",
						Effect: apiv1.TaintEffectNoExecute,
					},
				}),
			),
			wantMatch: false,
		},
		{
			name: "CRD user defined taints do not match node group taints by effects",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
					},
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedTaints([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectPreferNoSchedule,
					},
				}),
			),
			wantMatch: false,
		},
		{
			name: "Node group with Node Version, CRD without",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-1"},
					NodeVersion: "1.32.9-gke.1726000",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "Node group with Node Version, CRD with Node Version",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-1"},
					NodeVersion: "1.32.9-gke.1726000",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithNodeVersion("1.32.9-gke.1726000"),
			),
			wantMatch: true,
		},
		{
			name: "Node group without Node Version, CRD with Node Version",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{testCrdLabel: "crd-1"},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithNodeVersion("1.32.9-gke.1726000"),
			),
			wantMatch: false,
		},
		{
			name: "Node group with Node Version, CRD with another Node Version",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-1"},
					NodeVersion: "1.32.9-gke.1726000",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithNodeVersion("1.31.9-gke.1726000"),
			),
			wantMatch: false,
		},
		{
			name: "CRD user-defined taints are a subset of node group taints",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
						{
							Key:    "extra-label-1",
							Value:  "extra-value-1",
							Effect: apiv1.TaintEffectPreferNoSchedule,
						},
					},
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithUserDefinedTaints([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectNoExecute,
					},
				}),
			),
			wantMatch: true,
		},
		{
			name: "CRD with DRA TPU driver mode matches node group with DRA label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:              "crd-1",
						gkelabels.DraTpuNodeLabel: "true",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithTpuDriverMode(crd.TpuDriverModeDynamicResourceAllocation),
			),
			wantMatch: true,
		},
		{
			name: "CRD with DRA TPU driver mode does not match node group without DRA label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithTpuDriverMode(crd.TpuDriverModeDynamicResourceAllocation),
			),
			wantMatch: false,
		},
		{
			name: "CRD with DevicePlugin TPU driver mode matches node group without DRA label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel: "crd-1",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithTpuDriverMode(crd.TpuDriverModeDevicePlugin),
			),
			wantMatch: true,
		},
		{
			name: "CRD with DevicePlugin TPU driver mode does not match node group with DRA label",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{
						testCrdLabel:              "crd-1",
						gkelabels.DraTpuNodeLabel: "true",
					},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithTpuDriverMode(crd.TpuDriverModeDevicePlugin),
			),
			wantMatch: false,
		},
		{
			name: "Node group with ArchitectureTaintBehavior, CRD without",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:            map[string]string{testCrdLabel: "crd-1"},
					ArchTaintBehavior: "NONE",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
			),
			wantMatch: true,
		},
		{
			name: "CRD with default ArchitectureTaintBehavior (ARM), Node group without",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{testCrdLabel: "crd-1"},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithArchitectureTaintBehavior("ARM"),
			),
			wantMatch: true,
		},
		{
			name: "CRD with non-default ArchitectureTaintBehavior (NONE), Node group without",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{testCrdLabel: "crd-1"},
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithArchitectureTaintBehavior("NONE"),
			),
			wantMatch: false,
		},
		{
			name: "Node group and CRD with same ArchitectureTaintBehavior",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:            map[string]string{testCrdLabel: "crd-1"},
					ArchTaintBehavior: "NONE",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithArchitectureTaintBehavior("NONE"),
			),
			wantMatch: true,
		},
		{
			name: "Node group and CRD with different ArchitectureTaintBehavior",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:            map[string]string{testCrdLabel: "crd-1"},
					ArchTaintBehavior: "NONE",
				}).
				Build(),
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-1"),
				crd.WithArchitectureTaintBehavior("ARM"),
			),
			wantMatch: false,
		}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			mockProvider := NewMockGKEProvider(nil, machinetypes.E2)
			if tc.autopilotEnabled {
				mockProvider.SetAutopilotEnabled()
			}
			matcherObj := NewMatcher(mockCrdLister, mockProvider)
			assert.Equal(t, tc.wantMatch, matcherObj.MatchesCrdConfig(tc.nodeGroup, tc.crd))
		})
	}
}

func TestFirstMatchingRule(t *testing.T) {
	testCrdLabel := "test.io/crd"
	testCrdName := "crd-1"
	testSA := "test@1234.iam.gserviceaccount.com"

	testMig := gke.NewTestGkeMigBuilder().
		SetNodePoolName("cool-pool").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:         map[string]string{testCrdLabel: testCrdName},
			ServiceAccount: testSA,
		}).
		Build()

	testCases := []struct {
		name      string
		crd       crd.CRD
		mig       *gke.GkeMig
		wantMatch bool
		wantRule  rules.Rule
		wantIndex int
	}{
		{
			name: "crd with no rules",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount(testSA),
			),
			wantMatch: false,
		},
		{
			name: "Single rule matching",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount(testSA),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
				}),
			),
			wantMatch: true,
			wantIndex: 0,
			wantRule:  rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
		},
		{
			name: "Single rule matching, does not match CRD (should not happen)",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("other-name"),
				crd.WithServiceAccount(testSA),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
				}),
			),
			wantMatch: false,
		},
		{
			name: "Single rule matching, does not match CRD config",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount("other-service-account"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
				}),
			),
			wantMatch: false,
		},
		{
			name: "Single rule non-matching, DoNotScaleUp",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount(testSA),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"default-pool", "boring"})),
				}),
			),
			wantMatch: false,
		},
		{
			name: "Single rule non-matching, ScaleUpAnyway",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount(testSA),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"default-pool", "boring"})),
				}),
				crd.WithScaleUpAnyway(),
			),
			wantMatch: false,
		},
		{
			name: "Non-matching and matching rules",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount(testSA),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"default-pool", "boring"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
				}),
			),
			wantMatch: true,
			wantIndex: 1,
			wantRule:  rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
		},
		{
			name: "Multiple matching rules",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount(testSA),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool", "also-cool"})),
				}),
			),
			wantMatch: true,
			wantIndex: 0,
			wantRule:  rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
		},
		{
			name: "Multiple rules, matching and not",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithServiceAccount(testSA),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"default-pool", "boring"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"default-pool", "boring"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"default-pool", "boring"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool", "also-cool"})),
				}),
			),
			wantMatch: true,
			wantIndex: 2,
			wantRule:  rules.NewRule(rules.WithNodePoolsRule([]string{"cool-pool"})),
		},
		{
			name: "GPU defaulting, matching machine type",
			crd: ccc.NewCccCrd(&v1.ComputeClass{ // use real CCC, so real Rules implementation is used
				ObjectMeta: metav1.ObjectMeta{
					Name: testCrdName,
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: ptr.To("g2-standard-12"),
						},
					},
				},
			}, "test-project", false, crd.TestDefaultDataProvider(), nil),
			mig: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "g2-standard-12",
					Accelerators: []*container.AcceleratorConfig{
						{
							AcceleratorType:  gkelabels.NvidiaL4,
							AcceleratorCount: 1,
						},
					},
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: testCrdName,
					},
				}).
				Build(),
			wantMatch: true,
			wantIndex: 0,
			wantRule: rules.NewRule(
				rules.WithMachineTypeRule(ptr.To("g2-standard-12")),
				rules.WithGpuRule(&machinetypes.GpuRequest{
					Count:            1,
					PhysicalGPUCount: 1,
					Config: machinetypes.GpuConfig{
						GpuType: gkelabels.NvidiaL4,
					},
				})),
		},
		{
			name: "GPU defaulting, not matching GPU request",
			crd: ccc.NewCccCrd(&v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: testCrdName,
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: ptr.To("g2-standard-12"),
						},
					},
				},
			}, "test-project", false, crd.TestDefaultDataProvider(), nil),
			mig: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "g2-standard-12",
					Accelerators: []*container.AcceleratorConfig{
						{
							AcceleratorType:  gkelabels.NvidiaB200,
							AcceleratorCount: 2,
						},
					},
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: testCrdName,
					},
				}).
				Build(),
			wantMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(tc.crd.Label())
			mockProvider := NewMockGKEProvider(nil, machinetypes.E2)
			matcherObj := NewMatcher(mockCrdLister, mockProvider)
			mig := testMig
			if tc.mig != nil {
				mig = tc.mig
			}
			match, idx, rule := matcherObj.FirstMatchedRule(mig, tc.crd)

			assert.Equal(t, tc.wantMatch, match)
			assert.Equal(t, tc.wantIndex, idx)
			assert.Equal(t, tc.wantRule, rule)
		})
	}
}

func TestFirstMatchedRuleGroup(t *testing.T) {
	testCrdLabel := "test.io/crd"
	testCrdName := "crd-1"
	testSA := "test@1234.iam.gserviceaccount.com"

	testMig := gke.NewTestGkeMigBuilder().
		SetNodePoolName("cool-pool").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:         map[string]string{testCrdLabel: testCrdName},
			ServiceAccount: testSA,
		}).
		Build()

	testCases := []struct {
		name           string
		crd            crd.CRD
		mig            *gke.GkeMig
		wantMatch      bool
		wantRule       rules.Rule
		wantGroupIndex int
	}{
		{
			name: "CRD with no priorities",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
			),
			mig: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "g2-standard-12",
					Labels: map[string]string{
						testCrdLabel: testCrdName,
					},
				}).
				Build(),
			wantMatch: false,
		},
		{
			name: "CRD with priorityScore - no match",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithGroupedRules([][]rules.Rule{
					{
						rules.NewRule(rules.WithMachineFamilyRule(ptr.To("n1"))),
						rules.NewRule(rules.WithMachineFamilyRule(ptr.To("n2"))),
					}, // PriorityScore 10
					{
						rules.NewRule(rules.WithMachineFamilyRule(ptr.To("e2"))),
						rules.NewRule(rules.WithMachineFamilyRule(ptr.To("t2a"))),
					}, // PriorityScore 5
				}),
			),
			mig: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "g2-standard-12",
					Labels: map[string]string{
						testCrdLabel: testCrdName,
					},
				}).
				Build(),
			wantMatch: false,
		},
		{
			name: "CRD with priorityScore - match",
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName(testCrdName),
				crd.WithGroupedRules([][]rules.Rule{
					{
						rules.NewRule(rules.WithMachineFamilyRule(ptr.To("a2"))),
						rules.NewRule(rules.WithMachineFamilyRule(ptr.To("g2"))),
					},
					{
						rules.NewRule(rules.WithMachineTypeRule(ptr.To("g2-standard-12"))),
					},
				}),
			),
			mig: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "g2-standard-12", // matches priorityScore 15 (higher)
					Labels: map[string]string{
						testCrdLabel: testCrdName,
					},
				}).
				Build(),
			wantMatch:      true,
			wantRule:       rules.NewRule(rules.WithMachineFamilyRule(ptr.To("g2"))),
			wantGroupIndex: 0, // groups are sorted according to priorityScore.
		},
		{
			name: "CCC with priorityScore - multiple priorities with same priorityScore",
			crd: ccc.NewCccCrd(&v1.ComputeClass{ // use real CCC, so real GroupedRules implementation is used
				ObjectMeta: metav1.ObjectMeta{
					Name: testCrdName,
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: ptr.To("t2a"),
							PriorityScore: ptr.To(5),
						},
						{
							MachineFamily: ptr.To("e2"),
							PriorityScore: ptr.To(10),
						},
						{
							MachineFamily: ptr.To("n2"),
							PriorityScore: ptr.To(10),
						},
					},
				},
			}, "test-project", false, crd.TestDefaultDataProvider(), nil),
			mig: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n2-standard-4",
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: testCrdName,
					},
				}).
				Build(),
			wantMatch:      true,
			wantRule:       rules.NewRule(rules.WithMachineFamilyRule(ptr.To("n2"))),
			wantGroupIndex: 0, // PriorityScore 10 is the highest, so it should be index 0.
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(tc.crd.Label())
			mockProvider := NewMockGKEProvider(nil, machinetypes.E2)
			matcherObj := NewMatcher(mockCrdLister, mockProvider)
			mig := testMig
			if tc.mig != nil {
				mig = tc.mig
			}
			match, idx, rule := matcherObj.FirstMatchedRuleGroup(mig, tc.crd)

			assert.Equal(t, tc.wantMatch, match)
			assert.Equal(t, tc.wantGroupIndex, idx)
			assert.Equal(t, tc.wantRule, rule)
		})
	}
}

type machineConfigProvider struct{}

func (p *machineConfigProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}
