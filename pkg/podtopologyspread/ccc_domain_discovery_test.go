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
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	ccc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestCCCDomainDiscovery(t *testing.T) {
	domainKey := "compute-class-domain"
	domains := []string{"domain-A", "domain-B", "domain-C"}
	ccc := crd.NewTestCrd(
		crd.WithName("test-ccc"),
		crd.WithLabel(gkelabels.ComputeClassLabel),
		crd.WithRules(
			[]rules.Rule{
				rules.NewRule(rules.WithLabelsRule(map[string]string{
					domainKey: domains[0],
				})),
				rules.NewRule(rules.WithLabelsRule(map[string]string{
					domainKey: domains[1],
				})),
				rules.NewRule(rules.WithLabelsRule(map[string]string{
					domainKey: domains[2],
				})),
			},
		),
	)

	testCases := []struct {
		description           string
		experimentDisabled    bool
		pods                  []*apiv1.Pod
		crds                  []crd.CRD
		expectedEligibilities []ptsEligibilityForTest
	}{
		{
			description: "Non-PTS Pod",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1),
			},
			crds:                  []crd.CRD{ccc},
			expectedEligibilities: nil,
		},
		{
			description:        "PTS Pod using a matching CCC, experiment disabled",
			experimentDisabled: true,
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds: []crd.CRD{ccc},
		},
		{
			description: "PTS Pod using a matching CCC",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds: []crd.CRD{ccc},
			expectedEligibilities: []ptsEligibilityForTest{
				{
					domainNames:         domains,
					domainDiscoveryName: cccDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "new-pod",
				},
			},
		},
		{
			description: "Many PTS Pods using a matching CCC",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1),
				test.BuildTestPod("new-pod-3", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds: []crd.CRD{ccc},
			expectedEligibilities: []ptsEligibilityForTest{
				{
					domainNames:         domains,
					domainDiscoveryName: cccDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "new-pod-1",
				},
				{
					domainNames:         domains,
					domainDiscoveryName: cccDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "new-pod-3",
				},
			},
		},
		{
			description: "PTS Pod - multiple topology spread constraints",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       "random-key",
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(2),
						MaxSkew:           1,
					}),
				),
			},
			crds: []crd.CRD{ccc},
			expectedEligibilities: []ptsEligibilityForTest{
				{
					domainNames:         domains,
					domainDiscoveryName: cccDomainDiscoveryName,
					constraintIdx:       proto.Int32(1),
					podName:             "new-pod",
				},
			},
		},
		{
			description: "PTS Pod - min domain is larger than available domains",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						MinDomains:        proto.Int32(32),
					}),
				),
			},
			crds:                  []crd.CRD{ccc},
			expectedEligibilities: nil,
		},
		{
			description: "PTS Pod - CCC doesn't have required nodeLabels in all rules",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					})),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("ccc"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithRules(
						[]rules.Rule{
							rules.NewRule(rules.WithLabelsRule(map[string]string{
								domainKey: "domain-A",
							})),
							rules.NewRule(rules.WithLabelsRule(map[string]string{
								domainKey: "domain-B",
							})),
							rules.NewRule(rules.WithLabelsRule(map[string]string{
								"invalid-key": "domain-C",
							})),
						},
					),
				),
			},
			expectedEligibilities: nil,
		},
		{
			description: "PTS pod - without CCC",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds:                  []crd.CRD{ccc},
			expectedEligibilities: nil,
		},
		{
			description: "PTS pod - CCC having no rules",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds: []crd.CRD{crd.NewTestCrd(
				crd.WithName("test-ccc"),
				crd.WithLabel(gkelabels.ComputeClassLabel),
			)},
			expectedEligibilities: nil,
		},
		{
			description: "PTS Pod - with unmatched CCC",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc-2"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds: []crd.CRD{
				ccc,
				crd.NewTestCrd(
					crd.WithName("test-ccc-2"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithRules(
						[]rules.Rule{
							rules.NewRule(rules.WithLabelsRule(map[string]string{
								"abc": "domain-A",
							})),
							rules.NewRule(rules.WithLabelsRule(map[string]string{
								"abc": "domain-B",
							})),
							rules.NewRule(rules.WithLabelsRule(map[string]string{
								"abc": "domain-C",
							})),
						},
					),
				),
			},
			expectedEligibilities: nil,
		},
		{
			description: "PTS pod - with incorrect domain name",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       "abc",
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds:                  []crd.CRD{ccc},
			expectedEligibilities: nil,
		},
		{
			description: "DoNotSchedule is prioritized over ScheduleAnyway",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withComputeClass("test-ccc"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
						MaxSkew:           1,
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			crds: []crd.CRD{ccc},
			expectedEligibilities: []ptsEligibilityForTest{
				{
					domainNames:         domains,
					domainDiscoveryName: cccDomainDiscoveryName,
					constraintIdx:       proto.Int32(1),
					podName:             "new-pod",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			lister := ccc_lister.NewMockCrdListerWithLabel(tc.crds, gkelabels.ComputeClassLabel)
			exps := []string{}
			if !tc.experimentDisabled {
				exps = []string{cccExperimentName}
			}
			dd := NewCCCDomainDiscovery(experiments.NewMockManager(exps...), lister)
			eligibility := dd.EligiblePTSPods(tc.pods)

			expectedEligibilities := ptsEligibilityFromTestOnes(t, tc.expectedEligibilities, tc.pods)
			assert.Equal(t, expectedEligibilities, eligibility)
		})
	}
}
