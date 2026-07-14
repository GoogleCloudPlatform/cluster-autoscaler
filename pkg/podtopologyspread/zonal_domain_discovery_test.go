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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestNewZonalDomainDiscovery(t *testing.T) {
	zones := []string{"zone-A", "zone-B", "zone-C"}

	testCases := []struct {
		name               string
		experimentDisabled bool
		pods               []*apiv1.Pod
		wantConfig         []ptsEligibilityForTest
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
			name:               "pod with zonal PTS, experiment disabled",
			experimentDisabled: true,
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(3),
					}),
				),
			},
		},
		{
			name: "pod with zonal PTS",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(3),
					}),
				),
			},
			wantConfig: []ptsEligibilityForTest{
				{
					domainNames:         zones,
					domainDiscoveryName: zonalDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with non-zonal PTS",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       "other-key",
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(3),
					}),
				),
			},
		},
		{
			name: "pod with one zonal PTS and one non-zonal PTS - eligible",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       "other-key",
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(3),
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(3),
					}),
				),
			},
			wantConfig: []ptsEligibilityForTest{
				{
					domainNames:         zones,
					domainDiscoveryName: zonalDomainDiscoveryName,
					constraintIdx:       proto.Int32(1),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with ScheduleAnyway zonal PTS - eligible",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					}),
				),
			},
			wantConfig: []ptsEligibilityForTest{
				{
					domainNames:         zones,
					domainDiscoveryName: zonalDomainDiscoveryName,
					constraintIdx:       proto.Int32(0),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with ScheduleAnyway and DoNotSchedule zonal PTS - prioritize DoNotSchedule",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(3),
					}),
				),
			},
			wantConfig: []ptsEligibilityForTest{
				{
					domainNames:         zones,
					domainDiscoveryName: zonalDomainDiscoveryName,
					constraintIdx:       proto.Int32(1),
					podName:             "p1",
				},
			},
		},
		{
			name: "pod with DoNotSchedule zonal PTS with too high minDomains - not eligible",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(4),
					}),
				),
			},
		},
		{
			name: "pod with DoNotSchedule zonal PTS with too high minDomains and DoNotSchedule zonal PTS - prioritize DoNotSchedule, even if it is not eligible",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MinDomains:        proto.Int32(4),
					}),
				),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutoprovisioningLocations(zones...).Build()
			exps := []string{}
			if !tc.experimentDisabled {
				exps = []string{zonalExperimentName}
			}
			dd := NewZonalDomainDiscovery(experiments.NewMockManager(exps...), cp)

			configs := dd.EligiblePTSPods(tc.pods)
			wantConfig := ptsEligibilityFromTestOnes(t, tc.wantConfig, tc.pods)

			assert.Equal(t, wantConfig, configs)
		})
	}
}
