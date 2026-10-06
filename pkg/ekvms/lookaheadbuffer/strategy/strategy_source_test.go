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

package strategy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

var resizableFamilies = []struct {
	name           string
	experimentFlag string
}{
	{
		name:           machinetypes.EK.Name(),
		experimentFlag: experiments.EkLookaheadPodsV1Flag,
	},
	{
		name:           machinetypes.E4A.Name(),
		experimentFlag: experiments.E4aLookaheadPodsV1Flag,
	},
	{
		name:           machinetypes.E4.Name(),
		experimentFlag: experiments.E4LookaheadPodsV1Flag,
	},
}

func TestExperimentSourceRefresh(t *testing.T) {
	componentVersion := version.Version{31, 157, 3}
	enabledFlags := map[string]bool{}

	for _, family := range resizableFamilies {
		t.Run(family.name, func(t *testing.T) {
			testCases := []struct {
				desc string
				experiments.Manager
				want LookaheadPodStrategy
			}{
				{
					desc: "experiment strategy is unspecified when the flag is unset",
					Manager: experiments.NewMockManagerWithOptions(
						componentVersion,
						enabledFlags,
						map[string]string{},
					),
					want: LookaheadPodStrategy{Status: Unspecified},
				},
				{
					desc: "experiment strategy is unspecified when the flag is empty",
					Manager: experiments.NewMockManagerWithOptions(
						componentVersion,
						enabledFlags,
						map[string]string{family.experimentFlag: ""},
					),
					want: LookaheadPodStrategy{Status: Unspecified},
				},
				{
					desc: "experiment strategy is unspecified when the flag is invalid JSON",
					Manager: experiments.NewMockManagerWithOptions(
						componentVersion,
						enabledFlags,
						map[string]string{family.experimentFlag: "{"},
					),
					want: LookaheadPodStrategy{Status: Unspecified},
				},
				{
					desc: "experiment strategy is disabled",
					Manager: experiments.NewMockManagerWithOptions(
						componentVersion,
						enabledFlags,
						map[string]string{family.experimentFlag: `
					{
						"minCaVersion": "30.0.0",
						"status": "STATUS_DISABLED"
					}
					`},
					),
					want: LookaheadPodStrategy{
						MinCaVersion: "30.0.0",
						Status:       Disabled,
					},
				},
				{
					desc: "experiment strategy is unspecified when MinCaVersion is bigger than component version",
					Manager: experiments.NewMockManagerWithOptions(
						componentVersion,
						enabledFlags,
						map[string]string{family.experimentFlag: `
					{
						"minCaVersion": "999.999.999",
						"status": "STATUS_ENABLED",
						"tieredStrategy": {
							"tiers": [
								{
									"numLookaheadPods": 1,
									"lookaheadPodMilliCpu": 8000,
									"lookaheadPodMemKib": 134217728,
									"minTargetNodesCpu": 200
								}
							]
						}
					}
					`},
					),
					want: LookaheadPodStrategy{Status: Unspecified},
				},
				{
					desc: fmt.Sprintf("experiment strategy is set when %s flag is valid", family.experimentFlag),
					Manager: experiments.NewMockManagerWithOptions(
						componentVersion,
						enabledFlags,
						map[string]string{family.experimentFlag: `
					{
						"minCaVersion": "30.0.0",
						"status": "STATUS_ENABLED",
						"tieredStrategy": {
							"tiers": [
								{
									"numLookaheadPods": 1,
									"lookaheadPodMilliCpu": 8000,
									"lookaheadPodMemKib": 134217728,
									"minTargetNodesCpu": 200
								}
							]
						}
					}
					`},
					),
					want: LookaheadPodStrategy{
						MinCaVersion: "30.0.0",
						Status:       Enabled,
						TieredStrategy: &TieredStrategy{
							Tiers: []Tier{
								{
									NumLookaheadPods:     1,
									LookaheadPodMilliCPU: 8000,
									LookaheadPodMemKib:   134217728,
									MinTargetNodesCPU:    200,
								},
							},
						},
					},
				},
			}
			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					s := newExperimentStrategySource(tc.Manager, componentVersion, family.experimentFlag, family.name)
					assert.Equal(t, tc.want, s.cachedStrategy)
				})
			}
		})
	}
}
