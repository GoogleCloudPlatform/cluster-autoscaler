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

package tracking

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestEkvmsIncrementStepFieldSetValue(t *testing.T) {
	coarseStep := apiv1.ResourceList{
		apiv1.ResourceCPU:    resource.MustParse("2"),
		apiv1.ResourceMemory: resource.MustParse("1Mi"),
	}
	oneCpuStep := apiv1.ResourceList{
		apiv1.ResourceCPU:    resource.MustParse("1"),
		apiv1.ResourceMemory: resource.MustParse("1Mi"),
	}
	halfCpuStep := apiv1.ResourceList{
		apiv1.ResourceCPU:    resource.MustParse("500m"),
		apiv1.ResourceMemory: resource.MustParse("1Mi"),
	}
	tenMilliCpuStep := apiv1.ResourceList{
		apiv1.ResourceCPU:    resource.MustParse("10m"),
		apiv1.ResourceMemory: resource.MustParse("1Mi"),
	}
	customFlagStep := apiv1.ResourceList{
		apiv1.ResourceCPU:    resource.MustParse("250m"),
		apiv1.ResourceMemory: resource.MustParse("1Mi"),
	}

	testCases := []struct {
		testName          string
		flagStep          apiv1.ResourceList
		boolExperiments   map[string]bool
		stringExperiments map[string]string
		wantStep          apiv1.ResourceList
	}{
		{
			testName: "Default (no experiments defined) stays OFF (coarse 2 CPU step)",
			flagStep: coarseStep,
			wantStep: coarseStep,
		},
		{
			testName: "Experiment enabled with step=1 (MinCAVersion satisfied) switches to 1 CPU step",
			flagStep: coarseStep,
			stringExperiments: map[string]string{
				experiments.EkFineGrainedResizeMinCAVersionFlag:  "1.37.0",
				experiments.EkFineGrainedResizeIncrementStepFlag: "1",
			},
			wantStep: oneCpuStep,
		},
		{
			testName: "Experiment enabled with step=500m (0.5 CPU) switches to 500m CPU step",
			flagStep: coarseStep,
			stringExperiments: map[string]string{
				experiments.EkFineGrainedResizeMinCAVersionFlag:  "1.37.0",
				experiments.EkFineGrainedResizeIncrementStepFlag: "500m",
			},
			wantStep: halfCpuStep,
		},
		{
			testName: "Experiment enabled with step=10m (0.01 CPU) switches to 10m CPU step",
			flagStep: coarseStep,
			stringExperiments: map[string]string{
				experiments.EkFineGrainedResizeMinCAVersionFlag:  "1.37.0",
				experiments.EkFineGrainedResizeIncrementStepFlag: "10m",
			},
			wantStep: tenMilliCpuStep,
		},
		{
			testName: "Experiment disabled via EkFineGrainedResizeEnabledFlag=false falls back to coarse 2 CPU step",
			flagStep: coarseStep,
			boolExperiments: map[string]bool{
				experiments.EkFineGrainedResizeEnabledFlag: false,
			},
			stringExperiments: map[string]string{
				experiments.EkFineGrainedResizeMinCAVersionFlag:  "1.37.0",
				experiments.EkFineGrainedResizeIncrementStepFlag: "1",
			},
			wantStep: coarseStep,
		},
		{
			testName: "MinCAVersion not satisfied falls back to coarse 2 CPU step",
			flagStep: coarseStep,
			stringExperiments: map[string]string{
				experiments.EkFineGrainedResizeMinCAVersionFlag:  "999.999.999",
				experiments.EkFineGrainedResizeIncrementStepFlag: "1",
			},
			wantStep: coarseStep,
		},
		{
			testName: "Invalid step quantity falls back to coarse 2 CPU step",
			flagStep: coarseStep,
			stringExperiments: map[string]string{
				experiments.EkFineGrainedResizeMinCAVersionFlag:  "1.37.0",
				experiments.EkFineGrainedResizeIncrementStepFlag: "invalid",
			},
			wantStep: coarseStep,
		},
		{
			testName: "Explicit non-default CLI flag overrides experiment",
			flagStep: customFlagStep,
			stringExperiments: map[string]string{
				experiments.EkFineGrainedResizeMinCAVersionFlag:  "1.37.0",
				experiments.EkFineGrainedResizeIncrementStepFlag: "1",
			},
			wantStep: customFlagStep,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					EkvmsIncrementStep: tc.flagStep,
				},
			}
			// Current CA version used in test
			caVer, _ := version.FromString("1.37.228")
			experimentsManager := experiments.NewMockManagerWithOptions(caVer, tc.boolExperiments, tc.stringExperiments)

			optsToModify := internalopts.AutoscalingOptions{}
			err := ekvmsIncrementStepField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantStep, optsToModify.EkvmsIncrementStep)
		})
	}
}
