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

package preemption

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestTypeFromToleration(t *testing.T) {
	tcs := []struct {
		name           string
		toleration     apiv1.Toleration
		expectedResult VmPreemptionType
	}{
		{
			"preemptible key, exists",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: apiv1.TolerationOpExists,
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			LegacyPreemptible,
		},
		{
			"preemptible key, exists, empty effect",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: apiv1.TolerationOpExists,
				Effect:   "",
			},
			LegacyPreemptible,
		},
		{
			"preemptible key, exists, bad effect",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: apiv1.TolerationOpExists,
				Effect:   apiv1.TaintEffectNoExecute,
			},
			NoPreemption,
		},
		{
			"preemptible key, equal",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "true",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			LegacyPreemptible,
		},
		{
			"preemptible key, no operator",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: "",
				Value:    "true",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			LegacyPreemptible,
		},
		{
			"preemptible key, equal, bad value",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "blah",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			NoPreemption,
		},
		{
			"preemptible key, equal, bad effect",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "true",
				Effect:   apiv1.TaintEffectNoExecute,
			},
			NoPreemption,
		},
		{
			"preemptible key, equal, empty effect",
			apiv1.Toleration{
				Key:      labels.PreemptibleLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "true",
				Effect:   "",
			},
			LegacyPreemptible,
		},
		{
			"spot key, exists",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: apiv1.TolerationOpExists,
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			Spot,
		},
		{
			"spot key, exists, empty effect",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: apiv1.TolerationOpExists,
				Effect:   "",
			},
			Spot,
		},
		{
			"spot key, exists, bad effect",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: apiv1.TolerationOpExists,
				Effect:   apiv1.TaintEffectNoExecute,
			},
			NoPreemption,
		},
		{
			"spot key, equal",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "true",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			Spot,
		},
		{
			"spot key, no operator",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: "",
				Value:    "true",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			Spot,
		},
		{
			"spot key, equal, bad value",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "blah",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			NoPreemption,
		},
		{
			"spot key, equal, bad effect",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "true",
				Effect:   apiv1.TaintEffectNoExecute,
			},
			NoPreemption,
		},
		{
			"spot key, equal, empty effect",
			apiv1.Toleration{
				Key:      labels.SpotLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    "true",
				Effect:   "",
			},
			Spot,
		},
		{
			"bad key",
			apiv1.Toleration{
				Key:      "some.other.key",
				Operator: "Exists",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
			NoPreemption,
		},
	}

	for _, testCase := range tcs {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, TypeFromToleration(testCase.toleration))
		})
	}
}

func TestTypeFromLabels(t *testing.T) {
	tcs := []struct {
		name           string
		labels         map[string]string
		expectedResult VmPreemptionType
	}{
		{
			"no preemption labels",
			map[string]string{"abc": "xyz", "def": "ghi"},
			NoPreemption,
		},
		{
			"preemptible label present",
			map[string]string{"abc": "xyz", labels.PreemptibleLabel: "true"},
			LegacyPreemptible,
		},
		{
			"spot label present",
			map[string]string{"abc": "xyz", labels.SpotLabel: "true"},
			Spot,
		},
		{
			"both preemption labels present",
			map[string]string{"abc": "xyz", labels.SpotLabel: "true", labels.PreemptibleLabel: "true"},
			Spot,
		},
	}

	for _, testCase := range tcs {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, TypeFromLabels(testCase.labels))
		})
	}
}

func TestProvisioningLabelValue(t *testing.T) {
	tcs := []struct {
		name           string
		preemptionType VmPreemptionType
		expectedResult string
	}{
		{
			"No preemption provisioning value",
			NoPreemption,
			labels.StandardProvisioningValue,
		},
		{
			"Spot preemption provisioning value",
			Spot,
			labels.SpotProvisioningValue,
		},
		{
			"Preemptible preemption provisioning value",
			LegacyPreemptible,
			labels.PreemptibleProvisioningValue,
		},
	}
	for _, testCase := range tcs {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, testCase.preemptionType.ProvisioningLabelValue())
		})
	}
}

func TestToleratedVmPreemptionForPod(t *testing.T) {
	tcs := []struct {
		name           string
		pod            *apiv1.Pod
		expectedResult VmPreemptionType
	}{
		{
			"no tolerations at all",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: nil,
				},
			},
			NoPreemption,
		},
		{
			"no preemptible tolerations",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: []apiv1.Toleration{
						{
							Key:      "some.other.key",
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
						{
							Key:      "even.other.key",
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			NoPreemption,
		},
		{
			"has preemptible tolerations",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: []apiv1.Toleration{
						{
							Key:      labels.PreemptibleLabel,
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
						{
							Key:      "even.other.key",
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			LegacyPreemptible,
		},
		{
			"has spot tolerations",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: []apiv1.Toleration{
						{
							Key:      labels.SpotLabel,
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
						{
							Key:      "even.other.key",
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			Spot,
		},
		{
			"has both tolerations",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: []apiv1.Toleration{
						{
							Key:      labels.SpotLabel,
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
						{
							Key:      labels.PreemptibleLabel,
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
						{
							Key:      "even.other.key",
							Operator: "Exists",
							Effect:   apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			Spot,
		},
	}

	for _, testCase := range tcs {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, ToleratedVmPreemptionForPod(testCase.pod))
		})
	}
}

func TestToleratedVmPreemptionForAnyPod(t *testing.T) {
	noTolerationsPod := &apiv1.Pod{
		Spec: apiv1.PodSpec{
			Tolerations: nil,
		},
	}
	unrelatedTolerationsPod := &apiv1.Pod{
		Spec: apiv1.PodSpec{
			Tolerations: []apiv1.Toleration{
				{
					Key:      "some.other.key",
					Operator: "Exists",
					Effect:   apiv1.TaintEffectNoSchedule,
				},
			},
		},
	}
	preemptiblePod := &apiv1.Pod{
		Spec: apiv1.PodSpec{
			Tolerations: []apiv1.Toleration{
				{
					Key:      labels.PreemptibleLabel,
					Operator: "Exists",
					Effect:   apiv1.TaintEffectNoSchedule,
				},
			},
		},
	}
	spotPod := &apiv1.Pod{
		Spec: apiv1.PodSpec{
			Tolerations: []apiv1.Toleration{
				{
					Key:      labels.SpotLabel,
					Operator: "Exists",
					Effect:   apiv1.TaintEffectNoSchedule,
				},
			},
		},
	}
	tcs := []struct {
		name           string
		pods           []*apiv1.Pod
		expectedResult VmPreemptionType
	}{
		{
			"no pods with preemptible tolerations",
			[]*apiv1.Pod{noTolerationsPod, unrelatedTolerationsPod},
			NoPreemption,
		},
		{
			"one pod with preemptible toleration",
			[]*apiv1.Pod{noTolerationsPod, unrelatedTolerationsPod, preemptiblePod},
			LegacyPreemptible,
		},
		{
			"one pod with spot toleration",
			[]*apiv1.Pod{noTolerationsPod, unrelatedTolerationsPod, spotPod},
			Spot,
		},
		{
			"pods with both spot and preemptible tolerations",
			[]*apiv1.Pod{noTolerationsPod, unrelatedTolerationsPod, preemptiblePod, spotPod},
			Spot,
		},
	}

	for _, testCase := range tcs {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, ToleratedVmPreemptionForAnyPod(testCase.pods))
		})
	}
}

func TestPodRequiresPreemption(t *testing.T) {
	tcs := []struct {
		name           string
		pod            *apiv1.Pod
		expectedResult bool
	}{
		{
			"no node selector/affinity",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{},
			},
			false,
		},
		{
			"node selector for PVMs",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{labels.PreemptibleLabel: labels.PreemptionValue},
				},
			},
			true,
		},
		{
			"node affinity for PVMs",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      labels.PreemptibleLabel,
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{labels.PreemptionValue},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			true,
		},
		{
			"node selector for Spot",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{labels.SpotLabel: labels.PreemptionValue},
				},
			},
			true,
		},
		{
			"node selector for Spot, but wrong value",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{labels.SpotLabel: labels.PreemptionValue},
				},
			},
			true,
		},
		{
			"node affinity for Spot",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      labels.SpotLabel,
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{labels.PreemptionValue},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			true,
		},
	}

	for _, testCase := range tcs {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, PodRequiresPreemption(testCase.pod))
		})
	}
}
