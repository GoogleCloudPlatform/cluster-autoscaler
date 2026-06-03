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

package podrequirements

import (
	"reflect"
	"sort"
	"testing"

	netapi "github.com/GoogleCloudPlatform/gke-networking-api/apis/network/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestGetTolerations(t *testing.T) {
	tests := []struct {
		name        string
		tolerations []apiv1.Toleration
		expected    []apiv1.Toleration
	}{
		{"empty", []apiv1.Toleration{}, nil},
		{
			name: "all kinds of tolerations",
			tolerations: []apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "ns", Operator: apiv1.TolerationOpExists, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
				{Key: "ps", Operator: apiv1.TolerationOpExists, Value: "bat", Effect: apiv1.TaintEffectPreferNoSchedule},
			},
			expected: []apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "ns", Operator: apiv1.TolerationOpExists, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
			},
		},
		{
			name: "all kinds of operators",
			tolerations: []apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoExecute},
				{Key: "ns", Operator: apiv1.TolerationOpEqual, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
			},
			expected: []apiv1.Toleration{
				{Key: "ne", Operator: apiv1.TolerationOpExists, Value: "bar", Effect: apiv1.TaintEffectNoExecute},
				{Key: "ns", Operator: apiv1.TolerationOpEqual, Value: "bas", Effect: apiv1.TaintEffectNoExecute},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: tc.tolerations,
				},
			}
			r := getTolerations(p)
			if !reflect.DeepEqual(tc.expected, r) {
				t.Errorf("Expected: %+v, got: %+v", tc.expected, r)
			}
		})
	}
}

func TestGetNodeSelectors(t *testing.T) {
	tests := []struct {
		name      string
		nSelector map[string]string
		expected  map[string]Values
	}{
		{"empty", nil, map[string]Values{}},
		{
			"a few",
			map[string]string{"a": "b", "c": "d"},
			map[string]Values{
				"a": NewValues("b"),
				"c": NewValues("d"),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: tc.nSelector,
				},
			}
			r := getNodeSelectors(p)
			if !reflect.DeepEqual(tc.expected, r) {
				t.Errorf("Expected: %v, got: %v", tc.expected, r)
			}
		})
	}
}

func TestGetNodeAffinities(t *testing.T) {
	tests := []struct {
		name     string
		nsTerms  []apiv1.NodeSelectorTerm
		expected map[string]Values
	}{
		{"empty", []apiv1.NodeSelectorTerm{}, map[string]Values{}},
		{
			name: "extracts only In and Exists matches",
			nsTerms: []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
						{Key: "aa", Operator: apiv1.NodeSelectorOpIn, Values: []string{"bb", "cc"}},
						{Key: "b", Operator: apiv1.NodeSelectorOpNotIn, Values: []string{"d"}},
						{Key: "c", Operator: apiv1.NodeSelectorOpExists, Values: []string{""}},
						{Key: "d", Operator: apiv1.NodeSelectorOpDoesNotExist, Values: []string{""}},
						{Key: "e", Operator: apiv1.NodeSelectorOpGt, Values: []string{"1"}},
						{Key: "f", Operator: apiv1.NodeSelectorOpLt, Values: []string{"2"}},
					},
				},
			},
			expected: map[string]Values{"a": NewValues("b"), "aa": NewValues("bb", "cc"), "c": AnyValue()},
		},
		{
			name: "two terms",
			nsTerms: []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
						{Key: "x", Operator: apiv1.NodeSelectorOpExists},
					},
				},
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "c", Operator: apiv1.NodeSelectorOpIn, Values: []string{"d", "e"}},
						{Key: "y", Operator: apiv1.NodeSelectorOpExists},
					},
				},
			},
			expected: map[string]Values{"a": NewValues("b"), "c": NewValues("d", "e"), "x": AnyValue(), "y": AnyValue()},
		},
		{
			name: "Exists matches don't overwrite In matches",
			nsTerms: []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
						{Key: "a", Operator: apiv1.NodeSelectorOpExists},
					},
				},
			},
			expected: map[string]Values{"a": NewValues("b")},
		},
		{
			name: "Exists matches don't overwrite In matches across terms",
			nsTerms: []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpIn, Values: []string{"b"}},
					},
				},
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{Key: "a", Operator: apiv1.NodeSelectorOpExists},
					},
				},
			},
			expected: map[string]Values{"a": NewValues("b")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: tc.nsTerms,
							},
						},
					},
				},
			}
			r := getNodeAffinities(p)
			if !reflect.DeepEqual(tc.expected, r) {
				t.Errorf("Expected: %v, got: %v", tc.expected, r)
			}
		})
	}
}

func TestWorkloadSeparationMatch(t *testing.T) {
	tests := []struct {
		name         string
		toleration   apiv1.Toleration
		requirements map[string]Values
		expectedB    bool
		expectedS    string
	}{
		{
			name:       "exists toleration match",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			requirements: map[string]Values{
				"k":  NewValues("v"),
				"k2": NewValues("v2"),
			},
			expectedB: true,
			expectedS: "v",
		},
		{
			name:       "exists toleration match, but affinity for multiple values",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			requirements: map[string]Values{
				"k":  NewValues("v", "v1"),
				"k2": NewValues("v2"),
			},
			expectedB: false,
			expectedS: "",
		},
		{
			name:       "exists toleration match, but without label value specified",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			requirements: map[string]Values{
				"k":  AnyValue(),
				"k2": NewValues("v2"),
			},
			expectedB: false,
			expectedS: "",
		},
		{
			name:       "equal toleration match",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			requirements: map[string]Values{
				"k":  NewValues("v"),
				"k2": NewValues("v2"),
			},
			expectedB: true,
			expectedS: "v",
		},
		{
			name:       "equal toleration match, but affinity for multiple values",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			requirements: map[string]Values{
				"k":  NewValues("v", "v1"),
				"k2": NewValues("v2"),
			},
			expectedB: false,
			expectedS: "",
		},
		{
			name:       "equal toleration match, but without label value specified",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			requirements: map[string]Values{
				"k":  AnyValue(),
				"k2": NewValues("v2"),
			},
			expectedB: false,
			expectedS: "",
		},
		{
			name:       "exists toleration mismatch",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpExists, Value: ""},
			requirements: map[string]Values{
				"k1": NewValues("v1"),
				"k2": NewValues("v2"),
			},
			expectedB: false,
			expectedS: "",
		},
		{
			name:       "equal toleration mismatch",
			toleration: apiv1.Toleration{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v"},
			requirements: map[string]Values{
				"k":  NewValues("v1"),
				"k2": NewValues("v2"),
			},
			expectedB: false,
			expectedS: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs, rb := workloadSeparationMatch(tc.toleration, NewLabelRequirements(tc.requirements))
			if rs != tc.expectedS || rb != tc.expectedB {
				t.Errorf("Expected (%s, %v), got (%s, %v)", tc.expectedS, tc.expectedB, rs, rb)
			}
		})
	}
}

func TestWorkloadSeparationTaintsAndLabels(t *testing.T) {
	tests := []struct {
		name                               string
		tolerations                        []apiv1.Toleration
		requirements                       map[string]Values
		allowlistedSystemLabelPatterns     []string
		allowedNonWorkloadSeparationLabels map[string]bool
		expectLabels                       map[string]string
		expectTaints                       []apiv1.Taint
		expectErr                          error
	}{
		{
			name:         "no requirements and no tolerations",
			tolerations:  nil,
			requirements: nil,
			expectLabels: map[string]string{},
			expectTaints: []apiv1.Taint{},
			expectErr:    nil,
		},
		{
			name: "match with exists",
			tolerations: []apiv1.Toleration{
				{Key: "k", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k": NewValues("v")},
			expectLabels: map[string]string{"k": "v"},
			expectTaints: []apiv1.Taint{
				{Key: "k", Value: "v", Effect: apiv1.TaintEffectNoSchedule},
			},
			expectErr: nil,
		},
		{
			name: "match with equals",
			tolerations: []apiv1.Toleration{
				{Key: "k2", Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k": NewValues("v"), "k2": NewValues("v2")},
			expectErr:    NewInvalidWorkloadSeparationError("k"),
		},
		{
			name: "two matches",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k2", Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v"), "k2": NewValues("v2")},
			expectLabels: map[string]string{"k1": "v", "k2": "v2"},
			expectTaints: []apiv1.Taint{
				{Key: "k1", Value: "v", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "toleration without a match is ok",
			tolerations: []apiv1.Toleration{
				{Key: "k", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			expectTaints: []apiv1.Taint{},
			expectLabels: map[string]string{},
		},
		{
			name:         "requirement without a toleration is an error",
			requirements: map[string]Values{"k": NewValues("v")},
			expectErr:    NewInvalidWorkloadSeparationError("k"),
		},
		{
			name:                               "allowlisted labels without a toleration is not an error",
			requirements:                       map[string]Values{"k": NewValues("v")},
			allowedNonWorkloadSeparationLabels: map[string]bool{"k": true},
			expectTaints:                       []apiv1.Taint{},
			expectLabels:                       map[string]string{},
		},
		{
			name: "match with additional unmatched tolerations is ok",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k2", Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v1")},
			expectLabels: map[string]string{"k1": "v1"},
			expectTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "match with additional unmatched requirements is an error",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2")},
			expectErr:    NewInvalidWorkloadSeparationError("k2"),
		},
		{
			name: "system match",
			tolerations: []apiv1.Toleration{
				{Key: gkelabels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{gkelabels.GPULabel: NewValues("v")},
			expectTaints: []apiv1.Taint{},
			expectLabels: map[string]string{},
		},
		{
			name: "one system match, one non-system match",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: gkelabels.GPULabel, Operator: apiv1.TolerationOpEqual, Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": NewValues("v1"), gkelabels.GPULabel: NewValues("v2")},
			expectLabels: map[string]string{"k1": "v1"},
			expectTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "no match for any value",
			tolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k1": AnyValue()},
			expectErr:    NewInvalidWorkloadSeparationError("k1"),
		},
		{
			name: "no match for same keys but different values",
			tolerations: []apiv1.Toleration{
				{Key: "k", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{"k": NewValues("v2")},
			expectErr:    NewInvalidWorkloadSeparationError("k"),
		},
		{
			name: "allow listed system label match",
			tolerations: []apiv1.Toleration{
				{Key: gkelabels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements:                   map[string]Values{gkelabels.GPULabel: NewValues("true")},
			allowlistedSystemLabelPatterns: []string{gkelabels.GPULabel},
			expectLabels:                   map[string]string{gkelabels.GPULabel: "true"},
			expectTaints: []apiv1.Taint{
				{Key: gkelabels.GPULabel, Value: "true", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "allow listed & non allow listed system label",
			tolerations: []apiv1.Toleration{
				{Key: gkelabels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements:                   map[string]Values{"cloud.google.com/my-feature": NewValues("true"), gkelabels.GPULabel: NewValues("true")},
			allowlistedSystemLabelPatterns: []string{"cloud.google.com/my-feature"},
			expectLabels:                   map[string]string{"cloud.google.com/my-feature": "true"},
			expectTaints: []apiv1.Taint{
				{Key: "cloud.google.com/my-feature", Value: "true", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			name: "allow listed & non allow listed system label patterns",
			tolerations: []apiv1.Toleration{
				{Key: gkelabels.GPULabel, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature-1", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature-2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/unrecognized", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			requirements: map[string]Values{
				gkelabels.GPULabel:              NewValues("true"),
				"cloud.google.com/my-feature-1": NewValues("true"),
				"cloud.google.com/my-feature-2": NewValues("false"),
				"cloud.google.com/unrecognized": NewValues("skip"),
			},
			allowlistedSystemLabelPatterns: []string{`cloud.google.com/my-feature-\d`},
			expectLabels: map[string]string{
				"cloud.google.com/my-feature-1": "true",
				"cloud.google.com/my-feature-2": "false",
			},
			expectTaints: []apiv1.Taint{
				{Key: "cloud.google.com/my-feature-1", Value: "true", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "cloud.google.com/my-feature-2", Value: "false", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := Requirements{Tolerations: tc.tolerations, LabelReq: NewLabelRequirements(tc.requirements)}
			matcher, err := gkelabels.NewMatcher(tc.allowlistedSystemLabelPatterns)
			assert.NoError(t, err)
			c := NewWorkloadSeparationWorkloadChecker(matcher)
			resultTaints, resultLabels, resultErr := req.WorkloadSeparationTaintsAndLabels(c, tc.allowedNonWorkloadSeparationLabels)
			if !reflect.DeepEqual(tc.expectTaints, resultTaints) {
				t.Errorf("Expected taints %+v got %+v", tc.expectTaints, resultTaints)
			}
			if !reflect.DeepEqual(tc.expectLabels, resultLabels) {
				t.Errorf("Expected labels %v got %v", tc.expectLabels, resultLabels)
			}
			if diff := cmp.Diff(tc.expectErr, resultErr); diff != "" {
				t.Errorf("Expected error mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetRequirements(t *testing.T) {
	tests := []struct {
		name                          string
		pod                           *apiv1.Pod
		expectedTolerations           []apiv1.Toleration
		expectedLabelReq              map[string]Values
		expectedPodCapacity           string
		expectedQueuedProvisioningReq QueuedProvisioningRequirements
	}{
		{
			name:                          "plain pod",
			pod:                           &apiv1.Pod{},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with tolerations (soft tolerations not taken into account)",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Tolerations: []apiv1.Toleration{
						{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
						{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
						{Key: "k3", Operator: apiv1.TolerationOpExists, Effect: ""},
						{Key: "k4", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectPreferNoSchedule},
					},
				},
			},
			expectedTolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
				{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k3", Operator: apiv1.TolerationOpExists},
			},
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with node selectors",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v1", "k2": "v2"},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with node affinity (only required affinity taken into account)",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k1",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v1"},
											},
											{
												Key:      "k2",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v2", "v3"},
											},
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpExists,
											},
										},
									},
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k4",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v4"},
											},
										},
									},
								},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: []apiv1.PreferredSchedulingTerm{
								{
									Preference: apiv1.NodeSelectorTerm{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k5",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v5"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2", "v3"), "k3": AnyValue(), "k4": NewValues("v4")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with tolerations, node selectors, and node affinities",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v1", "k2": "v2"},
					Tolerations: []apiv1.Toleration{
						{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
						{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
					},
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v3"},
											},
											{
												Key:      "k4",
												Operator: apiv1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedTolerations: []apiv1.Toleration{
				{Key: "k1", Operator: apiv1.TolerationOpEqual, Value: "v1", Effect: apiv1.TaintEffectNoExecute},
				{Key: "k2", Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule},
			},
			expectedLabelReq:              map[string]Values{"k1": NewValues("v1"), "k2": NewValues("v2"), "k3": NewValues("v3"), "k4": AnyValue()},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "affinities with values override node selectors",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "abc",
					Namespace: "def",
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v10", "k2": "v20"},
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k2",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v21"},
											},
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v30"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{"k1": NewValues("v10"), "k2": NewValues("v21"), "k3": NewValues("v30")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "affinities without values don't override node selectors",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "abc",
					Namespace: "def",
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{"k1": "v10", "k2": "v20"},
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "k2",
												Operator: apiv1.NodeSelectorOpExists,
											},
											{
												Key:      "k3",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"v30"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{"k1": NewValues("v10"), "k2": NewValues("v20"), "k3": NewValues("v30")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with ppvm 1",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(gkelabels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "1",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with ppvm 2",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("other"): resource.MustParse("1"),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(gkelabels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "1",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with ppvm 3",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(gkelabels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName(gkelabels.PodCapacityLabel): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "2",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod without ppvm 1",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod without ppvm 2",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "pod with deprecated queued provisioning label",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						pods.DeprecatedProvisioningRequestPodAnnotationKey: "prov-req-1",
						pods.DeprecatedProvisioningClassPodAnnotationKey:   queuedwrapper.QueuedProvisioningClassName,
					},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{Enabled: true, ResizeRequestName: resizerequestclient.ResizeRequestName("", "prov-req-1")},
		},
		{
			name: "pod with queued provisioning label",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						v1.ProvisioningRequestPodAnnotationKey: "prov-req-1",
						v1.ProvisioningClassPodAnnotationKey:   queuedwrapper.QueuedProvisioningClassName,
					},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{Enabled: true, ResizeRequestName: resizerequestclient.ResizeRequestName("", "prov-req-1")},
		},
		{
			name: "best effort atomic pod with deprecated annotations",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						pods.DeprecatedProvisioningRequestPodAnnotationKey: "prov-req-1",
						pods.DeprecatedProvisioningClassPodAnnotationKey:   "best-effort-atomic-scale-up.autoscaling.x-k8s.io",
					},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "best effort atomic pod",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						v1.ProvisioningRequestPodAnnotationKey: "prov-req-1",
						v1.ProvisioningClassPodAnnotationKey:   "best-effort-atomic-scale-up.autoscaling.x-k8s.io",
					},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "resource label selector",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"annotationKey": "refkey:refvalue"},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"cloud.google.com/resourcelabel_annotationKey": "",
					},
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{"cloud.google.com/resourcelabel_annotationKey": NewValues("refkey:refvalue")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "resource label selector, missing annotation",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"cloud.google.com/resourcelabel_annotationKey": "",
					},
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{"cloud.google.com/resourcelabel_annotationKey": NewValues("")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "resource label selector, non empty value",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"annotationKey": "refkey:refvalue"},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"cloud.google.com/resourcelabel_annotationKey": "non-empty",
					},
					Containers: []apiv1.Container{
						{
							Name: "container1",
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{"cloud.google.com/resourcelabel_annotationKey": NewValues("non-empty")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "edp affinity processed correctly",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{},
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      gkelabels.ExtendedDurationPodsLabel,
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"3", "X"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{gkelabels.ExtendedDurationPodsLabel: NewValues("3", "X")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
		{
			name: "edp affinity with only X value is processed correctly",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{},
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      gkelabels.ExtendedDurationPodsLabel,
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"X"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedTolerations:           nil,
			expectedLabelReq:              map[string]Values{gkelabels.ExtendedDurationPodsLabel: NewValues("X")},
			expectedPodCapacity:           "",
			expectedQueuedProvisioningReq: QueuedProvisioningRequirements{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := GetRequirements(tc.pod)

			if !reflect.DeepEqual(req.Tolerations, tc.expectedTolerations) {
				t.Errorf("Expected tolerations %+v, got %+v", tc.expectedTolerations, req.Tolerations)
			}
			if !reflect.DeepEqual(req.LabelReq, NewLabelRequirements(tc.expectedLabelReq)) {
				t.Errorf("Expected label requirements %+v, got %+v", NewLabelRequirements(tc.expectedLabelReq), req.LabelReq)
			}
			if req.PodCapacity != tc.expectedPodCapacity {
				t.Errorf("Expected podCapacity %q, got %q", tc.expectedPodCapacity, req.PodCapacity)
			}
			if req.QueuedProvisioningReq != tc.expectedQueuedProvisioningReq {
				t.Errorf("Expected queuedProvisioningReq %+v, got %+v", tc.expectedQueuedProvisioningReq, req.QueuedProvisioningReq)
			}
		})
	}
}

func TestGetAllKeyValueMatches(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		req      map[string]Values
		want     map[string]string
	}{
		{
			name:     "match single pattern",
			patterns: []string{"^cloud\\.google\\.com/.*"},
			req: map[string]Values{
				"cloud.google.com/gke-nodepool": NewValues("default-pool"),
				"user-label":                    NewValues("user-value"),
			},
			want: map[string]string{
				"cloud.google.com/gke-nodepool": "default-pool",
			},
		},
		{
			name:     "match multiple patterns",
			patterns: []string{"^label1$", "^label2$"},
			req: map[string]Values{
				"label1":     NewValues("v1"),
				"label2":     NewValues("v2"),
				"label3":     NewValues("v3"),
				"otherlabel": NewValues("v4"),
			},
			want: map[string]string{
				"label1": "v1",
				"label2": "v2",
			},
		},
		{
			name:     "no match",
			patterns: []string{"^nonexistent$"},
			req: map[string]Values{
				"label1": NewValues("v1"),
			},
			want: map[string]string{},
		},
		{
			name:     "any value is ignored",
			patterns: []string{".*"},
			req: map[string]Values{
				"label1": AnyValue(),
				"label2": NewValues("v2"),
			},
			want: map[string]string{
				"label2": "v2",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matcher, err := gkelabels.NewMatcher(tc.patterns)
			if err != nil {
				t.Fatalf("Failed to create matcher: %v", err)
			}
			lr := NewLabelRequirements(tc.req)
			got := lr.GetAllKeyValueMatches(matcher)
			if !reflect.DeepEqual(tc.want, got) {
				t.Errorf("GetAllKeyValueMatches() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetSingleValue(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		expectedVal   string
		expectedFound bool
		req           map[string]Values
	}{
		{
			name:          "single value present",
			key:           "k",
			expectedVal:   "v",
			expectedFound: true,
			req:           map[string]Values{"k": NewValues("v")},
		},
		{
			name:          "multiple values present",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           map[string]Values{"k": NewValues("v", "w")},
		},
		{
			name:          "any value present",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           map[string]Values{"k": AnyValue()},
		},
		{
			name:          "no value",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           map[string]Values{},
		},
		{
			name:          "nil map",
			key:           "k",
			expectedVal:   "",
			expectedFound: false,
			req:           nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			labelReq := NewLabelRequirements(tc.req)
			val, isSpecified := labelReq.GetSingleValue(tc.key)
			if val != tc.expectedVal {
				t.Errorf("GetSingleValue value: expected %s, got %s", tc.expectedVal, val)
			}
			if isSpecified != tc.expectedFound {
				t.Errorf("GetSingleValue found: expected %v, got %v", tc.expectedFound, isSpecified)
			}
		})
	}
}

func TestGetValues(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		expectedVals  Values
		expectedFound bool
		req           map[string]Values
	}{
		{
			name:          "single value present",
			key:           "k",
			expectedVals:  NewValues("v"),
			expectedFound: true,
			req:           map[string]Values{"k": NewValues("v")},
		},
		{
			name:          "multiple values present",
			key:           "k",
			expectedVals:  NewValues("v", "w", "z"),
			expectedFound: true,
			req:           map[string]Values{"k": NewValues("v", "w", "z")},
		},
		{
			name:          "any value present",
			key:           "k",
			expectedVals:  AnyValue(),
			expectedFound: true,
			req:           map[string]Values{"k": AnyValue()},
		},
		{
			name:          "no value",
			key:           "k",
			expectedVals:  Values{},
			expectedFound: false,
			req:           map[string]Values{},
		},
		{
			name:          "nil map",
			key:           "k",
			expectedVals:  Values{},
			expectedFound: false,
			req:           nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			labelReq := NewLabelRequirements(tc.req)
			vals, found := labelReq.GetValues(tc.key)
			compareAllUnexportedOpt := cmp.Exporter(func(t reflect.Type) bool { return true })
			if diff := cmp.Diff(tc.expectedVals, vals, compareAllUnexportedOpt); diff != "" {
				t.Errorf("GetValues values diff (-want +got):\n%s", diff)
			}
			if found != tc.expectedFound {
				t.Errorf("GetValues found: expected %v, got %v", tc.expectedFound, found)
			}
		})
	}
}

func TestNetworkingRequirements(t *testing.T) {
	for desc, tc := range map[string]struct {
		pod            *apiv1.Pod
		wantNetworkReq NetworkingRequirements
	}{
		"no networking resources": {
			pod: &apiv1.Pod{},
		},
		"multi networking resources": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/red-net.IP"},
			},
		},
		"high performance networking resources": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/netdev-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
									apiv1.ResourceName("networking.gke.io.networks/netdev-net"):    *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/netdev-net", "networking.gke.io.networks/netdev-net.IP"},
			},
		},
		"networking resources in multiple containers": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/blue-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/blue-net.IP", "networking.gke.io.networks/red-net.IP"},
			},
		},
		"networking resources in one of containers": {
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceMemory: *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			wantNetworkReq: NetworkingRequirements{
				AdditionalNetworkResources: []string{"networking.gke.io.networks/red-net.IP"},
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotReq := GetRequirements(tc.pod)
			sort.Strings(gotReq.NetworkingReq.AdditionalNetworkResources)
			if diff := cmp.Diff(tc.wantNetworkReq, gotReq.NetworkingReq); diff != "" {
				t.Errorf("GetRequirements.NetworkingReq values diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInterfaceAnnotation(t *testing.T) {
	for desc, tc := range map[string]struct {
		pod            *apiv1.Pod
		wantAnnotation string
	}{
		"no annotations": {
			pod: &apiv1.Pod{},
		},
		"empty annotations": {
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
		},
		"interface annotation specified": {
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						netapi.InterfaceAnnotationKey: "interface-test",
					},
				},
			},
			wantAnnotation: "interface-test",
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotReq := GetRequirements(tc.pod)
			if diff := cmp.Diff(tc.wantAnnotation, gotReq.NetworkingAnnotation); diff != "" {
				t.Errorf("GetRequirements.NetworkingAnnotation values diff (-want +got):\n%s", diff)
			}
		})
	}
}
