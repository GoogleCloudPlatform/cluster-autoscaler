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

package inplace_test

import (
	"encoding/json"
	"maps"
	"reflect"
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/inplace"
)

func TestPlanHash_NilPlan(t *testing.T) {
	var plan *inplace.Plan
	if got := plan.Hash(); got != "" {
		t.Errorf("(*Plan)(nil).Hash() = %q, want %q", got, "")
	}
}

func TestPlanHash(t *testing.T) {
	taintA := apiv1.Taint{Key: "dedicated", Value: "gpu", Effect: apiv1.TaintEffectNoSchedule}
	taintB := apiv1.Taint{Key: "tier", Value: "batch", Effect: apiv1.TaintEffectNoExecute}
	now := metav1.NewTime(time.Unix(1700000000, 0))
	taintAWithTimestamp := apiv1.Taint{
		Key:       "dedicated",
		Value:     "gpu",
		Effect:    apiv1.TaintEffectNoSchedule,
		TimeAdded: &now,
	}

	base := &inplace.Plan{
		NodePoolName: "pool-a",
		DesiredSpec: &gkeclient.NodePoolSpec{
			Labels:            map[string]string{"tier": "frontend", gkelabels.ComputeClassConfigHashLabel: "h1"},
			Taints:            []apiv1.Taint{{Key: "dedicated", Value: "web", Effect: apiv1.TaintEffectNoSchedule}},
			ArchTaintBehavior: "ARM",
			SelfServiceMetadata: selfServiceWithTags(
				t,
				[]string{"net-a", "net-b"},
				map[string]string{"tagKeys/1": "tagValues/1"},
			),
		},
	}

	tests := []struct {
		name      string
		first     *inplace.Plan
		other     *inplace.Plan
		wantEqual bool
	}{
		{
			name: "label_and_taint_ordering_ignored",
			first: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{"a": "1", "b": "2"},
					Taints: []apiv1.Taint{taintA, taintB},
				},
			},
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{"b": "2", "a": "1"},
					Taints: []apiv1.Taint{taintB, taintA},
				},
			},
			wantEqual: true,
		},
		{
			name: "duplicate_taints_and_time_added_ignored",
			first: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Taints: []apiv1.Taint{taintA},
				},
			},
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Taints: []apiv1.Taint{taintAWithTimestamp, taintA},
				},
			},
			wantEqual: true,
		},
		{
			name: "nil_and_empty_collections_match",
			first: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec:  &gkeclient.NodePoolSpec{},
			},
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:              map[string]string{},
					Taints:              []apiv1.Taint{},
					SelfServiceMetadata: map[string]string{gkelabels.TagsLabelKey: "{}"},
				},
			},
			wantEqual: true,
		},
		{
			name: "nil_and_zero_desired_spec_match",
			first: &inplace.Plan{
				NodePoolName: "pool-a",
			},
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec:  &gkeclient.NodePoolSpec{},
			},
			wantEqual: true,
		},
		{
			name: "compute_class_name_current_spec_and_non_metadata_fields_ignored",
			first: &inplace.Plan{
				NodePoolName:     "pool-a",
				ComputeClassName: "cc-prod",
				CurrentSpec:      &gkeclient.NodePoolSpec{Labels: map[string]string{"tier": "old"}},
				DesiredSpec: &gkeclient.NodePoolSpec{
					MachineType:         "e2-standard-4",
					DiskSize:            100,
					Labels:              map[string]string{"tier": "new"},
					SelfServiceMetadata: map[string]string{gkelabels.GvnicLabelKey: "true"},
				},
			},
			other: &inplace.Plan{
				NodePoolName:     "pool-a",
				ComputeClassName: "cc-debug",
				CurrentSpec:      &gkeclient.NodePoolSpec{Labels: map[string]string{"tier": "new"}},
				DesiredSpec: &gkeclient.NodePoolSpec{
					MachineType:         "n2-standard-8",
					DiskSize:            200,
					Labels:              map[string]string{"tier": "new"},
					SelfServiceMetadata: map[string]string{gkelabels.ImageStreamingLabelKey: "true"},
				},
			},
			wantEqual: true,
		},
		{
			name: "resource_manager_tags_json_formatting_ignored",
			first: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					SelfServiceMetadata: map[string]string{
						gkelabels.TagsLabelKey: `{"tagKeys/1":"tagValues/1","tagKeys/2":"tagValues/2"}`,
					},
				},
			},
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					SelfServiceMetadata: map[string]string{
						gkelabels.TagsLabelKey: `{ "tagKeys/2" : "tagValues/2" , "tagKeys/1" : "tagValues/1" }`,
					},
				},
			},
			wantEqual: true,
		},
		{
			name:  "different_node_pool",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-b",
				DesiredSpec:  base.DesiredSpec,
			},
			wantEqual: false,
		},
		{
			name:  "different_label_value",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:              map[string]string{"tier": "backend", gkelabels.ComputeClassConfigHashLabel: "h1"},
					Taints:              base.DesiredSpec.Taints,
					ArchTaintBehavior:   base.DesiredSpec.ArchTaintBehavior,
					SelfServiceMetadata: base.DesiredSpec.SelfServiceMetadata,
				},
			},
			wantEqual: false,
		},
		{
			name:  "different_config_hash_label",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:              map[string]string{"tier": "frontend", gkelabels.ComputeClassConfigHashLabel: "h2"},
					Taints:              base.DesiredSpec.Taints,
					ArchTaintBehavior:   base.DesiredSpec.ArchTaintBehavior,
					SelfServiceMetadata: base.DesiredSpec.SelfServiceMetadata,
				},
			},
			wantEqual: false,
		},
		{
			name:  "empty_string_label_value_vs_absent",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:              map[string]string{"tier": "frontend", gkelabels.ComputeClassConfigHashLabel: "h1", "flag": ""},
					Taints:              base.DesiredSpec.Taints,
					ArchTaintBehavior:   base.DesiredSpec.ArchTaintBehavior,
					SelfServiceMetadata: base.DesiredSpec.SelfServiceMetadata,
				},
			},
			wantEqual: false,
		},
		{
			name:  "different_taint_effect",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:              base.DesiredSpec.Labels,
					Taints:              []apiv1.Taint{{Key: "dedicated", Value: "web", Effect: apiv1.TaintEffectNoExecute}},
					ArchTaintBehavior:   base.DesiredSpec.ArchTaintBehavior,
					SelfServiceMetadata: base.DesiredSpec.SelfServiceMetadata,
				},
			},
			wantEqual: false,
		},
		{
			name:  "different_arch_taint_behavior",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:              base.DesiredSpec.Labels,
					Taints:              base.DesiredSpec.Taints,
					ArchTaintBehavior:   "NONE",
					SelfServiceMetadata: base.DesiredSpec.SelfServiceMetadata,
				},
			},
			wantEqual: false,
		},
		{
			name:  "different_network_tags",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:            base.DesiredSpec.Labels,
					Taints:            base.DesiredSpec.Taints,
					ArchTaintBehavior: base.DesiredSpec.ArchTaintBehavior,
					SelfServiceMetadata: selfServiceWithTags(
						t,
						[]string{"net-a", "net-c"},
						map[string]string{"tagKeys/1": "tagValues/1"},
					),
				},
			},
			wantEqual: false,
		},
		{
			name:  "different_resource_manager_tags",
			first: base,
			other: &inplace.Plan{
				NodePoolName: "pool-a",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels:            base.DesiredSpec.Labels,
					Taints:            base.DesiredSpec.Taints,
					ArchTaintBehavior: base.DesiredSpec.ArchTaintBehavior,
					SelfServiceMetadata: selfServiceWithTags(
						t,
						[]string{"net-a", "net-b"},
						map[string]string{"tagKeys/1": "tagValues/999"},
					),
				},
			},
			wantEqual: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotFirst := tc.first.Hash()
			gotOther := tc.other.Hash()
			if gotFirst == "" || gotOther == "" {
				t.Fatalf("%s: (first.Hash(), other.Hash()) = (%q, %q), want non-empty hashes", tc.name, gotFirst, gotOther)
			}
			gotEqual := gotFirst == gotOther
			if gotEqual != tc.wantEqual {
				t.Errorf("%s: (first.Hash() == other.Hash()) = %v (%q vs %q), want %v", tc.name, gotEqual, gotFirst, gotOther, tc.wantEqual)
			}
		})
	}
}

func TestPlanSatisfied(t *testing.T) {
	taintWeb := apiv1.Taint{Key: "dedicated", Value: "web", Effect: apiv1.TaintEffectNoSchedule}
	taintExtra := apiv1.Taint{Key: "extra", Value: "ok", Effect: apiv1.TaintEffectPreferNoSchedule}

	fullDesired := &gkeclient.NodePoolSpec{
		Labels: map[string]string{
			"tier":                                "frontend",
			"empty-val":                           "",
			"node.gke.io/node-readiness-rule-np":  "enabled",
			gkelabels.ComputeClassConfigHashLabel: "hash-v2",
		},
		Taints:            []apiv1.Taint{taintWeb},
		ArchTaintBehavior: "NONE",
		SelfServiceMetadata: selfServiceWithTags(
			t,
			[]string{"fw-ingress", "fw-egress"},
			map[string]string{"tagKeys/1": "tagValues/10"},
		),
	}

	tests := []struct {
		name     string
		plan     *inplace.Plan
		reported *gkeclient.NodePoolSpec
		want     bool
	}{
		{
			name:     "nil_plan",
			plan:     nil,
			reported: fullDesired,
			want:     false,
		},
		{
			name:     "nil_desired_spec",
			plan:     &inplace.Plan{NodePoolName: "pool"},
			reported: fullDesired,
			want:     false,
		},
		{
			name:     "nil_reported_spec",
			plan:     &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: nil,
			want:     false,
		},
		{
			name:     "exact_match",
			plan:     &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: fullDesired,
			want:     true,
		},
		{
			name: "superset_labels_taints_and_network_tags_satisfy_plan",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:            mergeStringMaps(fullDesired.Labels, map[string]string{"out-of-band": "kept"}),
				Taints:            []apiv1.Taint{taintExtra, taintWeb},
				ArchTaintBehavior: "NONE",
				SelfServiceMetadata: selfServiceWithTags(
					t,
					[]string{"fw-egress", "fw-extra", "fw-ingress"},
					map[string]string{"tagKeys/1": "tagValues/10"},
				),
			},
			want: true,
		},
		{
			name: "extra_reported_resource_manager_tag_not_satisfied",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:            fullDesired.Labels,
				Taints:            fullDesired.Taints,
				ArchTaintBehavior: fullDesired.ArchTaintBehavior,
				SelfServiceMetadata: selfServiceWithTags(
					t,
					[]string{"fw-ingress", "fw-egress"},
					map[string]string{"tagKeys/1": "tagValues/10", "tagKeys/2": "tagValues/20"},
				),
			},
			want: false,
		},
		{
			name: "resource_manager_tags_unset_in_desired_ignores_reported",
			plan: &inplace.Plan{
				NodePoolName: "pool",
				DesiredSpec:  &gkeclient.NodePoolSpec{Labels: map[string]string{"a": "1"}},
			},
			reported: &gkeclient.NodePoolSpec{
				Labels: map[string]string{"a": "1"},
				SelfServiceMetadata: selfServiceWithTags(
					t,
					nil,
					map[string]string{"tagKeys/1": "tagValues/10"},
				),
			},
			want: true,
		},
		{
			name: "missing_label_with_empty_value",
			plan: &inplace.Plan{
				NodePoolName: "pool",
				DesiredSpec: &gkeclient.NodePoolSpec{
					Labels: map[string]string{"empty-val": ""},
				},
			},
			reported: &gkeclient.NodePoolSpec{
				Labels: map[string]string{"other": "1"},
			},
			want: false,
		},
		{
			name: "stale_label_value",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:              mergeStringMaps(fullDesired.Labels, map[string]string{"tier": "legacy"}),
				Taints:              fullDesired.Taints,
				ArchTaintBehavior:   fullDesired.ArchTaintBehavior,
				SelfServiceMetadata: fullDesired.SelfServiceMetadata,
			},
			want: false,
		},
		{
			name: "missing_taint",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:              fullDesired.Labels,
				Taints:              []apiv1.Taint{{Key: "dedicated", Value: "web", Effect: apiv1.TaintEffectNoExecute}},
				ArchTaintBehavior:   fullDesired.ArchTaintBehavior,
				SelfServiceMetadata: fullDesired.SelfServiceMetadata,
			},
			want: false,
		},
		{
			name: "arch_taint_behavior_default_arm_matches_empty_reported",
			plan: &inplace.Plan{
				NodePoolName: "pool",
				DesiredSpec:  &gkeclient.NodePoolSpec{ArchTaintBehavior: gkeclient.DefaultArchTaintBehavior},
			},
			reported: &gkeclient.NodePoolSpec{ArchTaintBehavior: ""},
			want:     true,
		},
		{
			name: "arch_taint_behavior_none_not_satisfied_by_default",
			plan: &inplace.Plan{
				NodePoolName: "pool",
				DesiredSpec:  &gkeclient.NodePoolSpec{ArchTaintBehavior: "NONE"},
			},
			reported: &gkeclient.NodePoolSpec{ArchTaintBehavior: ""},
			want:     false,
		},
		{
			name: "arch_taint_behavior_unset_in_desired_ignores_reported",
			plan: &inplace.Plan{
				NodePoolName: "pool",
				DesiredSpec:  &gkeclient.NodePoolSpec{Labels: map[string]string{"a": "1"}},
			},
			reported: &gkeclient.NodePoolSpec{
				Labels:            map[string]string{"a": "1"},
				ArchTaintBehavior: "NONE",
			},
			want: true,
		},
		{
			name: "missing_network_tag",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:            fullDesired.Labels,
				Taints:            fullDesired.Taints,
				ArchTaintBehavior: fullDesired.ArchTaintBehavior,
				SelfServiceMetadata: selfServiceWithTags(
					t,
					[]string{"fw-ingress"},
					map[string]string{"tagKeys/1": "tagValues/10"},
				),
			},
			want: false,
		},
		{
			name: "stale_resource_manager_tag",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:            fullDesired.Labels,
				Taints:            fullDesired.Taints,
				ArchTaintBehavior: fullDesired.ArchTaintBehavior,
				SelfServiceMetadata: selfServiceWithTags(
					t,
					[]string{"fw-ingress", "fw-egress"},
					map[string]string{"tagKeys/1": "tagValues/old"},
				),
			},
			want: false,
		},
		{
			name: "missing_resource_manager_tags",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:              fullDesired.Labels,
				Taints:              fullDesired.Taints,
				ArchTaintBehavior:   fullDesired.ArchTaintBehavior,
				SelfServiceMetadata: selfServiceWithTags(t, []string{"fw-ingress", "fw-egress"}, nil),
			},
			want: false,
		},
		{
			name: "malformed_reported_resource_manager_tags",
			plan: &inplace.Plan{NodePoolName: "pool", DesiredSpec: fullDesired},
			reported: &gkeclient.NodePoolSpec{
				Labels:            fullDesired.Labels,
				Taints:            fullDesired.Taints,
				ArchTaintBehavior: fullDesired.ArchTaintBehavior,
				SelfServiceMetadata: map[string]string{
					"network-tags.cloud.google.com/fw-ingress": "true",
					"network-tags.cloud.google.com/fw-egress":  "true",
					gkelabels.TagsLabelKey:                     "{bad",
				},
			},
			want: false,
		},
		{
			name: "malformed_desired_resource_manager_tags",
			plan: &inplace.Plan{
				NodePoolName: "pool",
				DesiredSpec: &gkeclient.NodePoolSpec{
					SelfServiceMetadata: map[string]string{gkelabels.TagsLabelKey: "{bad"},
				},
			},
			reported: &gkeclient.NodePoolSpec{
				SelfServiceMetadata: map[string]string{gkelabels.TagsLabelKey: "{bad"},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.plan.Satisfied(tc.reported); got != tc.want {
				t.Errorf("plan.Satisfied(reported) [%s] = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestPlanSatisfied_CurrentVsDesired(t *testing.T) {
	current := &gkeclient.NodePoolSpec{Labels: map[string]string{"env": "dev"}}
	desired := &gkeclient.NodePoolSpec{Labels: map[string]string{"env": "prod"}}
	plan := &inplace.Plan{
		NodePoolName: "nap-pool-1",
		CurrentSpec:  current,
		DesiredSpec:  desired,
	}
	if plan.Satisfied(current) {
		t.Errorf("plan.Satisfied(current) = true, want false")
	}
	if !plan.Satisfied(desired) {
		t.Errorf("plan.Satisfied(desired) = false, want true")
	}
}

// TestPlan_HashAndSatisfiedFieldParity verifies via reflection over
// NodePoolSpec (including per-field mutations of KubeletConfig and
// LinuxNodeConfig and per-key classes of SelfServiceMetadata) that Hash and
// Satisfied cover the exact same set of fields without manual list maintenance.
func TestPlan_HashAndSatisfiedFieldParity(t *testing.T) {
	empty := &gkeclient.NodePoolSpec{}
	baseHash := (&inplace.Plan{NodePoolName: "pool-a", DesiredSpec: empty}).Hash()

	mutations := nodePoolSpecFieldMutations(t)
	liveFields := 0
	for _, tc := range mutations {
		if (&inplace.Plan{NodePoolName: "pool-a", DesiredSpec: tc.spec}).Hash() != baseHash {
			liveFields++
		}
		t.Run(tc.name, func(t *testing.T) {
			plan := &inplace.Plan{
				NodePoolName: "pool-a",
				CurrentSpec:  empty,
				DesiredSpec:  tc.spec,
			}
			changesHash := plan.Hash() != baseHash
			checkedBySatisfied := !plan.Satisfied(empty)
			if changesHash != checkedBySatisfied {
				t.Errorf("field %s: (plan.Hash() != baseHash) = %v, (!plan.Satisfied(empty)) = %v; want equal", tc.name, changesHash, checkedBySatisfied)
			}
			if checkedBySatisfied && !plan.Satisfied(tc.spec) {
				t.Errorf("field %s: plan.Satisfied(DesiredSpec) = false, want true", tc.name)
			}
		})
	}
	if liveFields == 0 {
		t.Fatalf("count of NodePoolSpec fields changing Plan.Hash() = %d, want > 0", liveFields)
	}
}

type specMutation struct {
	name string
	spec *gkeclient.NodePoolSpec
}

func nodePoolSpecFieldMutations(t *testing.T) []specMutation {
	t.Helper()
	specType := reflect.TypeFor[gkeclient.NodePoolSpec]()
	var result []specMutation

	for i := range specType.NumField() {
		field := specType.Field(i)
		if !field.IsExported() {
			continue
		}
		switch field.Name {
		case "SelfServiceMetadata":
			result = append(result,
				specMutation{
					name: "SelfServiceMetadata_network_tag",
					spec: &gkeclient.NodePoolSpec{
						SelfServiceMetadata: map[string]string{
							"network-tags.cloud.google.com/fw-test": "true",
						},
					},
				},
				specMutation{
					name: "SelfServiceMetadata_resource_manager_tags",
					spec: &gkeclient.NodePoolSpec{
						SelfServiceMetadata: map[string]string{
							gkelabels.TagsLabelKey: `{"tagKeys/1":"tagValues/1"}`,
						},
					},
				},
				specMutation{
					name: "SelfServiceMetadata_non_live_feature",
					spec: &gkeclient.NodePoolSpec{
						SelfServiceMetadata: map[string]string{
							gkelabels.GvnicLabelKey: "true",
						},
					},
				},
			)
		case "KubeletConfig", "LinuxNodeConfig":
			subType := field.Type.Elem()
			for j := range subType.NumField() {
				subField := subType.Field(j)
				if !subField.IsExported() || subField.Tag.Get("json") == "-" {
					continue
				}
				spec := &gkeclient.NodePoolSpec{}
				subPtr := reflect.New(subType)
				populateNonZero(t, subPtr.Elem().Field(j), map[reflect.Type]bool{})
				reflect.ValueOf(spec).Elem().Field(i).Set(subPtr)
				result = append(result, specMutation{
					name: field.Name + "_" + subField.Name,
					spec: spec,
				})
			}
		default:
			spec := &gkeclient.NodePoolSpec{}
			populateNonZero(t, reflect.ValueOf(spec).Elem().Field(i), map[reflect.Type]bool{})
			result = append(result, specMutation{
				name: field.Name,
				spec: spec,
			})
		}
	}
	return result
}

func populateNonZero(t *testing.T, v reflect.Value, visited map[reflect.Type]bool) {
	t.Helper()
	if v.Type() == reflect.TypeFor[time.Time]() {
		v.Set(reflect.ValueOf(time.Unix(1700000000, 0)))
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(42)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(42)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.5)
	case reflect.String:
		v.SetString("NONE")
	case reflect.Slice:
		s := reflect.MakeSlice(v.Type(), 1, 1)
		populateNonZero(t, s.Index(0), visited)
		v.Set(s)
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		k := reflect.New(v.Type().Key()).Elem()
		elem := reflect.New(v.Type().Elem()).Elem()
		populateNonZero(t, k, visited)
		populateNonZero(t, elem, visited)
		m.SetMapIndex(k, elem)
		v.Set(m)
	case reflect.Pointer:
		if visited[v.Type()] {
			return
		}
		nextVisited := maps.Clone(visited)
		nextVisited[v.Type()] = true
		ptr := reflect.New(v.Type().Elem())
		populateNonZero(t, ptr.Elem(), nextVisited)
		v.Set(ptr)
	case reflect.Struct:
		for i := range v.NumField() {
			sf := v.Type().Field(i)
			if !sf.IsExported() || sf.Tag.Get("json") == "-" {
				continue
			}
			populateNonZero(t, v.Field(i), visited)
		}
	default:
		t.Fatalf("unsupported kind %s (%s); extend populateNonZero", v.Kind(), v.Type())
	}
}

func selfServiceWithTags(t *testing.T, networkTags []string, rmTags map[string]string) map[string]string {
	t.Helper()
	md := make(map[string]string)
	for _, tag := range networkTags {
		md["network-tags.cloud.google.com/"+tag] = "true"
	}
	if len(rmTags) > 0 {
		encoded, err := json.Marshal(rmTags)
		if err != nil {
			t.Fatalf("json.Marshal(%v) failed: %v", rmTags, err)
		}
		md[gkelabels.TagsLabelKey] = string(encoded)
	}
	return md
}

func mergeStringMaps(base, overrides map[string]string) map[string]string {
	merged := maps.Clone(base)
	if merged == nil {
		merged = make(map[string]string, len(overrides))
	}
	maps.Copy(merged, overrides)
	return merged
}
