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

package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const ErrNone errors.AutoscalerErrorType = ""

func resourcePolicy(modifiers ...func(*gceclient.GceResourcePolicy) *gceclient.GceResourcePolicy) *gceclient.GceResourcePolicy {
	ret := &gceclient.GceResourcePolicy{}
	for _, modifier := range modifiers {
		ret = modifier(ret)
	}
	return ret
}
func withTopology(topology string) func(*gceclient.GceResourcePolicy) *gceclient.GceResourcePolicy {
	return func(r *gceclient.GceResourcePolicy) *gceclient.GceResourcePolicy {
		r.WorkloadPolicy.AcceleratorTopology = topology
		return r
	}
}

func TestFromLabels(t *testing.T) {
	tcs := []struct {
		name              string
		labels            map[string]string
		expected          Spec
		wantUsesPlacement bool
	}{
		{
			"no placement",
			map[string]string{"abc": "xyz"},
			Spec{"", "", nil},
			false,
		},
		{
			"compact",
			map[string]string{labels.PlacementGroupLabel: "placement-id"},
			Spec{"placement-id", "", nil},
			true,
		},
		{
			"byopp/byowp",
			map[string]string{labels.PolicyLabel: "pp-id"},
			Spec{"", "pp-id", nil},
			true,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p := FromLabels(tc.labels)
			assert.Equal(t, tc.expected, p)
			assert.Equal(t, tc.wantUsesPlacement, p.UsesPlacement())
		})
	}
}

func TestFromRequirements(t *testing.T) {
	tcs := []struct {
		name     string
		pod      *apiv1.Pod
		expected Spec
	}{
		{
			"no placement",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{},
			},
			Spec{"", "", nil},
		},
		{
			"compact",
			&apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{labels.PlacementGroupLabel: "placement-id"},
				},
			},
			Spec{"placement-id", "", nil},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, FromRequirements(podrequirements.GetRequirements(tc.pod).LabelReq))
		})
	}
}

func TestFromReservationResourcePolices(t *testing.T) {
	tcs := []struct {
		name string
		// rrp - reservation's resource policy
		rrp      map[string]string
		expected Spec
		errType  errors.AutoscalerErrorType
	}{
		{
			name:     "spec from placement label",
			rrp:      map[string]string{"placement": "projects/test-project/regions/us-central1-c/resourcePolicies/reservation-from-placement-label"},
			expected: Spec{"", "reservation-from-placement-label", nil},
		},
		{
			name:     "spec from policy label",
			rrp:      map[string]string{"policy": "projects/test-project/regions/us-central1-c/resourcePolicies/reservation-from-policy-label"},
			expected: Spec{"", "reservation-from-policy-label", nil},
		},
		{
			name:     "no placement",
			rrp:      map[string]string{"other-label": "other-value"},
			expected: Spec{"", "", nil},
		},
		{
			name:     "invalid placement",
			rrp:      map[string]string{"placement": "invalid-placement", "other-label": "other-value"},
			expected: Spec{"", "", nil},
			errType:  InvalidPlacementPolicyError,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := FromReservationResourcePolices(tc.rrp)
			if tc.errType == ErrNone {
				assert.Nil(t, err)
			} else {
				assert.Equal(t, err.Type(), tc.errType)
			}
			assert.Equal(t, tc.expected, spec)
		})
	}
}

func TestValidate(t *testing.T) {
	tcs := []struct {
		name              string
		group             Spec
		existingNodePools sets.Set[string]
		wantErrType       errors.AutoscalerErrorType
	}{
		{
			name:              "no placement",
			group:             Spec{"", "", nil},
			existingNodePools: sets.New[string](),
		},
		{
			name:              "doesn't exist",
			group:             Spec{"my-group", "", nil},
			existingNodePools: sets.New[string]("other-group"),
		},
		{
			name:              "already exists",
			group:             Spec{"my-group", "", nil},
			existingNodePools: sets.New[string]("other-group", "my-group"),
			wantErrType:       NodeGroupAlreadyExistsError,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.group.Validate(tc.existingNodePools)
			if tc.wantErrType == ErrNone {
				assert.Nil(t, err)
			} else {
				assert.Equal(t, err.Type(), tc.wantErrType)
			}
		})
	}
}

func TestValidateAndUpdateMachineSpec(t *testing.T) {
	tcs := []struct {
		name            string
		group           Spec
		machineSpec     machinetypes.MachineSpec
		selectionType   machinetypes.SelectionType
		tpuTopology     string
		tpuChipsPerNode int64
		wantErrType     errors.AutoscalerErrorType
		wantMachineSpec machinetypes.MachineSpec
	}{
		{
			name:  "no placement",
			group: Spec{"", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.E2},
			},
			selectionType: machinetypes.SelectionTypeDefault,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.E2},
			},
		},
		{
			name:  "default types not supported",
			group: Spec{"my-group", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2},
			},
			selectionType: machinetypes.SelectionTypeDefault,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2},
			},
			wantErrType: InvalidMachineFamilyError,
		},
		{
			name:  "implied machine family",
			group: Spec{"my-group", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2},
			},
		},
		{
			name:  "implied multiple machine families",
			group: Spec{"my-group", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{
					machinetypes.C2,
					machinetypes.N1,
					machinetypes.A4X,
				},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2},
			},
		},
		{
			name:  "implied unsupported machine family",
			group: Spec{"my-group", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.N1},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.N1},
			},
			wantErrType: InvalidMachineFamilyError,
		},
		{
			name:  "specified machine family",
			group: Spec{"my-group", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2},
			},
			selectionType: machinetypes.SelectionTypeSpecified,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2},
			},
		},
		{
			name:  "specified unsupported machine family",
			group: Spec{"my-group", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2, machinetypes.N1},
			},
			selectionType: machinetypes.SelectionTypeSpecified,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.C2, machinetypes.N1},
			},
			wantErrType: InvalidMachineFamilyError,
		},
		{
			name:  "implied non-compact machine family for managed compact placement",
			group: Spec{"my-group", "", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			wantErrType: InvalidMachineFamilyError,
		},
		{
			name:  "implied_non-compact_machine_family_err_without_policy",
			group: Spec{"", "some-policy", nil},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			wantErrType: InvalidPlacementPolicyError,
		},
		{
			name:  "implied_non-compact_machine_family_tpu_mutlihost_without_policy",
			group: Spec{"", "some-policy", nil},
			machineSpec: machinetypes.MachineSpec{
				TpuType:  "tpu7x",
				Families: []machinetypes.MachineFamily{machinetypes.TPU7X},
			},
			tpuTopology:     "2x2x2",
			tpuChipsPerNode: 4,
			selectionType:   machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				TpuType:  "tpu7x",
				Families: []machinetypes.MachineFamily{machinetypes.TPU7X},
			},
			wantErrType: InvalidPlacementPolicyError,
		},
		{
			name:  "implied_non-compact_machine_family_tpu_singlehost_without_policy",
			group: Spec{"", "some-policy", nil},
			machineSpec: machinetypes.MachineSpec{
				TpuType:  "tpu7x",
				Families: []machinetypes.MachineFamily{machinetypes.TPU7X},
			},
			tpuTopology:     "2x2x1",
			tpuChipsPerNode: 4,
			selectionType:   machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				TpuType:  "tpu7x",
				Families: []machinetypes.MachineFamily{machinetypes.TPU7X},
			},
		},
		{
			name:  "implied_non-compact_machine_family_for_any_policy",
			group: Spec{"", "some-policy", resourcePolicy(withTopology("2x2"))},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
		},
		{
			name:  "implied_non-compact_machine_family_for_group_and_policy",
			group: Spec{"group-id", "some-policy", resourcePolicy(withTopology("2x2"))},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
		},
		{
			name:  "implied_non-compact_machine_family_for_policy_no_topology",
			group: Spec{"", "some-policy", resourcePolicy()},
			machineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			selectionType: machinetypes.SelectionTypeImplied,
			wantMachineSpec: machinetypes.MachineSpec{
				Families: []machinetypes.MachineFamily{machinetypes.A4X},
			},
			wantErrType: InvalidPlacementPolicyError,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.group.UpdateMachineSpec(&tc.machineSpec, tc.selectionType)
			if err == nil {
				err = tc.group.ValidateMachineSpec(&tc.machineSpec, tc.selectionType, tc.tpuTopology, tc.tpuChipsPerNode, &machinetypes.MachineConfigProvider{})
			}
			if tc.wantErrType == ErrNone {
				assert.Nil(t, err)
			} else {
				assert.Equal(t, err.Type(), tc.wantErrType)
			}
			assert.Equal(t, tc.machineSpec, tc.wantMachineSpec)
		})
	}
}

func TestMaxNodes(t *testing.T) {
	a4x, err := machinetypes.NewMachineConfigProvider(nil).ToMachineType("a4x-highgpu-4g")
	assert.NoError(t, err)

	testCase := []struct {
		description    string
		machineType    string
		resourcePolicy *gceclient.GceResourcePolicy
		wantError      bool
		want           int64
	}{
		{
			description: "a2 family",
			machineType: "a2-highgpu-1g",
			want:        150,
		},
		{
			description: "a3 family",
			machineType: "a3-highgpu-8g",
			want:        96,
		},
		{
			description: "c2 family",
			machineType: "c2-standard-16",
			want:        150,
		},
		{
			description: "c2d family",
			machineType: "c2d-standard-56",
			want:        150,
		},
		{
			description: "c3 family",
			machineType: "c3-standard-88",
			want:        150,
		},
		{
			description: "c3-metal family",
			machineType: "c3-standard-192-metal",
			want:        150,
		},
		{
			description: "c3d family",
			machineType: "c3d-standard-30",
			want:        150,
		},
		{
			description: "h3 family",
			machineType: "h3-standard-88",
			want:        150,
		},
		{
			description: "n2 family",
			machineType: "n2-standard-2",
			want:        150,
		},
		{
			description: "n2d family",
			machineType: "n2d-standard-48",
			want:        150,
		},
		{
			description: "g2 family",
			machineType: "g2-standard-48",
			want:        150,
		},
		{
			description: "non-existent machine type",
			machineType: "custom",
			wantError:   true,
		},
		{
			description: "tpu machine type",
			machineType: "ct4l-hightpu-4t",
			wantError:   true,
		},
		{
			description: "machine family does not have compact placement max node limit",
			machineType: "e2-standard-2",
			wantError:   true,
		},
		{
			description:    "hasResourcePolicy_nonExistentMachineType_returnError",
			resourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "2x32"}},
			machineType:    "custom",
			wantError:      true,
		},
		{
			description:    "hasResourcePolicy_valid_setFromTopology",
			resourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "2x32"}},
			machineType:    a4x.Name,
			want:           (2 * 32) / int64(a4x.FixedGpuCount()),
		},
		{
			description:    "hasResourcePolicy_valid_setFromTopology",
			resourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
			machineType:    a4x.Name,
			want:           18,
		},
		{
			description:    "hasResourcePolicy_nonZeroRemainder",
			resourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x5"}},
			machineType:    a4x.Name,
			wantError:      true,
		},
		{
			description:    "hasResourcePolicy_TpuMachineType",
			resourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "2x2x1"}},
			machineType:    "tpu7x-standard-4t",
			want:           1,
		},
		{
			description:    "hasResourcePolicy_TpuMachineType",
			resourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "2x2x1"}},
			machineType:    "tpu7x-standard-1t",
			want:           4,
		},
		{
			description:    "hasResourcePolicy_FallbackToCompact",
			resourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "invalid-topology"}},
			machineType:    "c2-standard-16",
			want:           150,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.description, func(t *testing.T) {
			maxNodeLimit, err := MaxNodes(machinetypes.NewMachineConfigProvider(nil), tc.machineType, tc.resourcePolicy)
			if tc.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, maxNodeLimit)
			}
		})
	}
}

func TestIsMachineFamilySupported(t *testing.T) {
	tests := []struct {
		name          string
		spec          Spec
		family        machinetypes.MachineFamily
		wantSupported bool
	}{
		{
			name:          "C2 (supports compact) - Managed compact placement",
			spec:          Spec{GroupId: "c2-group"},
			family:        machinetypes.C2,
			wantSupported: true,
		},
		{
			name:          "C2 (supports compact) - BYOPP/BYOWP",
			spec:          Spec{Policy: "c2-policy"},
			family:        machinetypes.C2,
			wantSupported: true,
		},
		{
			name:          "A4X (supports non-compact) - Managed compact placement",
			spec:          Spec{GroupId: "a4x-group"},
			family:        machinetypes.A4X,
			wantSupported: false,
		},
		{
			name:          "A4X (supports non-compact) - BYOPP/BYOWP",
			spec:          Spec{Policy: "a4x-policy"},
			family:        machinetypes.A4X,
			wantSupported: true,
		},
		{
			name:          "A4X (supports non-compact) - Non-compact placement",
			spec:          Spec{Policy: "a4x-policy", GroupId: "a4x-group"},
			family:        machinetypes.A4X,
			wantSupported: true,
		},
		{
			name:          "E2 (supports neither) - Compact placement",
			spec:          Spec{GroupId: "e2-group"},
			family:        machinetypes.E2,
			wantSupported: false,
		},
		{
			name:          "E2 (supports neither) - Non-compact placement",
			spec:          Spec{Policy: "e2-policy"},
			family:        machinetypes.E2,
			wantSupported: false,
		},
		{
			name:          "E2 (supports neither) - Non-compact placement",
			spec:          Spec{Policy: "e2-policy", GroupId: "e2-group"},
			family:        machinetypes.E2,
			wantSupported: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSupported := tc.spec.SupportsMachineFamily(tc.family)
			assert.Equal(t, tc.wantSupported, gotSupported)
		})
	}
}

func TestUsesSlice(t *testing.T) {
	tests := []struct {
		name          string
		placement     Spec
		wantUsesSlice bool
	}{
		{
			name:          "null_resource_policy_-_returns_false",
			placement:     Spec{Policy: "policy"},
			wantUsesSlice: false,
		},
		{
			name: "policy_without_topology_-_returns_false",
			placement: Spec{
				Policy:         "policy",
				ResourcePolicy: resourcePolicy(),
			},
			wantUsesSlice: false,
		},
		{
			name: "empty_topology_-_returns_false",
			placement: Spec{
				Policy:         "policy",
				ResourcePolicy: resourcePolicy(withTopology("")),
			},
			wantUsesSlice: false,
		},
		{
			name: "non_empty_topology_-_returns_true",
			placement: Spec{
				Policy:         "policy",
				ResourcePolicy: resourcePolicy(withTopology("test")),
			},
			wantUsesSlice: true,
		},
		{
			name: "valid_topology_-_returns_true",
			placement: Spec{
				Policy:         "policy",
				ResourcePolicy: resourcePolicy(withTopology("2x32x2")),
			},
			wantUsesSlice: true,
		},
		{
			name: "missing_policy_name_-_returns_false",
			placement: Spec{
				ResourcePolicy: resourcePolicy(withTopology("2x32x2")),
			},
			wantUsesSlice: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.placement.UsesSlice()
			assert.Equal(t, tc.wantUsesSlice, actual)
		})
	}
}
