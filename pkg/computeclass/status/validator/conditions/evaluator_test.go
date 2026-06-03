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

package conditions

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestEvaluator_GetCRDConditions_CrdMisconfigured(t *testing.T) {
	existingMachineFamily := "N2"
	existingCpuCores := 4
	existingMemoryGb := 32
	nonExistingMachineFamily := "XYZ"
	nonExistingCpuCores := 256
	nonExistingMemoryGb := 1024
	nonExistingMachineType := "invalid-machine-type"
	existingMachineType := "n2-standard-4"
	n2dHighCpu := "n2d-highcpu-224"
	ssdProvider := localssdsize.NewSimpleLocalSSDProvider()

	testCases := []struct {
		name          string
		crd           crd.CRD
		migs          map[string]*gke.GkeMig
		wantCondition bool
		wantReason    string
		wantMessage   string
	}{
		{
			name: "crd without rules",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: false,
		},
		{
			name: "crd with only nodepools rule",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			migs: map[string]*gke.GkeMig{
				"nodepool-1": gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-1").Build(),
			},
			wantCondition: false,
		},
		{
			name: "machine family exists",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&existingMachineFamily, nil, nil, nil),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: false,
		},
		{
			name: "machine family doesn't exist",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&nonExistingMachineFamily, nil, nil, nil),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    MachineFamilyNotFoundReason,
			wantMessage:   fmt.Sprintf(MachineFamilyNotFoundMessage, nonExistingMachineFamily),
		},
		{
			name: "machine family not exist in one rule",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&nonExistingMachineFamily, nil, nil, nil),
					rules.NewMachineSpecRule(&existingMachineFamily, nil, nil, nil),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    MachineFamilyNotFoundReason,
			wantMessage:   fmt.Sprintf(MachineFamilyNotFoundMessage, nonExistingMachineFamily),
		},
		{
			name: "invalid CPU requirement",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&existingMachineFamily, nil, &nonExistingCpuCores, nil),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    NoSuitableMachineExistsReason,
			wantMessage:   fmt.Sprintf(NoSuitableMachineExistsMessage, nonExistingCpuCores, 0),
		},
		{
			name: "invalid memory requirement",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&existingMachineFamily, nil, nil, &nonExistingMemoryGb),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    NoSuitableMachineExistsReason,
			wantMessage:   fmt.Sprintf(NoSuitableMachineExistsMessage, 0, nonExistingMemoryGb),
		},
		{
			name: "valid CPU requirement but invalid memory requirement",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&existingMachineFamily, nil, &existingCpuCores, &nonExistingMemoryGb),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    NoSuitableMachineExistsReason,
			wantMessage:   fmt.Sprintf(NoSuitableMachineExistsMessage, existingCpuCores, nonExistingMemoryGb),
		},
		{
			name: "valid memory requirement but invalid cpu requirement",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&existingMachineFamily, nil, &nonExistingCpuCores, &existingMemoryGb),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    NoSuitableMachineExistsReason,
			wantMessage:   fmt.Sprintf(NoSuitableMachineExistsMessage, nonExistingCpuCores, existingMemoryGb),
		},
		{
			name: "valid cpu and memory requirement",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&existingMachineFamily, nil, &existingCpuCores, &existingMemoryGb),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: false,
		},
		{
			name: "invalid machine type",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineTypeRule(&nonExistingMachineType)),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    MachineTypeNotFoundReason,
			wantMessage:   fmt.Sprintf(MachineTypeNotFoundMessage, nonExistingMachineType),
		},
		{
			name: "valid machine type",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineTypeRule(&existingMachineType)),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: false,
		},
		{
			name: "valid machine type but unavailable in zone (from validateMachineTypeConfig)",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineTypeRule(&n2dHighCpu)),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantReason:    UnavailableMachineTypeReason,
			wantMessage:   fmt.Sprintf(UnavailableMachineTypeMessage, n2dHighCpu, fmt.Errorf("Machine type %s is not available in zone %s.", n2dHighCpu, "zone-c")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAutoprovisioningLocations("zone-a", "zone-b", "zone-c").
				WithValidMachineTypes([]gce.MachineTypeKey{{MachineTypeName: existingMachineType, Zone: "zone-a"}}).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			evaluator := newTestEvaluator(provider,
				withSsdProvider(ssdProvider),
				withLister(lister.NewMockCrdLister(nil)))

			migs := tc.migs
			if migs == nil {
				migs = make(map[string]*gke.GkeMig)
			}
			conditions := evaluator.GetCRDConditions(tc.crd, migs)

			if tc.wantCondition {
				assert.NotEmpty(t, conditions)
				found := false
				for _, c := range conditions {
					if c.Type == CrdMisconfiguredCondition {
						assert.Equal(t, tc.wantReason, c.Reason)
						assert.Equal(t, tc.wantMessage, c.Message)
						found = true
						break
					}
				}
				assert.True(t, found, "Expected CrdMisconfiguredCondition not found")
			} else {
				for _, c := range conditions {
					assert.NotEqual(t, CrdMisconfiguredCondition, c.Type)
				}
			}
		})
	}
}

func TestEvaluator_GetCRDConditions_Deduplication(t *testing.T) {
	nonExistingMachineType := "invalid-machine-type"
	// Create provider with NAP disabled (default)
	provider := newTestProvider().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	evaluator := newTestEvaluator(provider,
		withLister(lister.NewMockCrdLister(nil)))

	// CRD with NAP enabled (triggers NapCannotBeEnabledCheck -> CrdMisconfigured)
	// AND invalid machine type rule (triggers validateNodeConfigRule -> CrdMisconfigured)
	crd := crd.NewTestCrd(
		crd.WithLabel("crd-1"),
		crd.WithAutoprovisioningEnabled(), // enabled
		crd.WithRules([]rules.Rule{
			rules.NewRule(rules.WithMachineTypeRule(&nonExistingMachineType)),
		}),
	)

	conditions := evaluator.GetCRDConditions(crd, make(map[string]*gke.GkeMig))

	count := 0
	for _, c := range conditions {
		if c.Type == CrdMisconfiguredCondition {
			count++
		}
	}
	assert.Equal(t, 1, count, "Expected exactly 1 CrdMisconfiguredCondition, got %d", count)

	if len(conditions) > 0 {
		assert.Contains(t, []string{NapCannotBeEnabledReason, MachineTypeNotFoundReason}, conditions[0].Reason)
	}
}
