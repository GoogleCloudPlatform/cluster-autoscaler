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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateMachineTypeConfig(t *testing.T) {
	n2dHighCpu := "n2d-highcpu-224"
	customMachineType1 := "custom-4-32768"
	customMachineType2 := "n2d-custom-8-65536"
	customMachineType3 := "e2-custom-16-131072"
	customMachineType4 := "n2-custom-4-32768"
	customMachineType5 := "n2-custom-8-65536"
	customMachineType6 := "n4-custom-4-16384"

	zoneA := "zone-a"
	zoneB := "zone-b"
	zoneC := "zone-c"

	validMachineTypes := []gce.MachineTypeKey{
		{
			MachineTypeName: customMachineType1,
			Zone:            zoneA,
		},
		{
			MachineTypeName: customMachineType2,
			Zone:            zoneB,
		},
		{
			MachineTypeName: customMachineType3,
			Zone:            zoneB,
		},
		{
			MachineTypeName: customMachineType3,
			Zone:            zoneC,
		},
		{
			MachineTypeName: customMachineType5,
			Zone:            zoneB,
		},
		{
			MachineTypeName: customMachineType6,
			Zone:            zoneA,
		},
	}

	testCases := []struct {
		name        string
		rule        rules.Rule
		wantReason  string
		wantMessage string
	}{
		{
			name:        "predefined unavailable machine type",
			rule:        rules.NewRule(rules.WithMachineTypeRule(&n2dHighCpu)),
			wantReason:  UnavailableMachineTypeReason,
			wantMessage: fmt.Sprintf(UnavailableMachineTypeMessage, n2dHighCpu, fmt.Errorf("Machine type %s is not available in zone %s.", n2dHighCpu, zoneC)),
		},
		{
			name: "custom machine type available in zone a",
			rule: rules.NewRule(rules.WithMachineTypeRule(&customMachineType1)),
		},
		{
			name:        "custom machine type not available available in required zone",
			rule:        rules.NewRule(rules.WithMachineTypeRule(&customMachineType1), rules.WithLocationRule([]string{zoneB})),
			wantReason:  UnavailableMachineTypeReason,
			wantMessage: fmt.Sprintf(UnavailableMachineTypeMessage, customMachineType1, fmt.Errorf("Machine type %s is not available in zone %s.", customMachineType1, zoneB)),
		},
		{
			name: "custom machine type available in zone b",
			rule: rules.NewRule(rules.WithMachineTypeRule(&customMachineType2)),
		},
		{
			name: "custom machine type available in zones b and c",
			rule: rules.NewRule(rules.WithMachineTypeRule(&customMachineType3)),
		},
		{
			name:        "custom machine type unavailable in any of the zones",
			rule:        rules.NewRule(rules.WithMachineTypeRule(&customMachineType4)),
			wantReason:  UnavailableMachineTypeReason,
			wantMessage: fmt.Sprintf(UnavailableMachineTypeMessage, customMachineType4, fmt.Errorf("Machine type %s is not available in zone %s.", customMachineType4, zoneC)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAutoprovisioningLocations(zoneA, zoneB, zoneC).
				WithValidMachineTypes(validMachineTypes).
				Build()
			checker := &machineTypeConfigChecker{provider: provider}
			// We access the first rule because the test cases are constructed with one rule
			condition := checker.checkRule(tc.rule)
			if tc.wantReason != "" {
				assert.NotNil(t, condition)
				assert.Equal(t, RuleMisconfiguredCondition, condition.Type)
				assert.Equal(t, tc.wantReason, condition.Reason)
				assert.Equal(t, tc.wantMessage, condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
