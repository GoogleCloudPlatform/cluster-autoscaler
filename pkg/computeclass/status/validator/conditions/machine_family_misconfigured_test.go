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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateMachineFamilyConfig(t *testing.T) {
	testCases := []struct {
		name          string
		rule          rules.Rule
		wantCondition bool
		wantReason    string
		wantMessage   string
	}{
		{
			name: "Valid machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(strPtr("e2")),
				rules.WithMinCoresRule(intPtr(2)),
				rules.WithMinMemoryGbRule(intPtr(4)),
			),
			wantCondition: false,
		},
		{
			name: "Invalid machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(strPtr("invalid-family")),
			),
			wantCondition: true,
			wantReason:    MachineFamilyNotFoundReason,
			wantMessage:   fmt.Sprintf(MachineFamilyNotFoundMessage, "invalid-family"),
		},
		{
			name: "No suitable machine exists",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(strPtr("e2")),
				rules.WithMinCoresRule(intPtr(1000)),
				rules.WithMinMemoryGbRule(intPtr(1000)),
			),
			wantCondition: true,
			wantReason:    NoSuitableMachineExistsReason,
			wantMessage:   fmt.Sprintf(NoSuitableMachineExistsMessage, 1000, 1000),
		},
		{
			name: "Default machine family used",
			rule: rules.NewRule(
				rules.WithMinCoresRule(intPtr(2)),
				rules.WithMinMemoryGbRule(intPtr(4)),
			),
			wantCondition: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			checker := &machineFamilyConfigChecker{provider: provider}
			condition := checker.checkRule(tc.rule)

			if tc.wantCondition {
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

func strPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func TestValidateMachineTypeExistence(t *testing.T) {
	testCases := []struct {
		name          string
		rule          rules.Rule
		wantCondition bool
		wantReason    string
		wantMessage   string
	}{
		{
			name: "Valid machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(strPtr("e2-medium")),
			),
			wantCondition: false,
		},
		{
			name: "Invalid machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(strPtr("invalid-type")),
			),
			wantCondition: true,
			wantReason:    MachineTypeNotFoundReason,
			wantMessage:   fmt.Sprintf(MachineTypeNotFoundMessage, "invalid-type"),
		},
		{
			name: "Empty machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(strPtr("")),
			),
			wantCondition: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			checker := &machineTypeExistenceChecker{provider: provider}
			condition := checker.checkRule(tc.rule)

			if tc.wantCondition {
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
