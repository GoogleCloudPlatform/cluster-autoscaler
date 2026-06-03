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

package rules

import (
	"fmt"
	"reflect"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/zonetypes"
)

func TestLocationZoneTypesRuleMatchesNodeGroup(t *testing.T) {
	machineType := "n2-standard-8"
	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      NodePoolsRule
		expected  bool
	}{
		{
			name: "node group in all standard zones, prefecence is standard, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in all ai zones, prefecence is ai, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-ai1", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in all cluster zones, prefecence is cluster default, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"CLUSTER_DEFAULT"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in all standard and ia zones, prefecence is standard+ai, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-ai1", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in cluster's ai and all standard zones, prefecence is standard+cluster default, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "CLUSTER_DEFAULT"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in cluster's standard and all ia zones, prefecence is ai+cluster default, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-ai1", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI", "CLUSTER_DEFAULT"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in all standard and ia zones, prefecence is standard+ai+cluster default, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-ai1", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI", "CLUSTER_DEFAULT"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in all standard and ia zones, prefecence contains unknown type, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-ai1", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI", "UNKNOWN"}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in not all standard zones, prefecence is standard, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createDefaultProvider())),
			expected: false,
		},
		{
			name: "node group in not all ai zones, prefecence is ai, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-ai1"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, createDefaultProvider())),
			expected: false,
		},
		{
			name: "node group in not all clsuster's zones, prefecence is cluster default, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"CLUSTER_DEFAULT"}, createProvider([]string{}, []string{}, []string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil))),
			expected: false,
		},
		{
			name: "node group in all standrad and not all ai zones, prefecence is standard+ai, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-ai1"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI"}, createDefaultProvider())),
			expected: false,
		},
		{
			name: "node group in all ai and not all standard zones, prefecence is standard+ai, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-ai1", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI"}, createDefaultProvider())),
			expected: false,
		},
		{
			name:      "mig spec is nil, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(nil).Build(),
			rule:      NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createDefaultProvider())),
			expected:  false,
		},
		{
			name: "prefecence is empty, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-ai1", "us-central1-ai2"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{}, createDefaultProvider())),
			expected: true,
		},
		{
			name: "node group in trimmed zones, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createProvider([]string{"us-central1-a", "us-central1-b"}, []string{}, []string{}, []string{"us-central1-a"}))),
			expected: true,
		},
		{
			name: "node group in more zones than trimmed, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a", "us-central1-b"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createProvider([]string{"us-central1-a", "us-central1-b"}, []string{}, []string{}, []string{"us-central1-a"}))),
			expected: false,
		},
		{
			name: "node group in less zones than trimmed, no match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createProvider([]string{"us-central1-a", "us-central1-b"}, []string{}, []string{}, []string{"us-central1-a", "us-central1-b"}))),
			expected: false,
		},
		{
			name: "node group in less zones than trimmed, compact placement, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:    machineType,
				Locations:      []string{"us-central1-a"},
				PlacementGroup: placement.Spec{Policy: "compact-placement"},
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createProvider([]string{"us-central1-a", "us-central1-b"}, []string{}, []string{}, []string{"us-central1-a", "us-central1-b"}))),
			expected: true,
		},
		{
			name: "node group in less zones than trimmed, multi-host tpu, match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  machineType,
				Locations:    []string{"us-central1-a"},
				TpuMultiHost: true,
			}).Build(),
			rule:     NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createProvider([]string{"us-central1-a", "us-central1-b"}, []string{}, []string{}, []string{"us-central1-a", "us-central1-b"}))),
			expected: true,
		},
		{
			name: "error_fetching_standar_zones,_no_match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-a"},
			}).Build(),
			rule: NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI"}, &testZoneDataProvider{
				standardErr: fmt.Errorf("standard error"),
			})),
			expected: false,
		},
		{
			name: "error_fetching_ai_zones,_no_match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-ai1"},
			}).Build(),
			rule: NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, &testZoneDataProvider{
				aiErr: fmt.Errorf("ai error"),
			})),
			expected: false,
		},
		{
			name: "no_ai_zones_found,_no_match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"us-central1-ai1"},
			}).Build(),
			rule: NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, &testZoneDataProvider{
				aiZones: []string{},
			})),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.rule.Matches(tc.nodegroup)
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}

func TestGetZoneTypesZonesWithError(t *testing.T) {
	testCases := []struct {
		name            string
		rule            LocationZoneTypesRule
		expectedZones   []string
		expectedErrType caerrors.AutoscalerErrorType
	}{
		{
			name:          "all_zone_types_success",
			rule:          NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI", "CLUSTER_DEFAULT"}, createDefaultProvider())),
			expectedZones: []string{"us-central1-a", "us-central1-b", "us-central1-ai1", "us-central1-ai2"},
		},
		{
			name:          "only_cluster_default_success",
			rule:          NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"CLUSTER_DEFAULT"}, createDefaultProvider())),
			expectedZones: []string{"us-central1-a", "us-central1-ai2"},
		},
		{
			name:          "only_standard_success",
			rule:          NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, createDefaultProvider())),
			expectedZones: []string{"us-central1-a", "us-central1-b"},
		},
		{
			name:          "only_ai_success",
			rule:          NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, createDefaultProvider())),
			expectedZones: []string{"us-central1-ai1", "us-central1-ai2"},
		},
		{
			name:          "empty_zone_types_success",
			rule:          NewRule(WithLocationZoneTypesRule([]v1.ZoneType{}, createDefaultProvider())),
			expectedZones: nil,
		},
		{
			name:          "unknown_zone_type_success_skipped",
			rule:          NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"UNKNOWN"}, createDefaultProvider())),
			expectedZones: nil,
		},
		{
			name: "standard_zones_failure",
			rule: NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, &testZoneDataProvider{
				standardErr: fmt.Errorf("standard error"),
			})),
			expectedErrType: caerrors.CloudProviderError,
		},
		{
			name: "ai_zones_failure",
			rule: NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, &testZoneDataProvider{
				aiErr: fmt.Errorf("ai error"),
			})),
			expectedErrType: caerrors.CloudProviderError,
		},
		{
			name: "no_ai_zones_failure",
			rule: NewRule(WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, &testZoneDataProvider{
				aiZones: []string{},
			})),
			expectedErrType: zonetypes.MissingAIZonesError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			zones, err := tc.rule.GetZoneTypesZones()
			if tc.expectedErrType != "" {
				if err == nil {
					t.Fatalf("Test: %q failed, expected error type: %v, got: nil", tc.name, tc.expectedErrType)
				}
				if err.Type() != tc.expectedErrType {
					t.Errorf("Test: %q failed, expected error type: %v, got: %v", tc.name, tc.expectedErrType, err.Type())
				}
				return
			}
			if err != nil {
				t.Fatalf("Test: %q failed, unexpected error: %v", tc.name, err)
			}
			if !reflect.DeepEqual(zones, tc.expectedZones) {
				t.Errorf("Test: %q failed, want zones: %v, got: %v", tc.name, tc.expectedZones, zones)
			}
		})
	}
}

func createDefaultProvider() *testZoneDataProvider {
	return &testZoneDataProvider{
		autoprovisioningLocations: []string{"us-central1-a", "us-central1-ai2"},
		standardZones:             []string{"us-central1-a", "us-central1-b"},
		aiZones:                   []string{"us-central1-ai1", "us-central1-ai2"},
	}
}

func createProvider(standardZones []string, aiZones []string, autoprovisioningLocations []string, trimmedLocations []string) *testZoneDataProvider {
	return &testZoneDataProvider{
		autoprovisioningLocations: autoprovisioningLocations,
		standardZones:             standardZones,
		aiZones:                   aiZones,
		trimmedLocations:          trimmedLocations,
	}
}

type testZoneDataProvider struct {
	autoprovisioningLocations []string
	standardZones             []string
	aiZones                   []string
	trimmedLocations          []string
	standardErr               error
	aiErr                     error
}

func (p *testZoneDataProvider) GetAIZones() ([]string, error) {
	if p.aiErr != nil {
		return nil, p.aiErr
	}
	return p.aiZones, nil
}

func (p *testZoneDataProvider) GetStandardZones() ([]string, error) {
	if p.standardErr != nil {
		return nil, p.standardErr
	}
	return p.standardZones, nil
}

func (p *testZoneDataProvider) GetAutoprovisioningLocations() []string {
	return p.autoprovisioningLocations
}

func (p *testZoneDataProvider) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	if p.trimmedLocations != nil {
		return p.trimmedLocations
	}
	return locations
}
