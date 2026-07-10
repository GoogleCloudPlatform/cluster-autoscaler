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

package machineselection

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"

	v1 "k8s.io/api/core/v1"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/utils/ptr"
)

func TestCrdMachineFamilies(t *testing.T) {
	generalPurposePodFamily := "general-purpose"
	unknownPodFamily := "unknown"
	knownMachineType := "n2d-standard-4"
	unknownMachineType := "unknown"
	knownMachineFamily := "n2"
	unknownMachineFamily := "unknown"

	for tn, tc := range map[string]struct {
		rule         rules.Rule
		wantFamilies []machinetypes.MachineFamily
		wantFound    bool
		wantErr      bool
		wantErrMsg   string
	}{
		"no rule": {
			rule:      nil,
			wantFound: false,
		},
		"pod family specified": {
			rule:         rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			wantFamilies: []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK, machinetypes.E4},
			wantFound:    true,
		},
		"unknown pod family specified": {
			rule:    rules.NewRule(rules.WithPodFamilyRule(&unknownPodFamily)),
			wantErr: true,
		},
		"known machine type specified": {
			rule:         rules.NewRule(rules.WithMachineTypeRule(&knownMachineType)),
			wantFamilies: []machinetypes.MachineFamily{machinetypes.N2D},
			wantFound:    true,
		},
		"unknown machine type specified": {
			rule:    rules.NewRule(rules.WithMachineTypeRule(&unknownMachineType)),
			wantErr: true,
		},
		"gpu request specified": {
			rule:      rules.NewRule(rules.WithMachineFamilyRule(&knownMachineFamily), rules.WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "some gpu"}})),
			wantFound: false,
		},
		"known machine family specified": {
			rule:         rules.NewRule(rules.WithMachineFamilyRule(&knownMachineFamily)),
			wantFamilies: []machinetypes.MachineFamily{machinetypes.N2},
			wantFound:    true,
		},
		"unknown machine family specified": {
			rule:    rules.NewRule(rules.WithMachineFamilyRule(&unknownMachineFamily)),
			wantErr: true,
		},
		"empty rule": {
			rule:      rules.NewRule(),
			wantFound: false,
		},
		"pod family and mismatched machine family specified": {
			rule:       rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily), rules.WithMachineFamilyRule(ptr.To("n2"))),
			wantErr:    true,
			wantErrMsg: "n2 (not in pod family general-purpose)",
		},
		"pod family and matched machine family specified": {
			rule:         rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily), rules.WithMachineFamilyRule(ptr.To("e2"))),
			wantFamilies: []machinetypes.MachineFamily{machinetypes.E2},
			wantFound:    true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			families, found, err := crdMachineFamilies(provider, tc.rule)
			if tc.wantErr {
				assert.Error(t, err)
				if tc.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tc.wantErrMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantFamilies, families)
				assert.Equal(t, tc.wantFound, found)
			}
		})
	}
}

func TestPodArchitectures(t *testing.T) {
	for tn, tc := range map[string]struct {
		labelReq          map[string]podrequirements.Values
		wantArchitectures map[gce.SystemArchitecture]bool
		wantErr           error
	}{
		"no architecture specified": {
			labelReq:          nil,
			wantArchitectures: nil,
		},
		"single architecture specified": {
			labelReq:          map[string]podrequirements.Values{v1.LabelArchStable: podrequirements.NewValues(gce.Amd64.Name())},
			wantArchitectures: map[gce.SystemArchitecture]bool{gce.Amd64: true},
		},
		"multiple architectures specified": {
			labelReq:          map[string]podrequirements.Values{v1.LabelArchStable: podrequirements.NewValues(gce.Amd64.Name(), gce.Arm64.Name())},
			wantArchitectures: map[gce.SystemArchitecture]bool{gce.Amd64: true, gce.Arm64: true},
		},
		"AnyValue architecture specified -> treat as if nothing was specified": {
			labelReq:          map[string]podrequirements.Values{v1.LabelArchStable: podrequirements.AnyValue()},
			wantArchitectures: nil,
		},
		"invalid architecture specified -> error": {
			labelReq:          map[string]podrequirements.Values{v1.LabelArchStable: podrequirements.NewValues("some-arch")},
			wantArchitectures: nil,
			wantErr:           cmpopts.AnyError,
		},
		"some of architectures specified are invalid -> error": {
			labelReq:          map[string]podrequirements.Values{v1.LabelArchStable: podrequirements.NewValues(gce.Amd64.Name(), "some-arch")},
			wantArchitectures: nil,
			wantErr:           cmpopts.AnyError,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			gotArchitectures, gotErr := podArchitectures(podrequirements.NewLabelRequirements(tc.labelReq))
			if diff := cmp.Diff(tc.wantArchitectures, gotArchitectures, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("podArchitectures diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantErr, gotErr, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("podArchitectures error diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCrdMinCpuPlatform(t *testing.T) {
	knownCpuPlatform := "Intel Emerald Rapids"
	unknownCpuPlatform := "unknown"
	emptyCpuPlatform := ""

	for tn, tc := range map[string]struct {
		rule               rules.Rule
		wantMinCpuPlatform machinetypes.CpuPlatform
		wantFound          bool
		wantErr            bool
	}{
		"no rule": {
			rule:               nil,
			wantMinCpuPlatform: machinetypes.UnknownPlatform,
			wantFound:          false,
		},
		"rule without MinCpuPlatform setting specified": {
			rule:               rules.NewRule(),
			wantMinCpuPlatform: machinetypes.AnyPlatform,
			wantFound:          true,
		},
		"rule with nil pointer MinCpuPlatform setting specified": {
			rule:               rules.NewRule(rules.WithMinCpuPlatformRule(nil)),
			wantMinCpuPlatform: machinetypes.AnyPlatform,
			wantFound:          true,
		},
		"rule with empty string MinCpuPlatform setting specified": {
			rule:               rules.NewRule(rules.WithMinCpuPlatformRule(&emptyCpuPlatform)),
			wantMinCpuPlatform: machinetypes.UnknownPlatform,
			wantFound:          true,
			wantErr:            true,
		},
		"rule with unknown MinCpuPlatform setting specified": {
			rule:               rules.NewRule(rules.WithMinCpuPlatformRule(&unknownCpuPlatform)),
			wantMinCpuPlatform: machinetypes.UnknownPlatform,
			wantFound:          true,
			wantErr:            true,
		},
		"rule with known MinCpuPlatform setting specified": {
			rule:               rules.NewRule(rules.WithMinCpuPlatformRule(&knownCpuPlatform)),
			wantMinCpuPlatform: machinetypes.IntelEmeraldRapids,
			wantFound:          true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			minCpuPlatform, found, err := crdMinCpuPlatform(tc.rule)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantMinCpuPlatform, minCpuPlatform)
				assert.Equal(t, tc.wantFound, found)
			}
		})
	}
}
