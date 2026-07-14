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
	"testing"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestGpuRuleMatchesNodeGroup(t *testing.T) {
	nonDefaultMachineFamilyName := machinetypes.N2.Name()
	machineType := "n2-standard-8"

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      GpuRule
		expected  bool
	}{
		{
			name:      "rule with GPU and family, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4", Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}}}).Build(),
			rule:      NewRule(WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu"}, Count: 2, PhysicalGPUCount: 2}), WithMachineFamilyRule(&nonDefaultMachineFamilyName)),
			expected:  true,
		},
		{
			name:      "rule with GPU and instance type, node group with same GPU and instance type, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-8", Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}}}).Build(),
			rule:      NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu"}, Count: 2, PhysicalGPUCount: 2})),
			expected:  true,
		},
		{
			name:      "rule with GPU and instance type, node group with different GPU and same instance type, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-8", Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}}}).Build(),
			rule:      NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu2"}, Count: 2, PhysicalGPUCount: 2})),
			expected:  false,
		},
		{
			name:      "rule with GPU and instance type, node group with same GPU and different instance type, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4", Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}}}).Build(),
			rule:      NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu"}, Count: 2, PhysicalGPUCount: 2})),
			expected:  false,
		},
		{
			name: "rule with GPU and driver version, node group with same GPU and driver version, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}},
				Labels:       map[string]string{labels.GPUDriverVersionLabel: "default"},
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: "default"}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with GPU and driver version, node group with same GPU but different driver version, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}},
				Labels:       map[string]string{labels.GPUDriverVersionLabel: "version-x"},
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: "default"}, Count: 2, PhysicalGPUCount: 2})),
			expected: false,
		},
		{
			name:      "rule without GPU, node group with GPU, everything else matches",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-8", Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}}}).Build(),
			rule:      NewRule(WithMachineTypeRule(&machineType)),
			expected:  true,
		},
		{
			name: "rule with GPU and driver version, node group with same GPU and driver version (case-insensitive), matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}},
				Labels:       map[string]string{labels.GPUDriverVersionLabel: "LATEST"},
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: "latest"}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with GPU and driver version, node group with same GPU and driver version (different casing), matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{GpuDriverVersion: "LATEST"}}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: "latest"}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with GPU and driver version, node group with same GPU but different driver version (case-insensitive), not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{GpuDriverVersion: "version-x"}}},
				Labels:       map[string]string{labels.GPUDriverVersionLabel: "version-x"},
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: "default"}, Count: 2, PhysicalGPUCount: 2})),
			expected: false,
		},
		{
			name: "rule with GPU and driver version, node group with missing driver version label, but driver version in config, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{GpuDriverVersion: "latest"}}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: "latest"}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with autoinstall-disabled GPU driver version, matching driver disabled label",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2}},
				Labels:       map[string]string{labels.GPUDriverVersionLabel: "disabled"},
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: labels.DisabledGPUDriverVersionValue}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with autoinstall-disabled GPU driver version, matching INSTALLATION_DISABLED in config",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{GpuDriverVersion: "INSTALLATION_DISABLED"}}},
				Labels:       map[string]string{},
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: labels.DisabledGPUDriverVersionValue}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with autoinstall-disabled GPU driver version, matching GPU_DRIVER_VERSION_UNSPECIFIED in config",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{GpuDriverVersion: "GPU_DRIVER_VERSION_UNSPECIFIED"}}},
				Labels:       map[string]string{},
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", DriverVersion: labels.DisabledGPUDriverVersionValue}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with GPU and partitioning, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuPartitionSize: "2g.5g"}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", PartitionSize: "2g.5g"}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with GPU and partitioning, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuPartitionSize: "2g.10g"}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", PartitionSize: "2g.5g"}, Count: 2, PhysicalGPUCount: 2})),
			expected: false,
		},
		{
			name: "rule with GPU and sharing strategy, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuSharingConfig: &gke_api_beta.GPUSharingConfig{GpuSharingStrategy: "MPS"}}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", SharingStrategy: "mps"}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with GPU and sharing strategy, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuSharingConfig: &gke_api_beta.GPUSharingConfig{GpuSharingStrategy: "strategy-1"}}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", SharingStrategy: "strategy-2"}, Count: 2, PhysicalGPUCount: 2})),
			expected: false,
		},
		{
			name: "rule with GPU, sharing strategy, max-shared-clients, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuSharingConfig: &gke_api_beta.GPUSharingConfig{GpuSharingStrategy: "MPS", MaxSharedClientsPerGpu: 2}}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", SharingStrategy: "mps", MaxSharedClients: "2"}, Count: 2, PhysicalGPUCount: 2})),
			expected: true,
		},
		{
			name: "rule with GPU, sharing strategy, max-shared-clients, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  "n2-standard-8",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "my-gpu", AcceleratorCount: 2, GpuSharingConfig: &gke_api_beta.GPUSharingConfig{GpuSharingStrategy: "strategy-1", MaxSharedClientsPerGpu: 3}}},
				Labels:       map[string]string{}, //No label
			}).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType), WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: "my-gpu", SharingStrategy: "strategy-1", MaxSharedClients: "2"}, Count: 2, PhysicalGPUCount: 2})),
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
