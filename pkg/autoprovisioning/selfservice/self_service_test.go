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

package selfservice

import (
	"os"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkesandbox "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/utils/ptr"
)

func TestMain(m *testing.M) {
	InitSelfService(nil)
	os.Exit(m.Run())
}

func TestNodepoolMetadata(t *testing.T) {
	testCases := []struct {
		name         string
		nodepool     *container.NodePool
		wantMetadata Metadata
	}{
		{
			name:         "Nodepool is nil",
			wantMetadata: make(Metadata),
		},
		{
			name: "Nodepool with GpuDirect strategy is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					GpuDirectConfig: &container.GPUDirectConfig{
						GpuDirectStrategy: "RDMA",
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.GpuDirectLabel: "rdma",
			},
		},
		{
			name:         "Nodepool config is nil",
			nodepool:     &container.NodePool{},
			wantMetadata: make(Metadata),
		},
		{
			name: "Nodepool config is empty",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{},
			},
			wantMetadata: make(Metadata),
		},
		{
			name: "Nodepool group label is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.NodePoolGroupNameLabel: "test-value",
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.NodePoolGroupNameLabel: "test-value",
			},
		},
		{
			name: "Workload type label is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.WorkloadTypeLabel: "test-value",
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.WorkloadTypeLabel: "test-value",
			},
		},
		{
			name: "Nodepool with image streaming enabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					GcfsConfig: &container.GcfsConfig{Enabled: true},
				},
			},
			wantMetadata: Metadata{
				gkelabels.ImageStreamingLabelKey: "true",
			},
		},
		{
			name: "Nodepool with image streaming explicitly disabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					GcfsConfig: &container.GcfsConfig{Enabled: false},
				},
			},
			wantMetadata: Metadata{},
		},
		{
			name: "Nodepool with gvnic enabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Gvnic: &container.VirtualNIC{Enabled: true},
				},
			},
			wantMetadata: Metadata{
				gkelabels.GvnicLabelKey: "true",
			},
		},
		{
			name: "Nodepool with gvnic explicitly disabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Gvnic: &container.VirtualNIC{Enabled: false},
				},
			},
			wantMetadata: Metadata{
				gkelabels.GvnicLabelKey: "false",
			},
		},
		{
			name: "Nodepool with sandbox configured is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					SandboxConfig: &container.SandboxConfig{Type: "gvisor"},
				},
			},
			wantMetadata: Metadata{
				gkelabels.SandboxLabelKey: "gvisor",
			},
		},
		{
			name: "Nodepool with sandbox configured as uppercase is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					SandboxConfig: &container.SandboxConfig{Type: "GVISOR"},
				},
			},
			wantMetadata: Metadata{
				gkelabels.SandboxLabelKey: "gvisor",
			},
		},
		{
			name: "Nodepool with gvnic implicitly disabled",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "a2-ultragpu-4g",
				},
			},
			wantMetadata: Metadata{
				gkelabels.GvnicLabelKey: "false",
			},
		},
		{
			name: "Nodepool with gvnic implicitly enabled",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "a3-ultragpu-8g", // gen-3
				},
			},
			wantMetadata: Metadata{
				gkelabels.GvnicLabelKey: "true",
			},
		},
		{
			name: "Nodepool with logging variant set returns correct LoggingConfigVariant label",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					LoggingConfig: &container.NodePoolLoggingConfig{
						VariantConfig: &container.LoggingVariantConfig{
							Variant: "MAX_THROUGHPUT",
						},
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.LoggingConfigVariant: "MAX_THROUGHPUT",
			},
		},
		{
			name: "Nodepool with empty logging variant returns empty metadata",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					LoggingConfig: &container.NodePoolLoggingConfig{
						VariantConfig: &container.LoggingVariantConfig{
							Variant: "",
						},
					},
				},
			},
			wantMetadata: Metadata{},
		},
		{
			name: "Nodepool with variant config not set returns empty metadata",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					LoggingConfig: &container.NodePoolLoggingConfig{},
				},
			},
			wantMetadata: Metadata{},
		},
		{
			name: "ConfidentialNodeType type label is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ConfidentialNodes: &container.ConfidentialNodes{ConfidentialInstanceType: "test-value"},
				},
			},
			wantMetadata: Metadata{
				gkelabels.GkeConfidentialNodeType: "test-value",
			},
		},
		{
			name: "CapacityCheckWaitTimeSeconds label is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
		},
		{
			name: "Auto repair and upgrade settings are set to false",
			nodepool: &container.NodePool{
				Management: &container.NodeManagement{
					AutoRepair:  false,
					AutoUpgrade: false,
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoRepairLabelKey:  "false",
				gkelabels.AutoUpgradeLabelKey: "false",
			},
		},
		{
			name: "Auto repair and upgrade settings are set to true",
			nodepool: &container.NodePool{
				Management: &container.NodeManagement{
					AutoRepair:  true,
					AutoUpgrade: true,
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoRepairLabelKey:  "true",
				gkelabels.AutoUpgradeLabelKey: "true",
			},
		},
		{
			name: "Auto repair setting is set to true, auto upgrade setting is set to false",
			nodepool: &container.NodePool{
				Management: &container.NodeManagement{
					AutoRepair:  true,
					AutoUpgrade: false,
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoRepairLabelKey:  "true",
				gkelabels.AutoUpgradeLabelKey: "false",
			},
		},
		{
			name: "Auto repair setting is set to false, auto upgrade setting is set to true",
			nodepool: &container.NodePool{
				Management: &container.NodeManagement{
					AutoRepair:  false,
					AutoUpgrade: true,
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoRepairLabelKey:  "false",
				gkelabels.AutoUpgradeLabelKey: "true",
			},
		},
		{
			name: "Nodepool with resource manager tags is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ResourceManagerTags: &container.ResourceManagerTags{
						Tags: map[string]string{"test-project/test-key-1": "test-value-1"},
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.TagsLabelKey: "{\"test-project/test-key-1\":\"test-value-1\"}",
			},
		},
		{
			name: "Nodepool with DRANET label is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.DraNetNodeLabel: "true",
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.DraNetNodeLabel: "true",
			},
		},
		{
			name: "Nodepool with LocationPolicy is processed correctly",
			nodepool: &container.NodePool{
				Autoscaling: &container.NodePoolAutoscaling{
					LocationPolicy: "BALANCED",
				},
			},
			wantMetadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "BALANCED",
			},
		},
		{
			name: "Nodepool with invalid LocationPolicy is processed correctly",
			nodepool: &container.NodePool{
				Autoscaling: &container.NodePoolAutoscaling{
					LocationPolicy: "abacaba",
				},
			},
			wantMetadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "abacaba",
			},
		},
		{
			name: "All self-service features are processed correctly",
			nodepool: &container.NodePool{
				Autoscaling: &container.NodePoolAutoscaling{
					LocationPolicy: "ANY",
				},
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.NodePoolGroupNameLabel:            "test-value-1",
						gkelabels.WorkloadTypeLabel:                 "test-value-2",
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
						gkelabels.DraNetNodeLabel:                   "true",
					},
					GcfsConfig: &container.GcfsConfig{Enabled: true},
					ResourceManagerTags: &container.ResourceManagerTags{
						Tags: map[string]string{
							"test-project/test-key-1": "test-value-1",
							"tagKeys/1234":            "tagValues/1234",
						},
					},
					ConfidentialNodes: &container.ConfidentialNodes{ConfidentialInstanceType: "test-value-4"},
				},
				PlacementPolicy: &container.PlacementPolicy{
					PolicyName: "test-value-3",
				},
				Management: &container.NodeManagement{
					AutoRepair:  false,
					AutoUpgrade: true,
				},
			},
			wantMetadata: Metadata{
				gkelabels.NodePoolGroupNameLabel:            "test-value-1",
				gkelabels.WorkloadTypeLabel:                 "test-value-2",
				gkelabels.GkeConfidentialNodeType:           "test-value-4",
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
				gkelabels.AutoRepairLabelKey:                "false",
				gkelabels.AutoUpgradeLabelKey:               "true",
				gkelabels.ImageStreamingLabelKey:            "true",
				gkelabels.LocationPolicyLabelKey:            "ANY",
				gkelabels.TagsLabelKey:                      "{\"tagKeys/1234\":\"tagValues/1234\",\"test-project/test-key-1\":\"test-value-1\"}",
				gkelabels.DraNetNodeLabel:                   "true",
			},
		},
		{
			name: "AcceleratorNetworkProfile label is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.AcceleratorNetworkProfileLabel: "auto",
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.AcceleratorNetworkProfileLabel: "auto",
			},
		},
		{
			name: "Nodepool with secure boot enabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{EnableSecureBoot: true},
				},
			},
			wantMetadata: Metadata{
				secureBootMetadataKey:          "true",
				integrityMonitoringMetadataKey: "false",
			},
		},
		{
			name: "Nodepool with integrity monitoring enabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{EnableIntegrityMonitoring: true},
				},
			},
			wantMetadata: Metadata{
				secureBootMetadataKey:          "false",
				integrityMonitoringMetadataKey: "true",
			},
		},
		{
			name: "Nodepool with secure boot and integrity monitoring disabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
						EnableSecureBoot:          false,
						EnableIntegrityMonitoring: false,
					},
				},
			},
			wantMetadata: Metadata{
				secureBootMetadataKey:          "false",
				integrityMonitoringMetadataKey: "false",
			},
		},
		{
			name: "Nodepool with secure boot and integrity monitoring enabled is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
						EnableSecureBoot:          true,
						EnableIntegrityMonitoring: true,
					},
				},
			},
			wantMetadata: Metadata{
				secureBootMetadataKey:          "true",
				integrityMonitoringMetadataKey: "true",
			},
		},
		{
			name: "InstanceMetadata is processed correctly",
			nodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Metadata: map[string]string{
						"foo": "bar",
					},
				},
			},
			wantMetadata: Metadata{
				"instance-metadata.cloud.google.com/foo": "bar",
			},
		},
		{
			name: "Nodepool with native EoS exclusion enabled is processed correctly",
			nodepool: &container.NodePool{
				MaintenancePolicy: &container.NodePoolMaintenancePolicy{
					ExclusionUntilEndOfSupport: &container.ExclusionUntilEndOfSupport{
						Enabled: true,
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.MaintenanceExclusionLabelKey: "UNTIL_END_OF_SUPPORT",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := NodepoolMetadata(tc.nodepool)
			assert.Equal(t, tc.wantMetadata, metadata)
		})
	}
}

func TestPodRequirementsMetadata(t *testing.T) {
	testCases := []struct {
		name         string
		req          podrequirements.LabelRequirements
		wantMetadata Metadata
	}{
		{
			name:         "Priority is empty",
			wantMetadata: make(Metadata),
		},
		{
			name: "GpuDirect requirement is processed correctly",
			req: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				gkelabels.GpuDirectLabel: podrequirements.NewValues("rdma"),
			}),
			wantMetadata: Metadata{
				gkelabels.GpuDirectLabel: "rdma",
			},
		},
		{
			name: "AcceleratorNetworkProfile requirement is processed correctly",
			req: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				gkelabels.AcceleratorNetworkProfileLabel: podrequirements.NewValues("auto"),
			}),
			wantMetadata: Metadata{
				gkelabels.AcceleratorNetworkProfileLabel: "auto",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := LabelRequirementsMetadata(tc.req)
			assert.Equal(t, tc.wantMetadata, metadata)
		})
	}
}

func TestComputeClassSpecMetadata(t *testing.T) {
	testCases := []struct {
		name         string
		spec         v1.ComputeClassSpec
		wantMetadata Metadata
	}{
		{
			name:         "Spec is empty",
			wantMetadata: make(Metadata),
		},
		{
			name: "WorkloadMetadata feature is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					WorkloadMetadata: ptr.To("GKE_METADATA"),
				},
			},
			wantMetadata: Metadata{
				gkelabels.WorkloadMetadataLabelKey: "GKE_METADATA",
			},
		},
		{
			name: "Nodepool group feature is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolGroup: &v1.NodePoolGroup{
					Name: "test-value",
				},
			},
			wantMetadata: Metadata{
				gkelabels.NodePoolGroupNameLabel: "test-value",
			},
		},
		{
			name: "Workload type feature is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					WorkloadType: "test-value",
				},
			},
			wantMetadata: Metadata{
				gkelabels.WorkloadTypeLabel: "test-value",
			},
		},
		{
			name: "Sandbox configuration feature is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					Sandbox: &v1.Sandbox{Type: "gvisor"},
				},
			},
			wantMetadata: Metadata{
				gkelabels.SandboxLabelKey: "gvisor",
			},
		},
		{
			name: "Sandbox configuration feature with microvm is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					Sandbox: &v1.Sandbox{Type: "microvm"},
				},
			},
			wantMetadata: Metadata{
				gkelabels.SandboxLabelKey: gkesandbox.MicroVMLabelValue,
			},
		},
		{
			name: "Sandbox configuration feature with uppercase type is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					Sandbox: &v1.Sandbox{Type: "GVISOR"},
				},
			},
			wantMetadata: Metadata{
				gkelabels.SandboxLabelKey: "gvisor",
			},
		},
		{
			name: "ConfidentialNodeType type feature is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					ConfidentialNodeType: "test-value",
				},
			},
			wantMetadata: Metadata{
				gkelabels.GkeConfidentialNodeType: "test-value",
			},
		},
		{
			name: "Auto repair is set to false",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					AutoRepair: ptr.To(false),
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoRepairLabelKey: "false",
			},
		},
		{
			name: "Auto repair is set to true",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					AutoRepair: ptr.To(true),
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoRepairLabelKey: "true",
			},
		},
		{
			name: "Auto upgrade is set to false",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					AutoUpgrade: ptr.To(false),
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoUpgradeLabelKey: "false",
			},
		},
		{
			name: "Auto upgrade is set to true",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					AutoUpgrade: ptr.To(true),
				},
			},
			wantMetadata: Metadata{
				gkelabels.AutoUpgradeLabelKey: "true",
			},
		},
		{
			name: "Image streaming is set to true",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					ImageStreaming: &v1.ImageStreaming{Enabled: true},
				},
			},
			wantMetadata: Metadata{
				gkelabels.ImageStreamingLabelKey: "true",
			},
		},
		{
			name: "Resource manager tags are processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					ResourceManagerTags: []v1.Tags{
						{
							Key:   "test-project/test-key",
							Value: "test-value",
						},
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.TagsLabelKey: "{\"test-project/test-key\":\"test-value\"}",
			},
		},
		{
			name: "Image streaming is set to false",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					ImageStreaming: &v1.ImageStreaming{Enabled: false},
				},
			},
			wantMetadata: Metadata{},
		},
		{
			name: "Gvnic is set to true",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					Gvnic: &v1.Gvnic{Enabled: true},
				},
			},
			wantMetadata: Metadata{
				gkelabels.GvnicLabelKey: "true",
			},
		},
		{
			name: "Gvnic is set to false",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					Gvnic: &v1.Gvnic{Enabled: false},
				},
			},
			wantMetadata: Metadata{
				gkelabels.GvnicLabelKey: "false",
			},
		},
		{
			name: "DRANET enabled",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					Dra: v1.Dra{
						Networking: v1.NetworkingDra{Enabled: true},
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.DraNetNodeLabel: "true",
			},
		},
		{
			name: "DRANET disabled",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					Dra: v1.Dra{
						Networking: v1.NetworkingDra{Enabled: false},
					},
				},
			},
			wantMetadata: Metadata{},
		},
		{
			name: "All self-service features are processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolGroup: &v1.NodePoolGroup{
					Name: "test-value-1",
				},
				NodePoolConfig: &v1.NodePoolConfig{
					WorkloadType:         "test-value-2",
					ConfidentialNodeType: "test-value-3",
					AutoRepair:           ptr.To(true),
					AutoUpgrade:          ptr.To(false),
					ImageStreaming:       &v1.ImageStreaming{Enabled: true},
					Gvnic:                &v1.Gvnic{Enabled: true},
					Sandbox:              &v1.Sandbox{Type: "gvisor"},
					Dra: v1.Dra{
						Networking: v1.NetworkingDra{Enabled: true},
					},
					ResourceManagerTags: []v1.Tags{
						{
							Key:   "test-project/test-key-1",
							Value: "test-value-1",
						},
						{
							Key:   "tagKeys/1234",
							Value: "tagValues/1234",
						},
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.NodePoolGroupNameLabel:  "test-value-1",
				gkelabels.WorkloadTypeLabel:       "test-value-2",
				gkelabels.GkeConfidentialNodeType: "test-value-3",
				gkelabels.AutoRepairLabelKey:      "true",
				gkelabels.AutoUpgradeLabelKey:     "false",
				gkelabels.ImageStreamingLabelKey:  "true",
				gkelabels.GvnicLabelKey:           "true",
				gkelabels.SandboxLabelKey:         "gvisor",
				gkelabels.TagsLabelKey:            "{\"tagKeys/1234\":\"tagValues/1234\",\"test-project/test-key-1\":\"test-value-1\"}",
				gkelabels.DraNetNodeLabel:         "true",
			},
		},
		{
			name: "Secure boot enabled",
			spec: v1.ComputeClassSpec{
				NodePoolAutoCreation: &v1.NodePoolAutoCreation{
					ShieldedInstanceConfig: &v1.ShieldedInstanceConfig{EnableSecureBoot: ptr.To(true)},
				},
			},
			wantMetadata: Metadata{
				secureBootMetadataKey: "true",
			},
		},
		{
			name: "Secure boot disabled",
			spec: v1.ComputeClassSpec{
				NodePoolAutoCreation: &v1.NodePoolAutoCreation{
					ShieldedInstanceConfig: &v1.ShieldedInstanceConfig{EnableSecureBoot: ptr.To(false)},
				},
			},
			wantMetadata: Metadata{
				secureBootMetadataKey: "false",
			},
		},
		{
			name: "Integrity monitoring enabled",
			spec: v1.ComputeClassSpec{
				NodePoolAutoCreation: &v1.NodePoolAutoCreation{
					ShieldedInstanceConfig: &v1.ShieldedInstanceConfig{EnableIntegrityMonitoring: ptr.To(true)},
				},
			},
			wantMetadata: Metadata{
				integrityMonitoringMetadataKey: "true",
			},
		},
		{
			name: "Integrity monitoring disabled",
			spec: v1.ComputeClassSpec{
				NodePoolAutoCreation: &v1.NodePoolAutoCreation{
					ShieldedInstanceConfig: &v1.ShieldedInstanceConfig{EnableIntegrityMonitoring: ptr.To(false)},
				},
			},
			wantMetadata: Metadata{
				integrityMonitoringMetadataKey: "false",
			},
		},
		{
			name: "Secure boot and integrity monitoring enabled",
			spec: v1.ComputeClassSpec{
				NodePoolAutoCreation: &v1.NodePoolAutoCreation{
					ShieldedInstanceConfig: &v1.ShieldedInstanceConfig{
						EnableSecureBoot:          ptr.To(true),
						EnableIntegrityMonitoring: ptr.To(true),
					},
				},
			},
			wantMetadata: Metadata{
				secureBootMetadataKey:          "true",
				integrityMonitoringMetadataKey: "true",
			},
		},
		{
			name: "InstanceMetadata specified",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					InstanceMetadata: map[string]string{
						"foo": "bar",
					},
				},
			},
			wantMetadata: Metadata{
				"instance-metadata.cloud.google.com/foo": "bar",
			},
		},
		{
			name: "MaintenanceExclusion in CCC is processed correctly",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					MaintenanceExclusion: ptr.To(v1.MaintenanceExclusionUntilEndOfSupport),
				},
			},
			wantMetadata: Metadata{
				gkelabels.MaintenanceExclusionLabelKey: "UNTIL_END_OF_SUPPORT",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := ComputeClassSpecMetadata(tc.spec)
			assert.Equal(t, tc.wantMetadata, metadata)
		})
	}
}

func TestPriorityMetadata(t *testing.T) {
	testCases := []struct {
		name         string
		priority     v1.Priority
		wantMetadata Metadata
	}{
		{
			name:         "Priority is empty",
			wantMetadata: make(Metadata),
		},
		{
			name: "Priority for GpuDirect",
			priority: v1.Priority{
				GpuDirect: "rdma",
			},
			wantMetadata: Metadata{
				gkelabels.GpuDirectLabel: "rdma",
			},
		},
		{
			name: "Priority for CapacityCheckWaitTimeSeconds",
			priority: v1.Priority{
				CapacityCheckWaitTimeSeconds: ptr.To(3600),
			},
			wantMetadata: Metadata{
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
		},
		{
			name: "Priority for all features",
			priority: v1.Priority{
				Placement: &v1.Placement{
					PolicyName: "test-value",
				},
				CapacityCheckWaitTimeSeconds: ptr.To(3600),
			},
			wantMetadata: Metadata{
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
		},
		{
			name: "Priority for LocationPolicy ANY",
			priority: v1.Priority{
				Location: &v1.Location{LocationPolicy: ptr.To("ANY")},
			},
			wantMetadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "ANY",
			},
		},
		{
			name: "Priority for LocationPolicy BALANCED",
			priority: v1.Priority{
				Location: &v1.Location{LocationPolicy: ptr.To("BALANCED")},
			},
			wantMetadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "BALANCED",
			},
		},
		{
			name: "Priority for LocationPolicy invalid",
			priority: v1.Priority{
				Location: &v1.Location{LocationPolicy: ptr.To("invalid")},
			},
			wantMetadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "invalid",
			},
		},
		{
			name: "Priority for empty LocationPolicy",
			priority: v1.Priority{
				Location: &v1.Location{LocationPolicy: ptr.To("")},
			},
			wantMetadata: Metadata{},
		},
		{
			name: "Priority for AcceleratorNetworkProfile",
			priority: v1.Priority{
				AcceleratorNetworkProfile: ptr.To("auto"),
			},
			wantMetadata: Metadata{
				gkelabels.AcceleratorNetworkProfileLabel: "auto",
			},
		},
		{
			name: "Priority for InstanceMetadata",
			priority: v1.Priority{
				InstanceMetadata: map[string]string{
					"priority-key": "priority-val",
				},
			},
			wantMetadata: Metadata{
				"instance-metadata.cloud.google.com/priority-key": "priority-val",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := PriorityMetadata(tc.priority)
			assert.Equal(t, tc.wantMetadata, metadata)
		})
	}
}

func TestUpdateNodePoolLabels(t *testing.T) {
	testCases := []struct {
		name       string
		metadata   Metadata
		wantLabels map[string]string
	}{
		{
			name:       "Metadata is empty",
			wantLabels: make(map[string]string),
		},
		{
			name: "GpuDirect label is processed correctly",
			metadata: Metadata{
				gkelabels.GpuDirectLabel: "rdma",
			},
			wantLabels: map[string]string{
				gkelabels.GpuDirectLabel: "rdma",
			},
		},
		{
			name: "Nodepool group label is processed correctly",
			metadata: Metadata{
				gkelabels.NodePoolGroupNameLabel: "test-value",
			},
			wantLabels: map[string]string{
				gkelabels.NodePoolGroupNameLabel: "test-value",
			},
		},
		{
			name: "Workload type label is processed correctly",
			metadata: Metadata{
				gkelabels.WorkloadTypeLabel: "test-value",
			},
			wantLabels: map[string]string{
				gkelabels.WorkloadTypeLabel: "test-value",
			},
		},
		{
			name: "ConfidentialNodeType type label is processed correctly",
			metadata: Metadata{
				gkelabels.GkeConfidentialNodeType: "test-value",
			},
			wantLabels: map[string]string{
				gkelabels.GkeConfidentialNodeType: "test-value",
			},
		},
		{
			name: "CapacityCheckWaitTimeSeconds label is processed correctly",
			metadata: Metadata{
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
			wantLabels: map[string]string{
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
		},
		{
			name: "DraNetNodeLabel label is processed correctly",
			metadata: Metadata{
				gkelabels.DraNetNodeLabel: "true",
			},
			wantLabels: map[string]string{
				gkelabels.DraNetNodeLabel: "true",
			},
		},
		{
			name: "All self-service features are processed correctly",
			metadata: Metadata{
				gkelabels.NodePoolGroupNameLabel:            "test-value-1",
				gkelabels.WorkloadTypeLabel:                 "test-value-2",
				gkelabels.GkeConfidentialNodeType:           "test-value-4",
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
			wantLabels: map[string]string{
				gkelabels.NodePoolGroupNameLabel:            "test-value-1",
				gkelabels.WorkloadTypeLabel:                 "test-value-2",
				gkelabels.GkeConfidentialNodeType:           "test-value-4",
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
		},
		{
			name: "AcceleratorNetworkProfile label is processed correctly",
			metadata: Metadata{
				gkelabels.AcceleratorNetworkProfileLabel: "auto",
			},
			wantLabels: map[string]string{
				gkelabels.AcceleratorNetworkProfileLabel: "auto",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			labels := make(map[string]string)
			UpdateNodePoolLabels(labels, tc.metadata)
			assert.Equal(t, tc.wantLabels, labels)
		})
	}
}

func TestUpdateNodepool(t *testing.T) {
	testCases := []struct {
		name         string
		metadata     Metadata
		wantNodepool *container.NodePool
	}{
		{
			name:         "Metadata is empty",
			wantNodepool: &container.NodePool{},
		},
		{
			name: "GpuDirect strategy is processed correctly",
			metadata: Metadata{
				gkelabels.GpuDirectLabel: "rdma",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					GpuDirectConfig: &container.GPUDirectConfig{
						GpuDirectStrategy: "RDMA",
					},
				},
			},
		},
		{
			name: "Nodepool group label is processed correctly",
			metadata: Metadata{
				gkelabels.NodePoolGroupNameLabel: "test-value",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.NodePoolGroupNameLabel: "test-value",
					},
				},
			},
		},
		{
			name: "Workload type label is processed correctly",
			metadata: Metadata{
				gkelabels.WorkloadTypeLabel: "test-value",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.WorkloadTypeLabel: "test-value",
					},
				},
			},
		},
		{
			name: "ConfidentialNodeType type label is processed correctly",
			metadata: Metadata{
				gkelabels.GkeConfidentialNodeType: "test-value",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ConfidentialNodes: &container.ConfidentialNodes{ConfidentialInstanceType: "test-value"},
					Labels: map[string]string{
						gkelabels.GkeConfidentialNodeType: "test-value",
					},
				},
			},
		},
		{
			name: "CapacityCheckWaitTimeSeconds label is processed correctly",
			metadata: Metadata{
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
					},
				},
			},
		},
		{
			name: "ImageStreaming label is processed correctly",
			metadata: Metadata{
				gkelabels.ImageStreamingLabelKey: "true",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					GcfsConfig: &container.GcfsConfig{Enabled: true},
				},
			},
		},
		{
			name: "LocationPolicy label is processed correctly",
			metadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "BALANCED",
			},
			wantNodepool: &container.NodePool{
				Autoscaling: &container.NodePoolAutoscaling{
					LocationPolicy: "BALANCED",
				},
			},
		},
		{
			name: "LocationPolicy label with invalid value is processed correctly",
			metadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "invalid",
			},
			wantNodepool: &container.NodePool{
				Autoscaling: &container.NodePoolAutoscaling{
					LocationPolicy: "invalid",
				},
			},
		},
		{
			name: "LocationPolicy label with empty value is processed correctly",
			metadata: Metadata{
				gkelabels.LocationPolicyLabelKey: "",
			},
			wantNodepool: &container.NodePool{},
		},
		{
			name: "LoggingConfigVariant label with valid value is processed correctly",
			metadata: Metadata{
				gkelabels.LoggingConfigVariant: "MAX_THROUGHPUT",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					LoggingConfig: &container.NodePoolLoggingConfig{
						VariantConfig: &container.LoggingVariantConfig{
							Variant: "MAX_THROUGHPUT",
						},
					},
				},
			},
		},
		{
			name: "LoggingConfigVariant label with empty value is processed correctly",
			metadata: Metadata{
				gkelabels.LoggingConfigVariant: "",
			},
			wantNodepool: &container.NodePool{},
		},
		{
			name: "Sandbox label is processed correctly",
			metadata: Metadata{
				gkelabels.SandboxLabelKey: "gvisor",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					SandboxConfig: &container.SandboxConfig{Type: "gvisor"},
				},
			},
		},
		{
			name: "Sandbox microvm label is processed correctly",
			metadata: Metadata{
				gkelabels.SandboxLabelKey: gkesandbox.MicroVMLabelValue,
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					SandboxConfig: &container.SandboxConfig{Type: "microvm"},
				},
			},
		},

		{
			name: "All self-service features are processed correctly",
			metadata: Metadata{
				gkelabels.NodePoolGroupNameLabel:            "test-value-1",
				gkelabels.WorkloadTypeLabel:                 "test-value-2",
				gkelabels.GkeConfidentialNodeType:           "test-value-4",
				gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
				gkelabels.TagsLabelKey:                      "{\"tagKeys/1234\":\"tagValues/1234\",\"test-project/test-key-1\":\"test-value-1\"}",
				gkelabels.LocationPolicyLabelKey:            "ANY",
				gkelabels.LoggingConfigVariant:              "MAX_THROUGHPUT",
				gkelabels.SandboxLabelKey:                   "gvisor",
			},
			wantNodepool: &container.NodePool{
				Autoscaling: &container.NodePoolAutoscaling{
					LocationPolicy: "ANY",
				},
				Config: &container.NodeConfig{
					ConfidentialNodes: &container.ConfidentialNodes{ConfidentialInstanceType: "test-value-4"},
					Labels: map[string]string{
						gkelabels.NodePoolGroupNameLabel:            "test-value-1",
						gkelabels.WorkloadTypeLabel:                 "test-value-2",
						gkelabels.GkeConfidentialNodeType:           "test-value-4",
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600",
					},
					ResourceManagerTags: &container.ResourceManagerTags{
						Tags: map[string]string{
							"test-project/test-key-1": "test-value-1",
							"tagKeys/1234":            "tagValues/1234",
						},
					},
					LoggingConfig: &container.NodePoolLoggingConfig{
						VariantConfig: &container.LoggingVariantConfig{
							Variant: "MAX_THROUGHPUT",
						},
					},
					SandboxConfig: &container.SandboxConfig{Type: "gvisor"},
				},
			},
		},
		{
			name: "AcceleratorNetworkProfile label is processed correctly",
			metadata: Metadata{
				gkelabels.AcceleratorNetworkProfileLabel: "auto",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Labels: map[string]string{
						gkelabels.AcceleratorNetworkProfileLabel: "auto",
					},
				},
			},
		},
		{
			name: "Secure boot is enabled",
			metadata: Metadata{
				secureBootMetadataKey: "true",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
						EnableSecureBoot: true,
						ForceSendFields:  []string{"EnableSecureBoot"},
					},
				},
			},
		},
		{
			name: "Secure boot is disabled",
			metadata: Metadata{
				secureBootMetadataKey: "false",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
						EnableSecureBoot: false,
						ForceSendFields:  []string{"EnableSecureBoot"},
					},
				},
			},
		},
		{
			name: "Integrity monitoring is enabled",
			metadata: Metadata{
				integrityMonitoringMetadataKey: "true",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
						EnableIntegrityMonitoring: true,
						ForceSendFields:           []string{"EnableIntegrityMonitoring"},
					},
				},
			},
		},
		{
			name: "Integrity monitoring is disabled",
			metadata: Metadata{
				integrityMonitoringMetadataKey: "false",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
						EnableIntegrityMonitoring: false,
						ForceSendFields:           []string{"EnableIntegrityMonitoring"},
					},
				},
			},
		},
		{
			name: "Secure boot and integrity monitoring are enabled",
			metadata: Metadata{
				secureBootMetadataKey:          "true",
				integrityMonitoringMetadataKey: "true",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
						EnableSecureBoot:          true,
						EnableIntegrityMonitoring: true,
						ForceSendFields:           []string{"EnableSecureBoot", "EnableIntegrityMonitoring"},
					},
				},
			},
		},
		{
			name: "InstanceMetadata is processed correctly",
			metadata: Metadata{
				"instance-metadata.cloud.google.com/global-key": "global-val",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Metadata: map[string]string{
						"global-key": "global-val",
					},
				},
			},
		},
		{
			name: "InstanceMetadata multiple keys",
			metadata: Metadata{
				"instance-metadata.cloud.google.com/global-key":   "global-val",
				"instance-metadata.cloud.google.com/priority-key": "priority-val",
				"instance-metadata.cloud.google.com/shared-key":   "priority-shared",
			},
			wantNodepool: &container.NodePool{
				Config: &container.NodeConfig{
					Metadata: map[string]string{
						"global-key":   "global-val",
						"priority-key": "priority-val",
						"shared-key":   "priority-shared",
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodepool := &container.NodePool{}
			UpdateNodepool(nodepool, tc.metadata)
			assert.Equal(t, tc.wantNodepool, nodepool)
		})
	}
}
