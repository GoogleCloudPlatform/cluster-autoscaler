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

package flexadvisor

import (
	"fmt"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gceprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
)

// Mock NodeGroup for non-GKE test case
type mockOtherNodeGroup struct {
	cloudprovider.NodeGroup
}

func (m *mockOtherNodeGroup) Id() string { return "mock-other-ng" }

const EmptyTpuType = ""
const EmptyTpuTopology = ""

func newTestMig(zone, machineType string, labels map[string]string, spot, flexStart bool, accelerators []*container.AcceleratorConfig, tpuType, tpuTopology, maxRunDurationInSeconds string) *gke.GkeMig {
	fakeGke := gke.NewFakeGkeManagerBuilder().WithMigTemplateNode(buildNodeWithLabels(labels)).Build()
	return gke.NewTestGkeMigBuilder().SetGceRef(gceprovider.GceRef{Zone: zone}).SetGkeManager(fakeGke).SetSpec(&gkeclient.NodePoolSpec{
		MachineType:             machineType,
		Labels:                  labels,
		Accelerators:            accelerators,
		Spot:                    spot,
		FlexStart:               flexStart,
		TpuType:                 tpuType,
		TpuTopology:             tpuTopology,
		MaxRunDurationInSeconds: maxRunDurationInSeconds,
	}).Build()
}

func newTestMigWithNilSpec(zone string, labels map[string]string) *gke.GkeMig {
	fakeGke := gke.NewFakeGkeManagerBuilder().WithMigTemplateNode(buildNodeWithLabels(labels)).Build()
	return gke.NewTestGkeMigBuilder().SetGceRef(gceprovider.GceRef{Zone: zone}).SetGkeManager(fakeGke).Build()
}

func buildNodeWithLabels(labels map[string]string) *apiv1.Node {
	node := apiv1.Node{}
	node.Labels = labels
	return &node
}

func TestConstructInstanceReference(t *testing.T) {
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scale-out",
		},
	}, "", false, nil, nil)
	crd2 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-pool",
		},
	}, "", false, nil, nil)

	tests := []struct {
		name               string
		nodeGroup          cloudprovider.NodeGroup
		experimentsManager experiments.Manager
		want               *InstanceReference
		wantErr            error
	}{
		{
			name:      "Success - Standard VM",
			nodeGroup: newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: e2-standard-4, provisioningMode: STANDARD",
			},
			wantErr: nil,
		},
		{
			name:      "Success - Spot VM with GPU",
			nodeGroup: newTestMig("us-central1-b", "n1-standard-8", map[string]string{labels.ComputeClassLabel: "gpu-pool"}, true, false, []*container.AcceleratorConfig{{AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 2}}, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			want: &InstanceReference{
				Zone:                "us-central1-b",
				FlexibilityScopeKey: "gpu-pool",
				InstanceConfigKey:   "machineType: n1-standard-8, provisioningMode: SPOT, gpuType: nvidia-tesla-t4, gpuCount: 2",
			},
			wantErr: nil,
		},
		{
			name:      "Error - Incorrect NodeGroup type",
			nodeGroup: &mockOtherNodeGroup{},
			want:      nil,
			wantErr:   fmt.Errorf("unexpected cloudprovider.NodeGroup type, got: *flexadvisor.mockOtherNodeGroup, want: gke.NodeGroup"),
		},
		{
			name:      "Error - Missing ComputeClassLabel",
			nodeGroup: newTestMig("us-central1-b", "n1-standard-8", map[string]string{}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			want:      nil,
			wantErr:   fmt.Errorf("ccc label/flexibility scope key not found in the nodeGroup"),
		},
		{
			name:      "Error - Missing Zone",
			nodeGroup: newTestMig("", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			want:      nil,
			wantErr:   fmt.Errorf("zone not found in nodeGroup"),
		},
		{
			name:      "TPU - FlexAdvisorTPU disabled explicitly - throws",
			nodeGroup: newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, "tpu7x", EmptyTpuTopology, api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					experiments.FlexAdvisorTPUEnabledFlag:      false,
					experiments.FlexAdvisorTPUMinCAVersionFlag: true,
				},
				map[string]string{},
			),
			want:    nil,
			wantErr: fmt.Errorf("tpu node pools are not supported by Flex Advisor"),
		},
		{
			name:               "TPU - FlexAdvisorTPU enabled but version flag off - throws",
			nodeGroup:          newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, "tpu7x", EmptyTpuTopology, api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorTPUEnabledFlag),
			want:               nil,
			wantErr:            fmt.Errorf("tpu node pools are not supported by Flex Advisor"),
		},
		{
			name:               "TPU - FlexAdvisorTPU enabled, empty topology - returns instance config without topology",
			nodeGroup:          newTestMig("us-central1-a", "ct5p-hightpu-4t", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, "tpu-v5p-slice", EmptyTpuTopology, api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: ct5p-hightpu-4t, provisioningMode: STANDARD",
			},
			wantErr: nil,
		},
		{
			name:               "TPU - FlexAdvisorTPU enabled, topology is present - returns instance config with topology",
			nodeGroup:          newTestMig("us-central1-a", "ct5p-hightpu-4t", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, "tpu-v5p-slice", "2x2x5", api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: ct5p-hightpu-4t, provisioningMode: STANDARD, acceleratorTopology: 2x2x5",
			},
			wantErr: nil,
		},
		{
			name:               "TPU with accelerators - FlexAdvisorTPU enabled, topology is present - returns instance config with topology but without GPU fields",
			nodeGroup:          newTestMig("us-central1-a", "ct5p-hightpu-4t", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, []*container.AcceleratorConfig{{AcceleratorType: "tpu-v5p-slice", AcceleratorCount: 4}}, "tpu-v5p-slice", "2x2x5", api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: ct5p-hightpu-4t, provisioningMode: STANDARD, acceleratorTopology: 2x2x5",
			},
			wantErr: nil,
		},
		{
			name:               "DWS TPU - FlexAdvisorTPU and FlexAdvisorDWS enabled, topology is present - returns instance config with topology",
			nodeGroup:          newTestMig("us-central1-a", "ct5p-hightpu-4t", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, true, nil, "tpu-v5p-slice", "2x2x5", "3600"),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag, experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: ct5p-hightpu-4t, provisioningMode: FLEX_START, maxRunDuration: 3600, acceleratorTopology: 2x2x5",
			},
			wantErr: nil,
		},
		{
			name:      "DWS - no experiments set - throws",
			nodeGroup: newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, true, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			want:      nil,
			wantErr:   fmt.Errorf("flex start node pools are not supported by Flex Advisor"),
		},
		{
			name:      "DWS - FlexAdvisorDWS disabled explicitly - throws",
			nodeGroup: newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, true, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					experiments.FlexAdvisorDWSEnabledFlag:      false,
					experiments.FlexAdvisorDWSMinCAVersionFlag: true,
				},
				map[string]string{},
			),
			want:    nil,
			wantErr: fmt.Errorf("flex start node pools are not supported by Flex Advisor"),
		},
		{
			name:               "DWS - FlexAdvisorDWS enabled but version flag off - throws",
			nodeGroup:          newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, true, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorDWSEnabledFlag),
			want:               nil,
			wantErr:            fmt.Errorf("flex start node pools are not supported by Flex Advisor"),
		},
		{
			name:               "DWS - FlexAdvisorDWS enabled - uses default MRD",
			nodeGroup:          newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, true, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: e2-standard-4, provisioningMode: FLEX_START, maxRunDuration: 604800",
			},
			wantErr: nil,
		},
		{
			name:               "DWS - FlexAdvisorDWS enabled - uses supplied MRD",
			nodeGroup:          newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, true, nil, EmptyTpuType, EmptyTpuTopology, "3600"),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: e2-standard-4, provisioningMode: FLEX_START, maxRunDuration: 3600",
			},
			wantErr: nil,
		},
		{
			name:               "non-DWS spec with MRD specified - FlexAdvisorDWS experiment off - doesn't use specified MRD",
			nodeGroup:          newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, "2000"),
			experimentsManager: experiments.NewMockManager(),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: e2-standard-4, provisioningMode: STANDARD",
			},
			wantErr: nil,
		},
		{
			name:               "non-DWS spec with MRD specified - FlexAdvisorDWS enabled - uses supplied MRD",
			nodeGroup:          newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scale-out"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, "2000"),
			experimentsManager: experiments.NewMockManager(experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag),
			want: &InstanceReference{
				Zone:                "us-central1-a",
				FlexibilityScopeKey: "scale-out",
				InstanceConfigKey:   "machineType: e2-standard-4, provisioningMode: STANDARD, maxRunDuration: 2000",
			},
			wantErr: nil,
		},
		{
			name:               "Error - nil spec",
			nodeGroup:          newTestMigWithNilSpec("us-central1-a", map[string]string{labels.ComputeClassLabel: "scale-out"}),
			experimentsManager: experiments.NewMockManager(),
			want:               nil,
			wantErr:            fmt.Errorf("nodeGroup spec is nil for nodeGroup"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crd1, crd2}, labels.ComputeClassLabel)
			manager := tc.experimentsManager
			if manager == nil {
				manager = experiments.NewMockManager()
			}
			got, err := ConstructInstanceReference(tc.nodeGroup, mockLister, manager)

			if tc.wantErr == nil {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, got)
			} else {
				assert.Contains(t, err.Error(), tc.wantErr.Error())
				assert.Nil(t, got)
			}
		})
	}
}
