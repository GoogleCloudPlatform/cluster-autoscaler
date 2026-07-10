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
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/utils/ptr"
	"k8s.io/utils/set"
)

func WithMaxInstanceConfigs(max int) generatorOption {
	return func(g *instanceConfigGenerator) {
		g.maxInstanceConfigs = max
	}
}

type mockInstanceConfigCloudProvider struct {
	autoprovisioningLocations []string
	gkeMigs                   []*gke.GkeMig
	defaultMachineFamily      machinetypes.MachineFamily
	availableByDefault        bool
	machineTypes              map[string]set.Set[string]
	autopilotEnabled          bool
	getMachineTypeCallsQty    int
}

func (m *mockInstanceConfigCloudProvider) GetAutoprovisioningLocations() []string {
	return m.autoprovisioningLocations
}

func (m *mockInstanceConfigCloudProvider) GetGkeMigs() []*gke.GkeMig {
	return m.gkeMigs
}

func (m *mockInstanceConfigCloudProvider) ExistingMigsInNodePool(nodePoolName string) []*gke.GkeMig {
	var result []*gke.GkeMig
	for _, mig := range m.gkeMigs {
		if mig.NodePoolName() == nodePoolName {
			result = append(result, mig)
		}
	}
	return result
}

func (m *mockInstanceConfigCloudProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	return m.defaultMachineFamily
}

func (m *mockInstanceConfigCloudProvider) GetMachineType(machineType string, zone string) (gce.MachineType, error) {

	m.getMachineTypeCallsQty++
	if m.availableByDefault {
		return gce.MachineType{}, nil
	}

	zones, found := m.machineTypes[machineType]
	if !found {
		return gce.MachineType{}, fmt.Errorf("machine type: %s not found in zone: %s", machineType, zone)
	}
	if zones.HasAny(zone) {
		return gce.MachineType{}, nil
	}
	return gce.MachineType{}, fmt.Errorf("machine type: %s not found in zone: %s", machineType, zone)
}

func (m *mockInstanceConfigCloudProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

func (m *mockInstanceConfigCloudProvider) GetClusterInfo() (projectId, location, clusterName string) {
	return "project1", "us-central1", "cluster1"
}

func (m *mockInstanceConfigCloudProvider) IsAutopilotEnabled() bool {
	return m.autopilotEnabled
}

func (m *mockInstanceConfigCloudProvider) GetAIZones() ([]string, error) {
	panic("not implemented")
}
func (m *mockInstanceConfigCloudProvider) GetStandardZones() ([]string, error) {
	panic("not implemented")
}
func (m *mockInstanceConfigCloudProvider) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	panic("not implemented")
}

type mockProviderOption func(*mockInstanceConfigCloudProvider)

func withAutopilotEnabled(enabled bool) mockProviderOption {
	return func(m *mockInstanceConfigCloudProvider) {
		m.autopilotEnabled = enabled
	}
}

func newMockInstanceConfigCloudProvider(autoprovisioningLocations []string, gkeMigs []*gke.GkeMig, defaultMachineFamily machinetypes.MachineFamily, availableByDefault bool, machineTypes map[string]set.Set[string], opts ...mockProviderOption) *mockInstanceConfigCloudProvider {
	m := &mockInstanceConfigCloudProvider{
		autoprovisioningLocations: autoprovisioningLocations,
		gkeMigs:                   gkeMigs,
		defaultMachineFamily:      defaultMachineFamily,
		availableByDefault:        availableByDefault,
		machineTypes:              machineTypes,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func TestGenerateInstanceConfigs(t *testing.T) {
	// register custom PCC for testing
	machinetypes.RegisterComputeClass(
		machinetypes.NewTestPredefinedComputeClass(
			"test-pcc",
			[]machinetypes.MachineFamily{machinetypes.A4},
			false,
			false,
		),
	)

	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "crd1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					// default machine
					MachineType: ptr.To("e2-standard-4"),
				},
				{
					// non default
					MachineType: ptr.To("e4a-standard-8"),
				},
				{
					// gpu machine
					MachineType: ptr.To("g2-standard-12"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	crd2 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "crd2",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					Nodepools: []string{"node-pool1", "node-pool2"},
				},
				{
					Gpu: &v1.GPU{
						Type:  "nvidia-b200",
						Count: 8,
					},
					Spot: ptr.To(true),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	crd3 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "crd3",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("machine-type-unknown"),
				},
				{
					MachineType: ptr.To("g2-standard-12"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	cccMrd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-mrd-1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-2"),
				},
				{
					MachineType:           ptr.To("e2-standard-2"),
					MaxRunDurationSeconds: ptr.To(int(3600)),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	cccDws1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-dws-1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-2"),
					FlexStart:   &v1.FlexStart{Enabled: true},
				},
				{
					MachineType:           ptr.To("e2-standard-2"),
					MaxRunDurationSeconds: ptr.To(int(3600)),
					FlexStart:             &v1.FlexStart{Enabled: true},
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	cccTpu1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-tpu-1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					Tpu: &v1.TPU{
						Type:     gkelabels.TpuV4LiteDeviceValue,
						Topology: "2x2x2",
					},
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	cccTpuWithDws1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-tpu-2",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					Tpu: &v1.TPU{
						Type:     gkelabels.TpuV4LiteDeviceValue,
						Topology: "2x2x2",
					},
				},
				{
					MachineType: ptr.To("tpu7x-standard-1t"),
					FlexStart: &v1.FlexStart{
						Enabled: true,
					},
					Tpu: &v1.TPU{
						Type:     gkelabels.Tpu7xValue,
						Topology: "2x2x2",
					},
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	mig1 := gke.NewTestGkeMigBuilder().SetNodePoolName("node-pool1").SetSpec(&gkeclient.NodePoolSpec{Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"}, MachineType: "n2-standard-2", Spot: true}).Build()
	mig2 := gke.NewTestGkeMigBuilder().SetNodePoolName("node-pool2").SetSpec(&gkeclient.NodePoolSpec{Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"}, MachineType: "a2-highgpu-1g", Spot: false, Accelerators: []*container.AcceleratorConfig{{AcceleratorType: "nvidia-tesla-a100", AcceleratorCount: 1}}}).Build()

	testCases := map[string]struct {
		flexibilityScopeKey string
		crd                 crd.CRD
		wantInstanceConfigs map[string]*api.InstanceConfig
		wantErrs            []error
		enabledFeatures     []string
		disabledFeatures    []string
		autopilotEnabled    bool
	}{
		"machine type rules": {
			flexibilityScopeKey: "crd1",
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("e2-standard-4", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-4", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e4a-standard-8", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e4a-standard-8", "", 0, 2, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("g2-standard-12", "nvidia-l4", 1, 3, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("g2-standard-12", "nvidia-l4", 1, 3, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
			wantErrs: []error{},
		},
		"1st rule: specifies node pools, 2nd rule: specifies gpu config": {
			flexibilityScopeKey: "crd2",
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-c")),
				api.NewInstanceConfigWithZones("a2-highgpu-1g", "nvidia-tesla-a100", 1, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-c")),
				api.NewInstanceConfigWithZones("a4-highgpu-8g", "nvidia-b200", 8, 2, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("a4-highgpu-8g-nolssd", "nvidia-b200", 8, 2, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
			wantErrs: []error{},
		},
		"crd not found": {
			flexibilityScopeKey: "crd-unknown",
			wantInstanceConfigs: map[string]*api.InstanceConfig{},
			wantErrs:            []error{fmt.Errorf("failed to get CRD for flexibilityScopeKey \"crd-unknown\": %w", fmt.Errorf("crd doesnt exist"))},
		},
		"1st rule: specifies an invalid machine type, 2nd rule: specifies a valid machine type": {
			flexibilityScopeKey: "crd3",
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("g2-standard-12", "nvidia-l4", 1, 2, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("g2-standard-12", "nvidia-l4", 1, 2, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
			wantErrs: []error{fmt.Errorf("error when generating instance configs for flexibilityScopeKey: crd3 err: machine type not found for machine-type-unknown")},
		},
		"DWS - FlexAdvisorDWS disabled - non-DWS MRD rules - generates configs without MRD": {
			flexibilityScopeKey: "ccc-mrd-1",
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
			wantErrs: []error{},
		},
		"DWS - FlexAdvisorDWS disabled, DWS rule - doesn't create configs": {
			flexibilityScopeKey: "ccc-dws-1",
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{}),
			wantErrs:            []error{},
		},
		"DWS - FlexAdvisorDWS enabled, non-DWS MRD rules - generated configs have MRD": {
			flexibilityScopeKey: "ccc-mrd-1",
			enabledFeatures:     []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Spot, "", set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Standard, "", set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Spot, "3600", set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.Standard, "3600", set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
			wantErrs: []error{},
		},
		"DWS - FlexAdvisorDWS enabled, DWS rules - generated configs have MRD": {
			flexibilityScopeKey: "ccc-dws-1",
			enabledFeatures:     []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.FlexStart, "604800", set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("e2-standard-2", "", 0, 1, instanceavailability.FlexStart, "3600", set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
			wantErrs: []error{},
		},
		"TPU - FlexAdvisorTPU disabled - TPU rules - doesnt include tpu fields": {
			flexibilityScopeKey: "ccc-tpu-1",
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{}),
			wantErrs:            []error{fmt.Errorf("error when generating instance configs for flexibilityScopeKey: ccc-tpu-1 err: tpu rules are not supported")},
		},
		"TPU - FlexAdvisorTPU enabled but version flag off - TPU rules - doesnt include tpu fields": {
			flexibilityScopeKey: "ccc-tpu-1",
			enabledFeatures:     []string{experiments.FlexAdvisorTPUEnabledFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{}),
			wantErrs:            []error{fmt.Errorf("error when generating instance configs for flexibilityScopeKey: ccc-tpu-1 err: tpu rules are not supported")},
		},
		"TPU - FlexAdvisorTPU enabled - TPU & DWS rules - generates TPU rules, skips DWS rules": {
			flexibilityScopeKey: "ccc-tpu-2",
			enabledFeatures:     []string{experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("ct4l-hightpu-4t", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
				api.NewInstanceConfigWithZones("ct4l-hightpu-4t", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
			}),
		},
		"TPU -  FlexAdvisorDWS & FlexAdvisorTPU enabled  - TPU & DWS rules - generates DWS & TPU rules": {
			flexibilityScopeKey: "ccc-tpu-2",
			enabledFeatures:     []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag, experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("ct4l-hightpu-4t", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
				api.NewInstanceConfigWithZones("ct4l-hightpu-4t", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
				api.NewInstanceConfigWithZones("tpu7x-standard-1t", "", 0, 6, instanceavailability.FlexStart, "604800", set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
			}),
		},
		"TPU - FlexAdvisorTPU enabled - tpuType set - generates tpu configs for all matching machine types": {
			flexibilityScopeKey: "test-ccc",
			crd: ccc.NewCccCrd(&v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ccc",
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							Tpu: &v1.TPU{
								Type: gkelabels.TpuV5LitePodsliceValue,
							},
						},
						{
							Tpu: &v1.TPU{
								Type: gkelabels.TpuV5LitePodsliceValue,
								// tpuType can match multiple machine types, count limits it to ones with matching count
								Count: 4,
							},
						},
						{
							Tpu: &v1.TPU{
								Type: gkelabels.TpuV5LitePodsliceValue,
								// tpuType can match multiple machine types, count limits it to ones with matching count
								Count:    8,
								Topology: "2x2x2",
							},
						},
					},
				},
			}, "", false, crd.TestDefaultDataProvider(), nil),
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag, experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("ct5lp-hightpu-1t", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("ct5lp-hightpu-1t", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("ct5lp-hightpu-4t", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("ct5lp-hightpu-4t", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("ct5lp-hightpu-8t", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("ct5lp-hightpu-8t", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),

				api.NewInstanceConfigWithZones("ct5lp-hightpu-4t", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("ct5lp-hightpu-4t", "", 0, 2, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),

				api.NewInstanceConfigWithZones("ct5lp-hightpu-8t", "", 0, 3, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
				api.NewInstanceConfigWithZones("ct5lp-hightpu-8t", "", 0, 3, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
			}),
		},
		"TPU - FlexAdvisorTPU enabled - tpuType & tpuTopology set - generates tpu configs": {
			flexibilityScopeKey: "test-ccc",
			crd: ccc.NewCccCrd(&v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ccc",
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							Tpu: &v1.TPU{
								Type:     gkelabels.TpuV4LiteDeviceValue,
								Topology: "2x2x2",
							},
						},
					},
				},
			}, "", false, crd.TestDefaultDataProvider(), nil),
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag, experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("ct4l-hightpu-4t", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
				api.NewInstanceConfigWithZones("ct4l-hightpu-4t", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
			}),
		},
		"TPU - FlexAdvisorTPU enabled - machineType & tpuType & tpuTopology set - generates tpu configs with topology": {
			flexibilityScopeKey: "test-ccc",
			crd: ccc.NewCccCrd(&v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ccc",
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: ptr.To("tpu7x-standard-1t"),
							Tpu: &v1.TPU{
								Type:     gkelabels.Tpu7xValue,
								Topology: "2x2x2",
							},
						},
					},
				},
			}, "", false, crd.TestDefaultDataProvider(), nil),
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag, experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("tpu7x-standard-1t", "", 0, 3, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
				api.NewInstanceConfigWithZones("tpu7x-standard-1t", "", 0, 3, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c"), api.WithWorkloadPolicies(api.WorkloadPolicies{AcceleratorTopology: "2x2x2"})),
			}),
		},
		"TPU - FlexAdvisorTPU enabled - tpu machineType set - generates machineType config, without topology": {
			flexibilityScopeKey: "test-ccc",
			crd: ccc.NewCccCrd(&v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ccc",
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: ptr.To("tpu7x-standard-1t"),
						},
					},
				},
			}, "", false, crd.TestDefaultDataProvider(), nil),
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag, experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("tpu7x-standard-1t", "", 0, 3, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("tpu7x-standard-1t", "", 0, 3, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
		},
		"PCC - FlexAdvisorPCC disabled - doesnt generate configs": {
			flexibilityScopeKey: "test-pcc",
			disabledFeatures: []string{
				experiments.FlexAdvisorPCCSupportEnabledFlag,
			},
			wantErrs: []error{fmt.Errorf("predefined compute class \"test-pcc\" is not supported: FlexAdvisorPCCSupport is disabled")},
		},
		"PCC - FlexAdvisorPCC enabled, standard cluster - doesnt generate configs": {
			flexibilityScopeKey: "test-pcc",
			wantErrs:            []error{fmt.Errorf("predefined compute classes are only available in Autopilot clusters")},
		},
		"PCC - FlexAdvisorPCC enabled, autopilot cluster - generates configs": {
			flexibilityScopeKey: "test-pcc",
			autopilotEnabled:    true,
			wantInstanceConfigs: testInstanceConfigMap([]*api.InstanceConfig{
				api.NewInstanceConfigWithZones("a4-highgpu-8g", "nvidia-b200", 8, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("a4-highgpu-8g", "nvidia-b200", 8, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("a4-highgpu-8g-nolssd", "nvidia-b200", 8, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("a4-highgpu-8g-nolssd", "nvidia-b200", 8, 1, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New[string]("us-west1-a", "us-west1-b", "us-west1-c")),
			}),
			wantErrs: []error{},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			availableCrds := []crd.CRD{
				crd1,
				crd2,
				crd3,
				cccMrd1,
				cccDws1,
				cccTpu1,
				cccTpuWithDws1,
			}
			if tc.crd != nil {
				availableCrds = append(availableCrds, tc.crd)
			}
			boolFlags := map[string]bool{}
			for _, f := range tc.enabledFeatures {
				boolFlags[f] = true
			}
			for _, f := range tc.disabledFeatures {
				boolFlags[f] = false
			}
			optionsTracker := optstracking.FakeOptionsTracker(
				options.AutoscalingOptions{},
				gkeclient.Cluster{},
				experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{}),
			)
			provider := newMockInstanceConfigCloudProvider(
				[]string{"us-west1-a", "us-west1-b", "us-west1-c"},
				[]*gke.GkeMig{mig1, mig2},
				machinetypes.E2,
				true,
				nil,
				withAutopilotEnabled(tc.autopilotEnabled),
			)
			g := NewInstanceConfigGenerator(context.Background(), lister.NewMockCrdLister(availableCrds), provider, optionsTracker)
			generated, errs := g.generateInstanceConfigs(tc.flexibilityScopeKey)
			var configs map[string]*api.InstanceConfig
			if generated != nil {
				configs = generated.Configs
			}
			compareInstanceConfigMaps(t, tc.wantInstanceConfigs, configs)
			assert.ElementsMatch(t, tc.wantErrs, errs)
		})
	}
}

func testInstanceConfigMap(configs []*api.InstanceConfig) map[string]*api.InstanceConfig {
	configMap := make(map[string]*api.InstanceConfig)
	for _, config := range configs {
		configMap[config.Signature()] = config
	}
	return configMap
}

func TestDeriveMachineConfigsFromRule(t *testing.T) {
	provider := newMockInstanceConfigCloudProvider(nil, nil, machinetypes.A4X, true, nil)
	rank := 1
	testCases := map[string]struct {
		rule                rules.Rule
		wantInstanceConfigs []*api.InstanceConfig
		wantErr             error
	}{
		"rule specifies machine type": {
			rule:                rules.NewRule(rules.WithMachineTypeRule(ptr.To("a4-highgpu-8g"))),
			wantInstanceConfigs: []*api.InstanceConfig{api.NewInstanceConfig("a4-highgpu-8g", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration)},
		},
		"GPU type is specified but machine family/families is/are not specified": {
			rule: rules.NewRule(rules.WithGpuRule(&machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: "nvidia-b200",
				},
				Count:            8,
				PhysicalGPUCount: 8,
			})),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("a4-highgpu-8g", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("a4-highgpu-8g-nolssd", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"rule specifies machine family": {
			rule: rules.NewRule(rules.WithMachineFamilyRule(ptr.To("a4"))),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("a4-highgpu-8g", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("a4-highgpu-8g-nolssd", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"rule does not specify machine family": {
			rule: rules.NewRule(),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("a4x-highgpu-4g", "nvidia-gb200", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("a4x-highgpu-4g-nolssd", "nvidia-gb200", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("a4x-maxgpu-4g-metal", "nvidia-gb300", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"machine family not found": {
			rule:                rules.NewRule(rules.WithMachineFamilyRule(ptr.To("unknown-machine-family"))),
			wantInstanceConfigs: []*api.InstanceConfig{},
			wantErr:             fmt.Errorf("machine family not found unsupported machine family \"unknown-machine-family\""),
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
			g := NewInstanceConfigGenerator(context.Background(), nil, provider, optionsTracker)
			configs, err := g.deriveMachineConfigsFromRule(tc.rule, rank)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestDeriveMachineConfigsFromRule_MinCpuPlatform(t *testing.T) {
	provider := newMockInstanceConfigCloudProvider(nil, nil, machinetypes.M1, true, nil)
	rank := 1
	intelSkylakeName := "Intel Skylake"
	intelBroadwellName := "Intel Broadwell"

	testCases := map[string]struct {
		rule                rules.Rule
		wantInstanceConfigs []*api.InstanceConfig
		wantErr             error
		boolFlags           map[string]bool
	}{
		"rule does not specify minCpuPlatform": {
			rule: rules.NewRule(rules.WithMachineFamilyRule(ptr.To("m1"))),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("m1-ultramem-40", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-ultramem-80", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-ultramem-160", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-megamem-96", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"rule specifies invalid minCpuPlatform": {
			rule:                rules.NewRule(rules.WithMachineFamilyRule(ptr.To("m1")), rules.WithMinCpuPlatformRule(ptr.To("not-a-valid-platform"))),
			wantInstanceConfigs: []*api.InstanceConfig{},
			wantErr:             fmt.Errorf("unknown CPU platform \"not-a-valid-platform\""),
		},
		"rule specifies minCpuPlatform Intel Skylake": {
			rule: rules.NewRule(rules.WithMachineFamilyRule(ptr.To("m1")), rules.WithMinCpuPlatformRule(&intelSkylakeName)),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("m1-megamem-96", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"rule specifies minCpuPlatform Intel Broadwell": {
			rule: rules.NewRule(rules.WithMachineFamilyRule(ptr.To("m1")), rules.WithMinCpuPlatformRule(&intelBroadwellName)),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("m1-ultramem-40", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-ultramem-80", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-ultramem-160", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"MinCpuPlatform disabled - ignores minCpuPlatform, derives all m1": {
			rule: rules.NewRule(rules.WithMachineFamilyRule(ptr.To("m1")), rules.WithMinCpuPlatformRule(ptr.To("not-a-valid-platform"))),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("m1-ultramem-40", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-ultramem-80", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-ultramem-160", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("m1-megamem-96", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
			boolFlags: map[string]bool{
				experiments.FlexAdvisorMinCpuPlatformEnabledFlag: false,
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			manager := experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, map[string]string{})
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, manager)
			g := NewInstanceConfigGenerator(context.Background(), nil, provider, optionsTracker)
			configs, err := g.deriveMachineConfigsFromRule(tc.rule, rank)

			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))

			if tc.wantErr != nil {
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), tc.wantErr.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}

}

func TestExpandConfigsByProvisioningMode(t *testing.T) {
	testCases := map[string]struct {
		rule                rules.Rule
		instanceConfigs     []*api.InstanceConfig
		wantInstanceConfigs []*api.InstanceConfig
		enabledFeatures     []string
	}{
		"rule specifies spot": {
			rule: rules.NewRule(rules.WithSpotRule(ptr.To(true))),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
		},
		"rule does not specify spot": {
			rule: rules.NewRule(),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 2, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 2, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"DWS - FlexAdvisorDWS disabled - doesn't generate DWS machines": {
			rule: rules.NewRule(rules.WithFlexStartRule(true, nil)),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: nil,
		},
		"DWS - FlexAdvisorDWS enabled - creates DWS machines": {
			rule:            rules.NewRule(rules.WithFlexStartRule(true, nil), rules.WithMaxRunDurationRule(ptr.To(3600))),
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				// expandConfigsByProvisioningMode sets all MRD values on DWS machines to default 7 days. Only later in expandConfigsByMaxRunDuration that value is overwritten by rule's MRD value.
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.FlexStart, "604800"),
				api.NewInstanceConfig("n2-standard-4", "", 0, 1, instanceavailability.FlexStart, "604800"),
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager(tc.enabledFeatures...))
			g := NewInstanceConfigGenerator(context.Background(), nil, &mockInstanceConfigCloudProvider{}, optionsTracker)
			configs := g.expandConfigsByProvisioningMode(tc.rule, tc.instanceConfigs)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
		})
	}
}

func TestExpandConfigsByMaxRunDuration(t *testing.T) {
	testCases := map[string]struct {
		enabledFeatures     []string
		rule                rules.Rule
		instanceConfigs     []*api.InstanceConfig
		wantInstanceConfigs []*api.InstanceConfig
	}{
		"FlexAdvisorDWS disabled - doesn't add MaxRunDuration values": {
			enabledFeatures: nil,
			rule:            rules.NewRule(rules.WithMaxRunDurationRule(ptr.To(3600))),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"FlexAdvisorDWS enabled - rule without MRD, does nothing": {
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			rule:            rules.NewRule(),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.FlexStart, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.FlexStart, api.EmptyMaxRunDuration),
			},
		},
		"FlexAdvisorDWS enabled - adds MaxRunDuration values from rules, overwrites existing values": {
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			rule:            rules.NewRule(rules.WithMaxRunDurationRule(ptr.To(3600))),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 1, instanceavailability.Standard, "7200"),
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.FlexStart, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.FlexStart, "7200"),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Standard, "3600"),
				api.NewInstanceConfig("n2-standard-4", "", 0, 1, instanceavailability.Standard, "3600"),
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.FlexStart, "3600"),
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.FlexStart, "3600"),
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager(tc.enabledFeatures...))
			g := NewInstanceConfigGenerator(context.Background(), nil, &mockInstanceConfigCloudProvider{}, optionsTracker)
			configs := g.expandConfigsByMaxRunDuration(tc.rule, tc.instanceConfigs)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
		})
	}
}

func TestAssignZonesToConfigs(t *testing.T) {
	provider := newMockInstanceConfigCloudProvider([]string{"us-west1-a", "us-west1-b", "us-west1-c"}, nil, machinetypes.E2, false, map[string]set.Set[string]{
		"n2-standard-2": set.New("us-central1-a", "us-central1-b", "us-central1-c", "us-west1-a", "us-west1-b", "us-west1-c", "us-central1-ai1a"),
		"n2-standard-4": set.New("us-central1-a", "us-central1-b", "us-central1-c", "us-west1-a", "us-west1-b", "us-west1-c"),
	})
	testCases := map[string]struct {
		rule                rules.Rule
		instanceConfigs     []*api.InstanceConfig
		wantInstanceConfigs []*api.InstanceConfig
		wantErrors          []error
		zoneTypesEnabled    bool
		boolFlags           map[string]bool
	}{
		"rule specifies zones": {
			rule: rules.NewRule(rules.WithLocationRule([]string{"us-central1-a", "us-central1-b", "us-central1-c"})),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-c")),
				api.NewInstanceConfigWithZones("n2-standard-4", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-c")),
			},
		},
		"rule specifies zone types": {
			rule: rules.NewRule(rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI"}, crd.TestDataProvider(
				[]string{"us-central1-a", "us-central1-b"},
				[]string{"us-central1-ai1a"},
				nil,
			))),
			zoneTypesEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      true,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: true,
			},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-ai1a")),
			},
		},
		"rule specifies zone types with overlapping zones": {
			rule: rules.NewRule(rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "CLUSTER_DEFAULT"}, crd.TestDataProvider(
				[]string{"us-central1-a", "us-central1-b"},
				nil,
				[]string{"us-central1-a", "us-central1-ai1a"},
			))),
			zoneTypesEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      true,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: true,
			},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-ai1a")),
			},
		},
		"rule specifies AI zone type - generates only AI zones": {
			rule: rules.NewRule(rules.WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, crd.TestDataProvider(
				[]string{"us-central1-a", "us-central1-b"},
				[]string{"us-central1-ai1a"},
				[]string{"us-central1-a", "us-central1-ai1a"},
			))),
			zoneTypesEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      true,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: true,
			},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-ai1a")),
			},
		},
		"rule with empty zone types - defaults to GetAutoprovisioningLocations": {
			rule: rules.NewRule(rules.WithLocationZoneTypesRule([]v1.ZoneType{}, crd.TestDataProvider(
				[]string{"us-central1-a", "us-central1-b"},
				[]string{"us-central1-ai1a"},
				[]string{"us-central1-a", "us-central1-ai1a"},
			))),
			zoneTypesEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      true,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: true,
			},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			},
		},
		"zonetypes disabled, defaults to zones not specified": {
			rule: rules.NewRule(rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, crd.TestDataProvider(
				[]string{"us-central1-a", "us-central1-b"},
				nil,
				nil,
			))),
			zoneTypesEnabled: false,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag: false,
			},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			},
		},
		"rule specifies zone types, zoneTypes return error - falls back to autoprovisioning locations": {
			rule: func() rules.Rule {
				p := crd.TestDataProvider(nil, nil, nil)
				p.SetStandardError(fmt.Errorf("standard error"))
				return rules.NewRule(rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, p))
			}(),
			zoneTypesEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      true,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: true,
			},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			},
		},
		"rule specifies zone types, FlexAdvisorZoneTypes disabled - defaults to zones not specified": {
			rule: rules.NewRule(rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, crd.TestDataProvider(
				[]string{"us-central1-a", "us-central1-b"},
				nil,
				nil,
			))),
			zoneTypesEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag: false,
			},
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			},
		},
		"rule does not specify zones": {
			rule: rules.NewRule(),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
				api.NewInstanceConfigWithZones("n2-standard-4", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-west1-a", "us-west1-b", "us-west1-c")),
			},
		},
		"unsupported machineType, zone combination": {
			rule: rules.NewRule(rules.WithLocationRule([]string{"us-central1-a", "us-central1-b", "us-central1-x"})),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n2-standard-4", "", 0, 2, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfigWithZones("n2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b")),
				api.NewInstanceConfigWithZones("n2-standard-4", "", 0, 12, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b")),
			},
		},
		"unsupported machineType in all targeted zones": {
			rule: rules.NewRule(rules.WithLocationRule([]string{"us-central1-a", "us-central1-b"})),
			instanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("tpu7-standard-1t", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration),
			},
			wantInstanceConfigs: []*api.InstanceConfig{},
			wantErrors: []error{
				fmt.Errorf("machineType=tpu7-standard-1t was removed due to not being available in any of the zones"),
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			manager := experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, map[string]string{})
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{
				InternalOptions: options.InternalOptions{
					ZoneTypesEnabled: tc.zoneTypesEnabled,
				},
			}, gkeclient.Cluster{}, manager)
			g := NewInstanceConfigGenerator(context.Background(), nil, provider, optionsTracker)
			configs, errs := g.assignZonesToConfigs(tc.rule, tc.instanceConfigs)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
			if len(tc.wantErrors) > 0 || len(errs) > 0 {
				assert.Equal(t, len(tc.wantErrors), len(errs))
				var wantErrMsgs, gotErrMsgs []string
				for _, err := range tc.wantErrors {
					wantErrMsgs = append(wantErrMsgs, err.Error())
				}
				for _, err := range errs {
					gotErrMsgs = append(gotErrMsgs, err.Error())
				}
				assert.ElementsMatch(t, wantErrMsgs, gotErrMsgs)
			}
		})
	}
}

func TestInstanceConfigsForNodePools(t *testing.T) {
	rank := 1
	mig1 := gke.NewTestGkeMigBuilder().SetNodePoolName("node-pool1").SetSpec(&gkeclient.NodePoolSpec{Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"}, MachineType: "n2-standard-2", Spot: true}).Build()
	mig2 := gke.NewTestGkeMigBuilder().SetNodePoolName("node-pool2").SetSpec(&gkeclient.NodePoolSpec{Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"}, MachineType: "a2-highgpu-1g", Spot: false, Accelerators: []*container.AcceleratorConfig{{AcceleratorType: "nvidia-tesla-a100", AcceleratorCount: 1}}}).Build()
	mig3 := gke.NewTestGkeMigBuilder().SetNodePoolName("node-pool3").Build()
	dwsMig := gke.NewTestGkeMigBuilder().SetNodePoolName("dws-pool").SetSpec(&gkeclient.NodePoolSpec{Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"}, MachineType: "e2-standard-2", FlexStart: true}).Build()
	dwsMigWithMRD := gke.NewTestGkeMigBuilder().SetNodePoolName("dws-pool-with-mrd").SetSpec(&gkeclient.NodePoolSpec{Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"}, MachineType: "e2-standard-2", FlexStart: true, MaxRunDurationInSeconds: "3600"}).Build()
	provider := newMockInstanceConfigCloudProvider(nil, []*gke.GkeMig{mig1, mig2, mig3, dwsMig, dwsMigWithMRD}, machinetypes.E2, true, nil)

	testCases := map[string]struct {
		rule                rules.Rule
		wantInstanceConfigs []*api.InstanceConfig
		wantErrors          []error
		enabledFeatures     []string
	}{
		"matching node pool exist": {
			rule:                rules.NewRule(rules.WithNodePoolsRule([]string{"node-pool1"})),
			wantInstanceConfigs: []*api.InstanceConfig{api.NewInstanceConfigWithZones("n2-standard-2", "", 0, rank, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-c"))},
			wantErrors:          []error{},
		},
		"matching node pool with gpus exist": {
			rule:                rules.NewRule(rules.WithNodePoolsRule([]string{"node-pool2"})),
			wantInstanceConfigs: []*api.InstanceConfig{api.NewInstanceConfigWithZones("a2-highgpu-1g", "nvidia-tesla-a100", 1, rank, instanceavailability.Standard, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-c"))},
			wantErrors:          []error{},
		},
		"DWS pool - FlexAdvisorDWS disabled - generates no configs": {
			rule:                rules.NewRule(rules.WithNodePoolsRule([]string{"dws-pool"})),
			wantInstanceConfigs: nil,
			wantErrors: []error{
				fmt.Errorf("flex start node pools are not supported by Flex Advisor"),
			},
		},
		"DWS pool - FlexAdvisorDWS enabled, pool doesnt have MRD set - generates configs with default MRD": {
			enabledFeatures:     []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			rule:                rules.NewRule(rules.WithNodePoolsRule([]string{"dws-pool"})),
			wantInstanceConfigs: []*api.InstanceConfig{api.NewInstanceConfigWithZones("e2-standard-2", "", 0, rank, instanceavailability.FlexStart, strconv.Itoa(7*24*60*60), set.New[string]("us-central1-a", "us-central1-b", "us-central1-c"))},
			wantErrors:          []error{},
		},
		"DWS pool - FlexAdvisorDWS enabled, pool has MRD - generates configs, copies MRD over": {
			enabledFeatures:     []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			rule:                rules.NewRule(rules.WithNodePoolsRule([]string{"dws-pool-with-mrd"})),
			wantInstanceConfigs: []*api.InstanceConfig{api.NewInstanceConfigWithZones("e2-standard-2", "", 0, rank, instanceavailability.FlexStart, "3600", set.New[string]("us-central1-a", "us-central1-b", "us-central1-c"))},
			wantErrors:          []error{},
		},
		"matching node pool exist, matching node pool does not exist, matching node pool exist with empty spec": {
			rule:                rules.NewRule(rules.WithNodePoolsRule([]string{"node-pool1", "node-pool-unknown", "node-pool3"})),
			wantInstanceConfigs: []*api.InstanceConfig{api.NewInstanceConfigWithZones("n2-standard-2", "", 0, rank, instanceavailability.Spot, api.EmptyMaxRunDuration, set.New("us-central1-a", "us-central1-b", "us-central1-c"))},
			wantErrors: []error{
				fmt.Errorf("mig not found for node pool: node-pool-unknown"),
				fmt.Errorf("mig spec is undefined for node pool: node-pool3"),
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager(tc.enabledFeatures...))
			g := NewInstanceConfigGenerator(context.Background(), nil, provider, optionsTracker)
			configs, errors := g.instanceConfigsForNodePools(tc.rule, rank)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
			assert.ElementsMatch(t, tc.wantErrors, errors)
		})
	}
}

func TestInstanceConfigForMachineType(t *testing.T) {
	rank := 1
	testCases := map[string]struct {
		rule                rules.Rule
		wantInstanceConfigs []*api.InstanceConfig
		wantErr             error
	}{
		"invalid machine type": {
			rule:                rules.NewRule(rules.WithMachineTypeRule(ptr.To("invalid-machine-type"))),
			wantInstanceConfigs: []*api.InstanceConfig{},
			wantErr:             fmt.Errorf("machine type not found for invalid-machine-type"),
		},
		"machine type not from n1 family - a4-highgpu-8g": {
			rule:                rules.NewRule(rules.WithMachineTypeRule(ptr.To("a4-highgpu-8g"))),
			wantInstanceConfigs: []*api.InstanceConfig{api.NewInstanceConfig("a4-highgpu-8g", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration)},
		},
		"machine type from n1 family with specified gpu type and count": {
			rule: rules.NewRule(rules.WithMachineTypeRule(ptr.To("n1-highmem-64")), rules.WithGpuRule(&machinetypes.GpuRequest{
				Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-t4"},
				Count:            4,
				PhysicalGPUCount: 4,
			})),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-t4", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"machine type from n1 family with only specified gpu type": {
			rule: rules.NewRule(rules.WithMachineTypeRule(ptr.To("n1-highmem-8")), rules.WithGpuRule(&machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{GpuType: "nvidia-tesla-t4"},
			})),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n1-highmem-8", "nvidia-tesla-t4", 1, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-8", "nvidia-tesla-t4", 2, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-8", "nvidia-tesla-t4", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"machine type from n1 family without gpu config specified": {
			rule: rules.NewRule(rules.WithMachineTypeRule(ptr.To("n1-highmem-64"))),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-k80", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-p100", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-v100", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-p4", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-t4", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
	}
	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			provider := newMockInstanceConfigCloudProvider(nil, nil, machinetypes.A4X, true, nil)
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
			g := NewInstanceConfigGenerator(context.Background(), nil, provider, optionsTracker)
			configs, err := g.deriveMachineConfigsFromRule(tc.rule, rank)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestInstanceConfigForMachineTypeFromN1Family(t *testing.T) {
	rank := 1
	n1Highmem64, err := machinetypes.NewMachineConfigProvider(nil).ToMachineType("n1-highmem-64")
	assert.Nil(t, err)
	testCases := map[string]struct {
		rule                rules.Rule
		machineType         machinetypes.MachineType
		wantInstanceConfigs []*api.InstanceConfig
	}{
		"with specified gpu type and count": {
			rule: rules.NewRule(rules.WithMachineTypeRule(ptr.To("n1-highmem-64")), rules.WithGpuRule(&machinetypes.GpuRequest{
				Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-t4"},
				Count:            4,
				PhysicalGPUCount: 4,
			})),
			machineType: n1Highmem64,
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-t4", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"machine type from n1 family without gpu config specified": {
			rule:        rules.NewRule(rules.WithMachineTypeRule(ptr.To("n1-highmem-64"))),
			machineType: n1Highmem64,
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-k80", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-p100", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-v100", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-p4", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "nvidia-tesla-t4", 4, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("n1-highmem-64", "", 0, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
	}
	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
			g := NewInstanceConfigGenerator(context.Background(), nil, &mockInstanceConfigCloudProvider{}, optionsTracker)
			configs := g.instanceConfigsForMachineTypeFromN1Family(tc.rule, tc.machineType, machinetypes.N1, rank)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
		})
	}
}

func TestInstanceConfigsForGpuTypes(t *testing.T) {
	rank := 1
	testCases := map[string]struct {
		rule                rules.Rule
		wantInstanceConfigs []*api.InstanceConfig
	}{
		"select machine types based on gpu": {
			rule: rules.NewRule(rules.WithGpuRule(&machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: "nvidia-b200",
				},
				Count:            8,
				PhysicalGPUCount: 8,
			})),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("a4-highgpu-8g", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("a4-highgpu-8g-nolssd", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"select machine types based on gpu and local ssd": {
			rule: rules.NewRule(rules.WithGpuRule(&machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: "nvidia-b200",
				},
				Count:            8,
				PhysicalGPUCount: 8,
			}), rules.WithStorageRule(ptr.To("hyperdisk-balanced"), nil, nil, nil)),
			wantInstanceConfigs: []*api.InstanceConfig{
				api.NewInstanceConfig("a4-highgpu-8g", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
				api.NewInstanceConfig("a4-highgpu-8g-nolssd", "nvidia-b200", 8, rank, instanceavailability.Standard, api.EmptyMaxRunDuration),
			},
		},
		"no matching machine types based on gpu": {
			rule: rules.NewRule(rules.WithGpuRule(&machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: "gpu-unknown",
				},
				Count:            8,
				PhysicalGPUCount: 8,
			})),
			wantInstanceConfigs: []*api.InstanceConfig{},
		},
		"no matching machine types based on local ssd": {
			rule:                rules.NewRule(rules.WithStorageRule(ptr.To("unknown-boot-disk"), nil, nil, nil)),
			wantInstanceConfigs: []*api.InstanceConfig{},
		},
	}
	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
			g := NewInstanceConfigGenerator(context.Background(), nil, &mockInstanceConfigCloudProvider{}, optionsTracker)
			configs, err := g.deriveMachineConfigsFromRule(tc.rule, rank)
			assert.NoError(t, err)
			compareInstanceConfigMaps(t, toInstanceConfigMap(tc.wantInstanceConfigs), toInstanceConfigMap(configs))
		})
	}
}

func toInstanceConfigMap(configs []*api.InstanceConfig) map[string]*api.InstanceConfig {
	configMap := make(map[string]*api.InstanceConfig)
	for _, config := range configs {
		configMap[config.Signature()] = config
	}
	return configMap
}

func compareInstanceConfigMaps(t *testing.T, want, got map[string]*api.InstanceConfig) {
	if len(want) != len(got) {
		// assert.Equal(t, want, got) provides diff between objects vs iterating over each field.
		// TODO(b/491088027): fix `rank` parameter in the tests so we can just do it for each case
		assert.Equal(t, want, got, fmt.Errorf("length do not match. len(got): %d, len(want): %d", len(got), len(want)))
	}

	for key, val := range want {
		actualVal, found := got[key]
		if !found {
			t.Fatalf("\"%s\" not present in generated configs, received %+v", key, got)
		}
		assert.Equal(t, val.Zones(), actualVal.Zones())
		assert.Equal(t, val.GpuType(), actualVal.GpuType())
		assert.Equal(t, val.ProvisioningMode(), actualVal.ProvisioningMode())
		assert.Equal(t, val.MachineType(), actualVal.MachineType())
		assert.Equal(t, val.GpuCount(), actualVal.GpuCount())
		assert.Equal(t, val.MaxRunDurationInSeconds(), actualVal.MaxRunDurationInSeconds())
	}
}

func TestMatchingCrd(t *testing.T) {
	provider := newMockInstanceConfigCloudProvider(
		nil,
		nil,
		machinetypes.E2,
		false,
		nil,
		withAutopilotEnabled(true),
	)
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "crd1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("g2-standard-12"),
				},
			},
		},
	}, "test-project", false, provider, nil)

	userSuppliedCrdWithOverlappingPCCName := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Balanced",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("g2-standard-12"),
				},
			},
		},
	}, "test-project", false, provider, nil)

	testCccCrdFromPcc := func(name string, optionsTracker *optstracking.OptionsTracker) crd.CRD {
		pcc, err := machinetypes.ToPredefinedComputeClass(name)
		if err != nil {
			klog.Fatalf("failed to convert %s to PCC: %v", name, err)
		}
		var priorities []v1.Priority
		for _, family := range pcc.MachineFamilies() {
			familyRef := family.Name()
			priorities = append(priorities, v1.Priority{
				MachineFamily: &familyRef,
			})
		}
		ccScaleOut := &v1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: v1.ComputeClassSpec{
				Priorities: priorities,
			},
		}
		return ccc.NewCccCrd(ccScaleOut, "project1", provider.IsAutopilotEnabled(), provider, optionsTracker)
	}

	testCases := map[string]struct {
		flexibilityScopeKey string
		lister              lister.Lister
		boolFlags           map[string]bool
		wantCrd             func(*optstracking.OptionsTracker) crd.CRD
		wantErr             error
	}{
		"matching crd exist": {
			flexibilityScopeKey: "crd1",
			lister:              lister.NewMockCrdLister([]crd.CRD{crd1}),
			wantCrd:             func(_ *optstracking.OptionsTracker) crd.CRD { return crd1 },
			wantErr:             nil,
		},
		"matching crd not exist": {
			flexibilityScopeKey: "crd2",
			lister:              lister.NewMockCrdLister([]crd.CRD{crd1}),
			wantCrd:             nil,
			wantErr:             fmt.Errorf("failed to get CRD for flexibilityScopeKey \"crd2\": %w", fmt.Errorf("crd doesnt exist")),
		},
		"PCC - FlexAdvisorPCCSupport disabled - returns error": {
			flexibilityScopeKey: "Balanced",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorPCCSupportEnabledFlag: false,
			},
			wantCrd: nil,
			wantErr: fmt.Errorf("predefined compute class \"Balanced\" is not supported: FlexAdvisorPCCSupport is disabled"),
		},
		"PCC - FlexAdvisorPCCSupport enabled - returns CCC CRD built from PCC": {
			flexibilityScopeKey: "Balanced",
			wantCrd: func(optionsTracker *optstracking.OptionsTracker) crd.CRD {
				return testCccCrdFromPcc("Balanced", optionsTracker)
			},
			wantErr: nil,
		},
		// User should not be able to create such CCC. This case is just to verify if someone changes this behavior through some refactorings
		"PCC - FlexAdvisorPCCSupport disabled - user's CCC name overlaps with name of a PCC - returns error": {
			flexibilityScopeKey: "Balanced",
			lister:              lister.NewMockCrdLister([]crd.CRD{userSuppliedCrdWithOverlappingPCCName}),
			boolFlags: map[string]bool{
				experiments.FlexAdvisorPCCSupportEnabledFlag: false,
			},
			wantCrd: nil,
			wantErr: fmt.Errorf("predefined compute class \"Balanced\" is not supported: FlexAdvisorPCCSupport is disabled"),
		},
		"PCC - FlexAdvisorPCCSupport enabled - user's CCC name overlaps with name of a PCC - returns PCC": {
			flexibilityScopeKey: "Balanced",
			lister:              lister.NewMockCrdLister([]crd.CRD{userSuppliedCrdWithOverlappingPCCName}),
			wantCrd: func(optionsTracker *optstracking.OptionsTracker) crd.CRD {
				return testCccCrdFromPcc("Balanced", optionsTracker)
			},
			wantErr: nil,
		},
	}
	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			boolFlags := tc.boolFlags
			if boolFlags == nil {
				boolFlags = map[string]bool{
					experiments.FlexAdvisorPCCSupportEnabledFlag:      true,
					experiments.FlexAdvisorPCCSupportMinCAVersionFlag: true,
				}
			}
			optionsTracker := optstracking.FakeOptionsTracker(
				options.AutoscalingOptions{},
				gkeclient.Cluster{},
				experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{}),
			)
			g := NewInstanceConfigGenerator(context.Background(), tc.lister, provider, optionsTracker)
			gotCrd, err := g.matchingCrd(tc.flexibilityScopeKey)
			var expectedCrd crd.CRD
			if tc.wantCrd != nil {
				expectedCrd = tc.wantCrd(optionsTracker)
			}
			assert.Equal(t, expectedCrd, gotCrd)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestMachineFamilies(t *testing.T) {
	mockInstanceConfigCloudProvider := &mockInstanceConfigCloudProvider{
		defaultMachineFamily: machinetypes.E2,
	}
	testCases := map[string]struct {
		rule                rules.Rule
		wantMachineFamilies []machinetypes.MachineFamily
		wantErr             error
	}{
		"machine family is specified in the rule": {
			rules.NewRule(rules.WithMachineFamilyRule(ptr.To("a2"))),
			[]machinetypes.MachineFamily{machinetypes.A2},
			nil,
		},
		"machine families are specified as pod families in the rule": {
			rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
			[]machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK, machinetypes.E4},
			nil,
		},
		"unknown pod family": {
			rules.NewRule(rules.WithPodFamilyRule(ptr.To("unknown-pod-family"))),
			[]machinetypes.MachineFamily{},
			fmt.Errorf("unknown pod family"),
		},
		"machine type": {
			rules.NewRule(rules.WithMachineTypeRule(ptr.To("e4a-standard-2"))),
			[]machinetypes.MachineFamily{machinetypes.E4A},
			nil,
		},
		"gpu type": {
			rules.NewRule(rules.WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name()}})),
			mockInstanceConfigCloudProvider.MachineConfigProvider().AllMachineFamilies(),
			nil,
		},
		"tpu type": {
			rules.NewRule(rules.WithTpuRule("t7x", 1, "2x2x1")),
			mockInstanceConfigCloudProvider.MachineConfigProvider().AllMachineFamilies(),
			nil,
		},
	}
	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
			g := NewInstanceConfigGenerator(context.Background(), nil, mockInstanceConfigCloudProvider, optionsTracker)
			machineFamilies, err := g.machineFamiliesForRule(tc.rule)
			gotNames := []string{}
			for _, mf := range machineFamilies {
				gotNames = append(gotNames, mf.Name())
			}
			wantNames := []string{}
			for _, mf := range tc.wantMachineFamilies {
				wantNames = append(wantNames, mf.Name())
			}
			assert.ElementsMatch(t, wantNames, gotNames)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestFilterMachineTypes(t *testing.T) {
	testCases := map[string]struct {
		rule         rules.Rule
		machineTypes map[string]machinetypes.MachineType
		want         map[string]machinetypes.MachineType
	}{
		"filter out by min CPU": {
			rule: rules.NewRule(rules.WithMinCoresRule(ptr.To(4))),
			machineTypes: map[string]machinetypes.MachineType{
				"test-machine-type-2": machinetypes.NewMachineTypeInfo("test-machine-type-2", 2, 20),
				"test-machine-type-4": machinetypes.NewMachineTypeInfo("test-machine-type-4", 4, 20),
				"test-machine-type-8": machinetypes.NewMachineTypeInfo("test-machine-type-8", 8, 20),
			},
			want: map[string]machinetypes.MachineType{
				"test-machine-type-4": machinetypes.NewMachineTypeInfo("test-machine-type-4", 4, 20),
				"test-machine-type-8": machinetypes.NewMachineTypeInfo("test-machine-type-8", 8, 20),
			},
		},
		"filter out by min memory": {
			rule: rules.NewRule(rules.WithMinMemoryGbRule(ptr.To(20))),
			machineTypes: map[string]machinetypes.MachineType{
				"test-machine-type-2": machinetypes.NewMachineTypeInfo("test-machine-type-2", 2, 10),
				"test-machine-type-4": machinetypes.NewMachineTypeInfo("test-machine-type-4", 4, 20),
				"test-machine-type-8": machinetypes.NewMachineTypeInfo("test-machine-type-8", 8, 40),
			},
			want: map[string]machinetypes.MachineType{
				"test-machine-type-4": machinetypes.NewMachineTypeInfo("test-machine-type-4", 4, 20),
				"test-machine-type-8": machinetypes.NewMachineTypeInfo("test-machine-type-8", 8, 40),
			},
		},
		"no filter out": {
			rule: rules.NewRule(),
			machineTypes: map[string]machinetypes.MachineType{
				"test-machine-type-2": machinetypes.NewMachineTypeInfo("test-machine-type-2", 2, 10),
				"test-machine-type-4": machinetypes.NewMachineTypeInfo("test-machine-type-4", 4, 20),
				"test-machine-type-8": machinetypes.NewMachineTypeInfo("test-machine-type-8", 8, 40),
			},
			want: map[string]machinetypes.MachineType{
				"test-machine-type-2": machinetypes.NewMachineTypeInfo("test-machine-type-2", 2, 10),
				"test-machine-type-4": machinetypes.NewMachineTypeInfo("test-machine-type-4", 4, 20),
				"test-machine-type-8": machinetypes.NewMachineTypeInfo("test-machine-type-8", 8, 40),
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			filtered := filterByCoresAndMemoryRequirements(tc.machineTypes, tc.rule)
			assert.Equal(t, tc.want, filtered)
		})
	}
}

func TestCapGeneratedInstanceConfigs(t *testing.T) {
	ig1 := api.NewInstanceConfig("n1-highmem-2", "nvidia-tesla-k80", 8, 1, instanceavailability.Standard, api.EmptyMaxRunDuration)
	ig2 := api.NewInstanceConfig("n1-highmem-4", "nvidia-tesla-k80", 8, 2, instanceavailability.Standard, api.EmptyMaxRunDuration)
	ig3 := api.NewInstanceConfig("n1-highmem-8", "nvidia-tesla-k80", 8, 3, instanceavailability.Standard, api.EmptyMaxRunDuration)
	ig4 := api.NewInstanceConfig("n1-highmem-16", "nvidia-tesla-k80", 8, 4, instanceavailability.Standard, api.EmptyMaxRunDuration)
	ig5 := api.NewInstanceConfig("n1-highmem-32", "nvidia-tesla-k80", 8, 5, instanceavailability.Standard, api.EmptyMaxRunDuration)
	ig6 := api.NewInstanceConfig("n1-highcpu-2", "nvidia-tesla-k80", 8, 6, instanceavailability.Standard, api.EmptyMaxRunDuration)
	ig7 := api.NewInstanceConfig("n1-highcpu-4", "nvidia-tesla-k80", 8, 7, instanceavailability.Standard, api.EmptyMaxRunDuration)
	ig8 := api.NewInstanceConfig("n1-highcpu-8", "nvidia-tesla-k80", 8, 8, instanceavailability.Standard, api.EmptyMaxRunDuration)
	testCases := map[string]struct {
		instanceConfigs     map[string]*api.InstanceConfig
		maxInstanceConfigs  int
		wantInstanceConfigs map[string]*api.InstanceConfig
		wantCappedKeysMap   map[string]bool
	}{
		"input is less than the limit": {
			instanceConfigs: map[string]*api.InstanceConfig{
				ig1.Signature(): ig1,
				ig2.Signature(): ig2,
				ig3.Signature(): ig3,
				ig4.Signature(): ig4,
				ig5.Signature(): ig5,
				ig6.Signature(): ig6,
				ig7.Signature(): ig7,
				ig8.Signature(): ig8,
			},
			maxInstanceConfigs: 100,
			wantInstanceConfigs: map[string]*api.InstanceConfig{
				ig1.Signature(): ig1,
				ig2.Signature(): ig2,
				ig3.Signature(): ig3,
				ig4.Signature(): ig4,
				ig5.Signature(): ig5,
				ig6.Signature(): ig6,
				ig7.Signature(): ig7,
				ig8.Signature(): ig8,
			},
			wantCappedKeysMap: map[string]bool{
				ig1.Signature(): false,
				ig2.Signature(): false,
				ig3.Signature(): false,
				ig4.Signature(): false,
				ig5.Signature(): false,
				ig6.Signature(): false,
				ig7.Signature(): false,
				ig8.Signature(): false,
			},
		},
		"input is capped at the limit": {
			instanceConfigs: map[string]*api.InstanceConfig{
				ig1.Signature(): ig1,
				ig2.Signature(): ig2,
				ig3.Signature(): ig3,
				ig4.Signature(): ig4,
				ig5.Signature(): ig5,
				ig6.Signature(): ig6,
				ig7.Signature(): ig7,
				ig8.Signature(): ig8,
			},
			maxInstanceConfigs: 5,
			wantInstanceConfigs: map[string]*api.InstanceConfig{
				ig1.Signature(): ig1,
				ig2.Signature(): ig2,
				ig3.Signature(): ig3,
				ig4.Signature(): ig4,
				ig5.Signature(): ig5,
			},
			wantCappedKeysMap: map[string]bool{
				ig1.Signature(): false,
				ig2.Signature(): false,
				ig3.Signature(): false,
				ig4.Signature(): false,
				ig5.Signature(): false,
				ig6.Signature(): true,
				ig7.Signature(): true,
				ig8.Signature(): true,
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
			g := NewInstanceConfigGenerator(context.Background(), nil, nil, optionsTracker, WithMaxInstanceConfigs(tc.maxInstanceConfigs))
			got, gotCappedKeysMap := g.capGeneratedInstanceConfigs(tc.instanceConfigs, "")
			assert.Equal(t, len(tc.wantInstanceConfigs), len(got))
			assert.Equal(t, tc.wantInstanceConfigs, got)
			assert.Equal(t, len(tc.wantCappedKeysMap), len(gotCappedKeysMap))
			assert.Equal(t, tc.wantCappedKeysMap, gotCappedKeysMap)
		})
	}
}

func TestGenerationValidation_Metrics(t *testing.T) {
	registerOnce.Do(metrics.RegisterAll)

	crdUnknownMachine := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "unknown-machine-crd",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("machine-type-unknown"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)

	crdUnavailableZones := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "unavailable-zones-crd",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-2"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)

	availableCrds := []crd.CRD{crdUnknownMachine, crdUnavailableZones}

	// provider with no zones for e2-standard-2, so it won't be available in any zones
	provider := newMockInstanceConfigCloudProvider(
		nil,
		nil,
		machinetypes.E2,
		false,
		map[string]set.Set[string]{},
	)

	optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
	g := NewInstanceConfigGenerator(context.Background(), lister.NewMockCrdLister(availableCrds), provider, optionsTracker)

	// 1. Test general generation error (unknown machine type)
	initialVal, err := metrics.GetFlexAdvisorGenerationErrorsCountForTest(metrics.ZeroConfigsGeneratedForRule)
	assert.NoError(t, err)

	_, _ = g.generateInstanceConfigs("unknown-machine-crd")

	newVal, err := metrics.GetFlexAdvisorGenerationErrorsCountForTest(metrics.ZeroConfigsGeneratedForRule)
	assert.NoError(t, err)
	assert.Equal(t, initialVal+1, newVal, "Metric ZeroConfigsGeneratedForRule should be incremented for unknown machine type")

	// 2. Test zone availability error (zero configs generated because machine unavailable in all zones)
	initialVal = newVal
	_, _ = g.generateInstanceConfigs("unavailable-zones-crd")

	newVal, err = metrics.GetFlexAdvisorGenerationErrorsCountForTest(metrics.ZeroConfigsGeneratedForRule)
	assert.NoError(t, err)
	assert.Equal(t, initialVal, newVal, "Metric ZeroConfigsGeneratedForRule should NOT be incremented for zone unavailability errors")
}

func TestInstanceConfigGenerator_MachineErrorCache(t *testing.T) {
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "crd1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-2"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	crd2 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "crd2",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-4"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)

	testCases := map[string]struct {
		boolFlags map[string]bool
		run       func(t *testing.T, g *instanceConfigGenerator, provider *mockInstanceConfigCloudProvider)
	}{
		"GCE doesnt return errors - cache is not used": {
			run: func(t *testing.T, g *instanceConfigGenerator, provider *mockInstanceConfigCloudProvider) {
				provider.machineTypes = map[string]set.Set[string]{
					"e2-standard-2": set.New("us-west1-a"),
					"e2-standard-4": set.New("us-west1-a"),
				}

				baseline := 0
				g.generateInstanceConfigs("crd1")
				assert.Equal(t, baseline+2, provider.getMachineTypeCallsQty)
				g.generateInstanceConfigs("crd1")
				assert.Equal(t, baseline+4, provider.getMachineTypeCallsQty)
				g.generateInstanceConfigs("crd1")
				assert.Equal(t, baseline+6, provider.getMachineTypeCallsQty)
			},
		},
		"GCE returns errors - doesnt call GCE if cached": {
			run: func(t *testing.T, g *instanceConfigGenerator, provider *mockInstanceConfigCloudProvider) {
				g.generateInstanceConfigs("crd1")
				assert.Equal(t, 1, provider.getMachineTypeCallsQty)

				g.generateInstanceConfigs("crd1")
				assert.Equal(t, 1, provider.getMachineTypeCallsQty)

				// e2-standard-4 is not cached yet
				g.generateInstanceConfigs("crd2")
				assert.Equal(t, 2, provider.getMachineTypeCallsQty)

				// Sleep to let it expire
				time.Sleep(machineErrorCacheTtl + 1*time.Second)

				// Third generation: should trigger FetchMachineType again because cache entry expired
				g.generateInstanceConfigs("crd1")
				assert.Equal(t, 3, provider.getMachineTypeCallsQty)
				g.generateInstanceConfigs("crd1")
				assert.Equal(t, 3, provider.getMachineTypeCallsQty)

				provider.machineTypes = map[string]set.Set[string]{
					"e2-standard-2": set.New("us-west1-a"),
					"e2-standard-4": set.New("us-west1-a"),
				}
				// after GCE stops returning error, cache is not used
				baseline := 3
				g.generateInstanceConfigs("crd2")
				// for each machineType SPOT and STANDARD are generated
				assert.Equal(t, baseline+2, provider.getMachineTypeCallsQty)
				g.generateInstanceConfigs("crd2")
				assert.Equal(t, baseline+4, provider.getMachineTypeCallsQty)
				g.generateInstanceConfigs("crd2")
				assert.Equal(t, baseline+6, provider.getMachineTypeCallsQty)
			},
		},
		"FlexAdvisorMachineErrorCache disabled - doesnt use cache": {
			boolFlags: map[string]bool{
				experiments.FlexAdvisorGeneratorMachineErrorsCacheEnabledFlag: false,
			},
			run: func(t *testing.T, g *instanceConfigGenerator, provider *mockInstanceConfigCloudProvider) {
				for i := 0; i < 100; i++ {
					g.generateInstanceConfigs("crd1")

					// for each machine type SPOT and STANDARD are generated
					assert.Equal(t, (i+1)*2, provider.getMachineTypeCallsQty)
					time.Sleep(1 * time.Minute)
				}
			},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				provider := newMockInstanceConfigCloudProvider(
					[]string{"us-west1-a"},
					nil,
					machinetypes.E2,
					false,
					nil,
				)

				optionsTracker := optstracking.FakeOptionsTracker(
					options.AutoscalingOptions{},
					gkeclient.Cluster{},
					experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, map[string]string{}),
				)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				g := NewInstanceConfigGenerator(ctx, lister.NewMockCrdLister([]crd.CRD{crd1, crd2}), provider, optionsTracker)

				tc.run(t, g, provider)
			})
		})
	}
}

// TestGuardPredefinedComputeClassFields is a fuse that should fail if PredefinedComputeClass struct is edited (fields added/removed). Generator's code in such case should be evaluated it's compatible
// and supports the new changes
func TestGuardPredefinedComputeClassFields(t *testing.T) {
	expectedFields := []string{
		"name",
		"machineFamilies",
		"machineFamilyBalancingEnabled",
		"sliceOfHardware",
		"acceleratorClass",
		"napLargerBootDisk",
	}

	structType := reflect.TypeOf(machinetypes.PredefinedComputeClass{})
	var actualFields []string
	for i := 0; i < structType.NumField(); i++ {
		actualFields = append(actualFields, structType.Field(i).Name)
	}

	assert.ElementsMatch(t, expectedFields, actualFields, "PredefinedComputeClass fields change detected, please verify InstanceConfigGenerator is compatible with the changes/supports new fields")
}

func TestNewPredefinedComputeClassCrd(t *testing.T) {

	testCases := map[string]struct {
		flexibilityScopeKey string
		wantName            string
		wantFamilies        []string
		wantErr             bool
		autopilotEnabled    bool
		disabledFeatures    []string
	}{
		"Balanced compute class": {
			flexibilityScopeKey: "Balanced",
			autopilotEnabled:    true,
			wantName:            "Balanced",
			wantFamilies:        []string{"n2", "n2d"},
		},
		"unsupported compute class": {
			flexibilityScopeKey: "Unsupported-Class",
			autopilotEnabled:    true,
			wantErr:             true,
		},
		"Scale-Out compute class - FlexAdvisorPCCSupport disabled - returns error": {
			flexibilityScopeKey: "Scale-Out",
			disabledFeatures: []string{
				experiments.FlexAdvisorPCCSupportEnabledFlag,
			},
			wantErr: true,
		},
		"Scale-Out compute class - standard cluster - returns error": {
			flexibilityScopeKey: "Scale-Out",
			wantErr:             true,
		},
		"Scale-Out compute class": {
			flexibilityScopeKey: "Scale-Out",
			autopilotEnabled:    true,
			wantName:            "Scale-Out",
			wantFamilies:        []string{"t2a", "t2d"},
		},
	}

	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			provider := newMockInstanceConfigCloudProvider(
				[]string{"us-west1-a", "us-west1-b"},
				nil,
				machinetypes.E2,
				true,
				nil,
				withAutopilotEnabled(tc.autopilotEnabled),
			)
			boolFlags := map[string]bool{}
			for _, f := range tc.disabledFeatures {
				boolFlags[f] = false
			}
			optionsTracker := optstracking.FakeOptionsTracker(
				options.AutoscalingOptions{},
				gkeclient.Cluster{},
				experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{}),
			)
			g := NewInstanceConfigGenerator(context.Background(), lister.NewMockCrdLister([]crd.CRD{}), provider, optionsTracker)
			crd, err := g.cccCrdFromPCC(tc.flexibilityScopeKey)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, crd)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, crd)
			assert.Equal(t, tc.wantName, crd.Name())
			assert.False(t, crd.AutopilotManaged())

			var families []string
			for _, rule := range crd.Rules() {
				if rule.MachineFamily() != "" {
					families = append(families, rule.MachineFamily())
				}
			}
			assert.ElementsMatch(t, tc.wantFamilies, families)
		})
	}
}
