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

package computeclass

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/mock"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

type MockGKEProvider struct {
	nodeGroups                        []cloudprovider.NodeGroup
	defaultMachineFamily              machinetypes.MachineFamily
	autoprovisioningLocations         []string
	resizableVmInAutopilotEnabled     map[string]bool
	resizableVmWithinPodFamilyEnabled map[string]bool
	autopilotEnabled                  bool
	validMachineTypes                 map[gce.MachineTypeKey]bool
}

type MockGKEProviderBuilder struct {
	provider MockGKEProvider
}

func woErr[T any](result T, err error) T {
	if err != nil {
		panic(err)
	}
	return result
}

func NewMockGKEProviderBuilder() *MockGKEProviderBuilder {
	return &MockGKEProviderBuilder{
		provider: MockGKEProvider{},
	}
}

func (b *MockGKEProviderBuilder) Build() *MockGKEProvider {
	return &b.provider
}

func (b *MockGKEProviderBuilder) WithNodeGroups(nodeGroups []cloudprovider.NodeGroup) *MockGKEProviderBuilder {
	b.provider.nodeGroups = nodeGroups
	return b
}

func (b *MockGKEProviderBuilder) WithDefaultMachineFamily(defaultMachineFamily machinetypes.MachineFamily) *MockGKEProviderBuilder {
	b.provider.defaultMachineFamily = defaultMachineFamily
	return b
}

func (b *MockGKEProviderBuilder) WithAutoprovisioningLocations(autoprovisioningLocations []string) *MockGKEProviderBuilder {
	b.provider.autoprovisioningLocations = autoprovisioningLocations
	return b
}

func (b *MockGKEProviderBuilder) WithValidMachineTypes(validMachineTypes []gce.MachineTypeKey) *MockGKEProviderBuilder {
	b.provider.validMachineTypes = make(map[gce.MachineTypeKey]bool)
	for _, validMachineType := range validMachineTypes {
		b.provider.validMachineTypes[validMachineType] = true
	}
	return b
}

func NewMockGKEProvider(nodeGroups []cloudprovider.NodeGroup, defaultMachineFamily machinetypes.MachineFamily) *MockGKEProvider {
	return &MockGKEProvider{
		nodeGroups:           nodeGroups,
		defaultMachineFamily: defaultMachineFamily,
	}
}

func (m *MockGKEProvider) SetAutopilotEnabled() {
	m.autopilotEnabled = true
}

func (m *MockGKEProvider) IsAutopilotEnabled() bool {
	return m.autopilotEnabled
}

func (m *MockGKEProvider) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	return m.resizableVmInAutopilotEnabled[machineFamily]
}

func (m *MockGKEProvider) SetResizableVmInAutopilotEnabled(machineFamily string, enabled bool) {
	if m.resizableVmInAutopilotEnabled == nil {
		m.resizableVmInAutopilotEnabled = make(map[string]bool)
	}
	m.resizableVmInAutopilotEnabled[machineFamily] = enabled
}

func (m *MockGKEProvider) SetResizableVmWithinPodFamilyEnabled(machineFamily string, enabled bool) {
	if m.resizableVmWithinPodFamilyEnabled == nil {
		m.resizableVmWithinPodFamilyEnabled = make(map[string]bool)
	}
	m.resizableVmWithinPodFamilyEnabled[machineFamily] = enabled
}

func (m *MockGKEProvider) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	return m.resizableVmWithinPodFamilyEnabled[machineFamily]
}

func (m *MockGKEProvider) GetGkeMigs() []*gke.GkeMig {
	var migs []*gke.GkeMig
	for _, ng := range m.nodeGroups {
		migs = append(migs, ng.(*gke.GkeMig))
	}
	return migs
}

func (m *MockGKEProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	return m.defaultMachineFamily
}

func (m *MockGKEProvider) GetAutoprovisioningLocations() []string {
	return m.autoprovisioningLocations
}

func (m *MockGKEProvider) ValidateMachineTypeConfig(machineType, zone string) error {
	key := gce.MachineTypeKey{
		MachineTypeName: machineType,
		Zone:            zone,
	}
	_, isValid := m.validMachineTypes[key]
	if isValid {
		return nil
	}
	return fmt.Errorf("Machine type %s is not available in zone %s.", machineType, zone)
}

func (m *MockGKEProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

type mockMetrics struct {
	mock.Mock
}

func NewMockMetrics() *mockMetrics {
	metrics := &mockMetrics{}
	metrics.On("ObserveNpcHealth", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	metrics.On("ObserveCrdUnhealthinessConditions", mock.Anything).Return().Maybe()
	metrics.On("ObserveNpcCount", mock.Anything).Return().Maybe()
	metrics.On("ObserveNpcRuleCount", mock.Anything).Return().Maybe()
	metrics.On("ObserveInvalidNpcScaleUpOrder").Return().Maybe()
	metrics.On("IncreaseScaledUpNodesPerRule", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	metrics.On("IncreaseScaledDownNodesPerRule", mock.Anything, mock.Anything).Return().Maybe()
	metrics.On("ObserveCcMinTargetNodesReactionLatency", mock.Anything, mock.Anything).Return().Maybe()
	metrics.On("ObserveCcMinTargetNodesProvisioningLatency", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	metrics.On("ObserveCcLongUnprovisionedMinTargetNodesCount", mock.Anything).Return().Maybe()
	return metrics
}

func (o *mockMetrics) ObserveNpcHealth(crdType string, healthy, unhealthy int) {
	o.MethodCalled("ObserveNpcHealth", crdType, healthy, unhealthy)
}

func (o *mockMetrics) ObserveCrdUnhealthinessConditions(samples []metrics.CrdUnhealthinessConditionSample) {
	o.MethodCalled("ObserveCrdUnhealthinessConditions", samples)
}

func (o *mockMetrics) ObserveNpcCount(samples []metrics.NpcCountSample) {
	o.MethodCalled("ObserveNpcCount", samples)
}

func (o *mockMetrics) ObserveNpcRuleCount(samples []metrics.NpcRuleCountSample) {
	o.MethodCalled("ObserveNpcRuleCount", samples)
}

func (o *mockMetrics) ObserveInvalidNpcScaleUpOrder() {
	o.MethodCalled("ObserveInvalidNpcScaleUpOrder")
}

func (o *mockMetrics) IncreaseScaledUpNodesPerRule(crdType string, ruleIndex int, count int) {
	o.MethodCalled("IncreaseScaledUpNodesPerRule", ruleIndex, count, crdType)
}

func (o *mockMetrics) IncreaseScaledDownNodesPerRule(crdType string, ruleIndex int) {
	o.MethodCalled("IncreaseScaledDownNodesPerRule", ruleIndex, crdType)
}

func (o *mockMetrics) ObserveCcMinTargetNodesReactionLatency(definedInPriority bool, duration time.Duration) {
	o.MethodCalled("ObserveCcMinTargetNodesReactionLatency", definedInPriority, duration)
}

func (o *mockMetrics) ObserveCcMinTargetNodesProvisioningLatency(provisioningErrorEncountered string, unhelpable, definedInPriority bool, duration time.Duration) {
	o.MethodCalled("ObserveCcMinTargetNodesProvisioningLatency", provisioningErrorEncountered, unhelpable, definedInPriority, duration)
}

func (o *mockMetrics) ObserveCcLongUnprovisionedMinTargetNodesCount(samples []metrics.CcLongUnprovisionedSample) {
	o.MethodCalled("ObserveCcLongUnprovisionedMinTargetNodesCount", samples)
}
