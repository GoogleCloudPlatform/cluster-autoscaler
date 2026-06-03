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

package crd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	crdRules "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	ctrl_client "sigs.k8s.io/controller-runtime/pkg/client"
)

type testCrd struct {
	label                         string
	crdType                       string
	name                          string
	groupedRules                  [][]crdRules.Rule
	autoprovisioningEnabled       bool
	autopilotManaged              bool
	scaleUpAnyway                 bool
	serviceAccount                string
	imageType                     string
	optimizeRulePriority          bool
	ensureAllDaemonSetPodsRunning bool
	dynamicMaxPodsPerNodeEnabled  bool
	dynamicBootDiskSizeEnabled    bool
	tpuDriverMode                 TpuDriverMode
	selfServiceLabels             map[string]string
	userDefinedLabels             map[string]string
	userDefinedTaints             []apiv1.Taint
	resourceManagerTags           []Tag
	conditions                    []metav1.Condition
	ruleConditions                map[string][]metav1.Condition

	consolidationDelay        *time.Duration
	consolidationThreshold    *int
	consolidationGpuThreshold *int
	nodeVersion               string
	targetNodeCount           *int
	architectureTaintBehavior string
}

func (t *testCrd) ArchitectureTaintBehavior() string {
	return t.architectureTaintBehavior
}

func (t *testCrd) Label() string {
	return t.label
}

func (t *testCrd) CrdType() string {
	return t.crdType
}

func (t *testCrd) Name() string {
	return t.name
}

func (t *testCrd) Rules() []crdRules.Rule {
	if t.groupedRules == nil {
		return nil
	}
	rules := []crdRules.Rule{}
	for _, ruleGroup := range t.groupedRules {
		rules = append(rules, ruleGroup...)
	}
	return rules
}

func (t *testCrd) GroupedRules() [][]crdRules.Rule {
	return t.groupedRules
}

func (t *testCrd) AutoprovisioningEnabled() bool {
	return t.autoprovisioningEnabled
}

func (t *testCrd) DynamicMaxPodsPerNodeEnabled() bool {
	return t.dynamicMaxPodsPerNodeEnabled
}

func (t *testCrd) DynamicBootDiskSizeEnabled() bool {
	return t.dynamicBootDiskSizeEnabled
}

func (t *testCrd) AutopilotManaged() bool {
	return t.autopilotManaged
}

func (t *testCrd) ScaleUpAnyway() bool {
	return t.scaleUpAnyway
}

func (t *testCrd) ServiceAccount() string {
	return t.serviceAccount
}

func (t *testCrd) ImageType() string {
	return t.imageType
}

func (t *testCrd) NodeVersion() string {
	return t.nodeVersion
}

func (t *testCrd) OptimizeRulePriority() bool {
	return t.optimizeRulePriority
}

func (t *testCrd) EnsureAllDaemonSetPodsRunning() bool {
	return t.ensureAllDaemonSetPodsRunning
}

func (t *testCrd) Conditions() []metav1.Condition {
	return t.conditions
}

func (t *testCrd) UpdateConditions(_ client.Client, conditions []metav1.Condition) error {
	t.conditions = conditions
	return nil
}

func (t *testCrd) ConsolidationDelay() *time.Duration {
	return t.consolidationDelay
}

func (t *testCrd) GetRuleCondition(ruleIdx string) []metav1.Condition {
	return t.ruleConditions[ruleIdx]
}

func (t *testCrd) ConsolidationThreshold() *int {
	return t.consolidationThreshold
}

func (t *testCrd) GPUConsolidationThreshold() *int {
	return t.consolidationGpuThreshold
}

func (t *testCrd) SelfServiceMetadata() map[string]string {
	return t.selfServiceLabels
}

func (t *testCrd) UserDefinedLabels() map[string]string {
	return t.userDefinedLabels
}

func (t *testCrd) UserDefinedTaints() []apiv1.Taint {
	return t.userDefinedTaints
}

func (t *testCrd) ResourceManagerTags() []Tag {
	return t.resourceManagerTags
}

func (t *testCrd) TpuDriverMode() TpuDriverMode {
	return t.tpuDriverMode
}

// TargetNodeCount returns the target node count for the ComputeClass.
func (t *testCrd) TargetNodeCount() *int {
	return t.targetNodeCount
}

type TestCrdOption func(crd *testCrd)

func WithLabel(label string) TestCrdOption {
	return func(crd *testCrd) {
		crd.label = label
	}
}

func WithCrdType(crdType string) TestCrdOption {
	return func(crd *testCrd) {
		crd.crdType = crdType
	}
}

func WithName(name string) TestCrdOption {
	return func(crd *testCrd) {
		crd.name = name
	}
}

func WithRules(rules []crdRules.Rule) TestCrdOption {
	var groupedRules [][]crdRules.Rule
	for _, rule := range rules {
		groupedRules = append(groupedRules, []crdRules.Rule{rule})
	}

	return func(crd *testCrd) {
		crd.groupedRules = groupedRules
	}
}

func WithGroupedRules(groupedRules [][]crdRules.Rule) TestCrdOption {
	return func(crd *testCrd) {
		crd.groupedRules = groupedRules
	}
}

func WithAutoprovisioningEnabled() TestCrdOption {
	return func(crd *testCrd) {
		crd.autoprovisioningEnabled = true
	}
}

func WithDynamicMaxPodsPerNodeEnabled() TestCrdOption {
	return func(crd *testCrd) {
		crd.dynamicMaxPodsPerNodeEnabled = true
	}
}

func WithDynamicBootDiskSizeEnabled() TestCrdOption {
	return func(crd *testCrd) {
		crd.dynamicBootDiskSizeEnabled = true
	}
}

func WithAutopilotManaged() TestCrdOption {
	return func(crd *testCrd) {
		crd.autopilotManaged = true
	}
}

func WithScaleUpAnyway() TestCrdOption {
	return func(crd *testCrd) {
		crd.scaleUpAnyway = true
	}
}

func WithServiceAccount(serviceAccount string) TestCrdOption {
	return func(crd *testCrd) {
		crd.serviceAccount = serviceAccount
	}
}

func WithImageType(imageType string) TestCrdOption {
	return func(crd *testCrd) {
		crd.imageType = imageType
	}
}

func WithNodeVersion(nodeVersion string) TestCrdOption {
	return func(crd *testCrd) {
		crd.nodeVersion = nodeVersion
	}
}

func WithOptimizeRulePriority() TestCrdOption {
	return func(crd *testCrd) {
		crd.optimizeRulePriority = true
	}
}

func WithEnsureAllDaemonSetPodsRunning() TestCrdOption {
	return func(crd *testCrd) {
		crd.ensureAllDaemonSetPodsRunning = true
	}
}

func WithConditions(conditions []metav1.Condition) TestCrdOption {
	return func(crd *testCrd) {
		crd.conditions = conditions
	}
}

func WithRuleConditions(ruleIdx string, conditions []metav1.Condition) TestCrdOption {
	return func(crd *testCrd) {
		if crd.ruleConditions == nil {
			crd.ruleConditions = make(map[string][]metav1.Condition)
		}
		crd.ruleConditions[ruleIdx] = conditions
	}
}

func WithConsolidationDelay(delay time.Duration) TestCrdOption {
	return func(crd *testCrd) {
		crd.consolidationDelay = &delay
	}
}

func WithConsolidationThreshold(threshold int) TestCrdOption {
	return func(crd *testCrd) {
		crd.consolidationThreshold = &threshold
	}
}

func WithGPUConsolidationThreshold(threshold int) TestCrdOption {
	return func(crd *testCrd) {
		crd.consolidationGpuThreshold = &threshold
	}
}

func WithSelfServiceMetadata(labels map[string]string) TestCrdOption {
	return func(crd *testCrd) {
		crd.selfServiceLabels = labels
	}
}

func WithUserDefinedLabels(labels map[string]string) TestCrdOption {
	return func(crd *testCrd) {
		crd.userDefinedLabels = labels
	}
}

func WithUserDefinedTaints(taints []apiv1.Taint) TestCrdOption {
	return func(crd *testCrd) {
		crd.userDefinedTaints = taints
	}
}

func WithResourceManagerTags(tags []Tag) TestCrdOption {
	return func(crd *testCrd) {
		crd.resourceManagerTags = tags
	}
}

func WithTpuDriverMode(mode TpuDriverMode) TestCrdOption {
	return func(crd *testCrd) {
		crd.tpuDriverMode = mode
	}
}

func WithTargetNodeCount(targetNodeCount *int) TestCrdOption {
	return func(crd *testCrd) {
		crd.targetNodeCount = targetNodeCount
	}
}

func WithArchitectureTaintBehavior(behavior string) TestCrdOption {
	return func(crd *testCrd) {
		crd.architectureTaintBehavior = behavior
	}
}

func NewTestCrd(options ...TestCrdOption) CRD {
	crd := &testCrd{}
	for _, o := range options {
		o(crd)
	}
	return crd
}

func CompareCrd(t *testing.T, c1, c2 CRD) {
	assert.Equal(t, c1.Label(), c2.Label())
	assert.Equal(t, c1.Name(), c2.Name())
	assert.Equal(t, c1.Rules(), c2.Rules())
	assert.Equal(t, c1.GroupedRules(), c2.GroupedRules())
	assert.Equal(t, c1.AutoprovisioningEnabled(), c2.AutoprovisioningEnabled())
	assert.Equal(t, c1.AutopilotManaged(), c2.AutopilotManaged())
	assert.Equal(t, c1.ScaleUpAnyway(), c2.ScaleUpAnyway())
	assert.Equal(t, c1.ServiceAccount(), c2.ServiceAccount())
	assert.Equal(t, c1.ImageType(), c2.ImageType())
	assert.Equal(t, c1.NodeVersion(), c2.NodeVersion())
	assert.Equal(t, c1.OptimizeRulePriority(), c2.OptimizeRulePriority())
	assert.Equal(t, c1.Conditions(), c2.Conditions())
	assert.Equal(t, c1.DynamicMaxPodsPerNodeEnabled(), c2.DynamicMaxPodsPerNodeEnabled())
	assert.Equal(t, c1.DynamicBootDiskSizeEnabled(), c2.DynamicBootDiskSizeEnabled())
	assert.ElementsMatch(t, c1.SelfServiceMetadata(), c2.SelfServiceMetadata())
	assert.Equal(t, c1.UserDefinedLabels(), c2.UserDefinedLabels())
	assert.Equal(t, c1.UserDefinedTaints(), c2.UserDefinedTaints())
	assert.Equal(t, c1.ResourceManagerTags(), c2.ResourceManagerTags())
	assert.Equal(t, c1.TargetNodeCount(), c2.TargetNodeCount())
	assert.Equal(t, c1.ArchitectureTaintBehavior(), c2.ArchitectureTaintBehavior())
}

func TestDefaultDataProvider() *testDataProvider {
	return &testDataProvider{
		autoprovisioningLocations: []string{"us-central1-a", "us-central1-ai2"},
		standardZones:             []string{"us-central1-a", "us-central1-b"},
		aiZones:                   []string{"us-central1-ai1", "us-central1-ai2"},
	}
}

func TestDataProvider(standardZones []string, aiZones []string, autoprovisioningLocations []string) *testDataProvider {
	return &testDataProvider{
		autoprovisioningLocations: autoprovisioningLocations,
		standardZones:             standardZones,
		aiZones:                   aiZones,
	}
}

type testDataProvider struct {
	autoprovisioningLocations []string
	standardZones             []string
	aiZones                   []string
	trimmedLocations          []string
	standardErr               error
	aiErr                     error
}

func (p *testDataProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

func (p *testDataProvider) GetAIZones() ([]string, error) {
	if p.aiErr != nil {
		return nil, p.aiErr
	}
	return p.aiZones, nil
}

func (p *testDataProvider) GetStandardZones() ([]string, error) {
	if p.standardErr != nil {
		return nil, p.standardErr
	}
	return p.standardZones, nil
}

func (p *testDataProvider) GetAutoprovisioningLocations() []string {
	return p.autoprovisioningLocations
}

func (p *testDataProvider) SetStandardError(err error) {
	p.standardErr = err
}

func (p *testDataProvider) SetAIError(err error) {
	p.aiErr = err
}

func (p *testDataProvider) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	return p.trimmedLocations

}

// MockCRDStatus is a shared test utility that implements CRDStatus.
// It embeds mock.Mock to allow for testify expectations, but also maintains
// explicit fields similar to previous manual mocks to support existing tests.
type MockCRDStatus struct {
	mock.Mock

	UpdateRuleConditionsCalled bool
	RuleIdx                    string
	Conditions                 []metav1.Condition
	ExistingRuleConditions     map[string][]metav1.Condition
}

// NewMockCRDStatus creates a new MockCRDStatus with optional existing rule conditions.
func NewMockCRDStatus(existingRuleConditions map[string][]metav1.Condition) *MockCRDStatus {
	m := &MockCRDStatus{
		ExistingRuleConditions: existingRuleConditions,
	}
	m.On("UpdateConditions", mock.Anything).Return().Maybe()
	m.On("UpdateResourceInfo", mock.Anything).Return().Maybe()
	m.On("UpdateRuleConditions", mock.Anything, mock.Anything).Return().Maybe()
	m.On("UpdateRuleResourceInfo", mock.Anything, mock.Anything).Return().Maybe()
	m.On("UpdateRuleScalingHistory", mock.Anything, mock.Anything).Return().Maybe()
	m.On("ResetAllScalingHistories").Return().Maybe()
	m.On("ResetAllResourceInfo").Return().Maybe()
	m.On("GetConditions").Return(([]metav1.Condition)(nil)).Maybe()
	m.On("GetRuleConditions", mock.Anything).Return(([]metav1.Condition)(nil)).Maybe()
	m.On("GetRuleScalingHistory", mock.Anything).Return((*ScalingEventsHistory)(nil)).Maybe()
	m.On("GetCRDStatusPatch").Return((ctrl_client.Object)(nil)).Maybe()
	m.On("DeepCopyObject").Return((ctrl_client.Object)(nil)).Maybe()
	return m
}

func (m *MockCRDStatus) UpdateConditions(conditions []metav1.Condition) {
	m.Called(conditions)
}

func (m *MockCRDStatus) UpdateResourceInfo(info ResourceInfo) {
	m.Called(info)
}

func (m *MockCRDStatus) UpdateRuleConditions(ruleIdx string, conditions []metav1.Condition) {
	m.UpdateRuleConditionsCalled = true
	m.RuleIdx = ruleIdx
	m.Conditions = conditions
	m.Called(ruleIdx, conditions)
}

func (m *MockCRDStatus) UpdateRuleResourceInfo(ruleIdx string, info ResourceInfo) {
	m.Called(ruleIdx, info)
}

func (m *MockCRDStatus) UpdateRuleScalingHistory(ruleIdx string, history ScalingEventsHistory) {
	m.Called(ruleIdx, history)
}

func (m *MockCRDStatus) ResetAllScalingHistories() {
	m.Called()
}

func (m *MockCRDStatus) ResetAllResourceInfo() {
	m.Called()
}

func (m *MockCRDStatus) GetConditions() []metav1.Condition {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]metav1.Condition)
}

func (m *MockCRDStatus) GetRuleConditions(ruleIdx string) []metav1.Condition {
	if m.ExistingRuleConditions != nil {
		if conds, ok := m.ExistingRuleConditions[ruleIdx]; ok {
			return conds
		}
	}
	// Fallback to mock mechanism if explicitly setup, else nil
	args := m.Called(ruleIdx)
	if len(args) == 0 {
		return nil
	}
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]metav1.Condition)
}

func (m *MockCRDStatus) GetRuleScalingHistory(ruleIdx string) *ScalingEventsHistory {
	args := m.Called(ruleIdx)
	if len(args) == 0 {
		return nil
	}
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*ScalingEventsHistory)
}

func (m *MockCRDStatus) GetCRDStatusPatch() ctrl_client.Object {
	args := m.Called()
	if len(args) == 0 {
		return nil
	}
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(ctrl_client.Object)
}

func (m *MockCRDStatus) DeepCopyObject() ctrl_client.Object {
	args := m.Called()
	if len(args) == 0 {
		return nil
	}
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(ctrl_client.Object)
}
