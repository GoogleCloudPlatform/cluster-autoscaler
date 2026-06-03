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
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

type Tag struct {
	Key   string
	Value string
}

// TpuDriverMode is an enumeration of supported Google TPU driver modes.
type TpuDriverMode string

const (
	// TpuDriverModeDevicePlugin enables managed device plugin mode for Google TPU driver.
	TpuDriverModeDevicePlugin TpuDriverMode = "DevicePlugin"
	// TpuDriverModeDynamicResourceAllocation enables managed DRA mode for Google TPU driver.
	TpuDriverModeDynamicResourceAllocation TpuDriverMode = "DynamicResourceAllocation"
)

// CRD is a representation of different node provisioning CRDs
type CRD interface {
	Label() string
	CrdType() string

	Name() string
	Rules() []rules.Rule
	GroupedRules() [][]rules.Rule
	AutoprovisioningEnabled() bool
	DynamicMaxPodsPerNodeEnabled() bool
	DynamicBootDiskSizeEnabled() bool
	AutopilotManaged() bool
	ScaleUpAnyway() bool
	ServiceAccount() string
	ImageType() string
	NodeVersion() string
	OptimizeRulePriority() bool
	EnsureAllDaemonSetPodsRunning() bool
	TpuDriverMode() TpuDriverMode
	ArchitectureTaintBehavior() string

	SelfServiceMetadata() map[string]string
	UserDefinedLabels() map[string]string
	UserDefinedTaints() []apiv1.Taint
	ResourceManagerTags() []Tag

	ConsolidationDelay() *time.Duration
	ConsolidationThreshold() *int
	GPUConsolidationThreshold() *int

	Conditions() []metav1.Condition
	UpdateConditions(client.Client, []metav1.Condition) error
	TargetNodeCount() *int
	GetRuleCondition(ruleIdx string) []metav1.Condition
}
