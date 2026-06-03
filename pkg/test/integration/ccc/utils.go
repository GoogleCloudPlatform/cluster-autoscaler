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

package ccc

import (
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gkev1beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	realccc "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
)

// CrdBuilder is a builder for the CCC CRD wrapper.
type CrdBuilder struct {
	cc               *v1.ComputeClass
	projectID        string
	autopilotEnabled bool
	dataProvider     any
	optionsTracker   *optstracking.OptionsTracker
}

// NewCccCrdBuilder creates a new CccCrdBuilder for the given ComputeClass with default test settings.
func NewCccCrdBuilder(cc *v1.ComputeClass) *CrdBuilder {
	return &CrdBuilder{
		cc:           cc,
		dataProvider: crd.TestDefaultDataProvider(),
	}
}

// WithProjectId sets the project ID.
func (b *CrdBuilder) WithProjectId(projectId string) *CrdBuilder {
	b.projectID = projectId
	return b
}

// WithAutopilotEnabled sets whether Autopilot is enabled.
func (b *CrdBuilder) WithAutopilotEnabled(enabled bool) *CrdBuilder {
	b.autopilotEnabled = enabled
	return b
}

// WithDataProvider sets the data provider.
func (b *CrdBuilder) WithDataProvider(provider any) *CrdBuilder {
	b.dataProvider = provider
	return b
}

// WithOptionsTracker sets the options tracker.
func (b *CrdBuilder) WithOptionsTracker(tracker *optstracking.OptionsTracker) *CrdBuilder {
	b.optionsTracker = tracker
	return b
}

// Build builds and returns the wrapped CRD.
func (b *CrdBuilder) Build() crd.CRD {
	var prov localDataProvider
	if b.dataProvider != nil {
		prov = b.dataProvider.(localDataProvider)
	}
	return realccc.NewCccCrd(b.cc, b.projectID, b.autopilotEnabled, prov, b.optionsTracker)
}

// localDataProvider matches the unexported ccc.dataProvider interface structurally.
type localDataProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
	GetAIZones() ([]string, error)
	GetStandardZones() ([]string, error)
	GetAutoprovisioningLocations() []string
	TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gkev1beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string
}

// ComputeClassBuilder is a builder for v1.ComputeClass.
type ComputeClassBuilder struct {
	cc *v1.ComputeClass
}

// NewComputeClassBuilder creates a new ComputeClassBuilder with default values.
func NewComputeClassBuilder(name string) *ComputeClassBuilder {
	return &ComputeClassBuilder{
		cc: &v1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: v1.ComputeClassSpec{},
		},
	}
}

// WithNodePoolAutoCreation sets the NodePoolAutoCreation field.
func (b *ComputeClassBuilder) WithNodePoolAutoCreation(enabled bool) *ComputeClassBuilder {
	b.cc.Spec.NodePoolAutoCreation = &v1.NodePoolAutoCreation{
		Enabled: enabled,
	}
	return b
}

// WithPriorities sets the priorities.
func (b *ComputeClassBuilder) WithPriorities(priorities ...v1.Priority) *ComputeClassBuilder {
	b.cc.Spec.Priorities = priorities
	return b
}

// AddPriority appends a priority to the list.
func (b *ComputeClassBuilder) AddPriority(priority v1.Priority) *ComputeClassBuilder {
	b.cc.Spec.Priorities = append(b.cc.Spec.Priorities, priority)
	return b
}

// Build returns the constructed ComputeClass.
func (b *ComputeClassBuilder) Build() *v1.ComputeClass {
	return b.cc
}
