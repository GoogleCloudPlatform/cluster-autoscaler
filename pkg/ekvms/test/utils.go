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

package test

import (
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

// EkNode32 creates an EK node with specified allocatable, capacity
// and instance label set to standard-32.
func EkNode32(name string, milliCpu, bytes int64) *v1.Node {
	b := ResizableNodeBuilder{node: test.BuildTestNode(name, milliCpu, bytes)}
	return b.WithStandard32Capacity().WithSupportedMachineType("ek-standard-32").WithMachineFamily("ek").Build()
}

// E4aNode32 creates an E4A node with specified allocatable, capacity
// and instance label set to standard-32.
func E4aNode32(name string, milliCpu, bytes int64) *v1.Node {
	b := ResizableNodeBuilder{node: test.BuildTestNode(name, milliCpu, bytes)}
	return b.WithStandard32Capacity().WithSupportedMachineType("e4a-standard-32").WithMachineFamily("e4a").Build()
}

// EkNode8 creates an EK node with specified allocatable, capacity
// and instance label set to standard-8.
func EkNode8(name string, milliCpu, bytes int64) *v1.Node {
	b := ResizableNodeBuilder{node: test.BuildTestNode(name, milliCpu, bytes)}
	return b.WithStandard8Capacity().WithSupportedMachineType("ek-standard-8").WithMachineFamily("ek").Build()
}

// E4aNode8 creates an E4A node with specified allocatable, capacity
// and instance label set to standard-8.
func E4aNode8(name string, milliCpu, bytes int64) *v1.Node {
	b := ResizableNodeBuilder{node: test.BuildTestNode(name, milliCpu, bytes)}
	return b.WithStandard8Capacity().WithSupportedMachineType("e4a-standard-8").WithMachineFamily("e4a").Build()
}

// ResizableNodeBuilder builds resizable nodes for test purposes.
type ResizableNodeBuilder struct {
	node *v1.Node
}

// NewResizableNodeBuilderFromNode creates new resizable node builder from a node.
func NewResizableNodeBuilderFromNode(node *v1.Node) *ResizableNodeBuilder {
	return &ResizableNodeBuilder{node: node}
}

// NewResizableNodeBuilder creates new resizable node builder with specified name and resources.
func NewResizableNodeBuilder(name string, milliCpu, giBytes int64) *ResizableNodeBuilder {
	b := ResizableNodeBuilder{node: test.BuildTestNode(name, milliCpu, giBytes*1024*1024*1024)}
	return b.WithStandard32Capacity()
}

// WithStandard32Capacity sets node capacity to standard-32.
func (b *ResizableNodeBuilder) WithStandard32Capacity() *ResizableNodeBuilder {
	b.node.Status.Capacity[v1.ResourceCPU] = *resource.NewMilliQuantity(32000, resource.DecimalSI)
	b.node.Status.Capacity[v1.ResourceMemory] = *resource.NewQuantity(128*1024*1024, resource.DecimalSI)
	return b
}

// WithStandard8Capacity sets node capacity to standard-8.
func (b *ResizableNodeBuilder) WithStandard8Capacity() *ResizableNodeBuilder {
	b.node.Status.Capacity[v1.ResourceCPU] = *resource.NewMilliQuantity(8000, resource.DecimalSI)
	b.node.Status.Capacity[v1.ResourceMemory] = *resource.NewQuantity(32*1024*1024, resource.DecimalSI)
	return b
}

func (b *ResizableNodeBuilder) WithProvider(providerId string) *ResizableNodeBuilder {
	b.node.Spec.ProviderID = providerId
	return b
}

func (b *ResizableNodeBuilder) WithSupportedMachineType(supportedMachineType string) *ResizableNodeBuilder {
	b.updateLabels(v1.LabelInstanceTypeStable, supportedMachineType)
	return b
}

func (b *ResizableNodeBuilder) WithMachineFamily(machineFamily string) *ResizableNodeBuilder {
	b.updateLabels(gkelabels.MachineFamilyLabel, machineFamily)
	return b
}

func (b *ResizableNodeBuilder) WithTaint(taint v1.Taint) *ResizableNodeBuilder {
	b.node.Spec.Taints = append(b.node.Spec.Taints, taint)
	return b
}

func (b *ResizableNodeBuilder) WithLabel(label string, value string) *ResizableNodeBuilder {
	b.updateLabels(label, value)
	return b
}

func (b *ResizableNodeBuilder) WithAnnotations(annotations map[string]string) *ResizableNodeBuilder {
	b.node.Annotations = annotations
	return b
}

func (b *ResizableNodeBuilder) WithReadyStatus() *ResizableNodeBuilder {
	for i, c := range b.node.Status.Conditions {
		if c.Type == v1.NodeReady {
			b.node.Status.Conditions[i].Status = v1.ConditionTrue
			return b
		}
	}
	b.node.Status.Conditions = append(b.node.Status.Conditions, v1.NodeCondition{Type: v1.NodeReady, Status: v1.ConditionTrue})
	return b
}

func (b *ResizableNodeBuilder) Build() *v1.Node {
	return b.node
}

func (b *ResizableNodeBuilder) updateLabels(label string, value string) {
	existingLabels := b.node.GetLabels()
	if existingLabels != nil {
		existingLabels[label] = value
	} else {
		existingLabels = map[string]string{label: value}
	}
	b.node.SetLabels(existingLabels)
}

type MockMachineFamilyProvider struct {
	mock.Mock
}

func NewMockMachineFamilyProvider(defaultFamily machinetypes.MachineFamily) *MockMachineFamilyProvider {
	mfProvider := MockMachineFamilyProvider{}
	mfProvider.On("GetAutoprovisioningDefaultFamily").Return(defaultFamily)
	return &mfProvider
}

func (m *MockMachineFamilyProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	args := m.Called()
	return args.Get(0).(machinetypes.MachineFamily)
}
