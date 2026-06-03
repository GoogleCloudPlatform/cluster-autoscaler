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
	"context"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
)

type ResumeCall struct {
	MIG       gce.GceRef
	Instances []gce.GceRef
}

type SuspendCall struct {
	MIG       gce.GceRef
	Instances []gce.GceRef
	Force     bool
}

type MockCloudProvider struct {
	BlockResume  chan<- struct{}
	BlockSuspend chan<- struct{}

	mutex         sync.Mutex
	NodeNameToMIG map[string]*gke.GkeMig
	ResumeErr     error
	SuspendErr    error
	Instances     func(gce.GceRef) *gce.GceInstance

	resumeCalls  []ResumeCall
	suspendCalls []SuspendCall
}

func (m *MockCloudProvider) GkeMigForNode(node *v1.Node) (*gke.GkeMig, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.NodeNameToMIG[node.Name], nil
}

func (m *MockCloudProvider) ResumeInstances(mig gce.GceRef, instances []gce.GceRef) error {
	if m.BlockResume != nil {
		m.BlockResume <- struct{}{}
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.resumeCalls = append(m.resumeCalls, ResumeCall{MIG: mig, Instances: instances})
	return m.ResumeErr
}

func (m *MockCloudProvider) GetResumeCalls() []ResumeCall {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	calls := make([]ResumeCall, len(m.resumeCalls))
	copy(calls, m.resumeCalls)
	return calls
}

func (m *MockCloudProvider) SuspendInstances(mig gce.GceRef, instances []gce.GceRef, force bool) error {
	if m.BlockSuspend != nil {
		m.BlockSuspend <- struct{}{}
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.suspendCalls = append(m.suspendCalls, SuspendCall{MIG: mig, Instances: instances, Force: force})
	return m.SuspendErr
}

func (m *MockCloudProvider) GetSuspendCalls() []SuspendCall {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	calls := make([]SuspendCall, len(m.suspendCalls))
	copy(calls, m.suspendCalls)
	return calls
}

func (m *MockCloudProvider) InstanceByRef(ref gce.GceRef) *gce.GceInstance {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.Instances == nil {
		return nil
	}
	return m.Instances(ref)
}

type PatchCall struct {
	Node  *v1.Node
	State csn.NodeState
}

type BufferAssignmentPatchCall struct {
	Node   *v1.Node
	Buffer *v1beta1.CapacityBuffer
}

type MockK8sClient struct {
	mutex      sync.Mutex
	PatchErr   error
	patchCalls []PatchCall

	bufferAssignmentCalls    []BufferAssignmentPatchCall
	BufferAssignmentPatchErr error

	softTaintPatchCalls []SoftTaintPatchCall
	SoftTaintPatchErr   error

	// nodeName -> whether suspension is blocked
	SuspensionBlocked    map[string]bool
	SuspensionBlockedErr error
}

type SoftTaintPatchCall struct {
	Node       *v1.Node
	TaintCount int
}

func (m *MockK8sClient) ApplyNodePatch(_ context.Context, node *v1.Node, desiredState csn.NodeState) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.patchCalls = append(m.patchCalls, PatchCall{Node: node, State: desiredState})
	return m.PatchErr
}

func (m *MockK8sClient) GetPatchCalls() []PatchCall {
	return m.patchCalls
}

func (m *MockK8sClient) IsSuspensionBlocked(_ context.Context, nodeName string) (bool, error) {
	return m.SuspensionBlocked[nodeName], m.SuspensionBlockedErr
}

func (m *MockK8sClient) ApplyNodeToBufferAssignmentPatch(_ context.Context, node *v1.Node, buffer *v1beta1.CapacityBuffer) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.bufferAssignmentCalls = append(m.bufferAssignmentCalls, BufferAssignmentPatchCall{Node: node, Buffer: buffer})
	return m.BufferAssignmentPatchErr
}

func (m *MockK8sClient) GetBufferAssignmentPatchCalls() []BufferAssignmentPatchCall {
	return m.bufferAssignmentCalls
}

func (m *MockK8sClient) ApplyAdditionalSoftTaintsPatch(_ context.Context, node *v1.Node, taintCount int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.softTaintPatchCalls = append(m.softTaintPatchCalls, SoftTaintPatchCall{Node: node, TaintCount: taintCount})
	return m.SoftTaintPatchErr
}

func (m *MockK8sClient) GetSoftTaintPatchCalls() []SoftTaintPatchCall {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.softTaintPatchCalls
}
