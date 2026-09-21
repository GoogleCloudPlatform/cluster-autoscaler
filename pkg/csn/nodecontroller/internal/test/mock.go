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
	"slices"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/utils/set"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

type ResumeCall struct {
	MIG       gce.GceRef
	Instances []gce.GceRef
}

type PollUntilCall struct {
	Action    gceclient.InstanceAction
	MIG       gce.GceRef
	Instances []gce.GceRef
}

type SuspendCall struct {
	MIG       gce.GceRef
	Instances []gce.GceRef
	Force     bool
}

// ManagedInstance builds a settled (no action in flight) ManagedInstance whose
// current and target statuses are both status.
func ManagedInstance(name, status string) *gceclient.ManagedInstance {
	return &gceclient.ManagedInstance{
		Name:           name,
		InstanceStatus: status,
		TargetStatus:   status,
		CurrentAction:  "NONE",
	}
}

type MockCloudProvider struct {
	BlockResume  chan<- struct{}
	BlockSuspend chan<- struct{}
	BlockPoll    chan<- struct{}

	mutex         sync.Mutex
	NodeNameToMIG map[string]*gke.GkeMig
	ResumeErr     error
	SuspendErr    error
	PollUntilErr  error
	// PollUntilReadyBeforeErr names the instances PollUntilActionStops reports as
	// ready before returning PollUntilErr, mirroring a poll that makes partial
	// progress and then times out. Ignored when PollUntilErr is nil, in which case
	// every instance is reported ready.
	PollUntilReadyBeforeErr set.Set[string]
	Instances               func(gce.GceRef) *gce.GceInstance
	// ManagedInstances is what FetchManagedInstances reports, keyed by MIG.
	ManagedInstances         map[gce.GceRef][]*gceclient.ManagedInstance
	FetchManagedInstancesErr error
	// NonBlockingError, when set, is yielded by PollUntilActionStops for every instance
	// that does not become ready. Its InstanceStatus is overwritten with the status
	// Instances reports for the instance. Ignored when PollUntilErr is nil.
	NonBlockingError *gceclient.NonBlockingInstanceError

	resumeCalls    []ResumeCall
	suspendCalls   []SuspendCall
	pollUntilCalls []PollUntilCall
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
	m.resumeCalls = append(m.resumeCalls, ResumeCall{MIG: mig, Instances: instances})
	resErr := m.ResumeErr
	m.mutex.Unlock()
	return resErr
}

// PollUntilActionStops mimics the real client's iterator: instances that make it are reported
// PollCompleted, and the ones that do not are reported PollRetrying (when configured) followed by
// PollAborted carrying PollUntilErr, which is what the real client reports on timeout. A cancelled
// ctx cuts the iteration short and reports the remaining instances as PollAborted with ctx.Err().
func (m *MockCloudProvider) PollUntilActionStops(ctx context.Context, action gceclient.InstanceAction, mig gce.GceRef, instances []gce.GceRef) gceclient.ActionPollSeq {
	return func(yield func(gce.GceRef, gceclient.PollUpdate) bool) {
		if m.BlockPoll != nil {
			// Selecting on ctx keeps a test that cancels mid-poll from deadlocking here,
			// just like the real client which stops waiting as soon as ctx is done.
			select {
			case m.BlockPoll <- struct{}{}:
			case <-ctx.Done():
			}
		}
		m.mutex.Lock()
		nonBlockingErr := m.NonBlockingError
		instancesFunc := m.Instances
		m.pollUntilCalls = append(m.pollUntilCalls, PollUntilCall{Action: action, MIG: mig, Instances: instances})
		pollErr := m.PollUntilErr
		readyBeforeErr := m.PollUntilReadyBeforeErr
		m.mutex.Unlock()

		for _, ref := range instances {
			if err := ctx.Err(); err != nil {
				// Cancelled: the real client gives up on everything still pending.
				if !yield(ref, gceclient.PollAborted{Err: err}) {
					return
				}
				continue
			}
			if pollErr == nil || readyBeforeErr.Has(ref.Name) {
				// Ready instances are never reported as failing, matching the real client.
				if !yield(ref, gceclient.PollCompleted{}) {
					return
				}
				continue
			}
			if nonBlockingErr != nil {
				// Copied so that the per-instance status does not leak into the next instance.
				perInstanceErr := *nonBlockingErr
				perInstanceErr.InstanceStatus = ""
				if instancesFunc != nil {
					if inst := instancesFunc(ref); inst != nil {
						perInstanceErr.InstanceStatus = inst.GCEStatus
					}
				}
				if !yield(ref, gceclient.PollRetrying{Err: &perInstanceErr}) {
					return
				}
			}
			if !yield(ref, gceclient.PollAborted{Err: pollErr}) {
				return
			}
		}
	}
}

func (m *MockCloudProvider) GetResumeCalls() []ResumeCall {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	calls := make([]ResumeCall, len(m.resumeCalls))
	copy(calls, m.resumeCalls)
	return calls
}

func (m *MockCloudProvider) GetPollUntilCalls() []PollUntilCall {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	calls := make([]PollUntilCall, len(m.pollUntilCalls))
	copy(calls, m.pollUntilCalls)
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

func (m *MockCloudProvider) FetchManagedInstances(mig gce.GceRef, _ string) ([]*gceclient.ManagedInstance, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.ManagedInstances[mig], m.FetchManagedInstancesErr
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
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

// MockNodeLister serves nodes the way the informer cache does in production:
// straight from memory, and reporting the nodes it does not know about as gone.
type MockNodeLister struct {
	mutex sync.Mutex
	// Nodes is keyed by node name.
	Nodes map[string]*v1.Node
	// GetFunc, when set, answers every lookup instead of Nodes, which is how a
	// test describes a node whose readiness changes as the lookups go.
	GetFunc  func(nodeName string) (*v1.Node, error)
	getCalls []string
}

// ReadyNodeLister returns a lister that reports every node it is asked about as
// ready, for the tests that are not about readiness.
func ReadyNodeLister() *MockNodeLister {
	return &MockNodeLister{
		GetFunc: func(nodeName string) (*v1.Node, error) {
			return CreateNode(nodeName, ReadyOpt(true)), nil
		},
	}
}

// Get returns the node, or a NotFound error if the lister does not know it.
func (m *MockNodeLister) Get(nodeName string) (*v1.Node, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.getCalls = append(m.getCalls, nodeName)
	if m.GetFunc != nil {
		return m.GetFunc(nodeName)
	}
	node, ok := m.Nodes[nodeName]
	if !ok {
		return nil, apierrors.NewNotFound(v1.Resource("node"), nodeName)
	}
	return node, nil
}

// GetCalls returns the names looked up so far, in order, which tells which
// nodes were watched and which were taken at face value.
func (m *MockNodeLister) GetCalls() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return slices.Clone(m.getCalls)
}

type MockBackoff struct {
	BackedOffMigs map[string]bool
}

func (m *MockBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	return time.Time{}
}

func (m *MockBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	if nodeGroup == nil {
		return base_backoff.Status{}
	}
	return base_backoff.Status{IsBackedOff: m.BackedOffMigs[nodeGroup.Id()]}
}

func (m *MockBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
}
func (m *MockBackoff) RemoveStaleBackoffData(currentTime time.Time) {}

func (m *MockBackoff) ReportResumptionError(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorCode, errorMessage, instanceStatus string) {
}
