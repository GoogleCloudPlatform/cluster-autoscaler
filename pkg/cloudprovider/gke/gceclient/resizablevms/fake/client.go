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

// Package fake provides an in-memory fake implementation of
// resizablevms.Client for tests.
package fake

import (
	"context"
	"maps"
	"slices"
	"sync"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
)

// ResizeCall records a single ResizeVm request received by the fake client.
type ResizeCall struct {
	Instance             gce.GceRef
	DesiredCPUMillicores int64
	DesiredMemoryKB      int64
}

// MachineTypeResolver resolves the GCE machine type name for an instance
// reference.
type MachineTypeResolver func(instance gce.GceRef) string

// Option configures a fake resizablevms Client.
type Option func(*Client)

// WithMachineTypeResolver configures how unconfigured instances resolve their
// initial max VM size from MaxResizableVmSizeProvider.
func WithMachineTypeResolver(resolver MachineTypeResolver) Option {
	return func(c *Client) {
		c.machineTypeResolver = resolver
	}
}

// WithDefaultSize sets an explicit default VM size for instances without
// per-instance state.
func WithDefaultSize(size ekvmsize.VmSize) Option {
	return func(c *Client) {
		c.defaultSize = size
		c.hasDefaultSize = true
	}
}

// Client is an in-memory stateful fake implementation of resizablevms.Client.
type Client struct {
	machineTypeResolver MachineTypeResolver

	mu             sync.Mutex
	defaultSize    ekvmsize.VmSize
	hasDefaultSize bool
	statusOnResize ekvmtypes.ResizeStatus

	states     map[gce.GceRef]ekvmtypes.ResizableVmState
	resizeErrs map[gce.GceRef]error
	getErrs    map[gce.GceRef]error

	bulkErr   error
	getErr    error
	resizeErr error

	bulkCalls   int
	getCalls    int
	resizeCalls []ResizeCall
}

var _ resizablevms.Client = (*Client)(nil)

// NewClient constructs a stateful fake resizablevms.Client.
func NewClient(opts ...Option) *Client {
	c := &Client{
		statusOnResize: ekvmtypes.ResizeStatusAtIntent,
		states:         make(map[gce.GceRef]ekvmtypes.ResizableVmState),
		resizeErrs:     make(map[gce.GceRef]error),
		getErrs:        make(map[gce.GceRef]error),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// SetDefaultSize sets the default VM size returned for instances without
// explicit state.
func (c *Client) SetDefaultSize(size ekvmsize.VmSize) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defaultSize = size
	c.hasDefaultSize = true
}

// SetVMState sets the state reported for instance.
func (c *Client) SetVMState(instance gce.GceRef, state ekvmtypes.ResizableVmState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states[instance] = state
}

// SetVMSize sets instance's reported size with ResizeStatusAtIntent.
func (c *Client) SetVMSize(instance gce.GceRef, size ekvmsize.VmSize) {
	c.SetVMState(instance, ekvmtypes.ResizableVmState{
		Size:   size,
		Status: ekvmtypes.ResizeStatusAtIntent,
	})
}

// DeleteVMState clears any recorded state for instance.
func (c *Client) DeleteVMState(instance gce.GceRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.states, instance)
}

// VMState returns the recorded state for instance if present.
func (c *Client) VMState(instance gce.GceRef) (ekvmtypes.ResizableVmState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[instance]
	return st, ok
}

// SetStatusOnResize overrides the ResizeStatus written after a successful
// ResizeVm call.
func (c *Client) SetStatusOnResize(status ekvmtypes.ResizeStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statusOnResize = status
}

// SetResizeError configures an error returned by all ResizeVm calls unless
// overridden per instance.
func (c *Client) SetResizeError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeErr = err
}

// SetInstanceResizeError configures an error returned by ResizeVm for a
// specific instance.
func (c *Client) SetInstanceResizeError(instance gce.GceRef, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		delete(c.resizeErrs, instance)
		return
	}
	c.resizeErrs[instance] = err
}

// SetGetError configures an error returned by all GetCurrentResizableVmState
// calls unless overridden per instance.
func (c *Client) SetGetError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getErr = err
}

// SetInstanceGetError configures an error returned by
// GetCurrentResizableVmState for a specific instance.
func (c *Client) SetInstanceGetError(instance gce.GceRef, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		delete(c.getErrs, instance)
		return
	}
	c.getErrs[instance] = err
}

// SetBulkFetchError configures an error returned by
// BulkFetchCurrentResizableVmStates.
func (c *Client) SetBulkFetchError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bulkErr = err
}

// ResizeCalls returns a copy of all ResizeVm calls recorded so far.
func (c *Client) ResizeCalls() []ResizeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.resizeCalls)
}

// ResetResizeCalls clears the recorded ResizeVm call history.
func (c *Client) ResetResizeCalls() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeCalls = nil
}

// CallCounts returns the number of bulk fetch, get state, and resize calls
// made.
func (c *Client) CallCounts() (bulkCalls, getCalls, resizeCalls int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bulkCalls, c.getCalls, len(c.resizeCalls)
}

// ResizeVm implements resizablevms.Client.
func (c *Client) ResizeVm(_ context.Context, instance gce.GceRef, desiredCpuMillicores, desiredMemoryKb int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.resizeCalls = append(c.resizeCalls, ResizeCall{
		Instance:             instance,
		DesiredCPUMillicores: desiredCpuMillicores,
		DesiredMemoryKB:      desiredMemoryKb,
	})
	if err, ok := c.resizeErrs[instance]; ok && err != nil {
		return err
	}
	if c.resizeErr != nil {
		return c.resizeErr
	}
	c.states[instance] = ekvmtypes.ResizableVmState{
		Size: ekvmsize.VmSize{
			MilliCpus: desiredCpuMillicores,
			KBytes:    desiredMemoryKb,
		},
		Status: c.statusOnResize,
	}
	return nil
}

// GetCurrentResizableVmState implements resizablevms.Client.
func (c *Client) GetCurrentResizableVmState(provider resizablevms.MaxResizableVmSizeProvider, instance gce.GceRef) (ekvmtypes.ResizableVmState, error) {
	if state, ok, err := c.lookupOrDefaultState(instance); ok || err != nil {
		return state, err
	}

	if c.machineTypeResolver == nil || provider == nil {
		return ekvmtypes.ResizableVmState{}, nil
	}
	mt := c.machineTypeResolver(instance)
	if mt == "" {
		return ekvmtypes.ResizableVmState{}, nil
	}
	maxSize, err := provider.GetMaxResizableVmSizeByMachineType(mt)
	if err != nil {
		return ekvmtypes.ResizableVmState{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if state, ok := c.states[instance]; ok {
		return state, nil
	}
	state := ekvmtypes.ResizableVmState{
		Size:   maxSize,
		Status: ekvmtypes.ResizeStatusAtIntent,
	}
	c.states[instance] = state
	return state, nil
}

// BulkFetchCurrentResizableVmStates implements resizablevms.Client.
func (c *Client) BulkFetchCurrentResizableVmStates(_ resizablevms.MaxResizableVmSizeProvider, _, _ string) (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.bulkCalls++
	if c.bulkErr != nil {
		return nil, c.bulkErr
	}
	return maps.Clone(c.states), nil
}

// lookupOrDefaultState records a get call and returns the configured error,
// cached state, or default-size state. The bool result reports whether a state
// was found; when false and err is nil, the caller resolves the machine type.
func (c *Client) lookupOrDefaultState(instance gce.GceRef) (ekvmtypes.ResizableVmState, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.getCalls++
	if err, ok := c.getErrs[instance]; ok && err != nil {
		return ekvmtypes.ResizableVmState{}, false, err
	}
	if c.getErr != nil {
		return ekvmtypes.ResizableVmState{}, false, c.getErr
	}
	if state, ok := c.states[instance]; ok {
		return state, true, nil
	}
	if c.hasDefaultSize {
		state := ekvmtypes.ResizableVmState{
			Size:   c.defaultSize,
			Status: ekvmtypes.ResizeStatusAtIntent,
		}
		c.states[instance] = state
		return state, true, nil
	}
	return ekvmtypes.ResizableVmState{}, false, nil
}
