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

package operationtracker

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/klog/v2"
)

type vmStateCache struct {
	provider cloudProvider

	mux      sync.Mutex
	vmStates map[gce.GceRef]ekvmtypes.ResizableVmState
}

func newVmStateCache(provider cloudProvider) *vmStateCache {
	return &vmStateCache{
		provider: provider,
		vmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{},
	}
}

// refresh fetches current states and sizes of resizable VMs from GCE and caches them.
func (c *vmStateCache) refresh() {
	klog.Info("Starting to update cached current resizable VM states and sizes.")
	vmStates, err := c.provider.BulkFetchCurrentResizableVmStates()
	if err != nil {
		klog.Errorf("Bulk fetching resizable VM states failed: %v", err)
		return
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	c.vmStates = vmStates
}

// getState retrieves the size of the underlying resizable VM from cache.
func (c *vmStateCache) getState(node *v1.Node) (ekvmtypes.ResizableVmState, error) {
	gceRef, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		return ekvmtypes.ResizableVmState{}, err
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	vmState, ok := c.vmStates[gceRef]
	if !ok {
		return ekvmtypes.ResizableVmState{}, fmt.Errorf("state and size for resizable VM is not cached for node %q", node.Name)
	}
	return vmState, nil
}

// getStateOrRefresh retrieves the VM state and size of the underlying resizable VM from cache or fetches it via provider.
func (c *vmStateCache) getStateOrRefresh(node *v1.Node) (ekvmtypes.ResizableVmState, error) {
	vmState, err := c.getState(node)
	if err == nil {
		return vmState, nil
	}
	vmState, err = c.provider.GetCurrentResizableVmState(node)
	if err != nil {
		return ekvmtypes.ResizableVmState{}, err
	}
	err = c.updateState(node, vmState)
	if err != nil {
		return ekvmtypes.ResizableVmState{}, err
	}
	return vmState, nil
}

// updateState updates the cached state of a resizable VM.
func (c *vmStateCache) updateState(node *v1.Node, vmState ekvmtypes.ResizableVmState) error {
	gceRef, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		return fmt.Errorf("updating resizable vm state cache failed for node %q: %w", node.Name, err)
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	c.vmStates[gceRef] = vmState
	return nil
}

func (c *vmStateCache) invalidate(node *v1.Node) error {
	gceRef, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		return err
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	delete(c.vmStates, gceRef)
	return nil
}
