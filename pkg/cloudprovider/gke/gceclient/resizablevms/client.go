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

package resizablevms

import (
	"context"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
)

// Client is used for communicating with VM resizing endpoint
type Client interface {
	// ResizeVm resizes a given VM Instance to a given size.
	ResizeVm(ctx context.Context, instance gce.GceRef, desiredCpuMillicores, desiredMemoryKb int64) error
	// GetCurrentResizableVmState fetches the current state of given resizable VM (including status and size).
	// TODO(b/463924566): Remove provider from the parameters, it should be set in the struct
	// implementing this interface instead.
	GetCurrentResizableVmState(provider MaxResizableVmSizeProvider, instance gce.GceRef) (ekvmtypes.ResizableVmState, error)
	// BulkFetchCurrentResizableVmStates fetches current states of EK VMs (including status and size).
	BulkFetchCurrentResizableVmStates(provider MaxResizableVmSizeProvider, project, clusterName string) (map[gce.GceRef]ekvmtypes.ResizableVmState, error)
}

// MaxResizableVmSizeProvider provides information about maximum resizable VM size.
type MaxResizableVmSizeProvider interface {
	GetMaxResizableVmSizeByMachineType(string) (ekvmsize.VmSize, error)
	ResizableFamilyNames() []string
}

type noOpClient struct{}

func (g *noOpClient) ResizeVm(_ context.Context, _ gce.GceRef, _, _ int64) error {
	return nil
}

func (g *noOpClient) GetCurrentResizableVmState(_ MaxResizableVmSizeProvider, _ gce.GceRef) (ekvmtypes.ResizableVmState, error) {
	return ekvmtypes.ResizableVmState{}, nil
}

func (g *noOpClient) BulkFetchCurrentResizableVmStates(_ MaxResizableVmSizeProvider, _, _ string) (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	return nil, nil
}

func NewNoOpClient() *noOpClient {
	return &noOpClient{}
}
