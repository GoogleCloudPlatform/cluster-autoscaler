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

package localssdsize

import (
	"sync"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/klog/v2"
)

// DynamicLocalSSDDiskSizeProvider is an implementation of `localssdsize.LocalSSDSizeProvider`
// which supports mapping machine types & families to arbitrary disk sizes.
// If an unsupported machine type or family is encountered, DynamicLocalSSDDiskSizeProvider falls back to `localssdsize.SimpleLocalSSDProvider` behaviour.
type DynamicLocalSSDDiskSizeProvider struct {
	lock              sync.RWMutex
	machineToDiskSize map[string]uint64

	simpleSSDProvider localssdsize.LocalSSDSizeProvider
}

// NewDynamicLocalSSDDiskSizeProvider creates & returns an instance of `DynamicLocalSSDDiskSizeProvider`
// `diskSizeMap` is a mapping from machine type/family to local ssd disk sizes in *GiB*
func NewDynamicLocalSSDDiskSizeProvider(diskSizeMap map[string]uint64) *DynamicLocalSSDDiskSizeProvider {
	return &DynamicLocalSSDDiskSizeProvider{
		machineToDiskSize: diskSizeMap,
		simpleSSDProvider: localssdsize.NewSimpleLocalSSDProvider(),
	}
}

// SSDSizeInGiB Returns disk size in GiB.
// If an exact machine type mapping is found it returns such mapping.
// Otherwise, it checks if a mapping for the machine family of input `machineType` and returns it if found.
// If none of the above is found, it falls back to call `localssdsize.SimpleLocalSSDProvider`.
func (dlsp *DynamicLocalSSDDiskSizeProvider) SSDSizeInGiB(machineType string) uint64 {
	dlsp.lock.RLock()
	defer dlsp.lock.RUnlock()

	if diskSize, found := dlsp.machineToDiskSize[machineType]; found {
		return diskSize
	}
	klog.V(5).Infof("Failed to find machine type '%s' in disk size provider, trying to search for a supported machine family", machineType)

	machineFamily, err := gce.GetMachineFamily(machineType)
	if err != nil {
		klog.Warningf("Failed to find machine family for machine type '%s': %v, defaulting to SimpleLocalSSDProvider", machineType, err)
		return dlsp.simpleSSDProvider.SSDSizeInGiB(machineType)
	}
	if diskSize, found := dlsp.machineToDiskSize[machineFamily]; found {
		return diskSize
	}

	klog.V(5).Infof("Failed to find machine family '%s' and machine type '%s', defaulting to SimpleLocalSSDProvider", machineFamily, machineType)
	return dlsp.simpleSSDProvider.SSDSizeInGiB(machineType)
}

func (dlsp *DynamicLocalSSDDiskSizeProvider) UpdateDiskSizes(diskSizeMap map[string]uint64) {
	dlsp.lock.Lock()
	defer dlsp.lock.Unlock()

	dlsp.machineToDiskSize = diskSizeMap
}
