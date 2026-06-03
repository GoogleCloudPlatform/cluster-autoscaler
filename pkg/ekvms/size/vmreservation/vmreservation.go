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

package vmreservation

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

type VmReservation interface {
	CalculateKernelReserved(osInfo gce.MigOsInfo, physicalMemory int64) int64
	PredictKubeReservedCpuMillicores(physicalCpuMillicores int64, machineType string, maxPodsPerNode int64) int64
	PredictKubeReservedMemory(physicalMemory int64, gcfsEnabled bool) int64
}

type reservation struct {
	reserved *gke.GkeReserved
}

func New(reserved *gke.GkeReserved) VmReservation {
	return &reservation{
		reserved: reserved,
	}
}

func (r *reservation) CalculateKernelReserved(osInfo gce.MigOsInfo, physicalMemory int64) int64 {
	return r.reserved.CalculateKernelReserved(osInfo, physicalMemory)
}

func (r *reservation) PredictKubeReservedCpuMillicores(physicalCpuMillicores int64, machineType string, maxPodsPerNode int64) int64 {
	return gke.PredictKubeReservedCpuMillicores(physicalCpuMillicores, machineType, maxPodsPerNode)
}

func (r *reservation) PredictKubeReservedMemory(physicalMemory int64, gcfsEnabled bool) int64 {
	return gke.PredictKubeReservedMemory(physicalMemory, gcfsEnabled)
}
