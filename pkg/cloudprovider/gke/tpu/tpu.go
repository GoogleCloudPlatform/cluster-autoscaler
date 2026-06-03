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

package tpu

import (
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
)

const (
	ResourceGoogleTPU = "google.com/tpu"
	DefaultTPU        = labels.TpuV4LiteDeviceValue

	WorkloadTypeBatch       = "BATCH"
	WorkloadTypeServing     = "SERVING"
	WorkloadTypeUnspecified = "UNSPECIFIED"
)

// NodeHasTpu returns true if a given node has TPU hardware.
// The result will be true if there is hardware capability. It doesn't matter
// if the drivers are installed and TPU is ready to use.
func NodeHasTpu(node *apiv1.Node) bool {
	_, hasTpuLabel := node.Labels[labels.TPULabel]
	tpuAllocatable, hasTpuAllocatable := node.Status.Allocatable[ResourceGoogleTPU]
	return hasTpuLabel || (hasTpuAllocatable && !tpuAllocatable.IsZero())
}

// GetNodeTpu returns the TPU information of a node if the node has TPU. Returns nil if node doesn't have a TPU.
func GetNodeTpu(node *apiv1.Node) *cloudprovider.GpuConfig {
	if NodeHasTpu(node) {
		return &cloudprovider.GpuConfig{Label: labels.TPULabel, Type: node.Labels[labels.TPULabel], ExtendedResourceName: ResourceGoogleTPU}
	}
	return nil
}

// HasTpuPodRequests returns true if a given pod has TPU request.
func HasTpuPodRequests(pod *apiv1.Pod) bool {
	podRequests := podutils.PodRequests(pod)
	_, tpuFound := podRequests[ResourceGoogleTPU]
	return tpuFound
}

// GetTpuMachineFamilyFromUrl extracts TPU machine family from the accelerator URL
func GetTpuMachineFamilyFromUrl(acceleratorUrl string) (string, bool) {
	// Expected form projects/{PROJECT}/zones/{ZONE}/acceleratorTypes/{MACHINE_FAMILY}
	parts := strings.Split(acceleratorUrl, "/")
	for i, part := range parts {
		if part == "acceleratorTypes" && len(parts) > i+1 {
			return parts[i+1], true
		}
	}

	return "", false
}
