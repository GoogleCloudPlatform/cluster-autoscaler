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

package machinetypes

const (
	// Node boot disk type
	// https://cloud.google.com/kubernetes-engine/docs/how-to/custom-boot-disks#specify
	// DiskTypeStandard is the type for standard persistent storage
	DiskTypeStandard = "pd-standard"
	// DiskTypeBalanced is the type for balanced persistent storage
	DiskTypeBalanced = "pd-balanced"
	// DiskTypeSSD is the type for persistent SSD storage
	DiskTypeSSD = "pd-ssd"
	// DiskTypePDExtreme is the type for extreme persistent storage
	DiskTypePDExtreme = "pd-extreme"
	// DiskTypeHyperdiskBalanced is the type for Hyperdisk balanced storage
	DiskTypeHyperdiskBalanced = "hyperdisk-balanced"
	// DiskTypeHyperdiskExtreme is the type for Hyperdisk extreme storage
	DiskTypeHyperdiskExtreme = "hyperdisk-extreme"
	// DiskTypeHyperdiskThroughput is the type for Hyperdisk throughput storage
	DiskTypeHyperdiskThroughput = "hyperdisk-throughput"
	// DiskTypeHyperdiskBalancedHighAvailability is the type for Hyperdisk balanced high availability storage
	DiskTypeHyperdiskBalancedHighAvailability = "hyperdisk-balanced-high-availability"
	// DiskTypeHyperdiskMl is the type for Hyperdisk ML storage
	DiskTypeHyperdiskMl = "hyperdisk-ml"

	// MinGceBootDiskSizeGb is the absolute minimum size for gcloud compute instance:
	// https://cloud.google.com/compute/docs/disks/#introduction
	// http://go/gke-scalable-clusters#slide=id.gcc56c68297_0_3014
	MinGceBootDiskSizeGb = 10

	// MinBootDiskSizeGBForNAP is the minimum boot disk size for node pools created by NAP.
	// TODO(b/324207517): Adjust based on upcoming MIG params (e.g. image)
	// until fixed we use the default value, which we know is sufficient
	MinBootDiskSizeGBForNAP = DefaultDiskSizeGBForStandard

	// MaxBootDiskSizeNonSharedCoreMachinesGb is the maximum boot disk size for most machine types. The only exceptions
	// are small, shared-core machines (e.g. e2-micro, e2-small, f1-micro, g1-small), for which the limit is much smaller.
	MaxBootDiskSizeNonSharedCoreMachinesGb = 64 * 1024 // 64 TiB

	// DefaultDiskSizeGBForStandard is the default size of the boot disk for node pools created by NAP in Standard clusters.
	DefaultDiskSizeGBForStandard = 100

	// DefaultDiskSizeGBForStandard is the default size of the boot disk for node pools created by NAP in Autopilot clusters.
	DefaultDiskSizeGBForAutopilot = 250

	// LargerDiskSizeGBForAutopilot is a higher-than-default size of the boot disk for node pools created by NAP in Autopilot clusters,
	// only applicable to certain Compute Classes.
	LargerDiskSizeGBForAutopilot = 400
)
