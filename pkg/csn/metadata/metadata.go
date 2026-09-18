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

// Package metadata holds the label and taint keys that identify cold standby nodes
// (go/csn-in-ca) and tie them to the capacity buffer they serve.
//
// It exists as a leaf so that packages describing GKE node metadata, notably
// pkg/cloudprovider/gke/labels, can name these keys without depending on pkg/csn and the
// behaviour it carries. Keep this package free of imports.
package metadata

const (
	// SoftWorkloadSeparationKey labels a node as belonging to a capacity buffer. It is also
	// used as the key of a PreferNoSchedule taint, to keep regular workloads away from standby
	// capacity without forbidding them from using it.
	SoftWorkloadSeparationKey string = "buffer.gke.io/standby-capacity-node"
	// SoftWorkloadSeparationValue is the only value SoftWorkloadSeparationKey ever takes.
	SoftWorkloadSeparationValue string = "true"

	// SuspendedTaintKey is the key of the NoSchedule taint marking a node whose VM is
	// suspended, and which therefore cannot run anything yet.
	SuspendedTaintKey string = "buffer.gke.io/standby-capacity-node-suspended"
	// SuspendedTaintValue is the only value SuspendedTaintKey ever takes.
	SuspendedTaintValue string = "true"

	// BufferAssignmentKey labels a node with the identifier of the capacity buffer it is
	// reserved for.
	BufferAssignmentKey = "buffer.gke.io/standby-capacity-node-buffer"
)
