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

package autoprovisioning

import (
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

const (
	// ProcessingStatusContextKey is the key under which the NAP manager communicates the status
	// of its node group processing.
	ProcessingStatusContextKey = "processing-status.autoprovisioning.gke-autoscaler"
)

// ProcessingResult represents the result of NAP NodeGroupListProcessor's Process().
type ProcessingResult int

const (
	// ProcessingOk - no errors during processing.
	ProcessingOk ProcessingResult = iota
	// NapDisabled - no node groups were added because NAP was disabled.
	NapDisabled
	// ResourceLimiterNotAvailable - no node groups were added because resource limiter couldn't be obtained.
	ResourceLimiterNotAvailable
	// MaxAutoprovisionedNodeGroupsLimitReached - no node groups were added because maximum count of autoprovisioned node groups has been reached.
	MaxAutoprovisionedNodeGroupsLimitReached
	// NoAutoprovisioningLocationsAvailable - no node groups were added because there weren't any NAP locations available.
	NoAutoprovisioningLocationsAvailable
	// DaemonSetsNotAvailable - no node groups were added because daemon set list couldn't be obtained.
	DaemonSetsNotAvailable
)

// NodeGroupDisregardedReason denotes why a node group was disregarded by NAP.
type NodeGroupDisregardedReason int

const (
	// NoReason - sanity check, this should never be set explicitly. If this is found in the wild, it means that it was
	// implicitly initialized and might indicate a bug.
	NoReason NodeGroupDisregardedReason = iota
	// UnableToBuildNodeGroup - the node group couldn't be built. This can mean an internal, unexpected error happened.
	// However, it's also expected in certain situations. E.g. when a pod requests a GPU and there are multiple autoprovisioning
	// locations - if a GPU is not available in one of them, all node groups in this location will fail to build.
	UnableToBuildNodeGroup
	// InResourceBasedBackoff - the node group is in resource-based backoff.
	InResourceBasedBackoff
	// InStandardBackoff - the node group is in standard backoff.
	InStandardBackoff
	// InternalError - an unexpected error happened while trying to create the node group.
	InternalError
)

// PodProcessingStatus contains information about processing a given pod by NAP's node group injection logic.
type PodProcessingStatus struct {
	// Picked denotes whether this pod was one of the pods picked to be worked on by NAP this loop.
	Picked bool
	// Err indicates if there's a problem specific to this pod that prevents NAP from injecting node groups for it.
	Err errors.AutoscalerError
}

// ProcessingStatus contains information about an invocation of the NAP NodeGroupListProcessor's Process().
type ProcessingStatus struct {
	// Result contains the result code of the processing.
	Result ProcessingResult
	// DisregardedNodeGroups contains information about why NAP didn't add node groups with particular variable parameters (e.g. zone, machine type, preemption).
	DisregardedNodeGroups map[NodeGroupOptions]NodeGroupDisregardedReason
	// PodStatuses contains per-pod processing statuses. The default value is valid for pods which don't have
	// an entry in the map (i.e. you don't have to check for presence, just assign while accessing this map).
	// Keyed by pod UID.
	PodStatuses map[types.UID]PodProcessingStatus
}

// SetResult sets the result of the processing.
func (s *ProcessingStatus) SetResult(result ProcessingResult) {
	s.Result = result
}

// AddDisregardedNodeGroup adds information about why NAP didn't add a node group with particular variable parameters.
func (s *ProcessingStatus) AddDisregardedNodeGroup(key NodeGroupOptions, reason NodeGroupDisregardedReason) {
	s.DisregardedNodeGroups[key] = reason
}

// MarkPodPicked marks the given pod as one of the pods picked to be worked on by NAP in this loop.
func (s *ProcessingStatus) MarkPodPicked(podUid types.UID) {
	podStatus := s.PodStatuses[podUid]
	podStatus.Picked = true
	s.PodStatuses[podUid] = podStatus
}

// SetPodError sets an error preventing NAP from injecting node groups for the given pod.
func (s *ProcessingStatus) SetPodError(podUid types.UID, err errors.AutoscalerError) {
	podStatus := s.PodStatuses[podUid]
	podStatus.Err = err
	s.PodStatuses[podUid] = podStatus
}

// NewProcessingStatus creates an empty instance of ProcessingStatus.
func NewProcessingStatus() *ProcessingStatus {
	return &ProcessingStatus{
		Result:                ProcessingOk,
		DisregardedNodeGroups: make(map[NodeGroupOptions]NodeGroupDisregardedReason),
		PodStatuses:           make(map[types.UID]PodProcessingStatus),
	}
}
