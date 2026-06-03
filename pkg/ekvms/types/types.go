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

package types

import (
	v1 "k8s.io/api/core/v1"

	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

// ResizeStatus contains supported resize states.
// Source of truth: http://google3/cloud/cluster/manager/gce_compute_resources.proto;l=10003-10017;rcl=762458883
type ResizeStatus string

// ExtendedDurationLabelX is used to mark node as schedulable for EDP pods with different CPU requirements.
const ExtendedDurationLabelX = "X"

const (
	// Unspecified case shouldn't happen unless API introduced new state not being handled by CA code or .
	ResizeStatusUnspecified       ResizeStatus = "RESIZE_STATE_UNSPECIFIED"
	ResizeStatusAtIntent          ResizeStatus = "RESIZE_STATE_AT_INTENT"
	ResizeStatusInProgress        ResizeStatus = "RESIZE_STATE_IN_PROGRESS"
	ResizeStatusGuestAgentError   ResizeStatus = "RESIZE_STATE_GUEST_AGENT_ERROR"
	ResizeStatusGuestAgentTimeout ResizeStatus = "RESIZE_STATE_GUEST_AGENT_TIMEOUT"

	// This means that we just don't know the state of the VM internally in CA, and we need to call GCE to get the updated state.
	ResizeStatusUnknownCA ResizeStatus = "RESIZE_STATE_UNKNOWN_CA"
)

// ResizableVmState contains the state of resizable VMs (including size and status).
type ResizableVmState struct {
	Size   ekvmsize.VmSize
	Status ResizeStatus
}

// BPResizeTaint is a taint used to mark EK nodes resize.
var BPResizeTaint = &v1.Taint{
	Key:    "node.gke.io/balloon-pod-resize",
	Value:  "true",
	Effect: v1.TaintEffectNoSchedule,
}

type EkAutoprovisioningMode string

const (
	EkAutoprovisioningUnspecified                EkAutoprovisioningMode = "EK_AUTOPROVISIONING_UNSPECIFIED"
	EkAutoprovisioningDisabled                   EkAutoprovisioningMode = "EK_AUTOPROVISIONING_DISABLED"
	EkAutoprovisioningEnabledCoarseGrainedResize EkAutoprovisioningMode = "EK_AUTOPROVISIONING_ENABLED_COARSE_GRAINED_RESIZE"
	EkAutoprovisioningDisabledCgroupv1Detected   EkAutoprovisioningMode = "EK_AUTOPROVISIONING_DISABLED_CGROUPV1_DETECTED"
)

type E4aAutoprovisioningMode string

const (
	E4aAutoprovisioningUnspecified                E4aAutoprovisioningMode = "E4A_AUTOPROVISIONING_UNSPECIFIED"
	E4aAutoprovisioningDisabled                   E4aAutoprovisioningMode = "E4A_AUTOPROVISIONING_DISABLED"
	E4aAutoprovisioningEnabledNoResize            E4aAutoprovisioningMode = "E4A_AUTOPROVISIONING_ENABLED_NO_RESIZE"
	E4aAutoprovisioningEnabledCoarseGrainedResize E4aAutoprovisioningMode = "E4A_AUTOPROVISIONING_ENABLED_COARSE_GRAINED_RESIZE"
)
