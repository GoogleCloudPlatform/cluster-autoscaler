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

package csn

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// defaultMinUnsupportedMemoryGB is the smallest node memory size, in decimal GB, that GCE VM
// Suspend/Resume cannot handle. Overridable via ColdStandbyNodesMinUnsupportedMemoryGBFlag.
const defaultMinUnsupportedMemoryGB = 209

// MemoryLimit is the node memory size above which GCE VM Suspend/Resume, and therefore standby
// buffers, cannot operate. MakePodCSN encodes it as a node affinity on the standby buffer fake
// pod.
//
// The zero value is usable and means the default limit.
type MemoryLimit struct {
	// minUnsupportedGB is the smallest unsupported node memory size in decimal GB, or 0 for the
	// default. Nodes must have strictly less than this to host a standby buffer.
	minUnsupportedGB int64
}

// NewMemoryLimit returns the currently configured MemoryLimit. It falls back to the default when
// the experiment is unset or holds an invalid value.
func NewMemoryLimit(experimentsManager experiments.Manager) MemoryLimit {
	flagName := experiments.ColdStandbyNodesMinUnsupportedMemoryGBFlag
	value := experimentsManager.EvaluateIntFlagOrFailsafe(flagName, defaultMinUnsupportedMemoryGB)
	if value < 1 {
		klog.Warningf("CSN Memory Limit: ignoring invalid (< 1) %s value %d, using default %d", flagName, value, defaultMinUnsupportedMemoryGB)
		return MemoryLimit{}
	}
	return MemoryLimit{minUnsupportedGB: int64(value)}
}

// GB returns the smallest node memory size, in decimal GB, that cannot host a standby buffer.
func (l MemoryLimit) GB() int64 {
	if l.minUnsupportedGB < 1 {
		return defaultMinUnsupportedMemoryGB
	}
	return l.minUnsupportedGB
}
