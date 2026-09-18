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
	"maps"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	podutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

// defaultMinUnsupportedMemoryGB is the smallest node memory size, in decimal GB, that GCE VM
// Suspend/Resume cannot handle. Overridable via ColdStandbyNodesMinUnsupportedMemoryGBFlag.
const defaultMinUnsupportedMemoryGB = 209

// MemoryLimit is the node memory size above which GCE VM Suspend/Resume, and therefore standby
// buffers, cannot operate. MakePodCSN encodes it as a node affinity on the standby buffer fake
// pod, and the methods here answer questions about the consequences of that affinity.
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

// ExceededByPodRequest reports whether pod requests more memory than any node small enough to be
// suspended could ever offer, which makes it unschedulable whatever node shapes are available.
//
// The bound is exact: a node satisfies the affinity injected by MakePodCSN only if its capacity is
// below the limit, and a pod's request cannot exceed allocatable, which is below capacity.
func (l MemoryLimit) ExceededByPodRequest(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	requests := podutils.PodRequests(pod)
	return requests.Memory().Value() >= l.GB()*units.GB
}

// BlocksPodOnAnyNode reports whether the memory limit is what keeps pod off at least one of nodes,
// that is whether pod would have been schedulable somewhere if standby buffers supported nodes of
// that size. Callers use it to attribute a scheduling failure to the limit rather than to the
// user's own constraints.
//
// Nodes are passed together rather than one at a time so that the pod's scheduling requirements
// are compiled once for the whole set.
func (l MemoryLimit) BlocksPodOnAnyNode(pod *apiv1.Pod, nodes ...*apiv1.Node) bool {
	if pod == nil || len(nodes) == 0 {
		return false
	}
	required := nodeaffinity.GetRequiredNodeAffinity(pod)
	for _, node := range nodes {
		if node == nil || !l.exceededByNode(node) {
			continue
		}
		if matched, err := required.Match(node); err != nil || matched {
			continue
		}
		// Dropping the label satisfies the DoesNotExist alternative of the injected terms, which
		// neutralizes the memory limit while leaving every other requirement in place. What
		// remains is precisely the question "would this node do if not for the limit?".
		matchedWithoutLimit, err := required.Match(nodeWithoutMemoryScalingLevel(node))
		if err == nil && matchedWithoutLimit {
			return true
		}
	}
	return false
}

// exceededByNode reports whether node is too large to be suspended, i.e. whether the node selector
// terms injected by MakePodCSN reject it. A node without the label is assumed to be supported,
// matching the DoesNotExist term.
func (l MemoryLimit) exceededByNode(node *apiv1.Node) bool {
	value, found := node.Labels[labels.MemoryScalingLevelLabel]
	if !found {
		return false
	}
	memoryGB, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		// The scheduler cannot compare a non-numeric value either, so it is not the limit that
		// rejects this node.
		return false
	}
	return memoryGB >= l.GB()
}

// nodeWithoutMemoryScalingLevel returns a copy of node with the memory scaling level label
// removed. Only the labels are deep-copied; nothing else is mutated.
func nodeWithoutMemoryScalingLevel(node *apiv1.Node) *apiv1.Node {
	stripped := *node
	stripped.Labels = maps.Clone(node.Labels)
	delete(stripped.Labels, labels.MemoryScalingLevelLabel)
	return &stripped
}
