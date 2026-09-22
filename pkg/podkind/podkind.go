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

// Package podkind provides a single classifier telling apart real, user-created
// pods from the various synthetic pods that Cluster Autoscaler processors inject
// into the scale-up loop.
//
// Consumers that need to attribute a scale-up to its cause (e.g. ComputeClass
// status conditions, metrics) should depend on this package only, instead of
// reaching for the individual markers owned by defrag, min capacity, capacity
// buffers, lookahead buffer or ProvisioningRequests.
package podkind

import (
	apiv1 "k8s.io/api/core/v1"
	cr_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	npc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	provreq_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	capacitybuffer "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/fake"
)

// Kind describes why a pod is present in the scale-up loop.
type Kind string

const (
	// PodPending is a real pod waiting to be scheduled.
	PodPending Kind = "PodPending"
	// ProvisioningRequest is a pod injected on behalf of a ProvisioningRequest,
	// either by the GKE or by the OSS ProvisioningRequest processor.
	ProvisioningRequest Kind = "ProvisioningRequest"
	// CapacityRequest is a pod injected on behalf of a CapacityRequest.
	CapacityRequest Kind = "CapacityRequest"
	// ActiveMigration is a copy of a real, running pod that defrag wants to move
	// off a candidate node.
	ActiveMigration Kind = "ActiveMigration"
	// MinCapacity is a pod injected to keep a ComputeClass rule at its target
	// node count.
	MinCapacity Kind = "MinCapacity"
	// CapacityBuffer is a pod injected for a capacity buffer. Standby capacity
	// (CSN) pods are capacity buffer pods as well.
	CapacityBuffer Kind = "CapacityBuffer"
	// Internal is a virtual pod that only exists to size the cluster and has no
	// counterpart the user could act on (e.g. the EK VM lookahead buffer).
	// Scale-ups caused solely by such pods should not be surfaced to users.
	Internal Kind = "Internal"
	// UnrecognizedFake is a pod carrying the generic OSS "fake pod" marker with
	// no more specific marker on top of it.
	//
	// Today the only such injector is the OSS proactive scale-up pod injection
	// processor (sigs.k8s.io/cluster-autoscaler/pkg/processors/podinjection),
	// enabled via --proactive-scaleup-enabled. Its pods are copies of a sample
	// pod of a ReplicaSet/Job/StatefulSet that is missing replicas, so they do
	// represent real user demand and callers should treat them as such.
	//
	// The kind is kept separate from PodPending so that a new injector, which
	// may well be internal, can be spotted before it is attributed to the user.
	UnrecognizedFake Kind = "UnrecognizedFake"
)

// Of returns the Kind of the given pod. Markers are checked from the most
// specific to the most generic one, as synthetic pods often carry several of
// them (e.g. CSN pods are also capacity buffer pods, lookahead pods also carry
// the generic fake pod annotation).
func Of(pod *apiv1.Pod) Kind {
	if pod == nil {
		return UnrecognizedFake
	}
	switch {
	case lookaheadbuffer.IsLookaheadPod(pod):
		return Internal
	case isProvisioningRequestPod(pod):
		return ProvisioningRequest
	case cr_utils.IsCapacityRequestPod(pod):
		return CapacityRequest
	case defrag.IsActiveMigrationPod(pod):
		return ActiveMigration
	case npc_processors.IsMinCapacityFakePod(pod):
		return MinCapacity
	case capacitybuffer.IsFakeCapacityBuffersPod(pod):
		return CapacityBuffer
	case fake.IsFake(pod):
		return UnrecognizedFake
	default:
		return PodPending
	}
}

// IsSynthetic returns true if the pod was injected into the scale-up loop by a
// processor rather than created by a user or a controller.
func IsSynthetic(pod *apiv1.Pod) bool {
	return Of(pod) != PodPending
}

func isProvisioningRequestPod(pod *apiv1.Pod) bool {
	_, found := provreq_pods.ProvisioningRequestName(pod)
	return found
}
