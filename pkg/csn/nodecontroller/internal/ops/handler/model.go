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

package handler

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
)

// K8sClient is responsible for performing actions on nodes.
type K8sClient interface {
	ApplyNodePatch(ctx context.Context, node *v1.Node, desiredState csn.NodeState) error
	IsSuspensionBlocked(ctx context.Context, nodeName string) (bool, error)
	ApplyNodeToBufferAssignmentPatch(ctx context.Context, node *v1.Node, buffer *v1beta1.CapacityBuffer) error
	ApplyAdditionalSoftTaintsPatch(ctx context.Context, node *v1.Node, taintCount int) error
}

// CloudProvider defines the cloud provider operations needed by the handlers.
type CloudProvider interface {
	ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error
	SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error
	InstanceByRef(ref gce.GceRef) *gce.GceInstance
}

// StateManager is an abstraction for the node state manager.
type StateManager interface {
	Get(nodeName string) (state.TrackedNode, bool)
	GetAssignedBuffers(nodeNames ...string) map[string]*v1beta1.CapacityBuffer
}

// Enqueue is an abstraction for adding operations.
type Enqueue func(o ops.Operation) error
