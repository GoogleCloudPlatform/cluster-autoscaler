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

package filter

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

type IgnoreNodeFilter func(node *framework.NodeInfo) bool

type scaleToZeroPodsFilter interface {
	// IsPodScaledDown returns true if the pod was part of a scaled down to zero event
	IsPodScaledToZero(podUID types.UID) bool
	// ObserveScaleToZero observes a scale to zero event for the give pods and all the pods
	// on the given nodes
	ObserveScaleToZero(pods []*apiv1.Pod, nodes []*framework.NodeInfo, ignoreNode IgnoreNodeFilter, scaledToZero bool)
	// ForgetPod cleans up any references to the given pod
	ForgetPod(podUID types.UID)
}

type scaleToZeroPodsFilterImp struct {
	scaledToZero bool
}

func (s *scaleToZeroPodsFilterImp) ObserveScaleToZero(_ []*apiv1.Pod, _ []*framework.NodeInfo, _ IgnoreNodeFilter, scaledToZero bool) {
	s.scaledToZero = scaledToZero
}

func (s *scaleToZeroPodsFilterImp) IsPodScaledToZero(_ types.UID) bool {
	return s.scaledToZero
}

func (s *scaleToZeroPodsFilterImp) ForgetPod(_ types.UID) {}

func newScaleToZeroPodsFilter() *scaleToZeroPodsFilterImp {
	return &scaleToZeroPodsFilterImp{}
}
