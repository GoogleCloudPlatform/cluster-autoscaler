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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type multitenantScaleToZeroPodsFilterImp struct {
	podScaledToZero             map[types.UID]bool
	experimentsManager          experiments.Manager
	backupScaleToZeroPodsFilter *scaleToZeroPodsFilterImp
}

func (m *multitenantScaleToZeroPodsFilterImp) ObserveScaleToZero(pods []*apiv1.Pod, nodes []*framework.NodeInfo, ignoreNode IgnoreNodeFilter, scaledToZero bool) {
	if m.isMultitenantScaleToZeroExpDisabled() {
		m.backupScaleToZeroPodsFilter.ObserveScaleToZero(pods, nodes, ignoreNode, scaledToZero)
		return
	}
	for _, pod := range pods {
		m.podScaledToZero[pod.UID] = scaledToZero
	}
	for _, node := range nodes {
		if ignoreNode != nil && ignoreNode(node) {
			continue
		}
		for _, pod := range node.Pods() {
			m.podScaledToZero[pod.UID] = scaledToZero
		}
	}
}

func (m *multitenantScaleToZeroPodsFilterImp) IsPodScaledToZero(podUID types.UID) bool {
	if m.isMultitenantScaleToZeroExpDisabled() {
		return m.backupScaleToZeroPodsFilter.IsPodScaledToZero(podUID)
	}
	return m.podScaledToZero[podUID]
}

func (m *multitenantScaleToZeroPodsFilterImp) ForgetPod(podUID types.UID) {
	if m.isMultitenantScaleToZeroExpDisabled() {
		m.backupScaleToZeroPodsFilter.ForgetPod(podUID)
		return
	}
	delete(m.podScaledToZero, podUID)
}

func (m *multitenantScaleToZeroPodsFilterImp) isMultitenantScaleToZeroExpDisabled() bool {
	return !m.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.MultitenancyScaleToZeroProcessorFlag, false)
}

func newMultitenatScaleToZeroPodsFilter(experimentsManager experiments.Manager) *multitenantScaleToZeroPodsFilterImp {
	return &multitenantScaleToZeroPodsFilterImp{
		podScaledToZero:             map[types.UID]bool{},
		experimentsManager:          experimentsManager,
		backupScaleToZeroPodsFilter: newScaleToZeroPodsFilter(),
	}
}
