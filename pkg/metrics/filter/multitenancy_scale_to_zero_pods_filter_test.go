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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestObserveScaleToZero(t *testing.T) {
	testCases := []struct {
		name               string
		pods               []*apiv1.Pod
		nodes              []*framework.NodeInfo
		experimentsManager experiments.Manager
		ignoreFn           IgnoreNodeFilter
		podsScaledToZero   map[string]bool
	}{
		{
			name: "experiment_enabled",
			pods: getPods("p1", "p2", "p3"),
			nodes: []*framework.NodeInfo{
				buildTestNodeInfo("n1", getPods("n1p1", "n1p2")),
				buildTestNodeInfo("n2", getPods("n2p1", "n2p2")),
			},
			experimentsManager: experiments.NewMockManager(experiments.MultitenancyScaleToZeroProcessorFlag),
			ignoreFn: func(node *framework.NodeInfo) bool {
				return node.Node().Name == "n2"
			},
			podsScaledToZero: map[string]bool{
				"p1":   true,
				"p2":   true,
				"p3":   true,
				"n1p1": true,
				"n1p2": true,
				"n2p1": false,
				"n2p2": false,
			},
		},
		{
			name:               "experiment_disabled",
			pods:               getPods("p1", "p2", "p3"),
			experimentsManager: experiments.NewMockManager(),
			ignoreFn: func(node *framework.NodeInfo) bool {
				return node.Node().Name == "n2"
			},
			podsScaledToZero: map[string]bool{
				"p1": true,
				"p2": true,
				"p3": true,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newMultitenatScaleToZeroPodsFilter(tc.experimentsManager)
			f.ObserveScaleToZero(tc.pods, tc.nodes, tc.ignoreFn, true)
			for pod, expected := range tc.podsScaledToZero {
				assert.Equal(t, expected, f.IsPodScaledToZero(types.UID(pod)))
			}
		})
	}
}

func buildTestNodeInfo(name string, pods []*apiv1.Pod) *framework.NodeInfo {
	n := BuildTestNode(name, 1000, 1000)
	nodeInfo := framework.NewTestNodeInfo(n, pods...)
	return nodeInfo
}
