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

package utils

import (
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestIsNodeInfoUpcoming(t *testing.T) {
	tests := []struct {
		name     string
		nodeInfo *framework.NodeInfo
		want     bool
	}{
		{
			name:     "no upcoming annotation",
			nodeInfo: nodeInfoWithAnnotations(map[string]string{}),
			want:     false,
		},
		{
			name:     "upcoming annotation present",
			nodeInfo: nodeInfoWithAnnotations(map[string]string{annotations.NodeUpcomingAnnotation: "true"}),
			want:     true,
		},
		{
			name:     "upcoming annotation value doesn't matter",
			nodeInfo: nodeInfoWithAnnotations(map[string]string{annotations.NodeUpcomingAnnotation: "false"}),
			want:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsNodeInfoUpcoming(tc.nodeInfo); got != tc.want {
				t.Errorf("IsNodeInfoUpcoming: want %v, got %v", tc.want, got)
			}
		})
	}
}

func nodeInfoWithAnnotations(annotations map[string]string) *framework.NodeInfo {
	node := test.BuildTestNode("node", 999, 1024)
	node.Annotations = annotations
	return framework.NewTestNodeInfo(node)
}
