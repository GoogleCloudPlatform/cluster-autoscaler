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

package systempods

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestSystemPodsClassifier(t *testing.T) {
	classifier := NewClassifier([]string{"kube-system"})
	tests := []struct {
		name string
		pod  *v1.Pod
		want bool
	}{
		{
			name: "system pod",
			pod:  test.BuildTestPod("p1", 200, 2, func(p *v1.Pod) { p.Namespace = "kube-system" }),
			want: true,
		},
		{
			name: "user pod",
			pod:  test.BuildTestPod("p2", 200, 2, func(p *v1.Pod) { p.Namespace = "default" }),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifier.IsSystemPod(tc.pod); got != tc.want {
				t.Errorf("IsSystemPod: want %v, got %v", tc.want, got)
			}
		})
	}
}
