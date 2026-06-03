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

package lookaheadbuffer

import (
	"fmt"
	"regexp"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

func BuildTestLookaheadPod(workloadID string, milliCpu, memBytes int64, options ...func(*apiv1.Pod)) *v1.Pod {
	pod := GenerateLookaheadPods(1, *resource.NewMilliQuantity(milliCpu, resource.DecimalSI), *resource.NewQuantity(memBytes, resource.BinarySI), workloadID)[0]

	for _, o := range options {
		o(pod)
	}

	return pod
}

// WithName updates the pod name and UID. Beware that UID's should be unique.
func WithName(name string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		pod.Name = name
		pod.UID = types.UID(name)
	}
}

// WithPosition updates the positional suffix in pod's name and UID.
// This is useful if you want to simulate multiple injected lookahead pods belonging to the same workload ID.
func WithPosition(pos int) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		re := regexp.MustCompile(`-\d+$`)
		baseName := re.ReplaceAllString(pod.Name, "")
		newName := fmt.Sprintf("%s-%d", baseName, pos)
		pod.Name = newName
		pod.UID = types.UID(newName)
	}
}

func WithNode(nodeName string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		pod.Spec.NodeName = nodeName
	}
}
