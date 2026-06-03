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

package labels

import (
	apiv1 "k8s.io/api/core/v1"
	kubeletapis "k8s.io/kubelet/pkg/apis"
)

// UpdateDeprecatedLabels updates beta and deprecated labels from stable labels
// It is used as a workaround to revert changes in https://github.com/kubernetes/autoscaler/pull/5276
// Additional context: b/264021154
func UpdateDeprecatedLabels(labels map[string]string) {
	if v, ok := labels[apiv1.LabelArchStable]; ok {
		labels[kubeletapis.LabelArch] = v
	}
	if v, ok := labels[apiv1.LabelOSStable]; ok {
		labels[kubeletapis.LabelOS] = v
	}
}
