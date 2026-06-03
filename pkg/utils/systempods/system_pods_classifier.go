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
	v1 "k8s.io/api/core/v1"
)

type Classifier interface {
	IsSystemPod(pod *v1.Pod) bool
}

type classifier map[string]bool

// NewClassifier returns a new SystemPodsClassifier.
func NewClassifier(namespaces []string) Classifier {
	nsMap := make(classifier, len(namespaces))
	for _, ns := range namespaces {
		nsMap[ns] = true
	}
	return nsMap
}

// Checks if pod is system pod, based on it's namespace.
func (c classifier) IsSystemPod(pod *v1.Pod) bool {
	return c[pod.Namespace]
}
