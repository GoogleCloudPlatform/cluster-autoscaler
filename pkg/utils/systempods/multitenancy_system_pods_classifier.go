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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
)

type multitenantClassifier map[string]bool

// NewMultitenantClassifier returns a new SystemPodsClassifier for multitenant clusters.
func NewMultitenantClassifier(namespaces []string) Classifier {
	nsMap := make(multitenantClassifier, len(namespaces))
	for _, ns := range namespaces {
		nsMap[ns] = true
	}
	return nsMap
}

// Checks if pod is system pod, based on it's namespace.
func (c multitenantClassifier) IsSystemPod(pod *v1.Pod) bool {
	// We still have some supervisor pods running in the kube-system namespace.
	return c[pod.Namespace] || multitenancy.IsTenantSystemNamespace(pod.Namespace)
}
