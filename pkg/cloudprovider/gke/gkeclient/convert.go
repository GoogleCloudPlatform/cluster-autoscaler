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

package gkeclient

import (
	gke_api_beta "google.golang.org/api/container/v1beta1"

	apiv1 "k8s.io/api/core/v1"
)

func v1beta1NodeTaints(taints []apiv1.Taint) []*gke_api_beta.NodeTaint {
	var nodeTaints []*gke_api_beta.NodeTaint
	for _, taint := range taints {
		effect, found := taintEffectsMap[taint.Effect]
		if !found {
			effect = "EFFECT_UNSPECIFIED"
		}
		taint := &gke_api_beta.NodeTaint{
			Effect: effect,
			Key:    taint.Key,
			Value:  taint.Value,
		}
		nodeTaints = append(nodeTaints, taint)
	}
	return nodeTaints
}
