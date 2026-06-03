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

package pod

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func WithNodeSelector(s map[string]string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.NodeSelector = s
	}
}

func WithCCC(cccName string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.NodeSelector = map[string]string{labels.ComputeClassLabel: cccName}
		p.Spec.Tolerations = []apiv1.Toleration{
			{
				Key:      labels.ComputeClassLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    cccName,
				Effect:   apiv1.TaintEffectNoSchedule,
			},
		}
	}
}
