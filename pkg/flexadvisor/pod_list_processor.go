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

package flexadvisor

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
)

// PodListProcessor gets flexibility scopes from unschedulable pods and register them in the Flex Advisor
type PodListProcessor struct {
	provider  instanceavailability.Provider
	cccLister lister.Lister
}

// NewPodListProcessor returns an instance of PodListProcessor for Flex Advisor
func NewPodListProcessor(provider instanceavailability.Provider, cccLister lister.Lister) *PodListProcessor {
	return &PodListProcessor{
		provider:  provider,
		cccLister: cccLister,
	}
}

// Process the list of unschedulable pods and register all the CCCs from unschedulable pods, in flex advisor
func (p *PodListProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	registeredKeys := make(map[string]bool)
	for _, pod := range unschedulablePods {
		key, found := p.flexibilityScopeKeyFromPod(pod)
		if !found || registeredKeys[key] {
			continue
		}
		p.provider.RegisterFlexibilityScope(key)
		registeredKeys[key] = true
	}
	return unschedulablePods, nil
}

// CleanUp is a no-op
func (p *PodListProcessor) CleanUp() {
}

func (p *PodListProcessor) flexibilityScopeKeyFromPod(pod *apiv1.Pod) (string, bool) {
	_, key, _ := p.cccLister.PodCrd(pod)
	if key == "" {
		return key, false
	}
	return key, true
}
