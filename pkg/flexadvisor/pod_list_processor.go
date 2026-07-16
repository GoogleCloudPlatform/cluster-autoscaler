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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

type podListProcessorMetrics interface {
	UpdateFlexAdvisorRejectedScopes(rejected int)
}

// PodListProcessor gets flexibility scopes from unschedulable pods and register them in the Flex Advisor
type PodListProcessor struct {
	provider           instanceavailability.Provider
	cccLister          lister.Lister
	experimentsManager experiments.Manager
	metrics            podListProcessorMetrics
}

type podListProcessorOption func(*PodListProcessor)

func withPodListProcessorMetrics(m podListProcessorMetrics) podListProcessorOption {
	return func(p *PodListProcessor) {
		p.metrics = m
	}
}

// NewPodListProcessor returns an instance of PodListProcessor for Flex Advisor
func NewPodListProcessor(provider instanceavailability.Provider, cccLister lister.Lister, experimentsManager experiments.Manager, opts ...podListProcessorOption) *PodListProcessor {
	p := &PodListProcessor{
		provider:           provider,
		cccLister:          cccLister,
		experimentsManager: experimentsManager,
		metrics:            metrics.Metrics,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Process the list of unschedulable pods and register all the CCCs from unschedulable pods, in flex advisor
func (p *PodListProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	if !IsFlexAdvisorProcessingEnabled(p.experimentsManager) {
		return unschedulablePods, nil
	}
	registeredKeys := make(map[string]bool)
	failures := 0
	for _, pod := range unschedulablePods {
		key, found := p.flexibilityScopeKeyFromPod(pod)
		if !found || registeredKeys[key] {
			continue
		}
		err := p.provider.RegisterFlexibilityScope(key)
		if err != nil {
			failures++
		}
		registeredKeys[key] = true
	}
	p.metrics.UpdateFlexAdvisorRejectedScopes(failures)
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
