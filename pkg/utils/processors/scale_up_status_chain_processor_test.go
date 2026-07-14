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

package processors

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	capacitybufferpodlister "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/processors/podinjection"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	cr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/processors"
	cb_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/capacitybuffer"
	processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleup"
	pr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/processors"
	vis_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/processors"
)

func TestBuildProcessorOrder(t *testing.T) {
	_, err := buildProcessorOrder()
	assert.NoError(t, err)
}

func TestProvisioningRequestScaleUpStatusProcessor(t *testing.T) {
	allowedPredecessors := sets.New[reflect.Type]()
	allowedPredecessors.Insert(reflect.TypeOf(new(cb_metrics.FakePodStateObserver)).Elem())
	allowedPredecessors.Insert(reflect.TypeOf(new(capacitybufferpodlister.FakePodsScaleUpStatusProcessor)).Elem())
	allowedPredecessors.Insert(reflect.TypeOf(new(podinjection.FakePodsScaleUpStatusProcessor)).Elem())
	allowedPredecessors.Insert(reflect.TypeOf(new(status.EventingScaleUpStatusProcessor)).Elem())
	allowedPredecessors.Insert(reflect.TypeOf(new(processors.InternalEventingScaleUpStatusProcessor)).Elem())
	allowedPredecessors.Insert(reflect.TypeOf(new(cr_processors.CapacityRequestScaleUpProcessor)).Elem())
	allowedPredecessors.Insert(reflect.TypeOf(new(vis_processors.ScaleUpStatusVisibilityProcessor)).Elem())
	allowedPredecessors.Insert(reflect.TypeOf(new(pr_processors.ProvisioningRequestScaleUpStatusProcessor)).Elem())

	chain := NewScaleUpStatusChainProcessor()
	prRank, _ := chain.orderOf(new(pr_processors.ProvisioningRequestScaleUpStatusProcessor))
	for processorType, rank := range ProcessorOrder {
		if rank <= prRank {
			assert.True(t, allowedPredecessors.Has(processorType), "Processor %v should not be allowed as a predecessor", processorType)
		}
	}
}

func TestSortProcessorsAsc(t *testing.T) {
	chain := NewScaleUpStatusChainProcessor()
	assert.NoError(t, chain.AddProcessor(new(processors.InternalEventingScaleUpStatusProcessor)))
	assert.NoError(t, chain.AddProcessor(new(vis_processors.ScaleUpStatusVisibilityProcessor)))
	assert.NoError(t, chain.AddProcessor(new(pr_processors.ProvisioningRequestScaleUpStatusProcessor)))
	assert.ErrorContains(t, chain.AddProcessor(&ptrProcessor{}), "order not defined for processor")
	assert.ErrorContains(t, chain.AddProcessor(valueProcessor{}), "order not defined for processor")
	assert.NotPanics(t, func() { chain.sortProcessorsAsc() })
	for i := 1; i < len(chain.processors); i++ {
		prev, _ := chain.orderOf(chain.processors[i-1])
		curr, _ := chain.orderOf(chain.processors[i])
		assert.True(t, prev <= curr, "Invalid order")
	}
	ptrOrder, _ := chain.orderOf(&ptrProcessor{})
	valOrder, _ := chain.orderOf(valueProcessor{})
	assert.Equal(t, math.MaxInt, ptrOrder)
	assert.Equal(t, math.MaxInt, valOrder)
}

func TestCapacityBufferFakePodsScaleUpStatusProcessorIsBeforePodInjection(t *testing.T) {
	chain := NewScaleUpStatusChainProcessor()
	assert.NoError(t, chain.AddProcessor(new(capacitybufferpodlister.FakePodsScaleUpStatusProcessor)))
	assert.NoError(t, chain.AddProcessor(new(podinjection.FakePodsScaleUpStatusProcessor)))

	capacityBufferOrder, _ := chain.orderOf(new(capacitybufferpodlister.FakePodsScaleUpStatusProcessor))
	podInjectionOrder, _ := chain.orderOf(new(podinjection.FakePodsScaleUpStatusProcessor))

	assert.True(t, capacityBufferOrder < podInjectionOrder)
}

func TestCapacityBufferFakePodStateObserverIsBeforeCapacityBufferFakePodsScaleUpStatusProcessor(t *testing.T) {
	chain := NewScaleUpStatusChainProcessor()
	assert.NoError(t, chain.AddProcessor(new(cb_metrics.FakePodStateObserver)))
	assert.NoError(t, chain.AddProcessor(new(capacitybufferpodlister.FakePodsScaleUpStatusProcessor)))

	capacityBufferOrder, _ := chain.orderOf(new(cb_metrics.FakePodStateObserver))
	podInjectionOrder, _ := chain.orderOf(new(capacitybufferpodlister.FakePodsScaleUpStatusProcessor))

	assert.True(t, capacityBufferOrder < podInjectionOrder)
}

type ptrProcessor struct{}

func (p *ptrProcessor) Process(context *context.AutoscalingContext, status *status.ScaleUpStatus) {}
func (p *ptrProcessor) CleanUp()                                                                  {}

type valueProcessor struct{}

func (p valueProcessor) Process(context *context.AutoscalingContext, status *status.ScaleUpStatus) {}
func (p valueProcessor) CleanUp()                                                                  {}
