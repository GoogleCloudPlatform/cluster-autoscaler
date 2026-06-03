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
	"cmp"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	capacitybufferpodlister "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/processors/podinjection"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	cr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/processors"
	npc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	npc_history "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
	cb_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/capacitybuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
	processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleup"
	pr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/processors"
	vis_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/processors"
	"k8s.io/klog/v2"
)

var ProcessorOrder, _ = buildProcessorOrder()

func buildProcessorOrder() (map[reflect.Type]int, error) {
	order := make(map[reflect.Type]int)
	entries := []struct {
		processor reflect.Type
		order     int
	}{
		{reflect.TypeOf(new(cb_metrics.FakePodStateObserver)).Elem(), 1},
		{reflect.TypeOf(new(capacitybufferpodlister.FakePodsScaleUpStatusProcessor)).Elem(), 2},
		{reflect.TypeOf(new(podinjection.FakePodsScaleUpStatusProcessor)).Elem(), 3},
		{reflect.TypeOf(new(status.EventingScaleUpStatusProcessor)).Elem(), 4},
		{reflect.TypeOf(new(processors.InternalEventingScaleUpStatusProcessor)).Elem(), 5},
		{reflect.TypeOf(new(cr_processors.CapacityRequestScaleUpProcessor)).Elem(), 6},
		{reflect.TypeOf(new(vis_processors.ScaleUpStatusVisibilityProcessor)).Elem(), 7},
		{reflect.TypeOf(new(pr_processors.ProvisioningRequestScaleUpStatusProcessor)).Elem(), 8},
		{reflect.TypeOf(new(npc_processors.CrdScaleUpStatusProcessor)).Elem(), 9},
		{reflect.TypeOf(new(npc_history.ScaleUpStatusHistoryProcessor)).Elem(), 10},
		{reflect.TypeOf(new(metrics_processors.ScaleUpStatusMetricsProcessor)).Elem(), 11},
		{reflect.TypeOf(new(annotator.PodAnnotator)).Elem(), 12},
		{reflect.TypeOf(new(podstate.PodStateObserver)).Elem(), 13},
	}
	for _, entry := range entries {
		if _, exists := order[entry.processor]; exists {
			return nil, fmt.Errorf("duplicate processor detected: %v", entry.processor)
		}
		order[entry.processor] = entry.order
	}
	return order, nil
}

// ScaleUpStatusChainProcessor chains the execution of a list of scale up status Processors.
type ScaleUpStatusChainProcessor struct {
	processors      []status.ScaleUpStatusProcessor
	sorted          bool
	processingMutex sync.Mutex
}

// NewScaleUpStatusChainProcessor returns ScaleUpStatusChainProcessor.
func NewScaleUpStatusChainProcessor() *ScaleUpStatusChainProcessor {
	return &ScaleUpStatusChainProcessor{
		processors: make([]status.ScaleUpStatusProcessor, 0),
		sorted:     true,
	}
}

// Process executes Process for all of the internal processors.
func (p *ScaleUpStatusChainProcessor) Process(context *context.AutoscalingContext, status *status.ScaleUpStatus) {
	// Scale-ups may happen in parallel. Example: scale-up during async node pool creation
	// Process one scale-up status at a time, because many processors keep some state that and that leads to race conditions.
	p.processingMutex.Lock()
	defer p.processingMutex.Unlock()
	if !p.sorted {
		p.sortProcessorsAsc()
	}
	for _, proc := range p.processors {
		proc.Process(context, status)
	}
}

// CleanUp executes CleanUp for all of the internal processors.
func (p *ScaleUpStatusChainProcessor) CleanUp() {
	for _, proc := range p.processors {
		proc.CleanUp()
	}
}

// AddProcessor adds a processor to the end of the processor chain.
func (p *ScaleUpStatusChainProcessor) AddProcessor(processor status.ScaleUpStatusProcessor) error {
	if _, err := p.orderOf(processor); err != nil {
		return fmt.Errorf("failed to add processor %T: %w", processor, err)
	}
	p.processors = append(p.processors, processor)
	p.sorted = false
	return nil
}

func (p *ScaleUpStatusChainProcessor) sortProcessorsAsc() {
	slices.SortStableFunc(p.processors, func(p1, p2 status.ScaleUpStatusProcessor) int {
		ord1, _ := p.orderOf(p1)
		ord2, _ := p.orderOf(p2)
		return cmp.Compare(ord1, ord2)
	})
	p.sorted = true

	processorTypes := make([]string, 0)
	for _, processor := range p.processors {
		processorTypes = append(processorTypes, fmt.Sprintf("%T", processor))
	}
	klog.V(1).Infof("ScaleUpStatusChainProcessor - Processors: %v", processorTypes)
}

func (p *ScaleUpStatusChainProcessor) orderOf(proc status.ScaleUpStatusProcessor) (int, error) {
	t := reflect.TypeOf(proc)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if order, exists := ProcessorOrder[t]; exists {
		return order, nil
	}
	return math.MaxInt, fmt.Errorf("order not defined for processor %T", proc)
}
