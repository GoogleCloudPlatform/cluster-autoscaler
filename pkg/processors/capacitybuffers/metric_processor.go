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

package capacitybuffers

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	capacitybufferpodlister "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

const (
	unknownProvisioningStrategy = "unknown"
)

// Metrics is an interface for reporting capacity buffer pod metrics.
type Metrics interface {
	UpdateCapacityBufferPods(counts map[metrics.CapacityBufferPodsKey]int)
	UpdateCapacityBuffersNumber(countsByType map[string]int)
}

// MetricProcessor is a processor that emits metrics for capacity buffer pods.
// TODO(b/494558643): Move it to OSS.
type MetricProcessor struct {
	client         *client.CapacityBufferClient
	bufferRegistry *fakepods.Registry
	m              Metrics
}

// NewMetricProcessor creates a new MetricProcessor.
func NewMetricProcessor(client *client.CapacityBufferClient, bufferRegistry *fakepods.Registry, m Metrics) *MetricProcessor {
	return &MetricProcessor{
		client:         client,
		bufferRegistry: bufferRegistry,
		m:              m,
	}
}

// ProcessMetrics emits metrics for both scheduled and unscheduled capacity buffer pods.
func (p *MetricProcessor) ProcessMetrics(ctx *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) error {
	if err := p.emitCapacityBuffersCount(); err != nil {
		klog.Errorf("Failed to emit capacity buffers count metrics: %v", err)
	}

	if err := p.emitCapacityBufferPods(ctx, unschedulablePods); err != nil {
		klog.Errorf("Failed to emit capacity buffer pods metrics: %v", err)
	}

	return nil
}

func (p *MetricProcessor) emitCapacityBuffersCount() error {
	buffers, err := p.client.ListCapacityBuffers("")
	if err != nil {
		return fmt.Errorf("failed to list capacity buffers: %v", err)
	}
	countsByType := map[string]int{}
	for _, buffer := range buffers {
		ps := unknownProvisioningStrategy
		if buffer.Status.ProvisioningStrategy != nil {
			ps = *buffer.Status.ProvisioningStrategy
		}
		countsByType[ps]++
	}

	p.m.UpdateCapacityBuffersNumber(countsByType)
	return nil
}

func (p *MetricProcessor) emitCapacityBufferPods(ctx *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) error {
	bufferPods, err := p.allScheduledPods(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all scheduled pods from cluster snapshot: %v", err)
	}

	for _, pod := range unschedulablePods {
		if !capacitybufferpodlister.IsFakeCapacityBuffersPod(pod) {
			continue
		}
		k := bufferKey(pod, nil, p.bufferRegistry)
		if k == nil {
			klog.Warningf("Failed to get buffer key for unschedulable capacity buffer pod %q", pod.Name)
			continue
		}
		bufferPods[*k]++
	}

	p.m.UpdateCapacityBufferPods(bufferPods)
	return nil
}

// allScheduledPods returns a map of capacity buffer pod counts grouped by their state and strategy.
func (p *MetricProcessor) allScheduledPods(ctx *context.AutoscalingContext) (map[metrics.CapacityBufferPodsKey]int, error) {
	nodeInfos, err := ctx.ClusterSnapshot.NodeInfos().List()
	if err != nil {
		return nil, fmt.Errorf("failed to get node infos: %v", err)
	}
	bufferPods := map[metrics.CapacityBufferPodsKey]int{}
	for _, nodeInfo := range nodeInfos {
		for _, podInfo := range nodeInfo.GetPods() {
			pod := podInfo.GetPod()
			if !capacitybufferpodlister.IsFakeCapacityBuffersPod(pod) {
				continue
			}
			k := bufferKey(pod, nodeInfo.Node(), p.bufferRegistry)
			if k == nil {
				klog.Warningf("Failed to get buffer key for scheduled capacity buffer pod %q in clustersnapshot", pod.Name)
				continue
			}
			bufferPods[*k]++
		}
	}
	return bufferPods, nil
}

// bufferKey generates a metrics key for a given capacity buffer pod.
func bufferKey(pod *apiv1.Pod, node *apiv1.Node, bufferRegistry *fakepods.Registry) *metrics.CapacityBufferPodsKey {
	buffer := bufferRegistry.GetCapacityBuffer(pod.UID)
	if buffer == nil {
		return nil
	}
	var state metrics.CapacityBufferPodState
	if node == nil {
		state = metrics.CapacityBufferPodStateNotReady
	} else if isUpcomingNode(node) {
		state = metrics.CapacityBufferPodStateProvisioning
	} else {
		state = metrics.CapacityBufferPodStateReady
	}

	ps := unknownProvisioningStrategy
	if buffer.Status.ProvisioningStrategy != nil {
		ps = *buffer.Status.ProvisioningStrategy
	}

	return &metrics.CapacityBufferPodsKey{
		ProvisioningStrategy: ps,
		State:                state,
	}
}

// isUpcomingNode returns true if the node is marked as upcoming (being provisioned).
func isUpcomingNode(node *apiv1.Node) bool {
	if node == nil {
		return false
	}
	_, isUpcoming := node.Annotations[annotations.NodeUpcomingAnnotation]
	return isUpcoming
}

// CleanUp cleans up the processor state.
func (p *MetricProcessor) CleanUp() {
}
