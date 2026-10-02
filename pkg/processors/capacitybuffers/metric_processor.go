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
	"context"
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/accelerators"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/client"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	capacitybufferpodlister "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	podutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
)

const (
	unknownProvisioningStrategy = "unknown"
)

// Metrics is an interface for reporting capacity buffer pod metrics.
type Metrics interface {
	UpdateCapacityBufferPods(counts map[metrics.CapacityBufferPodsKey]int)
	UpdateCapacityBuffersNumber(countsByType map[string]int)
	UpdateCapacityBufferResources(compute map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources, accelerators map[metrics.CapacityBufferAcceleratorKey]int64)
}

type resourceAccumulator struct {
	cpuMilli              int64
	memoryBytes           int64
	ephemeralStorageBytes int64
}

// MetricProcessor is a processor that emits metrics for capacity buffer pods.
// TODO(b/494558643): Move it to OSS.
type MetricProcessor struct {
	client             *client.CapacityBufferClient
	bufferRegistry     *fakepods.Registry
	m                  Metrics
	experimentsManager experiments.Manager
}

// NewMetricProcessor creates a new MetricProcessor.
func NewMetricProcessor(client *client.CapacityBufferClient, bufferRegistry *fakepods.Registry, m Metrics, experimentsManager experiments.Manager) *MetricProcessor {
	return &MetricProcessor{
		client:             client,
		bufferRegistry:     bufferRegistry,
		m:                  m,
		experimentsManager: experimentsManager,
	}
}

// ProcessMetrics emits metrics for both scheduled and unscheduled capacity buffer pods.
func (p *MetricProcessor) ProcessMetrics(ctx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) error {
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

func (p *MetricProcessor) emitCapacityBufferPods(ctx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) error {
	perBufferMetrics := p.experimentsManager != nil && p.experimentsManager.DirectLaunchBoolFlag(experiments.CapacityBuffersPerBufferMetrics)
	perAcceleratorModelMetrics := p.experimentsManager != nil && p.experimentsManager.DirectLaunchBoolFlag(experiments.CapacityBuffersPerAcceleratorModelMetrics)

	bufferPods, computeTotals, acceleratorTotals, err := p.allScheduledPods(ctx, perBufferMetrics, perAcceleratorModelMetrics)
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

		resKey := bufferResourceKey(pod, *k, p.bufferRegistry, perBufferMetrics)
		accumulatePodResources(pod, resKey, "", computeTotals, acceleratorTotals)
	}

	computeResources := make(map[metrics.CapacityBufferResourceKey]metrics.CapacityBufferResources, len(computeTotals))
	for k, acc := range computeTotals {
		computeResources[k] = metrics.CapacityBufferResources{
			CpuCores:              float64(acc.cpuMilli) / 1000.0,
			MemoryBytes:           acc.memoryBytes,
			EphemeralStorageBytes: acc.ephemeralStorageBytes,
		}
	}

	p.m.UpdateCapacityBufferPods(bufferPods)
	p.m.UpdateCapacityBufferResources(computeResources, acceleratorTotals)
	return nil
}

// allScheduledPods returns maps of capacity buffer pod counts and resource totals grouped by their state and strategy.
func (p *MetricProcessor) allScheduledPods(ctx *ca_context.AutoscalingContext, perBufferMetrics, perAcceleratorModelMetrics bool) (map[metrics.CapacityBufferPodsKey]int, map[metrics.CapacityBufferResourceKey]resourceAccumulator, map[metrics.CapacityBufferAcceleratorKey]int64, error) {
	nodeInfos, err := ctx.ClusterSnapshot.NodeInfos().List()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get node infos: %v", err)
	}
	bufferPods := map[metrics.CapacityBufferPodsKey]int{}
	computeTotals := map[metrics.CapacityBufferResourceKey]resourceAccumulator{}
	acceleratorTotals := map[metrics.CapacityBufferAcceleratorKey]int64{}
	for _, nodeInfo := range nodeInfos {
		var accelModel string
		if perAcceleratorModelMetrics {
			accelModel = nodeAcceleratorModel(ctx.CloudProvider, nodeInfo.Node())
		}
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

			resKey := bufferResourceKey(pod, *k, p.bufferRegistry, perBufferMetrics)
			accumulatePodResources(pod, resKey, accelModel, computeTotals, acceleratorTotals)
		}
	}
	return bufferPods, computeTotals, acceleratorTotals, nil
}

// accumulatePodResources adds the pod's resource requests to computeTotals and acceleratorTotals under the given key.
func accumulatePodResources(pod *apiv1.Pod, key metrics.CapacityBufferResourceKey, accelModel string, computeTotals map[metrics.CapacityBufferResourceKey]resourceAccumulator, acceleratorTotals map[metrics.CapacityBufferAcceleratorKey]int64) {
	requests := podutils.PodRequests(pod)
	acc := computeTotals[key]
	acc.cpuMilli += requests.Cpu().MilliValue()
	acc.memoryBytes += requests.Memory().Value()
	acc.ephemeralStorageBytes += requests.StorageEphemeral().Value()
	computeTotals[key] = acc

	for name, qty := range requests {
		if qty.Value() <= 0 || !accelerators.IsAccelerator(name) {
			continue
		}
		accKey := metrics.CapacityBufferAcceleratorKey{
			CapacityBufferResourceKey: key,
			ResourceName:              string(name),
			Model:                     accelModel,
		}
		acceleratorTotals[accKey] += qty.Value()
	}
}

// nodeAcceleratorModel returns the accelerator model (e.g. nvidia-tesla-t4) of the given node,
// or an empty string if the node is nil or doesn't have any accelerator.
func nodeAcceleratorModel(cloudProvider cloudprovider.CloudProvider, node *apiv1.Node) string {
	if cloudProvider == nil || node == nil {
		return ""
	}
	gpuConfig := cloudProvider.GetNodeGpuConfig(context.Background(), node)
	if gpuConfig == nil {
		return ""
	}
	return gpuConfig.Type
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

// bufferResourceKey generates a resource metrics key for a given capacity buffer pod.
func bufferResourceKey(pod *apiv1.Pod, podKey metrics.CapacityBufferPodsKey, bufferRegistry *fakepods.Registry, perBufferMetrics bool) metrics.CapacityBufferResourceKey {
	var entityNamespace, entityName, entityUID string
	if perBufferMetrics {
		if buffer := bufferRegistry.GetCapacityBuffer(pod.UID); buffer != nil {
			entityNamespace = buffer.Namespace
			entityName = buffer.Name
			entityUID = string(buffer.UID)
		}
	}
	return metrics.CapacityBufferResourceKey{
		EntityNamespace:      entityNamespace,
		EntityName:           entityName,
		EntityUID:            entityUID,
		ProvisioningStrategy: podKey.ProvisioningStrategy,
		State:                podKey.State,
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
