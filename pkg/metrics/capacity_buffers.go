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

package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
)

var (
	capacityBufferCpuRequestCoresMetric = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "capacity_buffer_cpu_request_cores",
			Help:      "Total number of CPU cores requested by the capacity buffer",
		},
		[]string{"entity_type", "entity_namespace", "entity_name", "entity_uid", "provisioning_strategy", "state"},
	)

	capacityBufferMemoryRequestBytesMetric = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "capacity_buffer_memory_request_bytes",
			Help:      "Total amount of runtime memory requested by the capacity buffer",
		},
		[]string{"entity_type", "entity_namespace", "entity_name", "entity_uid", "provisioning_strategy", "state"},
	)

	capacityBufferEphemeralStorageRequestBytesMetric = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "capacity_buffer_ephemeral_storage_request_bytes",
			Help:      "Total amount of ephemeral storage requested by the capacity buffer",
		},
		[]string{"entity_type", "entity_namespace", "entity_name", "entity_uid", "provisioning_strategy", "state"},
	)

	capacityBufferAcceleratorRequestDevicesMetric = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "capacity_buffer_accelerator_request_devices",
			Help:      "Number of accelerator devices requested by the capacity buffer",
		},
		[]string{"entity_type", "entity_namespace", "entity_name", "entity_uid", "provisioning_strategy", "state", "resource_name", "model"},
	)

	// capacityBuffersRegistry is a custom prometheus registry to export per-CapacityBuffer
	// metrics on a separate endpoint (/metrics/capacitybuffers) so that prom-to-sd can
	// process entity_type, entity_namespace, entity_name, and entity_uid labels for the
	// internal_gke_entity monitored resource.
	capacityBuffersRegistry = k8smetrics.NewKubeRegistry()

	// capacityBuffersMetrics is the single source of truth for all metrics registered on
	// capacityBuffersRegistry. Both init and ResetAllForTest (in metrics_test_accessor.go)
	// use this slice.
	capacityBuffersMetrics = []k8smetrics.Registerable{
		capacityBufferCpuRequestCoresMetric,
		capacityBufferMemoryRequestBytesMetric,
		capacityBufferEphemeralStorageRequestBytesMetric,
		capacityBufferAcceleratorRequestDevicesMetric,
	}
)

func init() {
	for _, m := range capacityBuffersMetrics {
		capacityBuffersRegistry.MustRegister(m)
	}
	// Lets prom-to-sd derive the start time of any cumulative metrics exposed here (b/566293374).
	if err := k8smetrics.RegisterProcessStartTime(capacityBuffersRegistry.Register); err != nil {
		klog.Errorf("Failed to register process_start_time_seconds in the capacity buffers metrics registry: %v", err)
	}
}

// CapacityBuffersMetricsRegistryHandler returns an HTTP handler for the capacity buffers metrics registry.
// When enabled is false, it returns a handler backed by an empty registry so /metrics/capacitybuffers
// still responds with 200 OK without exposing any metrics.
func CapacityBuffersMetricsRegistryHandler(enabled bool) http.Handler {
	if !enabled {
		emptyCapacityBuffersRegistry := k8smetrics.NewKubeRegistry()
		return promhttp.HandlerFor(emptyCapacityBuffersRegistry.Gatherer(), promhttp.HandlerOpts{})
	}
	return promhttp.HandlerFor(capacityBuffersRegistry.Gatherer(), promhttp.HandlerOpts{})
}

// CapacityBufferEntityType is the entity_type label value for CapacityBuffer k8s_entity metrics.
const CapacityBufferEntityType = "CapacityBuffer"

// capacityBufferResourceState is the state label value of the capacity buffer resource metrics.
// It mirrors CapacityBufferPodState, but uses different label values from the ones reported by capacity_buffer_pods.
type capacityBufferResourceState string

const (
	// capacityBufferResourceStateReady is the resource metrics equivalent of CapacityBufferPodStateReady.
	capacityBufferResourceStateReady capacityBufferResourceState = "ready"
	// capacityBufferResourceStateProvisioning is the resource metrics equivalent of CapacityBufferPodStateProvisioning.
	capacityBufferResourceStateProvisioning capacityBufferResourceState = "provisioning"
	// capacityBufferResourceStateUnready is the resource metrics equivalent of CapacityBufferPodStateNotReady.
	capacityBufferResourceStateUnready capacityBufferResourceState = "unready"
)

// toCapacityBufferResourceState converts a CapacityBufferPodState to the state label value reported by the capacity buffer resource metrics.
func toCapacityBufferResourceState(state CapacityBufferPodState) capacityBufferResourceState {
	switch state {
	case CapacityBufferPodStateReady:
		return capacityBufferResourceStateReady
	case CapacityBufferPodStateProvisioning:
		return capacityBufferResourceStateProvisioning
	case CapacityBufferPodStateNotReady:
		return capacityBufferResourceStateUnready
	default:
		klog.Warningf("Unknown capacity buffer pod state: %v", state)
		return capacityBufferResourceStateUnready
	}
}

// CapacityBufferResourceKey identifies a capacity buffer compute resource metric series by entity, provisioning strategy, and state.
type CapacityBufferResourceKey struct {
	EntityNamespace      string
	EntityName           string
	EntityUID            string
	ProvisioningStrategy string
	State                CapacityBufferPodState
}

// CapacityBufferResources holds the aggregated compute resource requests for capacity buffer pods.
type CapacityBufferResources struct {
	CpuCores              float64
	MemoryBytes           int64
	EphemeralStorageBytes int64
}

// CapacityBufferAcceleratorKey identifies a capacity buffer accelerator resource metric series.
type CapacityBufferAcceleratorKey struct {
	CapacityBufferResourceKey
	ResourceName string
	// Model is the accelerator model (e.g. nvidia-tesla-t4) of the node that the capacity buffer pods are placed on.
	// It's empty when the model isn't reported, e.g. for pods that aren't placed on any node.
	Model string
}

// UpdateCapacityBufferResources records the CPU, memory, ephemeral storage, and accelerator devices requested by capacity buffers.
func (*prometheusMetrics) UpdateCapacityBufferResources(compute map[CapacityBufferResourceKey]CapacityBufferResources, accelerators map[CapacityBufferAcceleratorKey]int64) {
	capacityBufferCpuRequestCoresMetric.Reset()
	capacityBufferMemoryRequestBytesMetric.Reset()
	capacityBufferEphemeralStorageRequestBytesMetric.Reset()
	capacityBufferAcceleratorRequestDevicesMetric.Reset()

	for key, res := range compute {
		state := string(toCapacityBufferResourceState(key.State))
		capacityBufferCpuRequestCoresMetric.WithLabelValues(CapacityBufferEntityType, key.EntityNamespace, key.EntityName, key.EntityUID, key.ProvisioningStrategy, state).Set(res.CpuCores)
		capacityBufferMemoryRequestBytesMetric.WithLabelValues(CapacityBufferEntityType, key.EntityNamespace, key.EntityName, key.EntityUID, key.ProvisioningStrategy, state).Set(float64(res.MemoryBytes))
		capacityBufferEphemeralStorageRequestBytesMetric.WithLabelValues(CapacityBufferEntityType, key.EntityNamespace, key.EntityName, key.EntityUID, key.ProvisioningStrategy, state).Set(float64(res.EphemeralStorageBytes))
	}

	for key, devices := range accelerators {
		state := string(toCapacityBufferResourceState(key.State))
		capacityBufferAcceleratorRequestDevicesMetric.WithLabelValues(CapacityBufferEntityType, key.EntityNamespace, key.EntityName, key.EntityUID, key.ProvisioningStrategy, state, key.ResourceName, key.Model).Set(float64(devices))
	}
}
