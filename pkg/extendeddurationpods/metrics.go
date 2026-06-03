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

package extendeddurationpods

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/utilization"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/klog/v2"
)

// UtilizationBucket is a type description for utilization bucket labels.
type UtilizationBucket string

type NodeType string

const (
	// This indicates the different utilization buckets, for which we will find the count of nodes.
	// This is an exhaustive list only capturing the ranges of utilization that we find actionable.
	// The possible label values with the range they capture:
	// “0.0-0.4” - [0,  0.4)
	// “0.4-0.45” - [0.4, 0.45)
	// “0.45-0.5” - [0.45, 0.5)
	// “0.5-0.55” - [0.5, 0.55)
	// “0.55-0.6” - [0.55, 0.6)
	// “0.6-0.65” - [0.6, 0.65)
	// “0.65-0.7” - [0.65, 0.7)
	// “0.70-1.0” - [0.7,  1.0]
	// "unsupported" - [<0] || [>1]

	// Please not changes in these bucket may have an impact alerts and dashboards
	// dependent on these buckets and should be modified to match the new structs.
	// Hence, it is recommended to avoid changing buckets (unless critical)

	utilization0To40       UtilizationBucket = "0-0.40"
	utilization40To45      UtilizationBucket = "0.40-0.45"
	utilization45To50      UtilizationBucket = "0.45-0.50"
	utilization50To55      UtilizationBucket = "0.50-0.55"
	utilization55To60      UtilizationBucket = "0.55-0.60"
	utilization60To65      UtilizationBucket = "0.60-0.65"
	utilization65To70      UtilizationBucket = "0.65-0.70"
	utilization70To100     UtilizationBucket = "0.70-1.0"
	utilizationUnsupported UtilizationBucket = "unsupported"

	// Node Type labels
	edp          NodeType = "edp"
	edpPacked    NodeType = "edp_packed"
	edpGpu       NodeType = "edp_gpu"
	edpGpuPacked NodeType = "edp_gpu_packed"
)

// Metrics is the struct used to process utilization metrics for edp nodes.
type Metrics struct {
}

// NewEdpMetrics instantiate a new Metrics object
func NewEdpMetrics() *Metrics {
	return &Metrics{}
}

// metricTuple is an internal object over which aggregation of metrics is done.
type metricTuple struct {
	nodeType     NodeType
	resourceName v1.ResourceName
	machineType  string
	utilBucket   UtilizationBucket
}

// Process is the processing func for calculating and updating edp node metrics.
func (m *Metrics) Process(ctx *context.AutoscalingContext, _ *clusterstate.ClusterStateRegistry, _ time.Time) error {
	metrics.Metrics.ResetNodeUtilization()
	allNodes, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return err
	}

	metricsCounts := map[metricTuple]int{}
	for _, node := range allNodes {
		if node.Node() == nil {
			continue
		}
		for _, t := range getContributingTuples(ctx, node) {
			if t.utilBucket == utilizationUnsupported {
				klog.Errorf("Unsupported utilization found for node: %s, %+v", node.Node().Name, t)
			}
			if _, f := metricsCounts[t]; !f {
				metricsCounts[t] = 0
			}
			metricsCounts[t]++
		}
	}

	for tuple, value := range metricsCounts {
		metrics.Metrics.UpdateNodeUtilization(string(tuple.nodeType), tuple.machineType, tuple.resourceName.String(), string(tuple.utilBucket), value)
	}
	return nil
}

// CleanUp is no-op.
func (m *Metrics) CleanUp() {
}

// getContributingTuples adds the node utilization contributors based on the utilization calculator used for scale-down for edp nodes.
func getContributingTuples(ctx *context.AutoscalingContext, nodeInfo *framework.NodeInfo) []metricTuple {
	var tuples []metricTuple
	if utils.IsNodeInfoUpcoming(nodeInfo) {
		return tuples
	}
	if taints.HasToBeDeletedTaint(nodeInfo.Node()) {
		return tuples
	}
	packed := false
	if edpLabel, f := nodeInfo.Node().Labels[labels.ExtendedDurationPodsLabel]; !f {
		return tuples
	} else if edpLabel == labels.ExtendedDurationPackedPodsValue {
		packed = true
	}
	machineType := ""
	if v, f := nodeInfo.Node().Labels[v1.LabelInstanceTypeStable]; f {
		machineType = v
	}
	nodeType := edp
	if packed {
		nodeType = edpPacked
	}
	gpuConfig := ctx.CloudProvider.GetNodeGpuConfig(nodeInfo.Node())
	if gpuConfig != nil {
		nodeType = edpGpu
		if packed {
			nodeType = edpGpuPacked
		}

		gpuUtilization, err := utilization.CalculateUtilizationOfResource(nodeInfo, gpuConfig.ExtendedResourceName, false, false, time.Now())
		if err == nil {
			utLabel := getUtilizationBucket(gpuUtilization)
			tuples = append(tuples, metricTuple{
				nodeType:     nodeType,
				resourceName: gpuConfig.ExtendedResourceName,
				machineType:  machineType,
				utilBucket:   utLabel,
			})
		}
	}

	cpuUtilization, err := utilization.CalculateUtilizationOfResource(nodeInfo, v1.ResourceCPU, false, false, time.Now())
	if err == nil {
		utLabel := getUtilizationBucket(cpuUtilization)
		tuples = append(tuples, metricTuple{
			nodeType:     nodeType,
			resourceName: v1.ResourceCPU,
			machineType:  machineType,
			utilBucket:   utLabel,
		})
	}

	memoryUtilization, err := utilization.CalculateUtilizationOfResource(nodeInfo, v1.ResourceMemory, false, false, time.Now())
	if err == nil {
		utLabel := getUtilizationBucket(memoryUtilization)
		tuples = append(tuples, metricTuple{
			nodeType:     nodeType,
			resourceName: v1.ResourceMemory,
			machineType:  machineType,
			utilBucket:   utLabel,
		})
	}

	return tuples
}

// getUtilizationBucket gets the utilization label for a given utilization amount.
func getUtilizationBucket(util float64) UtilizationBucket {
	if util < 0 {
		klog.Warningf("unsupported utilization found: %f", util)
		return utilizationUnsupported
	}
	if util < 0.4 {
		return utilization0To40
	}
	if util < 0.45 {
		return utilization40To45
	}
	if util < 0.50 {
		return utilization45To50
	}
	if util < 0.55 {
		return utilization50To55
	}
	if util < 0.60 {
		return utilization55To60
	}
	if util < 0.65 {
		return utilization60To65
	}
	if util < 0.70 {
		return utilization65To70
	}
	if util <= 1 {
		return utilization70To100
	}
	klog.Warningf("unsupported utilization found: %f", util)
	return utilizationUnsupported
}
