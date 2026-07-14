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

package podsharding

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	gketpu "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	klog "k8s.io/klog/v2"
)

type machineConfigProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// NewGkePodSharder creates instance of PodSharder for use in GKE environment
func NewGkePodSharder(provider machineConfigProvider, csnEnabled bool, matcher *gkelabels.Matcher) PodSharder {
	computeFunctions := []FeatureShardComputeFunction{
		{
			"tpu_type",
			func(p *v1.Pod, ngd *NodeGroupDescriptor) {
				computeTpuTypeShard(provider, p, ngd)
			},
		},
		{
			"gpu_type",
			func(p *v1.Pod, ngd *NodeGroupDescriptor) {
				computeGpuTypeShard(provider, p, ngd)
			},
		},
		{
			"workload",
			computeWorkloadSeparationShardWithAllowlistedLabels(matcher),
		},
	}

	computeFunctions = append(computeFunctions, FeatureShardComputeFunction{
		"ccc_name",
		computeCCCShard,
	})

	if csnEnabled {
		computeFunctions = append(computeFunctions, FeatureShardComputeFunction{
			"csn_buffer_id",
			computeCSNShard,
		})
	}

	// provisioningRequestShard overrides other shard fields, so it should be added as the last one.
	computeFunctions = append(computeFunctions, FeatureShardComputeFunction{
		"provisioning_request",
		provisioningRequestShard,
	})
	return NewCompositePodSharder(computeFunctions)
}

func computeGpuTypeShard(provider machineConfigProvider, pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	podRequests := podutils.PodRequests(pod)
	podGpuCount := podRequests[gpu.ResourceNvidiaGPU]

	if podGpuCount.Value() == 0 {
		return
	}

	nodeGroupDescriptor.RequiresGPU = true
	req := podrequirements.GetRequirements(pod)

	gpuType, found := req.LabelReq.GetSingleValue(gkelabels.GPULabel)
	if !found {
		return
	}

	nodeGroupDescriptor.SystemLabels[gkelabels.GPULabel] = gpuType

	gpuPartitionSize, _ := req.LabelReq.GetSingleValue(gkelabels.GPUPartitionSizeLabel)
	gpuMaxSharedClients, _ := req.LabelReq.GetSingleValue(gkelabels.GPUMaxSharedClientsLabel)
	if gpuMaxSharedClients == "" {
		// If max shared clients label is not set, but a valid sharing strategy is
		// provided, max shared clients will be set to a default value.
		sharingStrategy, found := req.LabelReq.GetSingleValue(gkelabels.GPUSharingStrategyLabel)
		if found {
			err := machinetypes.ValidateGpuSharingStrategy(sharingStrategy)
			if err != nil {
				klog.Errorf("received error when parsing '%v' gpu sharing strategy from labels: %v", sharingStrategy, err)
			} else {
				gpuMaxSharedClients = machinetypes.DefaultGPUMaxSharedClients
			}
		}
	}
	// We want to group all the pods requesting given type of gpu within single shard. We use maximum possible number
	// of gpus for given type for building expansion node info.
	maxGpuCountInt64, err := provider.MachineConfigProvider().GetMaxAllocatableGpuCount(gpuType, gpuPartitionSize, gpuMaxSharedClients)
	if err == nil {
		nodeGroupDescriptor.ExtraResources[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(int64(maxGpuCountInt64), resource.DecimalSI)
	} else {
		klog.Warningf("Got error while obtaining maxGpuCount for gpu type %v; %v", gpuType, err)
	}
}

func computeTpuTypeShard(provider machineConfigProvider, pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	podRequests := podutils.PodRequests(pod)
	podTpuCount := podRequests[gketpu.ResourceGoogleTPU]

	if podTpuCount.Value() == 0 {
		return
	}

	req := podrequirements.GetRequirements(pod)

	tpuType := gketpu.DefaultTPU
	if tpuTypeFromSelector, isSpecified := req.LabelReq.GetSingleValue(gkelabels.TPULabel); isSpecified {
		tpuType = tpuTypeFromSelector
	}
	nodeGroupDescriptor.SystemLabels[gkelabels.TPULabel] = tpuType

	if tpuTopologyFromSelector, isSpecified := req.LabelReq.GetSingleValue(gkelabels.TPUTopologyLabel); isSpecified {
		nodeGroupDescriptor.SystemLabels[gkelabels.TPUTopologyLabel] = tpuTopologyFromSelector
	}

	if acceleratorCountFromSelector, isSpecified := req.LabelReq.GetSingleValue(gkelabels.AcceleratorCountLabel); isSpecified {
		nodeGroupDescriptor.SystemLabels[gkelabels.AcceleratorCountLabel] = acceleratorCountFromSelector
	}

	// We want to group all the pods requesting given type of tpu within single shard.
	// We use maximum possible number of tpus for given type for building expansion node info.
	maxTpuCountInt64, err := provider.MachineConfigProvider().GetMaxTpuCount(tpuType)
	if err == nil {
		nodeGroupDescriptor.ExtraResources[gketpu.ResourceGoogleTPU] = *resource.NewQuantity(maxTpuCountInt64, resource.DecimalSI)
	} else {
		klog.Warningf("Got error while obtaining maxTpuCount for tpu type %v; %v", tpuType, err)
	}
}

func computeWorkloadSeparationShardWithAllowlistedLabels(matcher *gkelabels.Matcher) func(*v1.Pod, *NodeGroupDescriptor) {
	checker := podrequirements.NewWorkloadSeparationWorkloadChecker(matcher)
	return func(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
		req := podrequirements.GetRequirements(pod)
		taints, labels, err := req.WorkloadSeparationTaintsAndLabels(checker, podrequirements.AllowedNonWorkloadSeparationLabels(pod))
		if err != nil || len(labels) == 0 {
			return
		}

		for labelKey, labelValue := range labels {
			nodeGroupDescriptor.Labels[labelKey] = labelValue
		}
		for _, taint := range taints {
			foundExisting := false
			for _, existingTaint := range nodeGroupDescriptor.Taints {
				if existingTaint == taint {
					foundExisting = true
					break
				}
			}
			if !foundExisting {
				nodeGroupDescriptor.Taints = append(nodeGroupDescriptor.Taints, taint)
			}
		}
	}
}

func computeCCCShard(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	req := podrequirements.GetRequirements(pod)
	if name, found := req.LabelReq.GetSingleValue(gkelabels.ComputeClassLabel); found {
		nodeGroupDescriptor.SystemLabels[gkelabels.ComputeClassLabel] = name
	}
}

func computeCSNShard(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	id := csn.GetBufferIdFromPod(pod)
	nodeGroupDescriptor.CSNBufferID = id
}

func provisioningRequestShard(pod *v1.Pod, nodeGroupDescriptor *NodeGroupDescriptor) {
	provClass, found := pr_pods.ProvisioningClassName(pod)
	if !found {
		return
	}
	nodeGroupDescriptor.ProvisioningClassName = provClass
	provCapacitySearchStrategy, found := pr_pods.ProvisioningCapacitySearchStrategy(pod)
	if found {
		nodeGroupDescriptor.ProvisioningCapacitySearchStrategy = provCapacitySearchStrategy
	}

	// Remove other fields for OSS ProvReq.
	// Since OSS ProvReq injector process only one ProvReq during the loop, all pods from
	// OSS ProvReq will end up in one shard. These pods may then end up not being
	// schedulable together. However, splitting them into separate shards could lead
	// the orchestrator to assume that only one subset represents the whole ProvReq,
	// executing on those and marking the ProvReq done even if it is not possible.
	if provClass != queuedwrapper.QueuedProvisioningClassName {
		*nodeGroupDescriptor = NodeGroupDescriptor{ProvisioningClassName: provClass}
	}
}
