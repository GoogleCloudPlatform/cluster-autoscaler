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

package autoprovisioning

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/klog/v2"
)

// gpuPodsRequirements returns nodeGroupRequirements for each GPU spec (<gpu type, gpu partition size>) requested by
// any pod in pods argument. There can be different counts of a particular GPU spec requested by different pods.
// Returned requirements will only contain minimum requests for each GPU spec (and only the pods with that minimum request).
// If a pod doesn't request a specific GPU, it's treated as if it requested the default GPU (if a limit for that GPU is
// specified), or as if it requested each GPU type that has limit defined (otherwise). In the latter case, one pod
// can end up in multiple requirements. If there are any pod-specific errors encountered, they're reported via ctx.status.
func (m *AutoprovisioningNodeGroupManager) gpuPodsRequirements(ctx *injectionContext, pods []*apiv1.Pod) []nodeGroupRequirements {
	// TODO(b/353677998): Refactor and remove this logic - we should implement GPU specific
	// nodeGroupRequirementsGenerator and implement binpacking limiter, which would stop
	// binpacking if there was some option found within smallest GPU counts.
	gpuMinCounts := make(map[machinetypes.GpuConfig]machinetypes.PhysicalGpuCount)
	requestingPods := make(map[machinetypes.GpuRequest][]*apiv1.Pod)

	// No need to consider non-GPU pods for GPU requirements.
	pods = m.filterOutNonGpuPods(pods)
	// Replace individual pods with equivalence groups for the purpose of calculating pod requirements.
	// Within calculating pod requirements, we call validateSchedulingPredicates function which performs
	// heavyweight validations that are a performance problem if there are too many pods.
	eqGroups := equivalence.BuildPodGroups(pods)
	klog.Infof("NAP: Considering %d equivalence groups for GPU requirements from %d pods.", len(eqGroups), len(pods))

	for _, eqg := range eqGroups {
		if len(eqg.Pods) == 0 {
			klog.Errorf("NAP: generated empty equivalence group, this should not happen: %+v", eqg)
			continue
		}
		podRequests := podutils.PodRequests(eqg.Pods[0])
		gpuCount := podRequests[gpu.ResourceNvidiaGPU]
		cpuCount := podRequests[apiv1.ResourceCPU]
		memCount := podRequests[apiv1.ResourceMemory]

		if gpuCount.Value() == 0 {
			continue
		}

		req := podrequirements.GetRequirements(eqg.Pods[0])
		gpuConfig := gpuConfigFromLabelRequirements(req)

		var gpuRequests []machinetypes.GpuRequest

		cc, ccName, ccErr := m.computeClassLister.PodReqCrd(req)
		if ccErr != nil {
			ccType, ccTypeErr := m.computeClassLister.PodReqCrdType(req)
			for _, pod := range eqg.Pods {
				ctx.status.SetPodError(pod.UID, NewComputeClassNotFoundError(ccName, ccType, ccTypeErr))
			}
			continue
		}
		if cc != nil {
			// For ComputeClass pods we want to derive GPU config from ComputeClass in ComputeClass generator,
			// so we insert empty GPU Request.
			gpuRequests = append(gpuRequests, machinetypes.GpuRequest{})
		}

		if cc == nil || cc.ScaleUpAnyway() {
			nonCCGPURequest, err := m.getNonCCGPURequestsForPod(ctx, eqg.Pods[0], gpuConfig, gpuCount, cpuCount, memCount)
			if err == nil {
				gpuRequests = append(gpuRequests, nonCCGPURequest...)
			} else {
				klog.Errorf("Failed to get gpu requests for equivalence group with pod %s: %v", eqg.Pods[0].UID, err)
				for _, pod := range eqg.Pods {
					if cc == nil {
						ctx.status.SetPodError(pod.UID, err)
					}
				}
			}
		}

		for _, request := range gpuRequests {
			requestingPods[request] = append(requestingPods[request], eqg.Pods...)
			if minCount, found := gpuMinCounts[request.Config]; !found || request.PhysicalGPUCount < minCount {
				gpuMinCounts[request.Config] = request.PhysicalGPUCount
			}
		}
	}

	var result []nodeGroupRequirements
	for spec, physicalMinCount := range gpuMinCounts {
		allocatableCount, err := m.cloudProvider.MachineConfigProvider().PhysicalToAllocatableWithGpuName(physicalMinCount, spec.GpuType, spec.PartitionSize, spec.MaxSharedClients)
		if err != nil {
			if spec.GpuType != "" {
				// should not happen
				klog.Errorf("NAP: Could not evaluate allocatable gpu units for %+v and gpu-count=%v, err:%v", spec, physicalMinCount, err)
			}
			allocatableCount = machinetypes.AllocatableGpuCount(physicalMinCount)
		}
		minRequest := machinetypes.GpuRequest{Config: spec, Count: allocatableCount, PhysicalGPUCount: physicalMinCount}
		minRequestPods, found := requestingPods[minRequest]
		if !found {
			klog.Errorf("NAP: Could not find pods for min gpu request: %v", minRequest)
			continue
		}
		// Ignore pod errors since they should've been reported during validation in the loop above.
		allRequirements, _ := m.extractRequirements(minRequestPods, minRequest, TpuRequest{})
		for _, requirements := range allRequirements {
			if requirements.hasPods() {
				result = append(result, requirements)
			}
		}
	}
	return result
}

func (m *AutoprovisioningNodeGroupManager) filterOutNonGpuPods(pods []*apiv1.Pod) []*apiv1.Pod {
	filtered := make([]*apiv1.Pod, 0, len(pods))
	for _, pod := range pods {
		if m.isGpuPod(pod) {
			filtered = append(filtered, pod)
		}
	}
	return filtered
}

func (m *AutoprovisioningNodeGroupManager) getNonCCGPURequestsForPod(ctx *injectionContext, pod *apiv1.Pod, gpuConfig machinetypes.GpuConfig, gpuCount, cpuCount, memCount resource.Quantity) ([]machinetypes.GpuRequest, errors.AutoscalerError) {
	consideredGpuTypes, err := m.getConsideredGPUTypes(ctx.resourceLimiter, gpuConfig.GpuType)
	if err != nil {
		return nil, err
	}
	return m.gpuRequestsForPod(ctx, pod, consideredGpuTypes, gpuConfig, gpuCount, cpuCount, memCount)
}

func (m *AutoprovisioningNodeGroupManager) gpuRequestsForPod(ctx *injectionContext, pod *apiv1.Pod, consideredGpuTypes []string, gpuConfig machinetypes.GpuConfig, gpuCount, cpuCount, memCount resource.Quantity) ([]machinetypes.GpuRequest, errors.AutoscalerError) {
	var gpuRequests []machinetypes.GpuRequest
	var validationErrs []errors.AutoscalerError
	for _, consideredGpuType := range consideredGpuTypes {
		gpuType, ok := m.cloudProvider.MachineConfigProvider().ToGpuType(consideredGpuType)
		if !ok {
			return nil, NewGpuTypeNotSupportedError(consideredGpuType)
		}
		normalisedGpuCount, err := gpuType.GetNormalizedGpuCount(gpuConfig.PartitionSize, gpuConfig.MaxSharedClients, machinetypes.AllocatableGpuCount(gpuCount.Value()), cpuCount.Value(), memCount.Value())
		if err != nil {
			validationErrs = append(validationErrs, NewGpuRequestInvalidError(err.Error()))
			continue
		}

		// Find the lowest valid number of GPUs that matches the pod requirements for GPUs, CPUs and passes the validation.
		// This fixes situations when pods that request high memory and GPUs did not schedule at all, because the matching was based only on the amount of GPUs and CPUs,
		// therefore for high memory requests the validation failed and higher numbers of GPUs weren't considered at all.
		availableGpuConfigs, partitionCount, maxSharedClientsInt, err := m.cloudProvider.MachineConfigProvider().GetGpuConfigsGreaterOrEqual(gpuConfig.PartitionSize, gpuConfig.MaxSharedClients, consideredGpuType, normalisedGpuCount)
		if err != nil {
			validationErrs = append(validationErrs, NewGpuRequestInvalidError(err.Error()))
			continue
		}

		for _, consideredGpuCount := range availableGpuConfigs {
			request := machinetypes.GpuRequest{
				Count:            consideredGpuCount,
				PhysicalGPUCount: machinetypes.PhysicalGpuCount(int64(consideredGpuCount) / partitionCount / maxSharedClientsInt),
				Config: machinetypes.GpuConfig{
					GpuType:          consideredGpuType,
					PartitionSize:    gpuConfig.PartitionSize,
					MaxSharedClients: gpuConfig.MaxSharedClients,
					SharingStrategy:  gpuConfig.SharingStrategy,
					DriverVersion:    gpuConfig.DriverVersion,
				},
			}
			// Verify if the pod can pass scheduler predicates on any of the node groups based on this request that NAP would inject.
			// If the pod is misconfigured (e.g. it has node affinity for a non-existent system label), and has no chance of passing
			// scheduler predicates anyway, we want to skip it so that NAP doesn't starve pods with higher GPU requests (b/239066590).
			if err := m.validateSchedulingPredicates(ctx, pod, request); err != nil {
				validationErrs = append(validationErrs, err)
			} else {
				// Found the lowest valid number of GPUs to request
				gpuRequests = append(gpuRequests, request)
				break
			}
		}
	}

	// All considered GPU types were skipped because of validation errors - the pod is probably misconfigured, so report it and move on
	// so that pods with higher GPU requests can be considered.
	if len(gpuRequests) == 0 && len(validationErrs) > 0 {
		// If there are multiple errors, just pick the first one. Multiple errors will only happen for pods which
		// don't request any specific GPU. If the errors are unrelated to the GPU type (e.g. the pod specifies an
		// unknown machine family), they should all be the same anyway. If the errors are related to the GPU type
		// (e.g. the pod specifies just the A2 family, and there is no limit defined for A100/A100-80gb - we'll consider
		// all GPUs with limits defined, and all will be incompatible with the A2 family), they're likely of
		// the same nature for all types, so reporting any of them should usually work. We could try to aggregate
		// the errors somehow, but it's not clear how and this is a rare edge case anyway.
		return nil, validationErrs[0]
	}
	return gpuRequests, nil
}

// validateSchedulingPredicates verifies if injected node groups based on this GPU request can accommodate this pod. It's done
// by simulating the node groups NAP would inject for the biggest machine type under the CPU limit for a given GPU count, and checking
// if scheduler predicates for the pod pass on any of them. In particular this method also validates that the pod produces
// valid node group requirements.
func (m *AutoprovisioningNodeGroupManager) validateSchedulingPredicates(ctx *injectionContext, pod *apiv1.Pod, gpuReq machinetypes.GpuRequest) errors.AutoscalerError {
	allRequirements, errs := m.extractRequirements([]*apiv1.Pod{pod}, gpuReq, TpuRequest{})
	if err := errs[pod.UID]; err != nil {
		return err
	}
	var predicateFailureReasons []string
	var otherErrors []error
	for _, ngRequirements := range allRequirements {
		maxMachine, err := m.maxCpuMachineForGpuRequest(gpuReq, ngRequirements.machineSpec)
		if err != nil {
			return errors.NewAutoscalerError(errors.InternalError, err.Error())
		}

		options := []NodeGroupOptions{}
		// Ignores apiv1.LabelInstanceType as it's deprecated
		instanceType, instanceTypeFound := pod.Spec.NodeSelector[apiv1.LabelInstanceTypeStable]
		// filter options which are not of maxMachine type
		// or have machine type matching reservation
		// or have machine type matching instance type node selector.
		for _, option := range m.generateNodeGroupOptions(ctx, ngRequirements) {
			isMaxMachineType := option.MachineType == maxMachine
			isExplicitMachineType := instanceTypeFound && option.MachineType == instanceType
			isReservationMachineType := ngRequirements.reservation.name != "" && option.MachineType == ngRequirements.reservation.machineType
			if isMaxMachineType || isReservationMachineType || isExplicitMachineType {
				options = append(options, option)
			}
		}

		for _, opts := range options {
			ngParams, err := m.getNodeGroupParameters(ngRequirements, opts)
			if err != nil {
				otherErrors = append(otherErrors, err)
				continue
			}
			nodeGroup, err := m.cloudProvider.NewNodeGroup(ngParams.machineType, ngParams.labels, ngParams.systemLabels, ngParams.taints, ngParams.extraResources)
			if err != nil {
				otherErrors = append(otherErrors, err)
				continue
			}
			nodeInfo, err := simulator.SanitizedTemplateNodeInfoFromNodeGroup(nodeGroup, ctx.daemonSets, ctx.taintConfig)
			if err != nil {
				otherErrors = append(otherErrors, err)
				continue
			}

			// Run scheduler predicates on that node info to see if this GPU request would help in this zone.
			passing, reasons, err := runPredicates(ctx.clusterSnapshot, nodeInfo, pod)
			if err != nil {
				otherErrors = append(otherErrors, err)
				continue
			}
			if passing {
				// There is at least 1 zone where the pod passes scheduler predicates - injecting this GPU request makes sense.
				return nil
			}

			// Injecting this GPU request won't help in this zone.
			predicateFailureReasons = append(predicateFailureReasons, reasons...)
		}
	}
	// Predicates didn't pass for any of the options NAP would consider - there's no point in injecting this GPU request.
	if len(predicateFailureReasons) > 0 {
		// At least 1 zone managed to actually run the predicates - report that.
		return NewGpuRequestFailingPredicatesError(predicateFailureReasons)
	}
	// All zones encountered errors before running the predicates - this can happen e.g. because of NewNodeGroup errors
	// if a GPU is not available in any of the zones.
	return errors.NewAutoscalerErrorf(errors.InternalError, "couldn't run scheduler predicates for any zone while validating GPU request %v, errors: %v", gpuReq, otherErrors)
}

// gpuConfigFromLabelRequirements collects the requirements for gpu based on requirements labels and returns gpu config
func gpuConfigFromLabelRequirements(req *podrequirements.Requirements) machinetypes.GpuConfig {
	gpuConfig := machinetypes.GpuConfig{
		GpuType:       machinetypes.AnyGPU,
		DriverVersion: "",
	}
	if gpuTypeFromSelector, isSpecified := req.LabelReq.GetSingleValue(labels.GPULabel); isSpecified {
		gpuConfig.GpuType = gpuTypeFromSelector
	}
	if gpuDriverVersionFromSelector, isSpecified := req.LabelReq.GetSingleValue(labels.GPUDriverVersionLabel); isSpecified {
		gpuConfig.DriverVersion = gpuDriverVersionFromSelector
	}
	gpuConfig.PartitionSize, _ = req.LabelReq.GetSingleValue(labels.GPUPartitionSizeLabel)
	gpuConfig.MaxSharedClients, _ = req.LabelReq.GetSingleValue(labels.GPUMaxSharedClientsLabel)
	gpuConfig.SharingStrategy, _ = req.LabelReq.GetSingleValue(labels.GPUSharingStrategyLabel)
	if gpuConfig.SharingStrategy != "" && gpuConfig.MaxSharedClients == "" {
		gpuConfig.MaxSharedClients = machinetypes.DefaultGPUMaxSharedClients
	}
	if gpuConfig.SharingStrategy == "" && gpuConfig.MaxSharedClients != "" {
		gpuConfig.SharingStrategy = machinetypes.DefaultGPUSharingStrategy
	}
	return gpuConfig
}

// runPredicates simulates running scheduler predicates for the given pod on the given nodeInfo. The nodeInfo isn't permanently injected
// into the snapshot - the provided snapshot should remain unchanged after this function runs. The first return value is whether the predicates
// pass, the second is a list of reasons for the predicates not passing.
func runPredicates(snapshot clustersnapshot.ClusterSnapshot, nodeInfo *framework.NodeInfo, pod *apiv1.Pod) (bool, []string, errors.AutoscalerError) {
	snapshot.Fork()
	defer snapshot.Revert()

	if err := snapshot.AddNodeInfo(nodeInfo); err != nil {
		return false, nil, errors.NewAutoscalerErrorf(errors.InternalError, "couldn't add node to cluster snapshot: %v", err)
	}

	if predicateErr := snapshot.CheckPredicates(pod, nodeInfo.Node().Name); predicateErr != nil {
		return false, predicateErr.Reasons(), nil
	}
	return true, nil, nil
}

func (m *AutoprovisioningNodeGroupManager) maxCpuMachineForGpuRequest(gpuReq machinetypes.GpuRequest, machineSpec machinetypes.MachineSpec) (string, errors.AutoscalerError) {
	largestUnderCpuLimit := gce.MachineType{CPU: -1, Memory: -1}
	for _, machineType := range machineSpec.AutoprovisionedMachineTypes() {
		if err := m.cloudProvider.MachineConfigProvider().ValidateGpuForMachineType(gpuReq.Config.GpuType, gpuReq.Config.PartitionSize, gpuReq.Config.MaxSharedClients, machineType.Name, gpuReq.Count, machineType.CPU, machineType.Memory); err != nil {
			continue
		}
		if machineType.CPU > largestUnderCpuLimit.CPU || (machineType.CPU == largestUnderCpuLimit.CPU && machineType.Memory >= largestUnderCpuLimit.Memory) {
			largestUnderCpuLimit = machineType.MachineType
		}
	}
	if largestUnderCpuLimit.CPU == -1 {
		return "", errors.NewAutoscalerErrorf(errors.InternalError, "no machine types under CPU limit in spec %v for GPU request %v", machineSpec, gpuReq)
	}
	return largestUnderCpuLimit.Name, nil
}

func (m *AutoprovisioningNodeGroupManager) getConsideredGPUTypes(limiter *cloudprovider.ResourceLimiter, requestedGPUType string) ([]string, errors.AutoscalerError) {
	if requestedGPUType == machinetypes.AnyGPU {
		if limiter.HasMaxLimitSet(machinetypes.DeprecatedDefaultGPU) {
			return nil, NewGpuRequestInvalidError("GPU type is not specified")
		}
		// If there is no limit set for the default GPU, consider all GPUs available for autoprovisioning, if there are any.
		availableGPUs := m.cloudProvider.MachineConfigProvider().GetAvailableGpuTypes(limiter)
		if len(availableGPUs) > 0 {
			return availableGPUs, nil
		}
		// There are no limits for supported GPUs set at all - NAP won't be able to help.
		return nil, NewGpuTypeNoLimitDefinedError("any GPU")
	}

	// A specific GPU type is requested.
	if err := m.validateRequestedGPUType(limiter, requestedGPUType); err != nil {
		return nil, err
	}
	return []string{requestedGPUType}, nil
}

func (m *AutoprovisioningNodeGroupManager) validateRequestedGPUType(limiter *cloudprovider.ResourceLimiter, requestedGPUType string) errors.AutoscalerError {
	if !m.cloudProvider.MachineConfigProvider().IsGpuNapSupported(requestedGPUType) {
		// The user requested a wrong GPU type - likely a typo, NAP won't be able to help.
		return NewGpuTypeNotSupportedError(requestedGPUType)
	}
	if !limiter.HasMaxLimitSet(requestedGPUType) {
		// The user requested a specific GPU which doesn't have a limit defined - NAP won't be able to help.
		return NewGpuTypeNoLimitDefinedError(requestedGPUType)
	}
	return nil
}
