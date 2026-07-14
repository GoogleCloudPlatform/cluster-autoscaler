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
	"fmt"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

type TpuRequest struct {
	TpuType      string
	Topology     string
	ChipsPerNode int64
}

// String returns a human-readable description of the request.
func (r TpuRequest) String() string {
	if r.Empty() {
		return "no request"
	}
	return r.Signature()
}

// Signature returns a stable string representation of the request.
func (r TpuRequest) Signature() string {
	return fmt.Sprintf("type: %q, count: %d, topology: %q", r.TpuType, r.ChipsPerNode, r.Topology)
}

// Empty returns true if r isn't an actual TPU request, but e.g. a zero-value for TpuRequest.
func (r TpuRequest) Empty() bool {
	return r.ChipsPerNode == 0 || r.TpuType == ""
}

// tpuPodsRequirements returns nodeGroupRequirements for each TPU spec requested by
// any pod in pods argument.
func (m *AutoprovisioningNodeGroupManager) tpuPodsRequirements(ctx *injectionContext, pods []*apiv1.Pod) []nodeGroupRequirements {
	requestingPods := make(map[TpuRequest][]*apiv1.Pod)
	for _, pod := range pods {
		// We don't want to go through possibly creating TPU requirements for a misconfigured GPU pod
		if _, ok := pod.Spec.NodeSelector[labels.GPULabel]; ok {
			continue
		}

		// AcceleratorCountLabel is either injected by NAP if the resource request is set for TPU, or can also be set explicitly
		if _, found := pod.Spec.NodeSelector[labels.AcceleratorCountLabel]; !found && !m.isDraTpuPod(pod) {
			continue
		}

		req := podrequirements.GetRequirements(pod)
		tpuType, isSpecified := req.LabelReq.GetSingleValue(labels.TPULabel)

		cc, ccName, ccErr := m.computeClassLister.PodReqCrd(req)
		if ccErr != nil {
			ccType, ccTypeErr := m.computeClassLister.PodReqCrdType(req)
			ctx.status.SetPodError(pod.UID, NewComputeClassNotFoundError(ccName, ccType, ccTypeErr))
			continue
		}
		// At this point, we need to check if the pod requires the TPU by either CC rule or node selector
		if !isSpecified {
			if cc == nil || ccName == "" {
				continue
			}
			if !ccHasTPU(cc) {
				continue
			}
		}

		if cc != nil && ccName != "" {
			// For CC pods we want to derive TPU config from CC in computeclass generator,
			// so we insert empty TPU Request.
			tpuRequest := TpuRequest{}
			requestingPods[tpuRequest] = append(requestingPods[tpuRequest], pod)
		}

		if cc != nil && !cc.ScaleUpAnyway() {
			continue
		}

		// Default NAP flow is not supported for workloads requesting TPUs through DRA APIs
		// as we are not able to infer the amount of TPU chips per node from resource requests.
		if m.isDraTpuPod(pod) {
			continue
		}

		if !m.cloudProvider.MachineConfigProvider().IsTpuNapSupported(tpuType) {
			metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonTPUTypeUnsupported)
			err := NewTpuTypeNotSupportedError(tpuType)
			ctx.status.SetPodError(pod.UID, err)
			continue
		}

		if !ctx.resourceLimiter.HasMaxLimitSet(tpuType) {
			// Don't report UnexpectedPod metric - the pod is defined correctly,
			// but the NAP won't work for it due to cluster's autoprovisioning limit unset.
			err := NewTpuTypeNoLimitDefinedError(tpuType)
			ctx.status.SetPodError(pod.UID, err)
			continue
		}

		topologyLabelValue := pod.Spec.NodeSelector[labels.TPUTopologyLabel]
		chipsPerNodeValue := pod.Spec.NodeSelector[labels.AcceleratorCountLabel]
		chipsPerNode, err := strconv.Atoi(chipsPerNodeValue)
		if err != nil {
			metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonTPUAcceleratorCountMissing)
			// accelerator count label is injected by GCW based on TPU requirements
			err := NewTpuTypeNoLimitDefinedError(tpuType)
			ctx.status.SetPodError(pod.UID, err)
			continue
		}
		if !m.cloudProvider.MachineConfigProvider().IsTPUCountSupported(tpuType, int64(chipsPerNode)) {
			err := NewTpuTypeInvalidAcceleratorCount(tpuType, chipsPerNode)
			ctx.status.SetPodError(pod.UID, err)
			continue
		}

		tpuRequestForPod := TpuRequest{TpuType: tpuType, Topology: topologyLabelValue, ChipsPerNode: int64(chipsPerNode)}
		requestingPods[tpuRequestForPod] = append(requestingPods[tpuRequestForPod], pod)
	}

	var result []nodeGroupRequirements

	for tpuRequest, tpuRequestPods := range requestingPods {
		allRequirements, podErrors := m.extractRequirements(tpuRequestPods, machinetypes.GpuRequest{}, tpuRequest)
		for podUid, podErr := range podErrors {
			ctx.status.SetPodError(podUid, podErr)
		}
		for _, requirements := range allRequirements {
			if requirements.hasPods() {
				result = append(result, requirements)
			}
		}
	}
	return result
}

func ccHasTPU(cc crd.CRD) bool {
	for _, rule := range cc.Rules() {
		if rule.HasTpu() {
			return true
		}
	}
	return false
}
