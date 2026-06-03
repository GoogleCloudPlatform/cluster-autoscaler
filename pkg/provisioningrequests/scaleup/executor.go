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

package scaleup

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/klog/v2"
)

type scaleUpExecutor struct {
	autoscalingContext         *context.AutoscalingContext
	scaleStateNotifier         nodegroupchange.NodeGroupChangeObserver
	asyncNodeGroupStateChecker asyncnodegroups.AsyncNodeGroupStateChecker
}

// newScaleUpExecutor returns new instance of scaleUpExecutor.
func newScaleUpExecutor(
	autoscalingContext *context.AutoscalingContext,
	scaleStateNotifier nodegroupchange.NodeGroupChangeObserver,
	asyncNodeGroupStateChecker asyncnodegroups.AsyncNodeGroupStateChecker,
) *scaleUpExecutor {
	return &scaleUpExecutor{
		autoscalingContext:         autoscalingContext,
		scaleStateNotifier:         scaleStateNotifier,
		asyncNodeGroupStateChecker: asyncNodeGroupStateChecker,
	}
}

func (e *scaleUpExecutor) executeScaleUpForOption(
	option *CompositeOption,
	po *PartialOption,
	scaleUpState *scaleUpState,
	nodeInfos map[string]*framework.NodeInfo,
	now time.Time,
	podEquivalenceGroups []*equivalence.PodGroup,
	additionalInfo string,
	shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails,
) {
	targetSize, err := option.NodeGroup.TargetSize()
	if err != nil {
		scaleUpState.appendAutoscalerErrors(errors.ToAutoscalerError(errors.InternalError, err))
		return
	}
	targetSize += scaleUpState.registerScaleUp(option.NodeGroup.Id(), po.NodeCount)
	scaleUpInfo := nodegroupset.ScaleUpInfo{
		Group:       option.NodeGroup,
		CurrentSize: targetSize,
		NewSize:     targetSize + po.NodeCount,
		MaxSize:     option.NodeGroup.MaxSize(),
	}
	klog.V(1).Infof("Final scale-up plan: %v. %s", scaleUpInfo, additionalInfo)
	if aErr := e.executeScaleUp(scaleUpInfo, nodeInfos, po.ProvReqID, now, shouldUpdateProvReqDetails); aErr != nil {
		scaleUpState.appendResizeNodeGroups(scaleUpInfo.Group)
		scaleUpState.appendAutoscalerErrors(aErr)
		return
	}
	scaleUpState.appendScaleUpInfos(scaleUpInfo)
	scaleUpState.appendPodsAwaitEvaluation(orchestrator.GetPodsAwaitingEvaluation(podEquivalenceGroups, option.NodeGroup.Id())...)
}

// executeScaleUp creates Resize Request to execute the scale up option that was chosen.
func (e *scaleUpExecutor) executeScaleUp(
	info nodegroupset.ScaleUpInfo,
	nodeInfos map[string]*framework.NodeInfo,
	pr prpods.ProvReqID,
	now time.Time,
	shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails,
) errors.AutoscalerError {
	availableGPUTypes := e.autoscalingContext.CloudProvider.GetAvailableGPUTypes()
	nodeInfo, ok := nodeInfos[info.Group.Id()]
	if !ok {
		return errors.NewAutoscalerErrorf(errors.InternalError, "ProvReqScale-up: failed to get node info for node group %s", info.Group.Id())
	}
	gpuConfig := e.autoscalingContext.CloudProvider.GetNodeGpuConfig(nodeInfo.Node())
	gpuResourceName, gpuType := gpu.GetGpuInfoForMetrics(gpuConfig, availableGPUTypes, nodeInfo.Node(), nil)

	gkeMig, ok := info.Group.(*gke.GkeMig)
	if !ok {
		return errors.NewAutoscalerErrorf(errors.InternalError, "ProvReqScale-up: failed to cast node group %s to *gke.GkeMig, got type: %T", info.Group.Id(), info.Group)
	}

	klog.V(0).Infof("ProvReqScale-up: setting group %s size to %d. Provisioning Request: %s/%s", gkeMig.Id(), info.NewSize, pr.Namespace, pr.Name)
	e.autoscalingContext.LogRecorder.Eventf(apiv1.EventTypeNormal, "ProvReqScaledUpGroup", "ProvReqScale-up: Provisioning Request %s/%s setting group %s size to %d instead of %d (max: %d)", pr.Namespace, pr.Name, info.Group.Id(), info.NewSize, info.CurrentSize, info.MaxSize)
	increase := info.NewSize - info.CurrentSize
	if increase < 0 {
		return errors.NewAutoscalerErrorf(errors.InternalError, "increase in number of nodes cannot be negative, got: %v", increase)
	}
	if err := gkeMig.CreateQueuedInstances(pr, increase, shouldUpdateProvReqDetails); err != nil {
		e.autoscalingContext.LogRecorder.Eventf(apiv1.EventTypeWarning, "ProvReqFailedToScaleUpGroup", "ProvReqScale-up failed for Provisioning Request %s/%s for group %s: %v", pr.Namespace, pr.Name, info.Group.Id(), err)
		aerr := errors.ToAutoscalerError(errors.CloudProviderError, err).AddPrefix("failed to increase node group size: ")
		klog.Infof("Failed ProvReqScale-up in mig %q in zone %s: %v", gkeMig.GceRef().Name, gkeMig.GceRef().Zone, aerr)
		instanceErrorInfo := cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OtherErrorClass,
			ErrorCode:    string(aerr.Type()),
			ErrorMessage: aerr.Error(),
		}
		e.scaleStateNotifier.RegisterFailedScaleUp(gkeMig, increase, instanceErrorInfo, now)
		return aerr
	}
	if !info.Group.Exist() && e.asyncNodeGroupStateChecker.IsUpcoming(info.Group) {
		// Don't emit scale up event for upcoming node group as it will be generated after
		// the node group is created, during initial scale up.
		return nil
	}

	e.scaleStateNotifier.RegisterScaleUp(gkeMig, increase, now)
	metrics.RegisterScaleUp(increase, gpuResourceName, gpuType, "")
	e.autoscalingContext.LogRecorder.Eventf(apiv1.EventTypeNormal, "ProvReqScaledUpGroup", "ProvReqScale-up: Provisioning Request %s/%s group %s size set to %d instead of %d (max: %d)", pr.Namespace, pr.Name, info.Group.Id(), info.NewSize, info.CurrentSize, info.MaxSize)
	return nil
}
