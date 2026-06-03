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

package types

import (
	"reflect"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	klog "k8s.io/klog/v2"
)

// GetNapStatus retrieves the status of NAP node group processing shared in the ProcessorCallbacks. If case of any errors,
// or if there's no status shared, the default status is returned.
func GetNapStatus(context *context.AutoscalingContext) *NapStatus {
	if context.ProcessorCallbacks == nil {
		return DefaultNapStatus()
	}
	napStatusRawValue, present := context.ProcessorCallbacks.GetExtraValue(autoprovisioning.ProcessingStatusContextKey)
	if !present {
		return DefaultNapStatus()
	}

	originalNapStatus, ok := napStatusRawValue.(*autoprovisioning.ProcessingStatus)
	if ok {
		napStatus, err := ConvertNapStatus(originalNapStatus)
		if err != nil {
			klog.Errorf("Error converting NAP status: %v", err)
			return DefaultNapStatus()
		}
		return napStatus
	}

	klog.Errorf("Value under %s in ProcessorCallbacks isn't of type *autoprovisioning.ProcessingStatus, got %v, want *autoprovisioning.ProcessingStatus",
		autoprovisioning.ProcessingStatusContextKey, reflect.TypeOf(napStatusRawValue))
	return DefaultNapStatus()
}

// ScaleUpFailureToVisMessage translates provided failureReason to visibility message.
func ScaleUpFailureToVisMessage(failureReason string, nodeGroupId string, err error) *Message {
	if failureReason == string(metrics.APIError) {
		// This is a synchronous error, so we wouldn't have issued any events for this scale-up.
		return nil
	}
	if failureReason == string(metrics.Timeout) {
		return NewScaleUpErrorWaitingForInstancesTimeoutMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorCodeResourcePoolExhausted {
		return NewScaleUpErrorOutOfResourcesMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorCodeQuotaExceeded {
		return NewScaleUpErrorQuotaExceededMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorIPSpaceExhausted {
		return NewScaleUpErrorIPSpaceExhaustedMsg(nodeGroupId)
	}
	if failureReason == gkeclient.ServiceAccountDeleted {
		return NewScaleUpErrorServiceAccountDeletedMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorCodeOther {
		return NewScaleUpErrorOtherMsg(nodeGroupId)
	}
	if failureReason == string(placement.InvalidPlacementGroupNameError) {
		configErr, ok := err.(*placement.ErrUnsupportedCompactPlacementConfig)
		if ok {
			return NewScaleUpErrorUnsupportedCompactPlacementConfigMsg(configErr.Msg)
		}
		klog.Warningf("Scale up failure reason is %q, but autoscaler error %T couldn't be casted to ErrUnsupportedCompactPlacementConfig", failureReason, err)
		return NewScaleUpErrorUnsupportedCompactPlacementConfigMsg("unknown")
	}
	if failureReason == string(placement.NodeGroupAlreadyExistsError) {
		npExistsErr, ok := err.(*placement.ErrNodeGroupAlreadyExists)
		if ok {
			return NewScaleUpErrorCompactPlacementNodeGroupAlreadyExistsMsg(npExistsErr.NodeGroup)
		}
		klog.Warningf("Scale up failure reason is %q, but autoscaler error %T couldn't be casted to ErrNodeGroupAlreadyExists", failureReason, err)
		return NewScaleUpErrorCompactPlacementNodeGroupAlreadyExistsMsg("unknown")
	}
	if failureReason == string(tpu.InvalidTpuTopologyError) {
		invalidTopologyErr, ok := err.(*tpu.ErrInvalidTpuTopology)
		if ok {
			return NewScaleUpErrorTpuTopologyInvalid(invalidTopologyErr.Topology)
		}
		klog.Warningf("Scale up failure reason is %q, but autoscaler error %T couldn't be casted to ErrInvalidTpuTopology", failureReason, err)
		return NewScaleUpErrorTpuTopologyInvalid("unknown topology")
	}
	if failureReason == gce.ErrorUnsupportedTpuConfiguration {
		return NewScaleUpErrorTpuConfigurationInvalid(nodeGroupId)
	}
	if failureReason == gce.ErrorInvalidReservation {
		return NewScaleUpErrorInvalidReservationMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorReservationNotReady {
		return NewScaleUpErrorReservationNotReadyMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorReservationCapacityExceeded {
		return NewScaleUpErrorReservationCapacityExceededMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorReservationNotFound {
		return NewScaleUpErrorReservationNotFoundMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorReservationIncompatible {
		return NewScaleUpErrorReservationIncompatibleMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorAutomaticReservationsNotAvailable {
		return NewScaleUpErrorAutomaticReservationsNotAvailableMsg(nodeGroupId)
	}
	if failureReason == gce.ErrorAutomaticReservationsNoCapacity {
		return NewScaleUpErrorAutomaticReservationsNoCapacityMsg(nodeGroupId)
	}
	klog.Warningf("CA Viz scale-up failure unexpected reason encountered: %v", failureReason)
	return NewScaleUpErrorOtherMsg(nodeGroupId)
}
