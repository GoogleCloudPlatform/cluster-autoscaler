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

package events

import (
	"fmt"
	"reflect"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	"k8s.io/klog/v2"
)

const (
	failedScaleUpReason       = "FailedScaleUp"
	zonalErrorMessageTemplate = "Node scale up in zones %v associated with this pod failed: %v. Pod is at risk of not being scheduled."

	// error messages
	quotaExceededMessage                     = "GCE quota exceeded"
	outOfResourcesMessage                    = "GCE out of resources"
	serviceAccountDeletedMessage             = "Service Account was deleted"
	ipSpaceExhaustedMessage                  = "IP space exhausted"
	unsupportedCompactPlacementConfigMessage = "Unsupported Compact Placement Config"
	nodeGroupAlreadyExistsMessageTemplate    = "Node group %q already exists"
	invalidTpuTopologyMessage                = "Invalid TPU topology"
	invalidTpuConfigurationMessage           = "Invalid TPU configuration"
	invalidReservationMessage                = "Invalid Reservation"
	timeoutMessage                           = "Some nodes in node group %q failed to appear in time"
	reservationNotReadyMessage               = "Reservation not ready"
	reservationCapacityExceededMessage       = "Reservation capacity exceeded"
	reservationNotFoundMessage               = "Reservation not found or incorrectly shared"
	reservationIncompatibleMessage           = "Reservation incompatible with node group"
	internalErrorMessage                     = "Internal error"

	messageLengthLimit = 250
)

// FailedScaleUpEventLogger is an interface used for emitting events about failed scale ups.
type FailedScaleUpEventLogger interface {
	EmitEventsFromFailure(context *context.AutoscalingContext, pods []*vistypes.Pod, nodeGroups []cloudprovider.NodeGroup, failures []*vistypes.Message)
}

// NewFailedScaleUpEventLogger returns an instance of failed scale up event logger.
func NewFailedScaleUpEventLogger() FailedScaleUpEventLogger {
	return &failedScaleUpEventLoggerImpl{}
}

type failedScaleUpEventLoggerImpl struct {
}

func (el *failedScaleUpEventLoggerImpl) EmitEventsFromFailure(context *context.AutoscalingContext, pods []*vistypes.Pod, nodeGroups []cloudprovider.NodeGroup, failures []*vistypes.Message) {
	if len(failures) < 1 {
		return
	}
	var zones []string
	existingZones := make(map[string]bool)
	var reasons []string
	existingReasons := make(map[string]bool)
	for _, ng := range nodeGroups {
		zone, err2 := zoneFromNodeGroup(ng)
		if err2 != nil {
			klog.Errorf("Failed to emit events about failed scale ups: %v", err2)
			return
		}
		if found := existingZones[zone]; !found {
			zones = append(zones, zone)
			existingZones[zone] = true
		}
	}
	for _, failure := range failures {
		failureMessage := failureToMessage(failure)
		if found := existingReasons[failureMessage]; !found {
			reasons = append(reasons, failureMessage)
			existingReasons[failureMessage] = true
		}
	}
	message := getEventMessage(strings.Join(reasons, ", "), strings.Join(zones, ", "))
	el.emitEventOnPods(context, pods, message)
}

func (el *failedScaleUpEventLoggerImpl) emitEventOnPods(context *context.AutoscalingContext, pods []*vistypes.Pod, message string) {
	if len(message) > messageLengthLimit {
		message = message[:messageLengthLimit]
	}
	for _, pod := range pods {
		v1Pod := &v1.Pod{}
		v1Pod.Name = pod.Name
		v1Pod.Namespace = pod.Namespace
		v1Pod.UID = types.UID(pod.Uid)
		context.Recorder.Event(v1Pod, v1.EventTypeWarning, failedScaleUpReason, message)
	}
}

func getEventMessage(failureReason string, zone string) string {
	return fmt.Sprintf(zonalErrorMessageTemplate, zone, failureReason)
}

func failureToMessage(failureMessage *vistypes.Message) string {
	switch failureMessage.Id {
	case vistypes.ScaleUpErrorQuotaExceeded:
		return quotaExceededMessage
	case vistypes.ScaleUpErrorOutOfResources:
		return outOfResourcesMessage
	case vistypes.ScaleUpErrorServiceAccountDeleted:
		return serviceAccountDeletedMessage
	case vistypes.ScaleUpErrorIPSpaceExhausted:
		return ipSpaceExhaustedMessage
	case vistypes.ScaleUpErrorUnsupportedCompactPlacementConfig:
		message := unsupportedCompactPlacementConfigMessage
		if len(failureMessage.Params) > 0 {
			message = message + ": " + failureMessage.Params[0]
		}
		return message
	case vistypes.ScaleUpErrorCompactPlacementNodeGroupAlreadyExists:
		nodeGroup := "unknown"
		if len(failureMessage.Params) > 0 {
			nodeGroup = failureMessage.Params[0]
		}
		return fmt.Sprintf(nodeGroupAlreadyExistsMessageTemplate, nodeGroup)
	case vistypes.ScaleUpErrorTpuTopologyInvalid:
		message := invalidTpuTopologyMessage
		if len(failureMessage.Params) > 0 {
			message = message + ": " + failureMessage.Params[0]
		}
		return message
	case vistypes.ScaleUpErrorTpuConfigurationInvalid:
		return invalidTpuConfigurationMessage
	case vistypes.ScaleUpErrorInvalidReservation:
		message := invalidReservationMessage
		if len(failureMessage.Params) > 0 {
			message = message + ": " + failureMessage.Params[0]
		}
		return message
	case vistypes.ScaleUpErrorReservationNotReady:
		message := reservationNotReadyMessage
		if len(failureMessage.Params) > 0 {
			message = message + ": " + failureMessage.Params[0]
		}
		return message
	case vistypes.ScaleUpErrorWaitingForInstancesTimeout:
		message := fmt.Sprintf(timeoutMessage, failureMessage.Params)
		return message
	case vistypes.ScaleUpErrorReservationCapacityExceeded:
		message := reservationCapacityExceededMessage
		if len(failureMessage.Params) > 0 {
			message = message + ": " + failureMessage.Params[0]
		}
		return message
	case vistypes.ScaleUpErrorReservationNotFound:
		message := reservationNotFoundMessage
		if len(failureMessage.Params) > 0 {
			message = message + ": " + failureMessage.Params[0]
		}
		return message
	case vistypes.ScaleUpErrorReservationIncompatible:
		message := reservationIncompatibleMessage
		if len(failureMessage.Params) > 0 {
			message = message + ": " + failureMessage.Params[0]
		}
		return message

	default:
		klog.Warningf("Not found specific error message for failure %v. Mapping to internal error message.", failureMessage)
		return internalErrorMessage
	}
}

func zoneFromNodeGroup(ng cloudprovider.NodeGroup) (string, error) {
	mig, ok := ng.(*gke.GkeMig)
	if !ok {
		return "", fmt.Errorf("unexpected cloudprovider.NodeGroup type, got: %s, want: *gke.GkeMig", reflect.TypeOf(ng))
	}
	return mig.GceRef().Zone, nil
}
