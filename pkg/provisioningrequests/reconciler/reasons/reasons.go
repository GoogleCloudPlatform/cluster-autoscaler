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

package reasons

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	klog "k8s.io/klog/v2"
)

const (
	// Bulk MIGs
	BulkNodePoolUpdateFailedReason  = "InternalError"
	BulkNodePoolUpdateFailedMessage = "Provisioning Request could not update the associated node pool."
	BulkIncreaseSizeFailedReason    = "InternalError"
	BulkIncreaseSizeFailedMessage   = "Provisioning Request could not enqueue for instances."

	// We depend on the exact error message below and the `LIMIT_EXCEEDED` code to recognize Resize Request related `ValidUntilTime` timeout error
	// and change the error message to Provisioning Request specific one.
	queuedResizeRequestValidUntilExceededErrorMessage = "Resize request could not provision queued instances in the allocated time."
	limitExceededErrorCode                            = "LIMIT_EXCEEDED"
	validUntilExceededHelperCode                      = "WAIT_TIME_EXCEEDED" // Doesn't exist; introduced only to have consistent Failed reason messages
	validUntilExceededFailedReason                    = "WaitTimeExceeded"
	validUntilExceededFailedMessage                   = "Provisioning Request could not provision queued instances in the allocated time."

	// Error codes translations to reasons passed to the Provisioning Requests failed state.
	limitExceededReason                = "LimitExceeded"
	resourcePoolExhaustedReason        = "ResourcePoolExhausted"
	quotaExceededReason                = "QuotaExceeded"
	ipSpaceExhaustedReason             = "IPSpaceExhausted"
	permissionsErrorReason             = "PermissionsError"
	vmExternalIpAccessPolicyConstraint = "VMExternalIPAccessPolicyConstraint"
	internalErrorReason                = "InternalError"
	resizeRequestFailedNoErrorsReason  = "InternalErrorResizeRequestFailed"
	resizeRequestFailedNoErrorsMessage = "Provisioning Request failed, but no errors with details can be provided."

	DefaultSurfacedErrorsLimit      = 5
	FlexStartSurfacedErrorsLimit    = 1
	otherErrorMessagesPrefix        = "The remaining received errors"
	unrecognizedErrorMessagePrefix  = "Received unrecognized error"
	unrecognizedErrorsMessagePrefix = "Received unrecognized errors"
	errorsDelimiter                 = "; "

	// Resize Request creation error codes translations to reasons passed to the Provisioning Requests failed state.
	invalidArgumentCode                 = "INVALID_ARGUMENT"
	invalidArgumentReason               = "InvalidArgument"
	queuingInfeasibleNoCapacityCode     = "QUEUING_INFEASIBLE_NO_CAPACITY"
	queuingInfeasibleNoCapacityReason   = "QueuingInfeasibleNoCapacity"
	invalidMachineTypeCode              = "INVALID_MACHINE_TYPE"
	invalidMachineTypeReason            = "InvalidMachineType"
	invalidGpuTypeCode                  = "INVALID_GPU_TYPE"
	invalidGpuTypeReason                = "InvalidGPUType"
	aggregateReservationNotExistCode    = "AGGREGATE_RESERVATION_NOT_EXIST"
	aggregateReservationNotExistReason  = "ZoneNotSupported"
	aggregateReservationNotExistMessage = "Provisioning Request is not supported in this zone. Try different locations."
	permissionDeniedCode                = "PERMISSION_DENIED"
	permissionDeniedReason              = "PermissionDenied"
	conditionNotMetCode                 = "CONDITION_NOT_MET"
	conditionNotMetReason               = "ConditionNotMet"
	unsupportedOperationCode            = "UNSUPPORTED_OPERATION"
	unsupportedOperationReason          = "UnsupportedOperation"
	failedResizeRequestCreationReason   = "InternalErrorFailedToQueue"

	// Reservation error codes translations to reasons
	invalidReservationReason          = "InvalidReservation"
	reservationNotFoundReason         = "ReservationNotFound"
	reservationNotReadyReason         = "ReservationNotReady"
	reservationCapacityExceededReason = "ReservationCapacityExceeded"
	reservationIncompatibleReason     = "ReservationIncompatible"

	acceptedTimeoutedResReqNoErrorsReason  = "AcceptedRequestTimedOut"
	acceptedTimeoutedResReqNoErrorsMessage = "Request wasn't provisioned in the allocated time, no errors were provided."
	cancelledResReqNoErrorsReason          = "RequestWasCancelled"
	cancelledResReqNoErrorsMessage         = "Request was unexpectedly cancelled, no errors were provided."
	noReasonProvidedReason                 = "InternalErrorNoReason"
	noReasonProvidedMessage                = "Request wasn't provisioned, no errors were provided."
	groupByReasonDelimiter                 = "; "
	groupedMsgsPrefix                      = "Got multiple errors: "
	maxErrorGroupByReasonSize              = 5
)

type ResizeRequestCategory string

const (
	SuccessfulCategory ResizeRequestCategory = "successful"
	FailedCategory     ResizeRequestCategory = "failed"
	TimeoutCategory    ResizeRequestCategory = "timeout"
	QueueingCategory   ResizeRequestCategory = "queueing"
	UnexpectedCategory ResizeRequestCategory = "unexpected"
	CleanUpCategory    ResizeRequestCategory = "cleanup"
)

type ErrorReasonMessage struct {
	Reason, Message string
}

var (
	failedResizeRequestCreationMessage = func(nodepool, zone string, err error) string {
		return fmt.Sprintf("Provisioning Request failed to queue in nodepool %q in zone %s, got error: %v", nodepool, zone, err)
	}
	queuingInfeasibleNoCapacityMessage = func(zone string) string {
		return fmt.Sprintf("Could not enqueue the Provisioning Request due to insufficient capacity in zone %s. Try different locations or hardware.", zone)
	}
)

// DwsErrorInfo contains the error (or lack thereof) extracted from the ResizeRequest or BulkMig, as well as human-readable explanation for the error
type DwsErrorInfo struct {
	// Reason contains a human-readable reason which is surfaced in the Provisioning Request to the user, explaining the update
	Reason string
	// Message contains a human-readable message which is surfaced in the Provisioning Request to the user, explaining the update
	Message string
	// If the ResizeRequest/BulkMig has an error, InstanceError contains it in the InstanceErrorInfo format, which can be passed to RegisterFailedScaleUp
	InstanceError *cloudprovider.InstanceErrorInfo
}

// GetDwsErrorInfoFromLastAttemptErrors returns the DwsErrorInfo for a Resize Request based on the last attempt errors.
// It prioritizes the QUOTA_EXCEEDED errors as they are user actionable. It returns the info for a single error.
func GetDwsErrorInfoFromLastAttemptErrors(resizeRequestRef string, errors []resizerequestclient.DwsStatusError) DwsErrorInfo {
	return getDwsErrorInfoFromErrors(resizeRequestRef, "Resize Request", "last attempt", errors)
}

// GetDwsErrorInfoFromLastProgressCheckErrors returns the DwsErrorInfo for a Bulk Mig based on the last progress check errors.
// It prioritizes the QUOTA_EXCEEDED errors as they are user actionable. It returns the info for a single error.
func GetDwsErrorInfoFromLastProgressCheckErrors(bulkMigName string, errors []resizerequestclient.DwsStatusError) DwsErrorInfo {
	return getDwsErrorInfoFromErrors(bulkMigName, "Bulk Mig", "last progress check", errors)
}

// getDwsErrorInfoFromErrors returns the DwsErrorInfo for a Resize Request / BulkMig
// based on the provided last attempt / last progress check errors.
// It prioritizes the QUOTA_EXCEEDED errors as they are user actionable. It returns the info for a single error.
func getDwsErrorInfoFromErrors(ref, gceResource, errorKind string, errors []resizerequestclient.DwsStatusError) DwsErrorInfo {
	if len(errors) == 0 {
		return DwsErrorInfo{provreqstate.ProvisionedInitReason, provreqstate.ProvisionedInitMessage, nil}
	}
	allRawErrors := make([]string, 0, len(errors))
	for _, err := range errors {
		allRawErrors = append(allRawErrors, errorCodeMessage(err.Code, err.Message))
	}
	klog.Infof("%s %q got %d %s errors: %s", gceResource, ref, len(allRawErrors), errorKind, strings.Join(allRawErrors, errorsDelimiter))

	var errorInfo *cloudprovider.InstanceErrorInfo
	for _, err := range errors {
		// Pick the QUOTA_EXCEEDED error if exists, pick the first recognized otherwise.
		if newErrorInfo := getErrorInfoFromDwsStatusError(err); newErrorInfo != nil {
			if errorInfo == nil {
				errorInfo = newErrorInfo
			}
			if err.Code == gce.ErrorCodeQuotaExceeded {
				errorInfo = newErrorInfo
				break
			}
		}
	}
	if errorInfo == nil {
		klog.Warningf("%s %q didn't provide any %s errors.", gceResource, ref, errorKind)
		return DwsErrorInfo{provreqstate.ProvisionedInitReason, provreqstate.ProvisionedInitMessage, nil}
	}
	return DwsErrorInfo{errorCodeToReason(errorInfo.ErrorCode), errorInfo.ErrorMessage, errorInfo}
}

// GetDwsErrorInfoFromResizeRequestErrors returns the reason and message for a Failed Resize Request
// if there are recognized instance errors, the latest one will be used as the reason and main part of the message;
// additionally, combined error codes and messages of last `surfacedErrorsLimit` errors will be included in the message.
func GetDwsErrorInfoFromResizeRequestErrors(rrRef string, errors []resizerequestclient.DwsStatusError, surfacedErrorsLimit int) DwsErrorInfo {
	allRawErrors := make([]string, 0, len(errors))
	for _, err := range errors {
		allRawErrors = append(allRawErrors, errorCodeMessage(err.Code, err.Message))
	}
	klog.Infof("Resize Request %s failed with %d errors: %s", rrRef, len(allRawErrors), strings.Join(allRawErrors, errorsDelimiter))

	// Translate Resize Request GCE error messages to CA errorInfo and pick the last recognized error
	errorInfo, allErrorCodeMessages := pickMainError(errors)
	if errorInfo == nil {
		klog.Warningf("Resize Request %s failed without providing any errors.", rrRef)
		return DwsErrorInfo{resizeRequestFailedNoErrorsReason, resizeRequestFailedNoErrorsMessage, nil}
	}

	reason := errorCodeToReason(errorInfo.ErrorCode)
	if len(allErrorCodeMessages) == 1 || (surfacedErrorsLimit == 1 && len(allErrorCodeMessages) > 0) {
		if reason == internalErrorReason {
			return DwsErrorInfo{reason, fmt.Sprintf("%s: %s", unrecognizedErrorMessagePrefix, allErrorCodeMessages[0]), errorInfo}
		}
		return DwsErrorInfo{reason, errorInfo.ErrorMessage, errorInfo}
	}

	// Surface additional errors up to the given limit
	firstIncludedErrorIndex := maxInt(0, len(allErrorCodeMessages)-surfacedErrorsLimit)
	includedErrors := allErrorCodeMessages[firstIncludedErrorIndex:]

	// All errors were unrecognized - the message consists of last `surfacedErrorsLimit` errors
	if reason == internalErrorReason {
		return DwsErrorInfo{reason, fmt.Sprintf("%s: %s", unrecognizedErrorsMessagePrefix, strings.Join(includedErrors, errorsDelimiter)), errorInfo}
	}
	return DwsErrorInfo{reason, multipleErrorsMessage(errorInfo, includedErrors), errorInfo}
}

// GetDwsErrorInfoFromResizeRequestOperationError returns the DwsErrorInfo for a
// Failed Resize Request based on errors returned by its creation
// It also returnes boolean if the returned error should result in a backoff for the given MIG.
func GetDwsErrorInfoFromResizeRequestOperationError(err error, nodepool, zone string) (DwsErrorInfo, bool) {
	var multiErr *resizerequestclient.ResizeRequestOperationMultiError
	if errors.As(err, &multiErr) {
		if len(multiErr.Errors) > 1 {
			klog.Errorf("Received more than 1 Resize Request creation error (picking the last one and ignoring other ones): %s", multiErr.Error())
		}

		// Pick last error as the main one in case there are multiple
		mainError := multiErr.Errors[len(multiErr.Errors)-1]
		mainErrorAsInstanceErrorInfo := gce.GetErrorInfo(mainError.Code, mainError.Message, "", nil)
		switch mainError.Code {
		case invalidArgumentCode:
			return DwsErrorInfo{invalidArgumentReason, mainError.Message, nil}, false
		case queuingInfeasibleNoCapacityCode:
			return DwsErrorInfo{queuingInfeasibleNoCapacityReason, queuingInfeasibleNoCapacityMessage(zone), mainErrorAsInstanceErrorInfo}, true
		case invalidMachineTypeCode:
			return DwsErrorInfo{invalidMachineTypeReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case invalidGpuTypeCode:
			return DwsErrorInfo{invalidGpuTypeReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case aggregateReservationNotExistCode:
			return DwsErrorInfo{aggregateReservationNotExistReason, aggregateReservationNotExistMessage, mainErrorAsInstanceErrorInfo}, true
		case permissionDeniedCode:
			return DwsErrorInfo{permissionDeniedReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case conditionNotMetCode:
			return DwsErrorInfo{conditionNotMetReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case unsupportedOperationCode:
			return DwsErrorInfo{unsupportedOperationReason, mainError.Message, nil}, false
		case gce.ErrorCodeQuotaExceeded:
			return DwsErrorInfo{quotaExceededReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case gce.ErrorInvalidReservation:
			return DwsErrorInfo{invalidReservationReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case gce.ErrorReservationNotFound:
			return DwsErrorInfo{reservationNotFoundReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case gce.ErrorReservationNotReady:
			return DwsErrorInfo{reservationNotReadyReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case gce.ErrorReservationCapacityExceeded:
			return DwsErrorInfo{reservationCapacityExceededReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		case gce.ErrorReservationIncompatible:
			return DwsErrorInfo{reservationIncompatibleReason, mainError.Message, mainErrorAsInstanceErrorInfo}, true
		// TODO(b/381046606): remove after migration to CreateInstances API
		case resizerequestclient.FragmentedRRWarningCode:
			// Return backoff=false since there's no GCE issues, just a warning due to CA limitations.
			return DwsErrorInfo{resizerequestclient.FragmentedRRWarningReason, mainError.Message, nil}, false
		default:
			return DwsErrorInfo{failedResizeRequestCreationReason, failedResizeRequestCreationMessage(nodepool, zone, &mainError), mainErrorAsInstanceErrorInfo}, true
		}
	}
	return DwsErrorInfo{failedResizeRequestCreationReason, failedResizeRequestCreationMessage(nodepool, zone, err), nil}, true
}

func GroupResizeRequestErrors(failedRRs []resizerequestclient.ResizeRequestStatus, capacityCheckWaitTimeSeconds time.Duration, currentTime time.Time) (map[ErrorReasonMessage]int, DwsErrorInfo) {
	// Report cancelled/failed requests
	// errorReasonMessageCount is used for recording failed scale up events per specific error and number of affected VMs
	errorReasonMessageCount := map[ErrorReasonMessage]int{}
	// mainErrorInfo is used for registering a failed scale up in CSR, resulting in a scale up backoff
	// or as an error reason/message for OBTAINABILITY strategy Provisioning Requests
	var mainErrorEntry DwsErrorInfo
	for _, rr := range failedRRs {
		_, rrErrorInfo := ResizeRequestCategoryReasonMessage(rr, capacityCheckWaitTimeSeconds, currentTime)
		if rrErrorInfo == nil {
			continue
		}
		klog.Infof("Resize Request %s in state %q has error info %v", rr.Ref(), rr.State, rrErrorInfo)

		errorEntry := ErrorReasonMessage{Reason: rrErrorInfo.Reason, Message: rrErrorInfo.Message}
		errorReasonMessageCount[errorEntry] += int(rr.ResizeBy)

		// Picking first error as the main one,
		// unless the the first one is the `no errors` defaulted error, then replace it with a non-defaulted one
		if mainErrorEntry.Reason == "" || (isDefaultedNoErrorsReason(mainErrorEntry.Reason) && !isDefaultedNoErrorsReason(errorEntry.Reason)) {
			mainErrorEntry = *rrErrorInfo
		}
	}

	errorReasonMessageCount = groupByReason(errorReasonMessageCount)
	return errorReasonMessageCount, mainErrorEntry
}

// ResizeRequestCategoryReasonMessage returns failed/successful/queueing/unexpected Flex Resize Request category, along with the reason/message to report.
// Reason/message is extracted from appropriate error(s) based on the Resize Request state.
func ResizeRequestCategoryReasonMessage(rr resizerequestclient.ResizeRequestStatus, capacityCheckWaitTimeSeconds time.Duration, currentTime time.Time) (ResizeRequestCategory, *DwsErrorInfo) {
	rrErrorInfo := DwsErrorInfo{Reason: noReasonProvidedReason, Message: noReasonProvidedMessage, InstanceError: nil}
	switch rr.State {
	case resizerequestclient.ResizeRequestStateAccepted:
		if rr.CreationTime.Add(capacityCheckWaitTimeSeconds).After(currentTime) {
			klog.Infof("Resize Request %s is %s and new (created %+v), will expire in %+v, skipping", rr.Ref(), rr.State, rr.CreationTime, capacityCheckWaitTimeSeconds-currentTime.Sub(rr.CreationTime))
			return QueueingCategory, nil
		}

		if len(rr.LastAttemptErrors) > 0 {
			rrErrorInfo = GetDwsErrorInfoFromLastAttemptErrors(rr.Ref(), rr.LastAttemptErrors)
		} else {
			rrErrorInfo.Reason = acceptedTimeoutedResReqNoErrorsReason
			rrErrorInfo.Message = acceptedTimeoutedResReqNoErrorsMessage
		}
		return TimeoutCategory, &rrErrorInfo

	case resizerequestclient.ResizeRequestStateCancelled:
		if len(rr.LastAttemptErrors) > 0 {
			rrErrorInfo = GetDwsErrorInfoFromLastAttemptErrors(rr.Ref(), rr.LastAttemptErrors)
		} else {
			rrErrorInfo.Reason = cancelledResReqNoErrorsReason
			rrErrorInfo.Message = cancelledResReqNoErrorsMessage
		}
		return FailedCategory, &rrErrorInfo

	case resizerequestclient.ResizeRequestStateFailed:
		rrErrorInfo = GetDwsErrorInfoFromResizeRequestErrors(rr.Ref(), rr.Errors, FlexStartSurfacedErrorsLimit)
		return FailedCategory, &rrErrorInfo

	case resizerequestclient.ResizeRequestStateSucceeded:
		// TODO(b/480847645): why does this have rrErrorInfo returned?
		// I think it doesn't have to, but I'll remove and test this in a separate commit.
		return SuccessfulCategory, &rrErrorInfo

	case resizerequestclient.ResizeRequestStateDeleting, resizerequestclient.ResizeRequestStateCreating, resizerequestclient.ResizeRequestStateProvisioning:
		klog.Infof("Resize Request %s is %s, skipping", rr.Ref(), rr.State)
		return UnexpectedCategory, nil
	default:
		klog.Warningf("Resize Request %s has an unrecognized state %q", rr.Ref(), rr.State)
		return UnexpectedCategory, nil
	}
}

// multipleErrorsMessage returns message consisting of the chosen error's message used as the main message
// and the last 5 errors (4 if we have to exclude the chosen one)
func multipleErrorsMessage(errorInfo *cloudprovider.InstanceErrorInfo, errors []string) string {
	mainMessage := errorInfo.ErrorMessage
	mainErrorCodeMessage := errorCodeMessage(errorInfo.ErrorCode, mainMessage)
	for i, errCodeMessage := range errors {
		if errCodeMessage == mainErrorCodeMessage {
			errors = append(errors[:i], errors[i+1:]...)
			break
		}
	}
	return fmt.Sprintf("%s; %s: %s", mainMessage, otherErrorMessagesPrefix, strings.Join(errors, errorsDelimiter))
}

// pickMainError returns the last recognized error (or unrecognized if there are no recognized ones) and collects all error codes and messages
func pickMainError(errors []resizerequestclient.DwsStatusError) (*cloudprovider.InstanceErrorInfo, []string) {
	allErrorCodeMessages := make([]string, 0, len(errors))
	var errorInfo *cloudprovider.InstanceErrorInfo
	for _, err := range errors {
		if newErrorInfo := getErrorInfoFromDwsStatusError(err); newErrorInfo != nil {
			// Collect raw unrecognized errors and mapped recognized errors
			if newErrorInfo.ErrorCode == gce.ErrorCodeOther {
				allErrorCodeMessages = append(allErrorCodeMessages, errorCodeMessage(err.Code, err.Message))
			} else {
				allErrorCodeMessages = append(allErrorCodeMessages, errorCodeMessage(newErrorInfo.ErrorCode, newErrorInfo.ErrorMessage))
			}

			// Overwrite errorInfo when it's the first error or the new one was recognized
			if errorInfo == nil || newErrorInfo.ErrorCode != gce.ErrorCodeOther {
				errorInfo = newErrorInfo
			}
		}
	}
	return errorInfo, allErrorCodeMessages
}

// getErrorInfoFromDwsStatusError maps the error provided by Resize Request to CA instance error info.
func getErrorInfoFromDwsStatusError(err resizerequestclient.DwsStatusError) *cloudprovider.InstanceErrorInfo {
	// Error related directly to Resize Request is filtered as recognized one.
	if err.Code == limitExceededErrorCode {
		if err.Message == queuedResizeRequestValidUntilExceededErrorMessage {
			return &cloudprovider.InstanceErrorInfo{
				ErrorClass:   cloudprovider.OtherErrorClass,
				ErrorCode:    validUntilExceededHelperCode,
				ErrorMessage: validUntilExceededFailedMessage,
			}
		}
		return &cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OtherErrorClass,
			ErrorCode:    limitExceededErrorCode,
			ErrorMessage: err.Message,
		}
	}
	// Other errors are mapped using `GetErrorInfo` with "" as instanceStatus to ensure that returned errorInfo won't be `nil`
	// and `nil` as previousErrorInfo to always independly return errorInfo corresponding to the given error and not fallback to the previous one.
	errInfo := gce.GetErrorInfo(err.Code, err.Message, "", nil)
	errInfo.ErrorMessage = err.Message
	return errInfo
}

// errorCodeToReason translates CA instance error code to Failed reason.
func errorCodeToReason(code string) string {
	switch code {
	case validUntilExceededHelperCode:
		return validUntilExceededFailedReason
	case limitExceededErrorCode:
		return limitExceededReason
	case gce.ErrorCodeQuotaExceeded:
		return quotaExceededReason
	case gce.ErrorCodeResourcePoolExhausted:
		return resourcePoolExhaustedReason
	case gce.ErrorIPSpaceExhausted:
		return ipSpaceExhaustedReason
	case gce.ErrorCodePermissions:
		return permissionsErrorReason
	case gce.ErrorCodeVmExternalIpAccessPolicyConstraint:
		return vmExternalIpAccessPolicyConstraint
	case gce.ErrorInvalidReservation:
		return invalidReservationReason
	case gce.ErrorReservationNotFound:
		return reservationNotFoundReason
	case gce.ErrorReservationNotReady:
		return reservationNotReadyReason
	case gce.ErrorReservationCapacityExceeded:
		return reservationCapacityExceededReason
	case gce.ErrorReservationIncompatible:
		return reservationIncompatibleReason
	default: // gce.ErrorCodeOther
		return internalErrorReason
	}
}

func errorCodeMessage(code, msg string) string {
	return fmt.Sprintf("[%s] %q", code, msg)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Accumulates error counts with same reason and appends the messages into 1 entry
func groupByReason(errCntPerReason map[ErrorReasonMessage]int) map[ErrorReasonMessage]int {
	reasonToMessages := map[string]sets.Set[string]{}
	reasonToCount := map[string]int{}
	for errEntry, count := range errCntPerReason {
		if reasonToMessages[errEntry.Reason] == nil {
			reasonToMessages[errEntry.Reason] = sets.Set[string]{}
		}
		if len(reasonToMessages[errEntry.Reason]) < maxErrorGroupByReasonSize {
			reasonToMessages[errEntry.Reason].Insert(errEntry.Message)
		}
		reasonToCount[errEntry.Reason] += count
	}
	result := map[ErrorReasonMessage]int{}
	for reason, messages := range reasonToMessages {
		var message string
		if len(messages) > 1 {
			message = groupedMsgsPrefix + strings.Join(messages.UnsortedList(), groupByReasonDelimiter)
		} else {
			message, _ = messages.PopAny()
		}
		result[ErrorReasonMessage{Reason: reason, Message: message}] = reasonToCount[reason]
	}
	return result
}

func isDefaultedNoErrorsReason(reason string) bool {
	return reason == acceptedTimeoutedResReqNoErrorsReason || reason == cancelledResReqNoErrorsReason || reason == noReasonProvidedReason
}
