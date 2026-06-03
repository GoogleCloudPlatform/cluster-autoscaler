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

package provreqstate

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"

	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	klog "k8s.io/klog/v2"
)

type ProvisioningRequestState string

const (
	UninitializedState   = ProvisioningRequestState("Uninitialized")
	PendingState         = ProvisioningRequestState("Pending")
	AcceptedState        = ProvisioningRequestState(provreqv1.Accepted)
	ProvisionedState     = ProvisioningRequestState(provreqv1.Provisioned)
	BookingExpiredState  = ProvisioningRequestState(provreqv1.BookingExpired)
	CapacityRevokedState = ProvisioningRequestState(provreqv1.CapacityRevoked)
	FailedState          = ProvisioningRequestState(provreqv1.Failed)
	InvalidState         = ProvisioningRequestState("Invalid")

	DeprecatedProvisioningCondition = "Provisioning"
)

var (
	// BookingDuration - period during which the nodes requested by ProvisioningRequests are immune to scale down to ensure graceful start time for pods.
	BookingDuration = 10 * time.Minute
)

// ProvisioningRequestStatus contains information about the current status
// of the Provisioning Request.
type ProvisioningRequestStatus struct {
	State              ProvisioningRequestState
	Reason             string
	Message            string
	LastTransitionTime v1.Time
}

// StateOfProvisioningRequest returns the state of the ProvisioningRequest based on the Conditions present in the ProvisioningRequestStatus.
func StateOfProvisioningRequest(pr *provreqwrapper.ProvisioningRequest) ProvisioningRequestState {

	// The ProvisioningRequest is in "Uninitialized" state when it's new and doesn't have any Conditions
	// Otherwise, the ProvisioningRequest has a valid state if Conditions of the ProvisioningRequestStatus are:
	// * valid - Conditions can only be of types: "Accepted"/"Provisioned"/"Failed" (deprecated "Provisioning" is also valid),
	// * unique - only one Condition entry of each type exists,
	// * binary - each Condition can only have "True"/"False" status,
	// * the set of Conditions must be one of the following:
	//		- 0 "True" Conditions, indicating the Pending State,
	//		- 1 "True" Condition: "Accepted"/"Failed", indicating the corresponding states,
	//		- 2 "True" Conditions: "Accepted" and "Provisioned"/"Failed", the latter indicating the corresponding states.
	// 	 	The remaining Conditions in each case are required to be "False".
	// If any of these properties are violated, then the state is "Invalid" and a corresponding error is logged.
	// "Provisioned" and "Failed" states are terminal and cannot be updated.

	if len(pr.Status.Conditions) == 0 {
		return UninitializedState
	}

	// Validate the Conditions' types
	presentConditions := make(map[string]v1.Condition)
	for _, condition := range pr.Status.Conditions {
		if !isValid(condition.Type) {
			klog.Errorf("ProvisioningRequest %s/%s has unknown condition type %q", pr.Namespace, pr.Name, condition.Type)
			return InvalidState
		}

		if _, found := presentConditions[condition.Type]; found {
			klog.Errorf("ProvisioningRequest %s/%s has multiple conditions of type %q", pr.Namespace, pr.Name, condition.Type)
			return InvalidState
		}
		presentConditions[condition.Type] = condition
	}
	// Ignore the deprecated Provisioning condition
	delete(presentConditions, DeprecatedProvisioningCondition)

	// Validate the Conditions' statuses
	stateConditions := map[string]bool{}
	for _, condition := range presentConditions {
		if condition.Status == v1.ConditionUnknown {
			klog.Errorf("ProvisioningRequest %s/%s has condition of type %q with status \"Unknown\"", pr.Namespace, pr.Name, condition.Type)
			return InvalidState
		}
		if condition.Status == v1.ConditionTrue {
			stateConditions[condition.Type] = true
		}
	}

	// Find the Condition indicating the state
	if stateConditions[provreqv1.CapacityRevoked] {
		if err := validateConditions(stateConditions, []string{provreqv1.Accepted, provreqv1.Provisioned, provreqv1.BookingExpired}, []string{provreqv1.Failed}); err != nil {
			klog.Errorf("ProvisioningRequest %s/%s failed with following error: %v", pr.Namespace, pr.Name, err)
			return InvalidState
		}
		return CapacityRevokedState
	}
	if stateConditions[provreqv1.BookingExpired] {
		if err := validateConditions(stateConditions, []string{provreqv1.Accepted, provreqv1.Provisioned}, []string{provreqv1.Failed}); err != nil {
			klog.Errorf("ProvisioningRequest %s/%s failed with following error: %v", pr.Namespace, pr.Name, err)
			return InvalidState
		}
		return BookingExpiredState
	}
	if stateConditions[provreqv1.Failed] {
		if err := validateConditions(stateConditions, nil, []string{provreqv1.Provisioned}); err != nil {
			klog.Errorf("ProvisioningRequest %s/%s failed with following error: %v", pr.Namespace, pr.Name, err)
			return InvalidState
		}
		return FailedState
	}
	if stateConditions[provreqv1.Provisioned] {
		if err := validateConditions(stateConditions, []string{provreqv1.Accepted}, nil); err != nil {
			klog.Errorf("ProvisioningRequest %s/%s failed with following error: %v", pr.Namespace, pr.Name, err)
			return InvalidState
		}
		return ProvisionedState
	}
	if stateConditions[provreqv1.Accepted] {
		return AcceptedState
	}
	if len(stateConditions) == 0 {
		return PendingState
	}

	klog.Errorf("ProvisioningRequest %s/%s has unrecognized conditions %v", pr.Namespace, pr.Name, stateConditions)
	return InvalidState
}

// StatusOfProvisioningRequest returns the state of the ProvisioningRequest and its reason.
func StatusOfProvisioningRequest(pr *provreqwrapper.ProvisioningRequest) ProvisioningRequestStatus {
	state := StateOfProvisioningRequest(pr)
	if state == InvalidState || state == PendingState || state == UninitializedState {
		return ProvisioningRequestStatus{
			State:              state,
			Reason:             string(state),
			LastTransitionTime: v1.NewTime(time.Time{}),
		}
	}

	status := ProvisioningRequestStatus{
		State: state,
	}
	if foundCondition := k8sapimeta.FindStatusCondition(pr.Status.Conditions, string(state)); foundCondition != nil {
		status.Reason = foundCondition.Reason
		status.Message = foundCondition.Message
		status.LastTransitionTime = foundCondition.LastTransitionTime
	}
	return status
}

// SetProvisioningClassDetails of the Provisioning Request or return error if the request is not in Pending state or has details already assigned.
func SetProvisioningClassDetails(pr *provreqwrapper.ProvisioningRequest, details *queuedwrapper.ProvisioningClassDetails) error {
	currentState := StateOfProvisioningRequest(pr)
	obtainabilityStrategyProvReqUpdate := allowedObtainabilityStrategyDetailsUpdate(pr, currentState)
	if currentState != PendingState && !obtainabilityStrategyProvReqUpdate {
		return fmt.Errorf("update of provisioning class details of Provisioning Request %s/%s in state %s is forbidden", pr.Namespace, pr.Name, currentState)
	}
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	if currentDetails, found := GetProvisioningClassDetails(qpr); found && !obtainabilityStrategyProvReqUpdate {
		return fmt.Errorf("ProvisioningRequest %s/%s has details already assigned: %s", pr.Namespace, pr.Name, currentDetails)
	}

	qpr.SetProvisioningClassDetails(details)
	return nil
}

func allowedObtainabilityStrategyDetailsUpdate(pr *provreqwrapper.ProvisioningRequest, currentState ProvisioningRequestState) bool {
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	return qpr.ObtainabilityStrategy() && (currentState == PendingState || currentState == AcceptedState)
}

// ClearProvisioningClassDetails of the Provisioning Request or return error if the request is not in Pending state.
func ClearProvisioningClassDetails(pr *provreqwrapper.ProvisioningRequest) error {
	currentState := StateOfProvisioningRequest(pr)
	if currentState != PendingState {
		return fmt.Errorf("update of provisioning class details of Provisioning Request %s/%s in state %s is forbidden", pr.Namespace, pr.Name, currentState)
	}

	queuedwrapper.ToQueuedProvisioningRequest(*pr).ClearProvisioningClassDetails()
	return nil
}

// GetProvisioningClassDetails checks if ResizeRequestName, NodeGroupName, NodePoolName, AcceleratorType and Zone are set in ProvisioningRequest and returns present fields and their values.
func GetProvisioningClassDetails(pr *queuedwrapper.ProvisioningRequest) (string, bool) {
	detailsSet := []string{}
	if pr.NodeGroupName() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("MIG name = %s", *pr.NodeGroupName()))
	}
	if pr.ResizeRequestName() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("Resize Request name = %s", *pr.ResizeRequestName()))
	}
	if pr.NodePoolName() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("Node pool name = %s", *pr.NodePoolName()))
	}
	if pr.AcceleratorType() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("Accelerator type = %s", *pr.AcceleratorType()))
	}
	if pr.SelectedZone() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("Selected zone = %s", *pr.SelectedZone()))
	}
	if pr.NodePoolAutoProvisioned() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("Node Pool auto-provisioned = %s", *pr.NodePoolAutoProvisioned()))
	}
	if pr.PodTemplateName() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("Pod template name = %s", *pr.PodTemplateName()))
	}
	if pr.ProvisioningMode() != nil {
		detailsSet = append(detailsSet, fmt.Sprintf("Provisioning mode = %s", *pr.ProvisioningMode()))
	}
	details := strings.Join(detailsSet, ", ")
	return details, details != ""
}

func ForceSetStateCustomReasonMessage(pr *provreqwrapper.ProvisioningRequest, state ProvisioningRequestState, reason, message string, time v1.Time) error {
	return updateProvisioningRequestConditions(pr, state, reason, message, time, true)
}

// SetStateCustomReasonMessage updates state of a given ProvisioningRequest, with a user defined reason and message.
func SetStateCustomReasonMessage(pr *provreqwrapper.ProvisioningRequest, state ProvisioningRequestState, reason, message string, time v1.Time) error {
	return updateProvisioningRequestConditions(pr, state, reason, message, time, false)
}

// SetState updates state of the given ProvisioningRequest.
func SetState(pr *provreqwrapper.ProvisioningRequest, state ProvisioningRequestState, time v1.Time) error {
	return updateProvisioningRequestConditions(pr, state, "", "", time, false)
}

// UpdateOrSetProvisioningRequestCondition updates the given condition's status message and reason.
func UpdateOrSetProvisioningRequestCondition(pr *provreqwrapper.ProvisioningRequest, conditionType string, status v1.ConditionStatus, reason, message string, time v1.Time) bool {
	conditions := pr.Status.Conditions
	conditionChanged, conditionFound := false, false
	// Try to find the condition in exisiting list of conditions
	if foundCondition := k8sapimeta.FindStatusCondition(conditions, conditionType); foundCondition != nil {
		conditionFound = true
		if foundCondition.Status != status || foundCondition.Message != message || foundCondition.Reason != reason {
			foundCondition.Status = status
			foundCondition.Message = message
			foundCondition.Reason = reason
			foundCondition.LastTransitionTime = time
			foundCondition.ObservedGeneration = pr.Generation
			conditionChanged = true
		}
	}
	// If not found add it to the list at the end
	if !conditionFound {
		conditions = append(conditions, v1.Condition{
			Type:               conditionType,
			Status:             status,
			Message:            message,
			Reason:             reason,
			LastTransitionTime: time,
			ObservedGeneration: pr.Generation,
		})
		conditionChanged = true
	}

	if conditionChanged {
		pr.Status.Conditions = conditions
	}
	return conditionChanged
}

func updateProvisioningRequestConditions(pr *provreqwrapper.ProvisioningRequest, state ProvisioningRequestState, reason, message string, time v1.Time, force bool) error {
	currentState := StateOfProvisioningRequest(pr)
	if currentState == FailedState || currentState == CapacityRevokedState {
		return fmt.Errorf("cannot update Provisioning Request %s/%s from terminal state %s", pr.Namespace, pr.Name, currentState)
	}
	if state == UninitializedState {
		return fmt.Errorf("cannot update Provisioning Request %s/%s to uninitialized state", pr.Namespace, pr.Name)
	}
	if currentState == UninitializedState && state != PendingState && !force {
		return fmt.Errorf("cannot update Provisioning Request %s/%s to %s state, it has to be initialized first", pr.Namespace, pr.Name, state)
	}

	if state == PendingState {
		if message != "" || reason != "" {
			return fmt.Errorf("cannot update Provisioning Request %s/%s to Pending state with custom reason and message", pr.Namespace, pr.Name)
		}
		if len(pr.Status.Conditions) != 0 {
			return fmt.Errorf("already initialized Provisioning Request %s/%s", pr.Namespace, pr.Name)
		}

		initializeProvisioningRequestStatus(pr, time)
		return nil
	}

	conditionUpdate, err := provisioningRequestStateToConditionUpdate(state)
	if err != nil {
		return err
	}
	if reason == "" {
		reason = conditionUpdate.reason
	}
	if message == "" {
		message = conditionUpdate.message
	}

	UpdateOrSetProvisioningRequestCondition(pr, conditionUpdate.conditionType, v1.ConditionTrue, reason, message, time)
	return nil
}

func validateConditions(stateConditions map[string]bool, conditionsPresent []string, conditionsNotPresent []string) error {
	for _, condition := range conditionsPresent {
		if !stateConditions[condition] {
			return fmt.Errorf("condition %q was not yet set to \"True\", but should be", condition)
		}
	}
	for _, condition := range conditionsNotPresent {
		if stateConditions[condition] {
			return fmt.Errorf("condition %q was set to \"True\", but shouldn't be", condition)
		}
	}
	return nil
}

const (
	acceptedMessage        = "Provisioning Request was successfully queued."
	acceptedReason         = "SuccessfullyQueued"
	provisionedMessage     = "Provisioning Request was successfully provisioned."
	provisionedReason      = "Provisioned"
	bookingExpiredMessage  = "Capacity booking for the Provisioning Request has expired and the nodes are now candidates for scale down when underutilized."
	bookingExpiredReason   = "BookingExpired"
	capacityRevokedMessage = "Capacity provisioned for the Provisioning Request was reclaimed and is no longer available in the cluster."
	capacityRevokedReason  = "CapacityRevoked"
	defaultFailedMessage   = "Provisioning Request has failed."
	defaultFailedReason    = "Failed"
)

func provisioningRequestStateToConditionUpdate(state ProvisioningRequestState) (*conditionUpdate, error) {
	var update conditionUpdate
	switch state {
	case AcceptedState:
		update = conditionUpdate{provreqv1.Accepted, acceptedReason, acceptedMessage}
	case ProvisionedState:
		update = conditionUpdate{provreqv1.Provisioned, provisionedReason, provisionedMessage}
	case BookingExpiredState:
		update = conditionUpdate{provreqv1.BookingExpired, bookingExpiredReason, bookingExpiredMessage}
	case CapacityRevokedState:
		update = conditionUpdate{provreqv1.CapacityRevoked, capacityRevokedReason, capacityRevokedMessage}
	case FailedState:
		update = conditionUpdate{provreqv1.Failed, defaultFailedReason, defaultFailedMessage}
	default: // `Uninitialized`, `Pending`, `Invalid`, unknown Provisioning Request state
		return nil, fmt.Errorf("cannot update Provisioning Request to state %s", state)
	}
	return &update, nil
}

type conditionUpdate struct {
	conditionType, reason, message string
}

const (
	AcceptedInitReason     = "NotAccepted"
	AcceptedInitMessage    = "Provisioning Request wasn't accepted."
	ProvisionedInitReason  = "NotProvisioned"
	ProvisionedInitMessage = "Provisioning Request wasn't provisioned."
)

func initialCondition(conditionType, reason, message string, time v1.Time, observedGeneration int64) v1.Condition {
	return v1.Condition{Type: conditionType, Status: v1.ConditionFalse, Message: message, LastTransitionTime: time, Reason: reason, ObservedGeneration: observedGeneration}
}

// initializeProvisioningRequestStatus sets the Conditions to a list of all valid Conditions initialized as False, effectively moving the ProvisioningRequest to the Pending state.
func initializeProvisioningRequestStatus(pr *provreqwrapper.ProvisioningRequest, initTime v1.Time) {
	pr.Status.Conditions = []v1.Condition{
		initialCondition(provreqv1.Accepted, AcceptedInitReason, AcceptedInitMessage, initTime, pr.Generation),
	}
}

func isValid(conditionType string) bool {
	switch conditionType {
	case provreqv1.Accepted, provreqv1.Provisioned, provreqv1.Failed, provreqv1.BookingExpired, provreqv1.CapacityRevoked, DeprecatedProvisioningCondition:
		return true
	}
	return false
}

type provreqClient interface {
	ProvisioningRequests() ([]*provreqwrapper.ProvisioningRequest, error)
}

func ProvisioningRequestsInState(client provreqClient, state ProvisioningRequestState) ([]*provreqwrapper.ProvisioningRequest, error) {
	prs, err := client.ProvisioningRequests()
	if err != nil {
		return nil, err
	}
	prsInState := make([]*provreqwrapper.ProvisioningRequest, 0, len(prs))
	for _, pr := range prs {
		// Filter-out non-queued Provisioning Requests
		if pr.Spec.ProvisioningClassName != queuedwrapper.QueuedProvisioningClassName {
			continue
		}
		if StateOfProvisioningRequest(pr) == state {
			prsInState = append(prsInState, pr)
		}
	}
	return prsInState, nil
}
