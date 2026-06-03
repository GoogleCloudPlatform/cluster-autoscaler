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
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

// ProvisioningRequestInStateForTests returns ProvisioningRequest in given state with an example valid set of conditions representing that state.
func ProvisioningRequestInStateForTests(namespace, name, resizeRequestName, migName string, state ProvisioningRequestState, initTime time.Time, timeInc time.Duration, opts ...ProvReqOption) *provreqwrapper.ProvisioningRequest {
	return ProvisioningRequestWithConditionsForTests(namespace, name, conditionsForState(state, initTime, timeInc), resizeRequestName, migName, opts...)
}

// ProvisioningRequestWithConditionsForTests returns ProvisioningRequest with given set of conditions.
func ProvisioningRequestWithConditionsForTests(namespace, name string, conditions []metav1.Condition, resizeRequestName, migName string, opts ...ProvReqOption) *provreqwrapper.ProvisioningRequest {
	if namespace == "" {
		namespace = "default"
	}
	podTemplates := []*apiv1.PodTemplate{examplePodTemplate(namespace, name, 0)}
	v1PR := &v1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.ProvisioningRequestSpec{
			ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
			PodSets: []v1.PodSet{
				{
					Count: 1,
					PodTemplateRef: v1.Reference{
						Name: podTemplates[0].Name,
					},
				},
			},
		},
		Status: v1.ProvisioningRequestStatus{
			Conditions:               conditions,
			ProvisioningClassDetails: map[string]v1.Detail{},
		},
	}

	pr := provreqwrapper.NewProvisioningRequest(v1PR, podTemplates)
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	if migName != "" || resizeRequestName != "" {
		qpr.SetProvisioningClassDetails(&queuedwrapper.ProvisioningClassDetails{NodeGroupName: migName, ResizeRequestName: resizeRequestName, NodePoolName: fmt.Sprintf("np-%s", migName), AcceleratorType: "nvidia-tesla-t4", SelectedZone: "us-central1-c", ProvisioningMode: queuedwrapper.ProvisioningModeResizeRequest})
	}

	for _, opt := range opts {
		opt(qpr)
	}
	// Without this, the opts' modifications of PodTemplates are not getting applied
	pr.PodTemplates = qpr.PodTemplates
	pr.ProvisioningRequest = qpr.ProvisioningRequest.ProvisioningRequest
	return pr
}

// conditionsForState returns an example valid set of conditions for the given state.
func conditionsForState(state ProvisioningRequestState, initTime time.Time, timeInc time.Duration) []metav1.Condition {
	conditionWithInitAndInc := func(conditionType string, condStatus CondStatus) metav1.Condition {
		return conditionFromStatus(conditionType, condStatus, initTime, timeInc)
	}

	var conditions []metav1.Condition // Uninitialized state
	switch state {
	case PendingState:
		conditions = []metav1.Condition{
			conditionWithInitAndInc(v1.Accepted, InitCond),
		}
	case AcceptedState:
		conditions = []metav1.Condition{
			conditionWithInitAndInc(v1.Accepted, TrueCond),
		}
	case ProvisionedState:
		conditions = []metav1.Condition{
			conditionWithInitAndInc(v1.Accepted, TrueCond),
			conditionWithInitAndInc(v1.Provisioned, TrueCond),
		}
	case BookingExpiredState:
		conditions = []metav1.Condition{
			conditionWithInitAndInc(v1.Accepted, TrueCond),
			conditionWithInitAndInc(v1.Provisioned, TrueCond),
			conditionWithInitAndInc(v1.BookingExpired, TrueCond),
		}
	case CapacityRevokedState:
		conditions = []metav1.Condition{
			conditionWithInitAndInc(v1.Accepted, TrueCond),
			conditionWithInitAndInc(v1.Provisioned, TrueCond),
			conditionWithInitAndInc(v1.BookingExpired, TrueCond),
			conditionWithInitAndInc(v1.CapacityRevoked, TrueCond),
		}
	case FailedState: // an example condition set, there are multiple sets representing Failed state
		conditions = []metav1.Condition{
			conditionWithInitAndInc(v1.Accepted, TrueCond),
			conditionWithInitAndInc(v1.Provisioned, InitCond),
			conditionWithInitAndInc(v1.Failed, TrueCond),
		}
	case InvalidState: // an example condition set, there are multiple sets representing Invalid state
		conditions = []metav1.Condition{
			conditionWithInitAndInc(v1.Accepted, InitCond),
			conditionWithInitAndInc(v1.Accepted, TrueCond),
		}
	}
	return conditions
}

type ProvReqOption func(*queuedwrapper.ProvisioningRequest)

func WithMultiplePodSets(n int) ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		pr.PodTemplates = make([]*apiv1.PodTemplate, 0, n)
		pr.Spec.PodSets = make([]v1.PodSet, 0, n)

		for i := 0; i < n; i++ {
			pr.PodTemplates = append(pr.PodTemplates, examplePodTemplate(pr.Namespace, pr.Name, i))
			pr.Spec.PodSets = append(pr.Spec.PodSets, v1.PodSet{
				Count: 1,
				PodTemplateRef: v1.Reference{
					Name: pr.PodTemplates[i].Name,
				},
			})
		}
	}
}

func WithBulkMigProvisioningMode() ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		if pr.Status.ProvisioningClassDetails == nil {
			pr.Status.ProvisioningClassDetails = make(map[string]v1.Detail)
		}
		pr.Status.ProvisioningClassDetails[queuedwrapper.ProvisioningModeDetailKey] = v1.Detail(queuedwrapper.ProvisioningModeBulkMig)
	}
}

func WithResizeRequestProvisioningMode() ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		if pr.Status.ProvisioningClassDetails == nil {
			pr.Status.ProvisioningClassDetails = make(map[string]v1.Detail)
		}
		pr.Status.ProvisioningClassDetails[queuedwrapper.ProvisioningModeDetailKey] = v1.Detail(queuedwrapper.ProvisioningModeResizeRequest)
	}
}

func WithProvisioningClass(provisioningClass string) ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		pr.Spec.ProvisioningClassName = provisioningClass
	}
}

func WithCreationTime(creationTime time.Time) ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		pr.CreationTimestamp = metav1.NewTime(creationTime)
	}
}

func WithMaxRunDuration(maxRunDurationSeconds string) ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		if pr.Spec.Parameters == nil {
			pr.Spec.Parameters = map[string]v1.Parameter{}
		}
		pr.Spec.Parameters[queuedwrapper.MaxRunDurationSecondsKey] = v1.Parameter(maxRunDurationSeconds)
	}
}

func WithSelectedZone(selectedZone string) ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		pr.SetProvisioningClassDetails(&queuedwrapper.ProvisioningClassDetails{
			SelectedZone: selectedZone,
		})
	}
}

func WithDetails(details *queuedwrapper.ProvisioningClassDetails) ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		pr.SetProvisioningClassDetails(details)
	}
}

func WithObtainabilityStrategy() ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		if pr.Spec.Parameters == nil {
			pr.Spec.Parameters = map[string]v1.Parameter{}
		}
		pr.Spec.Parameters[queuedwrapper.CapacitySearchStrategyKey] = queuedwrapper.CapacitySearchStrategyObtainability
	}
}

type CondStatus int

const (
	InitCond CondStatus = iota
	TrueCond
	PostCondOk
	PostCondFail
)

type conditionFields struct {
	status             metav1.ConditionStatus
	reason, message    string
	observedGeneration int64
}

var (
	conditionsInStatus = map[string]map[CondStatus]conditionFields{
		v1.Accepted: {
			InitCond: {metav1.ConditionFalse, AcceptedInitReason, AcceptedInitMessage, 0},
			TrueCond: {metav1.ConditionTrue, acceptedReason, acceptedMessage, 1},
		},
		v1.Provisioned: {
			TrueCond: {metav1.ConditionTrue, provisionedReason, provisionedMessage, 2},
		},
		v1.BookingExpired: {
			TrueCond: {metav1.ConditionTrue, bookingExpiredReason, bookingExpiredMessage, 3},
		},
		v1.CapacityRevoked: {
			TrueCond: {metav1.ConditionTrue, capacityRevokedReason, capacityRevokedMessage, 4},
		},
		v1.Failed: {
			TrueCond: {metav1.ConditionTrue, defaultFailedReason, defaultFailedMessage, 1},
		},
	}
)

// conditionFromStatus returns a condition with appropriate fields.
// For initialized only conditions, `lastTransitionTime` will be `initTime` and
// the other conditions will have timestamps of form `initTime + N * timeInc`,
// where N is calculated from condition's assigned ObservedGeneration.
func conditionFromStatus(conditionType string, condStatus CondStatus, transitionTime time.Time, timeInc time.Duration) metav1.Condition {
	cond := conditionsInStatus[conditionType][condStatus]
	timeShift := time.Duration(cond.observedGeneration) * timeInc

	return condition(
		metav1.NewTime(transitionTime.Add(timeShift)),
		cond.message,
		cond.observedGeneration,
		cond.reason,
		cond.status,
		conditionType)
}

func condition(transitionTime metav1.Time, message string, observedGeneration int64, reason string, status metav1.ConditionStatus, conditionType string) metav1.Condition {
	return metav1.Condition{
		LastTransitionTime: transitionTime,
		Message:            message,
		ObservedGeneration: observedGeneration,
		Reason:             reason,
		Status:             status,
		Type:               conditionType,
	}
}

func examplePodTemplate(namespace, name string, n int) *apiv1.PodTemplate {
	return &apiv1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-pod-template-%d", name, n),
			Namespace: namespace,
		},
		Template: apiv1.PodTemplateSpec{
			Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{
						Name:  "test-container",
						Image: "test-image",
					},
				},
			},
		},
	}
}
