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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
)

var (
	invalidReservationError = resizerequestclient.DwsStatusError{
		Code:    "INVALID_RESERVATION",
		Message: "Zone does not currently have sufficient capacity for the requested resources",
	}
	reservationNotFoundError = resizerequestclient.DwsStatusError{
		Code:    "RESERVATION_NOT_FOUND",
		Message: "Specified reservation abcd does not exist",
	}
	reservationNotReadyError = resizerequestclient.DwsStatusError{
		Code:    "RESERVATION_NOT_READY",
		Message: "Cannot use reservation, it requires reservation to be in READY state",
	}
	reservationCapacityExceededError = resizerequestclient.DwsStatusError{
		Code:    "RESERVATION_CAPACITY_EXCEEDED",
		Message: "Specified reservation xyz does not have available resources for the request.",
	}
	reservationIncompatibleError = resizerequestclient.DwsStatusError{
		Code:    "RESERVATION_INCOMPATIBLE",
		Message: "No available resources in specified reservations",
	}
	resizeRequestError = resizerequestclient.DwsStatusError{
		Code:    "LIMIT_EXCEEDED",
		Message: "Resize request could not provision queued instances in the allocated time.",
	}
	genericLimitExceededError = resizerequestclient.DwsStatusError{
		Code:    "LIMIT_EXCEEDED",
		Message: "Exceeded limit 'MAX_INSTANCES_IN_INSTANCE_GROUP' on resource 'us-central1-some-resource'. Limit: 2000.0",
	}
	quotaExceededError = resizerequestclient.DwsStatusError{
		Code:    "QUOTA_EXCEEDED",
		Message: "Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.",
	}
	quotaExceededError2 = resizerequestclient.DwsStatusError{
		Code:    "QUOTA_EXCEEDED",
		Message: "Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.",
	}
	resourcePoolExhaustedError = resizerequestclient.DwsStatusError{
		Code:    "RESOURCE_POOL_EXHAUSTED",
		Message: "The global or regional externalIP resource pool is exhausted.",
	}
	ipSpaceExhaustedError = resizerequestclient.DwsStatusError{
		Code:    "IP_SPACE_EXHAUSTED",
		Message: "Instance 'gke-instance-name-123456' creation failed: IP space of 'projects/project-id/regions/us-central1/subnetworks/subnet-name' is exhausted.",
	}
	permissionsError = resizerequestclient.DwsStatusError{
		Code:    "PERMISSIONS_ERROR",
		Message: "Instance 'gke-instance-name-123456' creation failed: Required 'compute.images.useReadOnly' permission for 'projects/project-id/global/images/image-name' (when acting as '1234567890@cloudservices.gserviceaccount.com').",
	}
	vmExternalIPAccessPolicyConstraintError = resizerequestclient.DwsStatusError{
		Code:    "CONDITION_NOT_MET",
		Message: "Instance 'gke-instance-name-123456' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.",
	}
	unrecognizedError = resizerequestclient.DwsStatusError{
		Code:    "SOME_ERROR",
		Message: "Some unrecognized error message.",
	}
	otherUnrecognizedError = resizerequestclient.DwsStatusError{
		Code:    "SOME_OTHER_ERROR",
		Message: "Some other unrecognized error message.",
	}
	zoneResourcePoolExhaustedError = resizerequestclient.DwsStatusError{
		Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
		Message: "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"a2-ultragpu-1g\", Attachment: \"local-ssd=1,nvidia-a100-80gb=1\".",
	}
	zoneResourcePoolExhaustedError2 = resizerequestclient.DwsStatusError{
		Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
		Message: "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"n1-standard-16\", Attachment: \"local-ssd=1,nvidia-tesla-p4=1\".",
	}
	zoneResourcePoolExhaustedError3 = resizerequestclient.DwsStatusError{
		Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
		Message: "Waiting for resources. Currently there are not enough resources available to fulfill the request. Expected time is indefinite.",
	}
)

func TestGetRRErrorInfoFromResizeRequestErrors(t *testing.T) {
	tests := []struct {
		name                      string
		errors                    []resizerequestclient.DwsStatusError
		expectedReason            string
		expectedMessage           string
		surfacedErrorsLimit       int
		expectedHasInstanceError  bool
		expectedInstanceErrorCode string
	}{
		{
			name: "Last error: limit exceeded - Resize Request specific error overwritten with Provisioning Request one",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError,
			},
			expectedReason:            "WaitTimeExceeded",
			expectedMessage:           "Provisioning Request could not provision queued instances in the allocated time.",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "WAIT_TIME_EXCEEDED",
		},
		{
			name: "Last error: limit exceeded - not related to Resize Request",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError,
				genericLimitExceededError,
			},
			expectedReason:            "LimitExceeded",
			expectedMessage:           "Exceeded limit 'MAX_INSTANCES_IN_INSTANCE_GROUP' on resource 'us-central1-some-resource'. Limit: 2000.0; The remaining received errors: [WAIT_TIME_EXCEEDED] \"Provisioning Request could not provision queued instances in the allocated time.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "LIMIT_EXCEEDED",
		},
		{
			name: "Last error: quota exceeded error",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError,
				genericLimitExceededError,
				quotaExceededError,
			},
			expectedReason:            "QuotaExceeded",
			expectedMessage:           "Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.; The remaining received errors: [WAIT_TIME_EXCEEDED] \"Provisioning Request could not provision queued instances in the allocated time.\"; [LIMIT_EXCEEDED] \"Exceeded limit 'MAX_INSTANCES_IN_INSTANCE_GROUP' on resource 'us-central1-some-resource'. Limit: 2000.0\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "QUOTA_EXCEEDED",
		},
		{
			name: "Last error: resource pool exhausted error",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError,
				genericLimitExceededError,
				quotaExceededError,
				resourcePoolExhaustedError,
			},
			expectedReason:            "ResourcePoolExhausted",
			expectedMessage:           "The global or regional externalIP resource pool is exhausted.; The remaining received errors: [WAIT_TIME_EXCEEDED] \"Provisioning Request could not provision queued instances in the allocated time.\"; [LIMIT_EXCEEDED] \"Exceeded limit 'MAX_INSTANCES_IN_INSTANCE_GROUP' on resource 'us-central1-some-resource'. Limit: 2000.0\"; [QUOTA_EXCEEDED] \"Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESOURCE_POOL_EXHAUSTED",
		},
		{
			name: "Last error: IP space exhausted error",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError,
				genericLimitExceededError,
				quotaExceededError,
				resourcePoolExhaustedError,
				ipSpaceExhaustedError,
			},
			expectedReason:            "IPSpaceExhausted",
			expectedMessage:           "Instance 'gke-instance-name-123456' creation failed: IP space of 'projects/project-id/regions/us-central1/subnetworks/subnet-name' is exhausted.; The remaining received errors: [WAIT_TIME_EXCEEDED] \"Provisioning Request could not provision queued instances in the allocated time.\"; [LIMIT_EXCEEDED] \"Exceeded limit 'MAX_INSTANCES_IN_INSTANCE_GROUP' on resource 'us-central1-some-resource'. Limit: 2000.0\"; [QUOTA_EXCEEDED] \"Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.\"; [RESOURCE_POOL_EXHAUSTED] \"The global or regional externalIP resource pool is exhausted.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "IP_SPACE_EXHAUSTED",
		},
		{
			name: "Last error: permissions error",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError, // not included in message (6th error)
				genericLimitExceededError,
				quotaExceededError,
				resourcePoolExhaustedError,
				ipSpaceExhaustedError,
				permissionsError,
			},
			expectedReason:            "PermissionsError",
			expectedMessage:           "Instance 'gke-instance-name-123456' creation failed: Required 'compute.images.useReadOnly' permission for 'projects/project-id/global/images/image-name' (when acting as '1234567890@cloudservices.gserviceaccount.com').; The remaining received errors: [LIMIT_EXCEEDED] \"Exceeded limit 'MAX_INSTANCES_IN_INSTANCE_GROUP' on resource 'us-central1-some-resource'. Limit: 2000.0\"; [QUOTA_EXCEEDED] \"Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.\"; [RESOURCE_POOL_EXHAUSTED] \"The global or regional externalIP resource pool is exhausted.\"; [IP_SPACE_EXHAUSTED] \"Instance 'gke-instance-name-123456' creation failed: IP space of 'projects/project-id/regions/us-central1/subnetworks/subnet-name' is exhausted.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "PERMISSIONS_ERROR",
		},
		{
			name: "Last error: VM external IP access policy constraint error",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError,        // not included in message
				genericLimitExceededError, // not included in message
				quotaExceededError,
				resourcePoolExhaustedError,
				ipSpaceExhaustedError,
				permissionsError,
				vmExternalIPAccessPolicyConstraintError,
			},
			expectedReason:            "VMExternalIPAccessPolicyConstraint",
			expectedMessage:           "Instance 'gke-instance-name-123456' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.; The remaining received errors: [QUOTA_EXCEEDED] \"Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.\"; [RESOURCE_POOL_EXHAUSTED] \"The global or regional externalIP resource pool is exhausted.\"; [IP_SPACE_EXHAUSTED] \"Instance 'gke-instance-name-123456' creation failed: IP space of 'projects/project-id/regions/us-central1/subnetworks/subnet-name' is exhausted.\"; [PERMISSIONS_ERROR] \"Instance 'gke-instance-name-123456' creation failed: Required 'compute.images.useReadOnly' permission for 'projects/project-id/global/images/image-name' (when acting as '1234567890@cloudservices.gserviceaccount.com').\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "VM_EXTERNAL_IP_ACCESS_POLICY_CONSTRAINT",
		},
		{
			name: "Last error: some unrecognized error - we keep the last recognized one",
			errors: []resizerequestclient.DwsStatusError{
				resizeRequestError,        // not included in message
				genericLimitExceededError, // not included in message
				quotaExceededError,        // not included in message
				resourcePoolExhaustedError,
				ipSpaceExhaustedError,
				permissionsError,
				vmExternalIPAccessPolicyConstraintError, // main error
				unrecognizedError,
			},
			expectedReason:            "VMExternalIPAccessPolicyConstraint",
			expectedMessage:           "Instance 'gke-instance-name-123456' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.; The remaining received errors: [RESOURCE_POOL_EXHAUSTED] \"The global or regional externalIP resource pool is exhausted.\"; [IP_SPACE_EXHAUSTED] \"Instance 'gke-instance-name-123456' creation failed: IP space of 'projects/project-id/regions/us-central1/subnetworks/subnet-name' is exhausted.\"; [PERMISSIONS_ERROR] \"Instance 'gke-instance-name-123456' creation failed: Required 'compute.images.useReadOnly' permission for 'projects/project-id/global/images/image-name' (when acting as '1234567890@cloudservices.gserviceaccount.com').\"; [SOME_ERROR] \"Some unrecognized error message.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "VM_EXTERNAL_IP_ACCESS_POLICY_CONSTRAINT",
		},
		{
			name: "Last error: some unrecognized error as only error - setting reason to `InternalError` and copying error code and message",
			errors: []resizerequestclient.DwsStatusError{
				unrecognizedError,
			},
			expectedReason:            "InternalError",
			expectedMessage:           "Received unrecognized error: [SOME_ERROR] \"Some unrecognized error message.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "OTHER",
		},
		{
			name: "Multiple unrecognized errors only - keeping the last 5 ones",
			errors: []resizerequestclient.DwsStatusError{
				unrecognizedError,
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
			},
			expectedReason:            "InternalError",
			expectedMessage:           "Received unrecognized errors: [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"; [SOME_ERROR] \"Some unrecognized error message.\"; [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"; [SOME_ERROR] \"Some unrecognized error message.\"; [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "OTHER",
		},
		{
			name: "Multiple unrecognized errors only - keeping the last X ones with explicit surfacedErrorsLimit=X",
			errors: []resizerequestclient.DwsStatusError{
				unrecognizedError,
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
			},
			expectedReason:            "InternalError",
			expectedMessage:           "Received unrecognized errors: [SOME_ERROR] \"Some unrecognized error message.\"; [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"",
			surfacedErrorsLimit:       2,
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "OTHER",
		},
		{
			name: "Multiple errors - picking last recognized error",
			errors: []resizerequestclient.DwsStatusError{
				unrecognizedError,         // not included in message
				resizeRequestError,        // not included in message
				genericLimitExceededError, // not included in message
				ipSpaceExhaustedError,     // not included in message
				otherUnrecognizedError,    // not included in message
				quotaExceededError,        // not included in message
				vmExternalIPAccessPolicyConstraintError,
				otherUnrecognizedError,
				permissionsError,
				resourcePoolExhaustedError, // main error
				unrecognizedError,
			},
			expectedReason:            "ResourcePoolExhausted",
			expectedMessage:           "The global or regional externalIP resource pool is exhausted.; The remaining received errors: [VM_EXTERNAL_IP_ACCESS_POLICY_CONSTRAINT] \"Instance 'gke-instance-name-123456' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.\"; [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"; [PERMISSIONS_ERROR] \"Instance 'gke-instance-name-123456' creation failed: Required 'compute.images.useReadOnly' permission for 'projects/project-id/global/images/image-name' (when acting as '1234567890@cloudservices.gserviceaccount.com').\"; [SOME_ERROR] \"Some unrecognized error message.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESOURCE_POOL_EXHAUSTED",
		},
		{
			name: "Multiple errors - picking last recognized error with explicit surfacedErrorsLimit",
			errors: []resizerequestclient.DwsStatusError{
				unrecognizedError,                       // not included in message
				resizeRequestError,                      // not included in message
				genericLimitExceededError,               // not included in message
				ipSpaceExhaustedError,                   // not included in message
				otherUnrecognizedError,                  // not included in message
				quotaExceededError,                      // not included in message
				vmExternalIPAccessPolicyConstraintError, // not included in message
				otherUnrecognizedError,                  // not included in message
				permissionsError,
				resourcePoolExhaustedError, // main error
				unrecognizedError,
			},
			expectedReason:            "ResourcePoolExhausted",
			expectedMessage:           "The global or regional externalIP resource pool is exhausted.; The remaining received errors: [PERMISSIONS_ERROR] \"Instance 'gke-instance-name-123456' creation failed: Required 'compute.images.useReadOnly' permission for 'projects/project-id/global/images/image-name' (when acting as '1234567890@cloudservices.gserviceaccount.com').\"; [SOME_ERROR] \"Some unrecognized error message.\"",
			surfacedErrorsLimit:       3,
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESOURCE_POOL_EXHAUSTED",
		},
		{
			name: "Multiple recognized errors with the same error code, but different messages - picking last recognized error",
			errors: []resizerequestclient.DwsStatusError{
				quotaExceededError,  // not included in message
				quotaExceededError2, // filtered out first occurrence instead of the last (because it's identical to main, doesn't matter)
				quotaExceededError,
				quotaExceededError2,
				quotaExceededError,
				quotaExceededError2, // main error
			},
			expectedReason:            "QuotaExceeded",
			expectedMessage:           "Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.; The remaining received errors: [QUOTA_EXCEEDED] \"Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.\"; [QUOTA_EXCEEDED] \"Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.\"; [QUOTA_EXCEEDED] \"Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.\"; [QUOTA_EXCEEDED] \"Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "QUOTA_EXCEEDED",
		},
		{
			name: "Multiple unrecognized errors and one old recognized one - recognized one is the main one, surfacing 6 errors total",
			errors: []resizerequestclient.DwsStatusError{
				quotaExceededError,     // main error
				unrecognizedError,      // not included in message
				otherUnrecognizedError, // not included in message
				unrecognizedError,      // not included in message
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
				unrecognizedError,
				otherUnrecognizedError,
			},
			expectedReason:            "QuotaExceeded",
			expectedMessage:           "Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.; The remaining received errors: [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"; [SOME_ERROR] \"Some unrecognized error message.\"; [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"; [SOME_ERROR] \"Some unrecognized error message.\"; [SOME_OTHER_ERROR] \"Some other unrecognized error message.\"",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "QUOTA_EXCEEDED",
		},
		{
			name:            "Resize Request Failed, but no errors were provided",
			errors:          []resizerequestclient.DwsStatusError{},
			expectedReason:  "InternalErrorResizeRequestFailed",
			expectedMessage: "Provisioning Request failed, but no errors with details can be provided.",
		},
		{
			name:                      "Invalid reservation",
			errors:                    []resizerequestclient.DwsStatusError{invalidReservationError},
			expectedReason:            "InvalidReservation",
			expectedMessage:           "Zone does not currently have sufficient capacity for the requested resources",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "INVALID_RESERVATION",
		},
		{
			name:                      "Reservation not found",
			errors:                    []resizerequestclient.DwsStatusError{reservationNotFoundError},
			expectedReason:            "ReservationNotFound",
			expectedMessage:           "Specified reservation abcd does not exist",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_NOT_FOUND",
		},
		{
			name:                      "Reservation not ready",
			errors:                    []resizerequestclient.DwsStatusError{reservationNotReadyError},
			expectedReason:            "ReservationNotReady",
			expectedMessage:           "Cannot use reservation, it requires reservation to be in READY state",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_NOT_READY",
		},
		{
			name:                      "Reservation capacity exceeded",
			errors:                    []resizerequestclient.DwsStatusError{reservationCapacityExceededError},
			expectedReason:            "ReservationCapacityExceeded",
			expectedMessage:           "Specified reservation xyz does not have available resources for the request.",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_CAPACITY_EXCEEDED",
		},
		{
			name:                      "Reservation incompatible",
			errors:                    []resizerequestclient.DwsStatusError{reservationIncompatibleError},
			expectedReason:            "ReservationIncompatible",
			expectedMessage:           "No available resources in specified reservations",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_INCOMPATIBLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.surfacedErrorsLimit == 0 {
				tt.surfacedErrorsLimit = DefaultSurfacedErrorsLimit
			}
			rrErrorInfo := GetDwsErrorInfoFromResizeRequestErrors("mig/zone/example-resize-request-name", tt.errors, tt.surfacedErrorsLimit)
			if diff := cmp.Diff(tt.expectedReason, rrErrorInfo.Reason); diff != "" {
				t.Errorf("Wrong reason from GetRRErrorInfoFromResizeRequestErrors in %q diff (-want +got):\n%s", tt.name, diff)
			}
			if diff := cmp.Diff(tt.expectedMessage, rrErrorInfo.Message); diff != "" {
				t.Errorf("Wrong message from GetRRErrorInfoFromResizeRequestErrors in %q diff (-want +got):\n%s", tt.name, diff)
			}
			if tt.expectedHasInstanceError {
				if diff := cmp.Diff(tt.expectedInstanceErrorCode, rrErrorInfo.InstanceError.ErrorCode); diff != "" {
					t.Errorf("Wrong instance error from GetRRErrorInfoFromResizeRequestErrors in %q diff (-want +got):\n%s", tt.name, diff)
				}
			} else {
				assert.Nil(t, rrErrorInfo.InstanceError)
			}
		})
	}
}

func TestGetRRErrorInfoFromLastAttemptErrors(t *testing.T) {
	tests := []struct {
		name                      string
		errors                    []resizerequestclient.DwsStatusError
		expectedReason            string
		expectedMessage           string
		expectedHasInstanceError  bool
		expectedInstanceErrorCode string
	}{
		{
			name:            "No errors provided",
			errors:          []resizerequestclient.DwsStatusError{},
			expectedReason:  "NotProvisioned",
			expectedMessage: "Provisioning Request wasn't provisioned.",
		},
		{
			name: "Quota Exceeded error",
			errors: []resizerequestclient.DwsStatusError{
				quotaExceededError,
			},
			expectedReason:            "QuotaExceeded",
			expectedMessage:           "Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "QUOTA_EXCEEDED",
		},
		{
			name: "Zone Resource Pool Exhausted error",
			errors: []resizerequestclient.DwsStatusError{
				zoneResourcePoolExhaustedError,
			},
			expectedReason:            "ResourcePoolExhausted",
			expectedMessage:           "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"a2-ultragpu-1g\", Attachment: \"local-ssd=1,nvidia-a100-80gb=1\".",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESOURCE_POOL_EXHAUSTED",
		},
		{
			name: "Multiple Quota Exceeded error, first one is picked",
			errors: []resizerequestclient.DwsStatusError{
				quotaExceededError,
				quotaExceededError2,
			},
			expectedReason:            "QuotaExceeded",
			expectedMessage:           "Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "QUOTA_EXCEEDED",
		},
		{
			name: "Quota Exceeded error with other errors",
			errors: []resizerequestclient.DwsStatusError{
				zoneResourcePoolExhaustedError,
				quotaExceededError,
				zoneResourcePoolExhaustedError,
				zoneResourcePoolExhaustedError,
			},
			expectedReason:            "QuotaExceeded",
			expectedMessage:           "Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "QUOTA_EXCEEDED",
		},
		{
			name: "Multiple Zone Resource Pool Exhausted errors, first one is picked",
			errors: []resizerequestclient.DwsStatusError{
				zoneResourcePoolExhaustedError,
				zoneResourcePoolExhaustedError2,
			},
			expectedReason:            "ResourcePoolExhausted",
			expectedMessage:           "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"a2-ultragpu-1g\", Attachment: \"local-ssd=1,nvidia-a100-80gb=1\".",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESOURCE_POOL_EXHAUSTED",
		},
		{
			name: "Zone Resource Pool Exhausted error with ETA",
			errors: []resizerequestclient.DwsStatusError{
				zoneResourcePoolExhaustedError3,
			},
			expectedReason:            "ResourcePoolExhausted",
			expectedMessage:           "Waiting for resources. Currently there are not enough resources available to fulfill the request. Expected time is indefinite.",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESOURCE_POOL_EXHAUSTED",
		},
		{
			name:                      "Invalid reservation",
			errors:                    []resizerequestclient.DwsStatusError{invalidReservationError},
			expectedReason:            "InvalidReservation",
			expectedMessage:           "Zone does not currently have sufficient capacity for the requested resources",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "INVALID_RESERVATION",
		},
		{
			name:                      "Reservation not found",
			errors:                    []resizerequestclient.DwsStatusError{reservationNotFoundError},
			expectedReason:            "ReservationNotFound",
			expectedMessage:           "Specified reservation abcd does not exist",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_NOT_FOUND",
		},
		{
			name:                      "Reservation not ready",
			errors:                    []resizerequestclient.DwsStatusError{reservationNotReadyError},
			expectedReason:            "ReservationNotReady",
			expectedMessage:           "Cannot use reservation, it requires reservation to be in READY state",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_NOT_READY",
		},
		{
			name:                      "Reservation capacity exceeded",
			errors:                    []resizerequestclient.DwsStatusError{reservationCapacityExceededError},
			expectedReason:            "ReservationCapacityExceeded",
			expectedMessage:           "Specified reservation xyz does not have available resources for the request.",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_CAPACITY_EXCEEDED",
		},
		{
			name:                      "Reservation incompatible",
			errors:                    []resizerequestclient.DwsStatusError{reservationIncompatibleError},
			expectedReason:            "ReservationIncompatible",
			expectedMessage:           "No available resources in specified reservations",
			expectedHasInstanceError:  true,
			expectedInstanceErrorCode: "RESERVATION_INCOMPATIBLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rrErrorInfo := GetDwsErrorInfoFromLastAttemptErrors("mig/zone/example-resize-request-name", tt.errors)
			if diff := cmp.Diff(tt.expectedReason, rrErrorInfo.Reason); diff != "" {
				t.Errorf("Wrong reason from TestGetRRErrorInfoFromLastAttemptErrors in %q diff (-want +got):\n%s", tt.name, diff)
			}
			if diff := cmp.Diff(tt.expectedMessage, rrErrorInfo.Message); diff != "" {
				t.Errorf("Wrong message from TestGetRRErrorInfoFromLastAttemptErrors in %q diff (-want +got):\n%s", tt.name, diff)
			}
			if tt.expectedHasInstanceError {
				if diff := cmp.Diff(tt.expectedInstanceErrorCode, rrErrorInfo.InstanceError.ErrorCode); diff != "" {
					t.Errorf("Wrong instance error from TestGetRRErrorInfoFromLastAttemptErrors in %q diff (-want +got):\n%s", tt.name, diff)
				}
			} else {
				assert.Nil(t, rrErrorInfo.InstanceError)
			}
		})
	}
}

func TestGetRRErrorInfoFromResizeRequestOperationError(t *testing.T) {
	tests := []struct {
		name                  string
		err                   error
		wantReason            string
		wantMessage           string
		wantBackoff           bool
		wantInstanceError     bool
		wantInstanceErrorCode string
	}{
		{
			name: "invalid argument: too small max run duration",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "INVALID_ARGUMENT", Message: "Specified maxRunDuration is too short. Minimum value is equal to 10 minutes."}}},
			wantReason:  "InvalidArgument",
			wantMessage: "Specified maxRunDuration is too short. Minimum value is equal to 10 minutes.",
		},
		{
			name:        "some error not from OperationError (so not a ResizeRequestOperationError)",
			err:         errors.New("some error"),
			wantReason:  "InternalErrorFailedToQueue",
			wantMessage: "Provisioning Request failed to queue in nodepool \"example-nodepool\" in zone us-central1-c, got error: some error",
			wantBackoff: true,
		},
		{
			name: "some unrecognized ResizeRequestOperationError error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "SOME_CODE", Message: "some error"}}},
			wantReason:            "InternalErrorFailedToQueue",
			wantMessage:           "Provisioning Request failed to queue in nodepool \"example-nodepool\" in zone us-central1-c, got error: [SOME_CODE] \"some error\"",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "queuingInfeasibleNoCapacity error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "QUEUING_INFEASIBLE_NO_CAPACITY", Message: "Could not create a queued resource due to insufficient capacity. Try different locations, different hardware, or a longer wait time.\""}}},
			wantReason:            "QueuingInfeasibleNoCapacity",
			wantMessage:           "Could not enqueue the Provisioning Request due to insufficient capacity in zone us-central1-c. Try different locations or hardware.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "invalidMachineType error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "INVALID_MACHINE_TYPE", Message: "Specified machine type is invalid."}}},
			wantReason:            "InvalidMachineType",
			wantMessage:           "Specified machine type is invalid.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "invalidGpuType error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "INVALID_GPU_TYPE", Message: "Specified accelerator type is invalid."}}},
			wantReason:            "InvalidGPUType",
			wantMessage:           "Specified accelerator type is invalid.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "aggregateReservationNotExist error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "AGGREGATE_RESERVATION_NOT_EXIST", Message: "Queued Resource Manager is not supported in this zone. Try different locations."}}},
			wantReason:            "ZoneNotSupported",
			wantMessage:           "Provisioning Request is not supported in this zone. Try different locations.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "permissionDenied error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "PERMISSION_DENIED", Message: "This feature is not supported."}}},
			wantReason:            "PermissionDenied",
			wantMessage:           "This feature is not supported.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "conditionNotMet error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "CONDITION_NOT_MET", Message: "Constraint constraints/compute.requireShieldedVm violated for project projects/some-project. Secure Boot is not enabled in the 'shielded_instance_config' field. See https://cloud.google.com/resource-manager/docs/organization-policy/org-policy-constraints for more information."}}},
			wantReason:            "ConditionNotMet",
			wantMessage:           "Constraint constraints/compute.requireShieldedVm violated for project projects/some-project. Secure Boot is not enabled in the 'shielded_instance_config' field. See https://cloud.google.com/resource-manager/docs/organization-policy/org-policy-constraints for more information.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "unsupported operation error",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "UNSUPPORTED_OPERATION", Message: "Requests without accelerators are not supported."}}},
			wantReason:  "UnsupportedOperation",
			wantMessage: "Requests without accelerators are not supported.",
		},
		{
			name: "multiple ResizeRequestOperationError errors - pick the last one",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "INVALID_GPU_TYPE", Message: "Specified accelerator type is invalid."},
				{Code: "INVALID_MACHINE_TYPE", Message: "Specified machine type is invalid."},
			}},
			wantReason:            "InvalidMachineType",
			wantMessage:           "Specified machine type is invalid.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "wrapped ResizeRequestOperationMultiError - pick the last one",
			err: fmt.Errorf("while ResizeRequest.Operation.Get got error: %w",
				&resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
					{Code: "INVALID_GPU_TYPE", Message: "Specified accelerator type is invalid."},
					{Code: "INVALID_MACHINE_TYPE", Message: "Specified machine type is invalid."},
				}}),
			wantReason:            "InvalidMachineType",
			wantMessage:           "Specified machine type is invalid.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "OTHER",
		},
		{
			name: "Quota active-resize-requests exceeded",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "QUOTA_EXCEEDED", Message: "Quota 'ACTIVE_RESIZE_REQUESTS' exceeded.  Limit: 100.0 in region us-central1."}}},
			wantReason:            "QuotaExceeded",
			wantMessage:           "Quota 'ACTIVE_RESIZE_REQUESTS' exceeded.  Limit: 100.0 in region us-central1.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "QUOTA_EXCEEDED",
		},
		{
			name: "fragmented flex start scale up resize request warning",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "FRAGMENTED_FLEX_START_SCALE_UP", Message: "Scale-up Limitation: Only 50 of 200 expected processed due to flex start non-queued preview scalability. Overflow will be handled in a later scale-up."}}},
			wantReason:  "FragmentedFlexStartScaleUp",
			wantMessage: "Scale-up Limitation: Only 50 of 200 expected processed due to flex start non-queued preview scalability. Overflow will be handled in a later scale-up.",
			wantBackoff: false,
		},
		{
			name: "invalid reservation",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "INVALID_RESERVATION", Message: "Zone does not currently have sufficient capacity for the requested resources"}}},
			wantReason:            "InvalidReservation",
			wantMessage:           "Zone does not currently have sufficient capacity for the requested resources",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "INVALID_RESERVATION",
		},
		{
			name: "reservation not found",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "RESERVATION_NOT_FOUND", Message: "Specified reservation abcd does not exist"}}},
			wantReason:            "ReservationNotFound",
			wantMessage:           "Specified reservation abcd does not exist",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "RESERVATION_NOT_FOUND",
		},
		{
			name: "reservation not ready",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "RESERVATION_NOT_READY", Message: "Cannot use reservation, it requires reservation to be in READY state"}}},
			wantReason:            "ReservationNotReady",
			wantMessage:           "Cannot use reservation, it requires reservation to be in READY state",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "RESERVATION_NOT_READY",
		},
		{
			name: "reservation capacity exceeded",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "RESERVATION_CAPACITY_EXCEEDED", Message: "Specified reservation xyz does not have available resources for the request."}}},
			wantReason:            "ReservationCapacityExceeded",
			wantMessage:           "Specified reservation xyz does not have available resources for the request.",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "RESERVATION_CAPACITY_EXCEEDED",
		},
		{
			name: "reservation incompatible",
			err: &resizerequestclient.ResizeRequestOperationMultiError{Errors: []resizerequestclient.ResizeRequestOperationError{
				{Code: "RESERVATION_INCOMPATIBLE", Message: "No available resources in specified reservations"}}},
			wantReason:            "ReservationIncompatible",
			wantMessage:           "No available resources in specified reservations",
			wantBackoff:           true,
			wantInstanceError:     true,
			wantInstanceErrorCode: "RESERVATION_INCOMPATIBLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rrErrorInfo, shouldBackoff := GetDwsErrorInfoFromResizeRequestOperationError(tt.err, "example-nodepool", "us-central1-c")
			if rrErrorInfo.Reason != tt.wantReason {
				t.Errorf("GetRRErrorInfoFromResizeRequestOperationError() gotReason = %q, wantReason %q", rrErrorInfo.Reason, tt.wantReason)
			}
			if rrErrorInfo.Message != tt.wantMessage {
				t.Errorf("GetReasonAndMessageFromResizeRequestOperationError() gotMessage = %q, wantMessage %q", rrErrorInfo.Message, tt.wantMessage)
			}
			if shouldBackoff != tt.wantBackoff {
				t.Errorf("GetReasonAndMessageFromResizeRequestOperationError() shouldBackoff = %v, wantBackoff %v", shouldBackoff, tt.wantBackoff)
			}
			if tt.wantInstanceError {
				assert.Equal(t, tt.wantInstanceErrorCode, rrErrorInfo.InstanceError.ErrorCode)
			} else {
				assert.Nil(t, rrErrorInfo.InstanceError)
			}
		})
	}
}

func TestResizeRequestCategoryReasonMessage(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	capacityCheckWaitTime := 5 * time.Minute

	tests := []struct {
		name         string
		rr           resizerequestclient.ResizeRequestStatus
		wantCategory ResizeRequestCategory
		wantReason   string
		wantMessage  string
		wantNilInfo  bool
	}{
		{
			name: "recent_Accepted",
			rr: resizerequestclient.ResizeRequestStatus{
				State:        resizerequestclient.ResizeRequestStateAccepted,
				CreationTime: now.Add(-1 * time.Minute),
			},
			wantCategory: QueueingCategory,
			wantNilInfo:  true,
		},
		{
			name: "old_Accepted_TimeoutCategory_with_LastAttemptErrors",
			rr: resizerequestclient.ResizeRequestStatus{
				State:             resizerequestclient.ResizeRequestStateAccepted,
				CreationTime:      now.Add(-10 * time.Minute),
				LastAttemptErrors: []resizerequestclient.DwsStatusError{quotaExceededError},
			},
			wantCategory: TimeoutCategory,
			wantReason:   "QuotaExceeded",
			wantMessage:  "Quota 'NVIDIA_A100_GPUS' exceeded.  Limit: 16.0 in region us-central1.",
		},
		{
			name: "old_Accepted_TimeoutCategory_no_LastAttemptErrors",
			rr: resizerequestclient.ResizeRequestStatus{
				State:        resizerequestclient.ResizeRequestStateAccepted,
				CreationTime: now.Add(-10 * time.Minute),
			},
			wantCategory: TimeoutCategory,
			wantReason:   "AcceptedRequestTimedOut",
			wantMessage:  "Request wasn't provisioned in the allocated time, no errors were provided.",
		},
		{
			name: "cancelled_with_last_attempt_errors",
			rr: resizerequestclient.ResizeRequestStatus{
				State:             resizerequestclient.ResizeRequestStateCancelled,
				LastAttemptErrors: []resizerequestclient.DwsStatusError{zoneResourcePoolExhaustedError},
			},
			wantCategory: FailedCategory,
			wantReason:   "ResourcePoolExhausted",
			wantMessage:  "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"a2-ultragpu-1g\", Attachment: \"local-ssd=1,nvidia-a100-80gb=1\".",
		},
		{
			name: "cancelled_no_last_attempt_errors",
			rr: resizerequestclient.ResizeRequestStatus{
				State: resizerequestclient.ResizeRequestStateCancelled,
			},
			wantCategory: FailedCategory,
			wantReason:   "RequestWasCancelled",
			wantMessage:  "Request was unexpectedly cancelled, no errors were provided.",
		},
		{
			name: "failed",
			rr: resizerequestclient.ResizeRequestStatus{
				State:  resizerequestclient.ResizeRequestStateFailed,
				Errors: []resizerequestclient.DwsStatusError{genericLimitExceededError},
			},
			wantCategory: FailedCategory,
			wantReason:   "LimitExceeded",
			wantMessage:  "Exceeded limit 'MAX_INSTANCES_IN_INSTANCE_GROUP' on resource 'us-central1-some-resource'. Limit: 2000.0",
		},
		{
			name: "succeeded",
			rr: resizerequestclient.ResizeRequestStatus{
				State: resizerequestclient.ResizeRequestStateSucceeded,
			},
			wantCategory: SuccessfulCategory,
			// TODO(b/480847645): why does SuccessfulCategory have reason and message?
			// I think it doesn't have to have them, but I'll remove this in separate commit.
			wantReason:  "InternalErrorNoReason",
			wantMessage: "Request wasn't provisioned, no errors were provided.",
		},
		{
			name: "unexpected_state_deleting",
			rr: resizerequestclient.ResizeRequestStatus{
				State: resizerequestclient.ResizeRequestStateDeleting,
			},
			wantCategory: UnexpectedCategory,
			wantNilInfo:  true,
		},
		{
			name: "unexpected_state_creating",
			rr: resizerequestclient.ResizeRequestStatus{
				State: resizerequestclient.ResizeRequestStateCreating,
			},
			wantCategory: UnexpectedCategory,
			wantNilInfo:  true,
		},
		{
			name: "unexpected_state_provisioning",
			rr: resizerequestclient.ResizeRequestStatus{
				State: resizerequestclient.ResizeRequestStateProvisioning,
			},
			wantCategory: UnexpectedCategory,
			wantNilInfo:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			category, info := ResizeRequestCategoryReasonMessage(tt.rr, capacityCheckWaitTime, now)

			assert.Equal(t, tt.wantCategory, category)

			if tt.wantReason == "" && tt.wantMessage == "" {
				assert.Nil(t, info)
				return
			}
			assert.NotNil(t, info)
			assert.Equal(t, tt.wantReason, info.Reason)
			assert.Equal(t, tt.wantMessage, info.Message)
		})
	}
}

func TestGroupResizeRequestErrors(t *testing.T) {
	now := time.Now()
	capacityCheckWaitTime := 5 * time.Minute

	tests := []struct {
		name               string
		failedRRs          []resizerequestclient.ResizeRequestStatus
		wantReasonMessages map[ErrorReasonMessage]int
		wantMainError      DwsErrorInfo
	}{
		{
			name: "single_failed_RR",
			failedRRs: []resizerequestclient.ResizeRequestStatus{
				{
					Name:     "rr-1",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{quotaExceededError},
					ResizeBy: 10,
				},
			},
			wantReasonMessages: map[ErrorReasonMessage]int{
				{Reason: "QuotaExceeded", Message: quotaExceededError.Message}: 10,
			},
			wantMainError: DwsErrorInfo{
				Reason:  "QuotaExceeded",
				Message: quotaExceededError.Message,
			},
		},
		{
			name: "multiple_failed_RRs_with_same_error",
			failedRRs: []resizerequestclient.ResizeRequestStatus{
				{
					Name:     "rr-1",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{quotaExceededError},
					ResizeBy: 10,
				},
				{
					Name:     "rr-2",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{quotaExceededError},
					ResizeBy: 5,
				},
			},
			wantReasonMessages: map[ErrorReasonMessage]int{
				{Reason: "QuotaExceeded", Message: quotaExceededError.Message}: 15,
			},
			wantMainError: DwsErrorInfo{
				Reason:  "QuotaExceeded",
				Message: quotaExceededError.Message,
			},
		},
		{
			name: "multiple_failed_RRs_different_errors_same_reason",
			failedRRs: []resizerequestclient.ResizeRequestStatus{
				{
					Name:     "rr-1",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{quotaExceededError},
					ResizeBy: 10,
				},
				{
					Name:     "rr-2",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{quotaExceededError2},
					ResizeBy: 5,
				},
			},
			wantReasonMessages: map[ErrorReasonMessage]int{
				{
					Reason:  "QuotaExceeded",
					Message: MultipleErrorsMessage([]string{quotaExceededError.Message, quotaExceededError2.Message}),
				}: 15,
			},
			wantMainError: DwsErrorInfo{
				Reason:  "QuotaExceeded",
				Message: quotaExceededError.Message,
			},
		},
		{
			name: "multiple_failed_RRs_different_reasons",
			failedRRs: []resizerequestclient.ResizeRequestStatus{
				{
					Name:     "rr-1",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{quotaExceededError},
					ResizeBy: 10,
				},
				{
					Name:     "rr-2",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{reservationNotFoundError},
					ResizeBy: 5,
				},
			},
			wantReasonMessages: map[ErrorReasonMessage]int{
				{Reason: "QuotaExceeded", Message: quotaExceededError.Message}:             10,
				{Reason: "ReservationNotFound", Message: reservationNotFoundError.Message}: 5,
			},
			wantMainError: DwsErrorInfo{
				Reason:  "QuotaExceeded",
				Message: quotaExceededError.Message,
			},
		},
		{
			name: "main_error_selection_skip_defaulted_reasons",
			failedRRs: []resizerequestclient.ResizeRequestStatus{
				{
					Name:     "rr-1",
					State:    resizerequestclient.ResizeRequestStateCancelled,
					ResizeBy: 10,
				},
				{
					Name:     "rr-2",
					State:    resizerequestclient.ResizeRequestStateFailed,
					Errors:   []resizerequestclient.DwsStatusError{quotaExceededError},
					ResizeBy: 5,
				},
			},
			wantReasonMessages: map[ErrorReasonMessage]int{
				{Reason: "RequestWasCancelled", Message: "Request was unexpectedly cancelled, no errors were provided."}: 10,
				{Reason: "QuotaExceeded", Message: quotaExceededError.Message}:                                           5,
			},
			wantMainError: DwsErrorInfo{
				Reason:  "QuotaExceeded",
				Message: quotaExceededError.Message,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReasonMessages, gotMainError := GroupResizeRequestErrors(tt.failedRRs, capacityCheckWaitTime, now)

			// Normalize messages to handle random ordering of grouped messages
			normalize := func(m map[ErrorReasonMessage]int) map[ErrorReasonMessage]int {
				res := map[ErrorReasonMessage]int{}
				for k, v := range m {
					msg := k.Message
					if strings.Contains(msg, groupedMsgsPrefix) {
						msg = SortGroupedEventsMessages([]string{msg})[0]
					}
					res[ErrorReasonMessage{Reason: k.Reason, Message: msg}] = v
				}
				return res
			}
			assert.Equal(t, normalize(tt.wantReasonMessages), normalize(gotReasonMessages))

			assert.Equal(t, tt.wantMainError.Reason, gotMainError.Reason)
			assert.Equal(t, tt.wantMainError.Message, gotMainError.Message)
		})
	}
}
