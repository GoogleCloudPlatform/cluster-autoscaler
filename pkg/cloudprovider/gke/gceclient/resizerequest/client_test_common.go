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

package resizerequestclient

import (
	"fmt"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

const (
	cancelTerminalResizeRequestErrorResponse = `{
	"error": {
	  "code": 412,
	  "message": "Cancelling resize request that has reached a final state is not possible.",
	  "errors": [
		{
		  "message": "Cancelling resize request that has reached a final state is not possible.",
		  "domain": "global",
		  "reason": "conditionNotMet",
		  "location": "If-Match",
		  "debugInfo": "java.lang.Exception\n\tat com.google.cloud.control.common.publicerrors.PublicErrorProtoUtils.newErrorBuilder(PublicErrorProtoUtils.java:2112)\n\tat com.google.cloud.control.common.publicerrors.PublicErrorProtoUtils.createConditionNotMetError(PublicErrorProtoUtils.java:1182)\n\tat com.google.cloud.cluster.manager.managedcompute.igm.utils.common.ResizeRequestValidator.throwIfCancelingResizeRequestIsNotPossible(ResizeRequestValidator.java:92)\n\tat com.google.cloud.cluster.manager.managedcompute.igm.zonal.services.InstanceGroupManagerResizeRequestsServiceCancelAction$CancelResizeRequestValidationHandler.runAttempt(InstanceGroupManagerResizeRequestsServiceCancelAction.java:142)\n\tat com.google.cloud.cluster.manager.managedcompute.igm.zonal.services.InstanceGroupManagerResizeRequestsServiceCancelAction$CancelResizeRequestValidationHandler.runAttempt(InstanceGroupManagerResizeRequestsServiceCancelAction.java:120)\n\tat com.google.cloud.cluster.metastore.RetryingMetastoreTransactionExecutor$1.runAttempt(RetryingMetastoreTransactionExecutor.java:94)\n\tat com.google.cloud.cluster.metastore.MetastoreRetryLoop.runHandler(MetastoreRetryLoop.java:523)\n\t...Stack trace is shortened.\n",
		  "locationType": "header"
		}
	  ]
	}
  }`
)

type errorMock struct {
	code     string
	location string
	message  string
}

type lastAttemptErrorMock struct {
	code         string
	message      string
	errorDetails *errorDetailsMock
}

type errorDetailsMock struct {
	reason    string
	metadatas map[string]string
}

var (
	projectID = "project1"
	migRef1   = gce.GceRef{
		Name:    "mig-name-1",
		Zone:    "us-central1-x",
		Project: projectID,
	}
	migRef2 = gce.GceRef{
		Name:    "mig-name-2",
		Zone:    "us-central1-z",
		Project: projectID,
	}
	err1 = fmt.Errorf("example error: fail")
	err2 = fmt.Errorf("another error: another fail")
)

func assertMapsEqual(t assert.TestingT, mapA interface{}, mapB interface{}) {
	// Maps are equal if they're subsets of each other
	assert.Subset(t, mapA, mapB)
	assert.Subset(t, mapB, mapA)
}
