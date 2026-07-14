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

package backoff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestIsStockout(t *testing.T) {

	testCases := []struct {
		name           string
		errorClass     cloudprovider.InstanceErrorClass
		errorCode      string
		errorMessage   string
		isFlex         bool
		isMultiHostTPU bool
		tpuType        string
		expected       bool
		deploymentType gke.DeploymentTypeEnum
	}{
		{
			name:       "OutOfResources without reservation is always a stockout",
			errorClass: cloudprovider.OutOfResourcesErrorClass,
			expected:   true,
		},
		{
			name:           "OutOfResources with reservation not a stockout",
			deploymentType: gke.DeploymentTypeDense,
			errorClass:     cloudprovider.OutOfResourcesErrorClass,
			expected:       false,
		},
		{
			name:           "OutOfResources QUOTA error with reservation is always stockout",
			deploymentType: gke.DeploymentTypeDense,
			errorClass:     cloudprovider.OutOfResourcesErrorClass,
			errorCode:      gce.ErrorCodeQuotaExceeded,
			expected:       true,
		},
		{
			name:       "OtherErrors are typically not a stockout",
			errorClass: cloudprovider.OtherErrorClass,
			expected:   false,
		},
		{
			name:       "Invalid reservation should be treated as a stockout if there is no reservation",
			errorClass: cloudprovider.OtherErrorClass,
			errorCode:  gce.ErrorInvalidReservation,
			expected:   true,
		},
		{
			name:           "Invalid reservation should not be treated as a stockout if there is a reservation",
			deploymentType: gke.DeploymentTypeDense,
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      gce.ErrorInvalidReservation,
			expected:       false,
		},
		{
			name:       "Flex Start can report ZRPE with OtherErrorClass",
			errorClass: cloudprovider.OtherErrorClass,
			errorCode:  gce.ErrorCodeResourcePoolExhausted,
			isFlex:     true,
			expected:   true,
		},
		{
			name:       "Flex Start can report quota with OtherErrorClass",
			errorClass: cloudprovider.OtherErrorClass,
			errorCode:  gce.ErrorCodeQuotaExceeded,
			isFlex:     true,
			expected:   true,
		},
		{
			name:       "Other FlexStart errors do not represent a stockout",
			errorClass: cloudprovider.OtherErrorClass,
			errorCode:  gce.ErrorCodeOther,
			isFlex:     true,
			expected:   false,
		},
		{
			name:           "Multi Host TPU can report ZRPE with OtherErrorClass",
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      gce.ErrorCodeResourcePoolExhausted,
			isMultiHostTPU: true,
			tpuType:        "tpuV5",
			expected:       true,
		},
		{
			name:           "Multi Host TPU can report quota with OtherErrorClass",
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      gce.ErrorCodeQuotaExceeded,
			isMultiHostTPU: true,
			tpuType:        "tpuV5",
			expected:       true,
		},
		{
			name:           "Other Multi Host TPU errors do not represent a stockout",
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      gce.ErrorCodeOther,
			isMultiHostTPU: true,
			tpuType:        "tpuV5",
			expected:       false,
		},
		{
			name:           "Multi Host TPU + Flex Start can report ZRPE with OtherErrorClass",
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      gce.ErrorCodeResourcePoolExhausted,
			isFlex:         true,
			isMultiHostTPU: true,
			tpuType:        "tpuV5",
			expected:       true,
		},
		{
			name:           "Multi Host TPU + Flex Start can report quota with OtherErrorClass",
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      gce.ErrorCodeQuotaExceeded,
			isFlex:         true,
			isMultiHostTPU: true,
			tpuType:        "tpuV5",
			expected:       true,
		},
		{
			name:           "Other Multi Host TPU + Flex Start errors do not represent a stockout",
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      gce.ErrorCodeOther,
			isFlex:         true,
			isMultiHostTPU: true,
			tpuType:        "tpuV5",
			expected:       false,
		},
		{
			name:           "MH_TPU_no_flex_start_is_stockout",
			errorClass:     cloudprovider.OtherErrorClass,
			errorCode:      "cloudProviderError",
			errorMessage:   "while creating Resize Request got 1 error(s): [INSUFFICIENT_CAPACITY]. Zone 'us-central1-c' is not available. Please try another zone",
			isMultiHostTPU: true,
			tpuType:        "tpu7x",
			expected:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deploymentType := gke.DeploymentTypeNone
			if tc.deploymentType != "" {
				deploymentType = tc.deploymentType
			}

			mig := gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{FlexStart: tc.isFlex, TpuType: tc.tpuType, TpuMultiHost: tc.isMultiHostTPU}).
				SetDeploymentType(deploymentType).
				Build()
			node := test.BuildTestNode("test-node", 1000, 1000)
			nodeInfo := framework.NewTestNodeInfo(node)
			errorInfo := cloudprovider.InstanceErrorInfo{ErrorClass: tc.errorClass, ErrorCode: tc.errorCode, ErrorMessage: tc.errorMessage}

			assert.Equal(t, tc.expected, IsStockout(mig, nodeInfo, errorInfo))
		})
	}
}
