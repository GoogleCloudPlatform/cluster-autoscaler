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

package providers

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestDryRunCreateBalloonPod(t *testing.T) {

	tests := []struct {
		name                 string
		cpuSize              int64
		expectedError        bool
		expectedErrorReason  string
		expectedErrorMessage string
	}{
		{
			name:          "Successful dry-run creation",
			expectedError: false,
		},
		{
			name:                 "Failure in pod creation",
			expectedError:        true,
			expectedErrorReason:  "pod failed validation",
			expectedErrorMessage: "balloon pod creation error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			fakeClient := fake.NewSimpleClientset()

			provider := &ResizableVmAutoprovisioningProvider{
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
				balloonPodChecker: &balloonPodChecker{
					clientSet: fakeClient,
				},
			}

			if tc.expectedError {
				fakeClient.PrependReactor("create", "pods", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, errors.New(tc.expectedErrorReason)
				})
			}

			err := provider.balloonPodChecker.dryRunCreateBalloonPod()

			if tc.expectedError {
				assert.Error(t, err)
				assert.Equal(t, fmt.Sprintf("%s: %s", tc.expectedErrorMessage, tc.expectedErrorReason), err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
