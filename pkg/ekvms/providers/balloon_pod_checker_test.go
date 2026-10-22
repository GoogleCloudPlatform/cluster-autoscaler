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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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

			err := provider.balloonPodChecker.dryRunCreateBalloonPod(t.Context())

			if tc.expectedError {
				assert.Error(t, err)
				assert.Equal(t, fmt.Sprintf("%s: %s", tc.expectedErrorMessage, tc.expectedErrorReason), err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBalloonPodCheckerConcurrentRunAndIsCreatable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		checker := &balloonPodChecker{
			clientSet:             fakeClient,
			runInterval:           time.Millisecond,
			isBalloonPodCreatable: true,
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const totalCalls = 10
		const failingCalls = 3
		var callCount atomic.Int32
		fakeClient.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
			n := int(callCount.Add(1))
			if n >= totalCalls {
				cancel()
			}
			if n <= failingCalls {
				return true, nil, errors.New("dry-run failure")
			}
			return true, nil, nil
		})

		var wg sync.WaitGroup
		wg.Go(func() {
			checker.Run(ctx)
		})
		wg.Go(func() {
			for ctx.Err() == nil {
				_ = checker.isCreatable()
				_ = checker.currentSize()
				time.Sleep(500 * time.Microsecond)
			}
		})
		wg.Wait()
		synctest.Wait()

		assert.True(t, checker.isCreatable())
		assert.Equal(t, balloonPodTestSizes[(totalCalls-failingCalls)%len(balloonPodTestSizes)], checker.currentSize())
	})
}
