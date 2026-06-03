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
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func TestIsBackedOffBasic(t *testing.T) {
	currentTime := time.Now()
	b := napBackoff{
		expBackoff: &exponentialBackoff{
			until:     currentTime.Add(5 * time.Second),
			reset:     currentTime.Add(2 * time.Second),
			errorInfo: quotaError,
		},
	}
	ni := framework.NewTestNodeInfo(nil)
	builder := gke.NewTestGkeMigBuilder().SetAutoprovisioned(true)
	ng := builder.Build()

	// Node group is backed off when reset time hasn't expiered.
	b.RemoveStaleBackoffData(currentTime.Add(1 * time.Second))
	assert.Equal(t, backoffWithQuotaError, b.BackoffStatus(ng, ni, currentTime))

	// Node group is not backed off after backoff interval.
	assert.Equal(t, noBackoff, b.BackoffStatus(ng, ni, currentTime.Add(5*time.Second+1*time.Millisecond)))

	// Existed node group is not backed-off.
	builder.SetExist(true)
	ngExist := builder.Build()
	assert.Equal(t, noBackoff, b.BackoffStatus(ngExist, ni, currentTime))

	// Node group is not backed off if reset time expired.
	b.RemoveStaleBackoffData(currentTime.Add(3 * time.Second))
	assert.Equal(t, noBackoff, b.BackoffStatus(ng, ni, currentTime.Add(3*time.Second+1*time.Millisecond)))

}

func TestBackOff(t *testing.T) {
	backoffDuration := 2 * time.Second
	maxBackoffDuration := 5 * time.Second
	minTimeTick := 1 * time.Second
	testCases := []struct {
		name                string
		errsPassedToBackoff []cloudprovider.InstanceErrorInfo
		wantBackoffStatus   []backoff.Status
		wantBackoffDuration []time.Duration
	}{
		{
			name:                "2 backoff call, none of them activate NapBackoff",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{quotaError, quotaError},
			wantBackoffStatus:   []backoff.Status{noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second, 0 * time.Second},
		},
		{
			name:                "1 GlobalPersistentError",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{gkePersistentOperationError},
			wantBackoffStatus:   []backoff.Status{backoffWithGkePersistentOperationError},
			wantBackoffDuration: []time.Duration{backoffDuration},
		},
		{
			name:                "1 GlobalPersistentError and 1 regular error, backoff is expired",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{gkePersistentOperationError, quotaError},
			wantBackoffStatus:   []backoff.Status{backoffWithGkePersistentOperationError, noBackoff},
			wantBackoffDuration: []time.Duration{backoffDuration, 0 * time.Second},
		},
		{
			name:                "1 regular error and 1 GlobalPersistentError, backoff is active",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{quotaError, gkePersistentOperationError},
			wantBackoffStatus:   []backoff.Status{noBackoff, backoffWithGkePersistentOperationError},
			wantBackoffDuration: []time.Duration{0 * time.Second, backoffDuration},
		},
		{
			name:                "2 GlobalPersistentError, backoff duration is doubled",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{gkePersistentOperationError, gkePersistentOperationError},
			wantBackoffStatus:   []backoff.Status{backoffWithGkePersistentOperationError, backoffWithGkePersistentOperationError},
			wantBackoffDuration: []time.Duration{backoffDuration, 2 * backoffDuration},
		},
		{
			name:                "3 GlobalPersistentError, maxBackoff duration is reached ",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{gkePersistentOperationError, gkePersistentOperationError, gkePersistentOperationError},
			wantBackoffStatus:   []backoff.Status{backoffWithGkePersistentOperationError, backoffWithGkePersistentOperationError, backoffWithGkePersistentOperationError},
			wantBackoffDuration: []time.Duration{backoffDuration, 2 * backoffDuration, maxBackoffDuration},
		},
		{
			name:                "2 GlobalPersistentError and 1 regular error, backoff duration is doubled ",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{gkePersistentOperationError, quotaError, gkePersistentOperationError},
			wantBackoffStatus:   []backoff.Status{backoffWithGkePersistentOperationError, noBackoff, backoffWithGkePersistentOperationError},
			wantBackoffDuration: []time.Duration{backoffDuration, 0 * time.Second, 2 * backoffDuration},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			b := NewNapBackoff(backoffDuration, maxBackoffDuration)
			ni := framework.NewTestNodeInfo(nil)
			ng := &gke.GkeMig{}
			currentTime := time.Now()

			nextTime := currentTime
			for i, err := range test.errsPassedToBackoff {

				endTime := b.Backoff(ng, ni, err, currentTime)
				gotBackoffDuration := 0 * time.Second
				if endTime.After(currentTime) {
					gotBackoffDuration = endTime.Sub(currentTime)
					nextTime = endTime
				} else {
					nextTime = currentTime.Add(minTimeTick)
				}

				assert.Equal(t, test.wantBackoffStatus[i], b.BackoffStatus(ng, ni, currentTime.Add(1*time.Millisecond)))
				assert.Equal(t, test.wantBackoffDuration[i], gotBackoffDuration)

				currentTime = nextTime
			}
		})
	}
}
