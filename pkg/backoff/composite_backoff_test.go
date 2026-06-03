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
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
)

func TestCompositeBackoff(t *testing.T) {
	keyFunc := func(nodeGroup cloudprovider.NodeGroup) string { return nodeGroup.Id() }
	backoffA := backoff.NewExponentialBackoff(10*time.Minute, 10*time.Minute, 10*time.Minute, keyFunc)
	backoffB := backoff.NewExponentialBackoff(10*time.Minute, 10*time.Minute, 10*time.Minute, keyFunc)
	compositeBackoff := NewCompositeBackoff([]backoff.Backoff{backoffA, backoffB}, nil)

	nodeGroupA := testNodeGroup("A", false, false)
	nodeGroupB := testNodeGroup("B", false, false)
	now := time.Now()

	assert.Equal(t, noBackoff, compositeBackoff.BackoffStatus(nodeGroupA, nil, now))
	assert.Equal(t, noBackoff, compositeBackoff.BackoffStatus(nodeGroupB, nil, now))

	backoffA.Backoff(nodeGroupA, nil, quotaError, now)
	assert.Equal(t, backoffWithQuotaError, compositeBackoff.BackoffStatus(nodeGroupA, nil, now))
	assert.Equal(t, backoffWithQuotaError, backoffA.BackoffStatus(nodeGroupA, nil, now))
	assert.Equal(t, noBackoff, backoffB.BackoffStatus(nodeGroupA, nil, now))

	compositeBackoff.Backoff(nodeGroupB, nil, ipSpaceExhaustedError(""), now)
	assert.Equal(t, backoffWithIPSpaceExhaustedError(""), compositeBackoff.BackoffStatus(nodeGroupB, nil, now))
	assert.Equal(t, backoffWithIPSpaceExhaustedError(""), backoffA.BackoffStatus(nodeGroupB, nil, now))
	assert.Equal(t, backoffWithIPSpaceExhaustedError(""), backoffB.BackoffStatus(nodeGroupB, nil, now))

	compositeBackoff.RemoveBackoff(nodeGroupB, nil)
	assert.Equal(t, noBackoff, compositeBackoff.BackoffStatus(nodeGroupB, nil, now))
	assert.Equal(t, noBackoff, backoffA.BackoffStatus(nodeGroupB, nil, now))
	assert.Equal(t, noBackoff, backoffB.BackoffStatus(nodeGroupB, nil, now))

}
