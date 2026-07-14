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
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
)

const (
	// napBackoffResetTime is the time after first backoff when the backoff duration is reset
	napBackoffResetTime = 20 * time.Hour
)

// napBackoff allows to backoff the NAP to create a new nodes.
type napBackoff struct {
	expBackoff *exponentialBackoff
}

// NewNapBackoff initialise napBackoff.
func NewNapBackoff(initialBackoffDuration, maxBackoffDuration time.Duration) base_backoff.Backoff {
	return &napBackoff{
		expBackoff: NewExponentialBackoff(initialBackoffDuration, maxBackoffDuration, napBackoffResetTime),
	}
}

// Backoff execution for NAP. Returns time till execution is backed off.
func (b *napBackoff) Backoff(_ cloudprovider.NodeGroup, _ *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	if isGkePersistentOperationError(errorInfo.ErrorCode) {
		b.expBackoff.Backoff(errorInfo, currentTime)
	}
	return b.expBackoff.BackoffUntil()
}

// BackoffStatus returns whether the execution is backed off for the given node group and error info when the node group is backed off.
func (b *napBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, _ *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	if nodeGroup.Exist() || currentTime.After(b.expBackoff.BackoffUntil()) {
		return base_backoff.Status{IsBackedOff: false}
	}
	return base_backoff.Status{IsBackedOff: true, ErrorInfo: b.expBackoff.ErrorInfo()}
}

// RemoveBackoff is not implemented for napBackoff.
func (b *napBackoff) RemoveBackoff(_ cloudprovider.NodeGroup, _ *framework.NodeInfo) {}

// RemoveStaleBackoffData removes stale backoff data.
func (b *napBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	b.expBackoff.RemoveStaleBackoffData(currentTime)
}

func isGkePersistentOperationError(errorCode string) bool {
	return errorCode == gceclient.GkePersistentOperationError
}
