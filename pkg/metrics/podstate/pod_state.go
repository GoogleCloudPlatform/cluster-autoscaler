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

package podstate

import (
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
)

// podStateData tracks a pending pod's wait timing and observed reaction for
// reaction-time and pending-pod metrics. Entries stored in PodStateObserver maps
// may only be accessed with mux held; call methods without mux only on value
// copies or entries removed from the maps.
type podStateData struct {
	systemPod          bool
	hasPVC             bool
	hasCSI             bool
	reactionType       metrics.ReactionType
	unschedulableSince *time.Time
	cccName            string
	allocationMode     podutils.DeviceAllocationMode
	// scaleUpEligibleAt is when the pod's scale-up delay ends. See
	// PodStateObserver.scaleUpEligibleAt.
	scaleUpEligibleAt time.Time
}

func (sd *podStateData) setUnschedulableSinceIfUnset(t *time.Time) {
	if sd.unschedulableSince == nil {
		sd.unschedulableSince = t
	}
}

func (sd *podStateData) isTimedOut(now time.Time) bool {
	return sd.unschedulableSince != nil && sd.elapsedReactionTime(now) >= metrics.MaxReactionTime
}

// elapsedReactionTime returns the time that passed since the reaction clock
// started: the later of when the pod was marked as unschedulable and when its
// scale-up delay ended, as CA doesn't scale up for the pod until then. It
// returns 0 if the pod was not marked as unschedulable.
func (sd *podStateData) elapsedReactionTime(now time.Time) time.Duration {
	if sd.unschedulableSince == nil {
		return 0
	}
	start := *sd.unschedulableSince
	if sd.scaleUpEligibleAt.After(start) {
		start = sd.scaleUpEligibleAt
	}
	return max(now.Sub(start), 0)
}
