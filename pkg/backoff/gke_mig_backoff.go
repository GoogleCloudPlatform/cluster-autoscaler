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

	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
)

const (
	// MaxNodeGroupBackoffDuration is the maximum backoff duration for a NodeGroup after new nodes failed to start.
	MaxNodeGroupBackoffDuration = 30 * time.Minute

	// InitialNodeGroupBackoffDuration is the duration of first backoff after a new node failed to start.
	InitialNodeGroupBackoffDuration = 5 * time.Minute

	// NodeGroupBackoffResetTimeout is the time after last failed scale-up when the backoff duration is reset.
	NodeGroupBackoffResetTimeout = 3 * time.Hour

	// NpcBackoffDuration is a duration of a backoff applied to entire NPC NodeConfig rule
	NpcBackoffDuration = 5 * time.Minute

	// ResizableFamilyInitialBackOffDuration is the duration of the first backoff applied to a specific machine family after a resizable node in that family failed to start.
	ResizableFamilyInitialBackOffDuration = 5 * time.Minute
)

// Config lists all input configuration of gkeBackoff
type Config struct {
	CustomResourceProcessor customresources.CustomResourcesProcessor
	NpcLister               npc_lister.Lister
	CloudProvider           backoffCloudProvider
	AsyncNodeGroupsEnabled  bool
	FrbConfig               *FutureReservationsBackoffConfig
	Observers               []BackoffObserver
}

// NewGkeBackoff creates an instance of GKE specific backoff where node groups are identified based on.
// canonical string build from node template.
func NewGkeBackoff(config Config) CompositeBackoff {
	nodeGroupBasedBackoff := NewSingleMigBackoff()
	resourceBasedBackoff := NewResourceBackoff(config.CustomResourceProcessor, InitialNodeGroupBackoffDuration, MaxNodeGroupBackoffDuration, NodeGroupBackoffResetTimeout)
	napBackoff := NewNapBackoff(InitialNodeGroupBackoffDuration, MaxNodeGroupBackoffDuration)
	cidrIpBackoff := NewCidrIpBackoff(InitialNodeGroupBackoffDuration, MaxNodeGroupBackoffDuration, NodeGroupBackoffResetTimeout)
	npcBackoff := NewNpcCrdBackoff(NpcBackoffDuration, config.NpcLister, config.CloudProvider, config.Observers...)
	reservationsBackoff := NewReservationsBackoff(InitialNodeGroupBackoffDuration, MaxNodeGroupBackoffDuration, config.CloudProvider)
	anyThenFailReservationsBackoff := NewAnyThenFailReservationsBackoff(InitialNodeGroupBackoffDuration, MaxNodeGroupBackoffDuration, NodeGroupBackoffResetTimeout)

	backoffs := []base_backoff.Backoff{
		nodeGroupBasedBackoff,
		resourceBasedBackoff,
		napBackoff,
		cidrIpBackoff,
		npcBackoff,
		reservationsBackoff,
		anyThenFailReservationsBackoff,
	}

	if config.FrbConfig != nil && config.FrbConfig.Enabled {
		futureReservationsBackoff := NewFutureReservationsBackoff(config.FrbConfig)
		// put Future Reservation backoff in front to return more specific status as CompositeBackoff.BackoffStatus returns
		// the first backoff status with IsBackedOff=true and many registered backoffs can return a status with
		// IsBackedOff=true, but if node pool is backed off because of future reservation not started, then frBackoff's
		// status will contain the details
		backoffs = append([]base_backoff.Backoff{futureReservationsBackoff}, backoffs...)
	}

	if config.AsyncNodeGroupsEnabled {
		return NewSynchronizedCompositeBackoff(backoffs, config.Observers)
	}
	return NewCompositeBackoff(backoffs, config.Observers)
}
