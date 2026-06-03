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

package instanceavailability

import (
	"fmt"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

type ProvisioningMode string

const (
	Spot      ProvisioningMode = "SPOT"
	Standard  ProvisioningMode = "STANDARD"
	FlexStart ProvisioningMode = "FLEX_START"
)

type Provider interface {
	// GetInstanceAvailability returns a Snapshot of instance availability if available, nil otherwise. Will trigger RegisterFlexibilityScope if flexibilityScopeKey is missing. non-blocking.
	GetInstanceAvailability(flexibilityScopeKey, instanceConfigKey string) *Snapshot
	// RegisterFlexibilityScope registers the flexibilityScopeKey in Provider, so it is fetched and refreshed in the background.
	RegisterFlexibilityScope(flexibilityScopeKey string)
	// AwaitInstanceAvailability returns a Snapshot of instance availability in a blocking manner.
	// Will trigger RegisterFlexibilityScope if flexibilityScopeKey is missing in Provider.
	// Blocks until the background refresh operations to finish if they are running for the first time.
	AwaitInstanceAvailability(flexibilityScopeKey, instanceConfigKey string) (*Snapshot, error)
	// MarkUsed in-place updates current InstanceAvailability with information about VMs decided to be provisioned.
	MarkUsed(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId string, zonalInstancesToProvision map[string]int) error
	// TODO(b/514577515): move balancing phase metrics reporting to single place, remove IncrementFlexAdvisorCacheQueryCount from this interface
	// IncrementFlexAdvisorCacheQueryCount attaches additional debugging info, emits the metric
	IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, flexibilityScopeKey string, instanceConfigKey string)
}

type Snapshot struct {
	provider                Provider
	flexibilityScopeKey     string
	instanceConfigKey       string
	guidanceId              string
	zonalInstanceCount      map[string]int
	zonalGcePreferenceScore map[string]float64
}

// String returns a string representation of Snapshot.
func (i *Snapshot) String() string {
	return fmt.Sprintf("Snapshot{flexibilityScopeKey: %v, instanceConfigKey: %v, guidanceId: %v, zonalInstanceCount: %v, zonalGcePreferenceScore: %v}",
		i.flexibilityScopeKey, i.instanceConfigKey, i.guidanceId, i.zonalInstanceCount, i.zonalGcePreferenceScore)
}

// NewSnapshot creates a new Snapshot
func NewSnapshot(provider Provider, flexibilityScopeKey, instanceConfigKey, guidanceId string, zonalInstanceCount map[string]int, zonalGcePreferenceScore map[string]float64) *Snapshot {
	return &Snapshot{
		provider:                provider,
		flexibilityScopeKey:     flexibilityScopeKey,
		instanceConfigKey:       instanceConfigKey,
		zonalInstanceCount:      zonalInstanceCount,
		zonalGcePreferenceScore: zonalGcePreferenceScore,
		guidanceId:              guidanceId,
	}
}

// MaxAvailableInstances returns the maximum available capacity for the zone.
func (i *Snapshot) MaxAvailableInstances(zone string) (int, bool) {
	count, found := i.zonalInstanceCount[zone]
	return count, found
}

// GcePreferenceScore returns the GCE zonal preference for the zone.
func (i *Snapshot) GcePreferenceScore(zone string) float64 {
	return i.zonalGcePreferenceScore[zone]
}

// MarkUsed internally accounts for the capacity consumption in the snapshot and inform the Provider about the capacity consumption.
func (i *Snapshot) MarkUsed(zonalInstancesToProvision map[string]int, decisionId string) error {
	err := i.provider.MarkUsed(i.flexibilityScopeKey, i.instanceConfigKey, i.guidanceId, decisionId, zonalInstancesToProvision)
	if err != nil {
		return err
	}
	for zone, count := range zonalInstancesToProvision {
		if _, found := i.zonalInstanceCount[zone]; !found {
			continue
		}
		i.zonalInstanceCount[zone] -= count
	}
	return nil
}

// SetProvider updates the Provider. This is intended only for testing.
func (i *Snapshot) SetProvider(provider Provider) {
	i.provider = provider
}

// GuidanceId returns the guidanceId of the snapshot.
func (i *Snapshot) GuidanceId() string {
	return i.guidanceId
}
