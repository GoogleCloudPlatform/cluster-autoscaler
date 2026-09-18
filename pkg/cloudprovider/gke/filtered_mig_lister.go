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

package gke

import (
	"sync"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

type nodeGroupFilter interface {
	IsNodeGroupOwned(nodeGroup cloudprovider.NodeGroup) bool
}

type migListerInterface interface {
	GetMigs() []gce.Mig
	GetGkeMigs() []*GkeMig
	GetBlockedGkeMigs(reason IrretrievableMigBlockReason) []*GkeMig
	GetGkeMigsLocations() []string
	HandleMigIssue(migRef gce.GceRef, err error)
	InvalidateIrretrievableMigsCacheIfExpired() map[gce.GceRef]bool
	IsMigOwned(mig cloudprovider.NodeGroup) bool
	InvalidateOwnershipCache()
}

// FilteredMigLister wraps a migListerInterface and filters node groups
// based on whether their originating CCC is owned by the current CA instance.
type FilteredMigLister struct {
	baseLister migListerInterface
	filter     nodeGroupFilter

	// Per-loop ownership cache (cleared on Refresh via
	// InvalidateOwnershipCache) to avoid repeated CCC annotation lookups across
	// NodeGroupForNode and MIG list calls.
	mu       sync.RWMutex
	migOwned map[gce.GceRef]bool
}

// NewFilteredMigLister creates a new FilteredMigLister wrapper, or returns base
// lister i.e. disables filtering if autoscalerNames is empty.
func NewFilteredMigLister(base migListerInterface, filter nodeGroupFilter, autoscalerNames []string) migListerInterface {
	if len(autoscalerNames) == 0 || filter == nil {
		klog.Infof("MIG ownership filtering disabled (autoscalerNames=%v, filter set: %t)", autoscalerNames, filter != nil)
		return base
	}
	klog.Infof("MIG ownership filtering enabled for autoscalerNames=%v", autoscalerNames)
	return &FilteredMigLister{
		baseLister: base,
		filter:     filter,
		migOwned:   make(map[gce.GceRef]bool),
	}
}

// InvalidateOwnershipCache clears the cached MIG ownership results.
func (f *FilteredMigLister) InvalidateOwnershipCache() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.migOwned)
}

// IsMigOwned returns whether the given MIG is owned by this CA instance,
// caching the result per loop.
func (f *FilteredMigLister) IsMigOwned(mig cloudprovider.NodeGroup) bool {
	if mig == nil {
		return false
	}
	gkeMig, ok := mig.(*GkeMig)
	if !ok || gkeMig == nil {
		klog.Errorf("Unexpected node group %T (%v) in FilteredMigLister, expected non-nil *GkeMig", mig, mig)
		return false
	}
	// Cache hit
	ref := gkeMig.GceRef()
	f.mu.RLock()
	owned, found := f.migOwned[ref]
	f.mu.RUnlock()
	if found {
		return owned
	}

	// Cache miss - query filter and update cache
	owned = f.filter.IsNodeGroupOwned(gkeMig)
	f.mu.Lock()
	f.migOwned[ref] = owned
	f.mu.Unlock()
	return owned
}

// GetGkeMigs returns GKE Migs owned by this CA instance.
func (f *FilteredMigLister) GetGkeMigs() []*GkeMig {
	var filtered []*GkeMig
	for _, mig := range f.baseLister.GetGkeMigs() {
		if f.IsMigOwned(mig) {
			filtered = append(filtered, mig)
		}
	}
	return filtered
}

// GetMigs returns GCE Migs owned by this CA instance.
func (f *FilteredMigLister) GetMigs() []gce.Mig {
	var filtered []gce.Mig
	for _, mig := range f.baseLister.GetMigs() {
		if f.IsMigOwned(mig) {
			filtered = append(filtered, mig)
		}
	}
	return filtered
}

// GetBlockedGkeMigs returns blocked GKE MIGs owned by this CA instance. It's
// only used by NAP to delete unneeded autoprovisioned node pools, so it's
// filtered to stop both CA instances from deleting the same node pools. Whether
// a MIG is blocked is checked directly in GkeCache (e.g. BlockReason), not via
// this method, so that isn't affected.
func (f *FilteredMigLister) GetBlockedGkeMigs(reason IrretrievableMigBlockReason) []*GkeMig {
	var filtered []*GkeMig
	for _, mig := range f.baseLister.GetBlockedGkeMigs(reason) {
		if f.IsMigOwned(mig) {
			filtered = append(filtered, mig)
		}
	}
	return filtered
}

// GetGkeMigsLocations returns locations of Migs owned by this CA instance.
func (f *FilteredMigLister) GetGkeMigsLocations() []string {
	var results []string
	added := make(map[string]bool)
	for _, mig := range f.GetGkeMigs() {
		if !added[mig.gceRef.Zone] {
			added[mig.gceRef.Zone] = true
			results = append(results, mig.gceRef.Zone)
		}
	}
	return results
}

// HandleMigIssue delegates to base lister.
func (f *FilteredMigLister) HandleMigIssue(migRef gce.GceRef, err error) {
	f.baseLister.HandleMigIssue(migRef, err)
}

// InvalidateIrretrievableMigsCacheIfExpired delegates to base lister.
func (f *FilteredMigLister) InvalidateIrretrievableMigsCacheIfExpired() map[gce.GceRef]bool {
	return f.baseLister.InvalidateIrretrievableMigsCacheIfExpired()
}
