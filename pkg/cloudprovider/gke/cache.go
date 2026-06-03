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
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	klog "k8s.io/klog/v2"
)

const maxZonesInRegionCacheValidity = 15 * time.Minute

// GceCacheInterface decouples the underlying gce.gceCache to that used in GkeCache
// This will allow for better testing if GkeCache
// Added only the functions are need in this GkeCache
type GceCacheInterface interface {
	// RegisterMig will register the Mig
	RegisterMig(gce.Mig) bool
	// UnregisterMig will un-register the Mig
	UnregisterMig(gce.Mig) bool
	// GetMigs returns the cache Migs
	GetMigs() []gce.Mig
	// AddMachine adds machines to cache
	AddMachine(gce.MachineType, string)
	// SetMigBasename sets basename for given mig in cache
	SetMigBasename(gce.GceRef, string)
	// GetMigBasename gets basename for given mig from cache.
	GetMigBasename(gce.GceRef) (string, bool)
	// InvalidateMigInstances clears the mig instances cache
	InvalidateMigInstances(gce.GceRef)
	// InvalidateAllMigInstances clears the mig instances cache
	InvalidateAllMigInstances()
	// InvalidateAllMigBasenames clears the basename cache
	InvalidateAllMigBasenames()
	// InvalidateAllMigTargetSizes clears the target size cache
	InvalidateAllMigTargetSizes()
	// InvalidateAllMigInstanceTemplateNames clears the instance template name cache
	InvalidateAllMigInstanceTemplateNames()
	// GetMachine retrieves machine type from cache under lock.
	GetMachine(string, string) (gce.MachineType, bool)
	// SetMachines sets the machines cache under lock.
	SetMachines(machinesCache map[gce.MachineTypeKey]gce.MachineType)
	// InvalidateAllMachines invalidates the machines cache under lock.
	InvalidateAllMachines()
	// InvalidateMigTargetSize clears the target size cache
	InvalidateMigTargetSize(gce.GceRef)
	// SetMigTargetSize sets targetSize for a GceRef
	SetMigTargetSize(gce.GceRef, int64)
	// SetResourceLimiter sets resource limiter.
	SetResourceLimiter(*cloudprovider.ResourceLimiter)
	// GetResourceLimiter returns resource limiter.
	GetResourceLimiter() (*cloudprovider.ResourceLimiter, error)
	// SetListManagedInstancesResults sets listManagedInstancesResults for a given mig in cache
	SetListManagedInstancesResults(gce.GceRef, string)
	// GetListManagedInstancesResults gets listManagedInstancesResults for a given mig from cache.
	GetListManagedInstancesResults(gce.GceRef) (string, bool)
	// InvalidateAllListManagedInstancesResults invalidates all listManagedInstancesResults entries.
	InvalidateAllListManagedInstancesResults()
	// DropInstanceTemplatesForMissingMigs clears the instance template
	// cache intended MIGs which are no longer present in the cluster
	DropInstanceTemplatesForMissingMigs(currentMigs []gce.Mig)
}

// GkeCache has embedded GceCache and gkeMigs to store and return GkeMig objects
// GkeMigs are refresh only when migs in GceCache are refreshed
type GkeCache struct {
	GceCacheInterface
	nodeTemplateCache          *nodetemplate.Cache
	mutex                      sync.Mutex
	gkeMigs                    map[gce.GceRef]*GkeMig
	lastMigRegistration        map[gce.GceRef]time.Time
	allNodePoolNames           sets.Set[string]
	availableCpuPlatforms      map[string][]string
	availableDiskTypes         map[string][]string
	markedIrretrievableMigs    map[gce.GceRef]int                         // markedIrretrievableMigs holds all the irretrievable counts Migs, which are not blocked
	blockedIrretrievableMigs   map[gce.GceRef]IrretrievableMigBlockReason // blockedIrretrievableMigs holds all the irretrievable Migs which are blocked
	zonesInRegionCache         *cache.Expiring                            // All zones - used by the reservation puller for project-wide visibility.
	standardZonesInRegionCache *cache.Expiring                            // Standard zones only - used by zoneTypes matching and injection logic, for node pools located strictly in standard zones.
	aiZonesInRegionCache       *cache.Expiring                            // AI zones only - used by zoneTypes matching and injection logic, for node pools located strictly in AI zones.
	nodesScaleDownAllowed      map[string]bool
	scaleUpTimeProvider        ScaleUpTimeProvider

	capacityCheckWaitTimes map[gce.GceRef]durationResult
	nodePoolToMigs         map[string]map[gce.GceRef]*GkeMig
	nodePoolSpecs          map[string]*gkeclient.NodePoolSpec
}

type durationResult struct {
	value time.Duration
	err   error
}

// NewGkeCache returns a new GkeCache
func NewGkeCache(gceCache GceCacheInterface, nodeTemplateCache *nodetemplate.Cache) *GkeCache {
	return &GkeCache{
		GceCacheInterface:          gceCache,
		nodeTemplateCache:          nodeTemplateCache,
		allNodePoolNames:           sets.New[string](),
		gkeMigs:                    make(map[gce.GceRef]*GkeMig),
		lastMigRegistration:        make(map[gce.GceRef]time.Time),
		blockedIrretrievableMigs:   make(map[gce.GceRef]IrretrievableMigBlockReason),
		availableDiskTypes:         make(map[string][]string),
		markedIrretrievableMigs:    make(map[gce.GceRef]int),
		zonesInRegionCache:         cache.NewExpiring(),
		standardZonesInRegionCache: cache.NewExpiring(),
		aiZonesInRegionCache:       cache.NewExpiring(),
		nodesScaleDownAllowed:      make(map[string]bool),
		capacityCheckWaitTimes:     make(map[gce.GceRef]durationResult),
		nodePoolToMigs:             make(map[string]map[gce.GceRef]*GkeMig),
		nodePoolSpecs:              make(map[string]*gkeclient.NodePoolSpec),
	}
}

// RegisterMig hides/overrides the GceCache's RegisterMig when passing a *GkeMig explicitly. It
// also refreshes gkeMigs if needed, checks which and adds the MIG to a list of MIGs of its nodepool
func (g *GkeCache) RegisterMig(mig *GkeMig) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.lastMigRegistration[mig.gceRef] = time.Now()
	if !g.GceCacheInterface.RegisterMig(mig) {
		// mig cache has not changed
		return false
	}
	g.gkeMigs[mig.gceRef] = mig
	g.addMigToNodePoolMigsNoLock(mig)
	return true
}

// UnregisterNodePool unregister all cached resources belonging to the node pool
func (g *GkeCache) UnregisterNodePool(nodePoolName string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	for _, mig := range g.nodePoolToMigs[nodePoolName] {
		g.unregisterMigNoLock(mig)
	}
	delete(g.nodePoolToMigs, nodePoolName)
}

// UnregisterMig hides/overrides the GceCache's RegisterMig when passing a *GkeMig explicitly. It
// also refreshes gkeMigs if needed
func (g *GkeCache) UnregisterMig(mig *GkeMig) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return g.unregisterMigNoLock(mig)
}

func (g *GkeCache) unregisterMigNoLock(mig *GkeMig) bool {
	delete(g.blockedIrretrievableMigs, mig.gceRef)
	delete(g.markedIrretrievableMigs, mig.gceRef)
	if !g.GceCacheInterface.UnregisterMig(mig) {
		// mig cache has not changed
		return false
	}
	delete(g.lastMigRegistration, mig.gceRef)
	if _, found := g.gkeMigs[mig.gceRef]; found {
		delete(g.gkeMigs, mig.gceRef)
		g.removeMigFromNodePoolMigsNoLock(mig)
		return true
	}
	// Should not reach here.
	klog.Errorf("could not find %v in GkeMigs. GkeMigs and GceMigs seem to be out of sync.", mig.GceRef())
	return true
}

// Removes mig from the cached nodepool to nodegroup mapping
func (g *GkeCache) removeMigFromNodePoolMigsNoLock(mig *GkeMig) {
	nodePoolName := mig.NodePoolName()
	if nodePoolName == "" || len(g.nodePoolToMigs[nodePoolName]) == 0 {
		return
	}

	delete(g.nodePoolToMigs[nodePoolName], mig.gceRef)
}

// Adds mig to the cached nodepool to nodegroup mapping
func (g *GkeCache) addMigToNodePoolMigsNoLock(mig *GkeMig) {
	nodePoolName := mig.NodePoolName()
	if nodePoolName == "" {
		return
	}
	if len(g.nodePoolToMigs[nodePoolName]) == 0 {
		g.nodePoolToMigs[nodePoolName] = make(map[gce.GceRef]*GkeMig)
	}
	g.nodePoolToMigs[nodePoolName][mig.gceRef] = mig
}

// LastMigRegistration returns the last registration time of a given mig.
func (g *GkeCache) LastMigRegistration(ref gce.GceRef) time.Time {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return g.lastMigRegistration[ref]
}

// GetGkeMigs returns the cached list of gkeMigs
func (g *GkeCache) GetGkeMigs() []*GkeMig {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	var result = make([]*GkeMig, 0, len(g.gkeMigs))
	for _, mig := range g.gkeMigs {
		result = append(result, mig)
	}
	return result
}

// QueuedProvisioning returns if the given mig is coming from queued nodepool.
func (g *GkeCache) QueuedProvisioning(migRef gce.GceRef) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	result, found := g.gkeMigs[migRef]
	if !found {
		return false
	}
	return result.QueuedProvisioning()
}

// CapacityCheckWaitTimeSeconds returns CapacityCheckWaitTimeSeconds for a mig ref.
func (g *GkeCache) CapacityCheckWaitTimeSeconds(migRef gce.GceRef) (time.Duration, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if t, found := g.capacityCheckWaitTimes[migRef]; found {
		return t.value, t.err
	}

	mig, found := g.gkeMigs[migRef]
	if !found {
		return 0, fmt.Errorf("failed to find GkeMig for migRef: %+v", migRef)
	}

	ccwt := durationResult{}
	ccwt.value, ccwt.err = mig.gkeManager.EvaluateCapacityCheckWaitTimeSeconds(mig)
	g.capacityCheckWaitTimes[migRef] = ccwt

	return ccwt.value, ccwt.err
}

// ScaleUpTime returns ScaleUpTime for a mig ref.
func (g *GkeCache) ScaleUpTime(migRef gce.GceRef) (time.Time, error) {
	if g.scaleUpTimeProvider == nil {
		return time.Time{}, fmt.Errorf("failed to find ScaleUpTime for migRef: %+v. nil scaleUpTimeProvider", migRef)
	}

	g.mutex.Lock()
	defer g.mutex.Unlock()

	mig, found := g.gkeMigs[migRef]
	if !found {
		return time.Time{}, fmt.Errorf("failed to find GkeMig for migRef: %+v", migRef)
	}
	return g.scaleUpTimeProvider.NodeGroupScaleUpTime(mig)
}

// FlexStartNonQueued returns if the given mig is coming from flex start nodepool.
func (g *GkeCache) FlexStartNonQueued(migRef gce.GceRef) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	mig, found := g.gkeMigs[migRef]
	if !found {
		return false
	}
	return mig.FlexStartNonQueued()
}

// IsTpuMig returns true if the given mig is a TPU mig.
func (g *GkeCache) IsTpuMig(migRef gce.GceRef) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	result, found := g.gkeMigs[migRef]
	if !found {
		return false
	}
	return result.IsTpuMig()
}

// GetAllNodePoolNames returns all node pool names
func (g *GkeCache) GetAllNodePoolNames() sets.Set[string] {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return g.allNodePoolNames
}

// SetAllNodePoolNames updates the cache with provided node pool names
func (g *GkeCache) SetAllNodePoolNames(names sets.Set[string]) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.allNodePoolNames = names
}

// SetAvailableCpuPlatforms sets availableCpuPlatforms map in cache.
func (g *GkeCache) SetAvailableCpuPlatforms(availableCpuPlatforms map[string][]string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.availableCpuPlatforms = availableCpuPlatforms
}

// GetAvailableCpuPlatforms gets availableCpuPlatforms map from cache.
func (g *GkeCache) GetAvailableCpuPlatforms() (availableCpuPlatforms map[string][]string, found bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if g.availableCpuPlatforms == nil {
		return nil, false
	}
	return g.availableCpuPlatforms, true
}

// InvalidateAvailableCpuPlatforms invalidates availableCpuPlatforms in cache.
func (g *GkeCache) InvalidateAvailableCpuPlatforms() {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.availableCpuPlatforms = nil
}

// InvalidateCapacityCheckWaitTimes invalidates capacityCheckWaitTimes
func (g *GkeCache) InvalidateCapacityCheckWaitTimes() {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.capacityCheckWaitTimes = make(map[gce.GceRef]durationResult)
}

// SetAvailableDiskTypes sets availableDiskTypes map in cache.
func (g *GkeCache) SetAvailableDiskTypes(zone string, availableDiskTypes []string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.availableDiskTypes[zone] = availableDiskTypes
}

// GetAvailableDiskTypes gets availableDiskTypes map from cache.
func (g *GkeCache) GetAvailableDiskTypes(zone string) (availableDiskTypes []string, found bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	diskTypes, zoneFound := g.availableDiskTypes[zone]
	return diskTypes, zoneFound
}

// InvalidateAvailableDiskTypes invalidates availableDiskTypes in cache.
func (g *GkeCache) InvalidateAvailableDiskTypes() {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.availableDiskTypes = make(map[string][]string)
}

// MarkIrretrievableMig marks a MIG as irretrievable, blocks it after exceeding threshold
func (g *GkeCache) MarkIrretrievableMig(migRef gce.GceRef, marksBeforeBlocked int, reason IrretrievableMigBlockReason) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	klog.V(4).Infof("Marking irretrievable MIG %v", migRef)
	g.markedIrretrievableMigs[migRef]++
	if g.markedIrretrievableMigs[migRef] >= marksBeforeBlocked {
		klog.Warningf("Blocking irretrievable MIG %v, reason: %v", migRef, reason)
		g.blockedIrretrievableMigs[migRef] = reason
		g.markedIrretrievableMigs[migRef] = marksBeforeBlocked
	}
	return g.blockedIrretrievableMigs[migRef] != 0
}

// IsMigBlocked checks if the Mig is blocked
func (g *GkeCache) IsMigBlocked(migRef gce.GceRef) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	return g.blockedIrretrievableMigs[migRef] != 0
}

// BlockReason returns whether the Mig is blocked and the reason of the block.
func (g *GkeCache) BlockReason(migRef gce.GceRef) (bool, IrretrievableMigBlockReason) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	return g.blockedIrretrievableMigs[migRef] != 0, g.blockedIrretrievableMigs[migRef]
}

// InvalidateMarkedIrretrievableMigs invalidates the marks of unavailable migs
func (g *GkeCache) InvalidateMarkedIrretrievableMigs() {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	klog.V(4).Infof("Invalidate marked irretrievable MIG cache, %d MIGs removed", len(g.markedIrretrievableMigs))
	g.markedIrretrievableMigs = make(map[gce.GceRef]int)
}

// InvalidateBlockedIrretrievableMigs invalidates the blocked list
func (g *GkeCache) InvalidateBlockedIrretrievableMigs() map[gce.GceRef]bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	// Before invalidating collects all the migs blocked because of CloudProviderError
	result := g.previouslyBlockedMigsForCloudProviderError()

	klog.V(4).Infof("Invalidate blocked irretrievable MIG cache, %d MIGs removed", len(g.blockedIrretrievableMigs))
	g.blockedIrretrievableMigs = make(map[gce.GceRef]IrretrievableMigBlockReason)
	return result
}

// SetZonesInRegion sets zones that are present in a given region
func (g *GkeCache) SetZonesInRegion(region string, zones []string) {
	g.zonesInRegionCache.Set(
		region,
		zones,
		maxZonesInRegionCacheValidity,
	)
}

// SetStandardZonesInRegion sets standard zones that are present in a given region from cache.
func (g *GkeCache) SetStandardZonesInRegion(region string, standardZones []string) {
	g.standardZonesInRegionCache.Set(
		region,
		standardZones,
		maxZonesInRegionCacheValidity,
	)
}

// GetStandardZonesInRegion returns standard zones that are present in a given region from cache.
func (g *GkeCache) GetStandardZonesInRegion(region string) ([]string, bool) {
	value, ok := g.standardZonesInRegionCache.Get(region)
	if !ok {
		return nil, false
	}

	standardZones, ok := value.([]string)
	if !ok {
		return nil, false
	}

	return standardZones, true
}

// GetZonesInRegion returns zones that are present in a given region
func (g *GkeCache) GetZonesInRegion(region string) ([]string, bool) {
	value, ok := g.zonesInRegionCache.Get(region)
	if !ok {
		return nil, false
	}

	zones, ok := value.([]string)
	if !ok {
		return nil, false
	}

	return zones, true
}

// SetAIZonesInRegion sets AI zones that are present in a given region
func (g *GkeCache) SetAIZonesInRegion(region string, aiZones []string) {
	g.aiZonesInRegionCache.Set(
		region,
		aiZones,
		maxZonesInRegionCacheValidity,
	)
}

// GetAIZonesInRegion returns AI zones that are present in a given region
func (g *GkeCache) GetAIZonesInRegion(region string) ([]string, bool) {
	value, ok := g.aiZonesInRegionCache.Get(region)
	if !ok {
		return nil, false
	}

	aiZones, ok := value.([]string)
	if !ok {
		return nil, false
	}

	return aiZones, true
}

// previouslyBlockedMigsForCloudProviderError returns all the Migs previously
// blocked because of an error of type CloudProvider
func (g *GkeCache) previouslyBlockedMigsForCloudProviderError() map[gce.GceRef]bool {
	result := make(map[gce.GceRef]bool)
	for migRef, reason := range g.blockedIrretrievableMigs {
		if reason == IrretrievableMigReasonCloudProviderError {
			result[migRef] = true
		}
	}
	return result
}

func (g *GkeCache) updateNodesScaleDownAllowed(nodesScaleDownAllowed map[string]bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if len(nodesScaleDownAllowed) == 0 {
		return
	}
	if g.nodesScaleDownAllowed == nil {
		g.nodesScaleDownAllowed = make(map[string]bool)
	}
	for nodeName, allowed := range nodesScaleDownAllowed {
		g.nodesScaleDownAllowed[nodeName] = allowed
	}
}

func (g *GkeCache) getNodesScaleDownAllowed(nodeNames []string) map[string]bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if len(nodeNames) == 0 || len(g.nodesScaleDownAllowed) == 0 {
		return make(map[string]bool)
	}

	scaleDownAllowed := make(map[string]bool)
	for _, nodeName := range nodeNames {
		if allowed, found := g.nodesScaleDownAllowed[nodeName]; found {
			scaleDownAllowed[nodeName] = allowed
		}
	}

	return scaleDownAllowed
}

func (g *GkeCache) invalidateNodesScaleDownAllowed() {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.nodesScaleDownAllowed = make(map[string]bool)
}

// ExistingMigsInNodePool returns a list of MIG references that belong to a given node pool.
func (g *GkeCache) ExistingMigsInNodePool(nodePoolName string) []*GkeMig {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	return slices.Collect(maps.Values(g.nodePoolToMigs[nodePoolName]))
}

// InvalidateNodePoolSpecCache clears the node pool spec cache.
func (g *GkeCache) InvalidateNodePoolSpecCache() {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.nodePoolSpecs = make(map[string]*gkeclient.NodePoolSpec)
}

// RegisterNodePoolSpecs registers the node pool specs in the cache.
func (g *GkeCache) RegisterNodePoolSpecs(specs map[string]*gkeclient.NodePoolSpec) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.nodePoolSpecs = specs
}

// GetNodePoolSpec returns the node pool spec for a given node pool name.
func (g *GkeCache) GetNodePoolSpec(nodePoolName string) (*gkeclient.NodePoolSpec, bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	spec, found := g.nodePoolSpecs[nodePoolName]
	return spec, found
}
