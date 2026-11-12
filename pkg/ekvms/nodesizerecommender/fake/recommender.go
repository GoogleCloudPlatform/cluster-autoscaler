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

// Package fake provides a stateful fake implementation of
// nodesizerecommender.NodeSizeRecommender for testing.
package fake

import (
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

// Recommender is a stateful fake implementation of
// nodesizerecommender.NodeSizeRecommender. Unless overridden per node, per
// zone, or with a default max size, it returns the physical max VM size of each
// resizable node's machine type.
type Recommender struct {
	mcp *machinetypes.MachineConfigProvider

	mu             sync.Mutex
	defaultMaxSize size.VmSize
	hasDefaultMax  bool
	nodeOverrides  map[string]*nodesizerecommender.MaxSizeRecommendation
	zoneMaxSizes   map[string]size.VmSize
	seenNodes      map[string]bool
	locations      []string
}

var _ nodesizerecommender.NodeSizeRecommender = (*Recommender)(nil)

// NewRecommender creates a new fake NodeSizeRecommender backed by mcp.
func NewRecommender(mcp *machinetypes.MachineConfigProvider) *Recommender {
	if mcp == nil {
		mcp = machinetypes.NewMachineConfigProvider(nil)
	}
	return &Recommender{
		mcp:           mcp,
		nodeOverrides: make(map[string]*nodesizerecommender.MaxSizeRecommendation),
		zoneMaxSizes:  make(map[string]size.VmSize),
		seenNodes:     make(map[string]bool),
	}
}

// WithMachineConfigProvider updates the MachineConfigProvider used to resolve
// physical max sizes. Call only during single-threaded setup before concurrent
// use.
func (r *Recommender) WithMachineConfigProvider(mcp *machinetypes.MachineConfigProvider) *Recommender {
	if mcp == nil {
		return r
	}
	r.mcp = mcp
	return r
}

// Factory returns a RecommenderFactory that yields this Recommender instance.
func (r *Recommender) Factory() nodesizerecommender.RecommenderFactory {
	return func(options.AutoscalingOptions, nodesizerecommender.InstanceFetcher, config.StringFlagEvaluator) (nodesizerecommender.NodeSizeRecommender, error) {
		return r, nil
	}
}

// SetDefaultMaxSize overrides the fallback max VM size returned when no node or
// zone override matches.
func (r *Recommender) SetDefaultMaxSize(maxSize size.VmSize) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultMaxSize = maxSize
	r.hasDefaultMax = true
}

// SetNodeMaxSize overrides the max VM size recommended for nodeName.
func (r *Recommender) SetNodeMaxSize(nodeName string, maxSize size.VmSize) {
	r.SetNodeRecommendation(nodeName, &nodesizerecommender.MaxSizeRecommendation{
		VmSize: maxSize,
	})
}

// SetNodeNonResizable marks nodeName as a non-resizable spillover VM ({0, 0}
// max size).
func (r *Recommender) SetNodeNonResizable(nodeName string) {
	r.SetNodeMaxSize(nodeName, size.VmSize{})
}

// SetNodeRecommendation sets an explicit recommendation (including CreationTime
// or nil) for nodeName.
func (r *Recommender) SetNodeRecommendation(nodeName string, rec *nodesizerecommender.MaxSizeRecommendation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec == nil {
		r.nodeOverrides[nodeName] = nil
		return
	}
	recCopy := *rec
	r.nodeOverrides[nodeName] = &recCopy
}

// ClearNodeOverride removes any per-node override for nodeName.
func (r *Recommender) ClearNodeOverride(nodeName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodeOverrides, nodeName)
}

// SetZoneMaxSize overrides the max VM size recommended for nodes in zone when
// no per-node override exists.
func (r *Recommender) SetZoneMaxSize(zone string, maxSize size.VmSize) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.zoneMaxSizes[zone] = maxSize
}

// HasSeenNode reports whether MaxSize has been queried for nodeName.
func (r *Recommender) HasSeenNode(nodeName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seenNodes[nodeName]
}

// Locations returns a copy of the cluster locations last observed via
// SetLocations.
func (r *Recommender) Locations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.locations)
}

// SetLocations implements nodesizerecommender.NodeSizeRecommender.
func (r *Recommender) SetLocations(locations []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.locations = slices.Clone(locations)
}

// MaxSize implements nodesizerecommender.NodeSizeRecommender.
func (r *Recommender) MaxSize(node *corev1.Node) *nodesizerecommender.MaxSizeRecommendation {
	if node == nil {
		return nil
	}
	now := time.Now()
	if rec, ok := r.lookupOverride(node.Name, resolveNodeZone(node), now); ok {
		return rec
	}

	mt := node.Labels[corev1.LabelInstanceTypeStable]
	if mt == "" || r.mcp == nil {
		return nil
	}
	maxSize, err := r.mcp.GetMaxResizableVmSizeByMachineType(mt)
	if err != nil {
		return nil
	}
	return &nodesizerecommender.MaxSizeRecommendation{
		VmSize:       maxSize,
		CreationTime: now,
	}
}

// lookupOverride records nodeName as seen and returns any configured node,
// zone, or default recommendation. The bool reports whether an override matched
// (including an explicit nil recommendation), so callers skip the provider
// fallback.
func (r *Recommender) lookupOverride(nodeName, zone string, now time.Time) (*nodesizerecommender.MaxSizeRecommendation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seenNodes[nodeName] = true

	if override, ok := r.nodeOverrides[nodeName]; ok {
		if override == nil {
			return nil, true
		}
		rec := *override
		if rec.CreationTime.IsZero() {
			rec.CreationTime = now
		}
		return &rec, true
	}

	if zone != "" {
		if zMax, ok := r.zoneMaxSizes[zone]; ok {
			return &nodesizerecommender.MaxSizeRecommendation{
				VmSize:       zMax,
				CreationTime: now,
			}, true
		}
	}

	if r.hasDefaultMax {
		return &nodesizerecommender.MaxSizeRecommendation{
			VmSize:       r.defaultMaxSize,
			CreationTime: now,
		}, true
	}

	return nil, false
}

func resolveNodeZone(node *corev1.Node) string {
	if z := node.Labels[corev1.LabelTopologyZone]; z != "" {
		return z
	}
	if ref, err := gce.GceRefFromProviderId(node.Spec.ProviderID); err == nil {
		return ref.Zone
	}
	return ""
}
