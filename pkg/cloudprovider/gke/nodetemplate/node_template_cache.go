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

package nodetemplate

// TODO(b/207768334): Rename file to cache.go

import (
	"fmt"
	"hash/maphash"
	"math"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/hash"
	"k8s.io/utils/clock"
)

const (
	ShortTTL = time.Minute * 10
	LongTTL  = time.Hour * 24
)

const (
	maxSize               = 1000
	deltaLoggingThreshold = 0.001
)

var hashSeed maphash.Seed

// BuildKeyForNAP is constructing key for template created by NodeAutoprovisioning.
func BuildKeyForNAP(spec *gkeclient.NodePoolSpec, osDistribution string, nodeVersion string, zone string) string {
	hasher := &maphash.Hash{}
	hasher.SetSeed(hashSeed)
	hash.DeepHashObject(hasher, spec)

	_, err := hasher.WriteString(osDistribution)
	if err != nil {
		klog.Errorf("Error while building a key for node template cache (should never happen): %v", err)
	}

	_, err = hasher.WriteString(nodeVersion)
	if err != nil {
		klog.Errorf("Error while building a key for node template cache (should never happen): %v", err)
	}

	_, err = hasher.WriteString(zone)
	if err != nil {
		klog.Errorf("Error while building a key for node template cache (should never happen): %v", err)
	}

	return fmt.Sprintf("nap-%v", hasher.Sum64())
}

// BuildKeyForCA is constructing key for template used in Scale from 0.
func BuildKeyForCA(instanceTemplateId uint64) string {
	return fmt.Sprintf("ca-%v", instanceTemplateId)
}

// Cache contains NodeTemplates created by Node Autoprovisioning or by Cluster Autoscaler for Node Pools with 0 nodes.
type Cache struct {
	lock  sync.Mutex
	cache *cache.LRUExpireCache
}

// NewCacheWithClock creates a new cache with given clock.
func NewCacheWithClock(clock cache.Clock) *Cache {
	return &Cache{
		cache: cache.NewLRUExpireCacheWithClock(maxSize, clock),
	}
}

// NewCache creates a new cache.
func NewCache() *Cache {
	return NewCacheWithClock(clock.RealClock{})
}

// Add adds item to the cache.
func (c *Cache) Add(key string, nodeTemplate *apiv1.Node, ttl time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Add(key, nodeTemplate, ttl)
}

// Cleanup removes all cache entries.
func (c *Cache) Cleanup() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.RemoveAll(func(key any) bool { return true })
}

// Get gets and returns the node template corresponding to a given key, or (nil, false) if the key is not present in the cache.
func (c *Cache) Get(key string) (*apiv1.Node, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	value, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}
	item := value.(*apiv1.Node)
	return item, true
}

// NodeCompareResult contains the results of comparison between real node and template node.
type NodeCompareResult struct {
	// relative difference between template's and node's resource values
	ResourceDiff        map[string]float64
	MissingSystemLabels map[string]bool
}

// Compare created node with template are returns NodeCompareResult.
// The pair (NodeCompareResult{}, nil) will be returned if the template doesn't exist in the cache.
func (c *Cache) Compare(key string, node *apiv1.Node) (NodeCompareResult, error) {
	result := NodeCompareResult{}
	template, ok := c.Get(key)
	if !ok {
		return result, nil
	}
	if node.Status.Allocatable == nil {
		return result, fmt.Errorf("allocatable for node %s is null", node.ObjectMeta.Name)
	}
	result.ResourceDiff = make(map[string]float64)
	fullLog := false
	for resource, templateQuantity := range template.Status.Allocatable {
		nodeQuantity, ok := node.Status.Allocatable[resource]
		if !ok || nodeQuantity.Value() == 0 {
			if templateQuantity.Value() == 0 {
				continue
			}
			return result, fmt.Errorf("resource %s is not present on the node %s, but is present on template", resource.String(), node.ObjectMeta.Name)
		}
		diff := float64(templateQuantity.Value() - nodeQuantity.Value())
		diff = diff / float64(nodeQuantity.Value())
		if math.Abs(diff) > deltaLoggingThreshold {
			fullLog = true
		}
		result.ResourceDiff[resource.String()] = diff
	}
	result.MissingSystemLabels = make(map[string]bool)
	for k := range node.Labels {
		if _, found := template.Labels[k]; !found && labels.IsSystemLabel(k) {
			result.MissingSystemLabels[k] = true
		}
	}
	klog.Infof("Node %q matched with template %q under key %q, diff: %v", node.Name, template.Name, key, result.ResourceDiff)
	if fullLog {
		logResources(node.Name, "allocatable", node.Status.Allocatable)
		logResources(template.Name, "allocatable", template.Status.Allocatable)
		logResources(node.Name, "capacity", node.Status.Capacity)
		logResources(template.Name, "capacity", template.Status.Capacity)
	}
	return result, nil
}

func logResources(id, class string, rl apiv1.ResourceList) {
	klog.Infof("%q node %s: [cpu: %s, memory: %s, ephemeral_storage: %s, pods: %s]", id, class, rl.Cpu(), rl.Memory(), rl.StorageEphemeral(), rl.Pods())
}

func init() {
	hashSeed = maphash.MakeSeed()
}
