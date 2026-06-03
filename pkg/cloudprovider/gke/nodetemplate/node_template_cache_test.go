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

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	clock "k8s.io/utils/clock/testing"
)

var testStartTime = time.Date(2025, 1, 1, 1, 1, 1, 1, time.UTC)

func TestBuildKeyForNAPProducesConsistentHash(t *testing.T) {
	spec1 := gkeclient.NodePoolSpec{
		LocalSSDConfig: &gkeclient.LocalSSDConfig{
			LocalSsdCount:                  1,
			EphemeralStorageConfig:         &container.EphemeralStorageConfig{},
			EphemeralStorageLocalSsdConfig: &container.EphemeralStorageLocalSsdConfig{},
			LocalNvmeSsdBlockConfig:        &container.LocalNvmeSsdBlockConfig{},
		},
		Accelerators: []*container.AcceleratorConfig{
			{
				AcceleratorType:  "",
				AcceleratorCount: 0,
				GpuPartitionSize: "",
				GpuSharingConfig: &container.GPUSharingConfig{},
			},
		},
		MachineType: "some-machine-type",
	}

	// Making a shallow copy and re-creating local SSD config (different pointer, same value)
	spec2 := spec1
	spec2.LocalSSDConfig = &gkeclient.LocalSSDConfig{
		LocalSsdCount:                  1,
		EphemeralStorageConfig:         &container.EphemeralStorageConfig{},
		EphemeralStorageLocalSsdConfig: &container.EphemeralStorageLocalSsdConfig{},
		LocalNvmeSsdBlockConfig:        &container.LocalNvmeSsdBlockConfig{},
	}

	hash1 := BuildKeyForNAP(&spec1, "", "", "")
	hash2 := BuildKeyForNAP(&spec2, "", "", "")

	assert.Equal(t, spec1, spec2) // safeguard not to miss any changes in tests
	assert.Equal(t, hash1, hash2)
}

func TestBuildKeyForNAPDifferentPerZone(t *testing.T) {
	spec := gkeclient.NodePoolSpec{}

	hash1 := BuildKeyForNAP(&spec, "", "", "A")
	hash2 := BuildKeyForNAP(&spec, "", "", "B")

	assert.NotEqual(t, hash1, hash2)
}

func TestNodeTemplateCache(t *testing.T) {
	cache := NewCache()
	node1 := apiv1.Node{}
	node1.ObjectMeta = metav1.ObjectMeta{
		Name: "node1",
	}
	node2 := apiv1.Node{}
	node2.ObjectMeta = metav1.ObjectMeta{
		Name: "node1",
	}
	nodes := []apiv1.Node{node1, node2}
	for i, node := range nodes {
		key := strconv.Itoa(i)
		cache.Add(key, &node, LongTTL)

		template, _ := cache.Get(key)
		if ok := cmp.Equal(template, &node); !ok {
			t.Errorf("get(%v) = %v, want %v", key, template, &node)
		}
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		name           string
		template       apiv1.ResourceList
		node           apiv1.ResourceList
		templateLabels map[string]string
		nodeLabels     map[string]string
		missingLabels  map[string]bool
		diff           map[string]float64
		err            string
	}{
		{
			name:     "Template and node have the same resource",
			template: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("4")},
			node:     apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("5")},
			diff:     map[string]float64{"cpu": -0.2},
		},
		{
			name:     "Empty template",
			template: apiv1.ResourceList{},
			node:     apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("4")},
			diff:     map[string]float64{},
		},
		{
			name:     "Node resource is missing",
			template: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("4"), apiv1.ResourceMemory: resource.MustParse("10")},
			node:     apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("4")},
			diff:     map[string]float64{"cpu": 0},
			err:      "resource memory is not present on the node",
		},
		{
			name:           "Missing labels present",
			template:       apiv1.ResourceList{},
			node:           apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("4")},
			templateLabels: map[string]string{"gke.io/test-label-3": "test-val"},
			nodeLabels:     map[string]string{"gke.io/test-label-1": "test-val", "gke.io/test-label-2": "test-val", "gke.io/test-label-3": "test-val"},
			missingLabels:  map[string]bool{"gke.io/test-label-1": true, "gke.io/test-label-2": true},
			diff:           map[string]float64{},
		},
		{
			name:           "No missing label present",
			template:       apiv1.ResourceList{},
			node:           apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("4")},
			templateLabels: map[string]string{"gke.io/test-label": "test-val"},
			nodeLabels:     map[string]string{"gke.io/test-label": "test-val"},
			missingLabels:  map[string]bool{},
			diff:           map[string]float64{},
		},
	}
	cache := NewCache()
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := strconv.Itoa(i)
			template := apiv1.Node{}
			template.Name = fmt.Sprintf("template-%02d", i)
			template.Status.Allocatable = test.template
			template.Labels = test.templateLabels
			node := apiv1.Node{}
			node.Name = fmt.Sprintf("node-%02d", i)
			node.Status.Allocatable = test.node
			node.Labels = test.nodeLabels
			cache.Add(key, &template, LongTTL)

			result, err := cache.Compare(key, &node)
			if test.err != "" {
				if ok := strings.Contains(err.Error(), test.err); !ok {
					t.Errorf("Compare(%v) =\n%s:\nwant:\n%s", key, err.Error(), test.err)
					return
				}
				templateFromCache, _ := cache.Get(key)
				if templateFromCache == nil {
					t.Errorf("get(%v) = nil, want: %v", key, &template)
				}
			} else {
				if diff := cmp.Diff(test.diff, result.ResourceDiff); diff != "" {
					t.Errorf("Compare(%v) mismatch (-want +got):\n%s", key, diff)
				}
			}
			if test.missingLabels != nil {
				assert.Equal(t, test.missingLabels, result.MissingSystemLabels)
			}
		})
	}
}

func TestCacheExpiration(t *testing.T) {
	tests := []struct {
		name          string
		ttl           time.Duration
		delay         time.Duration
		shouldBeFound bool
	}{
		{
			name:          "instant get",
			ttl:           time.Minute * 2,
			delay:         0,
			shouldBeFound: true,
		},
		{
			name:          "early get",
			ttl:           time.Minute * 2,
			delay:         time.Minute * 1,
			shouldBeFound: true,
		},
		{
			name:          "late get",
			ttl:           time.Minute * 2,
			delay:         time.Minute * 3,
			shouldBeFound: false,
		},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := strconv.Itoa(i)
			template := apiv1.Node{}
			template.Name = fmt.Sprintf("template-%02d", i)
			testClock := clock.NewFakeClock(testStartTime)
			cache := NewCacheWithClock(testClock)

			cache.Add(key, &template, test.ttl)
			testClock.Sleep(test.delay)
			_, found := cache.Get(key)

			if test.shouldBeFound {
				assert.True(t, found, "Key %s unexpectedly not found in the cache.", key)
			} else {
				assert.False(t, found, "Key %s unexpectedly found in the cache.", key)
			}
		})
	}
}
