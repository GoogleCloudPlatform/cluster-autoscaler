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

package daemonsetmutation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestMutationCache_Get(t *testing.T) {
	testCases := []struct {
		name      string
		ds        *appsv1.DaemonSet
		setCache  bool
		cacheUID  types.UID
		cachedPod *apiv1.Pod
		cachedGen int64
		expired   bool
		wantPod   *apiv1.Pod
		wantStale bool
	}{
		{
			name:      "empty UID returns stale",
			ds:        &appsv1.DaemonSet{},
			wantPod:   nil,
			wantStale: true,
		},
		{
			name:      "cache miss returns stale",
			ds:        &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 1}},
			wantPod:   nil,
			wantStale: true,
		},
		{
			name:      "cache hit returns valid pod",
			ds:        &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 1}},
			setCache:  true,
			cacheUID:  "ds-1",
			cachedPod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			cachedGen: 1,
			wantPod:   &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			wantStale: false,
		},
		{
			name:      "generation change returns stale",
			ds:        &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 2}},
			setCache:  true,
			cacheUID:  "ds-1",
			cachedPod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			cachedGen: 1,
			wantPod:   nil,
			wantStale: true,
		},
		{
			name:      "ttl expired returns pod and stale",
			ds:        &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 1}},
			setCache:  true,
			cacheUID:  "ds-1",
			cachedPod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			cachedGen: 1,
			expired:   true,
			wantPod:   &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			wantStale: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMutationCache()
			if tc.setCache {
				err := c.Set(tc.cacheUID, tc.cachedGen, tc.cachedPod)
				assert.NoError(t, err)
				if tc.expired {
					entry := c.items[tc.cacheUID]
					entry.expiresAt = time.Now().Add(-1 * time.Minute)
					c.items[tc.cacheUID] = entry
				}
			}

			var uid types.UID
			var gen int64
			if tc.ds != nil {
				uid = tc.ds.UID
				gen = tc.ds.Generation
			}
			gotPod, gotStale := c.Get(uid, gen)
			assert.Equal(t, tc.wantPod, gotPod)
			assert.Equal(t, tc.wantStale, gotStale)
		})
	}
}

func TestMutationCache_Remove(t *testing.T) {
	c := NewMutationCache()
	uid := types.UID("ds-1")
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: uid, Generation: 1}}

	err := c.Set(uid, 1, &apiv1.Pod{})
	assert.NoError(t, err)
	_, stale := c.Get(ds.UID, ds.Generation)
	assert.False(t, stale)

	c.Remove(uid)
	_, stale = c.Get(ds.UID, ds.Generation)
	assert.True(t, stale)
}

func TestRandomJitter(t *testing.T) {
	// Test with zero jitter
	assert.Equal(t, time.Duration(0), randomJitter(0))
	assert.Equal(t, time.Duration(0), randomJitter(-10*time.Second))

	// Test with positive jitter
	jitter := 10 * time.Second
	for i := 0; i < 100; i++ {
		res := randomJitter(jitter)
		assert.True(t, res >= -jitter && res < jitter, "jitter %v out of range [-%v, %v)", res, jitter, jitter)
	}
}

func TestMutationCache_Jitter(t *testing.T) {
	c := NewMutationCache()
	uid := types.UID("ds-1")
	pod := &apiv1.Pod{}

	before := time.Now()
	err := c.Set(uid, 1, pod)
	assert.NoError(t, err)

	entry, ok := c.items[uid]
	assert.True(t, ok)

	expected := before.Add(podMutationCacheTTL)
	assert.WithinDuration(t, expected, entry.expiresAt, podMutationCacheJitter+2*time.Second)
}

func TestMutationCache_DeepCopy(t *testing.T) {
	c := NewMutationCache()
	uid := types.UID("ds-1")
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "original",
			Labels: map[string]string{"key": "value"},
		},
	}

	// Verify Set copies the input pod.
	err := c.Set(uid, 1, pod)
	assert.NoError(t, err)
	pod.Labels["key"] = "mutated-after-set"

	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: uid, Generation: 1}}
	gotPod1, stale := c.Get(ds.UID, ds.Generation)
	assert.False(t, stale)
	assert.Equal(t, "value", gotPod1.Labels["key"], "mutating the original pod after Set should not affect the cache")
	assert.NotSame(t, pod, gotPod1)

	// Verify Get returns a copy (mutating the returned pod does not affect the cache).
	gotPod1.Labels["key"] = "mutated-after-get"
	gotPod2, _ := c.Get(ds.UID, ds.Generation)
	assert.Equal(t, "value", gotPod2.Labels["key"], "mutating the retrieved pod should not affect the cache")
	assert.NotSame(t, gotPod1, gotPod2)
}

func TestMutationCache_Set_NilPod(t *testing.T) {
	c := NewMutationCache()
	err := c.Set("ds-1", 1, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pod cannot be nil")
}
