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
		name             string
		ds               *appsv1.DaemonSet
		setCache         bool
		cacheUID         types.UID
		cachedPod        *apiv1.Pod
		cachedGen        int64
		expired          bool
		wantPod          *apiv1.Pod
		wantNeedsRefresh bool
	}{
		{
			name:             "empty UID returns needsRefresh",
			ds:               &appsv1.DaemonSet{},
			wantPod:          nil,
			wantNeedsRefresh: true,
		},
		{
			name:             "cache miss returns needsRefresh",
			ds:               &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 1}},
			wantPod:          nil,
			wantNeedsRefresh: true,
		},
		{
			name:             "cache hit returns valid pod",
			ds:               &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 1}},
			setCache:         true,
			cacheUID:         "ds-1",
			cachedPod:        &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			cachedGen:        1,
			wantPod:          &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			wantNeedsRefresh: false,
		},
		{
			name:             "generation change returns needsRefresh",
			ds:               &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 2}},
			setCache:         true,
			cacheUID:         "ds-1",
			cachedPod:        &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			cachedGen:        1,
			wantPod:          nil,
			wantNeedsRefresh: true,
		},
		{
			name:             "ttl expired returns pod and needsRefresh",
			ds:               &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "ds-1", Generation: 1}},
			setCache:         true,
			cacheUID:         "ds-1",
			cachedPod:        &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			cachedGen:        1,
			expired:          true,
			wantPod:          &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cached-pod"}},
			wantNeedsRefresh: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMutationCache()
			if tc.setCache {
				c.Set(tc.cacheUID, tc.cachedGen, tc.cachedPod)
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
			gotPod, gotNeedsRefresh := c.Get(uid, gen)
			assert.Equal(t, tc.wantPod, gotPod)
			assert.Equal(t, tc.wantNeedsRefresh, gotNeedsRefresh)
		})
	}
}

func TestMutationCache_Remove(t *testing.T) {
	c := NewMutationCache()
	uid := types.UID("ds-1")
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: uid, Generation: 1}}

	c.Set(uid, 1, &apiv1.Pod{})
	_, needsRefresh := c.Get(ds.UID, ds.Generation)
	assert.False(t, needsRefresh)

	c.Remove(uid)
	_, needsRefresh = c.Get(ds.UID, ds.Generation)
	assert.True(t, needsRefresh)
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
	c.Set(uid, 1, pod)

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
	c.Set(uid, 1, pod)
	pod.Labels["key"] = "mutated-after-set"

	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: uid, Generation: 1}}
	gotPod1, needsRefresh := c.Get(ds.UID, ds.Generation)
	assert.False(t, needsRefresh)
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
	uid := types.UID("ds-1")
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: uid, Generation: 1}}

	c.Set(uid, 1, nil)

	gotPod, needsRefresh := c.Get(ds.UID, ds.Generation)
	assert.False(t, needsRefresh)
	assert.Nil(t, gotPod)
}
