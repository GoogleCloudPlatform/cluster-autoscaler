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

package reconciler

import (
	"context"

	"golang.org/x/sync/singleflight"
	"k8s.io/utils/lru"
)

// fetchCache memoizes successful fetches by key in a bounded LRU. Concurrent
// callers that miss on the same key share one in-flight fetch instead of each
// calling the backend: the reconciler builds Nodes from many parallel workers
// that mostly share a few templates and machine types. Failed fetches are not
// cached, so the next caller retries.
type fetchCache[V any] struct {
	values *lru.Cache // thread-safe
	group  singleflight.Group
}

func newFetchCache[V any](size int) *fetchCache[V] {
	return &fetchCache[V]{values: lru.New(size)}
}

// get returns the cached value for key, fetching it with fetch on a miss.
//
// The fetch is shared by every caller waiting on key, so it runs with a context
// detached from the caller's cancellation: otherwise the first caller's ctx
// ending would fail the fetch for all of them. A caller whose ctx ends stops
// waiting and gets ctx.Err(); the fetch completes and its result is cached for
// the others.
func (c *fetchCache[V]) get(ctx context.Context, key string, fetch func(context.Context) (V, error)) (V, error) {
	if v, ok := c.values.Get(key); ok {
		return v.(V), nil
	}
	fetchCtx := context.WithoutCancel(ctx)
	ch := c.group.DoChan(key, func() (any, error) {
		v, err := fetch(fetchCtx)
		if err == nil {
			c.values.Add(key, v)
		}
		return v, err
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			var zero V
			return zero, res.Err
		}
		return res.Val.(V), nil
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	}
}
