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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchCacheSharesConcurrentFetches(t *testing.T) {
	cache := newFetchCache[string](8)
	var fetches atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	// One caller starts the fetch and blocks in it.
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		if _, err := cache.get(context.Background(), "key", func(context.Context) (string, error) {
			fetches.Add(1)
			close(started)
			<-release
			return "value", nil
		}); err != nil {
			t.Errorf("first get: %v", err)
		}
	}()
	<-started

	// The others arrive while it is in flight and must join it.
	const waiters = 15
	var entering, wg sync.WaitGroup
	results := make([]string, waiters)
	for i := 0; i < waiters; i++ {
		entering.Add(1)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			entering.Done()
			v, err := cache.get(context.Background(), "key", func(context.Context) (string, error) {
				fetches.Add(1)
				return "value", nil
			})
			if err != nil {
				t.Errorf("get: %v", err)
			}
			results[i] = v
		}(i)
	}
	entering.Wait()
	time.Sleep(50 * time.Millisecond) // let the waiters reach the in-flight fetch
	close(release)
	<-firstDone
	wg.Wait()

	if got := fetches.Load(); got != 1 {
		t.Errorf("expected 1 fetch for %d concurrent misses, got %d", waiters+1, got)
	}
	for i, v := range results {
		if v != "value" {
			t.Errorf("waiter %d got %q, want %q", i, v, "value")
		}
	}
}

func TestFetchCacheDoesNotCacheErrors(t *testing.T) {
	cache := newFetchCache[string](8)
	calls := 0
	fetch := func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("transient")
		}
		return "value", nil
	}

	if _, err := cache.get(context.Background(), "key", fetch); err == nil {
		t.Fatal("expected first fetch to fail")
	}
	for i := 0; i < 2; i++ {
		v, err := cache.get(context.Background(), "key", fetch)
		if err != nil || v != "value" {
			t.Fatalf("get #%d = %q, %v; want value, nil", i+2, v, err)
		}
	}
	if calls != 2 {
		t.Errorf("expected 2 fetches (1 failed, 1 cached), got %d", calls)
	}
}

func TestFetchCacheIsBounded(t *testing.T) {
	cache := newFetchCache[int](2)
	fetches := map[string]int{}
	get := func(key string) {
		if _, err := cache.get(context.Background(), key, func(context.Context) (int, error) {
			fetches[key]++
			return 0, nil
		}); err != nil {
			t.Fatalf("get(%q): %v", key, err)
		}
	}
	get("a")
	get("b")
	get("c") // evicts a
	get("a")
	if fetches["a"] != 2 || fetches["b"] != 1 || fetches["c"] != 1 {
		t.Errorf("expected a to be refetched after eviction, got %v", fetches)
	}
}

func TestFetchCacheWaiterHonorsContext(t *testing.T) {
	cache := newFetchCache[string](8)
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	go func() {
		_, _ = cache.get(context.Background(), "key", func(context.Context) (string, error) {
			close(started)
			<-release
			return "value", nil
		})
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.get(ctx, "key", func(context.Context) (string, error) {
		t.Error("second caller must join the in-flight fetch, not start one")
		return "", nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled while waiting, got %v", err)
	}
}

func TestFetchCacheFetchOutlivesFirstCallerContext(t *testing.T) {
	cache := newFetchCache[string](8)
	started := make(chan struct{})
	release := make(chan struct{})
	var fetchCtxErr error

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := cache.get(firstCtx, "key", func(ctx context.Context) (string, error) {
			close(started)
			<-release
			fetchCtxErr = ctx.Err()
			return "value", nil
		})
		firstDone <- err
	}()
	<-started

	// The first caller gives up while its fetch is in flight.
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller: got %v, want context.Canceled", err)
	}

	// A second caller joins the same fetch and gets its result.
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		v, err := cache.get(context.Background(), "key", func(context.Context) (string, error) {
			t.Error("second caller must join the in-flight fetch, not start one")
			return "", nil
		})
		if err != nil || v != "value" {
			t.Errorf("second caller: got %q, %v; want value, nil", v, err)
		}
	}()
	close(release)
	<-secondDone

	if fetchCtxErr != nil {
		t.Errorf("fetch context ended with the first caller: %v", fetchCtxErr)
	}
}
