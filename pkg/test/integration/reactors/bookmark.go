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

// Package reactors provides testing utilities to inject events into fake client-go watchers.
package reactors

import (
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	k8stesting "k8s.io/client-go/testing"
)

// initialEventsEndBookmarkResourceVersion is the resource version set on the
// synthetic Bookmark object emitted by the reactor. The exact value is
// arbitrary but must be non-empty so the informer accepts the event.
const initialEventsEndBookmarkResourceVersion = "1"

// watcher is a custom watch.Interface wrapper that prevents goroutine leaks
// when used inside testing frameworks like synctest. It intercepts the underlying
// watch stream and injects a Bookmark event before forwarding other events.
type watcher struct {
	watch.Interface
	ch     chan watch.Event
	stopCh chan struct{}
	once   sync.Once
}

// Stop safely signals the background goroutine to stop and stops the underlying watcher.
func (bw *watcher) Stop() {
	bw.once.Do(func() {
		close(bw.stopCh)
		bw.Interface.Stop()
	})
}

// ResultChan returns the channel containing the injected bookmark and subsequent events.
func (bw *watcher) ResultChan() <-chan watch.Event {
	return bw.ch
}

// SimulateInitialListStreamForWatchCalls prepends a watch reactor to the given fake client.
//
// Why this is needed:
// Since Kubernetes 1.27 (client-go v0.27.0), Informers enable `SendInitialEvents: true` by default
// during the initial `ListAndWatch`. When this is enabled, the Informer's `WaitForCacheSync()`
// will block indefinitely until it receives a `watch.Bookmark` event with the
// `k8s.io/initial-events-end: "true"` annotation, which signals the end of the initial list.
//
// The standard `k8s.io/client-go/testing.ObjectTracker` (used by most `fake.Clientset`s)
// does NOT automatically generate or send this Bookmark event. If you start an Informer
// backed by an unmodified `fake.Clientset` synchronously, it will hang forever.
//
// This reactor intercepts `Watch` calls matching the given resource (use "*" to match all
// resources on a fake client) and injects a `watch.Bookmark` event before forwarding events
// from the underlying tracker. The emptyObj must be an empty instance of the concrete type
// expected by the resource's informer (e.g. &v1.Pod{} for pods); the reactor will
// automatically set the required InitialEventsAnnotationKey annotation and ResourceVersion.
//
// TODO: b/530525860 - This reactor currently only sends the Bookmark event to unblock WaitForCacheSync,
// assuming an empty initial state. For a fully correct implementation, it should first list
// all existing objects in the tracker and send them as initial Add events before sending the Bookmark.
//
// The custom `watcher` struct ensures proper synchronization during teardown so no
// goroutines are leaked, satisfying strict `synctest` constraints.
func SimulateInitialListStreamForWatchCalls(fakeClient *k8stesting.Fake, tracker k8stesting.ObjectTracker, resource string, emptyObj runtime.Object) {
	fakeClient.PrependWatchReactor(resource, func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
		gvr := action.GetResource()
		ns := action.GetNamespace()

		// Create the underlying tracker watch
		w, err := tracker.Watch(gvr, ns)
		if err != nil {
			return true, nil, err
		}

		bw := &watcher{
			Interface: w,
			ch:        make(chan watch.Event),
			stopCh:    make(chan struct{}),
		}

		bookmarkObj := emptyObj.DeepCopyObject()

		// Ensure the bookmark object has the correct annotation and resource version.
		if accessor, err := meta.Accessor(bookmarkObj); err == nil {
			annotations := accessor.GetAnnotations()
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[metav1.InitialEventsAnnotationKey] = "true"
			accessor.SetAnnotations(annotations)
			accessor.SetResourceVersion(initialEventsEndBookmarkResourceVersion)
		}

		// Background goroutine to inject the Bookmark and proxy events.
		// Exits cleanly if Stop() is called or the underlying watch closes.
		go func() {
			defer close(bw.ch)
			// Inject the Bookmark event
			select {
			case bw.ch <- watch.Event{Type: watch.Bookmark, Object: bookmarkObj}:
			case <-bw.stopCh:
				return
			}
			// Forward all underlying events
			for {
				select {
				case e, ok := <-w.ResultChan():
					if !ok {
						return
					}
					select {
					case bw.ch <- e:
					case <-bw.stopCh:
						return
					}
				case <-bw.stopCh:
					return
				}
			}
		}()

		return true, bw, nil
	})
}
