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
	"context"
	"fmt"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	podutil "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	podMutationAPITimeout = 30 * time.Second
	// defaultWorkerCount is the default number of concurrent mutation resolution workers.
	defaultWorkerCount = 10
	// errorRetryBaseDelay is the initial backoff delay for retrying failed mutation resolutions.
	errorRetryBaseDelay = 1 * time.Minute
	// errorRetryMaxDelay is the maximum backoff delay for retrying failed mutation resolutions.
	errorRetryMaxDelay = 5 * time.Minute
)

// Controller handles background dry-run API requests for DaemonSets.
type Controller struct {
	mutationCache *MutationCache
	dsInformer    cache.SharedIndexInformer
	queue         workqueue.TypedRateLimitingInterface[string]
	resolver      fakepods.Resolver

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewController returns a new Controller instance.
func NewController(ctx context.Context, mutationCache *MutationCache, resolver fakepods.Resolver, informerFactory informers.SharedInformerFactory) *Controller {
	ctx, cancel := context.WithCancel(ctx)
	c := &Controller{
		mutationCache: mutationCache,
		dsInformer:    informerFactory.Apps().V1().DaemonSets().Informer(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](errorRetryBaseDelay, errorRetryMaxDelay),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "daemonset_mutation",
			},
		),
		resolver: resolver,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Register DaemonSet event handlers.
	c.registerInformer()

	// Shut down workqueue on context cancellation to prevent goroutine leaks & synctest deadlocks.
	context.AfterFunc(ctx, func() {
		c.queue.ShutDown()
	})

	return c
}

// Start launches worker goroutines for the controller.
func (c *Controller) Start() {
	for i := 0; i < defaultWorkerCount; i++ {
		c.wg.Add(1)
		go c.runWorker()
	}
}

// CleanUp synchronously shuts down the workqueue and waits for workers to exit.
func (c *Controller) CleanUp() {
	c.cancel()
	c.queue.ShutDown() // Synchronous shutdown guarantees workers unblock immediately before wg.Wait()
	c.wg.Wait()
}

// Enqueue queues a DaemonSet for background mutation resolution.
func (c *Controller) Enqueue(ds *appsv1.DaemonSet) {
	if c.resolver == nil || ds == nil {
		return
	}

	key, err := cache.MetaNamespaceKeyFunc(ds)
	if err != nil {
		klog.Errorf("[ds mutation] Failed to get key for DaemonSet %s/%s: %v", ds.Namespace, ds.Name, err)
		return
	}
	c.queue.Add(key)
}

// Refresh triggers background mutation if cache entry is missing, modified, or TTL-expired.
func (c *Controller) Refresh(ds *appsv1.DaemonSet) {
	if _, stale := c.mutationCache.Get(ds.UID, ds.Generation); stale {
		c.Enqueue(ds)
	}
}

// registerInformer registers AddFunc, UpdateFunc, and DeleteFunc handlers on the informer.
func (c *Controller) registerInformer() {
	c.dsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if ds, ok := obj.(*appsv1.DaemonSet); ok {
				c.Refresh(ds)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldDS, ok1 := oldObj.(*appsv1.DaemonSet)
			newDS, ok2 := newObj.(*appsv1.DaemonSet)
			if ok1 && ok2 && oldDS.Generation == newDS.Generation {
				return
			}
			if ok2 {
				c.Refresh(newDS)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if ds, ok := obj.(*appsv1.DaemonSet); ok {
				c.remove(ds.UID)
			} else if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if ds, ok := tombstone.Obj.(*appsv1.DaemonSet); ok {
					c.remove(ds.UID)
				}
			}
		},
	})
}

func (c *Controller) runWorker() {
	defer c.wg.Done()
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.resolveMutation(key); err != nil {
		klog.Errorf("[ds mutation] Failed to process mutation for key %q (will retry): %v", key, err)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

// resolveMutation performs the dry-run pod mutation for a DaemonSet and updates the cache.
func (c *Controller) resolveMutation(key string) error {
	if c.resolver == nil {
		return nil
	}

	ds, err := c.getDaemonSet(key)
	if err != nil {
		return err
	}
	if ds == nil {
		return nil
	}

	// Check if the cache already contains a non-stale entry for this generation.
	// This helps avoid unneeded dry-run API calls if we already resolved it recently.
	if _, stale := c.mutationCache.Get(ds.UID, ds.Generation); !stale {
		return nil
	}

	ctx, cancel := context.WithTimeout(c.ctx, podMutationAPITimeout)
	defer cancel()

	templateCopy := ds.Spec.Template.DeepCopy()
	start := time.Now()
	updatedPod, err := c.resolver.Resolve(ctx, ds.Namespace, templateCopy)
	observeDryRunResolution(err, time.Since(start))

	if err != nil {
		return fmt.Errorf("resolving dryrun mutation for %s/%s (gen: %d): %w", ds.Namespace, ds.Name, ds.Generation, err)
	}

	if changed, oldReq, newReq := resourcesChanged(templateCopy, updatedPod); changed {
		klog.V(4).Infof("[ds mutation] Successfully resolved dry-run mutation for DaemonSet %s/%s (gen: %d): resources changed. Old: %v, New: %v", ds.Namespace, ds.Name, ds.Generation, oldReq, newReq)
	}
	if err := c.mutationCache.Set(ds.UID, ds.Generation, updatedPod); err != nil {
		klog.Errorf("[ds mutation] Failed to cache mutation for DaemonSet %s/%s (gen: %d): %v", ds.Namespace, ds.Name, ds.Generation, err)
	}
	return nil
}

func (c *Controller) getDaemonSet(key string) (*appsv1.DaemonSet, error) {
	obj, exists, err := c.dsInformer.GetIndexer().GetByKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get DaemonSet from store: %w", err)
	}
	if !exists {
		return nil, nil
	}
	ds, ok := obj.(*appsv1.DaemonSet)
	if !ok {
		return nil, fmt.Errorf("object in store is not a DaemonSet: %T", obj)
	}
	return ds, nil
}

func resourcesChanged(template *apiv1.PodTemplateSpec, updatedPod *apiv1.Pod) (bool, apiv1.ResourceList, apiv1.ResourceList) {
	if template == nil || updatedPod == nil {
		return true, nil, nil
	}
	originalPod := podutil.GetPodFromTemplate(template)
	originalRequests := podutil.PodRequests(originalPod)
	updatedRequests := podutil.PodRequests(updatedPod)
	return !quota.Equals(originalRequests, updatedRequests), originalRequests, updatedRequests
}

// remove purges cache entries for a deleted DaemonSet UID.
func (c *Controller) remove(dsUID types.UID) {
	c.mutationCache.Remove(dsUID)
}
