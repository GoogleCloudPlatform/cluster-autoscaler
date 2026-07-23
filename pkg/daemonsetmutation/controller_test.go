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
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	podutil "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestController_Enqueue_NilResolver(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, nil, testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	ctrl.Enqueue(ds)
	assert.Equal(t, 0, ctrl.queue.Len())
}

func TestController_Enqueue_NilDS(t *testing.T) {
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, resolverWithoutChange(), testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	ctrl.Enqueue(nil)
	assert.Equal(t, 0, ctrl.queue.Len())
}

func TestController_Enqueue_Deduplication(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, resolverWithoutChange(), testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	ctrl.Enqueue(ds)
	assert.Equal(t, 1, ctrl.queue.Len())

	// Enqueue again, should be deduplicated by queue
	ctrl.Enqueue(ds)
	assert.Equal(t, 1, ctrl.queue.Len())

	// Enqueue with different generation, same key, should still be 1 (if not processing)
	dsGo := ds.DeepCopy()
	dsGo.Generation = 2
	ctrl.Enqueue(dsGo)
	assert.Equal(t, 1, ctrl.queue.Len())
}

func TestController_ResolveMutation_Success(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, resolverWithOverhead("1500m"), testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	err := ctrl.dsInformer.GetStore().Add(ds)
	assert.NoError(t, err)

	ctrl.Enqueue(ds)
	key, _ := cache.MetaNamespaceKeyFunc(ds)
	err = ctrl.resolveMutation(key)

	assert.NoError(t, err)
	pod, stale := mutationCache.Get(ds.UID, ds.Generation)
	assert.False(t, stale)
	assert.NotNil(t, pod)
	assert.Equal(t, resource.MustParse("1500m"), pod.Spec.Overhead[apiv1.ResourceCPU])
}

func TestController_ResolveMutation_Success_NoChange(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, resolverWithoutChange(), testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	err := ctrl.dsInformer.GetStore().Add(ds)
	assert.NoError(t, err)

	ctrl.Enqueue(ds)
	key, _ := cache.MetaNamespaceKeyFunc(ds)
	err = ctrl.resolveMutation(key)

	assert.NoError(t, err)
	pod, stale := mutationCache.Get(ds.UID, ds.Generation)
	assert.False(t, stale)
	assert.NotNil(t, pod)
	assert.Empty(t, pod.Spec.Overhead)
}

func TestController_ResolveMutation_CachedNotStale(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()

	resolveCount := 0
	resolver := &fakePodResolver{
		resolveFunc: func(template *apiv1.PodTemplateSpec) (*apiv1.Pod, error) {
			resolveCount++
			return mutatePodOverhead(template, "1500m"), nil
		},
	}
	ctrl := NewController(context.Background(), mutationCache, resolver, testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	err := ctrl.dsInformer.GetStore().Add(ds)
	assert.NoError(t, err)

	// First time: not cached, should call resolver.Resolve.
	key, _ := cache.MetaNamespaceKeyFunc(ds)
	err = ctrl.resolveMutation(key)
	assert.NoError(t, err)
	assert.Equal(t, 1, resolveCount)

	// Second time: cached, should return early and not call resolver.Resolve.
	err = ctrl.resolveMutation(key)
	assert.NoError(t, err)
	assert.Equal(t, 1, resolveCount)
}

func TestController_ResolveMutation_Error(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, resolverWithError(assert.AnError), testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	err := ctrl.dsInformer.GetStore().Add(ds)
	assert.NoError(t, err)

	ctrl.Enqueue(ds)
	key, _ := cache.MetaNamespaceKeyFunc(ds)
	err = ctrl.resolveMutation(key)

	assert.NoError(t, err)
	pod, stale := mutationCache.Get(ds.UID, ds.Generation)
	assert.False(t, stale)
	assert.Nil(t, pod)
}

func TestController_ResolveMutation_NilResolver(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, nil, testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	err := ctrl.dsInformer.GetStore().Add(ds)
	assert.NoError(t, err)

	ctrl.Enqueue(ds)
	key, _ := cache.MetaNamespaceKeyFunc(ds)
	err = ctrl.resolveMutation(key)
	assert.NoError(t, err)
	pod, stale := mutationCache.Get(ds.UID, ds.Generation)
	assert.True(t, stale)
	assert.Nil(t, pod)
}

func TestController_ResolveMutation_JobNotExists(t *testing.T) {
	mutationCache := NewMutationCache()
	ctrl := NewController(context.Background(), mutationCache, resolverWithoutChange(), testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	err := ctrl.resolveMutation("default/non-existent")
	assert.NoError(t, err)
	assert.Empty(t, mutationCache.items)
}

func TestController_Informer_Update(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ds, _ := setUpTestPodAndDS()
		fakeClient := fake.NewSimpleClientset(ds)
		factory := informers.NewSharedInformerFactory(fakeClient, 0)

		mutationCache := NewMutationCache()
		ctrl := NewController(context.Background(), mutationCache, resolverWithOverhead("1500m"), factory)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		factory.Start(ctx.Done())
		factory.WaitForCacheSync(ctx.Done())

		ctrl.Start()
		t.Cleanup(ctrl.CleanUp)

		synctest.Wait()

		// Initial sync should resolve it
		pod, stale := mutationCache.Get(ds.UID, ds.Generation)
		assert.False(t, stale)
		assert.NotNil(t, pod)

		// Clear cache to verify if update triggers it again
		mutationCache.Remove(ds.UID)

		// Update DS with same generation (e.g. annotation change)
		dsUpdate := ds.DeepCopy()
		dsUpdate.Annotations = map[string]string{"foo": "bar"}
		_, err := fakeClient.AppsV1().DaemonSets("default").Update(context.Background(), dsUpdate, metav1.UpdateOptions{})
		assert.NoError(t, err)

		synctest.Wait()
		// Cache should still be empty (stale) because generation didn't change
		_, stale = mutationCache.Get(ds.UID, ds.Generation)
		assert.True(t, stale)

		// Update DS with new generation
		dsUpdate2 := dsUpdate.DeepCopy()
		dsUpdate2.Generation = 2
		_, err = fakeClient.AppsV1().DaemonSets("default").Update(context.Background(), dsUpdate2, metav1.UpdateOptions{})
		assert.NoError(t, err)

		synctest.Wait()
		// Cache should be updated now
		pod, stale = mutationCache.Get(dsUpdate2.UID, dsUpdate2.Generation)
		assert.False(t, stale)
		assert.NotNil(t, pod)
	})
}

func TestController_Informer_Delete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ds, _ := setUpTestPodAndDS()
		fakeClient := fake.NewSimpleClientset(ds)
		factory := informers.NewSharedInformerFactory(fakeClient, 0)

		mutationCache := NewMutationCache()
		ctrl := NewController(context.Background(), mutationCache, resolverWithOverhead("1500m"), factory)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		factory.Start(ctx.Done())
		factory.WaitForCacheSync(ctx.Done())

		ctrl.Start()
		t.Cleanup(ctrl.CleanUp)

		synctest.Wait()

		// Should be in cache
		pod, stale := mutationCache.Get(ds.UID, ds.Generation)
		assert.False(t, stale)
		assert.NotNil(t, pod)

		// Delete DS
		err := fakeClient.AppsV1().DaemonSets("default").Delete(context.Background(), ds.Name, metav1.DeleteOptions{})
		assert.NoError(t, err)

		synctest.Wait()

		// Should be removed from cache
		pod, stale = mutationCache.Get(ds.UID, ds.Generation)
		assert.True(t, stale)
		assert.Nil(t, pod)
	})
}

func TestController_InformerEvents_Lifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ds, _ := setUpTestPodAndDS()
		mutationCache := NewMutationCache()
		ctrl := NewController(context.Background(), mutationCache, resolverWithOverhead("1500m"), testInformerFactory())
		ctrl.Start()
		t.Cleanup(ctrl.CleanUp)

		err := ctrl.dsInformer.GetStore().Add(ds)
		assert.NoError(t, err)

		ctrl.Refresh(ds)
		synctest.Wait()

		pod, stale := mutationCache.Get(ds.UID, ds.Generation)
		assert.False(t, stale)
		assert.Equal(t, resource.MustParse("1500m"), pod.Spec.Overhead[apiv1.ResourceCPU])

		ctrl.remove(ds.UID)
		assert.Empty(t, mutationCache.items)
	})
}

func TestResourcesChanged_NilInputs(t *testing.T) {
	changed, _, _ := resourcesChanged(nil, nil)
	assert.True(t, changed)
	changed, _, _ = resourcesChanged(&apiv1.PodTemplateSpec{}, nil)
	assert.True(t, changed)
	changed, _, _ = resourcesChanged(nil, &apiv1.Pod{})
	assert.True(t, changed)
}

func TestResourcesChanged_ChangeAndNoChange(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	template := &ds.Spec.Template

	// No change
	pod := podutil.GetPodFromTemplate(template)
	changed, _, _ := resourcesChanged(template, pod)
	assert.False(t, changed)

	// Change (overhead added)
	podWithOverhead := mutatePodOverhead(template, "100m")
	changed, oldReq, newReq := resourcesChanged(template, podWithOverhead)
	assert.True(t, changed)
	assert.NotEqual(t, oldReq, newReq)
}

func TestDryRunPodResolver_Resolve(t *testing.T) {
	namespace := "default"
	template := &apiv1.PodTemplateSpec{
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{Name: "main"},
			},
		},
	}

	fakeClient := fake.NewSimpleClientset()
	var createOptions metav1.CreateOptions
	fakeClient.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		createAction := action.(clienttesting.CreateActionImpl)
		createOptions = createAction.GetCreateOptions()
		return true, createAction.GetObject(), nil
	})

	resolver := fakepods.NewDryRunResolver(fakeClient)
	_, err := resolver.Resolve(context.Background(), namespace, template)
	assert.NoError(t, err)
	assert.Equal(t, []string{metav1.DryRunAll}, createOptions.DryRun)
}
