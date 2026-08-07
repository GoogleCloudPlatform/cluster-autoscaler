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
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	podutil "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
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

func TestController_ResolveMutation(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	key, _ := cache.MetaNamespaceKeyFunc(ds)

	tests := []struct {
		name               string
		setupStore         bool
		resolver           fakepods.Resolver
		key                string
		expectedStale      bool
		expectedPod        bool
		expectedOverhead   string
		expectedCacheEmpty bool
	}{
		{
			name:             "success with overhead",
			setupStore:       true,
			resolver:         resolverWithOverhead("1500m"),
			key:              key,
			expectedStale:    false,
			expectedPod:      true,
			expectedOverhead: "1500m",
		},
		{
			name:             "success no change",
			setupStore:       true,
			resolver:         resolverWithoutChange(),
			key:              key,
			expectedStale:    false,
			expectedPod:      true,
			expectedOverhead: "",
		},
		{
			name:          "resolver error (fallback)",
			setupStore:    true,
			resolver:      resolverWithError(assert.AnError),
			key:           key,
			expectedStale: false,
			expectedPod:   false,
		},
		{
			name:          "nil resolver",
			setupStore:    true,
			resolver:      nil,
			key:           key,
			expectedStale: true,
			expectedPod:   false,
		},
		{
			name:               "ds not exists",
			setupStore:         false,
			resolver:           resolverWithoutChange(),
			key:                "default/non-existent",
			expectedCacheEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutationCache := NewMutationCache()
			ctrl := NewController(context.Background(), mutationCache, tt.resolver, testInformerFactory())
			t.Cleanup(ctrl.CleanUp)

			if tt.setupStore {
				err := ctrl.dsInformer.GetStore().Add(ds)
				assert.NoError(t, err)
			}

			err := ctrl.resolveMutation(tt.key)
			assert.NoError(t, err)

			if tt.expectedCacheEmpty {
				assert.Empty(t, mutationCache.items)
			} else {
				pod, stale := mutationCache.Get(ds.UID, ds.Generation)
				assert.Equal(t, tt.expectedStale, stale)
				if tt.expectedPod {
					assert.NotNil(t, pod)
					if tt.expectedOverhead != "" {
						assert.Equal(t, resource.MustParse(tt.expectedOverhead), pod.Spec.Overhead[apiv1.ResourceCPU])
					} else {
						assert.Empty(t, pod.Spec.Overhead)
					}
				} else {
					assert.Nil(t, pod)
				}
			}
		})
	}
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

func TestResourcesChanged(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	template := &ds.Spec.Template
	pod := podutil.GetPodFromTemplate(template)
	podWithOverhead := mutatePodOverhead(template, "100m")

	tests := []struct {
		name            string
		template        *apiv1.PodTemplateSpec
		pod             *apiv1.Pod
		expectedChanged bool
		assertReqs      func(t *testing.T, oldReq, newReq apiv1.ResourceList)
	}{
		{
			name:            "nil inputs",
			template:        nil,
			pod:             nil,
			expectedChanged: true,
		},
		{
			name:            "nil pod",
			template:        &apiv1.PodTemplateSpec{},
			pod:             nil,
			expectedChanged: true,
		},
		{
			name:            "nil template",
			template:        nil,
			pod:             &apiv1.Pod{},
			expectedChanged: true,
		},
		{
			name:            "no change",
			template:        template,
			pod:             pod,
			expectedChanged: false,
		},
		{
			name:            "overhead added (changed)",
			template:        template,
			pod:             podWithOverhead,
			expectedChanged: true,
			assertReqs: func(t *testing.T, oldReq, newReq apiv1.ResourceList) {
				assert.NotEqual(t, oldReq, newReq)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed, oldReq, newReq := resourcesChanged(tt.template, tt.pod)
			assert.Equal(t, tt.expectedChanged, changed)
			if tt.assertReqs != nil {
				tt.assertReqs(t, oldReq, newReq)
			}
		})
	}
}

func TestDryRunResolver_Resolve(t *testing.T) {
	isController := true
	blockOwnerDeletion := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "apps/v1",
		Kind:               "DaemonSet",
		Name:               "test-ds",
		UID:                "test-uid",
		Controller:         &isController,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	tests := []struct {
		name                 string
		template             *apiv1.PodTemplateSpec
		expectedErr          string
		expectedGenerateName string
		expectedOwnerRefs    []metav1.OwnerReference
		expectedDryRun       bool
	}{
		{
			name: "success with owner reference",
			template: &apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{ownerRef},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{{Name: "main"}},
				},
			},
			expectedGenerateName: "test-ds-",
			expectedOwnerRefs:    []metav1.OwnerReference{ownerRef},
			expectedDryRun:       true,
		},
		{
			name: "success without owner reference",
			template: &apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{{Name: "main"}},
				},
			},
			expectedGenerateName: "ds-mutation-dryrun-",
			expectedOwnerRefs:    nil,
			expectedDryRun:       true,
		},
		{
			name:        "nil template",
			template:    nil,
			expectedErr: "template is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset()
			var createdPod *apiv1.Pod
			var createOptions metav1.CreateOptions
			fakeClient.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				createAction := action.(clienttesting.CreateActionImpl)
				createOptions = createAction.GetCreateOptions()
				createdPod = createAction.GetObject().(*apiv1.Pod)
				return true, createdPod, nil
			})

			resolver := NewDryRunResolver(fakeClient)
			pod, err := resolver.Resolve(context.Background(), "default", tt.template)

			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				assert.Nil(t, pod)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, pod)
				assert.Equal(t, tt.expectedGenerateName, pod.GenerateName)
				assert.Equal(t, tt.expectedOwnerRefs, pod.OwnerReferences)
				if tt.expectedDryRun {
					assert.Equal(t, []string{metav1.DryRunAll}, createOptions.DryRun)
				}
			}
		})
	}
}

func TestController_ResolveMutation_SetsOwnerReference(t *testing.T) {
	ds, _ := setUpTestPodAndDS()
	mutationCache := NewMutationCache()

	var capturedTemplate *apiv1.PodTemplateSpec
	resolver := &fakePodResolver{
		resolveFunc: func(template *apiv1.PodTemplateSpec) (*apiv1.Pod, error) {
			capturedTemplate = template
			return podutil.GetPodFromTemplate(template), nil
		},
	}
	ctrl := NewController(context.Background(), mutationCache, resolver, testInformerFactory())
	t.Cleanup(ctrl.CleanUp)

	err := ctrl.dsInformer.GetStore().Add(ds)
	assert.NoError(t, err)

	ctrl.Enqueue(ds)
	key, _ := cache.MetaNamespaceKeyFunc(ds)
	err = ctrl.resolveMutation(key)
	assert.NoError(t, err)

	assert.NotNil(t, capturedTemplate)
	assert.Len(t, capturedTemplate.OwnerReferences, 1)
	assert.Equal(t, ds.Name, capturedTemplate.OwnerReferences[0].Name)
	assert.Equal(t, ds.UID, capturedTemplate.OwnerReferences[0].UID)
	assert.Equal(t, "DaemonSet", capturedTemplate.OwnerReferences[0].Kind)
	assert.Equal(t, "apps/v1", capturedTemplate.OwnerReferences[0].APIVersion)
	assert.True(t, *capturedTemplate.OwnerReferences[0].Controller)
}

func TestFormatResourceList(t *testing.T) {
	tests := []struct {
		name     string
		input    apiv1.ResourceList
		expected string
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: "{}",
		},
		{
			name:     "empty input",
			input:    apiv1.ResourceList{},
			expected: "{}",
		},
		{
			name: "single resource",
			input: apiv1.ResourceList{
				apiv1.ResourceCPU: resource.MustParse("100m"),
			},
			expected: "{cpu: 100m}",
		},
		{
			name: "multiple resources",
			input: apiv1.ResourceList{
				apiv1.ResourceMemory: resource.MustParse("100Mi"),
				apiv1.ResourceCPU:    resource.MustParse("200m"),
			},
			expected: "{cpu: 200m, memory: 100Mi}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := formatResourceList(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
