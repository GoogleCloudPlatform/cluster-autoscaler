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
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

func TestInjectDaemonSetMutations_NilOrEmptyInputs(t *testing.T) {
	ds, nodeInfos := setUpTestPodAndDS()
	cache := NewMutationCache()
	ctrl := NewController(context.Background(), cache, nil, testInformerFactory())
	injector := NewInjector(cache, ctrl)

	// Empty nodeInfos
	got := injector.InjectDaemonSetMutations(map[string]*framework.NodeInfo{}, []*appsv1.DaemonSet{ds})
	assert.Empty(t, got)

	// Empty daemonsets
	got = injector.InjectDaemonSetMutations(nodeInfos, nil)
	assert.Equal(t, nodeInfos, got)

	// Nil cache or controller
	nilInjector := NewInjector(nil, nil)
	got = nilInjector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})
	assert.Equal(t, nodeInfos, got)
}

func TestInjectDaemonSetMutations_NoMatchingDaemonSet(t *testing.T) {
	ds, nodeInfos := setUpTestPodAndDS()
	// Remove owner references so the pod doesn't match the DaemonSet
	pod := nodeInfos["test-group"].Pods()[0].Pod
	pod.OwnerReferences = nil

	cache := NewMutationCache()
	mutatedPod := pod.DeepCopy()
	mutatedPod.Spec.Overhead = apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("1000m")}
	cache.Set(ds.UID, ds.Generation, mutatedPod)

	ctrl := NewController(context.Background(), cache, nil, testInformerFactory())
	injector := NewInjector(cache, ctrl)

	got := injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

	assert.Equal(t, nodeInfos, got)
	assert.Nil(t, got["test-group"].Pods()[0].Pod.Spec.Overhead)
}

func TestInjectDaemonSetMutations_SuccessfulInjection(t *testing.T) {
	ds, nodeInfos := setUpTestPodAndDS()
	cache := NewMutationCache()

	mutatedPodTemplate := nodeInfos["test-group"].Pods()[0].Pod.DeepCopy()
	mutatedPodTemplate.Spec.Overhead = apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("1500m")}
	mutatedPodTemplate.Labels = map[string]string{"injected-label": "yes"}
	mutatedPodTemplate.Annotations = map[string]string{"injected-annotation": "yes"}

	cache.Set(ds.UID, ds.Generation, mutatedPodTemplate)

	ctrl := NewController(context.Background(), cache, nil, testInformerFactory())
	injector := NewInjector(cache, ctrl)

	got := injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

	assert.NotEqual(t, nodeInfos, got)
	gotPod := got["test-group"].Pods()[0].Pod

	// Check that mutations from the cache are applied
	assert.Equal(t, resource.MustParse("1500m"), gotPod.Spec.Overhead[apiv1.ResourceCPU])
	assert.Equal(t, "yes", gotPod.Labels["injected-label"])
	assert.Equal(t, "yes", gotPod.Annotations["injected-annotation"])

	// Check that identity and original properties are preserved
	originalPod := nodeInfos["test-group"].Pods()[0].Pod
	assert.Equal(t, originalPod.Name, gotPod.Name)
	assert.Equal(t, originalPod.Namespace, gotPod.Namespace)
	assert.Equal(t, originalPod.UID, gotPod.UID)
	assert.Equal(t, originalPod.Spec.NodeName, gotPod.Spec.NodeName)
}

func TestInjectDaemonSetMutations_CacheMissTriggersRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ds, nodeInfos := setUpTestPodAndDS()
		cache := NewMutationCache()
		resolver := resolverWithOverhead("1000m")

		ctrl := NewController(context.Background(), cache, resolver, testInformerFactory())
		err := ctrl.dsInformer.GetStore().Add(ds)
		assert.NoError(t, err)
		ctrl.Start()
		t.Cleanup(ctrl.CleanUp)

		injector := NewInjector(cache, ctrl)

		// First call (cache is cold)
		got := injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

		// No overhead should be injected yet
		assert.Nil(t, got["test-group"].Pods()[0].Pod.Spec.Overhead)

		// Wait for the background controller to process the queued job
		synctest.Wait()

		// Second call after background resolution is complete
		got = injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

		// Now overhead should be injected
		assert.Equal(t, resource.MustParse("1000m"), got["test-group"].Pods()[0].Pod.Spec.Overhead[apiv1.ResourceCPU])
	})
}

func TestInjectDaemonSetMutations_StaleCacheServesStaleAndTriggersRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ds, nodeInfos := setUpTestPodAndDS()
		cache := NewMutationCache()

		// Set initial cache entry
		firstMutatedPod := nodeInfos["test-group"].Pods()[0].Pod.DeepCopy()
		firstMutatedPod.Spec.Overhead = apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("1000m")}
		cache.Set(ds.UID, ds.Generation, firstMutatedPod)

		// Set up resolver to return a different overhead next time
		resolver := resolverWithOverhead("2000m")

		ctrl := NewController(context.Background(), cache, resolver, testInformerFactory())
		err := ctrl.dsInformer.GetStore().Add(ds)
		assert.NoError(t, err)
		ctrl.Start()
		t.Cleanup(ctrl.CleanUp)

		injector := NewInjector(cache, ctrl)

		// Force TTL expiration of the cached entry
		expireCacheEntry(cache, ds.UID)

		// Call injector with stale cache
		got := injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

		// Should return the stale overhead
		assert.Equal(t, resource.MustParse("1000m"), got["test-group"].Pods()[0].Pod.Spec.Overhead[apiv1.ResourceCPU])

		// Wait for background controller to process the triggered refresh job
		synctest.Wait()

		// Call injector again after refresh completes
		got = injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

		// Should now return the updated overhead
		assert.Equal(t, resource.MustParse("2000m"), got["test-group"].Pods()[0].Pod.Spec.Overhead[apiv1.ResourceCPU])
	})
}

func TestInjectDaemonSetMutations_GenerationChangeInvalidatesCache(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ds, nodeInfos := setUpTestPodAndDS()
		cache := NewMutationCache()

		// Set initial cache entry
		mutatedPod := nodeInfos["test-group"].Pods()[0].Pod.DeepCopy()
		mutatedPod.Spec.Overhead = apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("1000m")}
		cache.Set(ds.UID, 1, mutatedPod)

		resolver := resolverWithOverhead("2000m")
		ctrl := NewController(context.Background(), cache, resolver, testInformerFactory())
		err := ctrl.dsInformer.GetStore().Add(ds)
		assert.NoError(t, err)
		ctrl.Start()
		t.Cleanup(ctrl.CleanUp)

		injector := NewInjector(cache, ctrl)

		// Modify DS generation
		ds.Generation = 2
		err = ctrl.dsInformer.GetStore().Update(ds)
		assert.NoError(t, err)

		// Call injector with modified generation
		got := injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

		// Should NOT return the old overhead (since generation changed, cache is invalid)
		assert.Nil(t, got["test-group"].Pods()[0].Pod.Spec.Overhead)

		// Wait for background controller to resolve new mutation
		synctest.Wait()

		// Call injector again after resolution is complete
		got = injector.InjectDaemonSetMutations(nodeInfos, []*appsv1.DaemonSet{ds})

		// Should now return the updated overhead
		assert.Equal(t, resource.MustParse("2000m"), got["test-group"].Pods()[0].Pod.Spec.Overhead[apiv1.ResourceCPU])
	})
}

func TestApplyCacheMutationToPod(t *testing.T) {
	simulatedPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "simulated-pod",
			Namespace:       "test-ns",
			UID:             types.UID("pod-uid"),
			OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: "ds", UID: "ds-uid"}},
			Annotations:     map[string]string{"original-anno": "value"},
			Labels:          map[string]string{"original-label": "value"},
		},
		Spec: apiv1.PodSpec{
			NodeName: "node-xyz",
		},
	}

	cachedPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{"injected-anno": "new-value"},
			Labels:      map[string]string{"injected-label": "new-value"},
		},
		Spec: apiv1.PodSpec{
			Overhead: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("500m")},
			NodeName: "node-ignored",
		},
	}

	got := applyCacheMutationToPod(simulatedPod, cachedPod)

	assert.Equal(t, "simulated-pod", got.Name)
	assert.Equal(t, "test-ns", got.Namespace)
	assert.Equal(t, types.UID("pod-uid"), got.UID)
	assert.Equal(t, simulatedPod.OwnerReferences, got.OwnerReferences)
	assert.Equal(t, "node-xyz", got.Spec.NodeName)
	assert.Equal(t, resource.MustParse("500m"), got.Spec.Overhead[apiv1.ResourceCPU])

	// Verify annotations and labels merged
	assert.Equal(t, "value", got.Annotations["original-anno"])
	assert.Equal(t, "new-value", got.Annotations["injected-anno"])
	assert.Equal(t, "value", got.Labels["original-label"])
	assert.Equal(t, "new-value", got.Labels["injected-label"])
}
