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

package reactors

import (
	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

var podsResource = apiv1.SchemeGroupVersion.WithResource("pods")

// PrependMockEvictionReactor makes the fake clientset honour pod evictions
// (create on the pods/eviction subresource) by deleting the evicted pod, the
// way the real eviction API does. Without it the fake client accepts the
// eviction but keeps the pod, so a node drain never completes.
//
// The pod is removed through the object tracker rather than through the
// clientset: reactors run under the clientset's lock, so a nested client call
// would deadlock, while the tracker has its own lock and still notifies the
// informers' watchers.
func PrependMockEvictionReactor(client *fake.Clientset) {
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		createAction, ok := action.(clienttesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		var namespace, name string
		switch eviction := createAction.GetObject().(type) {
		case *policyv1.Eviction:
			namespace, name = eviction.Namespace, eviction.Name
		case *policyv1beta1.Eviction:
			namespace, name = eviction.Namespace, eviction.Name
		default:
			return false, nil, nil
		}
		if namespace == "" {
			namespace = action.GetNamespace()
		}
		if err := client.Tracker().Delete(podsResource, namespace, name); err != nil && !apierrors.IsNotFound(err) {
			return true, nil, err
		}
		return true, createAction.GetObject(), nil
	})
}
