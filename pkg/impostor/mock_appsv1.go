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

package impostor

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typed_appsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
)

// newAppsV1 returns a new instance of appsV1.
func newAppsV1(mSSLister *statefulSetLister, appsV1Int typed_appsv1.AppsV1Interface) *appsV1 {
	return &appsV1{
		appsV1Int,
		make(map[string]*statefulSet),
		mSSLister,
	}
}

// appsV1 is a wrapper over AppsV1 interface.
type appsV1 struct {
	typed_appsv1.AppsV1Interface
	sSetsInterfaces map[string]*statefulSet
	ssLister        *statefulSetLister
}

// StatefulSets returns StatefulSetInterface for given namespace.
func (a *appsV1) StatefulSets(namespace string) typed_appsv1.StatefulSetInterface {
	if a.sSetsInterfaces[namespace] == nil {
		a.sSetsInterfaces[namespace] = newStatefulSet(a.ssLister)
	}
	return a.sSetsInterfaces[namespace]
}

// newStatefulSet creates a new instances of statefulSet.
func newStatefulSet(ssLister *statefulSetLister) *statefulSet {
	return &statefulSet{
		ssLister: ssLister,
	}
}

// statefulSet is a wrapper over StatefulSet interface.
type statefulSet struct {
	typed_appsv1.StatefulSetInterface
	ssLister *statefulSetLister
}

// Create creates a new StatefulSet.
func (s *statefulSet) Create(_ context.Context, statefulSet *appsv1.StatefulSet, _ metav1.CreateOptions) (result *appsv1.StatefulSet, err error) {
	s.ssLister.Add(statefulSet)
	return statefulSet, nil
}
