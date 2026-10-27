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

package karpenter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/kubernetes/scheme"
)

func TestCapacityBufferSchemeRegistration(t *testing.T) {
	// Re-registering canonical CapacityBuffer types into the global client-go
	// scheme (as done at runtime in pkg/autoscaler) must not panic or error.
	assert.NotPanics(t, func() {
		assert.NoError(t, cbv1beta1.AddToScheme(scheme.Scheme))
	})

	cbGVK := cbv1beta1.SchemeGroupVersion.WithKind("CapacityBuffer")
	obj, err := scheme.Scheme.New(cbGVK)
	if !assert.NoError(t, err) {
		return
	}
	assert.IsType(t, &cbv1beta1.CapacityBuffer{}, obj)

	cbListGVK := cbv1beta1.SchemeGroupVersion.WithKind("CapacityBufferList")
	listObj, err := scheme.Scheme.New(cbListGVK)
	if !assert.NoError(t, err) {
		return
	}
	assert.IsType(t, &cbv1beta1.CapacityBufferList{}, listObj)

	gvks, _, err := scheme.Scheme.ObjectKinds(&cbv1beta1.CapacityBuffer{})
	if !assert.NoError(t, err) {
		return
	}
	assert.Contains(t, gvks, cbGVK)
}
