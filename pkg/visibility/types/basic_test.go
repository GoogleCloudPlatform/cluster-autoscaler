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

package types

import (
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

func TestConvertPod(t *testing.T) {
	controllerTrue := true
	controllerFalse := false
	ownerControllerNil := metav1.OwnerReference{
		APIVersion: "v1",
		Name:       "owner1",
		Kind:       "owner1-kind",
		UID:        "owner1-uid",
		Controller: nil,
	}
	ownerControllerTrue := metav1.OwnerReference{
		APIVersion: "v1",
		Name:       "owner2",
		Kind:       "owner2-kind",
		UID:        "owner2-uid",
		Controller: &controllerTrue,
	}
	ownerControllerFalse := metav1.OwnerReference{
		APIVersion: "v1",
		Name:       "owner3",
		Kind:       "owner3-kind",
		UID:        "owner3-uid",
		Controller: &controllerFalse,
	}

	expectedVizPodController := &PodController{
		Uid:        "owner2-uid",
		Name:       "owner2",
		Kind:       "owner2-kind",
		ApiVersion: "v1",
	}

	for _, tc := range []struct {
		ownerRefs          []metav1.OwnerReference
		expectedController *PodController
	}{
		{ownerRefs: nil, expectedController: nil},
		{ownerRefs: []metav1.OwnerReference{}, expectedController: nil},
		{ownerRefs: []metav1.OwnerReference{ownerControllerNil}, expectedController: nil},
		{ownerRefs: []metav1.OwnerReference{ownerControllerFalse}, expectedController: nil},
		{ownerRefs: []metav1.OwnerReference{ownerControllerTrue}, expectedController: expectedVizPodController},
		{ownerRefs: []metav1.OwnerReference{ownerControllerNil, ownerControllerFalse, ownerControllerTrue}, expectedController: expectedVizPodController},
	} {
		spec := apiv1.PodSpec{
			Containers: []apiv1.Container{
				{Name: "cont1"},
			},
		}
		pod := &apiv1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "p1",
				Namespace:       "ns1",
				UID:             types.UID("uid1"),
				OwnerReferences: tc.ownerRefs,
			},
			Spec: spec,
		}
		expectedVizPod := &Pod{
			Uid:        "uid1",
			Name:       "p1",
			Namespace:  "ns1",
			Spec:       spec,
			Controller: tc.expectedController,
		}

		assert.Equal(t, expectedVizPod, ConvertPod(pod))
	}
}
