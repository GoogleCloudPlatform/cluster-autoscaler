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

package provreq

import (
	"encoding/json"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/runtime"
	prfake "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned/fake"
	coretesting "k8s.io/client-go/testing"
)

type FakeClientset struct {
	*prfake.Clientset
}

func NewFakeClientset() *FakeClientset {
	clientset := prfake.NewSimpleClientset()
	fakeClient := &FakeClientset{
		Clientset: clientset,
	}

	fakeClient.Fake.PrependReactor("patch", "provisioningrequests", fakeClient.handlePatch)

	return fakeClient
}

func (f *FakeClientset) handlePatch(action coretesting.Action) (handled bool, ret runtime.Object, err error) {
	patchAction, ok := action.(coretesting.PatchAction)
	if !ok {
		return false, nil, nil
	}

	obj, err := f.Tracker().Get(patchAction.GetResource(), patchAction.GetNamespace(), patchAction.GetName())
	if err != nil {
		return true, nil, err
	}

	originalJSON, err := json.Marshal(obj)
	if err != nil {
		return true, nil, err
	}

	patchedJSON, err := jsonpatch.MergePatch(originalJSON, patchAction.GetPatch())
	if err != nil {
		return true, nil, err
	}

	patchedObj := obj.DeepCopyObject()
	if err := json.Unmarshal(patchedJSON, patchedObj); err != nil {
		return true, nil, err
	}

	if err := f.Tracker().Update(patchAction.GetResource(), patchedObj, patchAction.GetNamespace()); err != nil {
		return true, nil, err
	}

	return true, patchedObj, nil
}
