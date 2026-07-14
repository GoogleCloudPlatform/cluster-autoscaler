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

package ccc

import (
	"context"
	"fmt"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	ccc_clientset "github.com/googlecloudplatform/compute-class-api/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FakeStatusClient implements sigs.k8s.io/controller-runtime/pkg/client.Client
// and applies status patches directly to the fake CccClient in-memory.
type FakeStatusClient struct {
	client.Client
	CccClient ccc_clientset.Interface
}

// NewFakeStatusClient creates a new FakeStatusClient wrapping the given CCC clientset.
func NewFakeStatusClient(cccClient ccc_clientset.Interface) *FakeStatusClient {
	return &FakeStatusClient{CccClient: cccClient}
}

func (f *FakeStatusClient) Status() client.SubResourceWriter {
	return &FakeSubResourceWriter{CccClient: f.CccClient}
}

type FakeSubResourceWriter struct {
	CccClient ccc_clientset.Interface
}

func (w *FakeSubResourceWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return nil
}

func (w *FakeSubResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return nil
}

// Patch simulates patching a ComputeClass status. Note that the patch type parameter is ignored and the method completely overwrites the existing status block with the status from obj.
func (w *FakeSubResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	cccObj, ok := obj.(*v1.ComputeClass)
	if !ok {
		return fmt.Errorf("expected *v1.ComputeClass, got %T", obj)
	}
	if cccObj.Status.Conditions == nil {
		cccObj.Status.Conditions = []metav1.Condition{}
	}
	if cccObj.Status.PriorityStatuses == nil {
		cccObj.Status.PriorityStatuses = []v1.PriorityStatus{}
	}
	if cccObj.Status.ResourceInfo == nil {
		cccObj.Status.ResourceInfo = []v1.ResourceInfo{}
	}
	for i := range cccObj.Status.PriorityStatuses {
		if cccObj.Status.PriorityStatuses[i].Conditions == nil {
			cccObj.Status.PriorityStatuses[i].Conditions = []metav1.Condition{}
		}
		if cccObj.Status.PriorityStatuses[i].ResourceInfo == nil {
			cccObj.Status.PriorityStatuses[i].ResourceInfo = []v1.ResourceInfo{}
		}
	}
	existing, err := w.CccClient.CloudV1().ComputeClasses().Get(ctx, cccObj.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	existing.Status = cccObj.Status
	_, err = w.CccClient.CloudV1().ComputeClasses().UpdateStatus(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (w *FakeSubResourceWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return nil
}
