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

package capacitybuffers

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
)

func TestInFlightCacheCreate(t *testing.T) {
	c := newCCCCache()

	// 1. Verify safe handling of nil / empty name
	c.RecordBufferCreate(nil)
	c.RecordBufferCreate(&cbv1beta1.CapacityBuffer{})
	if len(c.GetPendingBuffers()) != 0 {
		t.Fatalf("Expected 0 pending buffers, got %d", len(c.GetPendingBuffers()))
	}

	// 2. Record full CapacityBuffer
	limits := cbv1beta1.ResourceList{
		cbv1beta1.ResourceName("cpu"): resource.MustParse("2"),
	}
	strategy := "active"
	cb := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cb",
			UID:  "test-uid-1",
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &strategy,
			Limits:               &limits,
			PodTemplateRef: &cbv1beta1.LocalObjectRef{
				Name: "test-pt",
			},
		},
	}

	c.RecordBufferCreate(cb)

	pending := c.GetPendingBuffers()
	if len(pending) != 1 {
		t.Fatalf("Expected exactly 1 pending buffer, got %d", len(pending))
	}
	p := pending[0]
	if p.Name != "test-cb" || p.UID != "test-uid-1" || p.ProvisioningStrategy != "active" {
		t.Errorf("Pending buffer details incorrectly matched: %+v", p)
	}
	if v, exists := p.Limits[corev1.ResourceName("cpu")]; !exists || v.String() != "2" {
		t.Errorf("Pending buffer Limits structurally flawed")
	}
	if !p.PendingCreation {
		t.Errorf("Expected PendingCreation to be true")
	}

	// 3. Drop Buffer
	c.DropBufferCreate("test-cb")
	if len(c.GetPendingBuffers()) != 0 {
		t.Fatalf("Expected 0 pending buffers after dropping")
	}
}

func TestInFlightCacheDelete(t *testing.T) {
	c := newCCCCache()

	var uid types.UID = "delete-uid-1"

	if c.HasRecentBufferDelete(uid) {
		t.Fatalf("Expected fresh cache to report false")
	}

	c.RecordBufferDelete(uid, "delete-name-1")
	if !c.HasRecentBufferDelete(uid) {
		t.Fatalf("Expected HasRecentBufferDelete to report true after recording")
	}

	if c.HasRecentBufferDelete("fake-uid") {
		t.Fatalf("Expected HasRecentBufferDelete to report false for unknown uid")
	}

}

func TestInFlightCachePrune(t *testing.T) {
	c := newCCCCache()

	strategy := "active"
	cb := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cb",
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &strategy,
		},
	}

	// Write records natively
	c.RecordBufferCreate(cb)
	c.RecordBufferDelete("delete-uid-1", "other-cb")

	// Ensure they are readable before GC
	if len(c.GetPendingBuffers()) == 0 {
		t.Fatalf("Create record did not persist")
	}
	if !c.HasRecentBufferDelete("delete-uid-1") {
		t.Fatalf("Delete record did not persist")
	}

	// Force time-travel: set TTLs mathematically into the past (negative duration)
	c.createTTL = -1 * time.Second
	c.deleteTTL = -1 * time.Second

	// Run Prune sweeps
	c.PruneExpired()

	// Ensure all structures are fully empty
	// Note: We bypass GetPendingBuffers since it uses createTTL natively, and check the mapping length.
	if len(c.bufferCreateRequests) != 0 {
		t.Errorf("PruneExpired failed to purge bufferCreateRequests")
	}
	if len(c.bufferDeleteRequests) != 0 {
		t.Errorf("PruneExpired failed to purge bufferDeleteRequests")
	}
}
