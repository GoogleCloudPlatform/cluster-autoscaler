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

package status

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestSetupConfigDriftResetObserver(t *testing.T) {
	const ccName = "test-ccc"
	computeClass := func(generation int64) *cccv1.ComputeClass {
		return &cccv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{Name: ccName, Generation: generation},
		}
	}

	tests := map[string]struct {
		oldObj     any
		newObj     any
		enabled    bool
		wantResets int
	}{
		"a spec change clears the counters": {
			oldObj:     computeClass(1),
			newObj:     computeClass(2),
			enabled:    true,
			wantResets: 1,
		},
		"an update that leaves the spec alone is ignored": {
			// The autoscaler's own status patches come back as updates. Acting
			// on them would clear the counters it just reported, every flush.
			oldObj:     computeClass(2),
			newObj:     computeClass(2),
			enabled:    true,
			wantResets: 0,
		},
		"nothing is cleared while the feature is off": {
			oldObj:     computeClass(1),
			newObj:     computeClass(2),
			enabled:    false,
			wantResets: 0,
		},
		"an object that is not a ComputeClass is ignored": {
			oldObj:     computeClass(1),
			newObj:     "not-a-compute-class",
			enabled:    true,
			wantResets: 0,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			updatesCh := make(chan UpdateMessage, updatesChannelSize)
			informer := &fakeCccInformer{}
			SetupConfigDriftResetObserver(informer, updatesCh, experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{experiments.ComputeClassConfigDriftStatusEnabledFlag: test.enabled},
				map[string]string{},
			))
			if informer.handler.UpdateFunc == nil {
				t.Fatalf("SetupConfigDriftResetObserver() registered no update handler")
			}

			informer.handler.OnUpdate(test.oldObj, test.newObj)

			want := map[string]reportedStatus{}
			if test.wantResets > 0 {
				want[ccName] = reportedStatus{Resets: test.wantResets}
			}
			got := drainConfigDriftUpdates(t, updatesCh)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("reported statuses mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestConfigDriftResetObserverOnlyClearsTrackedComputeClasses(t *testing.T) {
	updatesCh := make(chan UpdateMessage, updatesChannelSize)
	informer := &fakeCccInformer{}
	SetupConfigDriftResetObserver(informer, updatesCh, experiments.NewMockManagerWithOptions(
		version.Version{},
		map[string]bool{experiments.ComputeClassConfigDriftStatusEnabledFlag: true},
		map[string]string{},
	))

	informer.handler.OnUpdate(
		&cccv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "test-ccc", Generation: 1}},
		&cccv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "test-ccc", Generation: 2}},
	)

	select {
	case msg := <-updatesCh:
		// Every shard of a sharded autoscaler sees the edit, but only the
		// shard reporting on the ComputeClass has anything to clear. A reset
		// that made the others start tracking it would have them overwrite
		// the reporting shard's fields on their next flush.
		if !msg.OnlyIfTracked {
			t.Error("the reset is not restricted to ComputeClasses this autoscaler already tracks")
		}
	default:
		t.Fatal("a spec change sent no reset")
	}
}

// fakeCccInformer captures the handler the observer registers, so that the
// events an informer would deliver can be raised by hand.
type fakeCccInformer struct {
	cache.SharedIndexInformer
	handler cache.ResourceEventHandlerFuncs
}

func (i *fakeCccInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	i.handler = handler.(cache.ResourceEventHandlerFuncs)
	return nil, nil
}
