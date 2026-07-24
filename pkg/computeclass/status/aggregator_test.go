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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	ctrl_client "sigs.k8s.io/controller-runtime/pkg/client"
)

type fakeStatusWriter struct {
	ctrl_client.SubResourceWriter
	patches []ctrl_client.Object
	err     error
}

func (w *fakeStatusWriter) Patch(ctx context.Context, obj ctrl_client.Object, patch ctrl_client.Patch, opts ...ctrl_client.SubResourcePatchOption) error {
	if w.err != nil {
		return w.err
	}
	w.patches = append(w.patches, obj)
	return nil
}

type fakeStatusClient struct {
	ctrl_client.Client
	writer *fakeStatusWriter
}

func newFakeStatusClient() *fakeStatusClient {
	return &fakeStatusClient{
		writer: &fakeStatusWriter{},
	}
}

func (c *fakeStatusClient) Status() ctrl_client.SubResourceWriter {
	return c.writer
}

func TestAggregator_ProcessMessage(t *testing.T) {
	testCrdLabel := "ComputeClass"
	crd1 := crd.NewTestCrd(
		crd.WithLabel(testCrdLabel),
		crd.WithName("test-ccc"),
	)
	now := time.Now()

	testCases := []struct {
		name      string
		crds      []crd.CRD
		msgId     CRDId
		setup     func(status crd.CRDStatus)
		mutate    Mutator
		wantDirty bool
		wantInMap bool
	}{
		{
			name:  "valid mutation changing status marks CRD as dirty",
			crds:  []crd.CRD{crd1},
			msgId: CRDId{CRDLabel: testCrdLabel, CRDName: "test-ccc"},
			mutate: func(status crd.CRDStatus) {
				status.UpdateConditions([]metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}})
			},
			wantDirty: true,
			wantInMap: true,
		},
		{
			name:  "no-op mutation does not mark CRD as dirty",
			crds:  []crd.CRD{crd1},
			msgId: CRDId{CRDLabel: testCrdLabel, CRDName: "test-ccc"},
			mutate: func(status crd.CRDStatus) {
				// No mutations performed
			},
			wantDirty: false,
			wantInMap: true,
		},
		{
			name:  "condition with different timestamp but other fields equal does not trigger change",
			crds:  []crd.CRD{crd1},
			msgId: CRDId{CRDLabel: testCrdLabel, CRDName: "test-ccc"},
			setup: func(status crd.CRDStatus) {
				status.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: metav1.NewTime(now),
				}})
			},
			mutate: func(status crd.CRDStatus) {
				status.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: metav1.NewTime(now.Add(1 * time.Minute)),
				}})
			},
			wantDirty: false,
			wantInMap: true,
		},
		{
			name:  "message for non-existent CRD is ignored",
			crds:  []crd.CRD{crd1},
			msgId: CRDId{CRDLabel: testCrdLabel, CRDName: "missing-ccc"},
			mutate: func(status crd.CRDStatus) {
				status.UpdateConditions([]metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}})
			},
			wantDirty: false,
			wantInMap: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			mockLister.SetCrdLabel(testCrdLabel)

			aggregator := NewAggregator(nil, mockLister, make(chan UpdateMessage), newFakeStatusClient())
			if tc.setup != nil {
				crdObj, _ := mockLister.Crd(tc.msgId.CRDLabel, tc.msgId.CRDName)
				if crdObj != nil {
					status := aggregator.getOrCreateStatus(crdObj)
					tc.setup(status)
				}
			}

			aggregator.processMessage(UpdateMessage{
				Id:     tc.msgId,
				Mutate: tc.mutate,
			})

			assert.Equal(t, tc.wantDirty, aggregator.dirtySet[tc.msgId])
			if tc.wantInMap {
				assert.Contains(t, aggregator.statusMap, tc.msgId)
			} else {
				assert.NotContains(t, aggregator.statusMap, tc.msgId)
			}
		})
	}
}
