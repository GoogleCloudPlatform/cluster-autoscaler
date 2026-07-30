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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
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

			mgr := experiments.NewMockManagerWithOptions(
				version.Version{2, 0, 0, 0},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
				map[string]string{experiments.ComputeClassEnhancedObservabilityMinCAVersionFlag: "1.0.0"},
			)
			aggregator := NewAggregator(nil, mockLister, make(chan UpdateMessage), newFakeStatusClient(), mgr)
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

func TestAggregator_ExperimentEnabledVsDisabled(t *testing.T) {
	testCrd := crd.NewTestCrd(
		crd.WithLabel("test-label"),
		crd.WithName("test-crd"),
	)
	mockLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
	mockLister.SetCrdLabel("test-label")

	t.Run("experiment disabled - ignores updates", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			inputCh := make(chan UpdateMessage, 10)
			mgr := experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: false},
				map[string]string{},
			)
			aggregator := NewAggregator(nil, mockLister, inputCh, nil, mgr)

			ctx, cancel := context.WithCancel(context.Background())

			go aggregator.Start(ctx)

			crdId := CRDId{CRDName: "test-crd", CRDLabel: "test-label"}
			inputCh <- UpdateMessage{
				Id: crdId,
				Mutate: func(s crd.CRDStatus) {
					// mutating status
				},
			}

			// Wait for inputCh processing, then stop aggregator loop completely
			synctest.Wait()
			cancel()
			synctest.Wait()

			assert.Empty(t, aggregator.dirtySet)
			assert.Empty(t, aggregator.statusMap)
		})
	})

	t.Run("experiment enabled - processes updates", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			inputCh := make(chan UpdateMessage, 10)
			mgr := experiments.NewMockManagerWithOptions(
				version.Version{2, 0, 0, 0},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
				map[string]string{experiments.ComputeClassEnhancedObservabilityMinCAVersionFlag: "1.0.0"},
			)
			aggregator := NewAggregator(nil, mockLister, inputCh, nil, mgr)

			ctx, cancel := context.WithCancel(context.Background())

			go aggregator.Start(ctx)

			crdId := CRDId{CRDName: "test-crd", CRDLabel: "test-label"}
			inputCh <- UpdateMessage{
				Id: crdId,
				Mutate: func(s crd.CRDStatus) {
					// mutating status
					s.UpdateConditions([]metav1.Condition{{
						Type: "Ready", Status: metav1.ConditionTrue,
					}})
				},
			}

			// Wait for inputCh processing, then stop aggregator loop completely
			synctest.Wait()
			cancel()
			synctest.Wait()

			assert.True(t, aggregator.dirtySet[crdId])
			assert.NotNil(t, aggregator.statusMap[crdId])
		})
	})
}

func TestErrorCode(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error returns 200",
			err:      nil,
			expected: "200",
		},
		{
			name:     "direct StatusError returns code",
			err:      &apierrors.StatusError{ErrStatus: metav1.Status{Code: 409}},
			expected: "409",
		},
		{
			name:     "wrapped StatusError returns code with errors.As",
			err:      fmt.Errorf("failed to patch: %w", &apierrors.StatusError{ErrStatus: metav1.Status{Code: 404}}),
			expected: "404",
		},
		{
			name:     "non-status error returns error",
			err:      fmt.Errorf("some generic error"),
			expected: "error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, errorCode(tc.err))
		})
	}
}

func TestAggregator_MakeUpdates_Metrics(t *testing.T) {
	registerOnce.Do(metrics.RegisterAll)
	testCrdLabel := "ComputeClass"
	crd1 := crd.NewTestCrd(
		crd.WithLabel(testCrdLabel),
		crd.WithName("test-ccc-1"),
	)
	mockLister := lister.NewMockCrdLister([]crd.CRD{crd1})
	mockLister.SetCrdLabel(testCrdLabel)

	mgr := experiments.NewMockManagerWithOptions(
		version.Version{2, 0, 0, 0},
		map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
		map[string]string{experiments.ComputeClassEnhancedObservabilityMinCAVersionFlag: "1.0.0"},
	)

	testCases := []struct {
		name         string
		patchErr     error
		expectedCode string
	}{
		{
			name:         "successful patch increments 200 metric",
			patchErr:     nil,
			expectedCode: "200",
		},
		{
			name:         "failed patch with StatusError increments error code metric",
			patchErr:     fmt.Errorf("failed to patch: %w", &apierrors.StatusError{ErrStatus: metav1.Status{Code: 409}}),
			expectedCode: "409",
		},
		{
			name:         "failed patch with generic error increments error metric",
			patchErr:     fmt.Errorf("generic error"),
			expectedCode: "error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics.ResetAllForTest()
			client := newFakeStatusClient()
			client.writer.err = tc.patchErr
			aggregator := NewAggregator(nil, mockLister, make(chan UpdateMessage), client, mgr)
			crdId := CRDId{CRDName: "test-ccc-1", CRDLabel: testCrdLabel}
			aggregator.dirtySet[crdId] = true

			aggregator.makeUpdates(context.Background())

			reqCount, err := metrics.GetCCStatusApiPatchRequestsCountForTest(tc.expectedCode)
			assert.NoError(t, err)
			assert.Equal(t, float64(1), reqCount)

			durCount, err := metrics.GetCCStatusApiPatchDurationCountForTest(tc.expectedCode)
			assert.NoError(t, err)
			assert.Equal(t, uint64(1), durCount)
			assert.Empty(t, aggregator.dirtySet)
		})
	}
}
