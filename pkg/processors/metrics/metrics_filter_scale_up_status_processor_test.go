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

package metrics_processors

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
)

func TestMetricsFilterScaleUpProcessor_RegisterFailedScaleUp(t *testing.T) {
	testCases := []struct {
		name                string
		errorCode           string
		expectStockout      bool
		expectRemainingPods int
	}{
		{
			name:                "Stockout error code",
			errorCode:           gce.ErrorCodeResourcePoolExhausted,
			expectStockout:      true,
			expectRemainingPods: 2,
		},
		{
			name:                "Quota exceeded error code",
			errorCode:           gce.ErrorCodeQuotaExceeded,
			expectStockout:      false,
			expectRemainingPods: 0,
		},
		{
			name:                "IP space exhausted error code",
			errorCode:           gce.ErrorIPSpaceExhausted,
			expectStockout:      false,
			expectRemainingPods: 0,
		},
		{
			name:                "Service account deleted error code",
			errorCode:           gkeclient.ServiceAccountDeleted,
			expectStockout:      false,
			expectRemainingPods: 0,
		},
		{
			name:                "Unrelated error code",
			errorCode:           "some-other-error",
			expectStockout:      false,
			expectRemainingPods: 2,
		},
		{
			name:                "Empty error code",
			errorCode:           "",
			expectStockout:      false,
			expectRemainingPods: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metricsFilter := filter.NewMetricsFilter()
			processor := NewMetricsFilterScaleUpProcessor(metricsFilter)

			ng := test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil)
			pod1 := &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod1",
					Namespace: "default",
					UID:       "pod1-uid",
				},
			}
			pod2 := &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod2",
					Namespace: "default",
					UID:       "pod2-uid",
				},
			}
			testTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			metricsFilter.ObserveScaleUp([]*apiv1.Pod{pod1, pod2}, []string{ng.Id()}, testTime)

			processor.RegisterFailedScaleUp(ng, 1, cloudprovider.InstanceErrorInfo{
				ErrorCode: tc.errorCode,
			}, testTime)

			stockoutPods := metricsFilter.GetsPodsEncounteringStockOut([]*apiv1.Pod{pod1, pod2})
			assert.Equal(t, tc.expectStockout, stockoutPods[pod1.UID], "Stockout status mismatch for pod1")
			assert.Equal(t, tc.expectStockout, stockoutPods[pod2.UID], "Stockout status mismatch for pod2")

			filteredPods := metricsFilter.FilterOutPods([]*apiv1.Pod{pod1, pod2})
			assert.Len(t, filteredPods, tc.expectRemainingPods)
		})
	}
}

func TestMetricsFilterScaleUpProcessor_RegisterFailedScaleUp_NilNodeGroup(t *testing.T) {
	metricsFilter := filter.NewMetricsFilter()
	processor := NewMetricsFilterScaleUpProcessor(metricsFilter)
	testTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	assert.NotPanics(t, func() {
		processor.RegisterFailedScaleUp(nil, 1, cloudprovider.InstanceErrorInfo{
			ErrorCode: gce.ErrorCodeQuotaExceeded,
		}, testTime)
	})
}

func TestMetricsFilterScaleUpProcessor_NoOpMethods(t *testing.T) {
	metricsFilter := filter.NewMetricsFilter()
	processor := NewMetricsFilterScaleUpProcessor(metricsFilter)
	ng := test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil)
	testTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	assert.NotPanics(t, func() {
		processor.RegisterScaleUp(ng, 1, testTime)
		processor.RegisterScaleDown(ng, "node1", testTime, testTime)
		processor.RegisterFailedScaleDown(ng, "reason", testTime)
	})

	assert.Empty(t, metricsFilter.GetsPodsEncounteringStockOut(nil))
}
