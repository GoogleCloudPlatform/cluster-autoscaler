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

package nodequota

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/context"
)

var testStartTime = time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)

func TestProcess(t *testing.T) {
	testCases := map[string]struct {
		initialMaxNodesTotal  int
		quota                 int
		expectedMaxNodesTotal int
	}{
		"Quota is zero, MaxNodesTotal set to quota": {
			initialMaxNodesTotal:  100,
			quota:                 0,
			expectedMaxNodesTotal: 0,
		},
		"MaxNodesTotal below quota, MaxNodesTotal set to quota": {
			initialMaxNodesTotal:  100,
			quota:                 200,
			expectedMaxNodesTotal: 200,
		},
		"MaxNodesTotal above quota, MaxNodesTotal set to quota": {
			initialMaxNodesTotal:  200,
			quota:                 100,
			expectedMaxNodesTotal: 100,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				AutoscalingOptions: config.AutoscalingOptions{
					MaxNodesTotal: tc.initialMaxNodesTotal,
				},
			}
			qw := &mockQuotaWatcher{quota: tc.quota}
			processor := NewNodeQuotaProcessor(qw)
			err := processor.Process(ctx, &clusterstate.ClusterStateRegistry{}, testStartTime)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedMaxNodesTotal, ctx.MaxNodesTotal)
		})
	}
}

type mockQuotaWatcher struct {
	quota int
}

func (qw *mockQuotaWatcher) GetNodeQuota() int {
	return qw.quota
}
func (qw *mockQuotaWatcher) Run(stopCh <-chan struct{}) {}
