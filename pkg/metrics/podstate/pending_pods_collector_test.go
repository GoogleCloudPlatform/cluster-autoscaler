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

package podstate

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate/types"
)

func TestPendingPodsCollector(t *testing.T) {
	testCases := []struct {
		name         string
		inputMetrics []types.PendingPodsMetric
		want         string
	}{
		{
			name: "basic scenario with all metrics",
			inputMetrics: []types.PendingPodsMetric{
				{Kind: types.ProvisioningInProgress, SystemPod: true, Count: 1},
				{Kind: types.ProvisioningInProgress, SystemPod: false, Count: 2},
				{Kind: types.UnableToProvision, SystemPod: true, Count: 3},
				{Kind: types.UnableToProvision, SystemPod: false, Count: 4},
				{Kind: types.Unprocessed, SystemPod: true, Count: 5},
				{Kind: types.Unprocessed, SystemPod: false, Count: 6},
				{Kind: types.NoActionTaken, SystemPod: true, Count: 7},
				{Kind: types.NoActionTaken, SystemPod: false, Count: 8},
			},
			want: "# HELP cluster_autoscaler_pending_pods Number of pending pods of various kinds in the cluster.\n# TYPE cluster_autoscaler_pending_pods gauge\ncluster_autoscaler_pending_pods{kind=\"no_action_taken\",system_pod=\"false\"} 8\ncluster_autoscaler_pending_pods{kind=\"no_action_taken\",system_pod=\"true\"} 7\ncluster_autoscaler_pending_pods{kind=\"provisioning_in_progress\",system_pod=\"false\"} 2\ncluster_autoscaler_pending_pods{kind=\"provisioning_in_progress\",system_pod=\"true\"} 1\ncluster_autoscaler_pending_pods{kind=\"unable_to_provision\",system_pod=\"false\"} 4\ncluster_autoscaler_pending_pods{kind=\"unable_to_provision\",system_pod=\"true\"} 3\ncluster_autoscaler_pending_pods{kind=\"unprocessed\",system_pod=\"false\"} 6\ncluster_autoscaler_pending_pods{kind=\"unprocessed\",system_pod=\"true\"} 5\n",
		},
		{
			name:         "no pending pods",
			inputMetrics: []types.PendingPodsMetric{},
			want:         "",
		},
		{
			name: "metrics with zero counts",
			inputMetrics: []types.PendingPodsMetric{
				{Kind: types.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: types.UnableToProvision, SystemPod: false, Count: 0},
			},
			want: "# HELP cluster_autoscaler_pending_pods Number of pending pods of various kinds in the cluster.\n# TYPE cluster_autoscaler_pending_pods gauge\ncluster_autoscaler_pending_pods{kind=\"provisioning_in_progress\",system_pod=\"true\"} 0\ncluster_autoscaler_pending_pods{kind=\"unable_to_provision\",system_pod=\"false\"} 0\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			collector := &pendingPodsCollector{
				metricsCalculationFunc: func() []types.PendingPodsMetric {
					return tc.inputMetrics
				},
			}
			if err := testutil.CollectAndCompare(collector, strings.NewReader(tc.want), "cluster_autoscaler_pending_pods"); err != nil {
				t.Errorf("unexpected collecting result:\n%s", err)
			}
		})
	}
}
