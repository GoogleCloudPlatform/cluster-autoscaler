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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate/types"
)

func TestPendingPodsPerCccCollector(t *testing.T) {
	testCases := []struct {
		name         string
		inputMetrics []types.PendingPodsPerCccMetric
		want         string
	}{
		{
			name: "basic scenario with all metrics",
			inputMetrics: []types.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.ProvisioningInProgress, SystemPod: true, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.ProvisioningInProgress, SystemPod: false, Count: 2}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.UnableToProvision, SystemPod: true, Count: 3}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.UnableToProvision, SystemPod: false, Count: 4}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.Unprocessed, SystemPod: true, Count: 5}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.Unprocessed, SystemPod: false, Count: 6}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.NoActionTaken, SystemPod: true, Count: 7}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.NoActionTaken, SystemPod: false, Count: 8}},
			},
			want: `
				# HELP cluster_autoscaler_cluster_pending_pods_per_ccc Number of pending pods in the cluster, broken down by reason, type and Custom Compute Class entity.
				# TYPE cluster_autoscaler_cluster_pending_pods_per_ccc gauge
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="no_action_taken",type="system_pod"} 7
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="no_action_taken",type="user_pod"} 8
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="provisioning_in_progress",type="system_pod"} 1
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="provisioning_in_progress",type="user_pod"} 2
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="unable_to_provision",type="system_pod"} 3
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="unable_to_provision",type="user_pod"} 4
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="unprocessed",type="system_pod"} 5
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="unprocessed",type="user_pod"} 6
				# HELP cluster_autoscaler_pending_pods_per_ccc Number of pending pods of various kinds in the cluster.
				# TYPE cluster_autoscaler_pending_pods_per_ccc gauge
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="no_action_taken",system_pod="false"} 8
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="no_action_taken",system_pod="true"} 7
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="provisioning_in_progress",system_pod="false"} 2
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="provisioning_in_progress",system_pod="true"} 1
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="unable_to_provision",system_pod="false"} 4
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="unable_to_provision",system_pod="true"} 3
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="unprocessed",system_pod="false"} 6
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="unprocessed",system_pod="true"} 5
				`,
		},
		{
			name:         "no pending pods",
			inputMetrics: []types.PendingPodsPerCccMetric{},
			want:         "",
		},
		{
			name: "metrics with zero counts",
			inputMetrics: []types.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: types.PendingPodsMetric{Kind: types.ProvisioningInProgress, SystemPod: false, Count: 0}},
			},
			want: `
				# HELP cluster_autoscaler_cluster_pending_pods_per_ccc Number of pending pods in the cluster, broken down by reason, type and Custom Compute Class entity.
				# TYPE cluster_autoscaler_cluster_pending_pods_per_ccc gauge
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="provisioning_in_progress",type="system_pod"} 0
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ComputeClass",reason="provisioning_in_progress",type="user_pod"} 0
				# HELP cluster_autoscaler_pending_pods_per_ccc Number of pending pods of various kinds in the cluster.
				# TYPE cluster_autoscaler_pending_pods_per_ccc gauge
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="provisioning_in_progress",system_pod="false"} 0
				cluster_autoscaler_pending_pods_per_ccc{entity_name="test-ccc",entity_type="ccc",kind="provisioning_in_progress",system_pod="true"} 0
				`,
		},
		{
			name: "metrics for pods without CCC",
			inputMetrics: []types.PendingPodsPerCccMetric{
				{CccName: "", PendingPodsMetric: types.PendingPodsMetric{Kind: types.ProvisioningInProgress, SystemPod: true, Count: 5}},
				{CccName: "", PendingPodsMetric: types.PendingPodsMetric{Kind: types.ProvisioningInProgress, SystemPod: false, Count: 6}},
			},
			want: `
				# HELP cluster_autoscaler_cluster_pending_pods_per_ccc Number of pending pods in the cluster, broken down by reason, type and Custom Compute Class entity.
				# TYPE cluster_autoscaler_cluster_pending_pods_per_ccc gauge
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="",entity_type="ComputeClass",reason="provisioning_in_progress",type="system_pod"} 5
				cluster_autoscaler_cluster_pending_pods_per_ccc{entity_name="",entity_type="ComputeClass",reason="provisioning_in_progress",type="user_pod"} 6
				# HELP cluster_autoscaler_pending_pods_per_ccc Number of pending pods of various kinds in the cluster.
				# TYPE cluster_autoscaler_pending_pods_per_ccc gauge
				cluster_autoscaler_pending_pods_per_ccc{entity_name="",entity_type="ccc",kind="provisioning_in_progress",system_pod="false"} 6
				cluster_autoscaler_pending_pods_per_ccc{entity_name="",entity_type="ccc",kind="provisioning_in_progress",system_pod="true"} 5
				`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			collector := &pendingPodsPerCccCollector{
				metricsCalculationFunc: func() []types.PendingPodsPerCccMetric {
					return tc.inputMetrics
				},
			}
			if err := testutil.CollectAndCompare(collector, strings.NewReader(tc.want), "cluster_autoscaler_pending_pods_per_ccc", "cluster_autoscaler_cluster_pending_pods_per_ccc"); err != nil {
				t.Errorf("unexpected collecting result:\n%s", err)
			}
		})
	}
}
