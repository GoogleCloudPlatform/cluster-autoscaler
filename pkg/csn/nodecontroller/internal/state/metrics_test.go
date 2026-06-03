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

package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
)

func TestNewMetricsEventHandler(t *testing.T) {
	handler := newMetricsEventHandler()

	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	node2 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}}

	tests := []struct {
		name            string
		event           NodeEvent
		metricVerifiers []test.MetricVerifier
	}{
		{
			name: "node1_added",
			event: NodeAdded{
				Node:  node1,
				State: csn.NodeStateChilling,
			},
			metricVerifiers: []test.MetricVerifier{
				delta(nodeEvents, 1, csn.NodeStateChilling, nodeAdded),
			},
		},
		{
			name: "node2_added",
			event: NodeAdded{
				Node:  node2,
				State: csn.NodeStateChilling,
			},
			metricVerifiers: []test.MetricVerifier{
				delta(nodeEvents, 1, csn.NodeStateChilling, nodeAdded),
			},
		},
		{
			name: "node1_deleted",
			event: NodeDeleted{
				Node:  node1,
				State: csn.NodeStateChilling,
			},
			metricVerifiers: []test.MetricVerifier{
				delta(nodeEvents, 1, csn.NodeStateChilling, nodeDeleted),
			},
		},
		{
			name: "node2_suspended",
			event: NodeUpdated{
				Node:     node2,
				OldState: csn.NodeStateChilling,
				NewState: csn.NodeStateSuspended,
			},
			metricVerifiers: []test.MetricVerifier{
				delta(nodeStateTransitions, 1, csn.NodeStateChilling, csn.NodeStateSuspended),
			},
		},
		{
			name: "node_counts_snapshot",
			event: NodeCounts{
				Counts: map[csn.NodeState]int{
					csn.NodeStateChilling:  5,
					csn.NodeStateSuspended: 3,
				},
			},
			metricVerifiers: []test.MetricVerifier{
				value(nodeCount, 5, csn.NodeStateChilling),
				value(nodeCount, 3, csn.NodeStateSuspended),
				value(nodeCount, 0, csn.NodeStateConsumed),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Read before
			for _, v := range tc.metricVerifiers {
				v.Init(t)
			}

			handler(tc.event)

			for _, v := range tc.metricVerifiers {
				assert.NoError(t, v.Verify(t))
			}
		})
	}
}

func delta(metric k8smetrics.Registerable, expected float64, states ...csn.NodeState) *test.MetricDelta {
	return test.NewMetricDelta(test.ExpectedValue(expected), metric, toString(states))
}

func value(metric k8smetrics.Registerable, expected float64, states ...csn.NodeState) *test.MetricValue {
	return test.NewMetricValue(test.ExpectedValue(expected), metric, toString(states))
}

func toString(states []csn.NodeState) []string {
	result := make([]string, 0, len(states))
	for _, s := range states {
		result = append(result, string(s))
	}
	return result
}
