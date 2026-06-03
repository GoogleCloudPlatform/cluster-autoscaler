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
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const namespace = "cluster_autoscaler"

var (
	nodeEvents = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: namespace,
			Name:      "csn_node_events_total",
			Help: "Total number of observed node events. Node events can cause " +
				"a node to be added to or removed from the cluster",
		},
		[]string{"state", "event_type"},
	)
	nodeStateTransitions = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: namespace,
			Name:      "csn_node_state_transitions_total",
			Help:      "Total number of node state transitions.",
		},
		[]string{"from_state", "to_state"},
	)
	nodeCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: namespace,
			Name:      "csn_controller_nodes",
			Help: "Number of nodes in the cluster seen from the perspective of " +
				"the CSN Node Controller",
		},
		[]string{"state"},
	)
)

func init() {
	legacyregistry.MustRegister(nodeEvents)
	legacyregistry.MustRegister(nodeStateTransitions)
	legacyregistry.MustRegister(nodeCount)
}

const (
	// event_type labels
	nodeAdded   = "added"
	nodeDeleted = "deleted"
)

// newMetricsEventHandler returns an EventHandler that records metrics locally.
func newMetricsEventHandler() EventHandler {
	return func(e NodeEvent) {
		switch event := e.(type) {
		case NodeAdded:
			nodeEvents.WithLabelValues(string(event.State), nodeAdded).Inc()
		case NodeDeleted:
			nodeEvents.WithLabelValues(string(event.State), nodeDeleted).Inc()
		case NodeUpdated:
			if event.OldState != event.NewState {
				nodeStateTransitions.WithLabelValues(string(event.OldState), string(event.NewState)).Inc()
			}
		case NodeCounts:
			nodeCount.Reset()
			for state, count := range event.Counts {
				nodeCount.WithLabelValues(string(state)).Set(float64(count))
			}
		}
	}
}
