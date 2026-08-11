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

package reconciliation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce"
)

func TestReconciliationMetrics_DeviatingNodes(t *testing.T) {
	mig := gce.GceRef{Project: "p1", Zone: "z1", Name: "mig"}
	gkeMig := gke.NewTestGkeMigBuilder().SetGceRef(mig).Build()

	node1 := test.CreateNode("node1", test.StateOpt(csn.NodeStateChilling))
	node2 := test.CreateNode("node2", test.StateOpt(csn.NodeStateChilling))

	ref1, err := gce.GceRefFromProviderId(node1.Spec.ProviderID)
	assert.NoError(t, err)
	ref2, err := gce.GceRefFromProviderId(node2.Spec.ProviderID)
	assert.NoError(t, err)

	instance1 := &gce.GceInstance{
		GCEStatus: "SUSPENDED", // out of sync for CHILLING
	}
	instance2 := &gce.GceInstance{
		GCEStatus: "RUNNING", // in sync for CHILLING
	}

	sm := &mockStateManager{
		trackedNodes: []state.TrackedNode{
			{Node: node1, State: csn.NodeStateChilling},
			{Node: node2, State: csn.NodeStateChilling},
		},
	}
	cp := &mockCloudProvider{
		nodeToMig: map[string]*gke.GkeMig{
			"node1": gkeMig,
			"node2": gkeMig,
		},
		instances: map[gce.GceRef]*gce.GceInstance{
			ref1: instance1,
			ref2: instance2,
		},
	}
	wq := &mockWorkQueue{}
	cfg := Config{MaxInvalidCount: 3}

	r := NewReconciler(sm, cp, wq, cfg)

	chillingStr := string(csn.NodeStateChilling)

	// Pass 1: node1 invalidCount = 1 (deviates), node2 invalidCount = 0 (in sync)
	valPass1Deviated := test.NewMetricValue(test.ExpectedValue(1), deviatingNodes, []string{chillingStr, instance1.GCEStatus, "1"})
	valPass1InSync := test.NewMetricValue(test.ExpectedValue(0), deviatingNodes, []string{chillingStr, instance2.GCEStatus, "1"})

	r.Reconcile()

	assert.NoError(t, valPass1Deviated.Verify(t))
	assert.NoError(t, valPass1InSync.Verify(t))

	// Pass 2: node1 invalidCount = 2 (deviates). Previous invalidCount=1 gauge should be reset to 0.
	valPass2Deviated := test.NewMetricValue(test.ExpectedValue(1), deviatingNodes, []string{chillingStr, instance1.GCEStatus, "2"})
	valPass2Old := test.NewMetricValue(test.ExpectedValue(0), deviatingNodes, []string{chillingStr, instance1.GCEStatus, "1"})

	r.Reconcile()

	assert.NoError(t, valPass2Deviated.Verify(t))
	assert.NoError(t, valPass2Old.Verify(t))
}

func TestReconciliationMetrics_ReconcileRequestsTotal(t *testing.T) {
	mig := gce.GceRef{Project: "p1", Zone: "z1", Name: "mig"}
	gkeMig := gke.NewTestGkeMigBuilder().SetGceRef(mig).Build()
	node1 := test.CreateNode("node1", test.StateOpt(csn.NodeStateChilling))

	ref, err := gce.GceRefFromProviderId(node1.Spec.ProviderID)
	assert.NoError(t, err)

	instance := &gce.GceInstance{
		GCEStatus: "SUSPENDED",
	}

	sm := &mockStateManager{
		trackedNodes: []state.TrackedNode{
			{Node: node1, State: csn.NodeStateChilling},
		},
	}
	cp := &mockCloudProvider{
		nodeToMig: map[string]*gke.GkeMig{"node1": gkeMig},
		instances: map[gce.GceRef]*gce.GceInstance{
			ref: instance,
		},
	}
	wq := &mockWorkQueue{}
	cfg := Config{MaxInvalidCount: 1}

	r := NewReconciler(sm, cp, wq, cfg)

	chillingStr := string(csn.NodeStateChilling)
	deltaRequests := test.NewMetricDelta(test.ExpectedValue(1), reconcileRequestsTotal, []string{chillingStr, string(csn.NodeStateConsumed)})
	deltaRequests.Init(t)

	r.Reconcile()

	assert.NoError(t, deltaRequests.Verify(t))
}

func TestReconciliationMetrics_ReconcileDurationSeconds(t *testing.T) {
	sm := &mockStateManager{}
	cp := &mockCloudProvider{}
	wq := &mockWorkQueue{}
	cfg := Config{MaxInvalidCount: 1}

	r := NewReconciler(sm, cp, wq, cfg)

	deltaDuration := test.NewMetricDelta(test.Positive(), reconcileDurationSeconds, nil)
	deltaDuration.Init(t)

	r.Reconcile()

	assert.NoError(t, deltaDuration.Verify(t))
}
