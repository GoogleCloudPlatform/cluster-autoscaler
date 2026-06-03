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

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestRealAndInjectedBackoffsTrigger(t *testing.T) {
	mgr := &gke.GkeManagerMock{}
	injected := testMig(mgr, "np-mig1-temporary", "us-central1-c", "np", false)
	real := testMig(mgr, "np-mig1", "us-central1-c", "np", true)
	mgr.SetInjectedMig(real, injected)

	t1 := time.Now()
	t2 := t1.Add(InitialNodeGroupBackoffDuration - time.Second)

	backoff := createGkeBackoff()

	backoff.Backoff(real, nil, randomError, t1)
	assert.Equal(t, backoffWithRandomError, backoff.BackoffStatus(injected, nil, t2))
	assert.Equal(t, backoffWithRandomError, backoff.BackoffStatus(real, nil, t2))
}

func TestInjectedBackoffsStack(t *testing.T) {
	mgr := &gke.GkeManagerMock{}
	mgr.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&v1.Node{}), nil)
	injected1 := testMig(mgr, "np-mig1-temporary", "us-central1-c", "np", false)
	injected2 := testMig(mgr, "np-mig2-temporary", "us-central1-c", "np", false)
	real1 := testMig(mgr, "np-mig1", "us-central1-c", "np", true)
	real2 := testMig(mgr, "np-mig2", "us-central1-c", "np", true)
	mgr.SetInjectedMig(real1, injected1)
	mgr.SetInjectedMig(real2, injected2)

	t1 := time.Now()
	t2 := t1.Add(InitialNodeGroupBackoffDuration + time.Second)
	t3 := t2.Add(InitialNodeGroupBackoffDuration + time.Second)

	backoff := createGkeBackoff()

	// real1 and real2 are different MIGs, so they use separate backoffs,
	// both expire after InitialNodeGroupBackoffDuration.
	// injected1 and injected2 have the same spec, so the subsequent backoffs will use longer durations
	backoff.Backoff(real1, nil, randomError, t1)
	backoff.Backoff(real2, nil, randomError, t2)
	assert.Equal(t, backoffWithRandomError, backoff.BackoffStatus(injected1, nil, t3))
	assert.Equal(t, backoffWithRandomError, backoff.BackoffStatus(injected2, nil, t3))
	assert.Equal(t, false, backoff.BackoffStatus(real1, nil, t3).IsBackedOff)
	assert.Equal(t, false, backoff.BackoffStatus(real2, nil, t3).IsBackedOff)
}

func testMig(mgr gke.GkeManager, name, zone, npName string, exists bool) *gke.GkeMig {
	return gke.NewTestGkeMigBuilder().
		SetMaxSize(10).
		SetAutoprovisioned(true).
		SetExist(exists).
		SetExtraResources(extraResources).
		SetGkeManager(mgr).
		SetGceRef(gce.GceRef{
			Project: "proj",
			Zone:    zone,
			Name:    name,
		}).
		SetNodePoolName(npName).
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-2",
			Labels:      map[string]string{"l1": "l1v"},
		}).Build()
}
