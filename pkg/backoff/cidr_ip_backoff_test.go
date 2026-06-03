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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestCidrIpBackoff(t *testing.T) {
	backoffDuration := 2 * time.Second
	maxBackoffDuration := 5 * time.Second
	minTimeTick := 1 * time.Second

	podIpv4CidrBlock1 := "1.1.1.1/24"
	podIpv4CidrBlock2 := "2.2.2.2/16"
	podIpv4CidrBlock3 := "3.3.3.3/32"
	node := test.BuildTestNode("test-node", 1000, 1000)
	nodeInfo := framework.NewTestNodeInfo(node)

	testCases := []struct {
		name                 string
		errsPassedToBackoff  []cloudprovider.InstanceErrorInfo
		subnets              []string
		podIpv4CidrBlocks    []string
		wantBackoffStatuses  []backoff.Status
		wantBackoffDurations []time.Duration
	}{
		{
			name:                 "Passing one random not ip space exhaustion error",
			errsPassedToBackoff:  []cloudprovider.InstanceErrorInfo{randomError},
			podIpv4CidrBlocks:    []string{podIpv4CidrBlock1},
			wantBackoffStatuses:  []backoff.Status{noBackoff},
			wantBackoffDurations: []time.Duration{0 * time.Second},
		},
		{
			name:                "Passing one ip space exhaustion error",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{ipSpaceExhaustedError(podIpv4CidrBlock2)},
			podIpv4CidrBlocks:   []string{podIpv4CidrBlock2},
			wantBackoffStatuses: []backoff.Status{
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock2),
			},
			wantBackoffDurations: []time.Duration{2 * time.Second},
		},
		{
			name:                "Passing first ip space exhaustion error, then 2 random ones with same cidr block",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{ipSpaceExhaustedError(podIpv4CidrBlock3), randomError, randomError},
			podIpv4CidrBlocks:   []string{podIpv4CidrBlock3, podIpv4CidrBlock3, podIpv4CidrBlock3},
			wantBackoffStatuses: []backoff.Status{
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock3),
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock3),
				noBackoff,
			},
			wantBackoffDurations: []time.Duration{2 * time.Second, 0 * time.Second, 0 * time.Second},
		},
		{
			name: "Passing 3 ip space exhaustions errors in a row with same cidr blocks",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{
				ipSpaceExhaustedError(podIpv4CidrBlock3),
				ipSpaceExhaustedError(podIpv4CidrBlock3),
				ipSpaceExhaustedError(podIpv4CidrBlock3),
			},
			podIpv4CidrBlocks: []string{podIpv4CidrBlock3, podIpv4CidrBlock3, podIpv4CidrBlock3},
			wantBackoffStatuses: []backoff.Status{
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock3),
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock3),
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock3),
			},
			wantBackoffDurations: []time.Duration{2 * time.Second, 4 * time.Second, 5 * time.Second},
		},
		{
			name: "Passing 3 ip space exhaustions errors in a row with different cidr blocks",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{
				ipSpaceExhaustedError(podIpv4CidrBlock1),
				ipSpaceExhaustedError(podIpv4CidrBlock2),
				ipSpaceExhaustedError(podIpv4CidrBlock3),
			},
			podIpv4CidrBlocks: []string{podIpv4CidrBlock1, podIpv4CidrBlock2, podIpv4CidrBlock3},
			wantBackoffStatuses: []backoff.Status{
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock1),
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock2),
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock3),
			},
			wantBackoffDurations: []time.Duration{2 * time.Second, 2 * time.Second, 2 * time.Second},
		},
		{
			name: "CIDR in error other than node groups pods CIDR, backoff on subnetwork",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{
				ipSpaceExhaustedError("10.0.0.1/20"),
			},
			podIpv4CidrBlocks:    []string{podIpv4CidrBlock1},
			wantBackoffStatuses:  []backoff.Status{backoffWithIPSpaceExhaustedError("10.0.0.1/20")},
			wantBackoffDurations: []time.Duration{2 * time.Second},
		},
		{
			name: "Multiple backoffs on subnet",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{
				ipSpaceExhaustedError("10.0.0.1/20"),
				ipSpaceExhaustedError("10.0.0.1/20"),
				ipSpaceExhaustedError("10.0.0.1/20"),
			},
			podIpv4CidrBlocks: []string{
				podIpv4CidrBlock1,
				podIpv4CidrBlock2,
				podIpv4CidrBlock3,
			},
			wantBackoffStatuses: []backoff.Status{
				backoffWithIPSpaceExhaustedError("10.0.0.1/20"),
				backoffWithIPSpaceExhaustedError("10.0.0.1/20"),
				backoffWithIPSpaceExhaustedError("10.0.0.1/20"),
			},
			wantBackoffDurations: []time.Duration{2 * time.Second, 4 * time.Second, 5 * time.Second},
		},
		{
			name: "Multiple backoff on different subnets",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{
				ipSpaceExhaustedError("10.0.0.0/24"),
				ipSpaceExhaustedError("10.0.1.0/24"),
				ipSpaceExhaustedError("10.0.1.0/24"),
			},
			subnets:           []string{"subnet1", "subnet2", "subnet2"},
			podIpv4CidrBlocks: []string{podIpv4CidrBlock1, podIpv4CidrBlock2, podIpv4CidrBlock3},
			wantBackoffStatuses: []backoff.Status{
				backoffWithIPSpaceExhaustedError("10.0.0.0/24"),
				backoffWithIPSpaceExhaustedError("10.0.1.0/24"),
				backoffWithIPSpaceExhaustedError("10.0.1.0/24"),
			},
			wantBackoffDurations: []time.Duration{2 * time.Second, 2 * time.Second, 4 * time.Second},
		},
		{
			name: "Mixed backoff on subnet and pod range",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{
				ipSpaceExhaustedError("10.0.0.1/20"),
				ipSpaceExhaustedError(podIpv4CidrBlock1),
				ipSpaceExhaustedError("10.0.0.1/20"),
				ipSpaceExhaustedError(podIpv4CidrBlock1),
			},
			podIpv4CidrBlocks: []string{
				podIpv4CidrBlock1,
				podIpv4CidrBlock1,
				podIpv4CidrBlock1,
				podIpv4CidrBlock1,
			},
			wantBackoffStatuses: []backoff.Status{
				backoffWithIPSpaceExhaustedError("10.0.0.1/20"),
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock1),
				backoffWithIPSpaceExhaustedError("10.0.0.1/20"),
				backoffWithIPSpaceExhaustedError(podIpv4CidrBlock1),
			},
			wantBackoffDurations: []time.Duration{2 * time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second},
		},
	}

	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			managerMock := &gke.GkeManagerMock{}
			nodeGroupBuilder := gke.NewTestGkeMigBuilder().SetGkeManager(managerMock)
			cidrIpBackoff := NewCidrIpBackoff(backoffDuration, maxBackoffDuration, NodeGroupBackoffResetTimeout)
			currentTime := time.Now()

			nextTime := currentTime
			for i, err := range testcase.errsPassedToBackoff {
				subnetwork := "default"
				if len(testcase.subnets) > 0 {
					subnetwork = testcase.subnets[i]
				}
				testSpec := &gkeclient.NodePoolSpec{
					PodIpv4CidrBlock: testcase.podIpv4CidrBlocks[i],
					Subnetwork:       subnetwork,
				}
				nodeGroup := nodeGroupBuilder.SetSpec(testSpec).Build()

				endTime := cidrIpBackoff.Backoff(nodeGroup, nodeInfo, err, currentTime)

				gotBackoffDuration := 0 * time.Second

				if endTime.After(currentTime) {
					gotBackoffDuration = endTime.Sub(currentTime)
					nextTime = endTime
				} else {
					nextTime = currentTime.Add(minTimeTick)
				}

				backoffStatus := cidrIpBackoff.BackoffStatus(nodeGroup, nodeInfo, currentTime)

				assert.Equal(t, testcase.wantBackoffStatuses[i], backoffStatus)
				assert.Equal(t, testcase.wantBackoffDurations[i], gotBackoffDuration)

				currentTime = nextTime
			}
		})
	}
}

func TestCidrIpBackoff_DifferentNodeGroups(t *testing.T) {
	backoffDuration := 2 * time.Second
	maxBackoffDuration := 5 * time.Second
	node := test.BuildTestNode("test-node", 1000, 1000)
	nodeInfo := framework.NewTestNodeInfo(node)

	podIpv4CidrBlock1 := "1.1.1.1/24"
	podIpv4CidrBlock2 := "2.2.2.2/16"
	subnet1 := "subnet1"
	subnet2 := "subnet2"

	testCases := []struct {
		name               string
		backedOffNodeGroup testMIG
		passedErr          cloudprovider.InstanceErrorInfo
		checkedNodeGroup   testMIG
		wantBackedOff      bool
	}{
		{
			name: "backed off on pod range, MIG with different pod range not backed off",
			backedOffNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock1,
				subnetwork:       subnet1,
				exists:           true,
			},
			passedErr: ipSpaceExhaustedError(podIpv4CidrBlock1),
			checkedNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock2,
				subnetwork:       subnet1,
				exists:           true,
			},
			wantBackedOff: false,
		},
		{
			name: "backed off on pod range, MIG with the same pod range backed off",
			backedOffNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock1,
				subnetwork:       subnet1,
				exists:           true,
			},
			passedErr: ipSpaceExhaustedError(podIpv4CidrBlock1),
			checkedNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock1,
				subnetwork:       subnet1,
				exists:           true,
			},
			wantBackedOff: true,
		},
		{
			name: "backed off on subnet, MIG with different subnet not backed off",
			backedOffNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock1,
				subnetwork:       subnet1,
				exists:           true,
			},
			passedErr: ipSpaceExhaustedError("10.0.0.0/20"),
			checkedNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock2,
				subnetwork:       subnet2,
				exists:           true,
			},
			wantBackedOff: false,
		},
		{
			name: "backed off on subnet, MIG with the same subnet backed off",
			backedOffNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock1,
				subnetwork:       subnet1,
				exists:           true,
			},
			passedErr: ipSpaceExhaustedError("10.0.0.0/20"),
			checkedNodeGroup: testMIG{
				podIpv4CidrBlock: podIpv4CidrBlock2,
				subnetwork:       subnet1,
				exists:           true,
			},
			wantBackedOff: true,
		},
		{
			name: "backed off on NAP, not existing MIG backed off",
			backedOffNodeGroup: testMIG{
				autoprovisioned: true,
				subnetwork:      subnet1,
				exists:          true,
			},
			passedErr: ipSpaceExhaustedError("10.0.0.0/20"),
			checkedNodeGroup: testMIG{
				autoprovisioned: true,
				exists:          false,
			},
			wantBackedOff: true,
		},
		{
			name: "backed off on NAP, MIG with the same subnet backed off",
			backedOffNodeGroup: testMIG{
				autoprovisioned:  true,
				subnetwork:       subnet1,
				podIpv4CidrBlock: podIpv4CidrBlock1,
				exists:           true,
			},
			passedErr: ipSpaceExhaustedError("10.0.0.0/20"),
			checkedNodeGroup: testMIG{
				subnetwork:       subnet1,
				podIpv4CidrBlock: podIpv4CidrBlock2,
				exists:           true,
			},
			wantBackedOff: true,
		},
		{
			name: "backed off on NAP, MIG with the same pod range backed off",
			backedOffNodeGroup: testMIG{
				subnetwork:       subnet1,
				podIpv4CidrBlock: podIpv4CidrBlock1,
				exists:           true,
			},
			passedErr: ipSpaceExhaustedError(podIpv4CidrBlock1),
			checkedNodeGroup: testMIG{
				subnetwork:       subnet1,
				podIpv4CidrBlock: podIpv4CidrBlock1,
				exists:           true,
			},
			wantBackedOff: true,
		},
		{
			name: "backed off on NAP, different MIG not backed off",
			backedOffNodeGroup: testMIG{
				autoprovisioned: true,
				subnetwork:      subnet1,
				exists:          true,
			},
			passedErr: ipSpaceExhaustedError("10.0.0.0/20"),
			checkedNodeGroup: testMIG{
				subnetwork: subnet2,
				exists:     true,
			},
			wantBackedOff: false,
		},
		{
			name: "node pool with running instances, NAP not backed off",
			backedOffNodeGroup: testMIG{
				autoprovisioned:  true,
				subnetwork:       subnet1,
				podIpv4CidrBlock: podIpv4CidrBlock1,
				exists:           true,
				instances: []gce.GceInstance{
					{
						Instance: cloudprovider.Instance{
							Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceRunning},
						},
					},
				},
			},
			passedErr: ipSpaceExhaustedError(podIpv4CidrBlock1),
			checkedNodeGroup: testMIG{
				autoprovisioned: true,
				exists:          false,
			},
			wantBackedOff: false,
		},
		{
			name: "node pool only with creating instances, NAP backed off",
			backedOffNodeGroup: testMIG{
				autoprovisioned:  true,
				subnetwork:       subnet1,
				podIpv4CidrBlock: podIpv4CidrBlock1,
				exists:           true,
				instances: []gce.GceInstance{
					{
						Instance: cloudprovider.Instance{
							Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
						},
					},
				},
			},
			passedErr: ipSpaceExhaustedError(podIpv4CidrBlock1),
			checkedNodeGroup: testMIG{
				autoprovisioned: true,
				exists:          false,
			},
			wantBackedOff: true,
		},
	}

	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			managerMock := &gke.GkeManagerMock{}
			nodeGroupBuilder := gke.NewTestGkeMigBuilder().SetGkeManager(managerMock)
			cidrIpBackoff := NewCidrIpBackoff(backoffDuration, maxBackoffDuration, NodeGroupBackoffResetTimeout)
			currentTime := time.Now()

			backedOffSpec := &gkeclient.NodePoolSpec{
				PodIpv4CidrBlock: testcase.backedOffNodeGroup.podIpv4CidrBlock,
				Subnetwork:       testcase.backedOffNodeGroup.subnetwork,
			}
			backedOffNodeGroup := nodeGroupBuilder.
				SetSpec(backedOffSpec).
				SetAutoprovisioned(testcase.backedOffNodeGroup.autoprovisioned).
				SetExist(testcase.backedOffNodeGroup.exists).
				Build()
			if testcase.backedOffNodeGroup.autoprovisioned {
				managerMock.On("GetMigNodes", backedOffNodeGroup).Return(testcase.backedOffNodeGroup.instances, nil).Once()
			}

			cidrIpBackoff.Backoff(backedOffNodeGroup, nodeInfo, testcase.passedErr, currentTime)

			checkedSpec := &gkeclient.NodePoolSpec{
				PodIpv4CidrBlock: testcase.checkedNodeGroup.podIpv4CidrBlock,
				Subnetwork:       testcase.checkedNodeGroup.subnetwork,
			}
			checkedNodeGroup := nodeGroupBuilder.
				SetSpec(checkedSpec).
				SetAutoprovisioned(testcase.checkedNodeGroup.autoprovisioned).
				SetExist(testcase.checkedNodeGroup.exists).
				Build()

			backoffStatus := cidrIpBackoff.BackoffStatus(checkedNodeGroup, nodeInfo, currentTime)
			assert.Equal(t, testcase.wantBackedOff, backoffStatus.IsBackedOff)
		})
	}
}

type testMIG struct {
	subnetwork       string
	podIpv4CidrBlock string
	autoprovisioned  bool
	exists           bool
	instances        []gce.GceInstance
}
