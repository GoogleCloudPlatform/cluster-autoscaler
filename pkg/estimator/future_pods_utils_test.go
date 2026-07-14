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

package estimator

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
)

type testResources struct {
	MilliCPU         int64
	Memory           int64
	EphemeralStorage int64
	GpuCount         int64
}

func testNodeInfo(allocatable, mainPodRequests testResources) *framework.NodeInfo {
	node := test.BuildTestNode("test-node", allocatable.MilliCPU, allocatable.Memory)
	test.AddEphemeralStorageToNode(node, allocatable.EphemeralStorage)
	if allocatable.GpuCount != 0 {
		test.AddGpusToNode(node, allocatable.GpuCount)
	}
	mainRequestsPod := testPod("main-requests-pod", mainPodRequests.MilliCPU, mainPodRequests.Memory, mainPodRequests.EphemeralStorage, mainPodRequests.GpuCount)
	return framework.NewTestNodeInfo(node, mainRequestsPod)
}

func TestFuturePods(t *testing.T) {
	for desc, tc := range map[string]struct {
		emptyPodsCount      int
		customPods          []*apiv1.Pod
		approximateResource gkeprice.Resource
		allocatable         testResources
		mainPodRequests     testResources
		wantFuturePods      int
	}{
		"empty CPU approximate resource": {
			approximateResource: gkeprice.Resource{
				Memory:           10,
				EphemeralStorage: 10,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         100,
				Memory:           900,
				EphemeralStorage: 900,
			},
			wantFuturePods: 10,
		},
		"empty memory approximate resource": {
			approximateResource: gkeprice.Resource{
				MilliCPU:         10,
				EphemeralStorage: 10,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         90,
				Memory:           900,
				EphemeralStorage: 900,
			},
			wantFuturePods: 1,
		},
		"empty eph storage approximate resource": {
			approximateResource: gkeprice.Resource{
				MilliCPU: 10,
				Memory:   10,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         90,
				Memory:           900,
				EphemeralStorage: 900,
			},
			wantFuturePods: 1,
		},
		"not enough CPU cores for future pod": {
			approximateResource: gkeprice.Resource{
				MilliCPU:         100,
				Memory:           10,
				EphemeralStorage: 10,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         90,
				Memory:           900,
				EphemeralStorage: 900,
			},
			wantFuturePods: 0,
		},
		"not enough memory for future pod": {
			approximateResource: gkeprice.Resource{
				MilliCPU:         10,
				Memory:           120,
				EphemeralStorage: 10,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         90,
				Memory:           900,
				EphemeralStorage: 900,
			},
			wantFuturePods: 0,
		},
		"not enough ephemeral storage for future pod": {
			approximateResource: gkeprice.Resource{
				MilliCPU:         10,
				Memory:           10,
				EphemeralStorage: 120,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         90,
				Memory:           900,
				EphemeralStorage: 900,
			},
			wantFuturePods: 0,
		},
		"5 pods could fit CPU, 2 pods memory, 1 ephemeral storage -> 1 future pod": {
			approximateResource: gkeprice.Resource{
				MilliCPU:         10,
				Memory:           10,
				EphemeralStorage: 120,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         50,
				Memory:           980,
				EphemeralStorage: 800,
			},
			wantFuturePods: 1,
		},
		"5 pods could fit CPU, 2 pods memory, 1 ephemeral storage, 5 GPU -> 5 future pods": {
			approximateResource: gkeprice.Resource{
				MilliCPU:         10,
				Memory:           10,
				EphemeralStorage: 120,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
				GpuCount:         6,
			},
			mainPodRequests: testResources{
				MilliCPU:         50,
				Memory:           980,
				EphemeralStorage: 800,
				GpuCount:         1,
			},
			wantFuturePods: 5,
		},
		"5 pods could fit CPU, 2 pods memory, 1 ephemeral storage, 0 GPU -> 0 future pods": {
			approximateResource: gkeprice.Resource{
				MilliCPU:         10,
				Memory:           10,
				EphemeralStorage: 120,
			},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
				GpuCount:         6,
			},
			mainPodRequests: testResources{
				MilliCPU:         50,
				Memory:           980,
				EphemeralStorage: 800,
				GpuCount:         6,
			},
			wantFuturePods: 0,
		},
		"empty approximate resources": {
			approximateResource: gkeprice.Resource{},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         50,
				Memory:           900,
				EphemeralStorage: 800,
			},
			wantFuturePods: maxSupportedPodsPerNode - 1,
		},
		"minimal requirements, trim future pods to max supported": {
			approximateResource: gkeprice.Resource{
				MilliCPU: 1,
				Memory:   1,
			},
			mainPodRequests: testResources{
				MilliCPU: 50,
				Memory:   900,
			},
			allocatable: testResources{
				MilliCPU:         1000,
				Memory:           10000,
				EphemeralStorage: 100000,
			},
			wantFuturePods: maxSupportedPodsPerNode - 1,
		},
		"include currentPodsCount when hitting the maxSupportedLimit": {
			approximateResource: gkeprice.Resource{
				MilliCPU: 1,
				Memory:   1,
			},
			mainPodRequests: testResources{
				MilliCPU: 50,
				Memory:   900,
			},
			emptyPodsCount: 200,
			allocatable: testResources{
				MilliCPU:         1000,
				Memory:           10000,
				EphemeralStorage: 100000,
			},
			wantFuturePods: maxSupportedPodsPerNode - 200 - 1,
		},
		"handle '0' resource request, some pods can fit": {
			approximateResource: gkeprice.Resource{
				MilliCPU: 0,
				Memory:   0,
			},
			mainPodRequests: testResources{
				MilliCPU: 500,
				Memory:   500,
			},
			emptyPodsCount: 200,
			allocatable: testResources{
				MilliCPU:         1000,
				Memory:           10000,
				EphemeralStorage: 100000,
			},
			wantFuturePods: maxSupportedPodsPerNode - 200 - 1,
		},
		"handle allocatable < requested, no future pods": {
			emptyPodsCount: 1,
			approximateResource: gkeprice.Resource{
				MilliCPU: 100,
				Memory:   100,
			},
			mainPodRequests: testResources{
				MilliCPU: 10000,
				Memory:   5000,
			},
			allocatable: testResources{
				MilliCPU:         1000,
				Memory:           10000,
				EphemeralStorage: 100000,
			},
			wantFuturePods: 0,
		},
		"non lookahead pods request resources": {
			customPods: []*apiv1.Pod{test.BuildTestPod("n1", 500, 500)},
			approximateResource: gkeprice.Resource{
				MilliCPU: 100,
				Memory:   100,
			},
			mainPodRequests: testResources{},
			allocatable: testResources{
				MilliCPU: 1000,
				Memory:   1000,
			},
			wantFuturePods: 5,
		},
		"exclude lookahead pods from requested resources": {
			customPods: []*apiv1.Pod{lookaheadbuffer.BuildTestLookaheadPod("", 500, 500)},
			approximateResource: gkeprice.Resource{
				MilliCPU: 100,
				Memory:   100,
			},
			mainPodRequests: testResources{},
			allocatable: testResources{
				MilliCPU: 1000,
				Memory:   1000,
			},
			wantFuturePods: 10,
		},
	} {
		tc := tc
		t.Run(desc, func(t *testing.T) {
			nodeInfo := testNodeInfo(tc.allocatable, tc.mainPodRequests)
			for _, pod := range createPods(tc.emptyPodsCount, 0, 0, 0) {
				nodeInfo.AddPod(framework.NewPodInfo(pod, nil))
			}
			for _, pod := range tc.customPods {
				nodeInfo.AddPod(framework.NewPodInfo(pod, nil))
			}
			futurePods := getFuturePods(*nodeInfo, tc.approximateResource)
			if futurePods != tc.wantFuturePods {
				t.Errorf("Unexepcted getFuturesPods(), got: %d, want: %d", futurePods, tc.wantFuturePods)
			}
		})
	}
}

func TestFuturePodsWithDefault(t *testing.T) {
	for desc, tc := range map[string]struct {
		currentPodsCount    int
		approximateResource gkeprice.Resource
		allocatable         testResources
		mainPodRequests     testResources
		wantFuturePods      int
	}{
		"empty approximate resources": {
			approximateResource: gkeprice.Resource{},
			allocatable: testResources{
				MilliCPU:         100,
				Memory:           1000,
				EphemeralStorage: 1000,
			},
			mainPodRequests: testResources{
				MilliCPU:         50,
				Memory:           900,
				EphemeralStorage: 800,
			},
			wantFuturePods: 0,
		},
		"handle '0' resource request, no more pods can fit": {
			currentPodsCount: 200,
			approximateResource: gkeprice.Resource{
				MilliCPU: 0,
				Memory:   0,
			},
			mainPodRequests: testResources{
				MilliCPU: 900,
				Memory:   900,
			},
			allocatable: testResources{
				MilliCPU:         1000,
				Memory:           10000,
				EphemeralStorage: 100000,
			},
			wantFuturePods: 0,
		},
		"handle '0' resource request, some pods can fit": {
			currentPodsCount: 200,
			approximateResource: gkeprice.Resource{
				MilliCPU: 0,
				Memory:   0,
			},
			mainPodRequests: testResources{
				MilliCPU: 500,
				Memory:   500 * units.GB,
			},
			allocatable: testResources{
				MilliCPU:         1500,
				Memory:           10000 * units.GB,
				EphemeralStorage: 100000 * units.GB,
			},
			wantFuturePods: 2,
		},
		"minimal requirements, trim future pods to max supported": {
			approximateResource: gkeprice.Resource{
				MilliCPU: 1,
				Memory:   1,
			},
			mainPodRequests: testResources{
				MilliCPU: 50,
				Memory:   900,
			},
			allocatable: testResources{
				MilliCPU:         1000,
				Memory:           10000,
				EphemeralStorage: 100000,
			},
			wantFuturePods: maxSupportedPodsPerNode - 1,
		},
	} {
		tc := tc
		t.Run(desc, func(t *testing.T) {
			nodeInfo := testNodeInfo(tc.allocatable, tc.mainPodRequests)
			pods := createPods(tc.currentPodsCount, 0, 0, 0)
			for _, pod := range pods {
				nodeInfo.AddPod(framework.NewPodInfo(pod, nil))
			}
			futurePods := getFuturePodsWithDefaultResources(*nodeInfo, tc.approximateResource)
			if futurePods != tc.wantFuturePods {
				t.Errorf("Unexepected getFuturesPods(), got: %d, want: %d", futurePods, tc.wantFuturePods)
			}
		})
	}
}
