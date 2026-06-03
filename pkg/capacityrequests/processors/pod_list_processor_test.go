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

package processors

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/tools/cache"
	cr_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	cr_fake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/clientset/versioned/fake"
	cr_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/listers/internal.autoscaling.gke.io/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
)

type commonState struct {
	nodes                     []*apiv1.Node
	allScheduled              []*apiv1.Pod
	unschedulablePods         []*apiv1.Pod
	capacityRequests          []*cr_types.CapacityRequest
	expectedAllScheduled      int
	expectedUnschedulablePods int
	verifyNodes               map[string]string
}

func TestPodListProcessor(t *testing.T) {
	// nodes
	n1 := BuildTestNode("n1", 100, 1000)
	n2 := BuildTestNode("n2", 100, 1000)
	n1Upcoming := BuildTestNode("n1", 100, 1000)
	n1Upcoming.Annotations = map[string]string{annotations.NodeUpcomingAnnotation: "true"}
	SetNodeReadyState(n1, true, time.Time{})
	SetNodeReadyState(n2, true, time.Time{})
	SetNodeReadyState(n1Upcoming, true, time.Time{})

	// scheduled pods
	p40n2 := BuildTestPod("p40n2", 40, 0)
	p40n2.Spec.NodeName = "n2"
	p10n1 := BuildTestPod("p10n1", 10, 0)
	p10n1.Spec.NodeName = "n1"

	// unscheduled pod
	p400 := BuildTestPod("p400", 400, 0)

	// capacity requests
	cr600 := utils.BuildTestCr("cr600", "600m", "0", []cr_types.CapacityRequestConditionType{})
	cr90 := utils.BuildTestCr("cr90", "90m", "0", []cr_types.CapacityRequestConditionType{})
	cr80 := utils.BuildTestCr("cr80", "80m", "0", []cr_types.CapacityRequestConditionType{})
	crWithRemovalOfp40n2 := utils.BuildTestCrWithRemoval("cr600", "500m", "0", []cr_types.CapacityRequestConditionType{}, []string{"p40n2"})

	testCases := []struct {
		caseName         string
		state            commonState
		verifyPresent    []*apiv1.Pod
		verifyNotPresent []*apiv1.Pod
		verifyNodes      map[string]string
		expectedActions  int
	}{
		{
			caseName: "No Capacity Requests.",
			state: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				allScheduled:              []*apiv1.Pod{p40n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				expectedAllScheduled:      1, // no change
				expectedUnschedulablePods: 1, // no change
				capacityRequests:          []*cr_types.CapacityRequest{},
			},
			verifyPresent:    []*apiv1.Pod{},
			verifyNotPresent: []*apiv1.Pod{},
			expectedActions:  0,
		},
		{
			caseName: "Capacity Request without removal of pods",
			state: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				allScheduled:              []*apiv1.Pod{p40n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				expectedAllScheduled:      1, // no change
				expectedUnschedulablePods: 2, // pod from CR added to unschedulable
				capacityRequests:          []*cr_types.CapacityRequest{cr600},
			},
			verifyPresent:    []*apiv1.Pod{utils.BuildPodFromCr(cr600)},
			verifyNotPresent: []*apiv1.Pod{},
			expectedActions:  0,
		},
		{
			caseName: "Capacity Request currently schedulable",
			state: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				allScheduled:              []*apiv1.Pod{p40n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				capacityRequests:          []*cr_types.CapacityRequest{cr90},
				expectedAllScheduled:      2, // pod from CR added to scheduled,
				expectedUnschedulablePods: 1, // no change
				verifyNodes:               map[string]string{"cr90": "n1"},
			},
			verifyPresent:    []*apiv1.Pod{utils.BuildPodFromCr(cr90)},
			verifyNotPresent: []*apiv1.Pod{},
			expectedActions:  1,
		},
		{
			caseName: "Capacity Request not schedulable if the node it fits onto is upcoming",
			state: commonState{
				nodes:                     []*apiv1.Node{n1Upcoming, n2},
				allScheduled:              []*apiv1.Pod{p40n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				capacityRequests:          []*cr_types.CapacityRequest{cr90},
				expectedAllScheduled:      1, // no change
				expectedUnschedulablePods: 2, // pod from CR added to unschedulable
			},
			verifyPresent:    []*apiv1.Pod{utils.BuildPodFromCr(cr90)},
			verifyNotPresent: []*apiv1.Pod{},
			expectedActions:  0,
		},
		{
			caseName: "Capacity Request with removal of pods",
			state: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				allScheduled:              []*apiv1.Pod{p40n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				capacityRequests:          []*cr_types.CapacityRequest{crWithRemovalOfp40n2},
				expectedAllScheduled:      0, // p40n2 removed from scheduled
				expectedUnschedulablePods: 2, // pod from CR added to unschedulable
			},
			verifyPresent:    []*apiv1.Pod{utils.BuildPodFromCr(crWithRemovalOfp40n2)},
			verifyNotPresent: []*apiv1.Pod{},
			expectedActions:  0,
		},
		{
			caseName: "Multiple Capacity Requests",
			state: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				allScheduled:              []*apiv1.Pod{p40n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				capacityRequests:          []*cr_types.CapacityRequest{cr600, cr90, crWithRemovalOfp40n2},
				expectedAllScheduled:      1, // p40n2 removed from allScheduled, schedulable pod added to scheduled
				expectedUnschedulablePods: 2, // extra one pod from cr added to unschedulable
				verifyNodes:               map[string]string{"cr90": "n1"},
			},
			verifyPresent:    []*apiv1.Pod{utils.BuildPodFromCr(crWithRemovalOfp40n2), utils.BuildPodFromCr(cr600), utils.BuildPodFromCr(cr90)},
			verifyNotPresent: []*apiv1.Pod{p40n2},
			expectedActions:  1,
		},
		{
			caseName: "Multiple Capacity Requests. Both fit on the node, but not at once. Only one marked as schedulable.",
			state: commonState{
				nodes:                     []*apiv1.Node{n1},
				allScheduled:              []*apiv1.Pod{p10n1},
				unschedulablePods:         []*apiv1.Pod{p400},
				capacityRequests:          []*cr_types.CapacityRequest{cr90, cr80},
				expectedAllScheduled:      2, // extra one pod added to schedulable
				expectedUnschedulablePods: 2, // extra one pod added to unschedulable
			},
			verifyPresent:    []*apiv1.Pod{utils.BuildPodFromCr(cr90), utils.BuildPodFromCr(cr80)},
			verifyNotPresent: []*apiv1.Pod{},
			expectedActions:  1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
			autoscalingContext := &context.AutoscalingContext{
				ClusterSnapshot: clusterSnapshot,
			}

			err := clusterSnapshot.SetClusterState(tc.state.nodes, tc.state.allScheduled, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
			assert.NoError(t, err)

			podListProcessor, fakeClient := newPodListProcessorForTesting(t, tc.state.capacityRequests)
			gotUnschedulablePods, err := podListProcessor.Process(autoscalingContext, tc.state.unschedulablePods)
			assert.NoError(t, err)

			gotAllScheduled, err := getAllPodsFromSnapshot(clusterSnapshot)
			assert.NoError(t, err)

			if len(gotUnschedulablePods) != tc.state.expectedUnschedulablePods || len(gotAllScheduled) != tc.state.expectedAllScheduled || err != nil {
				t.Errorf("Test case '%v' failed:\nError podListProcessor.Process() = %v, %v, %v want %v, %v, <nil> ",
					tc.caseName, len(gotUnschedulablePods), len(gotAllScheduled), err, tc.state.expectedUnschedulablePods, tc.state.expectedAllScheduled)
			}
			verifyPods(t, tc.caseName, gotUnschedulablePods, gotAllScheduled, tc.verifyPresent, tc.verifyNotPresent)
			actions := fakeClient.Actions()
			assert.Equal(t, tc.expectedActions, len(actions), "Test case '%v' failed. Wrong number of actions.", tc.caseName)
			if len(actions) > 0 {
				for _, a := range actions {
					assert.True(t, a.Matches("update", "capacityrequests"), "Test case '%v' failed. Unexpected action: %v", tc.caseName, a)
				}
			}
			crToPod := getCrToPodMap(tc.state.capacityRequests, append(gotAllScheduled, gotUnschedulablePods...))
			for cr, pod := range crToPod {
				if node, found := tc.verifyNodes[cr]; found {
					assert.Equal(t, node, pod.Spec.NodeName, "Test case '%v' failed. Expected node %v assigned to cr %v.", tc.caseName, node, cr)
				}
			}
		})
	}
}

func TestPodListProcessorSubsequentRuns(t *testing.T) {
	// nodes
	n1 := BuildTestNode("n1", 100, 1000)
	n2 := BuildTestNode("n2", 100, 1000)
	SetNodeReadyState(n1, true, time.Time{})
	SetNodeReadyState(n2, true, time.Time{})

	// pods
	p40n1 := BuildTestPod("p40n1", 40, 0)
	p40n1.Spec.NodeName = "n1"
	p400 := BuildTestPod("p400", 400, 0)

	// capacity requests
	cr600 := utils.BuildTestCr("cr600", "600m", "0", []cr_types.CapacityRequestConditionType{})
	cr90 := utils.BuildTestCr("cr90", "90m", "0", []cr_types.CapacityRequestConditionType{})
	cr80 := utils.BuildTestCr("cr80", "80m", "0", []cr_types.CapacityRequestConditionType{})
	testCases := []struct {
		caseName           string
		autoscalingContext *context.AutoscalingContext
		firstState         commonState
		secondState        commonState
		verifySameNode     map[string]bool
	}{
		{
			caseName: "Leaves CapacityRequest on same node when another node becomes empty.",
			firstState: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{p40n1},
				capacityRequests:          []*cr_types.CapacityRequest{cr600, cr90},
				expectedUnschedulablePods: 2, // one cr unschedulable
				expectedAllScheduled:      2, // one cr schedulable
				verifyNodes:               map[string]string{"cr90": "n2"},
			},
			secondState: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{}, // p40n1 disappeared, n1 is empty
				capacityRequests:          []*cr_types.CapacityRequest{cr600, cr90},
				expectedUnschedulablePods: 2,                               // one cr unschedulable
				expectedAllScheduled:      1,                               // one cr schedulable
				verifyNodes:               map[string]string{"cr90": "n2"}, // cr90 stays on n2
			},
		},
		{
			caseName: "Leaves Capacity Request on same node when second Capacity Request appears.",
			firstState: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{}, // both nodes empty
				capacityRequests:          []*cr_types.CapacityRequest{cr90},
				expectedUnschedulablePods: 1,
				expectedAllScheduled:      1, // one cr schedulable
			},
			secondState: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{},
				capacityRequests:          []*cr_types.CapacityRequest{cr90, cr80},
				expectedUnschedulablePods: 1,
				expectedAllScheduled:      2, // two crs schedulable
			},
			verifySameNode: map[string]bool{"c2": true},
		},
		{
			caseName: "Moves Capacity Request to unschedulable when pod got scheduled on the node.",
			firstState: commonState{
				nodes:                     []*apiv1.Node{n1},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{}, // both nodes empty
				capacityRequests:          []*cr_types.CapacityRequest{cr90},
				expectedUnschedulablePods: 1,
				expectedAllScheduled:      1, // one cr schedulable
				verifyNodes:               map[string]string{"cr90": "n1"},
			},
			secondState: commonState{
				nodes:                     []*apiv1.Node{n1},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{p40n1},
				capacityRequests:          []*cr_types.CapacityRequest{cr90},
				expectedUnschedulablePods: 2, // cr marked unschedulable
				expectedAllScheduled:      1,
			},
		},
		{
			caseName: "Moves Capacity Request to new node when pod got scheduled on the node.",
			firstState: commonState{
				nodes:                     []*apiv1.Node{n1},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{}, // both nodes empty
				capacityRequests:          []*cr_types.CapacityRequest{cr90},
				expectedUnschedulablePods: 1,
				expectedAllScheduled:      1, // one cr schedulable
				verifyNodes:               map[string]string{"cr90": "n1"},
			},
			secondState: commonState{
				nodes:                     []*apiv1.Node{n1, n2},
				unschedulablePods:         []*apiv1.Pod{p400},
				allScheduled:              []*apiv1.Pod{p40n1},
				capacityRequests:          []*cr_types.CapacityRequest{cr90},
				expectedUnschedulablePods: 1, // cr marked unschedulable
				expectedAllScheduled:      2,
				verifyNodes:               map[string]string{"cr90": "n2"}, // cr moved to n2
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
			autoscalingContext := &context.AutoscalingContext{
				ClusterSnapshot: clusterSnapshot,
			}

			err := clusterSnapshot.SetClusterState(tc.firstState.nodes, tc.firstState.allScheduled, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
			assert.NoError(t, err)

			podListProcessor, _ := newPodListProcessorForTesting(t, tc.firstState.capacityRequests)
			gotUnschedulablePods, err := podListProcessor.Process(autoscalingContext, tc.firstState.unschedulablePods)
			assert.NoError(t, err)
			gotAllScheduled, err := getAllPodsFromSnapshot(clusterSnapshot)
			assert.NoError(t, err)
			if len(gotUnschedulablePods) != tc.firstState.expectedUnschedulablePods || len(gotAllScheduled) != tc.firstState.expectedAllScheduled || err != nil {
				t.Errorf("Test case '%v' failed:\nError podListProcessor.Process(firstState) = %v, %v, %v want %v, %v, <nil> ",
					tc.caseName, len(gotUnschedulablePods), len(gotAllScheduled), err, tc.firstState.expectedUnschedulablePods, tc.firstState.expectedAllScheduled)
			}

			podListProcessor.crLister = newFakeCrsLister(t, tc.secondState.capacityRequests)
			nodeForCr := map[string]string{}
			crToPod := getCrToPodMap(tc.firstState.capacityRequests, append(gotAllScheduled, gotUnschedulablePods...))
			for cr, pod := range crToPod {
				if node, found := tc.firstState.verifyNodes[cr]; found {
					assert.Equal(t, node, pod.Spec.NodeName, "Test case '%v' failed. Expected node %v assigned to cr %v.", tc.caseName, node, cr)
					nodeForCr[cr] = pod.Spec.NodeName
				}
			}

			err = clusterSnapshot.SetClusterState(tc.secondState.nodes, tc.secondState.allScheduled, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
			assert.NoError(t, err)
			gotUnschedulablePods, err = podListProcessor.Process(autoscalingContext, tc.secondState.unschedulablePods)
			assert.NoError(t, err)
			gotAllScheduled, err = getAllPodsFromSnapshot(clusterSnapshot)
			assert.NoError(t, err)
			if len(gotUnschedulablePods) != tc.secondState.expectedUnschedulablePods || len(gotAllScheduled) != tc.secondState.expectedAllScheduled || err != nil {
				t.Errorf("Test case '%v' failed:\nError podListProcessor.Process(secondState) = %v, %v, %v want %v, %v, <nil> ",
					tc.caseName, len(gotUnschedulablePods), len(gotAllScheduled), err, tc.secondState.expectedUnschedulablePods, tc.secondState.expectedAllScheduled)
			}

			crToPod = getCrToPodMap(tc.secondState.capacityRequests, append(gotAllScheduled, gotUnschedulablePods...))
			for cr, pod := range crToPod {
				if node, found := tc.secondState.verifyNodes[cr]; found {
					assert.Equal(t, node, pod.Spec.NodeName, "Test case '%v' failed. Expected node %v assigned to cr %v.", tc.caseName, node, cr)
				}
				if tc.verifySameNode[cr] {
					assert.Equal(t, nodeForCr[cr], pod.Spec.NodeName)
				}
			}
		})
	}
}

func newPodListProcessorForTesting(t *testing.T, crs []*cr_types.CapacityRequest) (*CapacityRequestPodListProcessor, *cr_fake.Clientset) {
	fakeCrsLister := newFakeCrsLister(t, crs)
	fakeClient := cr_fake.NewSimpleClientset()
	status := utils.NewCapacityRequestState(fakeClient)
	return NewCapacityRequestPodListProcessor(status, fakeCrsLister), fakeClient
}

func newFakeCrsLister(t *testing.T, crs []*cr_types.CapacityRequest) cr_lister.CapacityRequestLister {
	crIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, cr := range crs {
		err := crIndexer.Add(cr)
		if err != nil {
			t.Fatalf("Error adding object to cache: %v", err)
		}
	}
	crLister := cr_lister.NewCapacityRequestLister(crIndexer)
	return crLister
}

func verifyPods(t *testing.T, caseName string, unschedulable, allScheduled, verifyPresent, verifyNotPresent []*apiv1.Pod) {
	allPods := append(allScheduled, unschedulable...)

	for _, pod := range verifyNotPresent {
		assert.NotContains(t, allPods, pod, "Test case '%v' failed:\nPod %v/%v should not be present.", caseName, pod.Namespace, pod.Name)
	}
	for _, searchedPod := range verifyPresent {
		assert.True(t, containsPartially(searchedPod, allPods), "Test case '%v' failed:\nPod %v/%v should be present.\nAllPods: %v", caseName, searchedPod.Namespace, searchedPod.Name, allPods)
	}
}

func containsPartially(searchedPod *apiv1.Pod, allPods []*apiv1.Pod) bool {
	for _, pod := range allPods {
		if pod.Namespace == searchedPod.Namespace &&
			strings.HasPrefix(pod.Name, searchedPod.Name) && pod.UID == searchedPod.UID {
			return true
		}
	}
	return false
}

func isPodForCr(cr *cr_types.CapacityRequest, pod *apiv1.Pod) bool {
	return pod.Namespace == cr.Namespace &&
		strings.HasPrefix(pod.Name, "capacity-request") && pod.UID == cr.UID
}

func getCrToPodMap(crs []*cr_types.CapacityRequest, allPods []*apiv1.Pod) map[string]*apiv1.Pod {
	result := map[string]*apiv1.Pod{}
	for _, cr := range crs {
		for _, pod := range allPods {
			if isPodForCr(cr, pod) {
				result[cr.Name] = pod
			}
		}
	}
	return result
}

func getAllPodsFromSnapshot(snapshot clustersnapshot.ClusterSnapshot) ([]*apiv1.Pod, error) {
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		return nil, err
	}
	var pods []*apiv1.Pod
	for _, nodeInfo := range nodeInfos {
		for _, podInfo := range nodeInfo.Pods() {
			pods = append(pods, podInfo.Pod)
		}
	}
	return pods, nil
}
