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

package gke

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	clock "k8s.io/utils/clock/testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	kube "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
	v1alpha1mock "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1/mock"
)

var (
	testStartTime = time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)

	n1 = BuildTestNode("n1", 1000, 1000)
	n2 = BuildTestNode("n2", 1000, 1000)
	n3 = BuildTestNode("n3", 1000, 1000)
	n4 = BuildTestNode("n4", 1000, 1000)

	migInQuestionId = createTestMigWithNodes("mig-in-question", nil).Id()
)

func createTestMigWithNodes(name string, nodeNames []string) *GkeMig {
	var nodes []gce.GceInstance
	for _, name := range nodeNames {
		nodes = append(nodes, gce.GceInstance{Instance: cloudprovider.Instance{Id: fmt.Sprintf("instance/%s", name)}})
	}

	managerMock := &GkeManagerMock{}
	managerMock.On("GetMigNodes", mock.Anything).Return(nodes, nil)

	return NewTestGkeMigBuilder().SetGceRefName(name).SetGkeManager(managerMock).SetExist(true).Build()
}

func n1n2UpdateInfo(migId string) *v1alpha1.UpdateInfo {
	return &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n1n2",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "n1",
			TargetNode:       "n2",
			InstanceGroupUrl: migId,
			Type:             "Upgrade",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
}

func n3n4UpdateInfo(migId string) *v1alpha1.UpdateInfo {
	return &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n3n4",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "n3",
			TargetNode:       "n4",
			InstanceGroupUrl: migId,
			Type:             "Upgrade",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
}

func expiredUpdateInfo(migId string) *v1alpha1.UpdateInfo {
	return &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n3n4",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "n3",
			TargetNode:       "n4",
			InstanceGroupUrl: migId,
			Type:             "Upgrade",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(-time.Hour)},
		},
	}
}

func TestSurgeResourceTracker_GetSurgeNodesInNodeGroup(t *testing.T) {
	testCases := []struct {
		desc              string
		nodesInNg         []string
		nodes             []*apiv1.Node
		mockUpdateInfos   []*v1alpha1.UpdateInfo
		mockErr           error
		expectedSurgeNode int
		expectedError     bool
	}{
		{
			desc:              "n1 and n2 are surge nodes",
			nodesInNg:         []string{"n1", "n2", "n3", "n4"},
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo(migInQuestionId), n3n4UpdateInfo(migInQuestionId)},
			expectedSurgeNode: 2,
			expectedError:     false,
		},
		{
			desc:              "n1 is surge node; additional expired updateInfo",
			nodesInNg:         []string{"n1", "n2", "n3", "n4"},
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo(migInQuestionId), expiredUpdateInfo(migInQuestionId)},
			expectedSurgeNode: 1,
			expectedError:     false,
		},
		{
			desc:              "n1 is surge node",
			nodesInNg:         []string{"n1", "n2", "n4"},
			nodes:             []*apiv1.Node{n1, n2, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo(migInQuestionId), n3n4UpdateInfo(migInQuestionId)},
			expectedSurgeNode: 1,
			expectedError:     false,
		},
		{
			desc:              "n1 is surge node; no target node",
			nodesInNg:         []string{"n1", "n4"},
			nodes:             []*apiv1.Node{n1, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo(migInQuestionId), n3n4UpdateInfo(migInQuestionId)},
			expectedSurgeNode: 0,
			expectedError:     false,
		},
		{
			desc:              "no surge nodes",
			nodesInNg:         []string{"n1", "n2", "n3", "n4"},
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   nil,
			expectedSurgeNode: 0,
			expectedError:     false,
		},
		{
			desc:              "no surge nodes; no nodes",
			nodesInNg:         []string{},
			nodes:             []*apiv1.Node{},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo(migInQuestionId), n3n4UpdateInfo(migInQuestionId)},
			expectedSurgeNode: 0,
			expectedError:     false,
		},
		{
			desc:              "no surge nodes",
			nodesInNg:         []string{"n1", "n2", "n3", "n4"},
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   nil,
			mockErr:           fmt.Errorf("some error"),
			expectedSurgeNode: 0,
			expectedError:     true,
		},
		{
			desc:              "some surge nodes outside current mig",
			nodesInNg:         []string{"n1", "n2"},
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo(migInQuestionId), n3n4UpdateInfo("different-mig")},
			mockErr:           nil,
			expectedSurgeNode: 1,
			expectedError:     false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			up := v1alpha1mock.NewMockUpdateInfoLister(ctrl)

			up.EXPECT().List(labels.Everything()).Return(tc.mockUpdateInfos, tc.mockErr).Times(1)
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			processor := NewMockProcessor()
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
			testClock := clock.NewFakeClock(testStartTime)
			tracker := NewSurgeUpgradeResourceTracker(processor, kube.NewTestNodeLister(tc.nodes), kubernetes.NewUpdateInfoFetcher(up, testClock))
			if tc.expectedError {
				assert.Error(t, tracker.Refresh())
				return
			}
			assert.NoError(t, tracker.Refresh())

			migInQuestion := createTestMigWithNodes("mig-in-question", tc.nodesInNg)
			gotSurge, _ := tracker.SurgeNodesInMIG(migInQuestion)
			assert.Equal(t, tc.expectedSurgeNode, gotSurge)
		})
	}
}

func TestSurgeResourceTracker_GetSurgeResources(t *testing.T) {
	testCases := []struct {
		desc              string
		nodes             []*apiv1.Node
		mockUpdateInfos   []*v1alpha1.UpdateInfo
		mockErr           error
		expectedResources map[string]int64
		expectedError     bool
	}{
		{
			desc:              "n1 and n2 are surge nodes",
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo("some-mig"), n3n4UpdateInfo("some-mig")},
			expectedResources: map[string]int64{"cpu": 2, "memory": 2000},
			expectedError:     false,
		},
		{
			desc:              "n1 is surge node",
			nodes:             []*apiv1.Node{n1, n2, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo("some-mig"), n3n4UpdateInfo("some-mig")},
			expectedResources: map[string]int64{"cpu": 1, "memory": 1000},
			expectedError:     false,
		},
		{
			desc:              "n1 is surge node; no target node",
			nodes:             []*apiv1.Node{n1, n4},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo("some-mig"), n3n4UpdateInfo("some-mig")},
			expectedResources: map[string]int64{"cpu": 0, "memory": 0},
			expectedError:     false,
		},
		{
			desc:              "no surge nodes",
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   nil,
			expectedResources: map[string]int64{"cpu": 0, "memory": 0},
			expectedError:     false,
		},
		{
			desc:              "no surge nodes; no nodes",
			nodes:             []*apiv1.Node{},
			mockUpdateInfos:   []*v1alpha1.UpdateInfo{n1n2UpdateInfo("some-mig"), n3n4UpdateInfo("some-mig")},
			expectedResources: map[string]int64{"cpu": 0, "memory": 0},
			expectedError:     false,
		},
		{
			desc:              "no surge nodes",
			nodes:             []*apiv1.Node{n1, n2, n3, n4},
			mockUpdateInfos:   nil,
			mockErr:           fmt.Errorf("some error"),
			expectedResources: nil,
			expectedError:     true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			up := v1alpha1mock.NewMockUpdateInfoLister(ctrl)

			up.EXPECT().List(labels.Everything()).Return(tc.mockUpdateInfos, tc.mockErr).Times(1)
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			testClock := clock.NewFakeClock(testStartTime)
			fetcher := kubernetes.NewUpdateInfoFetcher(up, testClock)
			processor := NewMockProcessor()
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
			tracker := NewSurgeUpgradeResourceTracker(processor, kube.NewTestNodeLister(tc.nodes), fetcher)
			if tc.expectedError {
				assert.Error(t, tracker.Refresh())
				return
			}
			assert.NoError(t, tracker.Refresh())

			gotSurgeResources, _ := tracker.GetSurgeResources(func(node *apiv1.Node) (group cloudprovider.NodeGroup, e error) {
				return nil, nil
			})
			assert.Equal(t, tc.expectedResources, gotSurgeResources)
		})
	}
}

func TestSurgeResourceTracker_ExcludeFromTracking(t *testing.T) {
	testCases := []struct {
		name            string
		nodes           []*apiv1.Node
		mockUpdateInfos []*v1alpha1.UpdateInfo
		nodeToCheck     *apiv1.Node
		want            bool
	}{
		{
			name:            "node-surge",
			nodes:           []*apiv1.Node{n1, n2},
			mockUpdateInfos: []*v1alpha1.UpdateInfo{n1n2UpdateInfo("mig1")},
			nodeToCheck:     n1,
			want:            true,
		},
		{
			name:            "node-target",
			nodes:           []*apiv1.Node{n1, n2},
			mockUpdateInfos: []*v1alpha1.UpdateInfo{n1n2UpdateInfo("mig1")},
			nodeToCheck:     n2,
			want:            false,
		},
		{
			name:            "node-unrelated",
			nodes:           []*apiv1.Node{n1, n2, n3},
			mockUpdateInfos: []*v1alpha1.UpdateInfo{n1n2UpdateInfo("mig1")},
			nodeToCheck:     n3,
			want:            false,
		},
		{
			name:            "surge-does-not-exist",
			nodes:           []*apiv1.Node{n1},
			mockUpdateInfos: []*v1alpha1.UpdateInfo{n1n2UpdateInfo("mig1")},
			nodeToCheck:     n1,
			want:            false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			lister := v1alpha1mock.NewMockUpdateInfoLister(ctrl)

			lister.EXPECT().List(labels.Everything()).Return(tc.mockUpdateInfos, nil).Times(1)

			testClock := clock.NewFakeClock(testStartTime)
			fetcher := kubernetes.NewUpdateInfoFetcher(lister, testClock)
			tracker := NewSurgeUpgradeResourceTracker(nil, kube.NewTestNodeLister(tc.nodes), fetcher)
			assert.NoError(t, tracker.Refresh())

			got := tracker.ExcludeFromTracking(tc.nodeToCheck)
			assert.Equal(t, tc.want, got)
		})
	}
}
