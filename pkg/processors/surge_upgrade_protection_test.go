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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1/mock"
	clock "k8s.io/utils/clock/testing"
)

var (
	testStartTime = time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)

	n1n2UpdateInfo = &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n1n2",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "n1",
			TargetNode:       "n2",
			Type:             UpgradeType,
			InstanceGroupUrl: "any",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
	n3n4UpdateInfo = &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n3n4",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "n3",
			TargetNode:       "n4",
			Type:             UpgradeType,
			InstanceGroupUrl: "any",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
	n5UpdateInfo = &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n5",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "",
			TargetNode:       "n5",
			Type:             RepairType,
			InstanceGroupUrl: "any",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
	expiredUpdateInfo1 = &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n1n2",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "n1",
			TargetNode:       "n2",
			Type:             UpgradeType,
			InstanceGroupUrl: "any",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(-time.Hour)},
		},
	}
	expiredUpdateInfo2 = &v1alpha1.UpdateInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "n3n4",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:        "n3",
			TargetNode:       "n4",
			Type:             UpgradeType,
			InstanceGroupUrl: "any",
			ValidUntil:       metav1.Time{Time: testStartTime.Add(-time.Hour)},
		},
	}
	n1 = BuildTestNode("n1", 1000, 1000)
	n2 = BuildTestNode("n2", 1000, 1000)
	n3 = BuildTestNode("n3", 1000, 1000)
	n4 = BuildTestNode("n4", 1000, 1000)
	n5 = BuildTestNode("n5", 1000, 1000)
	n6 = BuildTestNode("n6", 1000, 1000)
)

func TestSurgeUpdateScaleDownNodeProcessor(t *testing.T) {
	testCases := []struct {
		desc                        string
		mockReturnObj               []*v1alpha1.UpdateInfo
		mockReturnErr               error
		nodeGroups                  map[string][]*apiv1.Node
		expectedPodDestinations     []*apiv1.Node
		expectedScaleDownCandidates []*apiv1.Node
		expectedFilterError         errors.AutoscalerError
	}{
		{
			"filter out through multiple CRDs",
			[]*v1alpha1.UpdateInfo{n1n2UpdateInfo, n3n4UpdateInfo, n5UpdateInfo},
			nil,
			map[string][]*apiv1.Node{"ng1": {n1, n2, n3, n4, n5, n6}},
			[]*apiv1.Node{n6},
			[]*apiv1.Node{n6},
			nil,
		},
		{
			"filter out through multiple CRDs; one expired",
			[]*v1alpha1.UpdateInfo{n1n2UpdateInfo, expiredUpdateInfo2, n5UpdateInfo},
			nil,
			map[string][]*apiv1.Node{"ng1": {n1, n2, n3, n4, n5, n6}},
			[]*apiv1.Node{n3, n4, n6},
			[]*apiv1.Node{n3, n4, n6},
			nil,
		},
		{
			"extra nodes in CRD",
			[]*v1alpha1.UpdateInfo{n1n2UpdateInfo, n3n4UpdateInfo},
			nil,
			map[string][]*apiv1.Node{"ng1": {n1, n2, n5, n6}},
			[]*apiv1.Node{n5, n6},
			[]*apiv1.Node{n5, n6},
			nil,
		},
		{
			"no CRDs",
			[]*v1alpha1.UpdateInfo{},
			nil,
			map[string][]*apiv1.Node{"ng1": {n1, n2, n3, n4}},
			[]*apiv1.Node{n1, n2, n3, n4},
			[]*apiv1.Node{n1, n2, n3, n4},
			nil,
		},
		{
			"expired CRDs",
			[]*v1alpha1.UpdateInfo{expiredUpdateInfo1, expiredUpdateInfo2},
			nil,
			map[string][]*apiv1.Node{"ng1": {n1, n2, n3, n4}},
			[]*apiv1.Node{n1, n2, n3, n4},
			[]*apiv1.Node{n1, n2, n3, n4},
			nil,
		},
		{
			"surge node from CRD doesn't exist yet",
			[]*v1alpha1.UpdateInfo{n1n2UpdateInfo, n3n4UpdateInfo},
			nil,
			map[string][]*apiv1.Node{"ng1": {n1, n2, n4, n5}},
			[]*apiv1.Node{n5},
			[]*apiv1.Node{n5},
			nil,
		},
		{
			"no nodes",
			[]*v1alpha1.UpdateInfo{n1n2UpdateInfo, n3n4UpdateInfo},
			nil,
			map[string][]*apiv1.Node{},
			[]*apiv1.Node{},
			[]*apiv1.Node{},
			nil,
		},
		{
			"list failure",
			nil,
			fmt.Errorf("list failure"),
			map[string][]*apiv1.Node{"ng1": {n1, n2, n3, n4}},
			nil,
			nil,
			errors.NewAutoscalerError(errors.ApiCallError, "error computing upgrade nodes: error fetching updateInfos: list failure"),
		},
		{
			"multiple node groups",
			[]*v1alpha1.UpdateInfo{n1n2UpdateInfo, n3n4UpdateInfo},
			nil,
			map[string][]*apiv1.Node{"ng1": {n1, n2}, "ng2": {n4, n5}},
			[]*apiv1.Node{n5},
			[]*apiv1.Node{n5},
			nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			up := mock.NewMockUpdateInfoLister(ctrl)

			up.EXPECT().List(labels.Everything()).Return(testCase.mockReturnObj, testCase.mockReturnErr).Times(1)

			testClock := clock.NewFakeClock(testStartTime)
			fetcher := kubernetes.NewUpdateInfoFetcher(up, testClock)
			if testCase.mockReturnErr != nil {
				assert.Error(t, fetcher.Refresh())
				return
			}
			assert.NoError(t, fetcher.Refresh())

			cp := test.NewTestCloudProviderBuilder().Build()

			for ng, ngNodes := range testCase.nodeGroups {
				if ng == "" {
					continue
				}
				cp.AddNodeGroup(ng, 0, len(ngNodes), len(ngNodes))
				for _, node := range ngNodes {
					cp.AddNode(ng, node)
				}
			}
			allNodes := []*apiv1.Node{}
			for _, ngNodes := range testCase.nodeGroups {
				allNodes = append(allNodes, ngNodes...)
			}
			ctx := context.AutoscalingContext{CloudProvider: cp}
			processor := NewSurgeUpgradeScaleDownNodeProcessor(fetcher)

			got, _ := processor.GetPodDestinationCandidates(&ctx, allNodes)
			assert.Equal(t, testCase.expectedPodDestinations, got, testCase.desc)

			got, _ = processor.GetScaleDownCandidates(&ctx, allNodes)
			assert.Equal(t, testCase.expectedScaleDownCandidates, got, testCase.desc)

			ctrl.Finish()
		})
	}
}
