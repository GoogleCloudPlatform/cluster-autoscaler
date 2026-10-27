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

package nodeinfosprovider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func createSimpleMig(name, nodePoolName string) *gke.GkeMig {
	return gke.NewTestGkeMigBuilder().
		SetGceRefName(name).
		SetMaxSize(10).
		SetExist(true).
		SetNodePoolName(nodePoolName).
		Build()
}

func createSimplePod(name string, milliCpu, memory int64) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Name: "container-1",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(milliCpu, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(memory, resource.BinarySI),
						},
					},
				},
			},
		},
	}
}

func createSimpleResourceList(milliCpu, memory, ephemeralStorage int64) apiv1.ResourceList {
	return apiv1.ResourceList{
		apiv1.ResourceCPU:              *resource.NewMilliQuantity(milliCpu, resource.DecimalSI),
		apiv1.ResourceMemory:           *resource.NewQuantity(memory, resource.BinarySI),
		apiv1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStorage, resource.BinarySI),
	}
}

func createSimpleNode(name, zone string, capacity, allocatable apiv1.ResourceList, taints []apiv1.Taint, labels, annotations map[string]string) *apiv1.Node {
	labels["zone"] = zone
	return &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: apiv1.NodeSpec{
			Taints: taints,
		},
		Status: apiv1.NodeStatus{
			Capacity:    capacity,
			Allocatable: allocatable,
		},
	}
}

func createTestTemplateNodeInfo(name, zone string) *framework.NodeInfo {
	timeAdded := metav1.NewTime(time.Date(2000, 1, 1, 1, 1, 1, 1, time.UTC))
	pods := []*apiv1.Pod{
		createSimplePod("template-pod-1", 100, 100),
		createSimplePod("template-pod-2", 200, 200),
		createSimplePod("template-pod-3", 300, 300),
	}
	node := createSimpleNode(name, zone,
		createSimpleResourceList(2000, 2000, 2000),
		createSimpleResourceList(1800, 1800, 1800),
		[]apiv1.Taint{
			{Key: "key-1", Value: "val-1", Effect: apiv1.TaintEffectNoSchedule, TimeAdded: &timeAdded},
		},
		map[string]string{
			"template-specific-label": "true",
			"common-label":            "template",
		},
		map[string]string{
			labels.NodeGeneratedFromTemplateAnnotation: "true",
			"common-annotation":                        "template",
		},
	)
	nodeInfo := framework.NewTestNodeInfo(node, pods...)
	return nodeInfo
}

func createTestCorrectNodeInfo(name, zone string) *framework.NodeInfo {
	timeAdded := metav1.NewTime(time.Date(2000, 1, 1, 1, 1, 1, 1, time.UTC))
	pods := []*apiv1.Pod{
		createSimplePod("correct-pod-1", 400, 400),
		createSimplePod("correct-pod-2", 500, 500),
	}
	node := createSimpleNode(name, zone,
		createSimpleResourceList(1500, 1500, 1500),
		createSimpleResourceList(1300, 1300, 1300),
		[]apiv1.Taint{
			{Key: "key-2", Value: "val-2", Effect: apiv1.TaintEffectNoSchedule, TimeAdded: &timeAdded},
			{Key: "key-3", Value: "val-3", Effect: apiv1.TaintEffectNoSchedule, TimeAdded: &timeAdded},
		},
		map[string]string{
			"real-specific-label": "true",
			"common-label":        "real",
		},
		map[string]string{
			"real-specific-annotation": "true",
			"common-annotation":        "real",
		},
	)
	nodeInfo := framework.NewTestNodeInfo(node, pods...)
	return nodeInfo
}

func createTestAugmentedTemplateNodeInfo(name, zone string) *framework.NodeInfo {
	timeAdded := metav1.NewTime(time.Date(2000, 1, 1, 1, 1, 1, 1, time.UTC))
	// Pods should be the same as the template ones.
	pods := []*apiv1.Pod{
		createSimplePod("template-pod-1", 100, 100),
		createSimplePod("template-pod-2", 200, 200),
		createSimplePod("template-pod-3", 300, 300),
	}
	// Most of the node should be the same as the real one. Labels and annotations should
	// be merged, with the template ones overwriting real ones.
	node := createSimpleNode(name, zone,
		createSimpleResourceList(1500, 1500, 1500),
		createSimpleResourceList(1300, 1300, 1300),
		[]apiv1.Taint{
			{Key: "key-2", Value: "val-2", Effect: apiv1.TaintEffectNoSchedule, TimeAdded: &timeAdded},
			{Key: "key-3", Value: "val-3", Effect: apiv1.TaintEffectNoSchedule, TimeAdded: &timeAdded},
		},
		map[string]string{
			"zone":                    zone,
			"real-specific-label":     "true",
			"template-specific-label": "true",
			"common-label":            "template",
		},
		map[string]string{
			labels.NodeGeneratedFromTemplateAnnotation: "true",
			"real-specific-annotation":                 "true",
			"common-annotation":                        "template",
		},
	)
	nodeInfo := framework.NewTestNodeInfo(node, pods...)
	return nodeInfo
}

// assertNodeInfosRelevantPartsEqual asserts if two nodeInfos have identical Node and Pods.
func assertNodeInfosRelevantPartsEqual(t *testing.T, expected, actual *framework.NodeInfo) {
	if expected == nil && actual == nil {
		return
	}
	if !assert.NotNil(t, expected) || !assert.NotNil(t, actual) {
		return
	}

	expectedCopy := expected.Snapshot()
	actualCopy := actual.Snapshot()

	assert.Equal(t, len(expectedCopy.GetPods()), len(actualCopy.GetPods()))
	for i, actualPodInfo := range actualCopy.GetPods() {
		actualPod := actualPodInfo.GetPod()
		actualPod.Name = ""
		actualPod.UID = ""

		expectedPod := expectedCopy.GetPods()[i].GetPod()
		expectedPod.Name = ""
		expectedPod.UID = ""

		assert.Equal(t, expectedPod, actualPod)
	}

	assert.Equal(t, expectedCopy.Node(), actualCopy.Node())
}

func linkMigsToNodePools(migs []cloudprovider.NodeGroup) {
	poolMigs := make(map[string][]*gke.GkeMig)
	for _, m := range migs {
		if m == nil {
			continue
		}
		gm, ok := m.(*gke.GkeMig)
		if !ok || gm == nil {
			continue
		}
		poolMigs[gm.NodePoolName()] = append(poolMigs[gm.NodePoolName()], gm)
	}
	for name, ms := range poolMigs {
		gke.AddMigsToNodePool(name, ms...)
	}
}

func TestUpdateNodeInfosWithinNodePools(t *testing.T) {
	migs := []cloudprovider.NodeGroup{
		createSimpleMig("pool-1-zone-a", "pool-1"),
		createSimpleMig("pool-1-zone-b", "pool-1"),
		createSimpleMig("pool-2-zone-a", "pool-2"),
	}
	linkMigsToNodePools(migs)

	threeZoneMigs := []cloudprovider.NodeGroup{
		createSimpleMig("pool-1-zone-a", "pool-1"),
		createSimpleMig("pool-1-zone-b", "pool-1"),
		createSimpleMig("pool-1-zone-c", "pool-1"),
		createSimpleMig("pool-2-zone-a", "pool-2"),
	}
	linkMigsToNodePools(threeZoneMigs)

	var typedNilMig *gke.GkeMig
	migsWithNils := []cloudprovider.NodeGroup{
		nil,
		typedNilMig,
		threeZoneMigs[0],
		threeZoneMigs[1],
		threeZoneMigs[2],
		threeZoneMigs[3],
	}

	testTemplateLabels := map[string]string{
		"template-specific-label": "true",
		"common-label":            "template",
	}
	testTemplateTaints := []apiv1.Taint{
		{
			Key:    "key-1",
			Value:  "val-1",
			Effect: apiv1.TaintEffectNoSchedule,
		},
	}

	updatedTestTemplateLabels := map[string]string{
		"template-specific-label": "false",
		"common-label":            "template",
	}
	updatedTestTemplateTaints := []apiv1.Taint{
		{
			Key:    "key-1",
			Value:  "val-other",
			Effect: apiv1.TaintEffectNoSchedule,
		},
	}

	for _, testCase := range []struct {
		name              string
		migs              []cloudprovider.NodeGroup
		templateLabels    map[string]map[string]string
		templateTaints    map[string][]apiv1.Taint
		nodeInfos         map[string]*framework.NodeInfo
		expectedNodeInfos map[string]*framework.NodeInfo
	}{
		{
			name: "all nodeInfos real",
			templateLabels: map[string]map[string]string{
				migs[0].Id(): testTemplateLabels,
				migs[1].Id(): testTemplateLabels,
				migs[2].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				migs[0].Id(): testTemplateTaints,
				migs[1].Id(): testTemplateTaints,
				migs[2].Id(): testTemplateTaints,
			},
			nodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestCorrectNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
			expectedNodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestCorrectNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
		},
		{
			name: "template nodeInfo present but no other MIGs in the node pool",
			templateLabels: map[string]map[string]string{
				migs[0].Id(): testTemplateLabels,
				migs[1].Id(): testTemplateLabels,
				migs[2].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				migs[0].Id(): testTemplateTaints,
				migs[1].Id(): testTemplateTaints,
				migs[2].Id(): testTemplateTaints,
			},
			nodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestCorrectNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestTemplateNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
			expectedNodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestCorrectNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestTemplateNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
		},
		{
			name: "template nodeInfo present and other updated MIGs with real nodeInfo present in the same node pool",
			templateLabels: map[string]map[string]string{
				migs[0].Id(): updatedTestTemplateLabels,
				migs[1].Id(): testTemplateLabels,
				migs[2].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				migs[0].Id(): updatedTestTemplateTaints,
				migs[1].Id(): testTemplateTaints,
				migs[2].Id(): testTemplateTaints,
			},
			nodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
			expectedNodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
		},
		{
			name: "template nodeInfo present and other MIGs with real nodeInfo present in the same node pool",
			templateLabels: map[string]map[string]string{
				migs[0].Id(): testTemplateLabels,
				migs[1].Id(): testTemplateLabels,
				migs[2].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				migs[0].Id(): testTemplateTaints,
				migs[1].Id(): testTemplateTaints,
				migs[2].Id(): testTemplateTaints,
			},
			nodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
			expectedNodeInfos: map[string]*framework.NodeInfo{
				migs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				migs[1].Id(): createTestAugmentedTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				migs[2].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
		},
		{
			name: "multi-MIG node pool where one MIG is missing from nodeInfos while sibling MIGs have real and template nodeInfos",
			migs: threeZoneMigs,
			templateLabels: map[string]map[string]string{
				threeZoneMigs[0].Id(): testTemplateLabels,
				threeZoneMigs[1].Id(): testTemplateLabels,
				threeZoneMigs[2].Id(): testTemplateLabels,
				threeZoneMigs[3].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				threeZoneMigs[0].Id(): testTemplateTaints,
				threeZoneMigs[1].Id(): testTemplateTaints,
				threeZoneMigs[2].Id(): testTemplateTaints,
				threeZoneMigs[3].Id(): testTemplateTaints,
			},
			nodeInfos: map[string]*framework.NodeInfo{
				threeZoneMigs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				threeZoneMigs[1].Id(): createTestTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				// threeZoneMigs[2] ("pool-1-zone-c") is missing from nodeInfos
				threeZoneMigs[3].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
			expectedNodeInfos: map[string]*framework.NodeInfo{
				threeZoneMigs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				threeZoneMigs[1].Id(): createTestAugmentedTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				threeZoneMigs[3].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
		},
		{
			name: "nodeGroups slice containing nil and typed-nil GkeMig entries",
			migs: migsWithNils,
			templateLabels: map[string]map[string]string{
				threeZoneMigs[0].Id(): testTemplateLabels,
				threeZoneMigs[1].Id(): testTemplateLabels,
				threeZoneMigs[2].Id(): testTemplateLabels,
				threeZoneMigs[3].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				threeZoneMigs[0].Id(): testTemplateTaints,
				threeZoneMigs[1].Id(): testTemplateTaints,
				threeZoneMigs[2].Id(): testTemplateTaints,
				threeZoneMigs[3].Id(): testTemplateTaints,
			},
			nodeInfos: map[string]*framework.NodeInfo{
				threeZoneMigs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				threeZoneMigs[1].Id(): createTestTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				threeZoneMigs[3].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
			expectedNodeInfos: map[string]*framework.NodeInfo{
				threeZoneMigs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				threeZoneMigs[1].Id(): createTestAugmentedTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				threeZoneMigs[3].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
		},
		{
			name: "multi-MIG node pool where one MIG maps to nil in nodeInfos while sibling MIGs have real and template nodeInfos",
			migs: threeZoneMigs,
			templateLabels: map[string]map[string]string{
				threeZoneMigs[0].Id(): testTemplateLabels,
				threeZoneMigs[1].Id(): testTemplateLabels,
				threeZoneMigs[2].Id(): testTemplateLabels,
				threeZoneMigs[3].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				threeZoneMigs[0].Id(): testTemplateTaints,
				threeZoneMigs[1].Id(): testTemplateTaints,
				threeZoneMigs[2].Id(): testTemplateTaints,
				threeZoneMigs[3].Id(): testTemplateTaints,
			},
			nodeInfos: map[string]*framework.NodeInfo{
				threeZoneMigs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				threeZoneMigs[1].Id(): createTestTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				threeZoneMigs[2].Id(): nil,
				threeZoneMigs[3].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
			expectedNodeInfos: map[string]*framework.NodeInfo{
				threeZoneMigs[0].Id(): createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a"),
				threeZoneMigs[1].Id(): createTestAugmentedTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
				threeZoneMigs[2].Id(): nil,
				threeZoneMigs[3].Id(): createTestCorrectNodeInfo("pool-2-zone-a-node-789", "zone-a"),
			},
		},
		{
			name: "node pool where all MIGs are missing from nodeInfos",
			templateLabels: map[string]map[string]string{
				migs[0].Id(): testTemplateLabels,
				migs[1].Id(): testTemplateLabels,
				migs[2].Id(): testTemplateLabels,
			},
			templateTaints: map[string][]apiv1.Taint{
				migs[0].Id(): testTemplateTaints,
				migs[1].Id(): testTemplateTaints,
				migs[2].Id(): testTemplateTaints,
			},
			nodeInfos:         map[string]*framework.NodeInfo{},
			expectedNodeInfos: map[string]*framework.NodeInfo{},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			activeMigs := testCase.migs
			if activeMigs == nil {
				activeMigs = migs
			}
			cloudProvider := &gke.GkeCloudProviderMock{}
			cloudProvider.On("NodeGroups").Return(activeMigs)
			for _, mig := range activeMigs {
				if mig == nil {
					continue
				}
				if gm, ok := mig.(*gke.GkeMig); !ok || gm == nil {
					continue
				}
				cloudProvider.On("GetMigInstanceTemplateLabels", mig).Return(testCase.templateLabels[mig.Id()], nil)
				cloudProvider.On("GetMigInstanceTemplateTaints", mig).Return(testCase.templateTaints[mig.Id()], nil)
			}
			ctx := &context.AutoscalingContext{CloudProvider: cloudProvider}

			nodeInfos, err := UpdateNodeInfosWithinNodePools(ctx, testCase.nodeInfos)
			assert.NoError(t, err)

			assert.Equal(t, len(testCase.expectedNodeInfos), len(nodeInfos))
			for migId, nodeInfo := range nodeInfos {
				expectedNodeInfo := testCase.expectedNodeInfos[migId]
				assertNodeInfosRelevantPartsEqual(t, expectedNodeInfo, nodeInfo)
			}
		})
	}
}

func TestCreateAugmentedTemplateNodeInfo(t *testing.T) {
	validTemplate := createTestTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b")
	validReal := createTestCorrectNodeInfo("pool-1-zone-a-node-123", "zone-a")

	for _, tc := range []struct {
		name             string
		templateNodeInfo *framework.NodeInfo
		realNodeInfo     *framework.NodeInfo
		wantNodeInfo     *framework.NodeInfo
		wantErr          bool
	}{
		{
			name:             "nil templateNodeInfo returns error without panicking",
			templateNodeInfo: nil,
			realNodeInfo:     validReal,
			wantErr:          true,
		},
		{
			name:             "empty templateNodeInfo with nil Node() returns error without panicking",
			templateNodeInfo: framework.NewNodeInfo(nil, nil),
			realNodeInfo:     validReal,
			wantErr:          true,
		},
		{
			name:             "nil realNodeInfo returns error without panicking",
			templateNodeInfo: validTemplate,
			realNodeInfo:     nil,
			wantErr:          true,
		},
		{
			name:             "empty realNodeInfo with nil Node() returns error without panicking",
			templateNodeInfo: validTemplate,
			realNodeInfo:     framework.NewNodeInfo(nil, nil),
			wantErr:          true,
		},
		{
			name:             "valid templateNodeInfo and realNodeInfo succeeds",
			templateNodeInfo: validTemplate,
			realNodeInfo:     validReal,
			wantNodeInfo:     createTestAugmentedTemplateNodeInfo("pool-1-zone-b-node-456", "zone-b"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := createAugmentedTemplateNodeInfo(tc.templateNodeInfo, tc.realNodeInfo)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assertNodeInfosRelevantPartsEqual(t, tc.wantNodeInfo, got)
		})
	}
}
