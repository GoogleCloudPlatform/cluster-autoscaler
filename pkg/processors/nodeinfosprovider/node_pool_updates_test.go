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

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

func TestUpdateLabels(t *testing.T) {
	testCases := map[string]struct {
		nodeAnnotations map[string]string
		desiredLabels   map[string]string
		initialLabels   map[string]string
		expectedLabels  map[string]string
		expectedErr     bool
	}{
		"no annotations, empty template kube env": {
			initialLabels: map[string]string{
				"key1": "value1",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
			},
		},
		"no last applied labels, empty template kube env": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "",
			},
			initialLabels: map[string]string{
				"key1": "value1",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
			},
		},
		"one last applied label": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "key2=value2",
			},
			initialLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
			},
		},
		"many last applied labels": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "key1=value1,key2=value2",
			},
			initialLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedLabels: map[string]string{},
		},
		"missing last applied label": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "key2=value2,key3=value3",
			},
			initialLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
			},
		},
		"different value last applied label": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "key2=other-value",
			},
			initialLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
			},
		},
		"one desired label": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "",
			},
			desiredLabels: map[string]string{
				"key2": "value2",
			},
			initialLabels: map[string]string{
				"key1": "value1",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		"many desired labels": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "",
			},
			desiredLabels: map[string]string{
				"key2": "value2",
				"key3": "value3",
			},
			initialLabels: map[string]string{
				"key1": "value1",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
		"same last applied and desired labels": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "key2=value2",
			},
			desiredLabels: map[string]string{
				"key2": "value2",
			},
			initialLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		"different last applied and desired labels": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "key2=value2",
			},
			desiredLabels: map[string]string{
				"key3": "value3",
			},
			initialLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
				"key3": "value3",
			},
		},
		"no initial labels": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "",
			},
			desiredLabels: map[string]string{
				"key1": "value1",
			},
			initialLabels: nil,
			expectedLabels: map[string]string{
				"key1": "value1",
			},
		},
		"do not update labels if there are no annotation": {
			nodeAnnotations: map[string]string{},
			desiredLabels: map[string]string{
				"key2": "value2",
			},
			initialLabels: map[string]string{
				"key1": "value1",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
			},
		},
		"malformed last applied labels": {
			nodeAnnotations: map[string]string{
				lastAppliedLabelsKey: "some really incorrect value",
			},
			desiredLabels: map[string]string{
				"key3": "value3",
			},
			initialLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			nodeGroup := test.NewTestNodeGroup("group", 0, 0, 0, true, false, "e2-standard-4", nil, nil)

			node := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      tc.initialLabels,
					Annotations: tc.nodeAnnotations,
				},
			}
			nodeInfo := framework.NewTestNodeInfo(node.DeepCopy())

			newNodeInfo, err := updateLabels(nodeGroup, nodeInfo, tc.desiredLabels)
			if tc.expectedErr {
				assert.Error(t, err)
				assert.Equal(t, tc.initialLabels, nodeInfo.Node().Labels)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedLabels, newNodeInfo.Node().Labels)
			}
		})
	}
}

func TestUpdateTaints(t *testing.T) {
	testCases := map[string]struct {
		nodeAnnotations map[string]string
		desiredTaints   []v1.Taint
		initialTaints   []v1.Taint
		expectedTaints  []v1.Taint
		expectedErr     bool
	}{
		"no annotations, empty template kube env": {
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"no last applied taints, empty template kube env": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "",
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"one last applied taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "key2=value2:NoSchedule",
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"many last applied taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "key1=value1:NoSchedule,key2=value2:NoSchedule",
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{},
		},
		"missing last applied taint": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "key2=value2:NoSchedule,key3=value3:NoSchedule",
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"different value last applied taint": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "key2=other-value:NoSchedule",
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"one desired taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "",
			},
			desiredTaints: []v1.Taint{
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"many desired taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "",
			},
			desiredTaints: []v1.Taint{
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
				{Key: "key3", Value: "value3", Effect: v1.TaintEffectNoSchedule},
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
				{Key: "key3", Value: "value3", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"same last applied and desired taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "key2=value2:NoSchedule",
			},
			desiredTaints: []v1.Taint{
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"different last applied and desired taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "key2=value2:NoSchedule",
			},
			desiredTaints: []v1.Taint{
				{Key: "key3", Value: "value3", Effect: v1.TaintEffectNoSchedule},
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key3", Value: "value3", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"no initial taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "",
			},
			desiredTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
			initialTaints: nil,
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"do not update taints if there are no annotation": {
			nodeAnnotations: map[string]string{},
			desiredTaints: []v1.Taint{
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
		},
		"malformed last applied taints": {
			nodeAnnotations: map[string]string{
				lastAppliedTaintsKey: "some really incorrect value",
			},
			desiredTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			},
			initialTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
			},
			expectedErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			nodeGroup := test.NewTestNodeGroup("group", 0, 0, 0, true, false, "e2-standard-4", nil, nil)

			node := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: tc.nodeAnnotations,
				},
				Spec: v1.NodeSpec{
					Taints: tc.initialTaints,
				},
			}
			nodeInfo := framework.NewTestNodeInfo(node.DeepCopy())

			newNodeInfo, err := updateTaints(nodeGroup, nodeInfo, tc.desiredTaints)
			if tc.expectedErr {
				assert.Error(t, err)
				assert.Equal(t, tc.initialTaints, nodeInfo.Node().Spec.Taints)
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.expectedTaints, newNodeInfo.Node().Spec.Taints)
			}
		})
	}
}

func TestGetLastAppliedLabels(t *testing.T) {
	testCases := map[string]struct {
		annotations    map[string]string
		expectedLabels map[string]string
		expectedFound  bool
		expectedErr    bool
	}{
		"no annotations": {
			annotations:    map[string]string{},
			expectedLabels: nil,
			expectedFound:  false,
		},
		"no last applied annotation": {
			annotations: map[string]string{
				"irrelevant": "annotation",
			},
			expectedLabels: nil,
			expectedFound:  false,
		},
		"empty last applied annotation": {
			annotations: map[string]string{
				lastAppliedLabelsKey: "",
			},
			expectedLabels: nil,
			expectedFound:  true,
		},
		"single last applied label": {
			annotations: map[string]string{
				lastAppliedLabelsKey: "key=value",
			},
			expectedLabels: map[string]string{
				"key": "value",
			},
			expectedFound: true,
		},
		"many last applied labels": {
			annotations: map[string]string{
				lastAppliedLabelsKey: "key1=value1,key2=value2",
			},
			expectedLabels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedFound: true,
		},
		"malformed last applied labels": {
			annotations: map[string]string{
				lastAppliedLabelsKey: "key1=value1=effect1,key2=value2=effect2",
			},
			expectedFound: true,
			expectedErr:   true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			node := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: tc.annotations,
				},
			}
			nodeInfo := framework.NewTestNodeInfo(node)

			labels, found, err := getLastAppliedLabels(nodeInfo)
			assert.Equal(t, tc.expectedLabels, labels)
			assert.Equal(t, tc.expectedFound, found)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetLastAppliedTaints(t *testing.T) {
	testCases := map[string]struct {
		annotations map[string]string

		expectedTaints []v1.Taint
		expectedFound  bool
		expectedErr    bool
	}{
		"no annotations": {
			annotations:    map[string]string{},
			expectedTaints: nil,
			expectedFound:  false,
		},
		"no last applied annotation": {
			annotations: map[string]string{
				"irrelevant": "annotation",
			},
			expectedTaints: nil,
			expectedFound:  false,
		},
		"empty last applied annotation": {
			annotations: map[string]string{
				lastAppliedTaintsKey: "",
			},
			expectedTaints: nil,
			expectedFound:  true,
		},
		"single last applied taint": {
			annotations: map[string]string{
				lastAppliedTaintsKey: "key=value:NoSchedule",
			},
			expectedTaints: []v1.Taint{
				{
					Key:    "key",
					Value:  "value",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			expectedFound: true,
		},
		"any value single last applied taint": {
			annotations: map[string]string{
				lastAppliedTaintsKey: "key:NoSchedule",
			},
			expectedTaints: []v1.Taint{
				{
					Key:    "key",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			expectedFound: true,
		},
		"many last applied taints": {
			annotations: map[string]string{
				lastAppliedTaintsKey: "key1=value1:NoSchedule,key2=value2:PreferNoSchedule",
			},
			expectedTaints: []v1.Taint{
				{
					Key:    "key1",
					Value:  "value1",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "key2",
					Value:  "value2",
					Effect: v1.TaintEffectPreferNoSchedule,
				},
			},
			expectedFound: true,
		},
		"malformed last applied taints": {
			annotations: map[string]string{
				lastAppliedTaintsKey: "key1=value1=NoSchedule,key2=value2=PreferNoSchedule",
			},
			expectedFound: true,
			expectedErr:   true,
		},
		"unknown taint effect": {
			annotations: map[string]string{
				lastAppliedTaintsKey: "key=value:MaybeSchedule",
			},
			expectedFound: true,
			expectedErr:   true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			node := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: tc.annotations,
				},
			}
			nodeInfo := framework.NewTestNodeInfo(node)

			taints, found, err := getLastAppliedTaints(nodeInfo)
			assert.Equal(t, tc.expectedTaints, taints)
			assert.Equal(t, tc.expectedFound, found)

			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
