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

package podsharding

import (
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"

	"github.com/stretchr/testify/assert"
)

func podForUID(uid apitypes.UID) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: apitypes.UID(uid),
		},
	}
}

func assertNodeGroupDescriptorEqual(t *testing.T, expected, actual NodeGroupDescriptor) {
	t.Helper()
	if len(expected.Labels) != 0 || len(actual.Labels) != 0 {
		assert.Equal(t, expected.Labels, actual.Labels, "Labels")
	}
	if len(expected.SystemLabels) != 0 || len(actual.SystemLabels) != 0 {
		assert.Equal(t, expected.SystemLabels, actual.SystemLabels, "SystemLabels")
	}
	assert.ElementsMatch(t, expected.Taints, actual.Taints, "Taints")
	if len(expected.ExtraResources) != 0 || len(actual.ExtraResources) != 0 {
		assert.Equal(t, resourcesToIntMap(expected.ExtraResources), resourcesToIntMap(actual.ExtraResources), "ExtraResources")
	}
	assert.Equal(t, expected.ProvisioningClassName, actual.ProvisioningClassName, "ProvisioningClassName")
	assert.Equal(t, expected.CSNBufferID, actual.CSNBufferID, "CSNBufferID")
}

func resourcesToIntMap(quantityMap map[string]resource.Quantity) map[string]int64 {
	result := make(map[string]int64)
	for k, v := range quantityMap {
		valInt64, _ := v.AsInt64()
		result[k] = valInt64
	}
	return result
}

func TestSetPodShard(context *context.AutoscalingContext, podShard *PodShard) {
	context.ProcessorCallbacks.SetExtraValue(selectPodShardContextKey, podShard)
}

func createTestPodShard(provReqClass string, labels []string, podUIDs ...string) *PodShard {
	podUIDsMap := make(map[types.UID]bool, len(podUIDs))
	for _, podUID := range podUIDs {
		podUIDsMap[types.UID(podUID)] = true
	}
	labelsMap := make(map[string]string, len(labels))
	for _, label := range labels {
		labelsMap[label] = "true"
	}

	return &PodShard{
		PodUids: podUIDsMap,
		NodeGroupDescriptor: NodeGroupDescriptor{
			Labels:                labelsMap,
			SystemLabels:          map[string]string{},
			ProvisioningClassName: provReqClass,
		},
	}
}

func createTestPod(uid string) *apiv1.Pod {
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:               types.UID(uid),
			Namespace:         "default",
			Name:              uid,
			CreationTimestamp: metav1.NewTime(time.Date(2024, 8, 9, 12, 13, 14, 0, time.UTC)),
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{},
					},
				},
			},
		},
	}
	return pod
}

func createTestPodShardWithRequirement(labels []string, label string, value string, podUIDs ...string) *PodShard {
	result := createTestPodShard("", labels, podUIDs...)
	result.NodeGroupDescriptor.SystemLabels[label] = value
	return result
}

func createTestCSNPodShard(bufferId string, podUIDs ...string) *PodShard {
	result := createTestPodShard("", nil, podUIDs...)
	result.NodeGroupDescriptor.CSNBufferID = bufferId
	return result
}

func addPodRequirement(pod *apiv1.Pod, label string, value string) *apiv1.Pod {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}
	pod.Spec.NodeSelector[label] = value
	return pod
}
