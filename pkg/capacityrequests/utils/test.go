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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	cr_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	cr_clientset "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/clientset/versioned"
)

type testReason struct {
	message string
}

func (tr *testReason) Reasons() []string {
	return []string{tr.message}
}

var (
	notSchedulableReason = &testReason{"not schedulable"}
	maxLimitReason       = &testReason{"max limit reached"}
)

func AppendCRPods(t *testing.T, pods []*apiv1.Pod, crState *CapacityRequestState, crs []*cr_types.CapacityRequest) []*apiv1.Pod {
	result := []*apiv1.Pod{}
	result = append(result, pods...)
	for _, cr := range crs {
		pod, found := crState.CapacityRequestToPod(cr)
		assert.True(t, found, "Failed to find pod for Capacity Request: %v", cr)
		result = append(result, pod)
	}
	return result
}

func BuildTestNoScaleUpInfo(pod *apiv1.Pod) status.NoScaleUpInfo {
	return status.NoScaleUpInfo{
		Pod:                pod,
		RejectedNodeGroups: map[string]status.Reasons{"n1": notSchedulableReason},
		SkippedNodeGroups:  map[string]status.Reasons{"n2": maxLimitReason},
	}
}

func AppendCRNoScaleUpInfos(t *testing.T, noScaleUpInfos []status.NoScaleUpInfo, crState *CapacityRequestState, crs []*cr_types.CapacityRequest) []status.NoScaleUpInfo {
	result := []status.NoScaleUpInfo{}
	result = append(result, noScaleUpInfos...)
	for _, cr := range crs {
		pod, found := crState.CapacityRequestToPod(cr)
		assert.True(t, found, "Failed to find pod for Capacity Request: %v", cr)
		result = append(result, BuildTestNoScaleUpInfo(pod))
	}
	return result
}

// BuildTestCr returns a test Capacity Request with given name and requests.
func BuildTestCr(name, cpu, mem string, conditions []cr_types.CapacityRequestConditionType) *cr_types.CapacityRequest {
	cpuVal, _ := resource.ParseQuantity(cpu)
	memVal, _ := resource.ParseQuantity(mem)
	cr := &cr_types.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name),
		},
		Spec: cr_types.CapacityRequestSpec{
			Capacity: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{
						Name: "container1",
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    cpuVal,
								apiv1.ResourceMemory: memVal,
							},
						},
					},
				},
			},
		},
		Status: cr_types.CapacityRequestStatus{},
	}
	cr.Status.Conditions = []cr_types.CapacityRequestCondition{
		{
			Type:   cr_types.ResourcesAvailable,
			Status: apiv1.ConditionFalse,
		}, {
			Type:   cr_types.ResourcesInProvisioning,
			Status: apiv1.ConditionFalse,
		}, {
			Type:   cr_types.ResourcesUnattainable,
			Status: apiv1.ConditionFalse,
		},
	}
	for _, c := range conditions {
		cond := cr_types.CapacityRequestCondition{Type: c}
		cond.Status = apiv1.ConditionTrue
		var i int
		switch c {
		case cr_types.ResourcesAvailable:
			cond.Reason = "EnoughCapacity"
			cond.Message = "There is enough resources available in the cluster for this Capacity Request."
			i = 0
		case cr_types.ResourcesInProvisioning:
			cond.Reason = "ScalingUpCluster"
			cond.Message = "Resources for this Capacity Request are currently being provisioned."
			i = 1
		case cr_types.ResourcesUnattainable:
			cond.Reason = "CapacityUnattainable"
			cond.Message = "It is impossible to provision resources for this Capacity Request."
			i = 2
		}
		cr.Status.Conditions[i] = cond
	}
	return cr
}

// BuildTestCrWithRemoval returns a test Capacity Request with given name, requests and
// a list of pods to be added to the PodsToReplace list.
func BuildTestCrWithRemoval(name, cpu, mem string, conditions []cr_types.CapacityRequestConditionType, podsToReplace []string) *cr_types.CapacityRequest {
	cr := BuildTestCr(name, cpu, mem, conditions)
	cr.Spec.ProvisionPolicy = cr_types.CapacityProvisionPolicy{
		PodsToReplace: []apiv1.LocalObjectReference{},
	}
	for _, pod := range podsToReplace {
		cr.Spec.ProvisionPolicy.PodsToReplace = append(cr.Spec.ProvisionPolicy.PodsToReplace, apiv1.LocalObjectReference{Name: pod})
	}
	return cr
}

// BuildPodFromCr builds a pod similar to the real representation of Capacity Request
// in autoscaler simulations. The pod is not identical as the real one has a random
// component in the name.
func BuildPodFromCr(cr *cr_types.CapacityRequest) *apiv1.Pod {
	return &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: cr.ObjectMeta.Namespace, Name: "capacity-request-", UID: cr.UID}, Spec: cr.Spec.Capacity}
}

// NewTestCapacityRequestState creates a CapacityRequestState for testing from
// given capacity requests.
func NewTestCapacityRequestState(fakeClient cr_clientset.Interface, crs []*cr_types.CapacityRequest) *CapacityRequestState {
	crState := NewCapacityRequestState(fakeClient)
	crState.Update(crs)
	return crState
}
