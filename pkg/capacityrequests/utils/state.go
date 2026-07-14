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
	ctx "context"
	"fmt"
	"math/rand"
	"sync"

	apiv1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	cr_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	cr_clientset "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/clientset/versioned"
)

// this is just for internal bookkeeping, it should never show up outside of CA.
const crAnnotation = "cr.internal.gkeautoscaler.google.com/capacityrequest"

// ObjectRef uniquely identifies an object of a certain kind.
type ObjectRef struct {
	Namespace string
	Name      string
}

// CapacityRequestState holds the current known state of Capacity Requests in
// the cluster.
type CapacityRequestState struct {
	mux                  sync.Mutex
	crClient             cr_clientset.Interface
	podToCapacityRequest map[ObjectRef]*cr_types.CapacityRequest
	capacityRequestToPod map[ObjectRef]*apiv1.Pod
}

// NewCapacityRequestState creates a new CapacityRequestState.
func NewCapacityRequestState(crClient cr_clientset.Interface) *CapacityRequestState {
	return &CapacityRequestState{
		crClient:             crClient,
		podToCapacityRequest: make(map[ObjectRef]*cr_types.CapacityRequest),
		capacityRequestToPod: make(map[ObjectRef]*apiv1.Pod)}
}

// Update sets the state based on current CapacityRequests
func (s *CapacityRequestState) Update(crs []*cr_types.CapacityRequest) {
	s.mux.Lock()
	defer s.mux.Unlock()
	newPods := make(map[ObjectRef]*apiv1.Pod)
	newCrs := make(map[ObjectRef]*cr_types.CapacityRequest)
	for _, cr := range crs {
		var crPod *apiv1.Pod
		if pod, found := s.CapacityRequestToPod(cr); found {
			crPod = pod
		} else {
			podName := fmt.Sprintf("capacity-request-%d", rand.Int63())
			crPod = &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: cr.Namespace, UID: cr.UID, Labels: cr.Labels}, Spec: cr.Spec.Capacity, Status: apiv1.PodStatus{}}
			crPod.SetAnnotations(map[string]string{drain.PodSafeToEvictKey: "true", crAnnotation: "true"})
		}
		newPods[ObjectRef{Name: cr.Name, Namespace: cr.Namespace}] = crPod
		newCrs[ObjectRef{Name: crPod.Name, Namespace: crPod.Namespace}] = cr

	}
	s.podToCapacityRequest = newCrs
	s.capacityRequestToPod = newPods
}

// CapacityRequestToPod returns pod corresponding to given Capacity Request.
func (s *CapacityRequestState) CapacityRequestToPod(cr *cr_types.CapacityRequest) (*apiv1.Pod, bool) {
	p, found := s.capacityRequestToPod[ObjectRef{Name: cr.Name, Namespace: cr.Namespace}]
	return p, found
}

// PodToCapacityRequest returns Capacity Request corresponding to given pod.
func (s *CapacityRequestState) PodToCapacityRequest(pod *apiv1.Pod) (*cr_types.CapacityRequest, bool) {
	cr, found := s.podToCapacityRequest[ObjectRef{Name: pod.Name, Namespace: pod.Namespace}]
	return cr, found
}

// PodToCapacityRequestMap returns a mapping from pod to Capacity Request.
func (s *CapacityRequestState) PodToCapacityRequestMap() map[ObjectRef]*cr_types.CapacityRequest {
	s.mux.Lock()
	defer s.mux.Unlock()
	result := make(map[ObjectRef]*cr_types.CapacityRequest, len(s.podToCapacityRequest))
	for podRef, cr := range s.podToCapacityRequest {
		result[podRef] = cr
	}
	return result
}

// SetResourcesAvailable sets the condition of Capacity Request to ResourcesAvailable.
// Unsets all other conditions.
func (s *CapacityRequestState) SetResourcesAvailable(cr *cr_types.CapacityRequest) error {
	return s.updateConditionIfNeeded(cr, cr_types.ResourcesAvailable, "EnoughCapacity", "There is enough resources available in the cluster for this Capacity Request.")
}

// SetResourcesInProvisioning sets the condition of Capacity Request to ResourcesInProvisioning.
// Unsets all other conditions.
func (s *CapacityRequestState) SetResourcesInProvisioning(cr *cr_types.CapacityRequest) error {
	return s.updateConditionIfNeeded(cr, cr_types.ResourcesInProvisioning, "ScalingUpCluster", "Resources for this Capacity Request are currently being provisioned.")
}

// SetResourcesUnattainable sets the condition of Capacity Request to ResourcesUnattainable.
// Unsets all other conditions.
func (s *CapacityRequestState) SetResourcesUnattainable(cr *cr_types.CapacityRequest) error {
	return s.updateConditionIfNeeded(cr, cr_types.ResourcesUnattainable, "CapacityUnattainable", "It is impossible to provision resources for this Capacity Request.")
}

// SetResourcesUnknown unsets all conditions.
func (s *CapacityRequestState) SetResourcesUnknown(cr *cr_types.CapacityRequest) error {
	return s.updateConditionIfNeeded(cr, "", "", "")
}

func (s *CapacityRequestState) updateConditionIfNeeded(cr *cr_types.CapacityRequest, conditionType cr_types.CapacityRequestConditionType, reason, message string) error {
	newCr := cr.DeepCopy()
	newCr.Status.Conditions = []cr_types.CapacityRequestCondition{
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
	if conditionType != "" {
		for i, c := range newCr.Status.Conditions {
			if c.Type == conditionType {
				newCr.Status.Conditions[i] = cr_types.CapacityRequestCondition{Type: conditionType, Status: apiv1.ConditionTrue, Reason: reason, Message: message}
				break
			}
		}
	}
	// skip a write if we wouldn't need to update
	if !apiequality.Semantic.DeepEqual(&cr.Status, &newCr.Status) {
		_, err := s.crClient.InternalV1().CapacityRequests(cr.Namespace).Update(ctx.TODO(), newCr, metav1.UpdateOptions{})
		return err
	}
	return nil
}

// IsPodCapacityRequest tells if a pod is a capacity request.
func IsPodCapacityRequest(pod *apiv1.Pod) bool {
	return pod.GetAnnotations()[crAnnotation] == "true"
}
