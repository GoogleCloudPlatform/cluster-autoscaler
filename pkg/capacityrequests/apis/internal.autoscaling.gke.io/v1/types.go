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

// Package v1 contains definitions of Vertical Pod Autoscaler related objects.
package v1

import (
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:storageversion
// +kubebuilder:resource:shortName=capreq

// CapacityRequest is a way to express additional capacity that we would like to
// reserve in the cluster. Cluster Autoscaler can use this information in its
// calculations and signal if the additional capacity is available in the
// cluster or proactively add capacity if needed.
type CapacityRequest struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object metadata. More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Specification of the CapacityRequest object.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#spec-and-status.
	// +optional
	Spec CapacityRequestSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// Current status of the CapacityRequest.
	// +optional
	Status CapacityRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CapacityRequestList is a list of CapacityRequests objects.
type CapacityRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata" protobuf:"bytes,1,opt,name=metadata"`
	Items           []CapacityRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CapacityRequestSpec is the specification of additional capacity requested in
// the cluster.
type CapacityRequestSpec struct {
	// Capacity specifies additional capacity to reserve in the cluster as a
	// specification of the pod that should be scheduled in the cluster.
	Capacity apiv1.PodSpec `json:"capacity" protobuf:"bytes,1,name=capacity"`

	// ProvisionPolicy describes how to provision the additional capacity.
	// +optional
	ProvisionPolicy CapacityProvisionPolicy `json:"provisionPolicy" protobuf:"bytes,2,name=provisionPolicy"`

	// ProvisionedCapacitySelector identifies pods that this Capacity Request
	// is related to by.
	// This signifies that creation of a pod matching ProvisionedCapacitySelector
	// may mean that this Capacity Request is no longer needed. Note that this
	// field is ignored by Cluster Autoscaler and it is the responsibility of the
	// client to delete the Capacity Request when it becomes obsolete. If the
	// client consumes the capacity without deleting the request, the Cluster
	// Autoscaler will attempt to fulfill the request again.
	// +optional
	ProvisionedCapacitySelector *metav1.LabelSelector `json:"provisionedCapacitySelector,omitempty" protobuf:"bytes,3,opt,name=provisionedCapacitySelector"`
}

// CapacityProvisionPolicy describes how additional capacity should be
// provisioned.
type CapacityProvisionPolicy struct {
	// PodsToReplace is a list of pods that can be excluded from simulation when
	// reserving additional capacity. The semantics is that the pod for which
	// this capacity request reserves capacity will replace the pods in this list.
	PodsToReplace []apiv1.LocalObjectReference `json:"podsToReplace" protobuf:"bytes,1,rep,name=podsToReplace"`
}

// CapacityRequestStatus is the current status of the capacity request.
type CapacityRequestStatus struct {
	// LastUpdateTime is the time when the status was last refreshed.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,1,opt,name=lastUpdateTime"`
	// Conditions is a set of conditions indicating the state in which the
	// CapacityRequest is currently in.
	Conditions []CapacityRequestCondition `json:"conditions" protobuf:"bytes,2,rep,name=conditions"`
}

// CapacityRequestCondition indicates the state in which the CapacityRequest is
// currently in.
type CapacityRequestCondition struct {
	// Type describes the current condition.
	Type CapacityRequestConditionType `json:"type" protobuf:"bytes,1,name=type"`
	// Status of the condition (True, False, Unknown).
	Status apiv1.ConditionStatus `json:"status" protobuf:"bytes,2,name=status"`
	// LastTransitionTime is the last time the condition transitioned from
	// one status to another.
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// Reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// Message is a human-readable explanation containing details about
	// the transition.
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// CapacityRequestConditionType are the valid conditions of a CapacityRequest.
type CapacityRequestConditionType string

var (
	// ResourcesAvailable indicates if there are currently resources available
	// in the cluster to fulfil this CapacityRequest. Autoscaler will set this
	// condition to true when (to its best knowledge) there is enough space in
	// the cluster to schedule the pod from this CapacityRequest.
	ResourcesAvailable CapacityRequestConditionType = "ResourcesAvailable"
	// ResourcesInProvisioning indicates if the resources for this capacity
	// request are currently being provisioned. This will be set to true when
	// this CapacityRequest triggers a scale up or creation of a new node group.
	ResourcesInProvisioning CapacityRequestConditionType = "ResourcesInProvisioning"
	// ResourcesUnattainable indicates if it is impossible to obtain resources
	// to fulfil this CapacityRequest. Example situations in which it would be true:
	// - Node autoprovisioning is not enabled and there is no node pool in this
	// cluster that would satisfy this CapacityRequest (wrong node shapes).
	// - Scale up is impossible due to cluster reaching global resource limits set
	// in Cluster Autoscaler.
	ResourcesUnattainable CapacityRequestConditionType = "ResourcesUnattainable"
)
