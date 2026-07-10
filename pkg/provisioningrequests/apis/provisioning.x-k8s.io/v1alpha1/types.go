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

// Package v1alpha1 contains definitions of Cluster Autoscaler related objects.
package v1alpha1

import (
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TODO(b/265912746): Change/remove PodSet's MaxItems after multiple PodSets are supported.

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:storageversions
// +kubebuilder:resource:shortName=provreq;provreqs
// +kubebuilder:metadata:labels="addonmanager.kubernetes.io/mode=Reconcile"
// +kubebuilder:metadata:annotations="components.gke.io/layer=addon"
// +kubebuilder:deprecatedversion:warning="provisioning.x-k8s.io/v1alpha1 Provisioning Request is deprecated, please use the autoscaling.x-k8s.io/v1beta1 API"

// Deprecated: ProvisioningRequest v1alpha1 version is no longer supported in GKE 1.29+.
// TODO(b/324867774): The generated source files are still used by DeepMind in google3, so we can't delete them yet, because copybara service would sync the file deletion.
// ProvisioningRequest is a way to express additional capacity
// that we would like to provision in the cluster. Cluster Autoscaler
// can use this information in its calculations and signal if the additional
// capacity is available in the cluster or proactively add capacity if needed.
//
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Provisioned",type=string,JSONPath=`.status.conditions[?(@.type=="Provisioned")].status`
// +kubebuilder:printcolumn:name="Failed",type=string,JSONPath=`.status.conditions[?(@.type=="Failed")].status`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ProvisioningRequest struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object metadata. More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Spec contains specification of the ProvisioningRequest object.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#spec-and-status.
	//
	// +kubebuilder:validation:Required
	Spec ProvisioningRequestSpec `json:"spec" protobuf:"bytes,2,name=spec"`
	// Status of the ProvisioningRequest. CA constantly reconciles this field.
	//
	// +optional
	Status ProvisioningRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ProvisioningRequestList is a object for list of ProvisioningRequest.
type ProvisioningRequestList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	//
	// +optional
	metav1.ListMeta `json:"metadata" protobuf:"bytes,1,opt,name=metadata"`
	// Items, list of ProvisioningRequest returned from API.
	//
	// +optional
	Items []ProvisioningRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ProvisioningRequestSpec is a specification of additional pods for which we
// would like to provision additional resources in the cluster.
type ProvisioningRequestSpec struct {
	// PodSets lists groups of pods for which we would like to provision
	// resources.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable"
	// +kubebuilder:listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	PodSets []PodSet `json:"podSets" protobuf:"bytes,1,rep,name=podSets"`

	// OperationMode describes the different modes of provisioning the resources.
	// Currently supported values:
	// * GCP_QUEUED - queue for resources in Google Cloud
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Value is immutable"
	// +kubebuilder:validation:Enum=GCP_QUEUED
	OperationMode string `json:"operationMode" protobuf:"bytes,2,rep,name=operationMode"`
}

type PodSet struct {
	// Template representing pods that will consume this reservation.
	// Requirements for resources (CPU, RAM, GPUs, TPUs, storage) are
	// necessary (must be non-zero). Users need to make sure that the
	// tolerations and label selectors are consistent between this template
	// and actual pods consuming the Provisioning Request.
	Template apiv1.PodTemplateSpec `json:"template" protobuf:"bytes,1,name=template"`
	// Count contains the number of pods that will be created with a given
	// template.
	// +kubebuilder:validation:Minimum=1
	Count int32 `json:"count" protobuf:"bytes,2,name=count"`
}

// ProvisioningRequestStatus represents the status of the resource reservation.
type ProvisioningRequestStatus struct {
	// Conditions represent the observations of a Provisioning Request's
	// current state.
	//
	// +optional
	Conditions []metav1.Condition `json:"conditions" protobuf:"bytes,1,rep,name=conditions"`

	// GKE specific fields:

	// NodeGroupName contains the name of the Node Group the Resize Request was
	// created in.
	//
	// +optional
	NodeGroupName *string `json:"nodeGroupName" protobuf:"bytes,2,rep,name=nodeGroupName"`
	// ResizeRequestName contains a name of the MIG Resize Request that Cluster
	// Autoscaler created.
	//
	// +optional
	ResizeRequestName *string `json:"resizeRequestName" protobuf:"bytes,3,rep,name=resizeRequestName"`
}

// The following constants list all currently available Conditions Type values.
// See: https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition
const (
	// Accepted indicates that the request to provision resources for this
	// reservation was accepted and is waiting to be provisioned. For XGCE this
	// state means that reservation was successfully queued in XGCE API.
	Accepted string = "Accepted"
	// Provisioned indicates that all of the requested resources are
	// available in the cluster. Autoscaler will set this condition when the
	// VM creation finishes successfully.
	// This is a terminal stage.
	Provisioned string = "Provisioned"
	// Failed indicates that it is impossible to obtain resources to fulfill
	// this ProvisioningRequest.
	// Condition Reason will contain standardized error messages that is
	// consistent with (and extend those with XGCE specific ones):
	// https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-autoscaler-visibility#noscaleup-reasons
	// This is a terminal stage.
	Failed string = "Failed"
	// Deprecated: the Provisioning Condition Type is longer being populated since 1.28.
	// If the corresponding deprecated Resize Request Provisioning state is present,
	// it will be treated as the Accepted state.
	Provisioning string = "Provisioning"
)

// The following constants list all currently available operation modes.
const (
	// OperationModeGCPQueued denotes that the capacity will be acquired by
	// enqueuing in the Google Cloud.
	OperationModeGCPQueued string = "GCP_QUEUED"
)
