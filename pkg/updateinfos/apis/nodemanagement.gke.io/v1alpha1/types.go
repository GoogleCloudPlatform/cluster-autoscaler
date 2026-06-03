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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpdateInfo represents a CRD for an ongoing upgrade.
type UpdateInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              UpdateInfoSpec `json:"spec"`
}

// UpdateInfoSpec represents the spec for upgradeInfo CRD.
type UpdateInfoSpec struct {
	// TargetNode is the existing node that is the target for the upgrade.
	TargetNode string `json:"targetNode"`
	// SurgeNode is the replacement node provisioned to replace the TargetNode.
	// +optional
	SurgeNode string `json:"surgeNode"`
	// Type indicated the type of update. It can be one of "Upgrade" or "Repair"
	Type string
	// ValidUntil specifies the time this updateinfo should be considered valid.
	// UpdateInfo CRDs are supposed to be removed after node operation completes;
	// before ValidUntil time is reached. ValidUntil's purpose is solely to prevent
	// issues caused by errors that results in UpdateInfo not removed properly.
	ValidUntil metav1.Time `json:"validUntil"`
	// InstanceGroupUrl contains the URL of the instance group the nodes belong to.
	InstanceGroupUrl string `json:"instanceGroupUrl"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpdateInfoList is a object for list of UpgradeInfos.
type UpdateInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []UpdateInfo `json:"items"`
}
