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

package selfservice

import (
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// InitSelfService initializes supportedFeatures.
// This function MUST be called before anything else
// to ensure that features are set and operate correctly.
func InitSelfService(cp CloudProvider) {
	supportedFeatures = []feature{
		newNodePoolGroupName(),
		newWorkloadType(),
		newConfidentialNodeType(),
		newCapacityCheckWaitTimeSeconds(),
		newNodeAutoRepair(),
		newNodeAutoUpgrade(),
		newImageStreaming(),
		newGvnic(),
		newSandbox(),
		newResourceManagerTags(),
		newLocationPolicy(),
		newLoggingConfig(),
		newPrivateNode(cp),
		newDraFeature(),
		newAcceleratorNetworkProfile(),
		newGpuDirect(),
		newSecureBootFeature(),
		newWorkloadMetadata(),
		newInstanceMetadata(),
	}
}

// CloudProvider is an interface defining the subset of cloud provider methods
// required by the selfservice features. This allows for a minimal viable interface.                                                                                                              │
type CloudProvider interface {
	IsClusterUsingPSCInfrastructure() bool
	GetDefaultEnablePrivateNodes() bool
	IsAutopilotEnabled() bool
}

type GkeMigSetter interface {
	SetLocationPolicy(string)
	SetTaint(apiv1.Taint)
}

// Metadata stores self-service features metadata and is used for matching Migs
// to CCCs and Priorities. Mig matches a CCC/priority iff. Mig's nodepool
// metadata is a superset of the CCC/priority metadata.
type Metadata map[string]string

// feature is an interface used to easily add self-services to NAP. It covers
// basic gathering information about features from pod requests (either direct
// node selector or CCC), and then translates them to Nodepool structures
// for both scheduling simulation and nodepool creation.
type feature interface {
	// FromNodepool extracts self-service metadata from Nodepool definition
	// passed from GKE API. This method is needed for all features.
	FromNodepool(*container.NodePool) Metadata
	// FromLabelRequirements extracts self-service pod requests from label
	// requirements. It is needed only if feature request can be defined with node
	// selector / node affinity.
	FromLabelRequirements(podrequirements.LabelRequirements) Metadata
	// FromCccSpec extracts self-service pod requests from CCC spec. It is needed
	// only if feature request can be defined within ComputeClassSpec.
	FromCccSpec(v1.ComputeClassSpec) Metadata
	// FromPriority extracts self-service pod requests from CCC priority. It is
	// needed only if feature request can be defined within Priority.
	FromPriority(v1.Priority) Metadata
	// ToNodePoolLabels sets feature related labels that impact the scheduling
	// simulation. It needs to be used for features that are enabled using labels
	// or ones that are impact the labels present on nodes.
	ToNodePoolLabels(map[string]string, Metadata)
	// ToNodepool sets feature gates for Nodepool creation call passed to GKE API.
	// This method is needed for all features.
	ToNodepool(*container.NodePool, Metadata)

	internalFeature
}

// internalFeature is an extension of self-service with methods
// used for non-standard behavior.
// Do not use for self-service unless confirmed by CCC team.
type internalFeature interface {
	// UpdateMig allows setting mig and its nodepool spec directly before
	// it is injected for simulation. Used in limited cases related to HT NAP.
	UpdateMig(GkeMigSetter, Metadata)
}

type internalFeatureDefaultImplementation struct{}

func (ds *internalFeatureDefaultImplementation) UpdateMig(mig GkeMigSetter, metadata Metadata) {
}

var (
	// supportedFeatures lists all supported self-service features.
	// It is set by InitSelfService.
	supportedFeatures []feature
)

// NodepoolMetadata returns Metadata of provided Nodepool
func NodepoolMetadata(np *container.NodePool) Metadata {
	m := make(Metadata)
	for _, f := range supportedFeatures {
		for k, v := range f.FromNodepool(np) {
			m[k] = v
		}
	}
	return m
}

// LabelRequirementsMetadata returns Metadata of provided LabelRequirements
func LabelRequirementsMetadata(req podrequirements.LabelRequirements) Metadata {
	m := make(Metadata)
	for _, f := range supportedFeatures {
		for k, v := range f.FromLabelRequirements(req) {
			m[k] = v
		}
	}
	return m
}

// ComputeClassSpecMetadata returns Metadata of provided ComputeClassSpec
func ComputeClassSpecMetadata(s v1.ComputeClassSpec) Metadata {
	m := make(Metadata)
	for _, f := range supportedFeatures {
		for k, v := range f.FromCccSpec(s) {
			m[k] = v
		}
	}
	return m
}

// PriorityMetadata returns Metadata of provided Priority.
func PriorityMetadata(p v1.Priority) Metadata {
	m := make(Metadata)
	for _, f := range supportedFeatures {
		for k, v := range f.FromPriority(p) {
			m[k] = v
		}
	}
	return m
}

// UpdateNodePoolLabels modifies provided NodePoolSpec based on the provided Metadata.
func UpdateNodePoolLabels(labels map[string]string, m Metadata) {
	for _, f := range supportedFeatures {
		f.ToNodePoolLabels(labels, m)
	}
}

// UpdateNodepool modifies provided Nodepool based on the provided Metadata.
func UpdateNodepool(np *container.NodePool, m Metadata) {
	for _, f := range supportedFeatures {
		f.ToNodepool(np, m)
	}
}

// UpdateMig is called by cloud provider as the final step before NAP creates a mig for simulation.
func UpdateMig(mig GkeMigSetter, m Metadata) {
	for _, f := range supportedFeatures {
		f.UpdateMig(mig, m)
	}
}
