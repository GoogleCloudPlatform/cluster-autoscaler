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

package interfaces

import (
	"reflect"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

// CreateNodePoolResult represents result of creating node pool.
type CreateNodePoolResult struct {
	MainCreatedNodeGroup   AutoprovisionedNodeGroup
	ExtraCreatedNodeGroups []AutoprovisionedNodeGroup
}

// AllCreatedNodeGroups returns all created node groups - main node group with extra node groups.
func (r CreateNodePoolResult) AllCreatedNodeGroups() []AutoprovisionedNodeGroup {
	var result []AutoprovisionedNodeGroup
	if r.MainCreatedNodeGroup != nil && !reflect.ValueOf(r.MainCreatedNodeGroup).IsNil() {
		result = append(result, r.MainCreatedNodeGroup)
	}
	result = append(result, r.ExtraCreatedNodeGroups...)
	return result
}

// AutoprovisionedNodeGroup extends NodeGroup interface with autoprovisioning specific methods.
// It is exported for historical reasons related to AutoprovisionedCreate and should not be used outside the autoprovisioning package.
// TODO: Determine how to unexport it.
type AutoprovisionedNodeGroup interface {
	cloudprovider.NodeGroup

	// AutoprovisionedCreate creates the node group on the cloud provider side.
	// Extra node groups can be created on the way.
	// TODO: as soon as we remove Create() from OSS NodeGroup this method should be renamed to Create.
	AutoprovisionedCreate() (CreateNodePoolResult, error)
	// QueuedProvisioning returns whether a MIG uses queued provisioning
	QueuedProvisioning() bool
	// NodePoolName returns the name of the GKE node pool this Mig belongs to.
	NodePoolName() string
	// IsAutopilot returns whether a MIG was created in an Autopilot cluster.
	IsAutopilot() bool
	// CreateAsync creates node group asynchronously.
	// Immediately returns a result with upcoming node groups. Executes initializer for initial scaleup ane reporting.
	CreateAsync(updater AsyncNodeGroupUpdater, initializer AsyncNodeGroupInitializer) (CreateNodePoolResult, error)
	// DeleteAsync deletes node-group asynchronously.
	// Returns immediately. Executed finalizer when node group is deleted.
	DeleteAsync(finalizer AsyncNodeGroupFinalizer) error
	// IsStable returns true iff there are no ongoing operations on the node group.
	IsStable() (bool, error)
}

// AsyncNodeGroupUpdater responsible for updating asynchroneously created node groups before they are created.
type AsyncNodeGroupUpdater interface {
	// GetTargetSize return a size to which the provided node goup will be initialized.
	// Note that the node group may be different than the initialized node group, if node group creation
	// triggers creation of multiple node groups.
	GetTargetSize(nodeGroup string) int64
	// SetTargetSize updates a size to which the provided node goup will be initialized.
	// Note that the node group may be different than the initialized node group, if node group creation
	// results in creation of multiple node groups.
	SetTargetSize(nodeGroup string, size int64)
	// ChangeTargetSize changes by delta a size to which the provided node goup will be initialized.
	// Note that the node group may be different than the initialized node group, if node group creation
	// results in creation of multiple node groups.
	ChangeTargetSize(nodeGroup string, delta int64)
}

// AsyncNodeGroupInitializer responsible for initializing asynchroneously created node groups
type AsyncNodeGroupInitializer interface {
	// InitializeNodeGroup executed when node group is created asynchronously (but not yet reported as existing).
	// In most cases node group initialization should involve scaling up newly created node groups.
	InitializeNodeGroup(result AsyncCreateNodePoolResult)
}

// AsyncNodeGroupFinalizer responsible for finalizing node group deletion
type AsyncNodeGroupFinalizer interface {
	// FinalizeNodeGroup executed after node pool is deleted.
	FinalizeNodeGroup(result AsyncDeleteNodePoolResult)
}

// AsyncNodeGroupInitializerFunc functional represenations of AsyncNodeGroupInitializer
type AsyncNodeGroupInitializerFunc func(result AsyncCreateNodePoolResult)

func (f AsyncNodeGroupInitializerFunc) InitializeNodeGroup(result AsyncCreateNodePoolResult) {
	f(result)
}

// AsyncNodeGroupFinalizerFunc functional representation of AsyncNodeGroupFinalizer
type AsyncNodeGroupFinalizerFunc func(result AsyncDeleteNodePoolResult)

func (f AsyncNodeGroupFinalizerFunc) FinalizeNodeGroup(result AsyncDeleteNodePoolResult) {
	f(result)
}

// AsyncCreateNodePoolResult contains the result of asynchronous node pool creation process.
type AsyncCreateNodePoolResult struct {
	// InjectedMigs are migs that were injected to scale-up simulation when node pool was being created. Their ids may differ from values stored in CreationResult.
	InjectedMigs []AutoprovisionedNodeGroup
	// CreationResult contains node pool creation result.
	CreationResult CreateNodePoolResult
	// Error occurred during async node pool creation process.
	Error error
	// CreatedToUpcomingMapping specifies a mapping between previously created node groups
	// and the previously upcoming (injected) ones.
	CreatedToUpcomingMapping map[string]string
}

// AsyncDeleteNodePoolResult contains the result of asynchronous node pool deletion process.
type AsyncDeleteNodePoolResult struct {
	// Migs contains all migs removed with the node pool.
	Migs []AutoprovisionedNodeGroup
	// Error occurred during async node pool deletion process.
	Error error
}
