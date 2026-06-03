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

package dynamicresources

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	mt "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// ResourcePredictor is responsible for predicting DRA resources for a node pool in scale-from-0 scenarios. The prediction logic
// is thread-safe and can be accessed from outside the main Cluster Autoscaler goroutine.
//
// ResourcePredictor needs to update its internal state based on DRA objects in the cluster. Cluster Autoscaler tracks
// the DRA objects similarly to Pods and Nodes - it takes a snapshot of them at the beginning of StaticAutoscaler.RunOnce(),
// and stores it in ClusterSnapshot. We want all DRA logic to operate on this snapshot so that it all sees the same
// state within a single CA loop and is easy to reason about. ClusterSnapshot can't be safely read from separate
// goroutines, because it has no synchronization and the main goroutine can be writing to it. So ResourcePredictor needs to
// hook into the main CA goroutine somewhere in order to access the DRA snapshot. CustomResourcesProcessor.FilterOutNodesWithUnreadyResources()
// seems like the best place - it's called at the beginning of RunOnce(), right after the DRA objects are snapshot and before they're used
// to seed the ClusterSnapshot for the next CA loop. So ResourcePredictor technically implements the CustomResourcesProcessor interface,
// but only because its FilterOutNodesWithUnreadyResources() method is called in a convenient place in the main CA loop.
type ResourcePredictor struct {
	stateLock        sync.RWMutex
	deviceClassNames sets.Set[string]
	cloudProvider    CloudProvider
	dranetProvider   machinetypes.DranetConfigProvider

	// TODO(b/485527338): the initialization guard is a temporary solution
	// to a bootstrapping issue where cloud provider creation depends on
	// predictor, and predictor needs the cloud provider to access the
	// machine configuration.
	// The long-term solution requires a refactor to untangle this code and
	// remove one of the dependencies.
	initialized atomic.Bool
}

// GpuConfig contains the type and count of GPUs for a node.
type GpuConfig struct {
	Type  string
	Count int64
}

// NodeHardwareConfig represents the hardware capabilities needed to predict DRA slices.
type NodeHardwareConfig struct {
	MachineType  string
	TpuType      string
	Accelerators []GpuConfig
}

type CloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

func NewResourcePredictor() *ResourcePredictor {
	return &ResourcePredictor{
		deviceClassNames: sets.New[string](),
		dranetProvider:   machinetypes.NewDranetConfigProvider(),
	}
}

// SetCloudProvider sets cloud provider provider for the predictor to use.
func (p *ResourcePredictor) SetCloudProvider(provider CloudProvider) {
	p.cloudProvider = provider
	p.initialized.Store(true)
}

// ResourceSlicesForNode produces a list of resource slices to attach to the template
// node simulated by the autoscaler according to the DRA per-driver enablement
// and resource availability on the node (e.g. accelerator).
//
// ResourceSlicesForNode can be safely called outside the main goroutine (in particular as part of a
// GkeMig.TemplateNodeInfo() call).
func (p *ResourcePredictor) ResourceSlicesForNode(nodePoolSpec *gkeclient.NodePoolSpec, templateNode *apiv1.Node) ([]*resourceapi.ResourceSlice, error) {
	if !p.initialized.Load() {
		// This can happen only during the initalization of GKE cloud provider, when
		// it attempts to validate MIG templates during first force refresh.
		//
		// Because we don't return a cloud provider error here, the node group won't be backed off.
		return nil, fmt.Errorf("can't get DRA slices because provider wasn't initialized yet. This can happen during CA initialization")
	}

	hw := &NodeHardwareConfig{
		MachineType: nodePoolSpec.MachineType,
		TpuType:     nodePoolSpec.TpuType,
	}
	for _, acc := range nodePoolSpec.Accelerators {
		hw.Accelerators = append(hw.Accelerators, GpuConfig{
			Type:  acc.AcceleratorType,
			Count: acc.AcceleratorCount,
		})
	}

	channelClassFound, daemonClassFound := p.checkComputeDomainClasses()
	return PredictResourceSlices(p.cloudProvider.MachineConfigProvider(), p.dranetProvider, hw, templateNode, channelClassFound, daemonClassFound)
}

// FilterOutNodesWithUnreadyResources is called synchronously in StaticAutoscaler.RunOnce(), right after the DRA snapshot is obtained
// and before it's used to seed ClusterSnapshot for the loop. ResourcePredictor uses this method to safely update its state based on
// the DRA snapshot.
func (p *ResourcePredictor) FilterOutNodesWithUnreadyResources(_ *ca_context.AutoscalingContext, allNodes, readyNodes []*apiv1.Node, draSnapshot *drasnapshot.Snapshot) ([]*apiv1.Node, []*apiv1.Node) {
	// Only the main goroutine should modify ClusterSnapshot and the associated DRA snapshot. Since this method is called synchronously from
	// the main goroutine, it should be safe to read the DRA snapshot here.
	p.updateDraState(draSnapshot)

	// We don't actually want to filter anything, so just return the original lists.
	return allNodes, readyNodes
}

// GetNodeResourceTargets is a no-op, needed to satisfy the CustomResourcesProcessor interface.
func (p *ResourcePredictor) GetNodeResourceTargets(_ *ca_context.AutoscalingContext, _ *apiv1.Node, _ cloudprovider.NodeGroup) ([]customresources.CustomResourceTarget, errors.AutoscalerError) {
	return nil, nil
}

// CleanUp is a no-op, needed to satisfy the CustomResourcesProcessor interface.
func (p *ResourcePredictor) CleanUp() {
}

// updateDraState thread-safely updates internal state based on the provided DRA snapshot.
func (p *ResourcePredictor) updateDraState(draSnapshot *drasnapshot.Snapshot) {
	classNames, err := extractDeviceClassNames(draSnapshot)
	if err != nil {
		klog.Errorf("Error while trying to extract DeviceClass names from DRA snapshot: %v", err)
	}

	p.stateLock.Lock()
	p.deviceClassNames = classNames
	p.stateLock.Unlock()
}

// checkComputeDomainClasses thread-safely checks the cached DeviceClass names for ComputeDomain-specific classes.
func (p *ResourcePredictor) checkComputeDomainClasses() (channelClassFound, daemonClassFound bool) {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()
	channelClassFound = p.deviceClassNames.Has(ComputeDomainChannelDeviceClassName)
	daemonClassFound = p.deviceClassNames.Has(ComputeDomainDaemonDeviceClassName)
	return channelClassFound, daemonClassFound
}

// PredictResourceSlices predicts the DRA resource slices that would be created for a node with the given hardware.
// It's deliberately separated from the ResourcePredictor struct to be re-usable for node simulations in GKE Fakes.
// TODO(b/517092928): - per architecture-decisions/node_prediction_decoupling.md we will extract this part of logic into
// the `pkg/nodeprediction` package.
func PredictResourceSlices(
	mcp *mt.MachineConfigProvider,
	dranetProvider mt.DranetConfigProvider,
	hwConfig *NodeHardwareConfig,
	templateNode *apiv1.Node,
	channelClassFound bool,
	daemonClassFound bool,
) ([]*resourceapi.ResourceSlice, error) {
	slices := []*resourceapi.ResourceSlice{}

	if GpuDraDriverEnabled(templateNode) {
		gpuSlices, err := gpuResourceSlicesForNode(mcp, hwConfig, templateNode)
		if err != nil {
			return nil, err
		}
		slices = append(slices, gpuSlices...)
	}

	if TpuDraDriverEnabled(templateNode) {
		tpuSlices, err := tpuResourceSlicesForNode(mcp, hwConfig, templateNode)
		if err != nil {
			return nil, err
		}
		slices = append(slices, tpuSlices...)
	}

	if DRANETDriverEnabled(templateNode) {
		networkSlices, err := dranetResourceSlicesForNode(dranetProvider, hwConfig, templateNode)
		if err != nil {
			return nil, err
		}
		slices = append(slices, networkSlices...)
	}

	if computeDomainDriverEnabled(mcp, templateNode, channelClassFound, daemonClassFound) {
		computeDomainSlices, err := computeDomainResourceSlicesForNode(templateNode)
		if err != nil {
			return nil, err
		}
		slices = append(slices, computeDomainSlices...)
	}

	return slices, nil
}

// assignDevicesIntoResourceSlices builds resource slices for a given driver, node and resource pool
// and distributes devices among resource slices to consider a limit of the amount of devices available
// in a single slice
func assignDevicesIntoResourceSlices(nodeName, driverName, poolName string, allDevices []resourceapi.Device) []*resourceapi.ResourceSlice {
	var slices []*resourceapi.ResourceSlice

	numberOfSlices := 0
	assignedDevices := 0
	for assignedDevices < len(allDevices) {
		sliceName := fmt.Sprintf("%v-slice-%v", poolName, numberOfSlices)
		numberOfSlices += 1
		uuid, _ := uuid.NewRandom()
		slice := &resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: sliceName,
				UID:  types.UID(uuid.String()),
			},
			Spec: resourceapi.ResourceSliceSpec{
				NodeName: &nodeName,
				Driver:   driverName,
				Pool: resourceapi.ResourcePool{
					Name:       poolName,
					Generation: 1,
				},
			},
		}

		sliceDevicesNumber := min(resourceapi.ResourceSliceMaxDevices, len(allDevices)-assignedDevices)
		sliceDevices := allDevices[assignedDevices : assignedDevices+sliceDevicesNumber]
		assignedDevices += sliceDevicesNumber
		slice.Spec.Devices = sliceDevices
		slices = append(slices, slice)
	}

	for _, slice := range slices {
		slice.Spec.Pool.ResourceSliceCount = int64(numberOfSlices)
	}

	return slices
}

// draDriverEnabled determines a driver enablement based on the real
// or the in-memory node labels
//
// Context: go/dra-enablement-on-gke
func draDriverEnabled(node *apiv1.Node, driverLabel string) bool {
	value, foundLabel := node.Labels[driverLabel]
	return foundLabel && value == "true"
}

func extractDeviceClassNames(snapshot *drasnapshot.Snapshot) (sets.Set[string], error) {
	classes, err := snapshot.DeviceClasses().List()
	if err != nil {
		return nil, err
	}
	result := sets.Set[string]{}
	for _, class := range classes {
		result.Insert(class.Name)
	}
	return result, nil
}

// boolAttrValue creates a DeviceAttribute with a boolean value
func boolAttrValue(v bool) resourceapi.DeviceAttribute {
	return resourceapi.DeviceAttribute{
		BoolValue: ptr.To(v),
	}
}

// intAttrValue creates a DeviceAttribute with an integer value
func intAttrValue(v int64) resourceapi.DeviceAttribute {
	return resourceapi.DeviceAttribute{
		IntValue: ptr.To(v),
	}
}

// stringAttrValue creates a DeviceAttribute with a string value
func stringAttrValue(v string) resourceapi.DeviceAttribute {
	return resourceapi.DeviceAttribute{
		StringValue: ptr.To(v),
	}
}
