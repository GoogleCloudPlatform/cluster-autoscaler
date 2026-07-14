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

package fake

import (
	"fmt"
	"strings"
	"time"

	gcev1 "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

// fakeMig implements the Mig interface for GceRef().
type fakeMig struct {
	cloudprovider.NodeGroup
	ref gce.GceRef
}

func (m *fakeMig) GceRef() gce.GceRef {
	return m.ref
}

func (m *fakeMig) IsStable() (bool, error) {
	return true, nil
}

// buildNodeFromTemplate creates a fake v1.Node object using machine type and template info.
// It uses the production `GceTemplateBuilder` to ensure consistency.
// TODO(b/496548045): refactor this function to deduplicate TPU and DRA prediction logics.
func buildNodeFromTemplate(
	mig *gcev1.InstanceGroupManager,
	mt *gcev1.MachineType,
	template *gcev1.InstanceTemplate,
	nodeName string,
) (*apiv1.Node, error) {
	ke, err := gce.ExtractKubeEnv(template)
	if err != nil {
		return nil, err
	}

	builder := &gce.GceTemplateBuilder{}

	migOsInfo, err := builder.MigOsInfo(nodeName, ke)
	if err != nil {
		return nil, err
	}

	fakeMigRef := gce.GceRef{Name: mig.Name, Zone: mig.Zone}
	fMig := &fakeMig{ref: fakeMigRef}

	reserved := &gce.GceReserved{}
	localSSDSizeProvider := localssdsize.NewSimpleLocalSSDProvider()

	memoryBytes := mt.MemoryMb * 1024 * 1024

	node, err := builder.BuildNodeFromTemplate(fMig, migOsInfo, template, ke, mt.GuestCpus, memoryBytes, nil, reserved, localSSDSizeProvider)
	if err != nil {
		return nil, err
	}

	node.Name = nodeName

	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels["cloud.google.com/gke-nodepool"] = mig.Name

	if mfName, err := gce.GetMachineFamily(mt.Name); err == nil {
		node.Labels[gkelabels.MachineFamilyLabel] = mfName
	}

	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	// Needed to avoid Cluster is not ready for autoscaling warning.
	node.Annotations["node.gke.io/last-applied-node-labels"] = "fake-node-label"
	// Kubelet version is used by custom_resources_processor.go while calling nodetemplate.BuildKeyForNAP
	// as the nodeVersion argument. While not being set, causes tests using NAP node pools to panic.
	node.Status.NodeInfo.KubeletVersion = "v1.30.0"

	for _, acc := range mt.Accelerators {
		if !strings.HasPrefix(acc.GuestAcceleratorType, "nvidia") {
			tpuResourceName := apiv1.ResourceName("google.com/tpu")
			tpuQuantity := resource.MustParse(fmt.Sprintf("%d", acc.GuestAcceleratorCount))
			if node.Status.Capacity == nil {
				node.Status.Capacity = apiv1.ResourceList{}
			}
			node.Status.Capacity[tpuResourceName] = tpuQuantity
			node.Labels[gkelabels.TPULabel] = acc.GuestAcceleratorType
			node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
				Key:    "google.com/tpu",
				Value:  "present",
				Effect: apiv1.TaintEffectNoSchedule,
			})
			if node.Status.Allocatable == nil {
				node.Status.Allocatable = apiv1.ResourceList{}
			}
			node.Status.Allocatable[tpuResourceName] = tpuQuantity
		}
	}

	test.SetNodeReadyState(node, true, time.Now())
	return node, nil
}

func predictResourceSlices(
	mcp *machinetypes.MachineConfigProvider,
	dracp machinetypes.DranetConfigProvider,
	mt *gcev1.MachineType,
	node *apiv1.Node,
) ([]*v1.ResourceSlice, error) {
	hwConfig := &dynamicresources.NodeHardwareConfig{
		MachineType: mt.Name,
	}
	for _, acc := range mt.Accelerators {
		if strings.HasPrefix(acc.GuestAcceleratorType, "nvidia") {
			hwConfig.Accelerators = append(hwConfig.Accelerators, dynamicresources.GpuConfig{
				Type:  acc.GuestAcceleratorType,
				Count: acc.GuestAcceleratorCount,
			})
		}
	}
	tpuType, err := extractTpuType(mcp, mt.Name)
	if err != nil {
		return nil, err
	}
	hwConfig.TpuType = tpuType
	if len(hwConfig.Accelerators) > 0 && hwConfig.TpuType != "" {
		return nil, fmt.Errorf("machine type %s has both gpu and tpu type", mt.Name)
	}

	// Call the shared logic. We pass true for class findings to ensure compute domain slices
	// generate in the fake environment if the machine family matches.
	return dynamicresources.PredictResourceSlices(
		mcp,
		dracp,
		hwConfig,
		node,
		// TODO(b/463315524): currently these parameters are calculated by checkComputeDomainClasses() which
		// uses DRA ResourcePredictor internal state, i.e. locks. But in the long term the same logic should be
		// shared between production logic and test fakes.
		// For now, we're assuming there are no user-installed DRA drivers in the cluster.
		false,
		false,
	)
}

func extractTpuType(mcp *machinetypes.MachineConfigProvider, machineType string) (string, error) {
	mf, err := mcp.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		klog.V(4).Infof("Unable to determine machine family for machine type %q: %v. Proceeding with empty TPU type.", machineType, err)
		return "", nil
	}
	if !mf.IsTpuSupported() {
		return "", nil
	}
	tpuType, err := mcp.TpuTypeForMachineFamily(mf.Name())
	if err != nil {
		return "", err
	}
	return tpuType, nil
}
