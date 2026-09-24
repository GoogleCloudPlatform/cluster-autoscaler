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
	"context"
	"fmt"
	"strings"
	"time"

	gcev1 "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/resource/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	fakenodeprediction "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodeprediction/fake"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// buildNodeFromTemplate creates a fake v1.Node object using machine type and template info.
// It uses nodeprediction.BuildNodeFromTemplate to ensure consistency.
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

	fakeMigRef := gce.GceRef{Name: mig.Name, Zone: mig.Zone}
	node, err := fakenodeprediction.BuildNodeFromTemplate(context.TODO(), fakeMigRef, mt, template, ke, nodeName)
	if err != nil {
		return nil, err
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
