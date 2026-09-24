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

	gcev1 "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	labelUtils "sigs.k8s.io/cluster-autoscaler/pkg/utils/labels"
)

// FakeMig implements the gce.Mig interface for GceRef() and IsStable().
type FakeMig struct {
	cloudprovider.NodeGroup
	Ref gce.GceRef
}

// GceRef returns the GceRef associated with the FakeMig.
func (m *FakeMig) GceRef() gce.GceRef {
	return m.Ref
}

// IsStable returns true as fake MIGs are always stable.
func (m *FakeMig) IsStable() (bool, error) {
	return true, nil
}

// BuildNodeFromTemplate constructs a Kubernetes Node object from GCE instance template,
// machine type, and KubeEnv information using GceTemplateBuilder.
func BuildNodeFromTemplate(
	ctx context.Context,
	migRef gce.GceRef,
	mt *gcev1.MachineType,
	template *gcev1.InstanceTemplate,
	ke gce.KubeEnv,
	nodeName string,
) (*apiv1.Node, error) {
	if mt == nil {
		return nil, fmt.Errorf("machine type is nil for node %s", nodeName)
	}
	if template == nil {
		return nil, fmt.Errorf("instance template is nil for node %s", nodeName)
	}

	builder := &gce.GceTemplateBuilder{}

	migOsInfo, err := builder.MigOsInfo(ctx, nodeName, ke)
	if err != nil {
		return nil, fmt.Errorf("failed to get MigOsInfo for %s: %w", nodeName, err)
	}

	fMig := &FakeMig{Ref: migRef}
	reserved := &gce.GceReserved{}
	localSSDSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
	memoryBytes := mt.MemoryMb * 1024 * 1024

	templateToUse := template
	if template.Properties != nil && len(template.Properties.GuestAccelerators) == 0 && len(mt.Accelerators) > 0 {
		var guestAccelerators []*gcev1.AcceleratorConfig
		for _, acc := range mt.Accelerators {
			if strings.HasPrefix(acc.GuestAcceleratorType, "nvidia") {
				guestAccelerators = append(guestAccelerators, &gcev1.AcceleratorConfig{
					AcceleratorCount: acc.GuestAcceleratorCount,
					AcceleratorType:  acc.GuestAcceleratorType,
				})
			}
		}
		if len(guestAccelerators) > 0 {
			propsCopy := *template.Properties
			propsCopy.GuestAccelerators = guestAccelerators
			tmplCopy := *template
			tmplCopy.Properties = &propsCopy
			templateToUse = &tmplCopy
		}
	}

	node, err := builder.BuildNodeFromTemplate(ctx, fMig, migOsInfo, templateToUse, ke, mt.GuestCpus, memoryBytes, nil, reserved, localSSDSizeProvider)
	if err != nil {
		return nil, fmt.Errorf("failed to build node from template for %s: %w", nodeName, err)
	}

	node.Name = nodeName

	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	if nodeName != "" {
		node.Labels[apiv1.LabelHostname] = nodeName
	}
	if migRef.Name != "" {
		node.Labels["cloud.google.com/gke-nodepool"] = migRef.Name
	}
	if mfName, err := gce.GetMachineFamily(mt.Name); err == nil {
		node.Labels[gkelabels.MachineFamilyLabel] = mfName
	}
	labelUtils.UpdateDeprecatedLabels(node.Labels)
	gkelabels.UpdateDeprecatedLabels(node.Labels)

	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	// Default last-applied-node-labels if not already set by ke
	if _, exists := node.Annotations["node.gke.io/last-applied-node-labels"]; !exists {
		node.Annotations["node.gke.io/last-applied-node-labels"] = "fake-node-label"
	}

	// Default KubeletVersion if not already populated (needed to prevent panic in NAP tests)
	if node.Status.NodeInfo.KubeletVersion == "" {
		node.Status.NodeInfo.KubeletVersion = "v1.30.0"
	}

	// TPU capacity and taints for non-nvidia accelerators
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

	return node, nil
}
