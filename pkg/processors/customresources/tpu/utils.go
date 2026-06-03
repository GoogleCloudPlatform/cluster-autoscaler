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

package tpu

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	klog "k8s.io/klog/v2"
)

type TpuResource struct {
	TpuType  string
	Topology string
	Count    int64
}

func getTpuFromTemplate(nodeGroup cloudprovider.NodeGroup) (TpuResource, bool, errors.AutoscalerError) {
	template, err := nodeGroup.TemplateNodeInfo()
	if err != nil {
		klog.Errorf("Failed to build template for getting TPU estimation for node group %v: %v", nodeGroup.Id(), err)
		return TpuResource{}, false, errors.ToAutoscalerError(errors.CloudProviderError, err)
	}

	tpuLabel, found := template.Node().Labels[gkelabels.TPULabel]
	if !found {
		return TpuResource{}, false, nil
	}

	topology, found := template.Node().Labels[gkelabels.TPUTopologyLabel]
	if !found {
		return TpuResource{}, false, nil
	}

	tpuCapacity, found := template.Node().Status.Capacity[tpu.ResourceGoogleTPU]
	if !found {
		return TpuResource{}, false, nil
	}

	tr := TpuResource{
		TpuType:  tpuLabel,
		Topology: topology,
		Count:    tpuCapacity.Value(),
	}
	return tr, true, nil
}
