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
	"strconv"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

type nodeAutoRepair struct {
	internalFeatureDefaultImplementation
}

func newNodeAutoRepair() feature {
	return &nodeAutoRepair{}
}

func (w *nodeAutoRepair) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Management == nil {
		return nil
	}
	m := make(Metadata)
	m[gkelabels.AutoRepairLabelKey] = strconv.FormatBool(pool.Management.AutoRepair)
	return m
}

func (n *nodeAutoRepair) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (w *nodeAutoRepair) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.AutoRepair == nil {
		return nil
	}
	m := make(Metadata)
	m[gkelabels.AutoRepairLabelKey] = strconv.FormatBool(*spec.NodePoolConfig.AutoRepair)
	return m
}

func (w *nodeAutoRepair) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (n *nodeAutoRepair) ToNodePoolLabels(_ map[string]string, _ Metadata) {
}

func (w *nodeAutoRepair) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if pool.Management == nil {
		return
	}
	if autoRepairString, found := metadata[gkelabels.AutoRepairLabelKey]; found {
		autoRepair, err := strconv.ParseBool(autoRepairString)
		if err != nil {
			klog.Errorf("Auto repair parsing error: %v", err)
			return
		}
		pool.Management.AutoRepair = autoRepair
	}
}
