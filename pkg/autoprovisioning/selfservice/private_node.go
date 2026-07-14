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
	container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

const (
	privateNodeTrue  = "true"
	privateNodeFalse = "false"

	publicIPType  = "public"
	privateIPType = "private"

	privateNodeFromLabel = "internal.private-node-from-label"
	privateNodeFromCcc   = "internal.private-node-from-ccc"
)

type privateNode struct {
	cp CloudProvider
}

func newPrivateNode(cp CloudProvider) *privateNode {
	return &privateNode{cp: cp}
}

func (f *privateNode) FromNodepool(np *container.NodePool) Metadata {
	if np == nil || np.NetworkConfig == nil {
		return nil
	}

	value := strconv.FormatBool(np.NetworkConfig.EnablePrivateNodes)
	// It is fine to set both labels. It does not break the matcher.
	return Metadata{
		privateNodeFromLabel: value,
		privateNodeFromCcc:   value,
	}
}

func (f *privateNode) FromLabelRequirements(req podrequirements.LabelRequirements) Metadata {
	value, found := req.GetSingleValue(gkelabels.PrivateNodeLabel)
	if !found {
		return nil
	}

	switch value {
	case privateNodeTrue, privateNodeFalse:
	default:
		klog.Warningf("Invalid PrivateNodeLabel value: %q, ignoring", value)
		return nil
	}

	if !f.cp.IsClusterUsingPSCInfrastructure() {
		klog.Warningf("PrivateNodeLabel present in cluster with PSC infrastructure disabled, ignoring")
		return nil
	}

	return Metadata{privateNodeFromLabel: value}
}

func (f *privateNode) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.IPType == "" {
		return nil
	}

	if !f.cp.IsClusterUsingPSCInfrastructure() {
		klog.Warningf("CCC IPType defined for cluster with PSC infrastructure disabled, ignoring")
		return nil
	}

	switch spec.NodePoolConfig.IPType {
	case publicIPType:
		return Metadata{privateNodeFromCcc: privateNodeFalse}
	case privateIPType:
		return Metadata{privateNodeFromCcc: privateNodeTrue}
	default:
		klog.Errorf("Invalid IPType value %q - this should never happen", spec.NodePoolConfig.IPType)
		return nil
	}
}

func (f *privateNode) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (f *privateNode) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if value := f.resolveValue(metadata); value != "" {
		labels[gkelabels.PrivateNodeLabel] = value
	}
}

func (f *privateNode) ToNodepool(np *container.NodePool, metadata Metadata) {
	value := f.resolveValue(metadata)
	if value == "" {
		return
	}

	enablePrivateNodes, err := strconv.ParseBool(value)
	if err != nil {
		klog.Errorf("Invalid PrivateNodeLabel value %q - this should never happen: %v", value, err)
		return
	}

	if f.cp.GetDefaultEnablePrivateNodes() == enablePrivateNodes {
		return
	}

	if np.NetworkConfig == nil {
		np.NetworkConfig = &container.NodeNetworkConfig{}
	}
	np.NetworkConfig.EnablePrivateNodes = enablePrivateNodes
	np.NetworkConfig.ForceSendFields = append(np.NetworkConfig.ForceSendFields, "EnablePrivateNodes")
}

// resolveValue resolves the private node setting from metadata.
func (f *privateNode) resolveValue(metadata Metadata) string {
	l, labelFound := metadata[privateNodeFromLabel]
	c, cccFound := metadata[privateNodeFromCcc]

	if labelFound && cccFound {
		klog.Warningf("Conflicting private node settings: pod label and CCC IPType both specified, proceeding with label value")
		return l
	}

	if labelFound {
		return l
	}

	return c
}

func (f *privateNode) UpdateMig(mig GkeMigSetter, metadata Metadata) {
	if _, found := metadata[privateNodeFromCcc]; found {
		// Skip private-node taint; CCC's separation taint handles isolation.
		// Default CCC has no taint, which is also ok.
		return
	}

	value, found := metadata[privateNodeFromLabel]
	if !found {
		return
	}

	enablePrivateNodes, err := strconv.ParseBool(value)
	if err != nil {
		klog.Errorf("Invalid PrivateNodeLabel value %q - this should never happen: %v", value, err)
		return
	}

	if f.cp.GetDefaultEnablePrivateNodes() == enablePrivateNodes {
		return
	}

	if f.cp.IsAutopilotEnabled() {
		taint := apiv1.Taint{
			Key:    gkelabels.PrivateNodeLabel,
			Value:  value,
			Effect: apiv1.TaintEffectNoSchedule,
		}
		mig.SetTaint(taint)
	}
}
