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

package ccc

import (
	"reflect"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/klog/v2"
)

// cccProvider allows fetching the CCC name for a NodeGroup.
type cccProvider interface {
	NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error)
}

// NodeGroupChangePerCccMetricsProducer is an implementation of NodeGroupChangeObserver for reporting the scale up/down metrics per CCC
type NodeGroupChangePerCccMetricsProducer struct {
	cccProvider cccProvider
}

// NewNodeGroupChangePerCCCMetricsProducer returns a new NodeGroupChangePerCCCMetricsProducer.
func NewNodeGroupChangePerCCCMetricsProducer(cccProvider cccProvider) *NodeGroupChangePerCccMetricsProducer {
	return &NodeGroupChangePerCccMetricsProducer{cccProvider: cccProvider}
}

// RegisterScaleUp calls RegisterScaleUp for each observer.
func (p *NodeGroupChangePerCccMetricsProducer) RegisterScaleUp(nodeGroup cloudprovider.NodeGroup,
	delta int, currentTime time.Time) {
	cccName := p.getCrdNameForNodeGroup(nodeGroup)
	Metrics.RegisterScaleUp(cccName, delta)
}

// RegisterScaleDown calls RegisterScaleDown for each observer.
func (p *NodeGroupChangePerCccMetricsProducer) RegisterScaleDown(nodeGroup cloudprovider.NodeGroup,
	nodeName string, currentTime time.Time, expectedDeleteTime time.Time) {
}

// RegisterFailedScaleUp emits the failed scale up metric.
func (p *NodeGroupChangePerCccMetricsProducer) RegisterFailedScaleUp(nodeGroup cloudprovider.NodeGroup,
	delta int, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) {
}

// RegisterFailedScaleDown records failed scale-down for a nodegroup.
func (p *NodeGroupChangePerCccMetricsProducer) RegisterFailedScaleDown(nodeGroup cloudprovider.NodeGroup,
	reason string, currentTime time.Time) {
}

func (p *NodeGroupChangePerCccMetricsProducer) getCrdNameForNodeGroup(nodeGroup cloudprovider.NodeGroup) string {
	if p.cccProvider == nil || reflect.ValueOf(p.cccProvider).IsNil() {
		return ""
	}
	_, crdName, err := p.cccProvider.NodeGroupCrd(nodeGroup)
	if err != nil {
		klog.Errorf("Cannot fetch the crdName for nodegroup %q: , err: %v", nodeGroup.Id(), err)
		return ""
	}
	return crdName
}
