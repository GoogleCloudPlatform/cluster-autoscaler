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

package computeclass

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// AutoscalingOptionsProvider fetches autoscaling option overrides for a given node group
type AutoscalingOptionsProvider interface {
	ScaleDownUnneededTime(cloudprovider.NodeGroup) (time.Duration, bool, error)
	ScaleDownUtilizationThreshold(cloudprovider.NodeGroup) (float64, bool, error)
	ScaleDownGpuUtilizationThreshold(cloudprovider.NodeGroup) (float64, bool, error)
}

func NewAutoscalingOptionsProvider(lister lister.Lister, experimentsManager experiments.Manager) AutoscalingOptionsProvider {
	return &autoscalingOptionsProvider{
		lister:             lister,
		experimentsManager: experimentsManager,
	}
}

type autoscalingOptionsProvider struct {
	lister             lister.Lister
	experimentsManager experiments.Manager
}

func (p *autoscalingOptionsProvider) getCRDConsolidationDelay(nodeGroup cloudprovider.NodeGroup) (time.Duration, bool, error) {
	crd, crdName, err := p.lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return 0, false, err
	}

	if crd == nil || crdName == "" {
		return 0, false, nil
	}

	if crdDelay := crd.ConsolidationDelay(); crdDelay != nil {
		return *crdDelay, true, nil
	}
	return 0, false, nil
}

func (p *autoscalingOptionsProvider) getMigConsolidationDelay(nodeGroup cloudprovider.NodeGroup) (time.Duration, bool, error) {
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return 0, false, nil
	}

	migDelay, err := gkeMig.Spec().ConsolidationDelay()
	if err != nil {
		return 0, true, err
	}
	if migDelay != nil {
		return *migDelay, true, nil
	}
	return 0, false, nil
}

func (p *autoscalingOptionsProvider) ScaleDownUnneededTime(nodeGroup cloudprovider.NodeGroup) (time.Duration, bool, error) {
	if p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.NodePoolConsolidationDelayMinCAVersionFlag, false) {
		migDelay, hasConsolidationDelay, err := p.getMigConsolidationDelay(nodeGroup)
		if err != nil {
			return 0, hasConsolidationDelay, err
		}
		if hasConsolidationDelay {
			klog.V(5).Infof("Overriding consolidation delay for %q with MIG value; new val=%q", nodeGroup.Id(), migDelay)
			return migDelay, hasConsolidationDelay, err
		}
	}

	crdDelay, hasConsolidationDelay, err := p.getCRDConsolidationDelay(nodeGroup)
	if hasConsolidationDelay && err == nil {
		klog.V(5).Infof("Overriding consolidation delay for %q with CRD value; new val=%q", nodeGroup.Id(), crdDelay)
	}
	return crdDelay, hasConsolidationDelay, err
}

func (p *autoscalingOptionsProvider) ScaleDownUtilizationThreshold(nodeGroup cloudprovider.NodeGroup) (float64, bool, error) {
	crd, crdName, err := p.lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return 0, false, err
	}

	if crd == nil || crdName == "" {
		return 0, false, nil
	}

	// NOTE: cast from int to float64 percentage is required
	if threshold := crd.ConsolidationThreshold(); threshold != nil {
		return float64(*threshold) / 100.0, true, nil
	}

	return 0, false, nil
}

func (p *autoscalingOptionsProvider) ScaleDownGpuUtilizationThreshold(nodeGroup cloudprovider.NodeGroup) (float64, bool, error) {
	crd, crdName, err := p.lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return 0, false, err
	}

	if crd == nil || crdName == "" {
		return 0, false, nil
	}

	// NOTE: cast from int to float64 percentage is required
	if gpuThreshold := crd.GPUConsolidationThreshold(); gpuThreshold != nil {
		return float64(*gpuThreshold) / 100.0, true, nil
	}

	return 0, false, nil
}
