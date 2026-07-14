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

package processors

import (
	"errors"
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	autoscaling_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	auto_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/klog/v2"
)

// TotalMaxSizeProcessor wraps the NodeGroupSetProcessor and enforces
// the total size limits for the node pool. This processor works in tandem with
// change of semantics for the node group MaxSize function.
// For more details refer to: go/improve-nodepool-size-control-dd
type TotalMaxSizeProcessor struct {
	nodegroupset.NodeGroupSetProcessor
}

// NewTotalMaxSizeProcessor creates a new ScaleUpProcessor.
func NewTotalMaxSizeProcessor(p nodegroupset.NodeGroupSetProcessor) *TotalMaxSizeProcessor {
	return &TotalMaxSizeProcessor{
		NodeGroupSetProcessor: p,
	}
}

// FindSimilarNodeGroups returns a list of NodeGroups similar to the one provided in parameter.
func (s *TotalMaxSizeProcessor) FindSimilarNodeGroups(context *autoscaling_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	return s.NodeGroupSetProcessor.FindSimilarNodeGroups(context, nodeGroup, nodeInfosForGroups)
}

// BalanceScaleUpBetweenGroups enforces the total max size limit and runs the
// wrapped Processor.
func (s *TotalMaxSizeProcessor) BalanceScaleUpBetweenGroups(context *autoscaling_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {
	newNodes, err := s.enforceTotalMaxSizeLimit(groups, newNodes)
	if err != nil {
		return nil, auto_errors.NewAutoscalerErrorf(auto_errors.InternalError, "received error while enforcing total max size: %v", err)
	}
	if newNodes <= 0 {
		return []nodegroupset.ScaleUpInfo{}, nil
	}

	return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
}

func (s *TotalMaxSizeProcessor) enforceTotalMaxSizeLimit(groups []cloudprovider.NodeGroup, newNodes int) (int, error) {
	if len(groups) == 0 {
		return 0, errors.New("got empty groups slice")
	}

	migs, err := s.getMIGs(groups)
	if err != nil {
		return 0, err
	}
	if !migs[0].TotalSizeLimitEnabled() {
		return newNodes, nil
	}
	klog.V(2).Infof("Total max size limit enabled for nodepool: %s", migs[0].NodePoolName())

	totalMaxSize := migs[0].TotalMaxSize()
	nodePoolTargetSize, err := migs[0].NodePoolTargetSize()
	if err != nil {
		return 0, err
	}

	maxNewNodes := totalMaxSize - nodePoolTargetSize
	if newNodes > maxNewNodes {
		klog.V(2).Infof("Requested scale-up (%d) plus current size (%d) exceeds node pool total max size (%d), capping scale-up to %d", newNodes, nodePoolTargetSize, totalMaxSize, maxNewNodes)
		newNodes = maxNewNodes
	}
	return newNodes, nil
}

func (s *TotalMaxSizeProcessor) getMIGs(groups []cloudprovider.NodeGroup) ([]*gke.GkeMig, error) {
	migs := make([]*gke.GkeMig, 0, len(groups))
	for _, g := range groups {
		mig, ok := g.(*gke.GkeMig)
		if !ok {
			return nil, fmt.Errorf("got a NodeGroup that is not castable to GkeMig: %v", g)
		}
		migs = append(migs, mig)
	}
	return migs, nil
}
