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
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	oss_nodeinfosprovider "k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"

	"k8s.io/klog/v2"
)

const (
	nodeInfoOverwrittenSuccessfully = "overwritten"
	nodeInfoOverwritingFailure      = "failed_overwrite"
	nodeInfoNoOverwriting           = "no_overwrite"
)

// ShortLivedUpgradeNodeInfoProvider replaces nodeInfos returned by provided TemplateNodeInfoProvider for node pools with ShortLived upgrade in progress
type ShortLivedUpgradeNodeInfoProvider struct {
	// Using concrete type instead of interface to ensure that ShortLivedUpgradeNodeInfoProvider will apply right after nodeInfos are created by MixedTemplateNodeInfoProvider and the replaced nodeInfos will go through the rest of the providers
	nodeInfoProvider *oss_nodeinfosprovider.MixedTemplateNodeInfoProvider
}

// NewShortLivedUpgradeNodeInfoProvider returns a new instance of ShortLivedUpgradeNodeInfoProvider
func NewShortLivedUpgradeNodeInfoProvider(nodeInfoProvider *oss_nodeinfosprovider.MixedTemplateNodeInfoProvider) oss_nodeinfosprovider.TemplateNodeInfoProvider {
	return &ShortLivedUpgradeNodeInfoProvider{
		nodeInfoProvider: nodeInfoProvider,
	}
}

func (p *ShortLivedUpgradeNodeInfoProvider) Process(ctx *context.AutoscalingContext, nodes []*apiv1.Node, daemonsets []*appsv1.DaemonSet, taintConfig taints.TaintConfig, now time.Time) (map[string]*framework.NodeInfo, errors.AutoscalerError) {
	nodeInfos, err := p.nodeInfoProvider.Process(ctx, nodes, daemonsets, taintConfig, now)
	if err != nil {
		return nil, err
	}

	loggingQuota := logging.NodeGroupLoggingQuota()
	loggingAdditionalQuota := logging.NodeGroupLoggingQuota()
	nodeGroups := ctx.CloudProvider.NodeGroups()
	for _, nodeGroup := range nodeGroups {
		mig, ok := nodeGroup.(*gke.GkeMig)
		if !ok {
			klog.Errorf(`Unexpected NodeGroup type: want "*gke.GkeMig", got %q"`, reflect.TypeOf(nodeGroup))
			continue
		}

		if !mig.QueuedProvisioning() && !mig.FlexStartNonQueued() {
			continue
		}
		if !mig.ShortLivedUpgradeInProgress() || !isNodeInfoReal(nodeInfos[nodeGroup.Id()]) {
			metrics.Metrics.RecordOverwrittenShortLivedNodeInfos(mig.GceRef().Name, nodeInfoNoOverwriting)
			continue
		}
		// nodeInfo was created from existing node, so it might have used incorrect InstanceTemplate
		// overwrite with nodeInfo generated from Template, as it would be done in MixedTemplateNodeInfoProvider when there are no good node candidates
		generatedNodeInfo, err := simulator.SanitizedTemplateNodeInfoFromNodeGroup(nodeGroup, daemonsets, taintConfig)
		if err != nil {
			klog.Errorf(`Failed to retrieve TemplateNodeInfo for MIG %s in %s, got error: %v"`, mig.GceRef().Name, mig.GceRef().Zone, err)
			metrics.Metrics.RecordOverwrittenShortLivedNodeInfos(mig.GceRef().Name, nodeInfoOverwritingFailure)
			continue
		}
		klogx.V(1).UpTo(loggingQuota).Infof("Overwriting nodeInfo for nodeGroup %s, because there's ShortLived upgrade in progress and nodes with old InstanceTemplate are still running", nodeGroup.Id())
		klogx.V(1).UpTo(loggingAdditionalQuota).Infof("Old overwritten nodeInfo %+v, new nodeInfo %+v", nodeInfos[nodeGroup.Id()], generatedNodeInfo)
		nodeInfos[nodeGroup.Id()] = generatedNodeInfo
		metrics.Metrics.RecordOverwrittenShortLivedNodeInfos(mig.GceRef().Name, nodeInfoOverwrittenSuccessfully)
	}
	klogx.V(1).Over(loggingQuota).Infof("There are also %v other node groups having ShortLived upgrade in progress for nodeInfo was overwritten", -loggingQuota.Left())
	return nodeInfos, nil
}

func isNodeInfoReal(nodeInfo *framework.NodeInfo) bool {
	_, found := nodeInfo.Node().Annotations[labels.NodeGeneratedFromTemplateAnnotation]
	return !found
}

// CleanUp cleans up internal structures recursively
func (p *ShortLivedUpgradeNodeInfoProvider) CleanUp() {
	p.nodeInfoProvider.CleanUp()
}
