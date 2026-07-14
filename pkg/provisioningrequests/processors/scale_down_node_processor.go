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
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
	klog "k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// CloudProvider is the subset of GkeCloudProvider needed for ProvisioningRequestScaleDownNodeProcessor.
type CloudProvider interface {
	GkeMigForNode(node *apiv1.Node) (*gke.GkeMig, error)
	// QueuedProvisioningNodeHasScaleDownImmunity returns true if the provided QueuedProvisioning node still shouldn't get scaled down,
	// i.e. additionalImmunity hasn't ran out yet.
	QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool
}

// ProvisioningRequestScaleDownNodeProcessor ensures the nodes requested by ProvisioningRequests (nodes in nodepools with `QueuedProvisioning` flag) won't be scaled down for `BookingDuration` period of time.
type ProvisioningRequestScaleDownNodeProcessor struct {
	cloudProvider CloudProvider
	// The additional time (on top of the `scaleDownUnneededTime`) required to ensure that node won't be deleted in `BookingDuration` period.
	additionalImmunity time.Duration
	now                func() time.Time
}

func NewProvisioningRequestScaleDownNodeProcessor(cloudProvider CloudProvider, scaleDownUnneededTime time.Duration) (*ProvisioningRequestScaleDownNodeProcessor, bool) {
	// The nodes are being filtered out from being unneeded (being scale down candidates) for `additionalImmunity` period,
	// so that later  with the `scaleDownUnneededTime` the immunity period will sum up to the `BookingDuration`.
	additionalImmunity := provreqstate.BookingDuration - scaleDownUnneededTime
	if additionalImmunity <= time.Duration(0) {
		klog.Infof("ProvisioningRequestScaleDownNodeProcessor is not enabled since scaleDownUnneededTime is already %v.", scaleDownUnneededTime)
		return nil, false
	}

	klog.Infof("ProvisioningRequestScaleDownNodeProcessor is needed to grant additionalImmunity of %v, since scaleDownUnneededTime is only %v and Provisioning Request provisioned nodes need 10 min of scale down immunity guaranteed.", additionalImmunity, scaleDownUnneededTime)
	return &ProvisioningRequestScaleDownNodeProcessor{
		cloudProvider:      cloudProvider,
		additionalImmunity: additionalImmunity,
		now:                time.Now,
	}, true
}

// GetScaleDownCandidates returns nodes that potentially could be scaled down - it filters out the nodes with `QueuedProvisioning` flag to ensure they won't be deleted for `BookingDuration` period.
func (p *ProvisioningRequestScaleDownNodeProcessor) GetScaleDownCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	var filteredNodes []*apiv1.Node

	for _, node := range nodes {
		mig, err := p.cloudProvider.GkeMigForNode(node)
		if err != nil {
			klog.Errorf("Failed to retrieve MIG for node %s: %v", node.Name, err)
			filteredNodes = append(filteredNodes, node)
			continue
		}
		if mig == nil {
			klog.Warningf("Failed to find corresponding MIG for Node %s", node.Name)
			filteredNodes = append(filteredNodes, node)
			continue
		}

		if !mig.QueuedProvisioning() {
			filteredNodes = append(filteredNodes, node)
			continue
		}
		provisioningMode := reconciler.ResizeRequestProvisioningMode
		if mig.UsesBulkProvisioning() {
			provisioningMode = reconciler.BulkMigProvisioningMode
		}

		migSpec := &reconciler.QueuedProvisioningMigSpec{
			GceRef:           mig.GceRef(),
			ProvisioningMode: provisioningMode,
			Immunity:         p.additionalImmunity,
		}
		if !p.cloudProvider.QueuedProvisioningNodeHasScaleDownImmunity(node, migSpec, p.now()) {
			filteredNodes = append(filteredNodes, node)
			continue
		}
	}
	return filteredNodes, nil
}

// GetPodDestinationCandidates is a no-op, this processor needs to define it to implement ScaleDownNodeProcessor.
func (p *ProvisioningRequestScaleDownNodeProcessor) GetPodDestinationCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return nodes, nil
}

// CleanUp cleans up the processor's internal structures.
func (p *ProvisioningRequestScaleDownNodeProcessor) CleanUp() {
}
