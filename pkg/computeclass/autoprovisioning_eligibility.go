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
	"sync/atomic"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

type AutoprovisioningEligibility interface {
	// SetClusterAutoprovisioningEnabled sets the value of cluster NAP flag. Returns true if the flag has
	// changed, false otherwise (in case of a no-op).
	SetClusterAutoprovisioningEnabled(bool) bool
	// IsNodeAutoprovisioningEnabled returns true if autoprovisioning is enabled.
	// This method will be removed once CCC NAP enablement is stable.
	IsNodeAutoprovisioningEnabled() bool
	// AreClusterLimitsEnabled returns true if NAP cluster limits are enabled.
	AreClusterLimitsEnabled() bool
	// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
	UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool
	// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
	UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool
}

func NewAutoprovisioningEligibility(lister lister.Lister, cccNapEnabled bool) AutoprovisioningEligibility {
	return &autoprovisioningEligibility{
		lister:            lister,
		clusterNapEnabled: atomic.Bool{},
		cccNapEnabled:     cccNapEnabled,
	}
}

type autoprovisioningEligibility struct {
	lister            lister.Lister
	clusterNapEnabled atomic.Bool
	cccNapEnabled     bool
}

// SetClusterAutoprovisioningEnabled sets the value of cluster NAP flag. Returns true if the flag has
// changed, false otherwise (in case of a no-op).
func (e *autoprovisioningEligibility) SetClusterAutoprovisioningEnabled(flag bool) bool {
	return e.clusterNapEnabled.CompareAndSwap(!flag, flag)
}

// IsNodeAutoprovisioningEnabled returns true if autoprovisioning is enabled.
func (e *autoprovisioningEligibility) IsNodeAutoprovisioningEnabled() bool {
	return e.clusterNapEnabled.Load() || e.cccNapEnabled
}

// AreClusterLimitsEnabled returns true if NAP cluster limits are enabled.
// This method returns true only if cluster NAP is enabled. This maintains
// backward-compatibility from before NAP-less implementation. When NAP is
// disabled in cluster, limits can be set using CapacityQuotas.
func (e *autoprovisioningEligibility) AreClusterLimitsEnabled() bool {
	return e.clusterNapEnabled.Load()
}

// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
func (e *autoprovisioningEligibility) UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool {
	c, name, err := e.lister.PodReqCrd(req)
	if err != nil {
		klog.Warningf("cannot find crd %q crd specified by pod labels %v, defaulting to cluster level autoprovisioning enabled", name, e.lister.Labels())
	}
	return e.autoprovisioningEnabledForCrd(c)
}

// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
func (e *autoprovisioningEligibility) UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool {
	c, name, err := e.lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		klog.Warningf("cannot find crd %q crd specified by node group labels %v, defaulting to cluster level autoprovisioning enabled", name, e.lister.Labels())
	}
	return e.autoprovisioningEnabledForCrd(c)
}

func (e *autoprovisioningEligibility) autoprovisioningEnabledForCrd(c crd.CRD) bool {
	if e.clusterNapEnabled.Load() {
		return c == nil || c.AutoprovisioningEnabled()
	} else if e.cccNapEnabled {
		// TODO(b/402061639): Remove CRD type check once NPC is deprecated
		return c != nil && c.CrdType() == ccc.CrdType && c.AutoprovisioningEnabled()
	}
	return false
}
