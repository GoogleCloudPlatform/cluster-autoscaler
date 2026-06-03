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

package extendeddurationpods

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	"k8s.io/klog/v2"
)

const (
	// BlockedMigEDPUpgrade - the MIG has scaling blocked because of an ongoing EDP Upgrade.
	BlockedMigEDPUpgrade scaleblocking.BlockedMigReason = "blocked.mig.edp.upgrade"
)

// BlockedMigsSource provides the ids of MIGs which should have scaling blocked because of an ongoing EDP upgrade.
// More details: go/extended-duration-pod-design.
type BlockedMigsSource struct {
	cloudProvider CloudProvider
}

// NewBlockedMigsSource returns a new instance of extendeddurationpods.BlockedMigsSource.
func NewBlockedMigsSource(cloudProvider CloudProvider) BlockedMigsSource {
	return BlockedMigsSource{cloudProvider: cloudProvider}
}

// BlockedMigs returns the ids of MIGs which should have scaling blocked because of an ongoing EDP Upgrade.
func (b BlockedMigsSource) BlockedMigs() scaleblocking.BlockedMigs {
	result := scaleblocking.BlockedMigs{
		NoScaleUpMigs:   map[string]scaleblocking.BlockedMigReasonSet{},
		NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{},
	}
	for _, mig := range b.cloudProvider.GetGkeMigs() {
		shouldBlockScaleUp := shouldBlockScalingUp(mig, b.cloudProvider.GetClusterVersion())
		if shouldBlockScaleUp {
			result.NoScaleUpMigs[mig.Id()] = scaleblocking.BlockedMigReasonSet{BlockedMigEDPUpgrade: true}
		}
	}
	return result
}

// shouldBlockScalingUp returns whether a given MIG should have scaling blocked because of an ongoing EDP upgrade
// we should block scale up of mig which are yet to upgrade.
func shouldBlockScalingUp(mig *gke.GkeMig, clusterVersion string) (shouldBlockScaleUp bool) {
	if mig == nil || mig.GetNodeConfig() == nil || mig.Spec() == nil || !mig.Exist() {
		return false
	}
	if mig.Spec().ExtendedDurationPods == "" {
		return false
	}
	migVersion, err := version.FromString(mig.GetNodeConfig().Version)
	if err != nil {
		klog.Warningf("Unable to parse mig version: %s, %q", mig.GetNodeConfig().Version, err)
		return false
	}
	cVersion, err := version.FromString(clusterVersion)
	if err != nil {
		klog.Warningf("Unable to parse cluster version: %s, %q", clusterVersion, err)
		return false
	}
	if migVersion.LessThan(cVersion) {
		return true
	}
	return false
}

// CleanUp is a no-op.
func (b BlockedMigsSource) CleanUp() {
}
