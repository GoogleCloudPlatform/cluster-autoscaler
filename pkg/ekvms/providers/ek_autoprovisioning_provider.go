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

package providers

import (
	"fmt"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	resizable_vm_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

type ekAutoprovisioningProvider struct {
	mode                        resizable_vm_types.EkAutoprovisioningMode
	status                      LaunchStatus
	enabledOnManagedNodes       bool
	enabledOnManagedNodesCAFlag bool
	experimentsManager          experiments.Manager
	bpChecker                   *balloonPodChecker
	autopilotEnabled            bool
	metrics                     resizableVmMetrics
	countProvider               nodesCountProvider
}

// parseEkAutoprovisioningMode parses EkAutoprovisioning Mode
func parseEkAutoprovisioningMode(ekAutoprovisioning string) (resizable_vm_types.EkAutoprovisioningMode, error) {
	ekAutoprovisioningMode := resizable_vm_types.EkAutoprovisioningMode(ekAutoprovisioning)

	switch ekAutoprovisioningMode {
	case resizable_vm_types.EkAutoprovisioningUnspecified,
		resizable_vm_types.EkAutoprovisioningDisabled,
		resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize,
		resizable_vm_types.EkAutoprovisioningDisabledCgroupv1Detected:
		// valid values, do nothing
	default:
		return "", fmt.Errorf("unrecognized flag for EkAutoprovisioning: %q", ekAutoprovisioning)
	}
	return ekAutoprovisioningMode, nil
}

func newEkAutoprovisioningProvider(ekAutoprovisioning string, experimentsManager experiments.Manager, bpChecker *balloonPodChecker, autopilotEnabled bool, enabledOnManagedNodesCAFlag bool, metrics resizableVmMetrics) (*ekAutoprovisioningProvider, error) {
	ekAutoprovisioningMode, err := parseEkAutoprovisioningMode(ekAutoprovisioning)
	if err != nil {
		return nil, fmt.Errorf("error creating ekAutoprovisioningProvider: %v", err)
	}

	return &ekAutoprovisioningProvider{
		mode:               ekAutoprovisioningMode,
		experimentsManager: experimentsManager,
		status: LaunchStatus{
			phase:  launchNotEnabled,
			source: launchUndefined,
		},
		bpChecker:                   bpChecker,
		enabledOnManagedNodesCAFlag: enabledOnManagedNodesCAFlag,
		autopilotEnabled:            autopilotEnabled,
		metrics:                     metrics,
	}, nil
}

func (p *ekAutoprovisioningProvider) refreshManagedNodesStatus() {
	if p.enabledOnManagedNodesCAFlag {
		p.enabledOnManagedNodes = true
		return
	}

	if p.experimentsManager != nil {
		p.enabledOnManagedNodes = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.EKOnManagedNodesMinCAVersionFlag, false)
		return
	}
	// Default to false if CA flag is not enabled and experimentsManager is nil
	p.enabledOnManagedNodes = false
}

func (p *ekAutoprovisioningProvider) refreshLaunchStatus() {
	if !p.bpChecker.isBalloonPodCreatable {
		p.status = LaunchStatus{phase: launchDisabledBalloonPodError, source: launchUndefined}
		return
	}

	if p.mode == resizable_vm_types.EkAutoprovisioningDisabled {
		p.status = LaunchStatus{phase: launchDisabled, source: launchUndefined}
	} else if p.mode == resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize {
		p.status = LaunchStatus{phase: launchCoarseGrainedResize, source: launchClusterProto}
	} else if p.mode == resizable_vm_types.EkAutoprovisioningDisabledCgroupv1Detected {
		p.status = LaunchStatus{phase: launchDisabledCgroupv1, source: launchUndefined}
	} else if p.mode == resizable_vm_types.EkAutoprovisioningUnspecified {
		p.status = LaunchStatus{phase: launchCoarseGrainedResize, source: launchUndefined}
	} else {
		klog.Errorf("Unrecognized EkAutoprovisioningMode, defaulting to disabled: %q", p.mode)
		p.status = LaunchStatus{phase: launchNotEnabled, source: launchUndefined}
	}
}

func (p *ekAutoprovisioningProvider) refresh() {
	p.refreshLaunchStatus()
	p.refreshManagedNodesStatus()

	p.metrics.UpdateResizableVmLaunchStatus(machinetypes.EK.Name(), string(p.status.phase), string(p.status.source))
	p.metrics.UpdateResizableVmAutopilotComputeClassStatus(machinetypes.EK.Name(), p.enabledOnManagedNodes)
}

func (p *ekAutoprovisioningProvider) managedNodesEnabled() bool {
	return p.status.phase == launchCoarseGrainedResize && p.enabledOnManagedNodes
}

func (p *ekAutoprovisioningProvider) isEnabledInAutopilot() bool {
	return p.autopilotEnabled && p.status.phase == launchCoarseGrainedResize
}

func (p *ekAutoprovisioningProvider) resizingEnabled() bool {
	return (p.autopilotEnabled || p.enabledOnManagedNodes) && p.status.phase == launchCoarseGrainedResize
}

func (p *ekAutoprovisioningProvider) registerNodesCountProvider(countProvider nodesCountProvider) {
	p.countProvider = countProvider
}

func (p *ekAutoprovisioningProvider) nodesCount() int {
	return getNodesCount(p.countProvider, machinetypes.EK.Name())
}
