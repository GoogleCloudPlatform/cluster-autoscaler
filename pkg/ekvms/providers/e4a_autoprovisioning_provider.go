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

type e4aAutoprovisioningProvider struct {
	mode                        resizable_vm_types.E4aAutoprovisioningMode
	experimentsManager          experiments.Manager
	status                      LaunchStatus
	bpChecker                   *balloonPodChecker
	autopilotEnabled            bool
	enabledOnManagedNodes       bool
	enabledOnManagedNodesCAFlag bool
	metrics                     resizableVmMetrics
	countProvider               nodesCountProvider
}

// parseE4aAutoprovisioningMode parses E4aAutoprovisioning Mode
func parseE4aAutoprovisioningMode(e4aAutoprovisioning string) (resizable_vm_types.E4aAutoprovisioningMode, error) {
	if e4aAutoprovisioning == "" {
		return resizable_vm_types.E4aAutoprovisioningUnspecified, nil
	}
	e4aAutoprovisioningMode := resizable_vm_types.E4aAutoprovisioningMode(e4aAutoprovisioning)

	switch e4aAutoprovisioningMode {
	case resizable_vm_types.E4aAutoprovisioningUnspecified,
		resizable_vm_types.E4aAutoprovisioningDisabled,
		resizable_vm_types.E4aAutoprovisioningEnabledNoResize,
		resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize:
		// valid values, do nothing
	default:
		return "", fmt.Errorf("unrecognized flag for E4aAutoprovisioning: %q", e4aAutoprovisioningMode)
	}
	return e4aAutoprovisioningMode, nil
}

func newE4aAutoprovisioningProvider(e4aAutoprovisioning string, experimentsManager experiments.Manager, bpChecker *balloonPodChecker, autopilotEnabled bool, enabledOnManagedNodesCAFlag bool, metrics resizableVmMetrics) (*e4aAutoprovisioningProvider, error) {
	e4aAutoprovisioningMode, err := parseE4aAutoprovisioningMode(e4aAutoprovisioning)
	if err != nil {
		return nil, fmt.Errorf("error creating e4aAutoprovisioningProvider: %v", err)
	}

	return &e4aAutoprovisioningProvider{
		mode:               e4aAutoprovisioningMode,
		experimentsManager: experimentsManager,
		status: LaunchStatus{
			phase:  launchNotEnabled,
			source: launchUndefined,
		},
		bpChecker:                   bpChecker,
		autopilotEnabled:            autopilotEnabled,
		enabledOnManagedNodesCAFlag: enabledOnManagedNodesCAFlag,
		metrics:                     metrics,
	}, nil
}

func (p *e4aAutoprovisioningProvider) refresh() {
	p.refreshLaunchStatus()
	p.refreshManagedNodesStatus()

	p.metrics.UpdateResizableVmLaunchStatus(machinetypes.E4A.Name(), string(p.status.phase), string(p.status.source))
	p.metrics.UpdateResizableVmAutopilotComputeClassStatus(machinetypes.E4A.Name(), p.enabledOnManagedNodes)
}

func (p *e4aAutoprovisioningProvider) refreshLaunchStatus() {
	if !p.bpChecker.isBalloonPodCreatable {
		p.status = LaunchStatus{phase: launchDisabledBalloonPodError, source: launchUndefined}
		return
	}
	switch p.mode {
	case resizable_vm_types.E4aAutoprovisioningDisabled:
		p.status = LaunchStatus{phase: launchDisabled, source: launchUndefined}
		return
	case resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize:
		p.status = LaunchStatus{phase: launchCoarseGrainedResize, source: launchClusterProto}
		return
	case resizable_vm_types.E4aAutoprovisioningEnabledNoResize:
		p.status = LaunchStatus{phase: launchEnabledNoResize, source: launchClusterProto}
		return
	}

	if isE4aEnabledWithExperiment := p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4aWithResizeMinVersionFlag, false) && p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.AutopilotE4aWithResizeEnabledFlag, true); isE4aEnabledWithExperiment {
		p.status = LaunchStatus{phase: launchCoarseGrainedResize, source: launchExperiment}
		return
	}
	if isE4aNoResizeEnabledWithExperiment := p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4aNoResizeMinVersionFlag, false) && p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.AutopilotE4aNoResizeEnabledFlag, true); isE4aNoResizeEnabledWithExperiment {
		p.status = LaunchStatus{phase: launchEnabledNoResize, source: launchExperiment}
		return
	}

	// E4aAutoprovisioningUnspecified is a valid state where we just want to disable E4A without logging an error.
	if p.mode != resizable_vm_types.E4aAutoprovisioningUnspecified {
		klog.Errorf("Unrecognized E4aAutoprovisioningMode, defaulting to disabled: %q", p.mode)
	}
	p.status = LaunchStatus{phase: launchNotEnabled, source: launchUndefined}
}

func (p *e4aAutoprovisioningProvider) refreshManagedNodesStatus() {
	if p.enabledOnManagedNodesCAFlag {
		p.enabledOnManagedNodes = true
		return
	}

	if p.experimentsManager != nil {
		p.enabledOnManagedNodes = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.E4AOnManagedNodesMinCAVersionFlag, false) && p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.E4AOnManagedNodesEnabledFlag, true)
		return
	}
	// Default to false if CA flag is not enabled and experimentsManager is nil
	p.enabledOnManagedNodes = false
}

func (p *e4aAutoprovisioningProvider) managedNodesEnabled() bool {
	return p.enabledOnManagedNodes && (p.status.phase == launchCoarseGrainedResize || p.status.phase == launchEnabledNoResize)
}

func (p *e4aAutoprovisioningProvider) isEnabledInAutopilot() bool {
	return p.autopilotEnabled && (p.status.phase == launchCoarseGrainedResize || p.status.phase == launchEnabledNoResize)
}

func (p *e4aAutoprovisioningProvider) resizingEnabled() bool {
	return (p.autopilotEnabled || p.enabledOnManagedNodes) && p.status.phase == launchCoarseGrainedResize
}

func (p *e4aAutoprovisioningProvider) registerNodesCountProvider(countProvider nodesCountProvider) {
	p.countProvider = countProvider
}

func (p *e4aAutoprovisioningProvider) nodesCount() int {
	return getNodesCount(p.countProvider, machinetypes.E4A.Name())
}
