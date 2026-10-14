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

type e4AutoprovisioningProvider struct {
	mode                        resizable_vm_types.E4AutoprovisioningMode
	experimentsManager          experiments.Manager
	status                      LaunchStatus
	bpChecker                   *balloonPodChecker
	autopilotEnabled            bool
	enabledOnManagedNodes       bool
	enabledOnManagedNodesCAFlag bool
	metrics                     resizableVmMetrics
	countProvider               nodesCountProvider
	isExtendedFallbacksEnabled  bool
	isStatefulEnabled           bool
	isPrioritizationEnabled     bool
}

// parseE4AutoprovisioningMode parses E4Autoprovisioning Mode
func parseE4AutoprovisioningMode(e4Autoprovisioning string) (resizable_vm_types.E4AutoprovisioningMode, error) {
	if e4Autoprovisioning == "" {
		return resizable_vm_types.E4AutoprovisioningUnspecified, nil
	}
	e4AutoprovisioningMode := resizable_vm_types.E4AutoprovisioningMode(e4Autoprovisioning)

	switch e4AutoprovisioningMode {
	case resizable_vm_types.E4AutoprovisioningUnspecified,
		resizable_vm_types.E4AutoprovisioningDisabled,
		resizable_vm_types.E4AutoprovisioningEnabledNoResize,
		resizable_vm_types.E4AutoprovisioningEnabledCoarseGrainedResize:
		// valid values, do nothing
	default:
		return "", fmt.Errorf("unrecognized flag for E4Autoprovisioning: %q", e4AutoprovisioningMode)
	}
	return e4AutoprovisioningMode, nil
}

func newE4AutoprovisioningProvider(e4Autoprovisioning string, experimentsManager experiments.Manager, bpChecker *balloonPodChecker, autopilotEnabled bool, enabledOnManagedNodesCAFlag bool, metrics resizableVmMetrics) (*e4AutoprovisioningProvider, error) {
	e4AutoprovisioningMode, err := parseE4AutoprovisioningMode(e4Autoprovisioning)
	if err != nil {
		return nil, fmt.Errorf("error creating e4AutoprovisioningProvider: %v", err)
	}

	return &e4AutoprovisioningProvider{
		mode:               e4AutoprovisioningMode,
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

func (p *e4AutoprovisioningProvider) refresh() {
	p.refreshLaunchStatus()
	p.refreshManagedNodesStatus()
	p.refreshExtendedFallbacksStatus()
	p.refreshStatefulStatus()
	p.refreshPrioritizationStatus()

	p.metrics.UpdateResizableVmLaunchStatus(machinetypes.E4.Name(), string(p.status.phase), string(p.status.source))
	p.metrics.UpdateResizableVmAutopilotComputeClassStatus(machinetypes.E4.Name(), p.enabledOnManagedNodes)
}

func (p *e4AutoprovisioningProvider) refreshStatefulStatus() {
	if p.experimentsManager != nil {
		p.isStatefulEnabled = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4StatefulMinCAVersionFlag, false)
	} else {
		p.isStatefulEnabled = false
	}
}

func (p *e4AutoprovisioningProvider) refreshPrioritizationStatus() {
	if p.experimentsManager != nil {
		p.isPrioritizationEnabled = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4PrioritizationMinCAVersionFlag, false)
	} else {
		p.isPrioritizationEnabled = false
	}
}

func (p *e4AutoprovisioningProvider) refreshLaunchStatus() {
	switch p.mode {
	case resizable_vm_types.E4AutoprovisioningDisabled:
		p.status = LaunchStatus{phase: launchDisabled, source: launchUndefined}
		return
	case resizable_vm_types.E4AutoprovisioningEnabledCoarseGrainedResize:
		if !p.bpChecker.isBalloonPodCreatable {
			p.status = LaunchStatus{phase: launchDisabledBalloonPodError, source: launchUndefined}
			return
		}
		p.status = LaunchStatus{phase: launchCoarseGrainedResize, source: launchClusterProto}
		return
	case resizable_vm_types.E4AutoprovisioningEnabledNoResize:
		p.status = LaunchStatus{phase: launchEnabledNoResize, source: launchClusterProto}
		return
	}

	if p.experimentsManager == nil {
		p.status = LaunchStatus{phase: launchNotEnabled, source: launchUndefined}
		return
	}

	if isE4EnabledWithExperiment := p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4WithResizeMinVersionFlag, false) && p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.AutopilotE4WithResizeEnabledFlag, true); isE4EnabledWithExperiment {
		if !p.bpChecker.isBalloonPodCreatable {
			p.status = LaunchStatus{phase: launchDisabledBalloonPodError, source: launchUndefined}
			return
		}
		p.status = LaunchStatus{phase: launchCoarseGrainedResize, source: launchExperiment}
		return
	}
	if isE4NoResizeEnabledWithExperiment := p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4MinVersionFlag, false) && p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.AutopilotE4NoResizeEnabledFlag, true); isE4NoResizeEnabledWithExperiment {
		p.status = LaunchStatus{phase: launchEnabledNoResize, source: launchExperiment}
		return
	}

	// E4AutoprovisioningUnspecified is a valid state where we just want to disable E4 without logging an error.
	if p.mode != resizable_vm_types.E4AutoprovisioningUnspecified {
		klog.Errorf("Unrecognized E4AutoprovisioningMode, defaulting to disabled: %q", p.mode)
	}
	p.status = LaunchStatus{phase: launchNotEnabled, source: launchUndefined}
}

func (p *e4AutoprovisioningProvider) refreshManagedNodesStatus() {
	if p.enabledOnManagedNodesCAFlag {
		p.enabledOnManagedNodes = true
		return
	}
	if p.experimentsManager != nil {
		p.enabledOnManagedNodes = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.E4OnManagedNodesMinCAVersionFlag, false) && p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.E4OnManagedNodesEnabledFlag, true)
		return
	}
	p.enabledOnManagedNodes = false
}

func (p *e4AutoprovisioningProvider) refreshExtendedFallbacksStatus() {
	if p.experimentsManager != nil {
		p.isExtendedFallbacksEnabled = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4ExtendedFallbacksMinCAVersionFlag, false)
	} else {
		p.isExtendedFallbacksEnabled = false
	}
}

func (p *e4AutoprovisioningProvider) extendedFallbacksEnabled() bool {
	return p.isExtendedFallbacksEnabled
}

func (p *e4AutoprovisioningProvider) managedNodesEnabled() bool {
	return p.enabledOnManagedNodes && (p.status.phase == launchCoarseGrainedResize || p.status.phase == launchEnabledNoResize)
}

func (p *e4AutoprovisioningProvider) isEnabledInAutopilot() bool {
	return p.autopilotEnabled && (p.status.phase == launchCoarseGrainedResize || p.status.phase == launchEnabledNoResize)
}

func (p *e4AutoprovisioningProvider) resizingEnabled() bool {
	return (p.autopilotEnabled || p.enabledOnManagedNodes) && p.status.phase == launchCoarseGrainedResize
}

func (p *e4AutoprovisioningProvider) registerNodesCountProvider(countProvider nodesCountProvider) {
	p.countProvider = countProvider
}

func (p *e4AutoprovisioningProvider) nodesCount() int {
	return getNodesCount(p.countProvider, machinetypes.E4.Name())
}
