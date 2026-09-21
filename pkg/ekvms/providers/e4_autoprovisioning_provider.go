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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type e4AutoprovisioningProvider struct {
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

func newE4AutoprovisioningProvider(experimentsManager experiments.Manager, bpChecker *balloonPodChecker, autopilotEnabled bool, enabledOnManagedNodesCAFlag bool, metrics resizableVmMetrics) *e4AutoprovisioningProvider {
	return &e4AutoprovisioningProvider{
		experimentsManager: experimentsManager,
		status: LaunchStatus{
			phase:  launchNotEnabled,
			source: launchUndefined,
		},
		bpChecker:                   bpChecker,
		autopilotEnabled:            autopilotEnabled,
		enabledOnManagedNodesCAFlag: enabledOnManagedNodesCAFlag,
		metrics:                     metrics,
	}
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
	if p.experimentsManager == nil {
		p.status = LaunchStatus{phase: launchNotEnabled, source: launchUndefined}
		return
	}

	// Evaluate noResize first for testing
	noResize := p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.AutopilotE4NoResizeEnabledFlag, true)

	isE4EnabledWithExperiment := p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotE4MinVersionFlag, false)
	if isE4EnabledWithExperiment {
		if noResize {
			p.status = LaunchStatus{phase: launchEnabledNoResize, source: launchExperiment}
			return
		}

		// If resize is enabled (GA), E4 requires the balloon pod checker to be healthy
		if !p.bpChecker.isBalloonPodCreatable {
			p.status = LaunchStatus{phase: launchDisabledBalloonPodError, source: launchUndefined}
			return
		}

		p.status = LaunchStatus{phase: launchCoarseGrainedResize, source: launchExperiment}
		return
	}

	p.status = LaunchStatus{phase: launchNotEnabled, source: launchUndefined}
}

func (p *e4AutoprovisioningProvider) refreshManagedNodesStatus() {
	if p.enabledOnManagedNodesCAFlag {
		p.enabledOnManagedNodes = true
		return
	}
	if p.experimentsManager != nil {
		p.enabledOnManagedNodes = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.E4OnManagedNodesMinCAVersionFlag, false)
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
