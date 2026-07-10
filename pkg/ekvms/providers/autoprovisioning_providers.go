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
	"reflect"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	resizable_vm_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

type launchPhase string

const (
	launchCoarseGrainedResize     launchPhase = "COARSE_GRAINED_RESIZE"
	launchEnabledNoResize         launchPhase = "NO_RESIZE"
	launchNotEnabled              launchPhase = "NOT_ENABLED"
	launchDisabled                launchPhase = "DISABLED"
	launchDisabledCgroupv1        launchPhase = "DISABLED_CGROUPV1"
	launchDisabledBalloonPodError launchPhase = "DISABLED_BALLOON_POD_ERROR"
)

type launchSource string

const (
	launchExperiment   launchSource = "GIRAFFE"
	launchClusterProto launchSource = "CLUSTER_PROTO"
	launchUndefined    launchSource = ""
)

type LaunchStatus struct {
	phase  launchPhase
	source launchSource
}

type autoprovisioningProvider interface {
	refresh()
	isEnabledInAutopilot() bool
	managedNodesEnabled() bool
	resizingEnabled() bool
	registerNodesCountProvider(nodesCountProvider)
	nodesCount() int
}

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

func getNodesCount(countProvider nodesCountProvider, machineFamily string) int {
	if countProvider == nil || reflect.ValueOf(countProvider).IsNil() {
		return 0
	}
	return countProvider.NodesCount(machineFamily)
}

type e4AutoprovisioningProvider struct {
	experimentsManager          experiments.Manager
	status                      LaunchStatus
	bpChecker                   *balloonPodChecker
	autopilotEnabled            bool
	enabledOnManagedNodes       bool
	enabledOnManagedNodesCAFlag bool
	metrics                     resizableVmMetrics
	countProvider               nodesCountProvider
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

	p.metrics.UpdateResizableVmLaunchStatus(machinetypes.E4.Name(), string(p.status.phase), string(p.status.source))
	p.metrics.UpdateResizableVmAutopilotComputeClassStatus(machinetypes.E4.Name(), p.enabledOnManagedNodes)
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
		p.enabledOnManagedNodes = p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.E4OnManagedNodesMinCAVersionFlag, false) && p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.E4OnManagedNodesEnabledFlag, true)
		return
	}
	p.enabledOnManagedNodes = false
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
