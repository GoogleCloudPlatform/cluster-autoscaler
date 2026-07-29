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
	"context"
	"fmt"
	"time"

	clientset "k8s.io/client-go/kubernetes"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type nodesCountProvider interface {
	NodesCount(machineFamily string) int
}

type resizableVmMetrics interface {
	UpdateResizableVmLaunchStatus(machineFamily, phase, source string)
	UpdateResizableVmAutopilotComputeClassStatus(machineFamily string, enabled bool)
}

type ResizableVmAutoprovisioningProvider struct {
	machineConfigProvider     *machinetypes.MachineConfigProvider
	clientSet                 clientset.Interface
	autoprovisioningProviders map[string]autoprovisioningProvider
	balloonPodChecker         *balloonPodChecker
}

// NewResizableVmAutoprovisioningProvider creates a new ResizableVmAutoprovisioning provider instance.
func NewResizableVmAutoprovisioningProvider(clientSet clientset.Interface, mcp *machinetypes.MachineConfigProvider, em experiments.Manager, autopilotEnabled, ekOnManagedNodesEnabledCAFlag, e4aOnManagedNodesEnabledCAFlag bool, ekAutoprovisioning, e4aAutoprovisioning string, metrics resizableVmMetrics) (*ResizableVmAutoprovisioningProvider, error) {
	bpChecker := balloonPodChecker{
		clientSet:                    clientSet,
		isBalloonPodCreatable:        true,
		balloonPodCreationErrorCount: 0,
		balloonPodSizeIndex:          0,
		runInterval:                  15 * time.Second,
	}

	ekAutoprovisioningProvider, err := newEkAutoprovisioningProvider(ekAutoprovisioning, em, &bpChecker, autopilotEnabled, ekOnManagedNodesEnabledCAFlag, metrics)
	if err != nil {
		return nil, fmt.Errorf("error creating ResizableVmAutoprovisioningProvider: %v", err)
	}

	e4AutoprovisioningProvider := newE4AutoprovisioningProvider(em, &bpChecker, autopilotEnabled, false, metrics)

	e4aAutoprovisioningProvider, err := newE4aAutoprovisioningProvider(e4aAutoprovisioning, em, &bpChecker, autopilotEnabled, e4aOnManagedNodesEnabledCAFlag, metrics)
	if err != nil {
		return nil, fmt.Errorf("error creating ResizableVmAutoprovisioningProvider: %v", err)
	}

	autoprovisioningProviders := map[string]autoprovisioningProvider{
		machinetypes.EK.Name():  ekAutoprovisioningProvider,
		machinetypes.E4.Name():  e4AutoprovisioningProvider,
		machinetypes.E4A.Name(): e4aAutoprovisioningProvider,
	}

	provider := &ResizableVmAutoprovisioningProvider{
		machineConfigProvider:     mcp,
		clientSet:                 clientSet,
		autoprovisioningProviders: autoprovisioningProviders,
		balloonPodChecker:         &bpChecker,
	}
	provider.Refresh()
	return provider, nil
}

func (p *ResizableVmAutoprovisioningProvider) Run(ctx context.Context) {
	p.balloonPodChecker.Run(ctx)
}

// Refresh refreshes dynamic configuration values for EK launch. It's important that
// Refresh is called one per loop and config is cached for the duration of the loop to guarantee consistency within a loop.
func (p *ResizableVmAutoprovisioningProvider) Refresh() {
	for _, provider := range p.autoprovisioningProviders {
		provider.refresh()
	}
}

func (p *ResizableVmAutoprovisioningProvider) IsExtendedFallbacksEnabled() bool {
	e4Provider, ok := p.autoprovisioningProviders[machinetypes.E4.Name()].(*e4AutoprovisioningProvider)
	if ok {
		return e4Provider.extendedFallbacksEnabled()
	}
	return false
}

func (p *ResizableVmAutoprovisioningProvider) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	provider, found := p.autoprovisioningProviders[machineFamily]
	if !found {
		return false
	}
	return provider.isEnabledInAutopilot()
}

func (p *ResizableVmAutoprovisioningProvider) IsE4StatefulEnabledInAutopilot() bool {
	e4Provider, ok := p.autoprovisioningProviders[machinetypes.E4.Name()].(*e4AutoprovisioningProvider)
	if ok {
		return (e4Provider.isEnabledInAutopilot() || e4Provider.managedNodesEnabled()) && e4Provider.isStatefulEnabled
	}
	return false
}

func (p *ResizableVmAutoprovisioningProvider) IsE4PrioritizationEnabledInAutopilot() bool {
	e4Provider, ok := p.autoprovisioningProviders[machinetypes.E4.Name()].(*e4AutoprovisioningProvider)
	if ok {
		return e4Provider.isPrioritizationEnabled
	}
	return false
}

func (p *ResizableVmAutoprovisioningProvider) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	provider, found := p.autoprovisioningProviders[machineFamily]
	if !found {
		return false
	}
	return provider.managedNodesEnabled()
}

func (p *ResizableVmAutoprovisioningProvider) ResizingEnabled(machineFamily string) bool {
	provider, found := p.autoprovisioningProviders[machineFamily]
	if !found {
		return false
	}
	return provider.resizingEnabled()
}

func (p *ResizableVmAutoprovisioningProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return p.machineConfigProvider
}

func (p *ResizableVmAutoprovisioningProvider) RegisterNodesCountProvider(countProvider nodesCountProvider) {
	for _, provider := range p.autoprovisioningProviders {
		provider.registerNodesCountProvider(countProvider)
	}
}

func (p *ResizableVmAutoprovisioningProvider) NodesCount(machineFamily string) int {
	provider, found := p.autoprovisioningProviders[machineFamily]
	if !found {
		return 0
	}
	return provider.nodesCount()
}

// HasActiveResizableNodes returns true if resizing is enabled and nodes count > 0 for any supported resizable VM family.
func (p *ResizableVmAutoprovisioningProvider) HasActiveResizableNodes() bool {
	for _, family := range p.machineConfigProvider.AllResizableMachineFamilies() {
		name := family.Name()
		if p.ResizingEnabled(name) && p.NodesCount(name) > 0 {
			return true
		}
	}
	return false
}
