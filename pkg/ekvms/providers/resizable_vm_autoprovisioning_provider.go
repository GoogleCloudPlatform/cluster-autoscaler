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
	machineConfigProvider       *machinetypes.MachineConfigProvider
	clientSet                   clientset.Interface
	ekNodesCountProvider        nodesCountProvider
	ekAutoprovisioningProvider  autoprovisioningProvider
	e4AutoprovisioningProvider  autoprovisioningProvider
	e4aAutoprovisioningProvider autoprovisioningProvider
	balloonPodChecker           *balloonPodChecker
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

	provider := &ResizableVmAutoprovisioningProvider{
		machineConfigProvider:       mcp,
		clientSet:                   clientSet,
		ekAutoprovisioningProvider:  ekAutoprovisioningProvider,
		e4AutoprovisioningProvider:  e4AutoprovisioningProvider,
		e4aAutoprovisioningProvider: e4aAutoprovisioningProvider,
		balloonPodChecker:           &bpChecker,
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
	p.ekAutoprovisioningProvider.refresh()
	p.e4AutoprovisioningProvider.refresh()
	p.e4aAutoprovisioningProvider.refresh()
}

func (p *ResizableVmAutoprovisioningProvider) IsExtendedFallbacksEnabled() bool {
	if e4Provider, ok := p.e4AutoprovisioningProvider.(*e4AutoprovisioningProvider); ok {
		return e4Provider.extendedFallbacksEnabled()
	}
	return false
}

func (p *ResizableVmAutoprovisioningProvider) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	switch machineFamily {
	case machinetypes.EK.Name():
		return p.ekAutoprovisioningProvider.isEnabledInAutopilot()
	case machinetypes.E4.Name():
		return p.e4AutoprovisioningProvider.isEnabledInAutopilot()
	case machinetypes.E4A.Name():
		return p.e4aAutoprovisioningProvider.isEnabledInAutopilot()
	default:
		return false
	}
}

func (p *ResizableVmAutoprovisioningProvider) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	switch machineFamily {
	case machinetypes.EK.Name():
		return p.ekAutoprovisioningProvider.managedNodesEnabled()
	case machinetypes.E4.Name():
		return p.e4AutoprovisioningProvider.managedNodesEnabled()
	case machinetypes.E4A.Name():
		return p.e4aAutoprovisioningProvider.managedNodesEnabled()
	default:
		return false
	}
}

func (p *ResizableVmAutoprovisioningProvider) ResizingEnabled(machineFamily string) bool {
	switch machineFamily {
	case machinetypes.EK.Name():
		return p.ekAutoprovisioningProvider.resizingEnabled()
	case machinetypes.E4.Name():
		return p.e4AutoprovisioningProvider.resizingEnabled()
	case machinetypes.E4A.Name():
		return p.e4aAutoprovisioningProvider.resizingEnabled()
	default:
		return false
	}
}

func (p *ResizableVmAutoprovisioningProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return p.machineConfigProvider
}

func (p *ResizableVmAutoprovisioningProvider) RegisterNodesCountProvider(countProvider nodesCountProvider) {
	p.ekAutoprovisioningProvider.registerNodesCountProvider(countProvider)
	p.e4AutoprovisioningProvider.registerNodesCountProvider(countProvider)
	p.e4aAutoprovisioningProvider.registerNodesCountProvider(countProvider)
}

func (p *ResizableVmAutoprovisioningProvider) NodesCount(machineFamily string) int {
	switch machineFamily {
	case machinetypes.EK.Name():
		return p.ekAutoprovisioningProvider.nodesCount()
	case machinetypes.E4.Name():
		return p.e4AutoprovisioningProvider.nodesCount()
	case machinetypes.E4A.Name():
		return p.e4aAutoprovisioningProvider.nodesCount()
	default:
		return 0
	}
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
