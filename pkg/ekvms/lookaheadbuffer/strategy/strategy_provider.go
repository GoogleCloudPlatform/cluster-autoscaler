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

package strategy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	klog "k8s.io/klog/v2"
)

type metrics interface {
	UpdateLookaheadLaunchStatus(launchPhase, launchedFrom, strategy string)
}

type autoprovisioningProvider interface {
	ResizingEnabled(machineFamily string) bool
}

// Provider manages and provides lookahead buffer strategies across supported machine families.
type Provider struct {
	sources         map[string]strategySource
	laMetrics       metrics
	resizingEnabled map[string]bool
}

// NewProvider creates a new lookahead provider instance.
func NewProvider(
	em experiments.Manager,
	manifestFlagStrategies map[string]string,
	experimentFlags map[string]string,
	laMetrics metrics,
	componentVersion version.Version,
) (*Provider, error) {
	// We need to trim single quotes from lookaheadPodStrategy since it's a single-line JSON string
	// and we wrap it in single quotes for CA manifest to treat it as a string.
	sources := make(map[string]strategySource, len(manifestFlagStrategies))
	for family, configFlag := range manifestFlagStrategies {
		config, err := ParsePodStrategy(strings.Trim(configFlag, "'"))
		if err != nil {
			return nil, fmt.Errorf("cannot parse %s lookahead pod strategy, error: %v", family, err)
		}
		if config.Status != Unspecified {
			sources[family] = &flagStrategySource{flagStrategy: config}
		} else {
			sources[family] = newExperimentStrategySource(em, componentVersion, experimentFlags[family], family)
		}
	}

	return &Provider{
		sources:         sources,
		laMetrics:       laMetrics,
		resizingEnabled: make(map[string]bool, len(sources)),
	}, nil
}

// SetResizingEnabled sets whether resizing is enabled for each machine family
// using autoprovisioningProvider.
func (p *Provider) SetResizingEnabled(autoprovisioningProvider autoprovisioningProvider) {
	if p == nil || autoprovisioningProvider == nil {
		return
	}
	for machineFamily := range p.sources {
		p.resizingEnabled[machineFamily] = autoprovisioningProvider.ResizingEnabled(machineFamily)
	}
}

// Refresh refreshes the value of config from the experiment for each strategy source.
func (p *Provider) Refresh() {
	if p == nil {
		klog.Warning("refresh called on nil Provider. The value should not be nil.")
		return
	}
	for _, source := range p.sources {
		if source != nil {
			source.refresh()
		}
	}
}

// Strategy returns LookaheadPodStrategy for the corresponding machineFamily.
func (p *Provider) Strategy(machineFamily string) (LookaheadPodStrategy, error) {
	if p == nil {
		return unspecifiedStrategy, errors.New("strategy called on nil Provider. The value should not be nil")
	}
	source, ok := p.sources[machineFamily]
	if !ok {
		return unspecifiedStrategy, fmt.Errorf("no LookaheadPodStrategy for machineFamily %s", machineFamily)
	}
	if !p.resizingEnabled[machineFamily] {
		klog.Infof("%s resizing is not enabled, skipping lookahead buffer", machineFamily)
		return unspecifiedStrategy, nil
	}
	if source == nil {
		return unspecifiedStrategy, fmt.Errorf("strategy called with nil source for machineFamily %s", machineFamily)
	}
	strategy, sourceKind := source.strategy()
	p.updateLaunchStatus(machineFamily, strategy, sourceKind)
	return strategy, nil
}

func (p *Provider) updateLaunchStatus(machineFamily string, strategy LookaheadPodStrategy, launchedFrom launchSource) {
	if p.laMetrics == nil {
		return
	}

	// Currently lookahead_launch_status only tracks EK LA launch status.
	// TODO(b/567108065): Introduce new metric with machine_family label for all resizable VMs
	if machineFamily != machinetypes.EK.Name() {
		return
	}

	launchPhase := string(strategy.Status)
	launchStrategy := ""
	if strategy.Status == Enabled {
		// Stripping unneeded fields for strategy field in launch status metric.
		strategy.MinCaVersion = ""
		strategy.Status = ""

		b, err := json.Marshal(strategy)
		if err != nil {
			klog.Errorf("Failed to marshal %s LookaheadPodStrategy, will skip updating launch status metric. Error: %v\nStrategy: %+v", machineFamily, err, strategy)
			return
		}
		launchStrategy = string(b)
	}
	p.laMetrics.UpdateLookaheadLaunchStatus(launchPhase, string(launchedFrom), launchStrategy)
}
