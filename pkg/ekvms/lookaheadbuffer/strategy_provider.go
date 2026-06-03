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

package lookaheadbuffer

import (
	"encoding/json"
	"errors"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	klog "k8s.io/klog/v2"
)

type launchSource string

const (
	experimentSource   launchSource = "EXPERIMENT"
	clusterProtoSource launchSource = "CLUSTER_PROTO"
	undefinedSource    launchSource = ""
)

var unspecifiedStrategy = LookaheadPodStrategy{Status: Unspecified} // default strategy configuration if smth goes wrong

type StrategyProvider interface {
	SetEkResizingEnabled(ekResizingEnabled bool)
	RefreshStrategy()
	Strategy() (LookaheadPodStrategy, error)
}

// strategyProviderImpl parses and provides lookahead config.
type strategyProviderImpl struct {
	experiments.Manager
	// flagStrategy is passed via Cluster Autoscaler flags
	flagStrategy LookaheadPodStrategy
	// experimentStrategy is passed via an experiment
	experimentStrategy LookaheadPodStrategy
	laMetrics          Metrics
	ekResizingEnabled  bool
	componentVersion   version.Version
}

// NewStrategyProvider creates a new lookahead provider instance.
func NewStrategyProvider(em experiments.Manager, flagConfig LookaheadPodStrategy, laMetrics Metrics, componentVersion version.Version) *strategyProviderImpl {
	return &strategyProviderImpl{
		Manager:          em,
		flagStrategy:     flagConfig,
		laMetrics:        laMetrics,
		componentVersion: componentVersion,
	}
}

func (p *strategyProviderImpl) SetEkResizingEnabled(ekResizingEnabled bool) {
	p.ekResizingEnabled = ekResizingEnabled
}

// RefreshStrategy reads the value of config from experiment, parses it, and sets it.
func (p *strategyProviderImpl) RefreshStrategy() {
	if p == nil {
		klog.Warning("RefreshStrategy called on nil podStrategyProviderImpl. The value should not be nil.")
		return
	}
	experimentConfigFlag := p.EvaluateStringFlagOrFailsafe(experiments.EkLookaheadPodsV1Flag, `{"minCaVersion": "999.999.999"}`)
	experimentConfig, err := ParsePodStrategy(experimentConfigFlag)
	if err != nil {
		klog.Errorf("Cannot parse experiment %q flag: %v", experiments.EkLookaheadPodsV1Flag, err)
		p.experimentStrategy = unspecifiedStrategy
		return
	}

	experimentVersion, err := version.FromString(experimentConfig.MinCaVersion)
	if err != nil {
		klog.Errorf("Experiment %q provided invalid min version %q, using unspecified lookahead pod strategy", experiments.EkLookaheadPodsV1Flag, experimentConfig.MinCaVersion)
		p.experimentStrategy = unspecifiedStrategy
		return
	}

	// Fallback to unspecified lookahead pod strategy if component version is less than minCaVersion in experiment.
	if p.componentVersion.LessThan(experimentVersion) {
		p.experimentStrategy = unspecifiedStrategy
		return
	}

	p.experimentStrategy = experimentConfig
}

// Strategy returns the authoritative LookaheadPodStrategy.
func (p *strategyProviderImpl) Strategy() (LookaheadPodStrategy, error) {
	if p == nil {
		return unspecifiedStrategy, errors.New("Strategy called on nil podStrategyProviderImpl. The value should not be nil")
	}
	if !p.ekResizingEnabled {
		klog.Info("EK resizing is not enabled, skipping lookahead buffer")
		return unspecifiedStrategy, nil
	}
	if p.flagStrategy.Status != Unspecified {
		p.updateLaunchStatus(p.flagStrategy, clusterProtoSource)
		return p.flagStrategy, nil
	}
	if p.experimentStrategy.Status != Unspecified {
		p.updateLaunchStatus(p.experimentStrategy, experimentSource)
		return p.experimentStrategy, nil
	}
	p.updateLaunchStatus(unspecifiedStrategy, undefinedSource)
	return unspecifiedStrategy, nil
}

func (p *strategyProviderImpl) updateLaunchStatus(strategy LookaheadPodStrategy, launchedFrom launchSource) {
	launchPhase := string(strategy.Status)
	launchStrategy := ""
	if strategy.Status == Enabled {
		// Stripping unneeded fields for strategy field in launch status metric.
		strategy.MinCaVersion = ""
		strategy.Status = ""

		b, err := json.Marshal(strategy)
		if err != nil {
			klog.Errorf(`Failed to marshal LookaheadPodStrategy, will skip updating launch status metric. Error: %v\nStrategy: %+v`, err, strategy)
			return
		}
		launchStrategy = string(b)
	}
	p.laMetrics.UpdateLookaheadLaunchStatus(launchPhase, string(launchedFrom), launchStrategy)
}
