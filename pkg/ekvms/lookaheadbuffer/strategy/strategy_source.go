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

// strategySource provides the raw LookaheadPodStrategy and its launch source.
type strategySource interface {
	strategy() (LookaheadPodStrategy, launchSource)
	refresh()
}

// flagStrategySource provides a static strategy configured via CA flags.
type flagStrategySource struct {
	flagStrategy LookaheadPodStrategy
}

func (s *flagStrategySource) refresh() {}

func (s *flagStrategySource) strategy() (LookaheadPodStrategy, launchSource) {
	return s.flagStrategy, clusterProtoSource
}

// experimentStrategySource provides a strategy resolved dynamically via experiments.
type experimentStrategySource struct {
	experimentsManager experiments.Manager
	componentVersion   version.Version
	experimentFlag     string
	machineFamily      string
	cachedStrategy     LookaheadPodStrategy
}

func newExperimentStrategySource(
	em experiments.Manager,
	componentVersion version.Version,
	experimentFlag string,
	machineFamily string,
) *experimentStrategySource {
	s := &experimentStrategySource{
		experimentsManager: em,
		componentVersion:   componentVersion,
		experimentFlag:     experimentFlag,
		machineFamily:      machineFamily,
		cachedStrategy:     unspecifiedStrategy,
	}
	s.refresh()
	return s
}

func (s *experimentStrategySource) refresh() {
	if s.experimentsManager == nil {
		s.cachedStrategy = unspecifiedStrategy
		return
	}
	experimentConfigFlag := s.experimentsManager.EvaluateStringFlagOrFailsafe(s.experimentFlag, `{"minCaVersion": "999.999.999"}`)
	experimentConfig, err := ParsePodStrategy(experimentConfigFlag)
	if err != nil {
		klog.Errorf("Cannot parse experiment %q flag for %s: %v", s.experimentFlag, s.machineFamily, err)
		s.cachedStrategy = unspecifiedStrategy
		return
	}

	experimentVersion, err := version.FromString(experimentConfig.MinCaVersion)
	if err != nil {
		klog.Errorf("Experiment %q provided invalid min version %q for %s, using unspecified lookahead pod strategy", s.experimentFlag, experimentConfig.MinCaVersion, s.machineFamily)
		s.cachedStrategy = unspecifiedStrategy
		return
	}

	// Fallback to unspecified lookahead pod strategy if component version is less than minCaVersion in experiment.
	if s.componentVersion.LessThan(experimentVersion) {
		s.cachedStrategy = unspecifiedStrategy
		return
	}

	s.cachedStrategy = experimentConfig
}

func (s *experimentStrategySource) strategy() (LookaheadPodStrategy, launchSource) {
	if s.cachedStrategy.Status != Unspecified {
		return s.cachedStrategy, experimentSource
	}
	return unspecifiedStrategy, undefinedSource
}
