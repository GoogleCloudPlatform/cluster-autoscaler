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

package churntracker

import (
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// ConfigProvider provides the churn tracker configuration from the --lookahead-churn-circuit-breaker-config proto flag
// or, if it is not set, from the AutopilotResizableVM::LookaheadChurnCircuitBreaker experiment flag.
type ConfigProvider interface {
	// Refresh re-evaluates the experiment flag. It is a no-op if the configuration comes from the proto flag.
	Refresh()
	// IsEnabled returns true if the churn circuit breaker is enabled.
	IsEnabled() bool
	ChurnLifetimeThreshold() time.Duration
	ChurnWindow() time.Duration
	ChurnThreshold() int
	BackoffDurations() []time.Duration
	ResetWindow() time.Duration
	ResetMaxChurnEvents() int
}

type configProviderImpl struct {
	caVersion          version.Version
	experimentsManager experiments.Manager
	// fromProto indicates that the configuration comes from the proto flag, so the experiment is not evaluated.
	fromProto bool

	mu     sync.RWMutex
	config *Config
}

// NewConfigProvider creates a new ConfigProvider instance. If protoConfig (the --lookahead-churn-circuit-breaker-config flag
// value) is set and valid, it is used as a static configuration. Otherwise, the configuration is taken from the experiment.
func NewConfigProvider(em experiments.Manager, protoConfig string, caVersion version.Version) ConfigProvider {
	provider := &configProviderImpl{
		experimentsManager: em,
		caVersion:          caVersion,
		config:             defaultConfig(),
	}
	// Similarly to the lookahead/strategy/strategy_provider: this flag is a single-line JSON string
	// and we wrap it in single quotes for CA proto to treat it as a string.
	if config, ok := newProtoConfig(strings.TrimSpace(strings.Trim(protoConfig, "'")), caVersion); ok {
		provider.config = config
		provider.fromProto = true
		return provider
	}
	provider.Refresh()
	return provider
}

// newProtoConfig returns the Config from the trimmed proto flag value and true if it is set and valid.
// Like the experiment, the proto configuration is enabled only if its minCaVersion is valid and not higher than the CA version.
func newProtoConfig(protoConfig string, caVersion version.Version) (*Config, bool) {
	if protoConfig == "" || protoConfig == "{}" {
		return nil, false
	}
	flag, err := parseChurnCircuitBreakerFlag(protoConfig)
	if err != nil {
		klog.Warningf("Cannot parse --lookahead-churn-circuit-breaker-config flag, falling back to experiment %q: %v", experiments.ResizableVmChurnCircuitBreakerFlag, err)
		return nil, false
	}
	return NewConfig(flag, caVersion), true
}

// Refresh refreshes the churn tracker configuration from the experiments manager if proto config is not provided.
func (p *configProviderImpl) Refresh() {
	if p.fromProto || p.experimentsManager == nil {
		return
	}
	flagValue := p.experimentsManager.EvaluateStringFlagOrFailsafe(experiments.ResizableVmChurnCircuitBreakerFlag, `{"minCaVersion": "999.999.999"}`)
	flag, err := parseChurnCircuitBreakerFlag(flagValue)
	if err != nil {
		klog.Errorf("Cannot parse experiment %q flag: %v", experiments.ResizableVmChurnCircuitBreakerFlag, err)
		return
	}

	newConfig := NewConfig(flag, p.caVersion)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.config = newConfig
}

// IsEnabled returns true if the churn circuit breaker experiment is enabled.
func (p *configProviderImpl) IsEnabled() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.enabled
}

// ChurnLifetimeThreshold returns the maximum time between the churn tracker first seeing a node and its removal
// for the removal to be considered a lookahead churn event.
func (p *configProviderImpl) ChurnLifetimeThreshold() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.churnLifetimeThreshold
}

// ChurnWindow returns the period of time for which churn events are kept.
func (p *configProviderImpl) ChurnWindow() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.churnWindow
}

// ChurnThreshold returns the number of churn events of a workload separation within ChurnWindow that triggers the backoff.
func (p *configProviderImpl) ChurnThreshold() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.churnThreshold
}

// BackoffDurations returns the durations of the consecutive lookahead backoffs of a workload separation.
// The last duration is used for all the following backoffs.
func (p *configProviderImpl) BackoffDurations() []time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slices.Clone(p.config.backoffDurations)
}

// ResetWindow returns the period of time used to decide whether the backoff of a workload separation can be reset.
func (p *configProviderImpl) ResetWindow() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.resetWindow
}

// ResetMaxChurnEvents returns the maximum number of churn events within ResetWindow for the backoff to be reset.
func (p *configProviderImpl) ResetMaxChurnEvents() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.resetMaxChurnEvents
}
