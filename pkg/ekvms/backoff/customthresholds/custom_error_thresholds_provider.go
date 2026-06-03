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

package customthresholds

import (
	"sync"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// CustomThresholdsProvider provides the threshold for each error type.
type CustomThresholdsProvider interface {
	RefreshCustomThresholds()
	GetThreshold(errorType string) (int, bool)
	IsErrorThresholdsFeatureEnabled() bool
	GetUpsizeTriesThreshold() int
	IsForceScaleUpFeatureEnabled() bool
}

type customThresholdsProviderImpl struct {
	caVersion          version.Version
	experimentsManager experiments.Manager

	mu                sync.RWMutex
	currentThresholds *CustomErrorThresholds
}

// NewCustomThresholdsProvider creates a new CustomThresholdsProvider instance.
func NewCustomThresholdsProvider(em experiments.Manager, caVersion version.Version) CustomThresholdsProvider {
	provider := &customThresholdsProviderImpl{
		experimentsManager: em,
		caVersion:          caVersion,
	}
	provider.RefreshCustomThresholds()
	return provider
}

// RefreshCustomThresholds refreshes the custom thresholds from the experiment manager.
func (p *customThresholdsProviderImpl) RefreshCustomThresholds() {
	if p.experimentsManager == nil {
		return
	}
	experimentConfigFlag := p.experimentsManager.EvaluateStringFlagOrFailsafe(experiments.ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag, `{"minCaVersion": "999.999.999"}`)
	experimentConfig, err := parseCustomThresholdsPerErrorType(experimentConfigFlag)
	if err != nil {
		klog.Errorf("Cannot parse experiment %q flag: %v", experiments.ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag, err)
		return
	}

	newThresholds := NewCustomErrorThresholds(experimentConfig, p.caVersion)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.currentThresholds = newThresholds
}

// IsErrorThresholdsFeatureEnabled returns true if error thresholds feature is enabled.
func (p *customThresholdsProviderImpl) IsErrorThresholdsFeatureEnabled() bool {
	if p.currentThresholds == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return !p.currentThresholds.errorThresholdsFeatureDisabled
}

// GetThreshold returns the threshold for a given error type.
func (p *customThresholdsProviderImpl) GetThreshold(errorType string) (int, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentThresholds.getThreshold(errorType)
}

// IsForceScaleUpFeatureEnabled returns true if force scale up feature is enabled.
func (p *customThresholdsProviderImpl) IsForceScaleUpFeatureEnabled() bool {
	if p.currentThresholds == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return !p.currentThresholds.forceScaleUpFeatureDisabled
}

// GetUpsizeTriesThreshold returns UpsizeTriesThreshold defined in the experiment flag.
func (p *customThresholdsProviderImpl) GetUpsizeTriesThreshold() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentThresholds.upsizeTriesThreshold
}
