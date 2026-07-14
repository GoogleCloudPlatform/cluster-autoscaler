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

import "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"

// TODO(b/449919936): Cleanup after ekSpotEnabled experiment is over.
type ekSpotEnabledCache struct {
	experimentsManager experiments.Manager
	ekSpotEnabled      bool
}

func NewEkSpotEnabledCache(experimentsManager experiments.Manager) *ekSpotEnabledCache {
	p := &ekSpotEnabledCache{
		experimentsManager: experimentsManager,
	}
	p.RefreshValue()
	return p
}

func (p *ekSpotEnabledCache) RefreshValue() {
	p.ekSpotEnabled = p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.EkSpotFlag, false)
}

func (p *ekSpotEnabledCache) Get() bool {
	return p.ekSpotEnabled
}
