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

package flexadvisor

import (
	"context"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/binpacking"
)

type binpackingLimiter struct {
	limiterTracker ScaleUpLimiterTracker
}

// NewBinpackingLimiter creates a BinpackingLimiter for FlexAdvisor that resets per-pass trackers
// at the start of bin-packing.
func NewBinpackingLimiter(limiterTracker ScaleUpLimiterTracker) binpacking.BinpackingLimiter {
	return &binpackingLimiter{
		limiterTracker: limiterTracker,
	}
}

// InitBinpacking runs at the beginning of bin packing processor, once per scale-up pass.
// We use it to reset scaleUpLimiterTracker so that async HTNAP ScaleUpStatusProcessor
// invocations do not clear limiterTracker mid-scale-up.
func (l *binpackingLimiter) InitBinpacking(context *ca_context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup) {
	if l.limiterTracker != nil {
		l.limiterTracker.Reset()
	}
}

// MarkProcessed is a no-op to satisfy the binpacking.BinpackingLimiter interface.
func (l *binpackingLimiter) MarkProcessed(context *ca_context.AutoscalingContext, nodegroupId string) {
}

// StopBinpacking returns false to satisfy the binpacking.BinpackingLimiter interface.
func (l *binpackingLimiter) StopBinpacking(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, evaluatedOptions []expander.Option) bool {
	return false
}

// FinalizeBinpacking is a no-op to satisfy the binpacking.BinpackingLimiter interface.
func (l *binpackingLimiter) FinalizeBinpacking(context *ca_context.AutoscalingContext, finalOptions []expander.Option) {
}
