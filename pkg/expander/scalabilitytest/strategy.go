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

package scalabilitytest

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	klog "k8s.io/klog/v2"
)

const preferredInstanceType = "n1-standard-1"

// scalabilityTestStrategy is an expander strategy used for scalability testing, when we always want to choose a specific
// instance type to achieve a desired cluster shape. It also includes the original strategy computation to still test
// for possible regressions.
type scalabilityTestStrategy struct {
	originalStrategy expander.Strategy
}

// BestOption chooses an arbitrary option with the preferred instance type, falling back to originalStrategy result if
// there are no such options.
func (s *scalabilityTestStrategy) BestOption(options []expander.Option, nodeInfos map[string]*framework.NodeInfo) *expander.Option {
	// Include original strategy computation to test for any regressions.
	originalResult := s.originalStrategy.BestOption(options, nodeInfos)

	// Choose an arbitrary option with the preferred instance type.
	for _, option := range options {
		nodeInfo, found := nodeInfos[option.NodeGroup.Id()]
		if !found {
			klog.Errorf("No node info for %s", option.NodeGroup.Id())
			continue
		}
		if nodeInfo.Node().Labels == nil {
			continue
		}

		if instanceType, found := nodeInfo.Node().Labels[apiv1.LabelInstanceType]; found && instanceType == preferredInstanceType {
			return &option
		}
	}

	// Fall back to the original result if there are no options with the preferred instance type.
	klog.Warningf("No option with the preferred instance type %s, falling back to the original strategy result.", preferredInstanceType)
	return originalResult
}

// BestOptions narrows down the list of expansion options to a subset which is equally good as far a given expander is concerned.
// In case of scalability-test expander, there's only a single winning option.
func (s *scalabilityTestStrategy) BestOptions(expansionOptions []expander.Option, nodeInfos map[string]*framework.NodeInfo) []expander.Option {
	opts := make([]expander.Option, 0, 1)
	best := s.BestOption(expansionOptions, nodeInfos)
	if best != nil {
		opts = append(opts, *best)
	}
	return opts
}

// NewStrategy returns an instance of expander strategy for use in scalability testing.
func NewStrategy(originalStrategy expander.Strategy) *scalabilityTestStrategy {
	return &scalabilityTestStrategy{originalStrategy: originalStrategy}
}
