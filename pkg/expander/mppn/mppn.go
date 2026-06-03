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

package mppn

import (
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
)

// Filter is an implementation of expander.Filter used to filter out
// expander.Options having too large max pods per node if there are different,
// similar options using smaller max pods per node.
type Filter struct {
	gcr              gkeprice.GroupCountReducer
	autopilotCluster bool
}

func NewFilter(gcr gkeprice.GroupCountReducer, autopilotCluster bool) *Filter {
	return &Filter{
		gcr:              gcr,
		autopilotCluster: autopilotCluster,
	}
}

func (f *Filter) BestOptions(options []expander.Option, _ map[string]*framework.NodeInfo) []expander.Option {
	var result []expander.Option
	autopilotOptions := options
	if !f.autopilotCluster {
		autopilotOptions, result = splitAutopilotManagedOptions(options)
	}
	groupedAutopilotOptions := groupOptions(autopilotOptions)

	for _, optionGroup := range groupedAutopilotOptions {
		var bestOption *expander.Option
		bestOptionPenalty := 0.0
		for _, option := range optionGroup {
			if penalty := f.getOptionPenalty(option); bestOption == nil || penalty < bestOptionPenalty {
				bestOption = &option
				bestOptionPenalty = penalty
			}
		}
		result = append(result, *bestOption)
	}
	return result
}

// splitAutopilotManagedOptions splits options into those that can
// be filtered by mppn expander and those that can't. Today we do not filter out
// standard (i.e. not autopilot managed) node pools. This is due to the fact,
// that this logic was not present in GKE Standard clusters and introducing it
// might be considered a breaking change.
// In general only one type of nodes (i.e. either autopilot managed or not managed)
// should be passed here, due to sharding, though we cannot assume it won't change
// in the future.
func splitAutopilotManagedOptions(options []expander.Option) ([]expander.Option, []expander.Option) {
	var filterableOptions []expander.Option
	var nonFilterableOptions []expander.Option
	for _, option := range options {
		if isAutopilotManaged(option) {
			filterableOptions = append(filterableOptions, option)
		} else {
			nonFilterableOptions = append(nonFilterableOptions, option)
		}
	}
	return filterableOptions, nonFilterableOptions
}

func isAutopilotManaged(option expander.Option) bool {
	gkeMig := option.NodeGroup.(*gke.GkeMig)
	return gkeMig.Spec().AutopilotManaged
}

func (f *Filter) getOptionPenalty(option expander.Option) float64 {
	penalty := 1.0
	mig := option.NodeGroup.(*gke.GkeMig)
	if !option.NodeGroup.Exist() {
		penalty = f.gcr.BaseGroupCreationPenalty()
	}
	return penalty * getMppnPenalty(option, mig)
}

// getMppnPenalty is a penalty for a given option based on IP space wasted.
// It's meant to be larger than penalty for new node group while cluster size is small,
// but smaller as the number of node groups grows. It's a linear function with
// similar constant as the group count penalty, which however is a quadratic function.
// More details about the formula can be found here:
// go/dynamic-max-pods-per-node-design#bookmark=id.yjqslx2mcvp5.
func getMppnPenalty(option expander.Option, mig *gke.GkeMig) float64 {
	return 1.0 + 0.0008*float64(option.NodeCount*int(mig.Spec().MaxPodsPerNode)-len(option.Pods))
}
