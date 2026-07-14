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

package snowflake

import (
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	klog "k8s.io/klog/v2"
)

const (
	snowflakePrefix = "sf-"
)

type SnowflakeFilter struct {
	isAutopilotEnabled bool
}

func NewSnowflakeFilter(isAutopilotEnabled bool) *SnowflakeFilter {
	return &SnowflakeFilter{
		isAutopilotEnabled: isAutopilotEnabled,
	}
}

func (sf *SnowflakeFilter) BestOptions(options []expander.Option, nodeInfos map[string]*framework.NodeInfo) []expander.Option {
	// This feature should only be enabled for Autopilot clusters.
	if !sf.isAutopilotEnabled {
		return options
	}

	snowflakedOptions := []expander.Option{}
	for _, option := range options {
		if sf.isSnowflaked(option) {
			snowflakedOptions = append(snowflakedOptions, option)
		}
	}
	if len(snowflakedOptions) != 0 {
		klog.V(4).Infof("SnowflakeFilter: %d snowflaked Node Pools found", len(snowflakedOptions))
		return snowflakedOptions
	}
	// Return all options when there are no snowflakes.
	return options
}

// isSnowflaked returns true when Node Pool name starts with the snowflake prefix.
func (sf *SnowflakeFilter) isSnowflaked(option expander.Option) bool {
	mig, ok := option.NodeGroup.(*gke.GkeMig)
	if !ok {
		klog.Errorf("Couldn't cast to GkeMig: %v", option.NodeGroup)
		return false
	}
	return strings.HasPrefix(mig.NodePoolName(), snowflakePrefix)
}
