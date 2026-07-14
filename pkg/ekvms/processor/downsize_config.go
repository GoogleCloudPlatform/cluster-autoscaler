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

package processor

import (
	"math"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	processor_proto "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor/proto"
)

// DefaultDownsizeConfig returns a standard configuration of downsizing thresholds.
func DefaultDownsizeConfig() *processor_proto.DownsizeConfig {
	smallNode := &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 0.125,
		DownsizeDelay:          durationpb.New(time.Minute * 60),
		AllowedForScaledown:    true,
	}
	opportunisticReshape := &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 0.5,
		AllowedForScaledown:    true,
		DownsizeDelay:          durationpb.New(time.Minute * 10),
	}
	upsizeBuffer := &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 1,
		MinDownsizeFraction:    0.45,
		AllowedForScaledown:    false,
		DownsizeDelay:          durationpb.New(time.Minute * 1),
	}
	return &processor_proto.DownsizeConfig{
		Behaviors:             []*processor_proto.DownsizeConfig_DownsizeBehavior{smallNode, opportunisticReshape, upsizeBuffer},
		SmoothingWindowLength: durationpb.New(time.Minute * 1),
	}
}

// GetBehavior returns the first behaviour that can accommodate given EK node, or false if there is no such behaviour.
func GetBehavior(config *processor_proto.DownsizeConfig, node operationtracker.ResizableNode) (*processor_proto.DownsizeConfig_DownsizeBehavior, bool) {
	if config == nil || config.Behaviors == nil {
		return nil, false
	}
	for _, behavior := range config.Behaviors {
		maxDesiredSize := size.Allocatable{
			MilliCpus: int64(math.Round(behavior.MaxDesiredSizeFraction * float64(node.PhysicalMaxSize.MilliCpus))),
			KBytes:    int64(math.Round(behavior.MaxDesiredSizeFraction * float64(node.PhysicalMaxSize.KBytes))),
		}
		if (node.DesiredSize.MilliCpus <= maxDesiredSize.MilliCpus) || (node.DesiredSize.KBytes <= maxDesiredSize.KBytes) {
			return behavior, true
		}
	}
	return nil, false
}
