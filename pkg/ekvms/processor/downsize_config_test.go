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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	processor_proto "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor/proto"
)

func TestGetBehavior(t *testing.T) {
	smallNode := &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 0.125,
		DownsizeDelay:          durationpb.New(time.Minute * 60),
		AllowedForScaledown:    true,
	}
	bigNode := &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 0.5,
		DownsizeDelay:          durationpb.New(time.Minute * 20),
		AllowedForScaledown:    true,
	}
	downsizeConfig := &processor_proto.DownsizeConfig{
		Behaviors:             []*processor_proto.DownsizeConfig_DownsizeBehavior{smallNode, bigNode},
		SmoothingWindowLength: durationpb.New(5 * time.Minute),
	}

	testCases := map[string]struct {
		config           *processor_proto.DownsizeConfig
		node             operationtracker.ResizableNode
		expectedBehavior *processor_proto.DownsizeConfig_DownsizeBehavior
		expectedFound    bool
	}{
		"nil config": {
			config:           nil,
			node:             ekNode32(size.Allocatable{MilliCpus: 1, KBytes: 1}),
			expectedBehavior: nil,
			expectedFound:    false,
		},
		"empty config": {
			config:           &processor_proto.DownsizeConfig{},
			node:             ekNode32(size.Allocatable{MilliCpus: 1, KBytes: 1}),
			expectedBehavior: nil,
			expectedFound:    false,
		},
		"small node found - cpu": {
			config:           downsizeConfig,
			node:             ekNode32(size.Allocatable{MilliCpus: 8000, KBytes: 1}),
			expectedBehavior: smallNode,
			expectedFound:    true,
		},
		"small node found - memory": {
			config:           downsizeConfig,
			node:             ekNode32(size.Allocatable{MilliCpus: 1, KBytes: 32 * giBToKiB}),
			expectedBehavior: smallNode,
			expectedFound:    true,
		},
		"big node found": {
			config:           downsizeConfig,
			node:             ekNode32(size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB}),
			expectedBehavior: bigNode,
			expectedFound:    true,
		},
		"config not found": {
			config:           downsizeConfig,
			node:             ekNode32(size.Allocatable{MilliCpus: 32000, KBytes: 128 * giBToKiB}),
			expectedBehavior: nil,
			expectedFound:    false,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			behavior, found := GetBehavior(tc.config, tc.node)
			assert.Equal(t, tc.expectedBehavior, behavior)
			assert.Equal(t, tc.expectedFound, found)
		})
	}
}

func ekNode32(desiredSize size.Allocatable) operationtracker.ResizableNode {
	ek32MaxSize := size.Allocatable{MilliCpus: 32000, KBytes: 128 * giBToKiB}
	return operationtracker.ResizableNode{DesiredSize: desiredSize, PhysicalMaxSize: ek32MaxSize}
}
