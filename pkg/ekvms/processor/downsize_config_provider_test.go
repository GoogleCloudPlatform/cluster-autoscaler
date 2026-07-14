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

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	processor_proto "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor/proto"
)

var (
	config1 = &processor_proto.DownsizeConfig{
		Behaviors: []*processor_proto.DownsizeConfig_DownsizeBehavior{
			{
				MaxDesiredSizeFraction: 0.125,
				DownsizeDelay:          durationpb.New(time.Minute * 60),
				AllowedForScaledown:    false,
			},
		},
		SmoothingWindowLength: durationpb.New(time.Minute * 10),
	}
	json1 = `
{
  "behaviors": [
    {
      "maxDesiredSizeFraction": 0.125,
      "minDownsizeFraction": 0,
      "downsizeDelay": "3600s",
      "allowedForScaledown": false
    }
  ],
  "smoothingWindowLength": "600s"
}
	`
	config2 = &processor_proto.DownsizeConfig{
		Behaviors: []*processor_proto.DownsizeConfig_DownsizeBehavior{
			{
				MaxDesiredSizeFraction: 0.5,
				DownsizeDelay:          durationpb.New(time.Minute * 30),
				AllowedForScaledown:    true,
			},
		},
		SmoothingWindowLength: durationpb.New(time.Minute * 5),
	}
	json2 = `
{
  "behaviors": [
    {
      "maxDesiredSizeFraction": 0.5,
      "minDownsizeFraction": 0,
      "downsizeDelay": "1800s",
      "allowedForScaledown": true
    }
  ],
  "smoothingWindowLength": "300s"
}	`
)

func TestNewDownsizeConfigProvider(t *testing.T) {
	testCases := map[string]struct {
		flagJson       string
		experimentJson string
		expectedConfig *processor_proto.DownsizeConfig
	}{
		"no config": {
			expectedConfig: DefaultDownsizeConfig(),
		},
		"empty config": {
			flagJson:       "{}",
			expectedConfig: DefaultDownsizeConfig(),
		},
		"only flagConfig": {
			flagJson:       json1,
			expectedConfig: config1,
		},
		"only experimentConfig": {
			experimentJson: json2,
			expectedConfig: config2,
		},
		"both flagConfig and experimentConfig": {
			flagJson:       json1,
			experimentJson: json2,
			expectedConfig: config1,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			mockEvaluator := mockStringFlagEvaluator{value: tc.experimentJson}
			mcp := machinetypes.NewMachineConfigProvider(nil)
			provider := NewDownsizeConfigProvider(mcp, mockEvaluator, map[string]string{machinetypes.EK.Name(): tc.flagJson}, map[string]string{machinetypes.EK.Name(): "test-experiment-flag"})
			config := provider.Provide()
			ekConfig := config[machinetypes.EK.Name()]
			if diff := cmp.Diff(tc.expectedConfig, ekConfig, protocmp.Transform()); diff != "" {
				t.Errorf("downsizeConfig missmatch, diff (-expected +actual):\n%s", diff)
			}
		})
	}
}

// Mock implementation of StringFlagEvaluator for testing.
type mockStringFlagEvaluator struct {
	value string
}

func (m mockStringFlagEvaluator) EvaluateStringFlagOrFailsafe(_, _ string) string {
	return m.value
}
