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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func TestBestOptions(t *testing.T) {
	snowflake := expander.Option{NodeGroup: gke.NewTestGkeMigBuilder().SetNodePoolName("sf-abcds").Build()}
	notSnowflake := expander.Option{NodeGroup: gke.NewTestGkeMigBuilder().SetNodePoolName("nap-abcd").Build()}

	testCases := []struct {
		name               string
		isAutopilotEnabled bool
		options            []expander.Option
		wantOptions        []expander.Option
	}{
		{
			name:               "Autopilot not enabled",
			isAutopilotEnabled: false,
			options:            []expander.Option{snowflake, notSnowflake},
			wantOptions:        []expander.Option{snowflake, notSnowflake},
		},
		{
			name:               "snowflake Node Pool present",
			isAutopilotEnabled: true,
			options:            []expander.Option{snowflake, notSnowflake},
			wantOptions:        []expander.Option{snowflake},
		},
		{
			name:               "snowflake Node Pool not present",
			isAutopilotEnabled: true,
			options:            []expander.Option{notSnowflake},
			wantOptions:        []expander.Option{notSnowflake},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sf := NewSnowflakeFilter(tc.isAutopilotEnabled)
			assert.ElementsMatch(t, tc.wantOptions, sf.BestOptions(tc.options, nil))
		})
	}
}
