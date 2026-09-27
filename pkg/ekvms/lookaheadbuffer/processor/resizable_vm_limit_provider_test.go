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

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestProvide(t *testing.T) {
	tests := []struct {
		name            string
		caVersion       string
		experimentFlags map[string]string
		defaultLimit    int
		want            int
	}{
		{
			name:         "no experiment, use default",
			caVersion:    "33.0.0",
			defaultLimit: 2,
			want:         2,
		},
		{
			name:         "invalid flag",
			caVersion:    "33.0.0",
			defaultLimit: 0,
			experimentFlags: map[string]string{
				experiments.EkLookaheadMaxWorkloadSeparationsFlag: "1.2.3",
			},
			want: 0,
		},
		{
			name:         "invalid flag, non-int limit",
			caVersion:    "33.0.0",
			defaultLimit: 0,
			experimentFlags: map[string]string{
				experiments.EkLookaheadMaxWorkloadSeparationsFlag: "abc,1.2.3",
			},
			want: 0,
		},
		{
			name:         "empty flag",
			caVersion:    "33.0.0",
			defaultLimit: 0,
			experimentFlags: map[string]string{
				experiments.EkLookaheadMaxWorkloadSeparationsFlag: "",
			},
			want: 0,
		},
		{
			name:      "old CA version",
			caVersion: "32.0.0",
			experimentFlags: map[string]string{
				experiments.EkLookaheadMaxWorkloadSeparationsFlag: "5,33.0.0",
			},
			defaultLimit: 0,
			want:         0,
		},
		{
			name:      "valid version, use experimental limit",
			caVersion: "33.0.0",
			experimentFlags: map[string]string{
				experiments.EkLookaheadMaxWorkloadSeparationsFlag: "5,33.0.0",
			},
			defaultLimit: 0,
			want:         5,
		},
		{
			name:      "valid version, negative limit",
			caVersion: "33.0.0",
			experimentFlags: map[string]string{
				experiments.EkLookaheadMaxWorkloadSeparationsFlag: "-1,33.0.0",
			},
			defaultLimit: 0,
			want:         0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := version.FromString(tt.caVersion)
			if err != nil {
				t.Fatalf("component version %s is not a correct version %v", tt.caVersion, err)
			}

			r := newProvider(
				experiments.NewMockManagerWithOptions(v, nil, tt.experimentFlags),
				experiments.EkLookaheadMaxWorkloadSeparationsFlag,
				tt.defaultLimit,
				v,
			)
			assert.Equal(t, tt.want, r.provide())
		})
	}
}

func TestNewProvider(t *testing.T) {
	tests := []struct {
		name         string
		defaultLimit int
		want         *provider
	}{
		{
			name:         "negative default limit is set to 0",
			defaultLimit: -1,
			want:         &provider{flagName: experiments.EkLookaheadMaxWorkloadSeparationsFlag, defaultLimit: 0},
		},
		{
			name:         "zero default limit is unmodified",
			defaultLimit: 0,
			want:         &provider{flagName: experiments.EkLookaheadMaxWorkloadSeparationsFlag, defaultLimit: 0},
		},
		{
			name:         "positive default limit is unmodified",
			defaultLimit: 1,
			want:         &provider{flagName: experiments.EkLookaheadMaxWorkloadSeparationsFlag, defaultLimit: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, newProvider(nil, experiments.EkLookaheadMaxWorkloadSeparationsFlag, tt.defaultLimit, version.Version{}))
		})
	}
}
