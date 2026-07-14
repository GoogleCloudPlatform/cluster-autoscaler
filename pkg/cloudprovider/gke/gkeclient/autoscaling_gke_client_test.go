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

package gkeclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	gke_api_beta "google.golang.org/api/container/v1beta1"
)

func woErr[T any](result T, err error) T {
	if err != nil {
		panic(err)
	}
	return result
}

func TestEphemeralStorageOnLocalSsd(t *testing.T) {
	testCases := []struct {
		desc string
		cfg  *LocalSSDConfig
		want bool
	}{
		{
			desc: "nil config",
		},
		{
			desc: "PV localSSD doesn't count as ephemeral storage",
			cfg: &LocalSSDConfig{
				EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{},
				LocalSsdCount:          123,
			},
		},
		{
			desc: "ephemeral storage config with local ssd",
			cfg: &LocalSSDConfig{
				EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
					LocalSsdCount: 123,
				},
			},
			want: true,
		},
		{
			desc: "ephemeral storage local ssd config with local ssd",
			cfg: &LocalSSDConfig{
				EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{
					LocalSsdCount: 123,
				},
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// TODO(b/317518331): Add tests for different machine types.
			machineType := ""
			got := tc.cfg.EphemeralStorageOnLocalSsd(machineType)
			if tc.want != got {
				t.Errorf("%+v.EphemeralStorageOnLocalSsd(%s) = %v, want %v", tc.cfg, machineType, got, tc.want)
			}
		})
	}
}

func TestSpecConsolidationDelay(t *testing.T) {
	testCases := []struct {
		desc    string
		spec    *NodePoolSpec
		want    *time.Duration
		wantErr bool
	}{
		{
			desc: "nil spec",
			want: nil,
		},
		{
			desc: "nil consolidation delay",
			want: nil,
		},
		{
			desc: "empty consolidation delay",
			spec: &NodePoolSpec{
				ConsolidationDelayInSeconds: "",
			},
			want: nil,
		},
		{
			desc: "invalid consolidation delay",
			spec: &NodePoolSpec{
				ConsolidationDelayInSeconds: "invalid",
			},
			want:    nil,
			wantErr: true,
		},
		{
			desc: "invalid consolidation delay 2",
			spec: &NodePoolSpec{
				ConsolidationDelayInSeconds: "100s",
			},
			want:    nil,
			wantErr: true,
		},
		{
			desc: "valid consolidation delay",
			spec: &NodePoolSpec{
				ConsolidationDelayInSeconds: "123",
			},
			want: ptr.To(woErr(time.ParseDuration("123s"))),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := tc.spec.ConsolidationDelay()
			assert.Equal(t, tc.want, got)
			if tc.wantErr {
				assert.Error(t, err)
				assert.ErrorContains(t, err, "while parsing 'consolidationDelayInSeconds' got error")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
