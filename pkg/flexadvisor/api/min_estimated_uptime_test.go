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

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestMinEstimatedUptimeFromMinutes(t *testing.T) {
	testCases := []struct {
		name       string
		minutes    *int
		wantUptime MinEstimatedUptime
		wantIsSet  bool
		wantErr    bool
	}{
		{
			name:       "nil_minutes",
			minutes:    nil,
			wantUptime: MinUptimeUnset,
			wantIsSet:  false,
		},
		{
			name:       "1_minute",
			minutes:    ptr.To(1),
			wantUptime: MinUptime1Min,
			wantIsSet:  true,
		},
		{
			name:       "10_minutes",
			minutes:    ptr.To(10),
			wantUptime: MinUptime10Min,
			wantIsSet:  true,
		},
		{
			name:       "60_minutes",
			minutes:    ptr.To(60),
			wantUptime: MinUptime1Hour,
			wantIsSet:  true,
		},
		{
			name:       "zero_minutes_invalid",
			minutes:    ptr.To(0),
			wantUptime: MinUptimeUnset,
			wantIsSet:  false,
			wantErr:    true,
		},
		{
			name:       "negative_minutes_invalid",
			minutes:    ptr.To(-5),
			wantUptime: MinUptimeUnset,
			wantIsSet:  false,
			wantErr:    true,
		},
		{
			name:       "unsupported_positive_minutes_invalid",
			minutes:    ptr.To(123123),
			wantUptime: MinUptimeUnset,
			wantIsSet:  false,
			wantErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MinEstimatedUptimeFromMinutes(tc.minutes)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantUptime, got)
			assert.Equal(t, tc.wantIsSet, got.IsSet())
		})
	}
}

func TestMinEstimatedUptimeIsSetOnlyChecksUnset(t *testing.T) {
	assert.False(t, MinUptimeUnset.IsSet())
	for _, invalid := range []MinEstimatedUptime{
		MinEstimatedUptime("-60s"),
		MinEstimatedUptime("10s"),
		MinEstimatedUptime("300s"),
	} {
		assert.True(t, invalid.IsSet())
	}
}
