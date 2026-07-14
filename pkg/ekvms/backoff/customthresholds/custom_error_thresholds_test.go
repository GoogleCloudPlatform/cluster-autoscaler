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

package customthresholds

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		desc    string
		jsonStr string
		wantCt  CustomThresholdsPerErrorType
		wantErr bool
	}{
		{
			desc:    "empty string",
			jsonStr: "",
			wantCt: CustomThresholdsPerErrorType{
				Status:                  Disabled,
				ErrorThresholdsDisabled: false,
				UpsizeTriesThreshold:    0,
				ForceScaleUpDisabled:    false,
			},
			wantErr: false,
		},
		{
			desc:    "empty json",
			jsonStr: "{}",
			wantCt: CustomThresholdsPerErrorType{
				Status:                  Disabled,
				ErrorThresholdsDisabled: false,
				UpsizeTriesThreshold:    0,
				ForceScaleUpDisabled:    false,
			},
			wantErr: false,
		},
		{
			desc:    "invalid string",
			jsonStr: "this-is-invalid-json-string",
			wantCt:  CustomThresholdsPerErrorType{},
			wantErr: true,
		},
		{
			desc: "valid json with errors and force scale up is enabled",
			jsonStr: `{
  "errors": [
    {
      "name": "guestAgentResizeTimeout",
      "threshold": 3
    },
    {
      "name": "timeoutError",
      "threshold": 3
    },
    {
      "name": "rateLimitExceededError",
      "threshold": 3
    },
    {
      "name": "quotaExceededError",
      "threshold": 3
    }
  ],
  "minCaVersion": "35.197.0",
  "status": "STATUS_ENABLED"
}`,
			wantCt: CustomThresholdsPerErrorType{
				Errors: []ErrorThreshold{
					{Name: "guestAgentResizeTimeout", Threshold: 3},
					{Name: "timeoutError", Threshold: 3},
					{Name: "rateLimitExceededError", Threshold: 3},
					{Name: "quotaExceededError", Threshold: 3},
				},
				MinCaVersion:            "35.197.0",
				Status:                  Enabled,
				ErrorThresholdsDisabled: false,
				UpsizeTriesThreshold:    0,
				ForceScaleUpDisabled:    false,
			},
			wantErr: false,
		}, {
			desc: "valid json with errors and force scale up is disabled",
			jsonStr: `{
  "errors": [
    {
      "name": "guestAgentResizeTimeout",
      "threshold": 3
    },
    {
      "name": "timeoutError",
      "threshold": 3
    },
    {
      "name": "rateLimitExceededError",
      "threshold": 3
    },
    {
      "name": "quotaExceededError",
      "threshold": 3
    }
  ],
  "minCaVersion": "35.197.0",
  "status": "STATUS_ENABLED",
  "upsizeTriesThreshold": 2,
  "forceScaleUpDisabled": true
}`,
			wantCt: CustomThresholdsPerErrorType{
				Errors: []ErrorThreshold{
					{Name: "guestAgentResizeTimeout", Threshold: 3},
					{Name: "timeoutError", Threshold: 3},
					{Name: "rateLimitExceededError", Threshold: 3},
					{Name: "quotaExceededError", Threshold: 3},
				},
				MinCaVersion:         "35.197.0",
				Status:               Enabled,
				UpsizeTriesThreshold: 2,
				ForceScaleUpDisabled: true,
			},
			wantErr: false,
		},
		{
			desc: "status set to disabled and features are disabled",
			jsonStr: `
{
  "status": "STATUS_DISABLED",
  "errorThresholdsDisabled": true,
  "forceScaleUpDisabled": true
}
`,
			wantCt: CustomThresholdsPerErrorType{
				Status:                  Disabled,
				ErrorThresholdsDisabled: true,
				ForceScaleUpDisabled:    true,
			},
			wantErr: false,
		},
		{
			desc:    "status set to invalid",
			jsonStr: `{"status": "STATUS_INVALID"}`,
			wantCt: CustomThresholdsPerErrorType{
				Status:               Disabled,
				UpsizeTriesThreshold: 0,
			},
			wantErr: false,
		},
		{
			desc:    "status set to enabled, features are enabled, but thresholds are empty",
			jsonStr: `{"status": "STATUS_ENABLED"}`,
			wantCt: CustomThresholdsPerErrorType{
				Status:                  Enabled,
				ErrorThresholdsDisabled: false,
				ForceScaleUpDisabled:    false,
			},
			wantErr: false,
		},
		{
			desc: "status set to enabled with nil errors",
			jsonStr: `
{
  "minCaVersion": "35.197.0",
  "status": "STATUS_ENABLED",
}
`,
			wantCt: CustomThresholdsPerErrorType{
				MinCaVersion:            "35.197.0",
				Status:                  Enabled,
				ErrorThresholdsDisabled: true,
				ForceScaleUpDisabled:    false,
			},
			wantErr: true,
		},
		{
			desc: "status set to enabled with nil errors and upsizeTriesThreshold",
			jsonStr: `
{
  "minCaVersion": "35.197.0",
  "status": "STATUS_ENABLED",
  "upsizeTriesThreshold": 3,
  "forceScaleUpDisabled": true
}
`,
			wantCt: CustomThresholdsPerErrorType{
				MinCaVersion:         "35.197.0",
				Status:               Enabled,
				UpsizeTriesThreshold: 3,
				ForceScaleUpDisabled: true,
			},
			wantErr: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ct, err := parseCustomThresholdsPerErrorType(tc.jsonStr)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantCt, ct)
		})
	}
}
