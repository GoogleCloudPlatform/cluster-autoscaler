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

package gceclient

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api "google.golang.org/api/compute/v1"
)

func TestToGceReservationBlock(t *testing.T) {
	testCases := []struct {
		name       string
		blockName  string
		blockCount int64
		wantErr    bool
		err        string
	}{
		{
			name:       "Success",
			blockName:  "rb1",
			blockCount: 1,
			wantErr:    false,
			err:        "",
		},
		{
			name:    "Block is missing",
			wantErr: true,
			err:     "GCE reservation block is nil",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			id := uint64(time.Now().UnixNano())

			input := gce_api.ReservationBlock{
				Id:         id,
				Name:       tc.blockName,
				Count:      tc.blockCount,
				InUseCount: int64(0),
				Status:     "READY",
				Zone:       "rsv-zone",
			}

			var rb *GceReservationBlock
			var err error
			if tc.name == "Block is missing" {
				rb, err = toGceReservationBlock(nil)
			} else {
				rb, err = toGceReservationBlock(&input)
			}

			if tc.wantErr {
				assert.Nil(t, rb)
				assert.NotNil(t, err)
				assert.True(t, strings.Contains(err.Error(), tc.err))
			} else {
				assert.NotNil(t, rb)
				assert.Nil(t, err)
				assert.Equal(t, id, rb.Id)
				assert.Equal(t, "rb1", rb.Name)
				assert.Equal(t, int64(1), rb.Count)
				assert.Equal(t, "READY", rb.Status)
			}
		})
	}
}

func TestToGceReservationSubBlock(t *testing.T) {
	testCases := []struct {
		name       string
		blockName  string
		blockCount int64
		wantErr    bool
		err        string
	}{
		{
			name:       "Success",
			blockName:  "rbs1",
			blockCount: 1,
			wantErr:    false,
			err:        "",
		},
		{
			name:    "SubBlock is missing",
			wantErr: true,
			err:     "GCE reservation subblock is nil",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			id := uint64(time.Now().UnixNano())

			input := gce_api_beta.ReservationSubBlock{
				Id:         id,
				Name:       tc.blockName,
				Count:      tc.blockCount,
				InUseCount: int64(0),
				Status:     "READY",
				Zone:       "rsv-zone",
			}

			var rsb *GceReservationSubBlock
			var err error
			if tc.name == "SubBlock is missing" {
				rsb, err = toGceReservationSubBlock(nil)
			} else {
				rsb, err = toGceReservationSubBlock(&input)
			}

			if tc.wantErr {
				assert.Nil(t, rsb)
				assert.NotNil(t, err)
				assert.True(t, strings.Contains(err.Error(), tc.err))
			} else {
				assert.NotNil(t, rsb)
				assert.Nil(t, err)
				assert.Equal(t, id, rsb.Id)
				assert.Equal(t, "rbs1", rsb.Name)
				assert.Equal(t, int64(1), rsb.Count)
				assert.Equal(t, "READY", rsb.Status)
			}
		})
	}
}
