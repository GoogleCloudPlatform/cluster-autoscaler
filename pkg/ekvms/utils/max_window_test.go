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

package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	clock "k8s.io/utils/clock/testing"
)

var testStartTime = time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)

func TestTtlMaxWindow_Expiration(t *testing.T) {
	for _, tc := range []struct {
		name             string
		ttl              time.Duration
		wantMaxMilliCpus int64
		wantMaxKBytes    int64
	}{
		{
			name:             "three last samples",
			ttl:              35 * time.Minute,
			wantMaxMilliCpus: 3,
			wantMaxKBytes:    3,
		},
		{
			name:             "two last samples",
			ttl:              25 * time.Minute,
			wantMaxMilliCpus: 2,
			wantMaxKBytes:    3,
		},
		{
			name:             "one last sample",
			ttl:              15 * time.Minute,
			wantMaxMilliCpus: 1,
			wantMaxKBytes:    3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeClock := clock.NewFakeClock(testStartTime)
			w := NewTtlMaxWindow(fakeClock, tc.ttl)
			w.Add(size.Allocatable{MilliCpus: 3, KBytes: 1})
			fakeClock.Sleep(10 * time.Minute)
			w.Add(size.Allocatable{MilliCpus: 2, KBytes: 2})
			fakeClock.Sleep(10 * time.Minute)
			w.Add(size.Allocatable{MilliCpus: 1, KBytes: 3})
			fakeClock.Sleep(10 * time.Minute)
			gotMilliCpus, err := w.MaxMilliCpus()
			assert.Nil(t, err)
			assert.Equal(t, gotMilliCpus, tc.wantMaxMilliCpus)
			gotKBytes, err := w.MaxKBytes()
			assert.Nil(t, err)
			assert.Equal(t, gotKBytes, tc.wantMaxKBytes)
		})
	}
}

func TestTtlMaxWindow_NoSamples(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	ttl := 60 * time.Minute
	w := NewTtlMaxWindow(fakeClock, ttl)
	_, err := w.MaxMilliCpus()
	assert.Error(t, err)
}
