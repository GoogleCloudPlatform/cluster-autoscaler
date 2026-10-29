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

package cli

import (
	"flag"
	"slices"
	"testing"

	"sigs.k8s.io/cluster-autoscaler/pkg/config"

	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
)

const testScaleDownPriorityTaintKey = "cloud.google.com/gke-scale-down-priority"

// setFlag sets the command line flag name to value for the duration of the
// test.
func setFlag(t *testing.T, name, value string) {
	t.Helper()
	f := flag.CommandLine.Lookup(name)
	if f == nil {
		t.Fatalf("flag --%s is not defined", name)
	}
	previous := f.Value.String()
	if err := f.Value.Set(value); err != nil {
		t.Fatalf("Setting --%s=%q failed: %v", name, value, err)
	}
	t.Cleanup(func() {
		if err := f.Value.Set(previous); err != nil {
			t.Errorf("Restoring --%s=%q failed: %v", name, previous, err)
		}
	})
}

func TestPostProcessOssOptions_ScaleDownPriorityTaintKeyIsAStatusTaint(t *testing.T) {
	testCases := []struct {
		name               string
		flagValue          string
		wantInStatusTaints bool
	}{
		{
			name:               "flag unset",
			flagValue:          "",
			wantInStatusTaints: false,
		},
		{
			name:               "flag set",
			flagValue:          testScaleDownPriorityTaintKey,
			wantInStatusTaints: true,
		},
		{
			name:               "surrounding spaces are trimmed",
			flagValue:          "  " + testScaleDownPriorityTaintKey + " ",
			wantInStatusTaints: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setFlag(t, "scale-down-priority-taint-key", tc.flagValue)
			// PostProcessOssOptions exits unless the mppn expander and frequent
			// loops are enabled.
			opts := &config.AutoscalingOptions{
				ExpanderNames:        internalopts.MaxPodsPerNodeExpanderName,
				FrequentLoopsEnabled: true,
			}

			PostProcessOssOptions(opts)

			if got := slices.Contains(opts.StatusTaints, testScaleDownPriorityTaintKey); got != tc.wantInStatusTaints {
				t.Errorf("PostProcessOssOptions() StatusTaints = %q, contains %q: %t, want %t", opts.StatusTaints, testScaleDownPriorityTaintKey, got, tc.wantInStatusTaints)
			}
			if slices.Contains(opts.StatusTaints, "") {
				t.Errorf("PostProcessOssOptions() StatusTaints = %q, want no empty taint key", opts.StatusTaints)
			}
		})
	}
}
