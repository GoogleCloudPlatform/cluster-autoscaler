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

// This test lives in the external test package because pkg/processors depends
// on pkg/defrag/processor, which depends on the package under test. An internal
// test could not import the processors it needs to check.
package scaledown_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/extendeddurationpods"
	internal_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaledown"
)

// Compile-time assertions that the processors which claim a reason still
// satisfy the interface. A signature change breaks the build here rather than
// silently downgrading every affected node to a generic reason at runtime.
var (
	_ scaledown.ExclusionReasonProvider = &scaleblocking.Processor{}
	_ scaledown.ExclusionReasonProvider = &internal_processors.SurgeUpgradeScaleDownNodeProcessor{}
	_ scaledown.ExclusionReasonProvider = &extendeddurationpods.ScaleDownProcessor{}
)

func TestExclusionReasons(t *testing.T) {
	testCases := []struct {
		name     string
		provider scaledown.ExclusionReasonProvider
		want     observability.BlockReason
	}{
		{
			name:     "blocked MIG",
			provider: &scaleblocking.Processor{},
			want:     observability.NodePoolOperationInProgress,
		},
		{
			name:     "surge upgrade",
			provider: &internal_processors.SurgeUpgradeScaleDownNodeProcessor{},
			want:     observability.NodePoolOperationInProgress,
		},
		{
			name:     "extended duration pods",
			provider: &extendeddurationpods.ScaleDownProcessor{},
			want:     observability.BlockingPods,
		},
	}

	known := make(map[observability.BlockReason]bool)
	for _, reason := range observability.AllReasons() {
		known[reason] = true
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.provider.ExclusionReason()
			assert.Equal(t, tc.want, got)
			// A reason the ComputeClass API does not declare is rejected by the
			// apiserver, so it must never reach the status writer.
			assert.True(t, known[got], "reason %q is not one of observability.AllReasons()", got)
		})
	}
}
