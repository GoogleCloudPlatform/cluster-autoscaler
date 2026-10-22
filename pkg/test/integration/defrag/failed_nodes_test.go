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

package defrag

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/failednodes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestFailedNodesPluginWithoutCSNNodeController runs the failed-nodes defrag
// plugin when CA doesn't create the CSN node controller, which happens when
// CSN or Capacity Buffers are disabled.
func TestFailedNodesPluginWithoutCSNNodeController(t *testing.T) {
	disableCapacityBuffers := func(o *internalopts.AutoscalingOptions) *internalopts.AutoscalingOptions {
		o.CapacitybufferControllerEnabled = false
		o.CapacitybufferPodInjectionEnabled = false
		return o
	}
	testCases := []struct {
		name      string
		overrides []integration.Option[*internalopts.AutoscalingOptions]
	}{
		{
			name:      "CSN disabled",
			overrides: []integration.Option[*internalopts.AutoscalingOptions]{integration.WithCSNEnabled(false)},
		},
		{
			name:      "Capacity Buffers disabled",
			overrides: []integration.Option[*internalopts.AutoscalingOptions]{integration.WithCSNEnabled(true), disableCapacityBuffers},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithNodePools(integration.DefaultNodePool(integration.WithNodePoolSize(2))).
				WithOverrides(integration.WithDefragEnabled(failednodes.PluginName)).
				WithOverrides(tc.overrides...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Defrag asks the failed-nodes plugin for a candidate on every loop.
				for range 3 {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
				}
			})
		})
	}
}
