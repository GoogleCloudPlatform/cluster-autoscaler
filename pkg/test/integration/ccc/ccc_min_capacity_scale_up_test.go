/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccc_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func TestCCCScaleUpToMinCapacity(t *testing.T) {
	testCases := []struct {
		name              string
		experimentEnabled bool
		expectedNodeCount int
	}{
		{
			name:              "scales up to min capacity when experiment is enabled",
			experimentEnabled: true,
			expectedNodeCount: 2,
		},
		{
			name:              "does not scale up when experiment is disabled",
			experimentEnabled: false,
			expectedNodeCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder("my-ccc").
				WithNapEnabled().
				WithTargetNodeCount(ptr.To(2)).
				WithWhenUnsatisfiable("ScaleUpAnyway").
				WithPriorities(v1.Priority{MachineFamily: ptr.To("n1")}).
				Build()

			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithComputeClassMinCapacityEnabled(),
					integration.WithEnhancedCrdStatusReportingEnabled(),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
				).
				WithCccCrds(cc)

			if !tc.experimentEnabled {
				testConfig.WithExperimentOverrides(
					map[string]bool{"ComputeClassMinCapacity::Enabled": false},
					map[string]string{},
				)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Run the autoscaler loop once. If enabled, NAP should scale up to satisfy target minimum capacity.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Verify the node count.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodeCount, len(nodes.Items))

				for _, node := range nodes.Items {
					assert.Equal(t, "my-ccc", node.Labels["cloud.google.com/compute-class"])
				}

				if tc.experimentEnabled {
					// Let the background status aggregator flush the queued condition.
					time.Sleep(ccc.ConditionsFlushInterval)

					updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, "my-ccc", metav1.GetOptions{})
					assert.NoError(t, err)

					// The scale-up was triggered by min capacity fake pods, not by real pending pods.
					ccc.AssertPriorityCondition(t, updatedCCC, 0, history.ConditionTypeNodeProvisioningInProgress, history.ConditionReasonMinimumCapacity)
				}
			})
		})
	}
}
