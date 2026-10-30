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
	"strings"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/validator/conditions"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestCCCInvalidComputeClassName verifies that a ComputeClass whose name is
// longer than 63 characters (accepted by the CRD before the name length rule)
// is reported as misconfigured and unhealthy, and that its minimum capacity
// does not create nodes: no node can carry its name as a label value. A
// ComputeClass with a 63-character name and the same setup is the control.
func TestCCCInvalidComputeClassName(t *testing.T) {
	testCases := []struct {
		name                string
		ccName              string
		wantNodes           int
		wantHealthStatus    metav1.ConditionStatus
		wantMisconfigReason string
	}{
		{
			name:             "63-character name gets its minimum capacity",
			ccName:           "cc-" + strings.Repeat("a", 60),
			wantNodes:        2,
			wantHealthStatus: metav1.ConditionTrue,
		},
		{
			name:                "64-character name gets no nodes and is unhealthy",
			ccName:              "cc-" + strings.Repeat("a", 61),
			wantNodes:           0,
			wantHealthStatus:    metav1.ConditionFalse,
			wantMisconfigReason: conditions.InvalidComputeClassNameReason,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder(tc.ccName).
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

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				if err != nil {
					t.Fatalf("SetupAutoscaler() failed: %v", err)
				}

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				// Let the background status aggregator flush the queued conditions.
				time.Sleep(ccc.ConditionsFlushInterval)

				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				if err != nil {
					t.Fatalf("Listing nodes failed: %v", err)
				}
				assert.Len(t, nodes.Items, tc.wantNodes)
				for _, node := range nodes.Items {
					assert.Equal(t, tc.ccName, node.Labels["cloud.google.com/compute-class"])
				}

				updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, tc.ccName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Getting ComputeClass %q failed: %v", tc.ccName, err)
				}
				conds := updatedCCC.Status.Conditions

				var gotHealthStatus metav1.ConditionStatus
				if healthCond := k8sapimeta.FindStatusCondition(conds, conditions.HealthCondition); healthCond != nil {
					gotHealthStatus = healthCond.Status
				}
				assert.Equal(t, tc.wantHealthStatus, gotHealthStatus, "Health condition status")

				gotMisconfigReason := ""
				if misconfigCond := k8sapimeta.FindStatusCondition(conds, conditions.CrdMisconfiguredCondition); misconfigCond != nil && misconfigCond.Status == metav1.ConditionTrue {
					gotMisconfigReason = misconfigCond.Reason
				}
				assert.Equal(t, tc.wantMisconfigReason, gotMisconfigReason, "CrdMisconfigured=True reason")
			})
		})
	}
}
