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

package processors

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	client_testing "k8s.io/client-go/testing"
	cr_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	cr_fake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/clientset/versioned/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"

	"github.com/stretchr/testify/assert"
)

func TestScaleUpProcessor(t *testing.T) {
	autoscalingContext := &context.AutoscalingContext{ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t)}
	p1 := BuildTestPod("p1", 40, 0)
	p2 := BuildTestPod("p2", 400, 0)
	cr1 := utils.BuildTestCr("cr1", "600m", "0", []cr_types.CapacityRequestConditionType{})
	cr2 := utils.BuildTestCr("cr2", "10m", "0", []cr_types.CapacityRequestConditionType{})
	cr3 := utils.BuildTestCr("cr3", "100m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesAvailable})

	testCases := []struct {
		caseName               string
		context                *context.AutoscalingContext
		scaleUpStatus          *status.ScaleUpStatus
		CRsTrigerredScaleUp    []*cr_types.CapacityRequest
		CRsRemainUnschedulable []*cr_types.CapacityRequest
		CRsAwaitEvaluation     []*cr_types.CapacityRequest
		expectedConditions     map[string]cr_types.CapacityRequestConditionType
	}{
		{
			caseName:           "Empty status",
			context:            autoscalingContext,
			scaleUpStatus:      &status.ScaleUpStatus{},
			expectedConditions: map[string]cr_types.CapacityRequestConditionType{},
		}, {
			caseName: "Multiple CRs",
			context:  autoscalingContext,
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
			},
			CRsTrigerredScaleUp:    []*cr_types.CapacityRequest{cr2},
			CRsRemainUnschedulable: []*cr_types.CapacityRequest{cr1},
			CRsAwaitEvaluation:     []*cr_types.CapacityRequest{cr3},
			expectedConditions: map[string]cr_types.CapacityRequestConditionType{
				"cr1": cr_types.ResourcesUnattainable,
				"cr2": cr_types.ResourcesInProvisioning,
				"cr3": ""},
		}, {
			caseName: "Multiple CRs with other pods",
			context:  autoscalingContext,
			scaleUpStatus: &status.ScaleUpStatus{
				Result:                  status.ScaleUpSuccessful,
				PodsRemainUnschedulable: []status.NoScaleUpInfo{utils.BuildTestNoScaleUpInfo(p2)},
				PodsTriggeredScaleUp:    []*apiv1.Pod{p1},
			},
			CRsTrigerredScaleUp:    []*cr_types.CapacityRequest{cr2},
			CRsRemainUnschedulable: []*cr_types.CapacityRequest{cr1},
			expectedConditions: map[string]cr_types.CapacityRequestConditionType{
				"cr1": cr_types.ResourcesUnattainable,
				"cr2": cr_types.ResourcesInProvisioning},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			fakeClient := cr_fake.NewSimpleClientset()
			crs := []*cr_types.CapacityRequest{}
			crs = append(crs, tc.CRsTrigerredScaleUp...)
			crs = append(crs, tc.CRsRemainUnschedulable...)
			crs = append(crs, tc.CRsAwaitEvaluation...)

			crState := utils.NewTestCapacityRequestState(fakeClient, crs)

			tc.scaleUpStatus.PodsTriggeredScaleUp = utils.AppendCRPods(t, tc.scaleUpStatus.PodsTriggeredScaleUp, crState, tc.CRsTrigerredScaleUp)
			tc.scaleUpStatus.PodsRemainUnschedulable = utils.AppendCRNoScaleUpInfos(t, tc.scaleUpStatus.PodsRemainUnschedulable, crState, tc.CRsRemainUnschedulable)
			tc.scaleUpStatus.PodsAwaitEvaluation = utils.AppendCRPods(t, tc.scaleUpStatus.PodsAwaitEvaluation, crState, tc.CRsAwaitEvaluation)

			p := NewCapacityRequestScaleUpProcessor(crState)
			p.Process(tc.context, tc.scaleUpStatus)
			actions := fakeClient.Actions()
			assert.Equal(t, len(tc.expectedConditions), len(actions), "Unexpected number of actions.")
			for _, a := range actions {
				assert.Equal(t, "update", a.GetVerb(), "Unexpected action: %v", a)
				ua := a.(client_testing.UpdateAction)
				obj := ua.GetObject()
				cr, ok := obj.(*cr_types.CapacityRequest)
				assert.True(t, ok, "Failed to cast object to Capacity Request: %v", obj)
				expected := tc.expectedConditions[cr.ObjectMeta.Name]
				found := false
				for _, cond := range cr.Status.Conditions {
					if cond.Type == expected {
						assert.Equal(t, apiv1.ConditionTrue, cond.Status, "Condition %v should be true on CapacityRequest %v", cond.Type, cr)
						found = true
					} else {
						assert.NotEqual(t, apiv1.ConditionTrue, cond.Status, "Unexpected %v condition on CapacityRequest %v", cond.Type, cr)
					}
				}
				if expected != "" {
					assert.True(t, found, "Missing %v condition on CapacityRequest %v", expected, cr)
				}
			}
		})
	}
}
