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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	kube_record "k8s.io/client-go/tools/record"
	cr_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	cr_fake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/clientset/versioned/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	cr_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestEventingScaleUpStatusProcessor(t *testing.T) {
	p1 := BuildTestPod("p1", 0, 0)
	p2 := BuildTestPod("p2", 0, 0)
	p3 := BuildTestPod("p3", 0, 0)
	nsi1 := cr_utils.BuildTestNoScaleUpInfo(p1)
	nsi2 := cr_utils.BuildTestNoScaleUpInfo(p2)
	cr1 := cr_utils.BuildTestCr("cr1", "600m", "0", []cr_types.CapacityRequestConditionType{})
	cr2 := cr_utils.BuildTestCr("cr2", "10m", "0", []cr_types.CapacityRequestConditionType{})
	cr3 := cr_utils.BuildTestCr("cr3", "100m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesAvailable})
	pr1 := buildProvisioningRequest("pr-1", 3)

	testCases := []struct {
		caseName               string
		scaleUpStatus          *status.ScaleUpStatus
		CRsTrigerredScaleUp    []*cr_types.CapacityRequest
		CRsRemainUnschedulable []*cr_types.CapacityRequest
		PRsRemainUnschedulable []*provreqwrapper.ProvisioningRequest
		CRsAwaitEvaluation     []*cr_types.CapacityRequest
		expectedTriggered      int
		expectedNoTriggered    int
		expectedCRs            int
		expectedPRs            int
	}{
		{
			caseName: "cr_NoScaleUp",
			scaleUpStatus: &status.ScaleUpStatus{
				Result:                  status.ScaleUpNoOptionsAvailable,
				ScaleUpInfos:            []nodegroupset.ScaleUpInfo{{}},
				PodsRemainUnschedulable: []status.NoScaleUpInfo{nsi1, nsi2},
			},
			expectedNoTriggered: 2,
		},
		{
			caseName: "cr_scaleUp_onlyOnePodAddressed",
			scaleUpStatus: &status.ScaleUpStatus{
				Result:                  status.ScaleUpSuccessful,
				ScaleUpInfos:            []nodegroupset.ScaleUpInfo{{}},
				PodsTriggeredScaleUp:    []*apiv1.Pod{p3},
				PodsRemainUnschedulable: []status.NoScaleUpInfo{nsi1, nsi2},
			},
			expectedTriggered: 1,
			// NoTriggerScaleUp events delayed
			expectedNoTriggered: 0,
		},
		{
			caseName: "noScaleUpWithCRs",
			scaleUpStatus: &status.ScaleUpStatus{
				Result:                  status.ScaleUpNoOptionsAvailable,
				ScaleUpInfos:            []nodegroupset.ScaleUpInfo{{}},
				PodsRemainUnschedulable: []status.NoScaleUpInfo{nsi1, nsi2},
			},
			CRsRemainUnschedulable: []*cr_types.CapacityRequest{cr2},
			CRsAwaitEvaluation:     []*cr_types.CapacityRequest{cr3},
			expectedNoTriggered:    3,
			expectedCRs:            1,
		},
		{
			caseName: "scaleUpWithCRs",
			scaleUpStatus: &status.ScaleUpStatus{
				Result:                  status.ScaleUpSuccessful,
				ScaleUpInfos:            []nodegroupset.ScaleUpInfo{{}},
				PodsTriggeredScaleUp:    []*apiv1.Pod{p3},
				PodsRemainUnschedulable: []status.NoScaleUpInfo{nsi1, nsi2},
			},
			CRsTrigerredScaleUp:    []*cr_types.CapacityRequest{cr1},
			CRsRemainUnschedulable: []*cr_types.CapacityRequest{cr2},
			CRsAwaitEvaluation:     []*cr_types.CapacityRequest{cr3},
			expectedTriggered:      2,
			// NoTriggerScaleUp events delayed
			expectedNoTriggered: 0,
			expectedCRs:         1,
		},
		{
			caseName: "noScaleUpWithPRs",
			scaleUpStatus: &status.ScaleUpStatus{
				Result:                  status.ScaleUpNoOptionsAvailable,
				ScaleUpInfos:            []nodegroupset.ScaleUpInfo{{}},
				PodsRemainUnschedulable: []status.NoScaleUpInfo{},
			},
			PRsRemainUnschedulable: []*provreqwrapper.ProvisioningRequest{pr1},
			expectedNoTriggered:    1,
			expectedPRs:            1,
		},
		{
			caseName: "noScaleUpWithCRsAndPRs",
			scaleUpStatus: &status.ScaleUpStatus{
				Result:                  status.ScaleUpNoOptionsAvailable,
				ScaleUpInfos:            []nodegroupset.ScaleUpInfo{{}},
				PodsRemainUnschedulable: []status.NoScaleUpInfo{nsi1, nsi2},
			},
			CRsRemainUnschedulable: []*cr_types.CapacityRequest{cr2},
			PRsRemainUnschedulable: []*provreqwrapper.ProvisioningRequest{pr1},
			expectedNoTriggered:    4,
			expectedCRs:            1,
			expectedPRs:            1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			fakeRecorder := kube_record.NewFakeRecorder(10)
			context := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					Recorder: fakeRecorder,
				},
			}
			fakeClient := cr_fake.NewSimpleClientset()
			crs := []*cr_types.CapacityRequest{}
			crs = append(crs, tc.CRsTrigerredScaleUp...)
			crs = append(crs, tc.CRsRemainUnschedulable...)
			crs = append(crs, tc.CRsAwaitEvaluation...)

			crState := utils.NewTestCapacityRequestState(fakeClient, crs)
			tc.scaleUpStatus.PodsTriggeredScaleUp = cr_utils.AppendCRPods(t, tc.scaleUpStatus.PodsTriggeredScaleUp, crState, tc.CRsTrigerredScaleUp)
			tc.scaleUpStatus.PodsRemainUnschedulable = cr_utils.AppendCRNoScaleUpInfos(t, tc.scaleUpStatus.PodsRemainUnschedulable, crState, tc.CRsRemainUnschedulable)
			tc.scaleUpStatus.PodsRemainUnschedulable = appendPRNoScaleUpInfos(t, tc.scaleUpStatus.PodsRemainUnschedulable, tc.PRsRemainUnschedulable)
			tc.scaleUpStatus.PodsAwaitEvaluation = cr_utils.AppendCRPods(t, tc.scaleUpStatus.PodsAwaitEvaluation, crState, tc.CRsAwaitEvaluation)

			p := NewInternalEventingScaleUpStatusProcessor()
			p.EnableCapacityReqProcessing(crState)
			p.EnableProvReqProcessing()
			p.Process(context, tc.scaleUpStatus)

			triggered := 0
			noTriggered := 0
			crEvents := 0
			prEvents := 0
			for eventsLeft := true; eventsLeft; {
				select {
				case event := <-fakeRecorder.Events:
					if strings.Contains(event, "TriggeredScaleUp") {
						triggered += 1
					} else if strings.Contains(event, "NotTriggerScaleUp") {
						noTriggered += 1
					} else {
						t.Fatalf("Unexpected event %v", event)
					}
					if strings.Contains(event, "CapacityRequest") {
						crEvents += 1
					}
					if strings.Contains(event, "ProvisioningRequest") {
						prEvents += 1
					}
				default:
					eventsLeft = false
				}
			}

			assert.Equal(t, tc.expectedTriggered, triggered)
			assert.Equal(t, tc.expectedNoTriggered, noTriggered)
			assert.Equal(t, tc.expectedCRs, crEvents)
			assert.Equal(t, tc.expectedPRs, prEvents)
		})
	}
}

func appendPRNoScaleUpInfos(t *testing.T, noScaleUpInfos []status.NoScaleUpInfo, prs []*provreqwrapper.ProvisioningRequest) []status.NoScaleUpInfo {
	result := []status.NoScaleUpInfo{}
	result = append(result, noScaleUpInfos...)
	for _, pr := range prs {
		prPods, err := pr_pods.PodsForProvisioningRequest(nil, nil, pr)
		assert.NoError(t, err)
		for _, prPod := range prPods {
			result = append(result, cr_utils.BuildTestNoScaleUpInfo(prPod))
		}
	}
	return result
}

func buildProvisioningRequest(name string, podCount int32) *provreqwrapper.ProvisioningRequest {
	return provreqwrapper.NewProvisioningRequest(&prv1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID(fmt.Sprintf("pr/%s", name)),
		},
		Spec: prv1.ProvisioningRequestSpec{
			ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
			PodSets: []prv1.PodSet{{
				Count: podCount,
				PodTemplateRef: prv1.Reference{
					Name: fmt.Sprintf("pt-%s", name),
				},
			},
			},
		},
	}, []*apiv1.PodTemplate{{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("pt-%s", name),
		},
	}})
}
