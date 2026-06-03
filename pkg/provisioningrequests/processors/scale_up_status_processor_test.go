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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestProvisioningRequestScaleUpStatusProcessor(t *testing.T) {
	processor := NewProvisioningRequestScaleUpStatusProcessor()
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	pr := buildProvisioningRequest("pr-1", 1)
	prPods, _ := pr_pods.PodsForProvisioningRequest(provider, experiments.NewMockManager(), pr)

	initialStatus := &status.ScaleUpStatus{
		PodsRemainUnschedulable: []status.NoScaleUpInfo{
			{Pod: prPods[0]},
			{Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "standard-pod"}}},
		},
	}

	wantStatus := &status.ScaleUpStatus{
		PodsRemainUnschedulable: []status.NoScaleUpInfo{
			{Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "standard-pod"}}},
		},
	}

	processor.Process(nil, initialStatus)
	if diff := cmp.Diff(initialStatus, wantStatus); diff != "" {
		t.Errorf("status diff (-want +got):\n%s", diff)
	}
}

func buildProvisioningRequest(name string, podCount int32) *provreqwrapper.ProvisioningRequest {
	return provreqwrapper.NewProvisioningRequest(&prv1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now()),
			UID:               types.UID(fmt.Sprintf("pr/%s", name)),
		},
		Spec: prv1.ProvisioningRequestSpec{
			ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
			PodSets: []prv1.PodSet{
				{
					Count: podCount,
					PodTemplateRef: prv1.Reference{
						Name: fmt.Sprintf("pt-%s", name),
					},
				},
			},
		},
		Status: prv1.ProvisioningRequestStatus{
			Conditions: []metav1.Condition{
				{Type: prv1.Accepted, Status: metav1.ConditionFalse},
				{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
				{Type: prv1.Failed, Status: metav1.ConditionFalse},
			},
		},
	}, []*apiv1.PodTemplate{{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("pt-%s", name),
		},
		Template: apiv1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"app": "test-app",
				},
			},
			Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{
						Name:    "pi",
						Image:   "perl",
						Command: []string{"/bin/sh"},
						Resources: apiv1.ResourceRequirements{
							Limits: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse("700m"),
								apiv1.ResourceMemory: resource.MustParse("10M"),
							},
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse("700m"),
								apiv1.ResourceMemory: resource.MustParse("10M"),
							},
						},
					},
				},
			},
		},
	}})
}
