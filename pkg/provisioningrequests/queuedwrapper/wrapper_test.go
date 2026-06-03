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

package queuedwrapper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
)

func TestQueuedProvisioningRequestWrapper(t *testing.T) {
	creationTimestamp := metav1.NewTime(time.Date(2023, 11, 12, 13, 14, 15, 0, time.UTC))
	conditions := []metav1.Condition{
		{
			LastTransitionTime: metav1.NewTime(time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC)),
			Message:            "Message",
			ObservedGeneration: 1,
			Reason:             "Reason",
			Status:             "Status",
			Type:               "ConditionType",
		},
	}
	podSets := []provreqwrapper.PodSet{
		{
			Count: 1,
			PodTemplate: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:  "test-container",
							Image: "test-image",
						},
					},
				},
			},
		},
	}

	podTemplates := []*apiv1.PodTemplate{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "name-pod-template-v1",
				Namespace:         "namespace-v1",
				CreationTimestamp: creationTimestamp,
			},
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:  "test-container",
							Image: "test-image",
						},
					},
				},
			},
		},
	}
	v1PR := &v1.ProvisioningRequest{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1-api",
			Kind:       "v1-kind",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "name-v1",
			Namespace:         "namespace-v1",
			CreationTimestamp: creationTimestamp,
			UID:               types.UID("v1-uid"),
			Generation:        17,
		},
		Spec: v1.ProvisioningRequestSpec{
			ProvisioningClassName: "queued-provisioning.gke.io",
			Parameters: map[string]v1.Parameter{
				MaxRunDurationSecondsKey:  "3600",
				CapacitySearchStrategyKey: CapacitySearchStrategyObtainability,
			},
			PodSets: []v1.PodSet{
				{
					Count: 1,
					PodTemplateRef: v1.Reference{
						Name: podTemplates[0].Name,
					},
				},
			},
		},
		Status: v1.ProvisioningRequestStatus{
			Conditions:               conditions,
			ProvisioningClassDetails: map[string]v1.Detail{},
		},
	}

	wrappedV1PR := ToQueuedProvisioningRequest(*provreqwrapper.NewProvisioningRequest(v1PR, podTemplates))

	// Check Name, Namespace and Creation accessors
	assert.Equal(t, "name-v1", wrappedV1PR.Name)
	assert.Equal(t, "namespace-v1", wrappedV1PR.Namespace)
	assert.Equal(t, creationTimestamp, wrappedV1PR.CreationTimestamp)

	// Check APIVersion, Kind and UID accessors
	assert.Equal(t, "v1-api", wrappedV1PR.APIVersion)
	assert.Equal(t, "v1-kind", wrappedV1PR.Kind)
	assert.Equal(t, types.UID("v1-uid"), wrappedV1PR.UID)
	assert.Equal(t, int64(17), wrappedV1PR.Generation)

	// Check the initial provisioning class details
	assert.Nil(t, wrappedV1PR.ResizeRequestName())
	assert.Nil(t, wrappedV1PR.NodeGroupName())
	assert.Nil(t, wrappedV1PR.NodePoolName())
	assert.Nil(t, wrappedV1PR.AcceleratorType())
	assert.Nil(t, wrappedV1PR.SelectedZone())
	assert.Nil(t, wrappedV1PR.NodePoolAutoProvisioned())
	assert.Nil(t, wrappedV1PR.PodTemplateName())
	assert.Nil(t, wrappedV1PR.ProvisioningMode())

	// Set provisioning class details and check the values
	wrappedV1PR.SetProvisioningClassDetails(&ProvisioningClassDetails{
		NodeGroupName:           "node-group-name-v1",
		ResizeRequestName:       "resize-request-name-v1",
		NodePoolName:            "node-pool-name-v1",
		AcceleratorType:         "gpu-type",
		SelectedZone:            "us-central1-a",
		NodePoolAutoProvisioned: AutoprovisionedStatusFromBool(false),
		PodTemplateName:         "pod-template-name-v1",
		ProvisioningMode:        ProvisioningModeResizeRequest,
		CommittedZones:          []string{"us-central1-a", "us-central1-b"},
		OverprovisionedZones:    []string{"us-central1-a", "us-central1-b"},
	})
	assert.Equal(t, strPtr("resize-request-name-v1"), wrappedV1PR.ResizeRequestName())
	assert.Equal(t, strPtr("node-group-name-v1"), wrappedV1PR.NodeGroupName())
	assert.Equal(t, strPtr("node-pool-name-v1"), wrappedV1PR.NodePoolName())
	assert.Equal(t, strPtr("gpu-type"), wrappedV1PR.AcceleratorType())
	assert.Equal(t, strPtr("us-central1-a"), wrappedV1PR.SelectedZone())
	assert.Equal(t, strPtr("false"), wrappedV1PR.NodePoolAutoProvisioned())
	assert.Equal(t, strPtr("pod-template-name-v1"), wrappedV1PR.PodTemplateName())
	assert.Equal(t, strPtr("resize_request"), wrappedV1PR.ProvisioningMode())
	assert.Equal(t, strPtr("us-central1-a,us-central1-b"), wrappedV1PR.CommittedZones())
	assert.Nil(t, wrappedV1PR.CommittedNodeGroups())
	assert.Equal(t, strPtr("us-central1-a,us-central1-b"), wrappedV1PR.OverprovisionedZones())

	// Overwrite/fill some values
	wrappedV1PR.SetProvisioningClassDetails(&ProvisioningClassDetails{
		NodeGroupName:           "node-group-name-v2",
		ResizeRequestName:       "resize-request-name-v2",
		CommittedZones:          []string{"us-central1-c", "us-central1-d"},
		CommittedNodeGroups:     []string{"node-group-name-v1", "node-group-name-v2"},
		OverprovisionedZones:    []string{"us-central1-e", "us-central1-f"},
		NodePoolAutoProvisioned: AutoprovisionedStatusFromBool(true),
	})
	// These should have new values
	assert.Equal(t, strPtr("node-group-name-v2"), wrappedV1PR.NodeGroupName())
	assert.Equal(t, strPtr("resize-request-name-v2"), wrappedV1PR.ResizeRequestName())
	assert.Equal(t, strPtr("us-central1-c,us-central1-d"), wrappedV1PR.CommittedZones())
	assert.Equal(t, strPtr("node-group-name-v1,node-group-name-v2"), wrappedV1PR.CommittedNodeGroups())
	assert.Equal(t, strPtr("us-central1-e,us-central1-f"), wrappedV1PR.OverprovisionedZones())
	assert.Equal(t, strPtr("true"), wrappedV1PR.NodePoolAutoProvisioned())
	// These should not be changed
	assert.Equal(t, strPtr("node-pool-name-v1"), wrappedV1PR.NodePoolName())
	assert.Equal(t, strPtr("gpu-type"), wrappedV1PR.AcceleratorType())
	assert.Equal(t, strPtr("us-central1-a"), wrappedV1PR.SelectedZone())
	assert.Equal(t, strPtr("pod-template-name-v1"), wrappedV1PR.PodTemplateName())
	assert.Equal(t, strPtr("resize_request"), wrappedV1PR.ProvisioningMode())

	// Clear provisioning class details and check the values
	wrappedV1PR.ClearProvisioningClassDetails()
	assert.Nil(t, wrappedV1PR.ResizeRequestName())
	assert.Nil(t, wrappedV1PR.NodeGroupName())
	assert.Nil(t, wrappedV1PR.NodePoolName())
	assert.Nil(t, wrappedV1PR.AcceleratorType())
	assert.Nil(t, wrappedV1PR.SelectedZone())
	assert.Nil(t, wrappedV1PR.NodePoolAutoProvisioned())
	assert.Nil(t, wrappedV1PR.PodTemplateName())
	assert.Nil(t, wrappedV1PR.ProvisioningMode())
	assert.Nil(t, wrappedV1PR.CommittedZones())
	assert.Nil(t, wrappedV1PR.CommittedNodeGroups())
	assert.Nil(t, wrappedV1PR.OverprovisionedZones())

	// Check the initial conditions
	assert.Equal(t, conditions, wrappedV1PR.Status.Conditions)

	// Clear conditions and check the values
	wrappedV1PR.Status.Conditions = nil
	assert.Nil(t, wrappedV1PR.Status.Conditions)

	// Set conditions and check the values
	wrappedV1PR.Status.Conditions = conditions
	assert.Equal(t, conditions, wrappedV1PR.Status.Conditions)

	// Check the PodSets
	v1PodSets, v1Err := wrappedV1PR.PodSets()
	assert.Nil(t, v1Err)
	assert.Equal(t, podSets, v1PodSets)

	// Check CapacitySearchStrategy
	assert.True(t, wrappedV1PR.ObtainabilityStrategy())

	// Check the MaxRunDuration
	v1MaxRunDuration, v1Err := wrappedV1PR.MaxRunDuration()
	assert.Nil(t, v1Err)
	assert.NotNil(t, v1MaxRunDuration)
	assert.Equal(t, time.Hour, *v1MaxRunDuration)

	// Check the type accessors
	assert.Equal(t, v1PR, wrappedV1PR.ProvisioningRequest.ProvisioningRequest)
	assert.Equal(t, podTemplates, wrappedV1PR.PodTemplates)

	// Check case where the Provisioning Request is missing Pod Templates.
	wrappedV1PRMissingPodTemplates := provreqwrapper.NewProvisioningRequest(v1PR, nil)
	podSets, err := wrappedV1PRMissingPodTemplates.PodSets()
	assert.Nil(t, podSets)
	assert.EqualError(t, err, "missing pod templates, 1 pod templates were referenced, 1 templates were missing: name-pod-template-v1")

}

func strPtr(s string) *string {
	return &s
}

func TestQueuedProvisioningRequests(t *testing.T) {
	queuedPr := provreqclient.ProvisioningRequestWrapperForTesting("namespace", "name-1")
	queuedPr.Spec.ProvisioningClassName = QueuedProvisioningClassName
	testCases := []struct {
		name     string
		provreqs []*provreqwrapper.ProvisioningRequest
		want     []*provreqwrapper.ProvisioningRequest
	}{
		{
			name:     "empty list",
			provreqs: []*provreqwrapper.ProvisioningRequest{},
			want:     []*provreqwrapper.ProvisioningRequest{},
		},
		{
			name:     "no Queued class",
			provreqs: []*provreqwrapper.ProvisioningRequest{provreqclient.ProvisioningRequestWrapperForTesting("namespace", "name-1")},
			want:     []*provreqwrapper.ProvisioningRequest{},
		},
		{
			name:     "one Queued ProvisioningRequest",
			provreqs: []*provreqwrapper.ProvisioningRequest{provreqclient.ProvisioningRequestWrapperForTesting("namespace", "name-1"), queuedPr},
			want:     []*provreqwrapper.ProvisioningRequest{queuedPr},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := QueuedProvisioningRequests(tc.provreqs)
			assert.ElementsMatch(t, got, tc.want)
		})
	}
}
