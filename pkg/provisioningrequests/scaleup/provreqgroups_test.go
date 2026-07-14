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

package scaleup

import (
	"fmt"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/kubernetes/pkg/controller"
)

func prPodTemplate(i int) *v1.PodTemplate {
	return &v1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("pod-template-name-%d", i),
			Namespace: "test-ns",
		},
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  fmt.Sprintf("test-container-%d", i),
						Image: fmt.Sprintf("test-image-%d", i),
					},
				},
			},
		},
	}
}

func TestBuildProvReqGroups(t *testing.T) {
	ts1 := time.Date(2023, 12, 3, 0, 0, 0, 0, time.UTC) // Dec 3rd
	pods1, err := provReqPods("pr1", ts1, 1, 3)
	if err != nil {
		t.Fatalf("failed to create test pods: %v", err)
	}

	ts2 := time.Date(2023, 12, 2, 0, 0, 0, 0, time.UTC) // Dec 2nd
	pods2, err := provReqPods("pr2", ts2, 2, 2)
	if err != nil {
		t.Fatalf("failed to create test pods: %v", err)
	}

	ts3 := time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC) // Dec 1st
	pods3, err := provReqPods("pr3", ts3, 7, 1)
	if err != nil {
		t.Fatalf("failed to create test pods: %v", err)
	}

	pods4, err := nonProvReqPods(4)
	if err != nil {
		t.Fatalf("failed to create test pods: %v", err)
	}

	tests := []struct {
		name                    string
		podEquivalenceGroups    []*equivalence.PodGroup
		expectProvReqGroupCount int
	}{
		{
			name:                    "standard case with 3 ProvReqs in a Pod shard",
			podEquivalenceGroups:    equivalence.BuildPodGroups(append(append(pods1, pods2...), pods3...)),
			expectProvReqGroupCount: 3,
		},
		{
			name:                    "ProvReqs that are already sorted by creation timestamps",
			podEquivalenceGroups:    equivalence.BuildPodGroups(append(append(pods3, pods2...), pods1...)),
			expectProvReqGroupCount: 3,
		},
		{
			name:                    "single ProvReq in a Pod shard",
			podEquivalenceGroups:    equivalence.BuildPodGroups(pods1),
			expectProvReqGroupCount: 1,
		},
		{
			name:                    "Pods not owned by a ProvReq in a Pod shard",
			podEquivalenceGroups:    equivalence.BuildPodGroups(append(pods1, pods4...)),
			expectProvReqGroupCount: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provReqGroups := buildProvReqGroups(tt.podEquivalenceGroups)
			if len(provReqGroups) != tt.expectProvReqGroupCount {
				t.Fatalf("unexpected length of provReqGroups (got: %v, want: %v)", len(provReqGroups), tt.expectProvReqGroupCount)
			}
			// Check if provReqGroups are sorted in ascending order w.r.t. the ProvReq's creation time.
			for i := range provReqGroups {
				if i+1 == len(provReqGroups) {
					break
				}
				currentTS := provReqGroups[i].PodGroups[0].Pods[0].CreationTimestamp
				nextTS := provReqGroups[i+1].PodGroups[0].Pods[0].CreationTimestamp
				if currentTS.After(nextTS.Time) {
					t.Errorf("unsorted provReqGroups (provReqGroups[%v].Pods[0].CreationTimestamp = %v, provReqGroups[%v].Pods[0].CreationTimestamp = %v)", i, currentTS, i+1, nextTS)
				}
			}
		})
	}
}

func provReqPods(prName string, creationTS time.Time, podSets int, replicasPerPodSet int) ([]*v1.Pod, error) {
	pr := &prv1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:              prName,
			Namespace:         "test-ns",
			CreationTimestamp: metav1.NewTime(creationTS),
			UID:               types.UID(fmt.Sprintf("pr/%s", prName)),
		},
		Spec: prv1.ProvisioningRequestSpec{
			ProvisioningClassName: "queued-provisioning.gke.io",
			PodSets:               []prv1.PodSet{},
		},
	}

	templates := []*v1.PodTemplate{}
	for i := range podSets {
		pt := prPodTemplate(i)
		pr.Spec.PodSets = append(pr.Spec.PodSets,
			prv1.PodSet{
				Count: int32(replicasPerPodSet),
				PodTemplateRef: prv1.Reference{
					Name: pt.Name,
				},
			})
		templates = append(templates, pt)
	}

	return pods.PodsForProvisioningRequest(nil, nil, provreqwrapper.NewProvisioningRequest(pr, templates))
}

// Create a couple of Job-owned Pods to exercise the unhappy path of the logic.
func nonProvReqPods(count int) ([]*v1.Pod, error) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "test-ns",
			UID:       types.UID("job/test-job"),
		},
	}
	pods := make([]*v1.Pod, 0)
	for i := 0; i < count; i++ {
		pod, err := controller.GetPodFromTemplate(
			&v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "test-container", Image: "test-image"}}}}, job, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create pod from template: %w", err)
		}
		pods = append(pods, pod)
	}
	return pods, nil
}
