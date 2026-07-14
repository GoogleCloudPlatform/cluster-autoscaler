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

package provreq

import (
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
)

// BuildTestProvisioningRequest creates a new ProvisioningRequest wrapper for testing.
func BuildTestProvisioningRequest(namespace, name string, annotations map[string]string, podSpec apiv1.PodSpec, opts ...Option) *provreqwrapper.ProvisioningRequest {
	pr := &prv1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         types.UID(name),
			Annotations: annotations,
		},
		Spec: prv1.ProvisioningRequestSpec{
			ProvisioningClassName: "queued-provisioning.gke.io",
			PodSets: []prv1.PodSet{
				{
					Count: 1,
					PodTemplateRef: prv1.Reference{
						Name: name + "-template",
					},
				},
			},
		},
	}
	for _, opt := range opts {
		opt(pr)
	}
	wrapper := provreqwrapper.NewProvisioningRequest(pr, []*apiv1.PodTemplate{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name + "-template",
				Namespace: namespace,
			},
			Template: apiv1.PodTemplateSpec{
				Spec: podSpec,
			},
		},
	})
	return wrapper
}
