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

package daemonsetmutation

import (
	"context"
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podutil "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/client-go/kubernetes"
)

// DryRunResolver creates a fully defaulted and validated Pod using server-side dry-run,
// preserving OwnerReferences from the template.
type DryRunResolver struct {
	client kubernetes.Interface
}

// NewDryRunResolver returns a new instance of DryRunResolver.
func NewDryRunResolver(client kubernetes.Interface) *DryRunResolver {
	return &DryRunResolver{
		client: client,
	}
}

// Resolve builds a Pod based on the provided template, preserving OwnerReferences.
func (r *DryRunResolver) Resolve(ctx context.Context, namespace string, template *apiv1.PodTemplateSpec) (*apiv1.Pod, error) {
	if template == nil {
		return nil, fmt.Errorf("cannot resolve mutation: template is nil")
	}

	pod := podutil.GetPodFromTemplate(template)
	generateName := "ds-mutation-dryrun-"
	pod.OwnerReferences = template.OwnerReferences
	for _, ref := range template.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			generateName = ref.Name + "-"
			break
		}
	}
	pod.GenerateName = generateName

	createdPod, err := r.client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{
		DryRun: []string{metav1.DryRunAll},
	})
	if err != nil {
		return nil, err
	}

	return createdPod, nil
}
