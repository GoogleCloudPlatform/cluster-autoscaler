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

package daemonset

import (
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// WithResource sets the request and limit for a specified resource of the first container.
func WithResource(resourceName apiv1.ResourceName, req, lim string) func(*appsv1.DaemonSet) {
	return func(ds *appsv1.DaemonSet) {
		if len(ds.Spec.Template.Spec.Containers) > 0 {
			if ds.Spec.Template.Spec.Containers[0].Resources.Requests == nil {
				ds.Spec.Template.Spec.Containers[0].Resources.Requests = make(apiv1.ResourceList)
			}
			if ds.Spec.Template.Spec.Containers[0].Resources.Limits == nil {
				ds.Spec.Template.Spec.Containers[0].Resources.Limits = make(apiv1.ResourceList)
			}
			ds.Spec.Template.Spec.Containers[0].Resources.Requests[resourceName] = resource.MustParse(req)
			ds.Spec.Template.Spec.Containers[0].Resources.Limits[resourceName] = resource.MustParse(lim)
		}
	}
}
