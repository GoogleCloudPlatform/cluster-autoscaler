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

package pod

import (
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/features"
)

// PodRequests calculates Pod requests using a common resource helper shared with the scheduler
// TODO(b/393348297): replace with CA OSS utils/pod.PodRequests once vendored CA has https://github.com/kubernetes/autoscaler/pull/8049
func PodRequests(pod *v1.Pod) v1.ResourceList {
	inPlacePodVerticalScalingEnabled := utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling)
	podLevelResourcesEnabled := utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)

	return resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{
		UseStatusResources:    inPlacePodVerticalScalingEnabled,
		SkipPodLevelResources: !podLevelResourcesEnabled,
	})
}
