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

package utils

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
)

// PodRequestsAsSize returns the requests of a pod as size.Allocatable.
func PodRequestsAsSize(pod *v1.Pod) size.Allocatable {
	return size.ResourcesToSize(podutils.PodRequests(pod))
}
