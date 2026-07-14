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
	"k8s.io/apimachinery/pkg/types"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
)

// Option is a functional option for configuring a ProvisioningRequest.
type Option func(*prv1.ProvisioningRequest)

// WithUID sets the UID of the ProvisioningRequest.
func WithUID(uid types.UID) Option {
	return func(pr *prv1.ProvisioningRequest) {
		pr.UID = uid
	}
}

// WithPodCount sets the pod count for the first PodSet in the ProvisioningRequest.
func WithPodCount(count int32) Option {
	return func(pr *prv1.ProvisioningRequest) {
		if len(pr.Spec.PodSets) > 0 {
			pr.Spec.PodSets[0].Count = count
		}
	}
}
