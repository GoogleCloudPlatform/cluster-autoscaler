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

package flexadvisor

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

const (
	// FA returns one instance available, but GCEClient does not limit it
	OneInstanceAvailableMachineType       = "e2-standard-4" // 4cpu, 16gb
	ZeroCapacityRecommendationMachineType = "n2-standard-4" // 4cpu, 16gb
	StockOutMachineType                   = "n2-standard-4" // 4cpu, 16gb
	AvailableMachineType                  = "n1-standard-4" // 4cpu, 16gb
	UnknownAvailabilityMachineType        = "e2-standard-8"
	ZoneA                                 = "us-central1-a"
	ZoneB                                 = "us-central1-b"
	ZoneC                                 = "us-central1-c"
	ZoneF                                 = "us-central1-f"
)

func stockOutError() cloudprovider.InstanceErrorInfo {
	return cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
		ErrorCode:    "ZONE_RESOURCE_POOL_EXHAUSTED",
		ErrorMessage: "GCE API error: stock out",
	}
}
