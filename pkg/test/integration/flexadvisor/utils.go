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
	"context"
	"testing"
	"time"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
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

// primeFlexAdvisorCache primes FlexAdvisor cache by adding a temporary pod that triggers registration but cannot schedule.
func primeFlexAdvisorCache(ctx context.Context, t *testing.T, autoscaler core.Autoscaler, infra *integration.TestInfrastructure, cccName string) {
	t.Helper()
	tmpPod := tu.BuildTestPod("temporary-pod", 100, 100, pod.WithCCC(cccName), tu.MarkUnschedulable())
	// Add an invalid node selector to ensure it doesn't match any node group.
	tmpPod.Spec.NodeSelector["invalid-selector"] = "true"
	infra.Fakes.K8s.AddPod(tmpPod)

	// Run one cycle to trigger registration and background fetch.
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

	// Delete temporary pod.
	infra.Fakes.K8s.DeletePod(tmpPod.Namespace, tmpPod.Name)
}
