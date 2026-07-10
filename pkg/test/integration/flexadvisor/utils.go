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
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
)

const (
	// FA returns one instance available, but GCEClient does not limit it
	OneInstanceAvailableMachineType       = "e2-standard-4" // 4cpu, 16gb
	ZeroCapacityRecommendationMachineType = "n2-standard-4" // 4cpu, 16gb
	AvailableMachineType                  = "n1-standard-4" // 4cpu, 16gb
	UnknownAvailabilityMachineType        = "e2-standard-8"
	ZoneA                                 = "us-central1-a"
	ZoneB                                 = "us-central1-b"
	ZoneC                                 = "us-central1-c"
	ZoneF                                 = "us-central1-f"
)

func testCapacityGuidance() []fake.FakeCapacityGuidance {
	return []fake.FakeCapacityGuidance{
		fake.NewFakeCapacityGuidanceForMachineType(ZeroCapacityRecommendationMachineType, 0, 0.5),
		fake.NewFakeCapacityGuidanceForMachineType(AvailableMachineType, 10, 0.5),
		fake.NewFakeCapacityGuidanceForMachineType(OneInstanceAvailableMachineType, 1, 0.5),
	}
}

func annotateNodePoolWithCCCLabel(cccName string, nodePools []*gke_api_beta.NodePool) []*gke_api_beta.NodePool {
	res := make([]*gke_api_beta.NodePool, 0, len(nodePools))
	for _, np := range nodePools {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		if np.Config.Labels == nil {
			np.Config.Labels = make(map[string]string)
		}
		np.Config.Labels["cloud.google.com/compute-class"] = cccName
		res = append(res, np)
	}
	return res
}

// createCCCFromNodePools creates CCC with rules based on the nodePools.
// One CCC priority per node-pool.
func createCCCWithNodePoolsRules(nodePools []string) *v1.ComputeClass {
	var priorities []v1.Priority
	for _, name := range nodePools {
		priorities = append(priorities,
			v1.Priority{Nodepools: []string{name}})
	}
	return ccc.NewComputeClassBuilder("test-ccc").
		WithPriorities(priorities...).
		Build()
}

// The function is not in the OSS K8s test_utils,
// because CCC is GKE specific.
func withTestCCC(pod *apiv1.Pod) {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[gkelabels.ComputeClassLabel] = "test-ccc"
}

func createEmptyNodePool(poolName string, machineType string) *gke_api_beta.NodePool {
	return integration.DefaultNodePool(
		integration.WithNodePoolName(poolName),
		integration.WithNodePoolMachineType(machineType),
		integration.WithNodePoolSize(0),
		integration.WithNodePoolLocations(ZoneB),
	)
}

func stockOutError() cloudprovider.InstanceErrorInfo {
	return cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
		ErrorCode:    "ZONE_RESOURCE_POOL_EXHAUSTED",
		ErrorMessage: "GCE API error: stock out",
	}
}
