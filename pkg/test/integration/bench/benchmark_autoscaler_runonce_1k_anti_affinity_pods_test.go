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

package bench

import (
	"fmt"
	"testing"

	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
)

var (
	labels   = map[string]string{"app": "aa-bench"}
	num_pods = 1000
)

var runOnceScaleUp1kAntiAffinityPodsScenario = scenario{
	given: func() *integration.TestConfig {
		return defaultBenchmarkConfig().WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled())
	},
	when: func(infra *integration.TestInfrastructure) {
		for i := 0; i < num_pods; i++ {
			podName := fmt.Sprintf("pod-%d", i)
			pod := tu.BuildTestPod(
				podName, 600, 1000,
				tu.MarkUnschedulable(),
				tu.WithLabels(labels),
				tu.WithPodHostnameAntiAffinity(labels),
			)
			infra.Fakes.K8s.AddPod(pod)
		}
	},
	then: verifyTargetNumberOfNodes(1000),
}

func BenchmarkRunOnceScaleUp1kAntiAffinityPods(b *testing.B) {
	runOnceScaleUp1kAntiAffinityPodsScenario.run(b)
}

func TestRunOnceScaleUp1kAntiAffinityPods(t *testing.T) {
	runOnceScaleUp1kAntiAffinityPodsScenario.run(t)
}
