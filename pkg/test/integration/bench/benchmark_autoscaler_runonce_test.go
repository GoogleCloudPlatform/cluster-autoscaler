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

const (
	maxNGSize = 100
)

var runOnceScaleUpScenario = scenario{
	given: func() *integration.TestConfig {
		return defaultBenchmarkConfig().WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).WithOverrides(
			integration.WithMaxNodesPerScaleUp(maxNGSize))
	},
	when: func(infra *integration.TestInfrastructure) {
		for i := 0; i < 500; i++ {
			podName := fmt.Sprintf("pod-%d", i)
			pod := tu.BuildTestPod(podName, 600, 1000, tu.MarkUnschedulable())
			infra.Fakes.K8s.AddPod(pod)
		}
	},
	then: verifyTargetNumberOfNodes(10),
}

func BenchmarkRunOnceScaleUp(b *testing.B) {
	runOnceScaleUpScenario.run(b)
}

func TestRunOnceScaleUp(t *testing.T) {
	runOnceScaleUpScenario.run(t)
}
