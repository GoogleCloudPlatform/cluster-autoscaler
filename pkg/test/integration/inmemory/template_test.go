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

package inmemory

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// templateTestAdvancedScenario is a template for writing Big Unit Tests.
// You can copy this function to your test file and rename it to TestYourScenario.
// This function compiles but will not be run automatically by the test runner since it does not start with Test.
func templateTestAdvancedScenario(t *testing.T) {
	testConfig := integration.NewTestConfig(
	// Overrides ...
	)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		stopCh := make(chan struct{})

		// Given: Infrastructure and autoscaler are initialized.
		autoscaler, err := integration.SetupAutoscaler(t, ctx, testConfig, infra, stopCh)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel, stopCh)

		// When: A trigger action occurs (e.g., adding an unschedulable pod) and the autoscaler loop executes.
		pod := tu.BuildTestPod("my-pod", 1000, 1000, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod)
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		// Then: Verify the autoscaler's behavior.
		_, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		// assert.Equal(t, ... )
	})
}
