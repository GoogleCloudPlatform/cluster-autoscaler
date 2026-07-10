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
	mcv1 "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/cloud.google.com/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestMachineConfigSourceInfoMetric verifies that the machine_config_source_info
// gauge metric correctly reports the configuration source for each machine family.
// It creates a dynamic MachineConfig CRD for "c4n", runs a full autoscaler loop,
// and asserts that "c4n" is labeled as "dynamic" while built-in families like "n1"
// remain labeled as "hardcoded".
func TestMachineConfigSourceInfoMetric(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithMachineConfigEnabled()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)

		_, err := infra.Fakes.MccClient.CloudV1().MachineConfigs().Create(ctx, &mcv1.MachineConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "c4n",
				ResourceVersion: "1",
			},
			Spec: mcv1.MachineConfigSpec{
				MachineFamily: mcv1.MachineFamily{
					Name: "c4n",
				},
			},
		}, metav1.CreateOptions{})
		assert.NoError(t, err)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		val, err := metrics.GetMachineConfigSourceInfoValueForTest("c4n", "dynamic")
		assert.NoError(t, err)
		assert.Equal(t, float64(1), val)

		val, err = metrics.GetMachineConfigSourceInfoValueForTest("c4n", "hardcoded")
		assert.NoError(t, err)
		assert.Equal(t, float64(0), val)

		val, err = metrics.GetMachineConfigSourceInfoValueForTest("n1", "hardcoded")
		assert.NoError(t, err)
		assert.Equal(t, float64(1), val)

		val, err = metrics.GetMachineConfigSourceInfoValueForTest("n1", "dynamic")
		assert.NoError(t, err)
		assert.Equal(t, float64(0), val)
	})
}
