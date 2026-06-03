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

package validator

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/validator/conditions"
)

func TestValidatorWithUpdatesChannel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testCrdLabel := "test-crd"
		defaultGkeManager := &gke.GkeManagerMock{}
		defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)

		testCrd := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
			crd.WithName("crd-object-1"),
			crd.WithRules([]rules.Rule{
				rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
			}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled())

		// No migs -> should generate condition
		mockCrdLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
		mockCrdLister.SetCrdLabel(testCrdLabel)
		provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
			WithAutoprovisioningDefaultFamily(machinetypes.E2).
			WithAutoprovisioningEnabled(true).
			WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
			Build()

		updatesCh := make(chan status.UpdateMessage, 10)
		validator, _ := NewValidator(nil, mockCrdLister, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, updatesCh, true)

		// Execute loop
		go validator.loop()
		synctest.Wait()

		// Expect message on channel
		select {
		case msg := <-updatesCh:
			assert.Equal(t, "crd-object-1", msg.Id.CRDName)
			assert.Equal(t, testCrdLabel, msg.Id.CRDLabel)
		default:
			t.Fatal("Expected update message, got none")
		}
	})
}

func TestLoopClearsStaleConditions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testCrdLabel := "test-crd"
		defaultGkeManager := &gke.GkeManagerMock{}
		defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)

		// CRD is healthy in cache (no misconfigurations)
		testCrd := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
			crd.WithName("crd-object-1"),
			crd.WithRules([]rules.Rule{}),
			crd.WithScaleUpAnyway(),
			crd.WithAutoprovisioningEnabled())

		mockCrdLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
		mockCrdLister.SetCrdLabel(testCrdLabel)
		provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
			WithAutoprovisioningDefaultFamily(machinetypes.E2).
			WithAutoprovisioningEnabled(true).
			WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
			Build()

		updatesCh := make(chan status.UpdateMessage, 10)
		validator, _ := NewValidator(nil, mockCrdLister, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, updatesCh, true)

		// Execute loop
		go validator.loop()
		synctest.Wait()

		// Expect message on channel because health condition changed to Healthy
		select {
		case msg := <-updatesCh:
			assert.Equal(t, "crd-object-1", msg.Id.CRDName)

			// Simulate aggregator state having:
			// 1. An "other component" condition (MinCapacityProvisioning)
			// 2. A stale validator condition (CrdMisconfigured) from a previous run
			// 3. An old Health condition
			aggregatorConditions := []metav1.Condition{
				{
					Type:               status.MinCapacityProvisioning,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             status.ProvisioningStarted,
				},
				{
					Type:               conditions.CrdMisconfiguredCondition,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             "SomeOldError",
				},
				{
					Type:               conditions.HealthCondition,
					Status:             metav1.ConditionFalse,
					LastTransitionTime: metav1.Now(),
					Reason:             conditions.HealthCondition,
				},
			}

			mockStatus := &crd.MockCRDStatus{}
			mockStatus.On("GetConditions").Return(aggregatorConditions)
			mockStatus.On("UpdateConditions", mock.Anything).Return()

			msg.Mutate(mockStatus)

			// Verify that UpdateConditions was called
			mockStatus.AssertCalled(t, "UpdateConditions", mock.Anything)

			// Extract the conditions passed to UpdateConditions
			call := mockStatus.Calls[len(mockStatus.Calls)-1]
			assert.Equal(t, "UpdateConditions", call.Method)
			gotConds := call.Arguments.Get(0).([]metav1.Condition)

			// We expect:
			// 1. MinCapacityProvisioning to be preserved (Type: MinCapacityProvisioning)
			// 2. Stale CrdMisconfigured to be REMOVED (Type: CrdMisconfigured)
			// 3. Health to be updated to True (Type: Health)

			hasMinCap := false
			hasCrdMisconfigured := false
			hasHealthTrue := false

			for _, c := range gotConds {
				switch c.Type {
				case status.MinCapacityProvisioning:
					hasMinCap = true
				case conditions.CrdMisconfiguredCondition:
					hasCrdMisconfigured = true
				case conditions.HealthCondition:
					if c.Status == metav1.ConditionTrue {
						hasHealthTrue = true
					}
				}
			}

			assert.True(t, hasMinCap, "Expected MinCapacityProvisioning to be preserved")
			assert.False(t, hasCrdMisconfigured, "Expected stale CrdMisconfigured to be cleared")
			assert.True(t, hasHealthTrue, "Expected Health condition to be updated to True")

		default:
			t.Fatal("Expected update message, got none")
		}
	})
}

func TestLoopHealthWithOtherComponentConditions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testCrdLabel := "test-crd"
		defaultGkeManager := &gke.GkeManagerMock{}
		defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)

		// Cache has MinCapacityProvisioning with Status: True (active provisioning)
		cacheConditions := []metav1.Condition{
			{
				Type:               status.MinCapacityProvisioning,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				Reason:             status.ProvisioningStarted,
			},
		}

		testCrd := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
			crd.WithName("crd-object-1"),
			crd.WithRules([]rules.Rule{}),
			crd.WithScaleUpAnyway(),
			crd.WithAutoprovisioningEnabled(),
			crd.WithConditions(cacheConditions))

		mockCrdLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
		mockCrdLister.SetCrdLabel(testCrdLabel)
		provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
			WithAutoprovisioningDefaultFamily(machinetypes.E2).
			WithAutoprovisioningEnabled(true).
			WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
			Build()

		updatesCh := make(chan status.UpdateMessage, 10)
		validator, _ := NewValidator(nil, mockCrdLister, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, updatesCh, true)

		// Execute loop
		go validator.loop()
		synctest.Wait()

		// Expect message on channel
		select {
		case msg := <-updatesCh:
			mockStatus := &crd.MockCRDStatus{}
			mockStatus.On("GetConditions").Return([]metav1.Condition{})
			mockStatus.On("UpdateConditions", mock.Anything).Return()

			msg.Mutate(mockStatus)

			call := mockStatus.Calls[len(mockStatus.Calls)-1]
			gotConds := call.Arguments.Get(0).([]metav1.Condition)

			// We expect the CRD to remain HEALTHY (Health: True) because
			// MinCapacityProvisioning: True is NOT an error condition!
			hasHealthTrue := false
			for _, c := range gotConds {
				if c.Type == conditions.HealthCondition && c.Status == metav1.ConditionTrue {
					hasHealthTrue = true
				}
			}
			assert.True(t, hasHealthTrue, "CRD should be Healthy even if MinCapacityProvisioning is True")

		default:
			t.Fatal("Expected update message, got none")
		}
	})
}
