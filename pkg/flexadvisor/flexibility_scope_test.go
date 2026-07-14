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
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/utils/ptr"
)

func TestDoApiCallAndUpdateScope(t *testing.T) {
	oldAvailability1 := api.NewTestInstanceAvailabilityBuilder("scope-1", "InstanceConfig-key-1").WithZonalInstanceCount(map[string]int{"us-central1-a": 10}).Build()
	oldAvailability2 := api.NewTestInstanceAvailabilityBuilder("scope-1", "InstanceConfig-key-2").WithZonalInstanceCount(map[string]int{"us-central1-a": 30}).Build()
	newAvailability1 := api.NewTestInstanceAvailabilityBuilder("scope-1", "InstanceConfig-key-1").WithZonalInstanceCount(map[string]int{"us-central1-a": 20}).Build()
	newAvailability2 := api.NewTestInstanceAvailabilityBuilder("scope-1", "InstanceConfig-key-2").WithZonalInstanceCount(map[string]int{"us-central1-b": 5}).Build()
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scope-1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-2"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	testCases := []struct {
		name                          string
		initialSetup                  func(*flexAdvisor, *mockAdviceProvider)
		want                          map[string]*instanceavailability.Snapshot
		wantRemovedInstanceConfigKeys []string
	}{
		{
			name: "Successful API call updates existing and adds new",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				mockApiResponseFirst := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": oldAvailability1,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponseFirst, nil).Once()
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)

				mockApiResponseSecond := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": newAvailability1,
					"InstanceConfig-key-2": newAvailability2,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponseSecond, nil)
			},
			want: map[string]*instanceavailability.Snapshot{
				"InstanceConfig-key-1": newAvailability1.NewSnapshot(),
				"InstanceConfig-key-2": newAvailability2.NewSnapshot(),
			},
		},
		{
			name: "API call returns an error, initial state is preserved",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				mockApiResponseFirst := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": oldAvailability1,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponseFirst, nil).Once()
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)

				p.On("FetchCapacityGuidance").Return(nil, fmt.Errorf("api call error"))
			},
			want: map[string]*instanceavailability.Snapshot{
				"InstanceConfig-key-1": oldAvailability1.NewSnapshot(),
			},
		},
		{
			name: "API call removes an old config by not returning it",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				mockApiResponseFirst := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": oldAvailability1,
					"InstanceConfig-key-2": oldAvailability2,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponseFirst, nil).Once()
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)

				mockApiResponseSecond := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": newAvailability1,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponseSecond, nil)
			},
			want: map[string]*instanceavailability.Snapshot{
				"InstanceConfig-key-1": newAvailability1.NewSnapshot(),
			},
			wantRemovedInstanceConfigKeys: []string{"InstanceConfig-key-2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mockProvider := &mockAdviceProvider{}
				clock := newCustomFakeClock()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				instanceConfigCloudProvider := newMockInstanceConfigCloudProvider([]string{"us-west1-a", "us-west1-b", "us-west1-c"}, nil, machinetypes.E2, true, nil)
				optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, nil, withClock(clock))
				assert.NoError(t, err)

				tc.initialSetup(fa, mockProvider)
				// 1 waiter for flexibility scope refresh, 1 waiter for removing expired flexibility scopes
				err = clock.waitForClockWaiters(2)
				assert.NoError(t, err)

				// trigger the api call to refresh cache
				clock.Step(11 * time.Second)

				// wait for refresh to finish
				err = clock.waitForClockWaiters(2)
				assert.NoError(t, err)

				for instanceConfigKey, wantSnapshot := range tc.want {
					got := fa.GetInstanceAvailability("scope-1", instanceConfigKey)
					assert.NotNil(t, got)
					got.SetProvider(nil)
					assert.Equal(t, *wantSnapshot, *got)
				}

				for _, removedKey := range tc.wantRemovedInstanceConfigKeys {
					got, err := fa.AwaitInstanceAvailability("scope-1", removedKey)
					assert.Nil(t, got)
					assert.Equal(t, fmt.Errorf("instanceConfigKey=%s not present in availability data after refresh, flexibilityScopeKey=scope-1, lastRefreshErr=<nil>, cccState=, cccUsesScaleUpAnyway=false, keyGenerationState=not_generated", removedKey), err)
				}
			})
		})
	}
}

func TestCappedKeysMap_ScopeIsUpdatedWhenCrdChanges(t *testing.T) {
	maxInstanceConfigs := 3
	initializeTest := func(ctx context.Context, clock *customFakeClock) (*lister.MockCrdLister, *flexAdvisor) {
		mockProvider := &mockAdviceProvider{}
		mockLister := lister.NewMockCrdLister([]crd.CRD{ccc.NewCccCrd(&v1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scope-1",
			},
			Spec: v1.ComputeClassSpec{
				Priorities: []v1.Priority{
					{MachineType: ptr.To("e2-standard-2"), Spot: ptr.To(true)},
					{MachineType: ptr.To("e2-standard-4"), Spot: ptr.To(true)},
					{MachineType: ptr.To("e2-standard-8"), Spot: ptr.To(true)},
					{MachineType: ptr.To("e2-standard-16"), Spot: ptr.To(true)},
					{MachineType: ptr.To("e2-standard-32"), Spot: ptr.To(true)},
				},
			},
		}, "", false, crd.TestDefaultDataProvider(), nil),
		})

		instanceConfigCloudProvider := newMockInstanceConfigCloudProvider([]string{"us-central1-a"}, nil, machinetypes.E2, true, nil)
		optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
		instanceConfigGenerator := NewInstanceConfigGenerator(ctx, mockLister, instanceConfigCloudProvider, optionsTracker, WithMaxInstanceConfigs(maxInstanceConfigs))

		fa, err := NewFlexAdvisor(ctx,
			mockProvider,
			mockLister,
			instanceConfigCloudProvider,
			optionsTracker,
			nil,
			withClock(clock),
			withInstanceConfigGenerator(instanceConfigGenerator),
		)
		assert.NoError(t, err)

		mockProvider.On("FetchCapacityGuidance").Return(map[string]*api.InstanceAvailability{}, nil).Twice()

		fa.RegisterFlexibilityScope("scope-1")
		return mockLister, fa
	}

	// waitForWorkers pauses test until given amount of go routines are waiting for the clock to proceed
	waitForWorkers := func(clock *customFakeClock) {
		// we expect 2 workers: one for flexibility scope refresh, one for removing expired flexibility scopes
		err := clock.waitForClockWaiters(2)
		assert.NoError(t, err)
	}

	t.Run("CCC changes - cappedKeysMap is updated", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			clock := newCustomFakeClock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mockLister, fa := initializeTest(ctx, clock)

			waitForWorkers(clock)

			scope, ok, _ := fa.getScope("scope-1")
			assert.True(t, ok)

			assert.Equal(t, map[string]bool{
				api.NewInstanceConfig("e2-standard-2", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature():  false,
				api.NewInstanceConfig("e2-standard-4", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature():  false,
				api.NewInstanceConfig("e2-standard-8", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature():  false,
				api.NewInstanceConfig("e2-standard-16", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature(): true,
				api.NewInstanceConfig("e2-standard-32", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature(): true,
			}, scope.cappedKeysMap)

			// update crd to one with less machine types - should update the capped keys map
			mockLister.SetCrds([]crd.CRD{ccc.NewCccCrd(&v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "scope-1",
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{MachineType: ptr.To("e2-standard-2"), Spot: ptr.To(true)},
						{MachineType: ptr.To("e2-standard-4"), Spot: ptr.To(true)},
						{MachineType: ptr.To("e2-standard-8")},
						{MachineType: ptr.To("e2-standard-16"), Spot: ptr.To(true)},
					},
				},
			}, "", false, crd.TestDefaultDataProvider(), nil),
			})

			clock.Step(11 * time.Second)
			waitForWorkers(clock)

			scope, ok, _ = fa.getScope("scope-1")
			assert.True(t, ok)

			// cappedKeysMap has less keys to reflect updated CCC
			assert.Equal(t, map[string]bool{
				api.NewInstanceConfig("e2-standard-2", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature():     false,
				api.NewInstanceConfig("e2-standard-4", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature():     false,
				api.NewInstanceConfig("e2-standard-8", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature():     false,
				api.NewInstanceConfig("e2-standard-8", "", 0, 0, instanceavailability.Standard, api.EmptyMaxRunDuration).Signature(): true,
				api.NewInstanceConfig("e2-standard-16", "", 0, 0, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature():    true,
			}, scope.cappedKeysMap)
		})
	})
}

var registerOnce sync.Once

func TestResponseValidation_Metrics(t *testing.T) {
	registerOnce.Do(metrics.RegisterAll)
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scope-1",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-2"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)

	configKeyStandard := api.NewInstanceConfig("e2-standard-2", "", 0, 1, instanceavailability.Standard, api.EmptyMaxRunDuration).Signature()
	configKeySpot := api.NewInstanceConfig("e2-standard-2", "", 0, 1, instanceavailability.Spot, api.EmptyMaxRunDuration).Signature()

	testCases := []struct {
		name                 string
		mockCapacityGuidance map[string]*api.InstanceAvailability
		reason               metrics.FAResponseErrorReason
		expectedInc          int
	}{
		{
			name:                 "Backend response missing requested instance configuration",
			mockCapacityGuidance: map[string]*api.InstanceAvailability{},
			reason:               metrics.ResponseMissingInstanceConfig,
			expectedInc:          2,
		},
		{
			name: "Backend response missing requested zone",
			mockCapacityGuidance: map[string]*api.InstanceAvailability{
				configKeyStandard: api.NewTestInstanceAvailabilityBuilder("scope-1", configKeyStandard).
					WithZonalInstanceCount(map[string]int{"us-west1-b": 10}).
					Build(),
				configKeySpot: api.NewTestInstanceAvailabilityBuilder("scope-1", configKeySpot).
					WithZonalInstanceCount(map[string]int{"us-west1-b": 10}).
					Build(),
			},
			reason:      metrics.ResponseMissingZone,
			expectedInc: 1,
		},
		{
			name: "Backend response negative instance count",
			mockCapacityGuidance: map[string]*api.InstanceAvailability{
				configKeyStandard: api.NewTestInstanceAvailabilityBuilder("scope-1", configKeyStandard).
					WithZonalInstanceCount(map[string]int{"us-west1-a": -5, "us-west1-b": 10, "us-west1-c": 10}).
					Build(),
				configKeySpot: api.NewTestInstanceAvailabilityBuilder("scope-1", configKeySpot).
					WithZonalInstanceCount(map[string]int{"us-west1-a": -5, "us-west1-b": 10, "us-west1-c": 10}).
					Build(),
			},
			reason:      metrics.InvalidInstanceCount,
			expectedInc: 1,
		},
		{
			name: "Backend response invalid preference score",
			mockCapacityGuidance: map[string]*api.InstanceAvailability{
				configKeyStandard: api.NewTestInstanceAvailabilityBuilder("scope-1", configKeyStandard).
					WithZonalInstanceCount(map[string]int{"us-west1-a": 10, "us-west1-b": 10, "us-west1-c": 10}).
					WithZonalGcePreferenceScore(map[string]float64{"us-west1-a": 1.5}).
					Build(),
				configKeySpot: api.NewTestInstanceAvailabilityBuilder("scope-1", configKeySpot).
					WithZonalInstanceCount(map[string]int{"us-west1-a": 10, "us-west1-b": 10, "us-west1-c": 10}).
					WithZonalGcePreferenceScore(map[string]float64{"us-west1-a": 1.5}).
					Build(),
			},
			reason:      metrics.InvalidPreferenceScore,
			expectedInc: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mockProvider := &mockAdviceProvider{}
				clock := newCustomFakeClock()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				instanceConfigCloudProvider := newMockInstanceConfigCloudProvider([]string{"us-west1-a", "us-west1-b", "us-west1-c"}, nil, machinetypes.E2, true, nil)
				optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())

				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, nil, withClock(clock))
				assert.NoError(t, err)

				mockProvider.On("FetchCapacityGuidance").Return(tc.mockCapacityGuidance, nil)

				initialVal, err := metrics.GetFlexAdvisorResponseErrorsCountForTest(tc.reason)
				assert.NoError(t, err)

				// AwaitInstanceAvailability will block until the first fetch (which registers the scope) completes
				_, _ = fa.AwaitInstanceAvailability("scope-1", configKeyStandard)

				val, err := metrics.GetFlexAdvisorResponseErrorsCountForTest(tc.reason)
				assert.NoError(t, err)
				assert.Equal(t, initialVal+float64(tc.expectedInc), val)
			})
		})
	}
}

func TestFlexibilityScope_LastSuccessfulRefreshAt(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Minute)
	testCases := []struct {
		name        string
		initialTime time.Time
		refreshedAt time.Time
		err         error
		wantTime    time.Time
	}{
		{
			name:        "successful refresh - updates lastSuccessfulRefreshAt",
			initialTime: time.Time{},
			refreshedAt: now,
			err:         nil,
			wantTime:    now,
		},
		{
			name:        "failed refresh - doesn't update lastSuccessfulRefreshAt",
			initialTime: now,
			refreshedAt: later,
			err:         fmt.Errorf("refresh error"),
			wantTime:    now,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scope := newFlexibilityScope(nil, "scope-1", func() {})
			scope.lastSuccessfulRefreshAt = tc.initialTime

			scope.finishRefresh(nil, &inFlightProvisions{}, nil, nil, tc.refreshedAt, tc.err)
			assert.Equal(t, tc.wantTime, scope.getLastSuccessfulRefreshAt())
		})
	}
}
