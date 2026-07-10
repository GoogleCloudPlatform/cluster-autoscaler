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
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	clockutils "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// withClock is an Option to set a custom clock, typically for testing.
func withClock(clock clock.Clock) option {
	return func(f *flexAdvisor) {
		f.clock = clock
	}
}

// withInstanceConfigGenerator is an Option to set a custom instance config generator, typically for testing.
func withInstanceConfigGenerator(instanceConfigGenerator *instanceConfigGenerator) option {
	return func(f *flexAdvisor) {
		f.instanceConfigGenerator = instanceConfigGenerator
	}
}

func withMetrics(metrics flexAdvisorMetrics) option {
	return func(f *flexAdvisor) {
		f.metrics = metrics
	}
}

type flexAdvisorCacheMetricLabels struct {
	result             metrics.FACacheQueryResult
	cccState           metrics.CccState
	isScaleUpAnyway    *bool
	keyGenerationState metrics.KeyGenerationState
}

type mockFlexAdvisorMetrics struct {
	calledWith []flexAdvisorCacheMetricLabels
}

func (m *mockFlexAdvisorMetrics) IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, cccState metrics.CccState, isScaleUpAnyway *bool, keyGenerationState metrics.KeyGenerationState) {
	m.calledWith = append(m.calledWith, flexAdvisorCacheMetricLabels{
		result:             result,
		cccState:           cccState,
		isScaleUpAnyway:    isScaleUpAnyway,
		keyGenerationState: keyGenerationState,
	})
}

type mockAdviceProvider struct {
	mock.Mock
}

func (m *mockAdviceProvider) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*api.InstanceConfig) (availability map[string]*api.InstanceAvailability, err error) {
	args := m.Called()
	if args.Get(0) != nil {
		if fn, ok := args.Get(0).(func() map[string]*api.InstanceAvailability); ok {
			availability = fn()
		} else {
			availability = args.Get(0).(map[string]*api.InstanceAvailability)
		}
	}
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return
}

func (m *mockAdviceProvider) SendCapacityDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) (err error) {
	args := m.Called(decision)
	if args.Get(0) != nil {
		err = args.Get(0).(error)
	}
	return
}

// addScopeDataRaceBackgroundJob adds background job that constantly overwrites critical fields on the scope
func addScopeDataRaceBackgroundJob(f *flexAdvisor, flexibilityScopeKey string) {
	// iterates over all fields of map type on the object and overwrites all it's keys
	overwriteAllMapKeys := func(scope *flexibilityScope) {
		val := reflect.ValueOf(scope).Elem()
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			ptr := unsafe.Pointer(field.UnsafeAddr())
			setableField := reflect.NewAt(field.Type(), ptr).Elem()
			if setableField.Kind() == reflect.Map {
				if !setableField.IsNil() {
					for _, key := range setableField.MapKeys() {
						setableField.SetMapIndex(key, setableField.MapIndex(key))
					}
				}
			}
		}
	}
	// iterates over all object fields and overwrites them
	overwriteAllObjectFields := func(scope *flexibilityScope) {
		val := reflect.ValueOf(scope).Elem()
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			fieldName := val.Type().Field(i).Name
			fieldType := field.Type().String()
			// don't overwrite mutexes or other fields considered const
			if fieldType == "sync.Mutex" || fieldType == "sync.WaitGroup" || fieldName == "flexibilityScopeKey" || fieldName == "provider" || fieldName == "cancelFunc" {
				continue
			}
			ptr := unsafe.Pointer(field.UnsafeAddr())
			setableField := reflect.NewAt(field.Type(), ptr).Elem()
			setableField.Set(setableField)
		}
	}
	go func() {
		for {
			select {
			case <-f.context.Done():
				return
			default:
				scope, _, _ := f.getScope(flexibilityScopeKey)
				if scope == nil {
					continue
				}
				scope.mutex.Lock()
				overwriteAllMapKeys(scope)
				scope.mutex.Unlock()
			}
		}
	}()
	go func() {
		for {
			select {
			case <-f.context.Done():
				return
			default:
				scope, _, _ := f.getScope(flexibilityScopeKey)
				if scope == nil {
					continue
				}
				scope.mutex.Lock()
				overwriteAllObjectFields(scope)
				scope.mutex.Unlock()
			}
		}
	}()
}

func TestFlexAdvisor_GetInstanceAvailability(t *testing.T) {
	wantedAvailability := api.NewTestInstanceAvailabilityBuilder("scope-1", "InstanceConfig-key-1").Build()
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
		name               string
		initialSetup       func(f *flexAdvisor, p *mockAdviceProvider)
		scopeKeyToCheck    string
		instanceKeyToCheck string
		want               *instanceavailability.Snapshot
	}{
		{
			name: "Scope not found, creates new scope",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Return(nil, nil)
			},
			scopeKeyToCheck:    "non-existent-scope",
			instanceKeyToCheck: "any-key",
			want:               nil,
		},
		{
			name: "Scope found but instance config key not found",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Return(nil, nil)
				f.RegisterFlexibilityScope("scope-1")
			},
			scopeKeyToCheck:    "scope-1",
			instanceKeyToCheck: "non-existent-key",
			want:               nil,
		},
		{
			name: "Scope and instance config found successfully",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				wantedAvailability.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": wantedAvailability,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil)
				f.RegisterFlexibilityScope("scope-1")
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)
			},
			scopeKeyToCheck:    "scope-1",
			instanceKeyToCheck: "InstanceConfig-key-1",
			want:               wantedAvailability.NewSnapshot(),
		},
		{
			name: "Scope is stale, returns nil",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				wantedAvailability.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": wantedAvailability,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil)
				f.RegisterFlexibilityScope("scope-1")
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)

				scope, ok, _ := f.getScope("scope-1")
				assert.True(t, ok)

				// Simulate old refresh date
				scope.mutex.Lock()
				scope.lastSuccessfulRefreshAt = f.clock.Now().Add(-1 * time.Hour)
				scope.mutex.Unlock()
			},
			scopeKeyToCheck:    "scope-1",
			instanceKeyToCheck: "InstanceConfig-key-1",
			want:               nil,
		},
		{
			name: "data race test",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {

				addScopeDataRaceBackgroundJob(f, "scope-1")

				wantedAvailability.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": wantedAvailability,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil)
				f.RegisterFlexibilityScope("scope-1")

				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)
				snapShot, err = f.AwaitInstanceAvailability("scope-1", "non-existent-key")
			},
			scopeKeyToCheck:    "scope-1",
			instanceKeyToCheck: "InstanceConfig-key-1",
			want:               wantedAvailability.NewSnapshot(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mockProvider := &mockAdviceProvider{}
				clock := newCustomFakeClock()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				instanceConfigCloudProvider := newMockInstanceConfigCloudProvider([]string{"us-west1-a", "us-west1-b", "us-west1-c"}, []*gke.GkeMig{}, machinetypes.E2, true, nil)
				optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, withClock(clock))
				assert.NoError(t, err)
				tc.initialSetup(fa, mockProvider)

				result := fa.GetInstanceAvailability(tc.scopeKeyToCheck, tc.instanceKeyToCheck)

				if tc.want == nil {
					assert.Nil(t, result, "Wanted fa return nil result")
				} else {
					result.SetProvider(nil)
					assert.NotNil(t, result, "Wanted fa return non-nil result")
					assert.Equal(t, *tc.want, *result, "The returned value did not match the wanted value")
				}
			})
		})
	}
}

func TestFlexAdvisor_RegisterFlexibilityScope(t *testing.T) {
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "new-scope",
		},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{
				{
					MachineType: ptr.To("e2-standard-2"),
				},
			},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	crd2 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "existing-scope",
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
		name         string
		scopeKey     string
		initialSetup func(f *flexAdvisor, p *mockAdviceProvider)
	}{
		{
			name:     "register for a non existent scope",
			scopeKey: "new-scope",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": api.NewTestInstanceAvailabilityBuilder("new-scope", "InstanceConfig-key-1").Build(),
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil)
			},
		},
		{
			name:     "register for an existing scope",
			scopeKey: "existing-scope",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": api.NewTestInstanceAvailabilityBuilder("existing-scope", "InstanceConfig-key-1").Build(),
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil)
				f.RegisterFlexibilityScope("existing-scope")
				snapShot, err := f.AwaitInstanceAvailability("existing-scope", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)
			},
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
				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1, crd2}), instanceConfigCloudProvider, optionsTracker, withClock(clock))
				assert.NoError(t, err)
				tc.initialSetup(fa, mockProvider)
				fa.RegisterFlexibilityScope(tc.scopeKey)

				result, err := fa.AwaitInstanceAvailability(tc.scopeKey, "InstanceConfig-key-1")
				assert.NoError(t, err)
				assert.NotNil(t, result)
			})
		})
	}
}

func TestFlexAdvisor_RegisterFlexibilityScopeLimits(t *testing.T) {
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
		name                 string
		experiments          map[string]string
		wantRegisteredScopes int
	}{
		{
			name:                 "limit reached",
			experiments:          map[string]string{experiments.FlexAdvisorMaxActiveScopes: "1"},
			wantRegisteredScopes: 1,
		},
		{
			name:                 "negative value defaults to 50",
			experiments:          map[string]string{experiments.FlexAdvisorMaxActiveScopes: "-1"},
			wantRegisteredScopes: 50,
		},
		{
			name:                 "0 defaults to 50",
			experiments:          map[string]string{experiments.FlexAdvisorMaxActiveScopes: "0"},
			wantRegisteredScopes: 50,
		},
		{
			name:                 "100 uses 100",
			experiments:          map[string]string{experiments.FlexAdvisorMaxActiveScopes: "100"},
			wantRegisteredScopes: 100,
		},
		{
			name:                 "no experiment",
			experiments:          nil,
			wantRegisteredScopes: 50,
		},
		{
			name:                 "invalid experiment value",
			experiments:          map[string]string{experiments.FlexAdvisorMaxActiveScopes: "invalid-value"},
			wantRegisteredScopes: 50,
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
				var optionsManager experiments.Manager
				if tc.experiments != nil {
					optionsManager = experiments.NewMockManagerWithOptions(version.Version{}, nil, tc.experiments)
				} else {
					optionsManager = experiments.NewMockManager()
				}
				optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, optionsManager)
				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, withClock(clock))
				assert.NoError(t, err)

				mockProvider.On("FetchCapacityGuidance").Return(nil, nil)

				for i := 0; i < tc.wantRegisteredScopes; i++ {
					err := fa.RegisterFlexibilityScope(fmt.Sprintf("existing-scope-%d", i))
					assert.NoError(t, err, fmt.Sprintf("%d should succeed", i))
				}

				firstErr := fa.RegisterFlexibilityScope(fmt.Sprintf("existing-scope-%d", tc.wantRegisteredScopes))
				assert.Error(t, firstErr)
				assert.Contains(t, firstErr.Error(), "active scope limit of")
			})
		})
	}
}

func TestFlexAdvisor_AwaitInstanceAvailability(t *testing.T) {
	instanceConfig1 := api.NewTestInstanceAvailabilityBuilder("", "InstanceConfig-key-1").Build()
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
		name            string
		scopeKey        string
		instanceKey     string
		initialSetup    func(f *flexAdvisor, p *mockAdviceProvider)
		enabledFeatures map[string]string
		want            *instanceavailability.Snapshot
		wantErr         error
		wantErrContains string
	}{
		{
			name:        "Successful fetch returns correct value",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				instanceConfig1.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": instanceConfig1,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil).Once()
			},
			want:    instanceConfig1.NewSnapshot(),
			wantErr: nil,
		},
		{
			name:        "Return the value from cache. No new fetches",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				instanceConfig1.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": instanceConfig1,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil).Once()
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.NoError(t, err)
			},
			want:    instanceConfig1.NewSnapshot(),
			wantErr: nil,
		},
		{
			name:        "Fetch fails and returns an error",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Return(nil, fmt.Errorf("api call failed")).Once()
			},
			want:    nil,
			wantErr: fmt.Errorf("instanceConfigKey=InstanceConfig-key-1 not present in availability data after refresh, flexibilityScopeKey=scope-1, lastRefreshErr=api call failed, cccState=, cccUsesScaleUpAnyway=false, keyGenerationState=not_generated"),
		},
		{
			name:        "Successful fetch but instance key not found",
			scopeKey:    "scope-1",
			instanceKey: "non-existent-key",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": api.NewTestInstanceAvailabilityBuilder("scope-1", "InstanceConfig-key-1").Build(),
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil).Once()
			},
			want:    nil,
			wantErr: fmt.Errorf("instanceConfigKey=non-existent-key not present in availability data after refresh, flexibilityScopeKey=scope-1, lastRefreshErr=<nil>, cccState=, cccUsesScaleUpAnyway=false, keyGenerationState=not_generated"),
		},
		{
			name:        "doesn't timeout after 10 seconds",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				instanceConfig1.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": instanceConfig1,
				}
				p.On("FetchCapacityGuidance").Run(func(args mock.Arguments) {
					time.Sleep(10 * time.Second)
				}).Return(mockApiResponse, nil).Once()
			},
			want:    instanceConfig1.NewSnapshot(),
			wantErr: nil,
		},
		{
			name:        "times out after 15 seconds",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Run(func(args mock.Arguments) {
					// block execution until end of test
					select {
					case <-f.context.Done():
					}
				}).Return(nil, fmt.Errorf("GCE timeout")).Once()
			},
			want:    nil,
			wantErr: fmt.Errorf("timeout waiting for GCE Flex Advisor consultation, flexibilityScopeKey=scope-1"),
		},
		{
			name:        "times out after 5 seconds via experiment",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Run(func(args mock.Arguments) {
					select {
					case <-time.After(6 * time.Second):
						panic("waited for longer than 5 seconds, timeout experiment did not work")
					case <-f.context.Done():
					}
				}).Return(nil, fmt.Errorf("GCE timeout")).Once()
			},
			enabledFeatures: map[string]string{
				experiments.FlexAdvisorAwaitInstanceAvailabilityTimeoutSecondsFlag: "5",
			},
			want:    nil,
			wantErr: fmt.Errorf("timeout waiting for GCE Flex Advisor consultation, flexibilityScopeKey=scope-1"),
		},
		{
			name:        "invalid experiment timeout value falls back to 15 seconds",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Run(func(args mock.Arguments) {
					select {
					case <-f.context.Done():
					}
				}).Return(nil, fmt.Errorf("GCE timeout")).Once()
			},
			enabledFeatures: map[string]string{
				experiments.FlexAdvisorAwaitInstanceAvailabilityTimeoutSecondsFlag: "invalid-value",
			},
			want:    nil,
			wantErr: fmt.Errorf("timeout waiting for GCE Flex Advisor consultation, flexibilityScopeKey=scope-1"),
		},
		{
			name:        "negative experiment timeout value falls back to 15 seconds",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Run(func(args mock.Arguments) {
					select {
					case <-f.context.Done():
					}
				}).Return(nil, fmt.Errorf("GCE timeout")).Once()
			},
			enabledFeatures: map[string]string{
				experiments.FlexAdvisorAwaitInstanceAvailabilityTimeoutSecondsFlag: "-10",
			},
			want:    nil,
			wantErr: fmt.Errorf("timeout waiting for GCE Flex Advisor consultation, flexibilityScopeKey=scope-1"),
		},
		{
			name:        "Scope is stale, returns error",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				instanceConfig1.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": instanceConfig1,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil).Once()
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.NoError(t, err)

				scope, ok, _ := f.getScope("scope-1")
				assert.True(t, ok)
				scope.mutex.Lock()
				scope.lastSuccessfulRefreshAt = f.clock.Now().Add(-1 * time.Hour)
				scope.mutex.Unlock()
			},
			want:            nil,
			wantErrContains: "scope is stale, not using availability data, flexibilityScopeKey=scope-1",
		},
		{
			name:        "data race test",
			scopeKey:    "scope-1",
			instanceKey: "InstanceConfig-key-1",
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {

				addScopeDataRaceBackgroundJob(f, "scope-1")

				instanceConfig1.SetProvider(nil)
				mockApiResponse := map[string]*api.InstanceAvailability{
					"InstanceConfig-key-1": instanceConfig1,
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil).Once()
				snapShot, err := f.AwaitInstanceAvailability("scope-1", "InstanceConfig-key-1")
				assert.NotNil(t, snapShot)
				assert.NoError(t, err)
			},
			want:    instanceConfig1.NewSnapshot(),
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mockProvider := &mockAdviceProvider{}
				clock := clock.RealClock{}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				instanceConfigCloudProvider := newMockInstanceConfigCloudProvider([]string{"us-west1-a", "us-west1-b", "us-west1-c"}, nil, machinetypes.E2, true, nil)
				mockManager := experiments.NewMockManagerWithOptions(version.Version{}, nil, tc.enabledFeatures)
				optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, mockManager)
				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, withClock(clock))
				assert.NoError(t, err)
				tc.initialSetup(fa, mockProvider)

				result, err := fa.AwaitInstanceAvailability(tc.scopeKey, tc.instanceKey)
				if tc.wantErr != nil {
					assert.Nil(t, result)
					assert.Equal(t, tc.wantErr, err)
				} else if tc.wantErrContains != "" {
					assert.Nil(t, result)
					assert.ErrorContains(t, err, tc.wantErrContains)
				} else {
					assert.NotNil(t, result)
					assert.NoError(t, err)
					result.SetProvider(nil)
					assert.Equal(t, *tc.want, *result)
				}
				mockProvider.AssertExpectations(t)
			})
		})
	}
}

func TestFlexAdvisor_RemoveExpiredFlexibilityScopes(t *testing.T) {
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-scope",
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
		name              string
		advanceDuration   time.Duration
		wantGuidanceCalls int
		addDataRaceJob    bool
	}{
		{
			name:              "Scope does not expire before timeout",
			advanceDuration:   4 * time.Minute,
			wantGuidanceCalls: 2, // The cache is used, so only the initial and one cache refresh.
		},
		{
			name:              "Scope expires after timeout",
			advanceDuration:   11 * time.Minute, // More than the 10-minute lifetime
			wantGuidanceCalls: 3,                // The scope expires, forcing a second call. (initial call + one cache refresh + second call)
		},
		{
			name:              "data race test",
			advanceDuration:   11 * time.Minute, // More than the 10-minute lifetime
			wantGuidanceCalls: 3,                // The scope expires, forcing a second call. (initial call + one cache refresh + second call)
			addDataRaceJob:    true,
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
				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, withClock(clock))
				assert.NoError(t, err)

				if tc.addDataRaceJob {
					addScopeDataRaceBackgroundJob(fa, "my-scope")
				}

				mockProvider.On("FetchCapacityGuidance").Return(func() map[string]*api.InstanceAvailability {
					return map[string]*api.InstanceAvailability{"instance-1": api.NewTestInstanceAvailabilityBuilder("", "instance-1").Build()}
				}, nil).Times(tc.wantGuidanceCalls)

				// 1. Make the initial call to populate the cache and trigger the first API call.
				_, err = fa.AwaitInstanceAvailability("my-scope", "instance-1")
				assert.NoError(t, err)

				// 2. Wait for step 1 to finish before incrementing the clock.
				// 1 waiter for flexibility scope refresh, 1 waiter for removing expired flexibility scopes
				err = clock.waitForClockWaiters(2)
				assert.NoError(t, err)

				// 3. Instantly advance the mock clock by the duration specified in the test case.
				clock.Step(tc.advanceDuration)

				// 4. We have to wait until FlexAdvisor reacts to step 3.
				err = clock.waitForClockWaiters(2)
				assert.NoError(t, err)

				// 5. Attempt to get the same scope again.
				// This will either use the cache or trigger a new API call depending on the clock.
				_, err = fa.AwaitInstanceAvailability("my-scope", "instance-1")
				assert.NoError(t, err)

				// 6. Wait until FA to finish reacting to step 5.
				err = clock.waitForClockWaiters(2)
				assert.NoError(t, err)

				mockProvider.AssertExpectations(t)
			})
		})
	}
}

func TestFlexAdvisor_MarkUsed(t *testing.T) {
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "flex-scope-1",
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
		name                      string
		flexibilityScopeKey       string
		instanceConfigKey         string
		zonalInstancesToProvision map[string]int
		initialSetup              func(*flexAdvisor, *mockAdviceProvider)
		wantInstances             map[string]int
		wantErr                   error
	}{
		{
			name:                      "flexibility scope not found returns error",
			flexibilityScopeKey:       "non-existent-scope",
			instanceConfigKey:         "config-key-1",
			zonalInstancesToProvision: map[string]int{"us-central1-a": 1},
			initialSetup:              func(advisor *flexAdvisor, provider *mockAdviceProvider) {},
			wantErr:                   fmt.Errorf("flexibility scope not found for key: non-existent-scope"),
		},
		{
			name:                      "instance configuration not found returns error",
			flexibilityScopeKey:       "flex-scope-1",
			instanceConfigKey:         "non-existent-config",
			zonalInstancesToProvision: map[string]int{"us-central1-a": 1},
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				p.On("FetchCapacityGuidance").Return(nil, nil)
				f.RegisterFlexibilityScope("flex-scope-1")
			},
			wantErr: fmt.Errorf("instance configuration not found for flexibilityScopeKey: flex-scope-1, instanceConfigurationKey: non-existent-config"),
		},
		{
			name:                      "successfully calls InstanceAvailability.MarkUsed",
			flexibilityScopeKey:       "flex-scope-1",
			instanceConfigKey:         "config-key-1",
			zonalInstancesToProvision: map[string]int{"us-central1-a": 5, "us-central1-b": 3},
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {
				mockApiResponse := map[string]*api.InstanceAvailability{
					"config-key-1": api.NewTestInstanceAvailabilityBuilder("flex-scope-1", "config-key-1").WithZonalInstanceCount(map[string]int{"us-central1-a": 100, "us-central1-b": 50}).Build(),
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil)
				p.On("SendCapacityDecision", mock.AnythingOfType("ProvisioningDecisionNotification")).Return(nil)
				snapShot, err := f.AwaitInstanceAvailability("flex-scope-1", "config-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)
			},
			wantInstances: map[string]int{"us-central1-a": 95, "us-central1-b": 47},
			wantErr:       nil,
		},
		{
			name:                      "data race test",
			flexibilityScopeKey:       "flex-scope-1",
			instanceConfigKey:         "config-key-1",
			zonalInstancesToProvision: map[string]int{"us-central1-a": 5, "us-central1-b": 3},
			initialSetup: func(f *flexAdvisor, p *mockAdviceProvider) {

				addScopeDataRaceBackgroundJob(f, "flex-scope-1")

				mockApiResponse := map[string]*api.InstanceAvailability{
					"config-key-1": api.NewTestInstanceAvailabilityBuilder("flex-scope-1", "config-key-1").WithZonalInstanceCount(map[string]int{"us-central1-a": 100, "us-central1-b": 50}).Build(),
				}
				p.On("FetchCapacityGuidance").Return(mockApiResponse, nil)
				p.On("SendCapacityDecision", mock.AnythingOfType("ProvisioningDecisionNotification")).Return(nil)
				snapShot, err := f.AwaitInstanceAvailability("flex-scope-1", "config-key-1")
				assert.NotNil(t, snapShot)
				assert.Nil(t, err)
			},
			wantInstances: map[string]int{"us-central1-a": 95, "us-central1-b": 47},
			wantErr:       nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				provider := &mockAdviceProvider{}
				clock := newCustomFakeClock()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				instanceConfigCloudProvider := newMockInstanceConfigCloudProvider([]string{"us-west1-a", "us-west1-b", "us-west1-c"}, nil, machinetypes.E2, true, nil)
				optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
				fa, err := NewFlexAdvisor(ctx, provider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, withClock(clock))

				assert.NoError(t, err)
				tc.initialSetup(fa, provider)

				err = fa.MarkUsed(tc.flexibilityScopeKey, tc.instanceConfigKey, "", "", tc.zonalInstancesToProvision)
				if tc.wantErr != nil {
					assert.Equal(t, tc.wantErr, err)
				} else {
					got := fa.GetInstanceAvailability(tc.flexibilityScopeKey, tc.instanceConfigKey)
					for zone, wantCount := range tc.wantInstances {
						gotZone, _ := got.MaxAvailableInstances(zone)
						assert.Equal(t, wantCount, gotZone)
					}
				}
			})
		})
	}
}

func TestFlexAdvisor_IncrementFlexAdvisorCacheQueryCount(t *testing.T) {
	testCases := []struct {
		name                string
		metricType          metrics.FACacheQueryResult
		flexibilityScopeKey string
		instanceConfigKey   string
		cappedKeysMap       map[string]bool
		initialSetup        func(fa *flexAdvisor)
		wantLabels          flexAdvisorCacheMetricLabels
	}{
		{
			name:                "non existent flexibilityScopeKey - issues metric without additional parameters",
			metricType:          metrics.FACacheMissNoInstanceConfigKey,
			flexibilityScopeKey: "non-existent-scope",
			initialSetup:        func(fa *flexAdvisor) {},
			wantLabels: flexAdvisorCacheMetricLabels{
				result:             metrics.FACacheMissNoInstanceConfigKey,
				isScaleUpAnyway:    nil,
				keyGenerationState: "",
			},
		},
		{
			name:          "flexibilityScopeKey exists, key was not generated",
			metricType:    metrics.FACacheMissNoInstanceConfigKey,
			cappedKeysMap: map[string]bool{},
			wantLabels: flexAdvisorCacheMetricLabels{
				result:             metrics.FACacheMissNoInstanceConfigKey,
				isScaleUpAnyway:    ptr.To(false),
				keyGenerationState: metrics.KeyGenerationStateNotGenerated,
			},
		},
		{
			name:          "flexibilityScopeKey exists, key was generated, not capped",
			metricType:    metrics.FACacheMissNoInstanceConfigKey,
			cappedKeysMap: map[string]bool{"key-1": false},
			wantLabels: flexAdvisorCacheMetricLabels{
				result:             metrics.FACacheMissNoInstanceConfigKey,
				isScaleUpAnyway:    ptr.To(false),
				keyGenerationState: metrics.KeyGenerationStateGeneratedAndSent,
			},
		},
		{
			name:          "flexibilityScopeKey exists, key was generated and capped",
			metricType:    metrics.FACacheMissNoInstanceConfigKey,
			cappedKeysMap: map[string]bool{"key-1": true},
			wantLabels: flexAdvisorCacheMetricLabels{
				result:             metrics.FACacheMissNoInstanceConfigKey,
				isScaleUpAnyway:    ptr.To(false),
				keyGenerationState: metrics.KeyGenerationStateGeneratedButCapped,
			},
		},
		{
			name:                "CCC is scaleUpAnyway",
			metricType:          metrics.FACacheMissNoInstanceConfigKey,
			flexibilityScopeKey: "ccc-scale-up-anyway",
			cappedKeysMap:       map[string]bool{},
			wantLabels: flexAdvisorCacheMetricLabels{
				result:             metrics.FACacheMissNoInstanceConfigKey,
				isScaleUpAnyway:    ptr.To(true),
				keyGenerationState: metrics.KeyGenerationStateNotGenerated,
			},
		},
		{
			name:          "FACacheHit includes additional parameters",
			metricType:    metrics.FACacheHit,
			cappedKeysMap: map[string]bool{"key-1": false},
			wantLabels: flexAdvisorCacheMetricLabels{
				result:             metrics.FACacheHit,
				isScaleUpAnyway:    nil,
				keyGenerationState: "",
			},
		},
		{
			name:                "PredefinedComputeClass",
			metricType:          metrics.FACacheMissNoInstanceConfigKey,
			flexibilityScopeKey: "Scale-Out",
			cappedKeysMap:       map[string]bool{},
			wantLabels: flexAdvisorCacheMetricLabels{
				result:             metrics.FACacheMissNoInstanceConfigKey,
				cccState:           metrics.CccStateEmpty,
				isScaleUpAnyway:    ptr.To(false),
				keyGenerationState: metrics.KeyGenerationStateNotGenerated,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := &mockAdviceProvider{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			crdLister := lister.NewMockCrdLister([]crd.CRD{
				crd.NewTestCrd(crd.WithName("scope-1")),
				crd.NewTestCrd(crd.WithScaleUpAnyway(), crd.WithName("ccc-scale-up-anyway")),
			})
			optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
			fa, err := NewFlexAdvisor(ctx, mockProvider, crdLister, newMockInstanceConfigCloudProvider(nil, nil, machinetypes.E2, true, nil), optionsTracker)
			assert.NoError(t, err)

			addScopeDataRaceBackgroundJob(fa, "scope-1")
			addScopeDataRaceBackgroundJob(fa, "ccc-scale-up-anyway")
			addScopeDataRaceBackgroundJob(fa, "Scale-Out")

			fa.RegisterFlexibilityScope("scope-1")
			fa.RegisterFlexibilityScope("ccc-scale-up-anyway")
			fa.RegisterFlexibilityScope("Scale-Out")

			mockMetrics := &mockFlexAdvisorMetrics{}
			fa.metrics = mockMetrics

			if tc.cappedKeysMap != nil {
				scope, ok, _ := fa.getScope("scope-1")
				assert.True(t, ok, "scope-1 not found")
				scope.mutex.Lock()
				scope.cappedKeysMap = tc.cappedKeysMap
				scope.mutex.Unlock()
			}

			flexibilityScopeKey := "scope-1"
			if tc.flexibilityScopeKey != "" {
				flexibilityScopeKey = tc.flexibilityScopeKey
			}
			instanceConfigKey := "key-1"
			if tc.instanceConfigKey != "" {
				instanceConfigKey = tc.instanceConfigKey
			}
			fa.IncrementFlexAdvisorCacheQueryCount(tc.metricType, flexibilityScopeKey, instanceConfigKey)

			assert.Equal(t, []flexAdvisorCacheMetricLabels{tc.wantLabels}, mockMetrics.calledWith)
		})
	}
}

type customFakeClock struct {
	clockutils.FakeClock
	waiterUpdateNotifyChan chan struct{}
}

func (f *customFakeClock) After(d time.Duration) <-chan time.Time {
	ch := f.FakeClock.After(d)
	f.notifyWaitersUpdate()
	return ch
}

func (f *customFakeClock) Step(d time.Duration) {
	f.FakeClock.Step(d)
	f.notifyWaitersUpdate()
}

func (f *customFakeClock) AfterFunc(d time.Duration, cb func()) clock.Timer {
	timer := f.FakeClock.AfterFunc(d, cb)
	f.notifyWaitersUpdate()
	return timer
}

func (f *customFakeClock) Tick(d time.Duration) <-chan time.Time {
	ch := f.FakeClock.Tick(d)
	f.notifyWaitersUpdate()
	return ch
}

func (f *customFakeClock) notifyWaitersUpdate() {
	select {
	case f.waiterUpdateNotifyChan <- struct{}{}:
	default:
	}
}

func (f *customFakeClock) NewTicker(d time.Duration) clock.Ticker {
	ticker := f.FakeClock.NewTicker(d)
	f.notifyWaitersUpdate()
	return ticker
}

func (f *customFakeClock) waitForClockWaiters(waiterCount int) error {
	if f.Waiters() >= waiterCount {
		return nil
	}

	timer := time.NewTimer(5 * time.Second)

	for {
		select {
		case <-f.waiterUpdateNotifyChan:
			klog.Errorf("Interrupt: Waiting for waiters now: %d", f.Waiters())
			if f.Waiters() >= waiterCount {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for clock waiters")
		}
	}
}

func (f *customFakeClock) SetTime(t time.Time) {
	f.FakeClock.SetTime(t)
	f.notifyWaitersUpdate()
}

func newCustomFakeClock() *customFakeClock {
	return &customFakeClock{
		FakeClock:              *clockutils.NewFakeClock(time.Date(2025, 7, 23, 14, 30, 0, 0, time.UTC)),
		waiterUpdateNotifyChan: make(chan struct{}, 5),
	}
}

type customFakeTimer struct {
	mutex     sync.Mutex
	timer     clock.Timer
	fakeClock *customFakeClock
}

func (f *customFakeTimer) C() <-chan time.Time {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.timer.C()
}

func (f *customFakeTimer) Stop() bool {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.timer.Stop()
}

func (f *customFakeTimer) Reset(d time.Duration) bool {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	active := f.timer.Reset(d)
	if !active {
		f.fakeClock.notifyWaitersUpdate()
		return false
	}
	f.timer = f.fakeClock.NewTimer(d)
	return active
}

func (f *customFakeClock) NewTimer(d time.Duration) clock.Timer {
	timer := f.FakeClock.NewTimer(d)
	fakeTimer := &customFakeTimer{
		timer:     timer,
		fakeClock: f,
	}
	f.notifyWaitersUpdate()
	return fakeTimer
}

func TestIsFlexAdvisorTPUEnabled(t *testing.T) {
	testCases := []struct {
		name      string
		boolFlags map[string]bool
		want      bool
	}{
		{
			name: "both disabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorTPUEnabledFlag:      false,
				experiments.FlexAdvisorTPUMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "only flag enabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorTPUEnabledFlag:      true,
				experiments.FlexAdvisorTPUMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "only version enabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorTPUEnabledFlag:      false,
				experiments.FlexAdvisorTPUMinCAVersionFlag: true,
			},
			want: false,
		},
		{
			name: "both enabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorTPUEnabledFlag:      true,
				experiments.FlexAdvisorTPUMinCAVersionFlag: true,
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, map[string]string{})
			got := isFlexAdvisorTPUEnabled(manager)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsFlexAdvisorZoneTypesEnabled(t *testing.T) {
	testCases := []struct {
		name      string
		boolFlags map[string]bool
		want      bool
	}{
		{
			name:      "no flags (defaults to true)",
			boolFlags: map[string]bool{},
			want:      true,
		},
		{
			name: "both disabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      false,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: false,
			},
			want: false,
		},

		{
			name: "only flag enabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      true,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "only version enabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      false,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: true,
			},
			want: false,
		},
		{
			name: "both enabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorZoneTypesEnabledFlag:      true,
				experiments.FlexAdvisorZoneTypesMinCAVersionFlag: true,
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, map[string]string{})
			got := isFlexAdvisorZoneTypesEnabled(manager)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsFlexAdvisorMinCpuPlatformSupportEnabled(t *testing.T) {

	testCases := []struct {
		name      string
		boolFlags map[string]bool
		want      bool
	}{
		{
			name:      "no flags (defaults to true)",
			boolFlags: map[string]bool{},
			want:      true,
		},
		{
			name: "both disabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorMinCpuPlatformEnabledFlag:      false,
				experiments.FlexAdvisorMinCpuPlatformMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "only flag enabled (failsafe is true for version)",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorMinCpuPlatformEnabledFlag:      true,
				experiments.FlexAdvisorMinCpuPlatformMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "only version enabled (failsafe is true for flag)",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorMinCpuPlatformEnabledFlag:      false,
				experiments.FlexAdvisorMinCpuPlatformMinCAVersionFlag: true,
			},
			want: false,
		},
		{
			name: "both enabled",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorMinCpuPlatformEnabledFlag:      true,
				experiments.FlexAdvisorMinCpuPlatformMinCAVersionFlag: true,
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, map[string]string{})
			got := isFlexAdvisorMinCpuPlatformSupportEnabled(manager)

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsFlexAdvisorProcessingEnabled(t *testing.T) {
	testCases := []struct {
		name        string
		boolFlags   map[string]bool
		stringFlags map[string]string
		nilManager  bool
		want        bool
	}{
		{
			name:       "nil manager - defaults to true",
			nilManager: true,
			want:       true,
		},
		{
			name: "nothing set - returns true",
			want: true,
		},
		{
			name: "FlexAdvisor::EnableProcessing off - returns false",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			},
			want: false,
		},
		{
			name: "FlexAdvisor::ProcessingMinCAVersion doesn't match - returns false",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorProcessingMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "both enabled - returns true",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:      true,
				experiments.FlexAdvisorProcessingMinCAVersionFlag: true,
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var manager experiments.Manager
			if tc.nilManager && (tc.boolFlags != nil || tc.stringFlags != nil) {
				t.Fatalf("Invalid usage: nilManager cannot be set along with experiments")
			}
			if !tc.nilManager {
				manager = experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, tc.stringFlags)
			}
			got := IsFlexAdvisorProcessingEnabled(manager)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsFlexAdvisorGeneratorMachineErrorsCacheEnabled(t *testing.T) {
	testCases := []struct {
		name        string
		boolFlags   map[string]bool
		stringFlags map[string]string
		nilManager  bool
		want        bool
	}{
		{
			name:       "nil manager - defaults to true",
			nilManager: true,
			want:       true,
		},
		{
			name: "nothing set - returns true",
			want: true,
		},
		{
			name: "GeneratorMachineErrorsCache enabled off - returns false",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorGeneratorMachineErrorsCacheEnabledFlag: false,
			},
			want: false,
		},
		{
			name: "GeneratorMachineErrorsCacheMinCAVersion doesn't match - returns false",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorGeneratorMachineErrorsCacheMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "both enabled - returns true",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorGeneratorMachineErrorsCacheEnabledFlag:      true,
				experiments.FlexAdvisorGeneratorMachineErrorsCacheMinCAVersionFlag: true,
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var manager experiments.Manager
			if tc.nilManager && (tc.boolFlags != nil || tc.stringFlags != nil) {
				t.Fatalf("Invalid usage: nilManager cannot be set along with experiments")
			}
			if !tc.nilManager {
				manager = experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, tc.stringFlags)
			}
			got := isFlexAdvisorGeneratorMachineErrorsCacheEnabled(manager)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsFlexAdvisorDebugLogsEnabled(t *testing.T) {
	testCases := map[string]struct {
		boolFlags  map[string]bool
		nilManager bool
		want       bool
	}{
		"nil manager - defaults to false": {
			nilManager: true,
			want:       false,
		},
		"nothing set - defaults to false": {
			want: false,
		},
		"debug logs flag set to false - returns false": {
			boolFlags: map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: false,
			},
			want: false,
		},
		"debug logs flag set to true - returns true": {
			boolFlags: map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: true,
			},
			want: true,
		},
	}
	for des, tc := range testCases {
		t.Run(des, func(t *testing.T) {
			var manager experiments.Manager
			if tc.nilManager && tc.boolFlags != nil {
				t.Fatalf("Invalid usage: nilManager cannot be set along with experiments")
			}
			if !tc.nilManager {
				manager = experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, nil)
			}
			got := isFlexAdvisorDebugLogsEnabled(manager)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsFlexAdvisorPCCSupportEnabled(t *testing.T) {
	testCases := []struct {
		name        string
		boolFlags   map[string]bool
		stringFlags map[string]string
		nilManager  bool
		want        bool
	}{
		{
			name:       "nil manager - defaults to true",
			nilManager: true,
			want:       true,
		},
		{
			name: "nothing set - returns true",
			want: true,
		},
		{
			name: "PCCSupport enabled off - returns false",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorPCCSupportEnabledFlag: false,
			},
			want: false,
		},
		{
			name: "PCCSupportMinCAVersion doesn't match - returns false",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorPCCSupportMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name: "both enabled - returns true",
			boolFlags: map[string]bool{
				experiments.FlexAdvisorPCCSupportEnabledFlag:      true,
				experiments.FlexAdvisorPCCSupportMinCAVersionFlag: true,
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var manager experiments.Manager
			if tc.nilManager && (tc.boolFlags != nil || tc.stringFlags != nil) {
				t.Fatalf("Invalid usage: nilManager cannot be set along with experiments")
			}
			if !tc.nilManager {
				manager = experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, tc.stringFlags)
			}
			got := isFlexAdvisorPCCSupportEnabled(manager)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFlexAdvisor_IsStale(t *testing.T) {
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
		name        string
		nilScope    bool
		refreshTime func(now time.Time) time.Time
		clockStep   time.Duration
		want        bool
	}{
		{
			name:     "nil scope - not stale",
			nilScope: true,
			want:     false,
		},
		{
			name: "scope never successfully refreshed - not stale",
			want: false,
		},
		{
			name: "scope refreshed recently - not stale",
			refreshTime: func(now time.Time) time.Time {
				return now
			},
			clockStep: 0,
			want:      false,
		},
		{
			name: "scope refreshed before threshold - not stale",
			refreshTime: func(now time.Time) time.Time {
				return now
			},
			clockStep: 29 * time.Second,
			want:      false,
		},
		{
			name: "scope stale refreshed after the threshold - stale",
			refreshTime: func(now time.Time) time.Time {
				return now
			},
			clockStep: 31 * time.Second,
			want:      true,
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
				fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, withClock(clock))
				assert.NoError(t, err)

				var scope *flexibilityScope
				if !tc.nilScope {
					scope = newFlexibilityScope(nil, "scope-1", func() {})
					if tc.refreshTime != nil {
						scope.lastSuccessfulRefreshAt = tc.refreshTime(clock.Now())
					}
				}

				if tc.clockStep > 0 {
					clock.Step(tc.clockStep)
				}

				got := fa.isStale(scope)
				assert.Equal(t, tc.want, got)
			})
		})
	}
}
