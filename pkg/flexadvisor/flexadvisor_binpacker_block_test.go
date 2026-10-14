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
	"testing"
	"testing/synctest"
	"time"

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
	"k8s.io/utils/ptr"
)

const (
	binpackerBlockTestScopeKey          = "scope-1"
	binpackerBlockTestInstanceConfigKey = "InstanceConfig-key-1"
)

func binpackerBlockTestManager(enabled bool, stringFlags map[string]string) experiments.Manager {
	return experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.FlexAdvisorBinpackerBlockEnabledFlag:      enabled,
		experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: enabled,
	}, stringFlags)
}

func newBinpackerBlockTestFlexAdvisor(t *testing.T, ctx context.Context, manager experiments.Manager, opts ...flexAdvisorConstructorOption) (*flexAdvisor, *mockAdviceProvider) {
	t.Helper()
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: binpackerBlockTestScopeKey},
		Spec: v1.ComputeClassSpec{
			Priorities: []v1.Priority{{MachineType: ptr.To("e2-standard-2")}},
		},
	}, "", false, crd.TestDefaultDataProvider(), nil)
	mockProvider := &mockAdviceProvider{}
	instanceConfigCloudProvider := newMockInstanceConfigCloudProvider([]string{"us-west1-a", "us-west1-b", "us-west1-c"}, []*gke.GkeMig{}, machinetypes.E2, true, nil)
	optionsTracker := optstracking.TestOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, manager)
	fa, err := NewFlexAdvisor(ctx, mockProvider, lister.NewMockCrdLister([]crd.CRD{crd1}), instanceConfigCloudProvider, optionsTracker, nil, NewRecommendationsBypassTracker(manager), opts...)
	assert.NoError(t, err)
	return fa, mockProvider
}

func binpackerBlockTestAvailabilityResponse() map[string]*api.InstanceAvailability {
	availability := api.NewTestInstanceAvailabilityBuilder(binpackerBlockTestScopeKey, binpackerBlockTestInstanceConfigKey).Build()
	availability.SetProvider(nil)
	return map[string]*api.InstanceAvailability{binpackerBlockTestInstanceConfigKey: availability}
}

func fetchBlockedUntilContextDone(f *flexAdvisor, p *mockAdviceProvider) {
	p.On("FetchCapacityGuidance").Run(func(mock.Arguments) {
		<-f.context.Done()
	}).Return(nil, fmt.Errorf("GCE timeout"))
}

func fetchDelayed(p *mockAdviceProvider, delay time.Duration) {
	p.On("FetchCapacityGuidance").Run(func(mock.Arguments) {
		time.Sleep(delay)
	}).Return(binpackerBlockTestAvailabilityResponse(), nil).Once()
	p.On("FetchCapacityGuidance").Return(binpackerBlockTestAvailabilityResponse(), nil)
}

func registerWithFetchDone(t *testing.T, f *flexAdvisor, p *mockAdviceProvider) {
	t.Helper()
	p.On("FetchCapacityGuidance").Return(binpackerBlockTestAvailabilityResponse(), nil)
	assert.NoError(t, f.RegisterFlexibilityScope(binpackerBlockTestScopeKey))
	synctest.Wait()
}

func registerWithFetchFailed(t *testing.T, f *flexAdvisor, p *mockAdviceProvider) {
	t.Helper()
	p.On("FetchCapacityGuidance").Return(nil, fmt.Errorf("api call failed"))
	assert.NoError(t, f.RegisterFlexibilityScope(binpackerBlockTestScopeKey))
	synctest.Wait()
}

func registerWithFetchInFlight(t *testing.T, f *flexAdvisor, p *mockAdviceProvider) {
	t.Helper()
	fetchBlockedUntilContextDone(f, p)
	assert.NoError(t, f.RegisterFlexibilityScope(binpackerBlockTestScopeKey))
}

func scopeActiveFor(f *flexAdvisor, key string) (time.Duration, bool) {
	f.rwMutex.RLock()
	defer f.rwMutex.RUnlock()
	activeUntil, found := f.scopesActiveUntil[key]
	if !found {
		return 0, false
	}
	return activeUntil.Sub(f.clock.Now()), true
}

func TestFlexAdvisor_GetAvailabilitySnapshot_NeverBlocks(t *testing.T) {
	testCases := []struct {
		name           string
		binpackerBlock bool
		setup          func(t *testing.T, f *flexAdvisor, p *mockAdviceProvider)
		wantSnapshot   bool
	}{
		{
			name:           "binpacker block disabled, first fetch in flight - returns nil immediately",
			binpackerBlock: false,
			setup:          registerWithFetchInFlight,
			wantSnapshot:   false,
		},
		{
			name:           "binpacker block enabled, first fetch in flight - returns nil immediately",
			binpackerBlock: true,
			setup:          registerWithFetchInFlight,
			wantSnapshot:   false,
		},
		{
			name:           "binpacker block enabled, first fetch done - returns snapshot immediately",
			binpackerBlock: true,
			setup:          registerWithFetchDone,
			wantSnapshot:   true,
		},
		{
			name:           "binpacker block enabled, first fetch failed - returns nil immediately",
			binpackerBlock: true,
			setup:          registerWithFetchFailed,
			wantSnapshot:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				manager := binpackerBlockTestManager(tc.binpackerBlock, nil)
				fa, mockProvider := newBinpackerBlockTestFlexAdvisor(t, ctx, manager)
				tc.setup(t, fa, mockProvider)

				start := time.Now()
				snapshot, err := fa.GetAvailabilitySnapshot(binpackerBlockTestScopeKey, binpackerBlockTestInstanceConfigKey)
				waited := time.Since(start)

				assert.Equal(t, tc.wantSnapshot, snapshot != nil)
				assert.Equal(t, !tc.wantSnapshot, err != nil)
				assert.Equal(t, time.Duration(0), waited, "GetAvailabilitySnapshot must never block")
			})
		})
	}
}

func TestFlexAdvisor_AwaitFlexibilityScopeWithTimeout(t *testing.T) {
	testCases := []struct {
		name              string
		timeout           time.Duration
		setup             func(t *testing.T, f *flexAdvisor, p *mockAdviceProvider)
		wantErrContains   string
		wantWaited        time.Duration
		wantSnapshotAfter bool
	}{
		{
			name:    "fetch finishes within timeout - blocks until fetch completes and populates scope",
			timeout: 3 * time.Second,
			setup: func(t *testing.T, f *flexAdvisor, p *mockAdviceProvider) {
				fetchDelayed(p, 1*time.Second)
				assert.NoError(t, f.RegisterFlexibilityScope(binpackerBlockTestScopeKey))
			},
			wantWaited:        1 * time.Second,
			wantSnapshotAfter: true,
		},
		{
			name:    "scope not registered beforehand - registers scope, blocks, and populates scope",
			timeout: 3 * time.Second,
			setup: func(t *testing.T, f *flexAdvisor, p *mockAdviceProvider) {
				fetchDelayed(p, 1*time.Second)
			},
			wantWaited:        1 * time.Second,
			wantSnapshotAfter: true,
		},
		{
			name:            "fetch slower than timeout - gives up after timeout",
			timeout:         3 * time.Second,
			setup:           registerWithFetchInFlight,
			wantErrContains: "timeout when waiting for first fetch, no data available",
			wantWaited:      3 * time.Second,
		},
		{
			name:            "custom timeout 1s - gives up after 1s",
			timeout:         1 * time.Second,
			setup:           registerWithFetchInFlight,
			wantErrContains: "timeout when waiting for first fetch, no data available",
			wantWaited:      1 * time.Second,
		},
		{
			name:            "timeout 0 when fetch in flight - fails immediately without blocking",
			timeout:         0,
			setup:           registerWithFetchInFlight,
			wantErrContains: "timeout when waiting for first fetch, no data available",
			wantWaited:      0,
		},
		{
			name:            "first fetch failed - returns error immediately without blocking",
			timeout:         3 * time.Second,
			setup:           registerWithFetchFailed,
			wantErrContains: "api call failed",
			wantWaited:      0,
		},
		{
			name:              "scope already has data - returns nil immediately without blocking",
			timeout:           3 * time.Second,
			setup:             registerWithFetchDone,
			wantWaited:        0,
			wantSnapshotAfter: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				manager := binpackerBlockTestManager(true, nil)
				fa, mockProvider := newBinpackerBlockTestFlexAdvisor(t, ctx, manager)
				tc.setup(t, fa, mockProvider)

				start := time.Now()
				err := fa.AwaitFlexibilityScopeWithTimeout(binpackerBlockTestScopeKey, tc.timeout)
				waited := time.Since(start)

				if tc.wantErrContains != "" {
					assert.ErrorContains(t, err, tc.wantErrContains)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tc.wantWaited, waited, "blocked duration mismatch")

				snapshot, _ := fa.GetAvailabilitySnapshot(binpackerBlockTestScopeKey, binpackerBlockTestInstanceConfigKey)
				assert.Equal(t, tc.wantSnapshotAfter, snapshot != nil, "snapshot presence after await mismatch")
			})
		})
	}
}

func TestFlexAdvisor_TtlSemantics_WithBinpackerBlock(t *testing.T) {
	testCases := []struct {
		name          string
		call          func(t *testing.T, f *flexAdvisor)
		wantRemaining time.Duration
	}{
		{
			name: "GetAvailabilitySnapshot on existing scope - keep-alive untouched",
			call: func(t *testing.T, f *flexAdvisor) {
				_, _ = f.GetAvailabilitySnapshot(binpackerBlockTestScopeKey, binpackerBlockTestInstanceConfigKey)
			},
			wantRemaining: keepAliveInterval - 50*time.Second,
		},
		{
			name: "AwaitFlexibilityScopeWithTimeout on existing scope - keep-alive untouched",
			call: func(t *testing.T, f *flexAdvisor) {
				_ = f.AwaitFlexibilityScopeWithTimeout(binpackerBlockTestScopeKey, 3*time.Second)
			},
			wantRemaining: keepAliveInterval - 50*time.Second,
		},
		{
			name: "AwaitAvailabilitySnapshotWithTimeout on existing scope - keep-alive untouched",
			call: func(t *testing.T, f *flexAdvisor) {
				_, _ = f.AwaitAvailabilitySnapshotWithTimeout(binpackerBlockTestScopeKey, binpackerBlockTestInstanceConfigKey, firstFetchTimeout)
			},
			wantRemaining: keepAliveInterval - 50*time.Second,
		},
		{
			name: "BumpScopeTtl on existing scope - keep-alive extended back to 10m",
			call: func(t *testing.T, f *flexAdvisor) {
				assert.NoError(t, f.BumpScopeTtl(binpackerBlockTestScopeKey))
			},
			wantRemaining: keepAliveInterval,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				manager := binpackerBlockTestManager(true, nil)
				fa, mockProvider := newBinpackerBlockTestFlexAdvisor(t, ctx, manager)
				registerWithFetchDone(t, fa, mockProvider)
				time.Sleep(50 * time.Second)
				synctest.Wait()

				tc.call(t, fa)

				activeForAfter, found := scopeActiveFor(fa, binpackerBlockTestScopeKey)
				assert.True(t, found)
				assert.Equal(t, tc.wantRemaining, activeForAfter)
			})
		})
	}
}

func TestIsFlexAdvisorBinpackerBlockEnabled(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		expected           bool
	}{
		{
			name:               "nil experiments manager - returns false",
			experimentsManager: nil,
			expected:           false,
		},
		{
			name:               "default flags - returns true",
			experimentsManager: experiments.NewMockManager(),
			expected:           true,
		},
		{
			name: "bool flag disabled even when min CA version met - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorBinpackerBlockEnabledFlag:      false,
				experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: true,
			}, nil),
			expected: false,
		},
		{
			name: "bool flag enabled and min CA version met - returns true",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorBinpackerBlockEnabledFlag:      true,
				experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: true,
			}, nil),
			expected: true,
		},
		{
			name: "bool flag enabled but min CA version not met - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorBinpackerBlockEnabledFlag:      true,
				experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: false,
			}, nil),
			expected: false,
		},
		{
			name: "bool flag enabled and min CA version met but FlexAdvisorProcessing disabled - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:          false,
				experiments.FlexAdvisorBinpackerBlockEnabledFlag:      true,
				experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: true,
			}, nil),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsFlexAdvisorBinpackerBlockEnabled(tc.experimentsManager))
		})
	}
}
