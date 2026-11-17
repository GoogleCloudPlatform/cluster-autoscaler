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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func adaptivePollingTestManager(enabled bool, stringFlags map[string]string) experiments.Manager {
	return experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.FlexAdvisorAdaptivePollingEnabledFlag:      enabled,
		experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: enabled,
	}, stringFlags)
}

func TestFlexAdvisor_AdaptivePollingCadence(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		fetchLatency       time.Duration
		fetchErr           error
		scenario           func(t *testing.T, f *flexAdvisor, at func(target time.Duration))
		wantFetchTimes     []time.Duration
	}{
		{
			name:               "adaptive polling disabled - new scope starts in pollingSpeedFast and DeactivateFastPolling keeps 10s cadence",
			experimentsManager: adaptivePollingTestManager(false, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				scope, found, _ := f.getScope(testScopeKey)
				assert.True(t, found)
				assert.Equal(t, pollingSpeedFast, scope.getPollingSpeed())

				f.DeactivateFastPolling(testScopeKey)
				at(30 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				10 * time.Second,
				20 * time.Second,
				30 * time.Second,
			},
		},
		{
			name:               "adaptive polling enabled, new scope starts in pollingSpeedSlow by default - refreshes every 20s",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				scope, found, _ := f.getScope(testScopeKey)
				assert.True(t, found)
				assert.Equal(t, pollingSpeedSlow, scope.getPollingSpeed())

				at(45 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				20 * time.Second,
				40 * time.Second,
			},
		},
		{
			name: "adaptive polling enabled with custom 45s slow interval - refreshes every 45s in slow polling",
			experimentsManager: adaptivePollingTestManager(true, map[string]string{
				experiments.FlexAdvisorAdaptivePollingSlowIntervalSecondsFlag: "45",
			}),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(95 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				45 * time.Second,
				90 * time.Second,
			},
		},
		{
			name: "adaptive polling enabled with non-positive slow interval - falls back to default 20s",
			experimentsManager: adaptivePollingTestManager(true, map[string]string{
				experiments.FlexAdvisorAdaptivePollingSlowIntervalSecondsFlag: "0",
			}),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(25 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				20 * time.Second,
			},
		},
		{
			name:               "transition slow to fast at t=4s (less than 5s half-interval since last try) - does not refresh immediately, resets remaining timer to 6s (fires at t=10s)",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(4 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(24 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				10 * time.Second,
				20 * time.Second,
			},
		},
		{
			name:               "transition slow to fast at t=6s (at least 5s half-interval since last try) - triggers immediate refresh at t=6s, then refreshes every 10s",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(6 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(26 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				6 * time.Second,
				16 * time.Second,
				26 * time.Second,
			},
		},
		{
			name:               "transition slow to fast at t=12s (more than 10s since last try) - triggers immediate refresh at t=12s, then refreshes every 10s",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(12 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(32 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				12 * time.Second,
				22 * time.Second,
				32 * time.Second,
			},
		},
		{
			name:               "transition fast to slow at t=14s (after fast poll at t=12s) - resets remaining slow timer to 18s (fires at t=32s)",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(12 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(14 * time.Second)
				f.DeactivateFastPolling(testScopeKey)
				at(35 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				12 * time.Second,
				32 * time.Second,
			},
		},
		{
			name:               "single-shard cluster running ScaleUp every 10s (ActivateFastPolling at t=10s, DeactivateFastPolling at t=10.1s) - refreshes every 10s while active",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(100 * time.Millisecond)
				f.DeactivateFastPolling(testScopeKey)

				at(10 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(10*time.Second + 100*time.Millisecond)
				f.DeactivateFastPolling(testScopeKey)

				at(20 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(20*time.Second + 100*time.Millisecond)
				f.DeactivateFastPolling(testScopeKey)
			},
			wantFetchTimes: []time.Duration{
				0,
				10 * time.Second,
				20 * time.Second,
			},
		},
		{
			name:               "single-shard 10s loops with 800ms GCE RPC latency - half-interval wakeup triggers immediately at t=10s and t=20s without stretching to 20s",
			experimentsManager: adaptivePollingTestManager(true, nil),
			fetchLatency:       800 * time.Millisecond,
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(200 * time.Millisecond)
				f.DeactivateFastPolling(testScopeKey)

				at(10 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(10*time.Second + 200*time.Millisecond)
				f.DeactivateFastPolling(testScopeKey)

				at(20 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(20*time.Second + 200*time.Millisecond)
				f.DeactivateFastPolling(testScopeKey)

				at(21 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				10 * time.Second,
				20 * time.Second,
			},
		},
		{
			name:               "failed refresh followed by ActivateFastPolling at t=2s (less than 5s half-interval since last try) - does not retry immediately, fires at t=10s",
			experimentsManager: adaptivePollingTestManager(true, nil),
			fetchErr:           fmt.Errorf("simulated GCE outage"),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(2 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				at(22 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				10 * time.Second,
				20 * time.Second,
			},
		},
		{
			name:               "coalesced ActivateFastPolling and immediate DeactivateFastPolling before 20s - remaining-delay reset preserves 20s slow timer without starvation",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				at(4 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				f.DeactivateFastPolling(testScopeKey)
				synctest.Wait()

				at(8 * time.Second)
				f.ActivateFastPolling(testScopeKey)
				f.DeactivateFastPolling(testScopeKey)
				synctest.Wait()

				at(21 * time.Second)
			},
			wantFetchTimes: []time.Duration{
				0,
				20 * time.Second,
			},
		},
		{
			name:               "ActivateFastPolling and DeactivateFastPolling on unregistered scope - no-op without registering scope",
			experimentsManager: adaptivePollingTestManager(true, nil),
			scenario: func(t *testing.T, f *flexAdvisor, at func(target time.Duration)) {
				f.ActivateFastPolling("unregistered-scope")
				f.DeactivateFastPolling("unregistered-scope")
				_, exists, _ := f.getScope("unregistered-scope")
				assert.False(t, exists)
			},
			wantFetchTimes: []time.Duration{0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				fa, mockProvider := newTestFlexAdvisorWithServer(t, ctx, tc.experimentsManager)
				start := time.Now()
				var mu sync.Mutex
				var fetchTimes []time.Duration

				resp := testAvailabilityResponse()
				if tc.fetchErr != nil {
					resp = nil
				}
				mockProvider.On("FetchCapacityGuidance").Run(func(mock.Arguments) {
					mu.Lock()
					fetchTimes = append(fetchTimes, time.Since(start))
					mu.Unlock()
					if tc.fetchLatency > 0 {
						select {
						case <-time.After(tc.fetchLatency):
						case <-ctx.Done():
						}
					}
				}).Return(resp, tc.fetchErr)

				at := func(target time.Duration) {
					synctest.Wait()
					if d := time.Until(start.Add(target)); d > 0 {
						time.Sleep(d)
					}
					synctest.Wait()
				}

				assert.NoError(t, fa.RegisterFlexibilityScope(testScopeKey))
				synctest.Wait()

				tc.scenario(t, fa, at)

				mu.Lock()
				got := append([]time.Duration(nil), fetchTimes...)
				mu.Unlock()
				assert.Equal(t, tc.wantFetchTimes, got)
			})
		})
	}
}

func TestFlexAdvisor_StalenessThreshold_WithAdaptivePolling(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		wantThreshold      time.Duration
	}{
		{
			name:               "nil experiments manager - returns default 30s",
			experimentsManager: nil,
			wantThreshold:      30 * time.Second,
		},
		{
			name:               "adaptive polling disabled - returns max(30s, 3 * 10s) = 30s",
			experimentsManager: adaptivePollingTestManager(false, nil),
			wantThreshold:      30 * time.Second,
		},
		{
			name:               "adaptive polling enabled with default 20s slow interval - ignores slow interval and returns 30s",
			experimentsManager: adaptivePollingTestManager(true, nil),
			wantThreshold:      30 * time.Second,
		},
		{
			name: "adaptive polling enabled with 45s slow interval - ignores slow interval and uses fast interval (30s)",
			experimentsManager: adaptivePollingTestManager(true, map[string]string{
				experiments.FlexAdvisorAdaptivePollingSlowIntervalSecondsFlag: "45",
			}),
			wantThreshold: 30 * time.Second,
		},
		{
			name: "adaptive polling enabled with 20s fast refresh interval - returns 3 * 20s = 60s",
			experimentsManager: adaptivePollingTestManager(true, map[string]string{
				experiments.FlexAdvisorScopeRefreshIntervalSecondsFlag: "20",
			}),
			wantThreshold: 60 * time.Second,
		},
		{
			name: "adaptive polling enabled with explicit ScopeStalenessThresholdSeconds override - honors explicit override",
			experimentsManager: adaptivePollingTestManager(true, map[string]string{
				experiments.FlexAdvisorScopeStalenessThresholdSecondsFlag: "60",
			}),
			wantThreshold: 60 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fa, _ := newTestFlexAdvisorWithServer(t, ctx, tc.experimentsManager)
			assert.Equal(t, tc.wantThreshold, fa.getScopeStalenessThreshold())
		})
	}
}

func TestIsFlexAdvisorAdaptivePollingEnabled(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		want               bool
	}{
		{
			name:               "nil experiments manager - defaults to true",
			experimentsManager: nil,
			want:               true,
		},
		{
			name:               "default flags - defaults to true",
			experimentsManager: experiments.NewMockManager(),
			want:               true,
		},
		{
			name: "bool flag enabled and min CA version met - returns true",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: true,
			}, nil),
			want: true,
		},
		{
			name: "bool flag disabled - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil),
			want: false,
		},
		{
			name: "bool flag enabled but min CA version not met - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: false,
			}, nil),
			want: false,
		},
		{
			name: "bool flag enabled and min CA version met but FlexAdvisorProcessing disabled - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:           false,
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: true,
			}, nil),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsFlexAdvisorAdaptivePollingEnabled(tc.experimentsManager))
		})
	}
}

func TestFlexibilityScope_SetPollingSpeed(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		initialSpeed       pollingSpeed
		targetSpeed        pollingSpeed
		wantSignaled       bool
	}{
		{
			name: "speed unchanged with debug logs disabled - does not signal pollingSpeedCh",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: false,
			}, nil),
			initialSpeed: pollingSpeedSlow,
			targetSpeed:  pollingSpeedSlow,
			wantSignaled: false,
		},
		{
			name: "speed changed from slow to fast with debug logs disabled - updates speed and signals pollingSpeedCh",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: false,
			}, nil),
			initialSpeed: pollingSpeedSlow,
			targetSpeed:  pollingSpeedFast,
			wantSignaled: true,
		},
		{
			name: "speed changed from slow to fast with debug logs enabled - updates speed and signals pollingSpeedCh",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: true,
			}, nil),
			initialSpeed: pollingSpeedSlow,
			targetSpeed:  pollingSpeedFast,
			wantSignaled: true,
		},
		{
			name: "adaptive polling disabled on newFlexibilityScope with debug logs enabled - starts in fast polling and signals on change to slow",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
				experiments.FlexAdvisorEnableDebugLogsFlag:        true,
			}, nil),
			initialSpeed: pollingSpeedFast,
			targetSpeed:  pollingSpeedSlow,
			wantSignaled: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scope := newFlexibilityScope(nil, testScopeKey, tc.experimentsManager, func() {})
			assert.Equal(t, tc.initialSpeed, scope.getPollingSpeed())

			scope.setPollingSpeed(tc.targetSpeed)
			assert.Equal(t, tc.targetSpeed, scope.getPollingSpeed())

			signaled := false
			select {
			case <-scope.pollingSpeedCh:
				signaled = true
			default:
			}
			assert.Equal(t, tc.wantSignaled, signaled)
		})
	}
}

func TestFlexAdvisor_ActivateAndDeactivateFastPolling(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		registerScope      bool
		sleepBeforeAction  time.Duration
		initialSpeed       *pollingSpeed
		action             func(fa *flexAdvisor)
		wantSpeed          pollingSpeed
		wantRemainingTtl   time.Duration
	}{
		{
			name:               "ActivateFastPolling with adaptive polling enabled - switches to fast polling and refreshes 10m TTL",
			experimentsManager: adaptivePollingTestManager(true, nil),
			registerScope:      true,
			sleepBeforeAction:  2 * time.Minute,
			action: func(fa *flexAdvisor) {
				fa.ActivateFastPolling(testScopeKey)
			},
			wantSpeed:        pollingSpeedFast,
			wantRemainingTtl: keepAliveInterval,
		},
		{
			name:               "ActivateFastPolling with adaptive polling disabled - keeps fast polling and still refreshes 10m TTL",
			experimentsManager: adaptivePollingTestManager(false, nil),
			registerScope:      true,
			sleepBeforeAction:  2 * time.Minute,
			action: func(fa *flexAdvisor) {
				fa.ActivateFastPolling(testScopeKey)
			},
			wantSpeed:        pollingSpeedFast,
			wantRemainingTtl: keepAliveInterval,
		},
		{
			name:               "DeactivateFastPolling with adaptive polling enabled - switches back to slow polling without altering TTL",
			experimentsManager: adaptivePollingTestManager(true, nil),
			registerScope:      true,
			sleepBeforeAction:  2 * time.Minute,
			initialSpeed:       new(pollingSpeedFast),
			action: func(fa *flexAdvisor) {
				fa.DeactivateFastPolling(testScopeKey)
			},
			wantSpeed:        pollingSpeedSlow,
			wantRemainingTtl: 8 * time.Minute,
		},
		{
			name:               "DeactivateFastPolling with adaptive polling disabled - keeps fast polling and leaves TTL untouched",
			experimentsManager: adaptivePollingTestManager(false, nil),
			registerScope:      true,
			sleepBeforeAction:  2 * time.Minute,
			action: func(fa *flexAdvisor) {
				fa.DeactivateFastPolling(testScopeKey)
			},
			wantSpeed:        pollingSpeedFast,
			wantRemainingTtl: 8 * time.Minute,
		},
		{
			name: "ActivateFastPolling when FlexAdvisorProcessing is disabled - no-op on speed and TTL",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:           false,
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: true,
			}, nil),
			registerScope:     true,
			sleepBeforeAction: 2 * time.Minute,
			initialSpeed:      new(pollingSpeedSlow),
			action: func(fa *flexAdvisor) {
				fa.ActivateFastPolling(testScopeKey)
			},
			wantSpeed:        pollingSpeedSlow,
			wantRemainingTtl: 8 * time.Minute,
		},
		{
			name: "DeactivateFastPolling when FlexAdvisorProcessing is disabled - no-op on speed and TTL",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:           false,
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: true,
			}, nil),
			registerScope:     true,
			sleepBeforeAction: 2 * time.Minute,
			initialSpeed:      new(pollingSpeedFast),
			action: func(fa *flexAdvisor) {
				fa.DeactivateFastPolling(testScopeKey)
			},
			wantSpeed:        pollingSpeedFast,
			wantRemainingTtl: 8 * time.Minute,
		},
		{
			name:               "unregistered scope - ActivateFastPolling and DeactivateFastPolling are no-ops",
			experimentsManager: adaptivePollingTestManager(true, nil),
			registerScope:      false,
			action: func(fa *flexAdvisor) {
				fa.ActivateFastPolling(testScopeKey)
				fa.DeactivateFastPolling(testScopeKey)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				fa, mockProvider := newTestFlexAdvisorWithServer(t, ctx, tc.experimentsManager)
				mockProvider.On("FetchCapacityGuidance").Return(testAvailabilityResponse(), nil).Maybe()

				if tc.registerScope {
					assert.NoError(t, fa.RegisterFlexibilityScope(testScopeKey))
					synctest.Wait()
					if tc.initialSpeed != nil {
						scope, _, _ := fa.getScope(testScopeKey)
						scope.setPollingSpeed(*tc.initialSpeed)
					}
					if tc.sleepBeforeAction > 0 {
						time.Sleep(tc.sleepBeforeAction)
						synctest.Wait()
					}
				}

				now := fa.clock.Now()
				tc.action(fa)

				scope, found, _ := fa.getScope(testScopeKey)
				assert.Equal(t, tc.registerScope, found)
				fa.rwMutex.RLock()
				activeUntil, hasTtl := fa.scopesActiveUntil[testScopeKey]
				fa.rwMutex.RUnlock()
				if !tc.registerScope {
					assert.False(t, hasTtl)
					return
				}
				assert.True(t, hasTtl)
				assert.Equal(t, tc.wantSpeed, scope.getPollingSpeed())
				assert.Equal(t, now.Add(tc.wantRemainingTtl), activeUntil)
			})
		})
	}
}
