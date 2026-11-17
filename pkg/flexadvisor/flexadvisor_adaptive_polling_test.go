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

func adaptivePollingTestManager(shardBased bool, stringFlags map[string]string) experiments.Manager {
	sf := make(map[string]string, len(stringFlags)+1)
	for k, v := range stringFlags {
		sf[k] = v
	}
	if shardBased {
		sf[experiments.FlexAdvisorSchedulingModeFlag] = string(SchedulingModeShardBased)
	} else {
		sf[experiments.FlexAdvisorSchedulingModeFlag] = string(SchedulingModeLate)
	}
	return experiments.NewMockManagerWithOptions(version.Version{}, nil, sf)
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
			name:               "late mode - new scope starts in pollingSpeedFast and DeactivateFastPolling keeps 10s cadence",
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
			name:               "shard-based mode, new scope starts in pollingSpeedSlow by default - refreshes every 20s",
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
			name: "shard-based mode with custom 45s slow interval - refreshes every 45s in slow polling",
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
			name: "shard-based mode with non-positive slow interval - falls back to default 20s",
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
			name:               "late mode - returns max(30s, 3 * 10s) = 30s",
			experimentsManager: adaptivePollingTestManager(false, nil),
			wantThreshold:      30 * time.Second,
		},
		{
			name:               "shard-based mode with default 20s slow interval - ignores slow interval and returns 30s",
			experimentsManager: adaptivePollingTestManager(true, nil),
			wantThreshold:      30 * time.Second,
		},
		{
			name: "shard-based mode with 45s slow interval - ignores slow interval and uses fast interval (30s)",
			experimentsManager: adaptivePollingTestManager(true, map[string]string{
				experiments.FlexAdvisorAdaptivePollingSlowIntervalSecondsFlag: "45",
			}),
			wantThreshold: 30 * time.Second,
		},
		{
			name: "shard-based mode with 20s fast refresh interval - returns 3 * 20s = 60s",
			experimentsManager: adaptivePollingTestManager(true, map[string]string{
				experiments.FlexAdvisorScopeRefreshIntervalSecondsFlag: "20",
			}),
			wantThreshold: 60 * time.Second,
		},
		{
			name: "shard-based mode with explicit ScopeStalenessThresholdSeconds override - honors explicit override",
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

func TestFlexibilityScope_SetPollingSpeed(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		initialSpeed       pollingSpeed
		targetSpeed        pollingSpeed
		wantSignaled       bool
	}{
		{
			name: "shard-based mode, speed unchanged with debug logs disabled - does not signal pollingSpeedCh",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: false,
			}, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
			}),
			initialSpeed: pollingSpeedSlow,
			targetSpeed:  pollingSpeedSlow,
			wantSignaled: false,
		},
		{
			name: "shard-based mode, speed changed from slow to fast with debug logs disabled - updates speed and signals pollingSpeedCh",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: false,
			}, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
			}),
			initialSpeed: pollingSpeedSlow,
			targetSpeed:  pollingSpeedFast,
			wantSignaled: true,
		},
		{
			name: "shard-based mode, speed changed from slow to fast with debug logs enabled - updates speed and signals pollingSpeedCh",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: true,
			}, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
			}),
			initialSpeed: pollingSpeedSlow,
			targetSpeed:  pollingSpeedFast,
			wantSignaled: true,
		},
		{
			name: "late mode (non-shard-based) on newFlexibilityScope with debug logs enabled - starts in fast polling and signals on change to slow",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorEnableDebugLogsFlag: true,
			}, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeLate),
			}),
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
