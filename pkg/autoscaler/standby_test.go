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

package autoscaler

import (
	"context"
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	ccclister "github.com/googlecloudplatform/compute-class-api/client/listers/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const (
	standbyCheckInterval    = 10 * time.Second
	standbyMinCAVersionFlag = experiments.StandbyModeFeature + experiments.MinCAVersionSuffix
	standbyEnabledFlag      = experiments.StandbyModeFeature + experiments.EnabledSuffix
)

var solverAutoscalerNames = []string{internalopts.SolverAutoscalerName}

func computeClass(name, autoscalerName string) *cccv1.ComputeClass {
	ccc := &cccv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if autoscalerName != "" {
		ccc.Annotations = map[string]string{internalopts.AutoscalerNameAnnotation: autoscalerName}
	}
	return ccc
}

// newComputeClassCache returns an in-memory ComputeClass cache, as kept by the
// ComputeClass informer, holding cccs.
func newComputeClassCache(t *testing.T, cccs ...*cccv1.ComputeClass) cache.Indexer {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, ccc := range cccs {
		assert.NoError(t, indexer.Add(ccc))
	}
	return indexer
}

// experimentsConfigMap returns a ConfigMap holding flags serialized as
// experiments.json (or an empty ConfigMap when flags is empty).
func experimentsConfigMap(t *testing.T, flags map[string]any) *apiv1.ConfigMap {
	t.Helper()
	if len(flags) == 0 {
		return &apiv1.ConfigMap{}
	}
	raw, err := json.Marshal(flags)
	assert.NoError(t, err)
	return &apiv1.ConfigMap{
		Data: map[string]string{
			"experiments.json": string(raw),
		},
	}
}

func TestIsStandbyEnabled(t *testing.T) {
	caVersion, err := version.FromString("1.37.0")
	assert.NoError(t, err)

	assert.False(t, isStandbyEnabled(nil), "nil experimentsManager")

	testCases := []struct {
		name  string
		flags map[string]any
		want  bool
	}{
		{
			name: "unset flags default to enabled",
			want: true,
		},
		{
			name:  "explicitly enabled",
			flags: map[string]any{standbyEnabledFlag: true},
			want:  true,
		},
		{
			name:  "disabled via Enabled flag",
			flags: map[string]any{standbyEnabledFlag: false},
			want:  false,
		},
		{
			name:  "MinCAVersion met",
			flags: map[string]any{standbyMinCAVersionFlag: "1.37.0"},
			want:  true,
		},
		{
			name:  "disabled via MinCAVersion bump",
			flags: map[string]any{standbyMinCAVersionFlag: "1.38.0"},
			want:  false,
		},
		{
			name:  "MinCAVersion bump overrides Enabled=true",
			flags: map[string]any{standbyMinCAVersionFlag: "1.38.0", standbyEnabledFlag: true},
			want:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := experiments.NewManager(caVersion, experiments.NewConfigMapEvaluator(experimentsConfigMap(t, tc.flags)))
			assert.Equal(t, tc.want, isStandbyEnabled(manager))
		})
	}
}

// The tests below run in a synctest bubble, where time is fake: waiting takes
// no real time, and the checks happen exactly every standbyCheckInterval.

func TestWaitForOwnedComputeClassReturnsAtOnceWhenOwned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		computeClasses := newComputeClassCache(t,
			computeClass("unannotated", ""),
			computeClass("solver", internalopts.SolverAutoscalerName))
		start := time.Now()

		err := waitForOwnedComputeClass(t.Context(), ccclister.NewComputeClassLister(computeClasses), solverAutoscalerNames, standbyCheckInterval, experiments.NewMockManager())

		assert.NoError(t, err)
		assert.Zero(t, time.Since(start), "time spent waiting")
	})
}

func TestWaitForOwnedComputeClassWaitsUntilAnnotated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		computeClasses := newComputeClassCache(t,
			computeClass("unannotated", ""),
			computeClass("standard", internalopts.StandardAutoscalerName))
		result := make(chan error, 1)
		go func() {
			result <- waitForOwnedComputeClass(t.Context(), ccclister.NewComputeClassLister(computeClasses), solverAutoscalerNames, standbyCheckInterval, experiments.NewMockManager())
		}()

		// It keeps waiting while it owns no ComputeClass: the unannotated
		// and the standard ones belong to the other instance.
		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case err := <-result:
			t.Errorf("waitForOwnedComputeClass(..., %v, %v) = %v while owning no ComputeClass, want still waiting", solverAutoscalerNames, standbyCheckInterval, err)
		default:
			// Still waiting, as expected.
		}

		// Once a ComputeClass is annotated for it, it returns at the next
		// check.
		assert.NoError(t, computeClasses.Add(computeClass("solver", internalopts.SolverAutoscalerName)))
		time.Sleep(standbyCheckInterval)
		synctest.Wait()
		select {
		case err := <-result:
			assert.NoError(t, err)
		default:
			t.Errorf("waitForOwnedComputeClass(..., %v, %v) still waiting after %v, want return once ComputeClass annotated", solverAutoscalerNames, standbyCheckInterval, standbyCheckInterval)
		}
	})
}

func TestWaitForOwnedComputeClassWakesUpWhenExperimentDisabled(t *testing.T) {
	testCases := []struct {
		name         string
		disableFlags map[string]any
	}{
		{
			name:         "disabled via Enabled flag",
			disableFlags: map[string]any{standbyEnabledFlag: false},
		},
		{
			name:         "disabled via MinCAVersion bump",
			disableFlags: map[string]any{standbyMinCAVersionFlag: "1.38.0"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				caVersion, err := version.FromString("1.37.0")
				assert.NoError(t, err)
				// With no experiment flags set, stand-by mode defaults to enabled.
				evaluator := experiments.NewConfigMapEvaluator(experimentsConfigMap(t, nil))
				experimentsManager := experiments.NewManager(caVersion, evaluator)
				result := make(chan error, 1)
				go func() {
					result <- waitForOwnedComputeClass(t.Context(), ccclister.NewComputeClassLister(newComputeClassCache(t)), solverAutoscalerNames, standbyCheckInterval, experimentsManager)
				}()

				// It keeps waiting while stand-by mode is enabled and it owns no ComputeClass.
				time.Sleep(time.Hour)
				synctest.Wait()
				select {
				case err := <-result:
					t.Errorf("waitForOwnedComputeClass(..., %v, %v) = %v while stand-by mode enabled, want still waiting", solverAutoscalerNames, standbyCheckInterval, err)
				default:
					// Still waiting, as expected.
				}

				// Disabling stand-by mode (via Enabled=false or bumping MinCAVersion
				// above the running CA version 1.37.0) wakes the waiting instance at
				// the next check.
				evaluator.UpdateConfigMap(experimentsConfigMap(t, tc.disableFlags))
				time.Sleep(standbyCheckInterval)
				synctest.Wait()
				select {
				case err := <-result:
					assert.NoError(t, err)
				default:
					t.Errorf("waitForOwnedComputeClass(..., %v, %v) still waiting after %v, want return when stand-by disabled", solverAutoscalerNames, standbyCheckInterval, standbyCheckInterval)
				}
			})
		})
	}
}

func TestWaitForOwnedComputeClassReturnsErrorWhenContextDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
		defer cancel()

		err := waitForOwnedComputeClass(ctx, ccclister.NewComputeClassLister(newComputeClassCache(t)), solverAutoscalerNames, standbyCheckInterval, experiments.NewMockManager())

		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestWaitForOwnedComputeClassReturnsImmediatelyWhenNotWaiting(t *testing.T) {
	for _, autoscalerNames := range [][]string{nil, {internalopts.UnannotatedAutoscalerName, internalopts.StandardAutoscalerName}} {
		opts := internalopts.AutoscalingOptions{}
		opts.AutoscalerNames = autoscalerNames
		// For these instances WaitForOwnedComputeClass returns before it builds a
		// kubeconfig or starts a ComputeClass informer, so empty options and no
		// API server are enough. If it didn't return early, the test would crash
		// or hang.
		assert.NoError(t, WaitForOwnedComputeClass(t.Context(), opts, experiments.NewMockManager()), "autoscalerNames=%v", autoscalerNames)
	}

	// Same when stand-by mode is disabled via either experiment flag.
	opts := internalopts.AutoscalingOptions{}
	opts.AutoscalerNames = solverAutoscalerNames
	caVersion, err := version.FromString("1.37.0")
	assert.NoError(t, err)
	for _, disableFlags := range []map[string]any{
		{standbyEnabledFlag: false},
		{standbyMinCAVersionFlag: "1.38.0"},
	} {
		disabledExperiments := experiments.NewManager(caVersion, experiments.NewConfigMapEvaluator(experimentsConfigMap(t, disableFlags)))
		assert.NoError(t, WaitForOwnedComputeClass(t.Context(), opts, disabledExperiments), "flags=%v", disableFlags)
	}
}
