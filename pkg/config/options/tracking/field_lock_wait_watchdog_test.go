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

package tracking

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestLockWaitWatchdogExplicitOverrideBeatsMitigation(t *testing.T) {
	v, err := version.FromString("37.0.0-gke.0")
	require.NoError(t, err)

	optsFromFlags := internalopts.AutoscalingOptions{
		InternalOptions: internalopts.InternalOptions{
			LockWaitWatchdogEnabled: true,
			FlagOverrideResult: internalopts.FlagOverrideResult{
				OverriddenFlags: map[string]bool{enableLockWaitWatchdogFlag: true},
			},
		},
	}
	em := experiments.NewMockManagerWithOptions(
		v,
		map[string]bool{experiments.LockWaitWatchdogEnabledFlag: false},
		map[string]string{experiments.LockWaitWatchdogMinCAVersionFlag: "999.999.999"},
	)
	var gotOpts internalopts.AutoscalingOptions
	require.NoError(t, lockWaitWatchdogEnabledField.setValue(optsFromFlags, em, &gotOpts))
	assert.True(t, gotOpts.LockWaitWatchdogEnabled)

	optsFromFlags.LockWaitWatchdogEnabled = false
	emEnabled := experiments.NewMockManagerWithOptions(
		v,
		map[string]bool{experiments.LockWaitWatchdogEnabledFlag: true},
		nil,
	)
	require.NoError(t, lockWaitWatchdogEnabledField.setValue(optsFromFlags, emEnabled, &gotOpts))
	assert.False(t, gotOpts.LockWaitWatchdogEnabled)
}

func TestLockWaitWatchdogMitigationTogglesWithoutRestart(t *testing.T) {
	v, err := version.FromString("37.0.0-gke.0")
	require.NoError(t, err)

	optsFromFlags := internalopts.AutoscalingOptions{
		InternalOptions: internalopts.InternalOptions{
			LockWaitWatchdogEnabled: true,
		},
	}
	tracker := NewOptionsTracker(optsFromFlags, experiments.NewMockManagerWithOptions(v, nil, nil))
	tracker.StoreCluster(gkeclient.Cluster{ClusterVersion: "1.30.0-gke.0"})
	require.NoError(t, tracker.RecomputeOptions())
	assert.True(t, tracker.Options().LockWaitWatchdogEnabled)
	assert.False(t, tracker.OptionChangesRequireRestart())

	// Mitigation disables the watchdog at runtime without restarting CA.
	tracker.experimentsManager = experiments.NewMockManagerWithOptions(
		v,
		map[string]bool{experiments.LockWaitWatchdogEnabledFlag: false},
		nil,
	)
	require.NoError(t, tracker.RecomputeOptions())
	assert.False(t, tracker.Options().LockWaitWatchdogEnabled)
	assert.False(t, tracker.OptionChangesRequireRestart())

	// Removing the mitigation restores the CLI flag without a restart.
	tracker.experimentsManager = experiments.NewMockManagerWithOptions(v, nil, nil)
	require.NoError(t, tracker.RecomputeOptions())
	assert.True(t, tracker.Options().LockWaitWatchdogEnabled)
	assert.False(t, tracker.OptionChangesRequireRestart())
}
