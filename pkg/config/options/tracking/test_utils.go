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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// FakeOptionsTracker returns an initialized OptionsTracker that computes options from flagOpts, cluster,
// and experiments via RecomputeOptions() logic. Prefer this over StaticOptionsTracker in tests.
func FakeOptionsTracker(flagOpts internalopts.AutoscalingOptions, cluster gkeclient.Cluster, fakeexperimentsManager experiments.Manager) *OptionsTracker {
	experimentsManager := fakeexperimentsManager
	if experimentsManager == nil {
		experimentsManager = experiments.NewMockManager()
	}
	tracker := NewOptionsTracker(flagOpts, experimentsManager)
	tracker.StoreCluster(cluster)
	if err := tracker.RecomputeOptions(); err != nil {
		panic(err)
	}
	return tracker
}

// EmptyFakeOptionsTracker returns an instance of OptionsTracker to be used in tests.
func EmptyFakeOptionsTracker() *OptionsTracker {
	return FakeOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
}

// StaticOptionsTracker returns an OptionsTracker that returns the provided AutoscalingOptions from Options() as-is,
// bypassing RecomputeOptions() (which shouldn't be called on the result).
// Prefer FakeOptionsTracker as it accurately reflects prod logic; use StaticOptionsTracker only when testing
// components that read Options() without wanting to set up the flags, Cluster proto, and experiments required to compute them.
func StaticOptionsTracker(opts internalopts.AutoscalingOptions) *OptionsTracker {
	return &OptionsTracker{
		optsFromFlags:        opts,
		startupOpts:          opts,
		startupOptsFinalized: true,
		lastOpts:             opts,
		experimentsManager:   experiments.NewMockManager(),
	}
}

// ChangeTestExperimentsManager allows changing the OptionsTracker internal experiments manager in order to simulate experiments
// changing over time.
func ChangeTestExperimentsManager(optsTracker *OptionsTracker, fakeExperimentsManager experiments.Manager) {
	optsTracker.experimentsManager = fakeExperimentsManager
}
