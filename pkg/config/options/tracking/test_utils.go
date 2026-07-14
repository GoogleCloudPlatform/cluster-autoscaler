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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// FakeOptionsTracker returns an instance of OptionsTracker to be used in tests.
func FakeOptionsTracker(flagOpts internalopts.AutoscalingOptions, cluster gkeclient.Cluster, fakeexperimentsManager experiments.Manager) *OptionsTracker {
	experimentsManager := fakeexperimentsManager
	if experimentsManager == nil {
		experimentsManager = experiments.NewMockManager()
	}
	tracker := NewOptionsTracker(flagOpts, experimentsManager)
	tracker.RecomputeOptions(cluster)
	return tracker
}

// EmptyFakeOptionsTracker returns an instance of OptionsTracker to be used in tests.
func EmptyFakeOptionsTracker() *OptionsTracker {
	return FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
}

// ChangeTestExperimentsManager allows changing the OptionsTracker internal experiments manager in order to simulate experiments
// changing over time.
func ChangeTestExperimentsManager(optsTracker *OptionsTracker, fakeExperimentsManager experiments.Manager) {
	optsTracker.experimentsManager = fakeExperimentsManager
}
