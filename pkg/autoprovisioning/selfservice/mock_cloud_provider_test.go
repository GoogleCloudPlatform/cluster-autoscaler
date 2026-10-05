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

package selfservice

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	experimentsfake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments/fake"
)

type mockCloudProvider struct {
	isPSC              bool
	defaultPrivateNode bool
	isAutopilot        bool
}

func (m *mockCloudProvider) IsClusterUsingPSCInfrastructure() bool {
	return m.isPSC
}

func (m *mockCloudProvider) GetDefaultEnablePrivateNodes() bool {
	return m.defaultPrivateNode
}

func (m *mockCloudProvider) IsAutopilotEnabled() bool {
	return m.isAutopilot
}

func defaultExperimentsManager() experiments.Manager {
	evaluator := experimentsfake.NewEvaluator(
		map[string]bool{experiments.EnableNestedVirtualizationEnabledFlag: true},
		map[string]string{},
	)
	return experiments.NewManager(version.Version{}, evaluator)
}

// initDefaultSelfService initializes the package the same way for all tests.
func initDefaultSelfService() {
	InitSelfService(defaultExperimentsManager())
	SetCloudProvider(&mockCloudProvider{})
}
