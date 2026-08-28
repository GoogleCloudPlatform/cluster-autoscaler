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

package testutil

import (
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

type ExtendedNodeGroup struct {
	Name  string
	Nodes []*apiv1.Node
	Spec  *gkeclient.NodePoolSpec
}

func CreateMig(ng ExtendedNodeGroup, manager gke.GkeManager) *gke.GkeMig {
	builder := gke.NewTestGkeMigBuilder().SetExist(true)
	return builder.SetSpec(ng.Spec).SetGceRef(gce.GceRef{
		Name: ng.Name,
	}).SetGkeManager(manager).Build()
}

type MockRandSource struct{}

func (mrs *MockRandSource) Int63() int64 {
	return 0
}
func (mrs *MockRandSource) Seed(seed int64) {}

func MakeMockGkeManager() gke.GkeManager {
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil)
	return gkeManager
}

type MockPluginProvider struct {
	mock.Mock
}

func (m *MockPluginProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	args := m.Called()
	return args.Get(0).(machinetypes.MachineFamily)
}

func (m *MockPluginProvider) IsAutopilotEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}
