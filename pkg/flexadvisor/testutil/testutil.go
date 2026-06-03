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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	auto_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

type MockBalancer struct {
	mock.Mock
}

func (m *MockBalancer) FindSimilarNodeGroups(autoscalingCtx *ca_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	args := m.Called(autoscalingCtx, nodeGroup, nodeInfosForGroups)
	return args.Get(0).([]cloudprovider.NodeGroup), args.Get(1).(auto_errors.AutoscalerError)
}

func (m *MockBalancer) BalanceScaleUpBetweenGroups(autoscalingCtx *ca_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {
	args := m.Called(autoscalingCtx, groups, newNodes)
	var err auto_errors.AutoscalerError
	if args.Get(1) != nil {
		err = args.Get(1).(auto_errors.AutoscalerError)
	}
	return args.Get(0).([]nodegroupset.ScaleUpInfo), err
}

func (m *MockBalancer) CleanUp() {
	m.Called()
}
