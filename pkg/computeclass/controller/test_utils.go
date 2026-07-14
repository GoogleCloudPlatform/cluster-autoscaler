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

package controller

import (
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

type mockNodeLister struct {
	mock.Mock
}

func (m *mockNodeLister) List() ([]*apiv1.Node, error) {
	args := m.Called()
	return args.Get(0).([]*apiv1.Node), args.Error(1)
}

func (m *mockNodeLister) Get(name string) (*apiv1.Node, error) {
	args := m.Called(name)
	return args.Get(0).(*apiv1.Node), args.Error(1)
}

type mockMatcher struct {
	mock.Mock
}

func (m *mockMatcher) FirstMatchedRule(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) (bool, int, rules.Rule) {
	args := m.Called(nodeGroup, crd)
	var r rules.Rule
	if args.Get(2) != nil {
		r = args.Get(2).(rules.Rule)
	}
	return args.Bool(0), args.Int(1), r
}

func (m *mockMatcher) MatchesCrdLabel(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) bool {
	args := m.Called(nodeGroup, crd)
	return args.Bool(0)
}

func (m *mockMatcher) MatchesCrdConfig(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) bool {
	args := m.Called(nodeGroup, crd)
	return args.Bool(0)
}

func (m *mockMatcher) FirstMatchedRuleGroup(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) (bool, int, rules.Rule) {
	args := m.Called(nodeGroup, crd)
	var r rules.Rule
	if args.Get(2) != nil {
		r = args.Get(2).(rules.Rule)
	}
	return args.Bool(0), args.Int(1), r
}

type minCapacityMockCloudProvider struct {
	cloudprovider.CloudProvider
	nodeGroupForNodeFunc func(node *apiv1.Node) (cloudprovider.NodeGroup, error)
}

func (m *minCapacityMockCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	if m.nodeGroupForNodeFunc != nil {
		return m.nodeGroupForNodeFunc(node)
	}
	return nil, nil
}

type dummyNodeGroup struct {
	cloudprovider.NodeGroup
}

func (d *dummyNodeGroup) Id() string {
	return "dummy-id"
}

type mockMinCapacityObserver struct {
	mock.Mock
}

func (m *mockMinCapacityObserver) OnScaleUpDecision(ccName string, ruleIdx int, now time.Time) {
	m.Called(ccName, ruleIdx, now)
}

func (m *mockMinCapacityObserver) OnProvisioningComplete(cccName string, ruleIdx int, now time.Time) {
	m.Called(cccName, ruleIdx, now)
}

func (m *mockMinCapacityObserver) OnProvisioningError(cccName string, errType string, unhelpable bool, now time.Time) {
	m.Called(cccName, errType, unhelpable, now)
}

func (m *mockMinCapacityObserver) CheckLongUnprovisioned(now time.Time) {
	m.Called(now)
}

func (m *mockMinCapacityObserver) OnShortfallDetected(ccName string, ruleIdx int, now time.Time) {
	m.Called(ccName, ruleIdx, now)
}

func (m *mockMinCapacityObserver) OnComputeClassAdded(cc *cccv1.ComputeClass, now time.Time) {
	m.Called(cc, now)
}

func (m *mockMinCapacityObserver) OnComputeClassUpdated(oldCC, newCC *cccv1.ComputeClass, now time.Time) {
	m.Called(oldCC, newCC, now)
}

func (m *mockMinCapacityObserver) OnComputeClassDeleted(name string) {
	m.Called(name)
}
