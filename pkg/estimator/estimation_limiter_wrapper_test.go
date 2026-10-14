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

package estimator

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gceprovider "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
)

type fakeEstimationLimiter struct {
	startCalled      bool
	startCalls       int
	onStart          func()
	endCalled        bool
	permissionCalls  int
	permissionResult bool
}

func (f *fakeEstimationLimiter) StartEstimation(_ context.Context, _ []estimator.PodEquivalenceGroup, _ cloudprovider.NodeGroup, _ estimator.EstimationContext) {
	f.startCalled = true
	f.startCalls++
	if f.onStart != nil {
		f.onStart()
	}
}

func (f *fakeEstimationLimiter) EndEstimation() {
	f.endCalled = true
}

func (f *fakeEstimationLimiter) PermissionToAddNode(_ context.Context) bool {
	f.permissionCalls++
	return f.permissionResult
}

func newTestNodeGroup(name, cccName string) *gke.GkeMig {
	nodeLabels := map[string]string{}
	if cccName != "" {
		nodeLabels[labels.ComputeClassLabel] = cccName
	}
	templateNode := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: nodeLabels,
		},
	}
	fakeGke := gke.NewFakeGkeManagerBuilder().WithMigTemplateNode(templateNode).Build()
	return gke.NewTestGkeMigBuilder().
		SetGceRef(gceprovider.GceRef{Zone: "us-central1-a", Name: name}).
		SetGkeManager(fakeGke).
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "e2-standard-4",
			Labels:      nodeLabels,
		}).Build()
}

func binpackerBlockTestManager(enabled bool, stringFlags map[string]string) experiments.Manager {
	return experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.FlexAdvisorBinpackerBlockEnabledFlag:      enabled,
		experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: enabled,
	}, stringFlags)
}

func TestEstimationLimiterWrapper_DelegatesToInner(t *testing.T) {
	testCases := []struct {
		name             string
		permissionResult bool
	}{
		{
			name:             "delegates lifecycle calls when PermissionToAddNode returns true",
			permissionResult: true,
		},
		{
			name:             "delegates lifecycle calls when PermissionToAddNode returns false",
			permissionResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &fakeEstimationLimiter{permissionResult: tc.permissionResult}
			wrapper := NewEstimationLimiterWrapper(inner, nil, nil, nil)

			wrapper.StartEstimation(context.Background(), nil, nil, nil)
			assert.True(t, inner.startCalled)

			got := wrapper.PermissionToAddNode(context.Background())
			assert.Equal(t, tc.permissionResult, got)
			assert.Equal(t, 1, inner.permissionCalls)

			wrapper.EndEstimation()
			assert.True(t, inner.endCalled)
		})
	}
}

func TestEstimationLimiterWrapper_StartEstimation(t *testing.T) {
	crdScope1 := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "scope-1"}}, "", false, nil, nil)
	ngScope1 := newTestNodeGroup("mig-scope-1", "scope-1")
	ngNoCcc := newTestNodeGroup("mig-no-ccc", "")
	ngMissingCrd := newTestNodeGroup("mig-missing-crd", "nonexistent-scope")

	testCases := []struct {
		name               string
		nodeGroup          cloudprovider.NodeGroup
		experimentsManager experiments.Manager
		setupProvider      func(p *instanceavailability.MockProvider)
		wantBudgetUsed     bool
	}{
		{
			name:               "experiment disabled - does not call AwaitFlexibilityScopeWithTimeout and leaves budget unused",
			nodeGroup:          ngScope1,
			experimentsManager: binpackerBlockTestManager(false, nil),
			wantBudgetUsed:     false,
		},
		{
			name:      "general FlexAdvisorProcessing disabled - does not call AwaitFlexibilityScopeWithTimeout and leaves budget unused",
			nodeGroup: ngScope1,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:          false,
				experiments.FlexAdvisorBinpackerBlockEnabledFlag:      true,
				experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: true,
			}, nil),
			wantBudgetUsed: false,
		},
		{
			name:               "experiment enabled with default budget - awaits scope for 3s and marks budget used",
			nodeGroup:          ngScope1,
			experimentsManager: binpackerBlockTestManager(true, nil),
			setupProvider: func(p *instanceavailability.MockProvider) {
				p.On("AwaitFlexibilityScopeWithTimeout", "scope-1", 3*time.Second).Return(nil).Once()
			},
			wantBudgetUsed: true,
		},
		{
			name:      "experiment enabled with custom 5s budget - awaits scope for 5s and marks budget used",
			nodeGroup: ngScope1,
			experimentsManager: binpackerBlockTestManager(true, map[string]string{
				experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag: "5",
			}),
			setupProvider: func(p *instanceavailability.MockProvider) {
				p.On("AwaitFlexibilityScopeWithTimeout", "scope-1", 5*time.Second).Return(nil).Once()
			},
			wantBudgetUsed: true,
		},
		{
			name:      "experiment enabled with 0s budget - does not call AwaitFlexibilityScopeWithTimeout and marks budget used",
			nodeGroup: ngScope1,
			experimentsManager: binpackerBlockTestManager(true, map[string]string{
				experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag: "0",
			}),
			wantBudgetUsed: true,
		},
		{
			name:      "experiment enabled with negative budget (-5s) - falls back to 3s budget and marks budget used",
			nodeGroup: ngScope1,
			experimentsManager: binpackerBlockTestManager(true, map[string]string{
				experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag: "-5",
			}),
			setupProvider: func(p *instanceavailability.MockProvider) {
				p.On("AwaitFlexibilityScopeWithTimeout", "scope-1", 3*time.Second).Return(nil).Once()
			},
			wantBudgetUsed: true,
		},
		{
			name:               "experiment enabled and AwaitFlexibilityScopeWithTimeout returns error - still calls inner StartEstimation and marks budget used",
			nodeGroup:          ngScope1,
			experimentsManager: binpackerBlockTestManager(true, nil),
			setupProvider: func(p *instanceavailability.MockProvider) {
				p.On("AwaitFlexibilityScopeWithTimeout", "scope-1", 3*time.Second).Return(errors.New("timeout waiting for GCE Flex Advisor consultation")).Once()
			},
			wantBudgetUsed: true,
		},
		{
			name:               "node group without CCC (empty scope key) - does not await and leaves budget unused",
			nodeGroup:          ngNoCcc,
			experimentsManager: binpackerBlockTestManager(true, nil),
			wantBudgetUsed:     false,
		},
		{
			name:               "node group with missing CCC CRD (NodeGroupCrd error) - does not await and leaves budget unused",
			nodeGroup:          ngMissingCrd,
			experimentsManager: binpackerBlockTestManager(true, nil),
			wantBudgetUsed:     false,
		},
		{
			name:               "nil nodeGroup - does not panic or consume budget",
			nodeGroup:          nil,
			experimentsManager: binpackerBlockTestManager(true, nil),
			wantBudgetUsed:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(instanceavailability.MockProvider)
			if tc.setupProvider != nil {
				tc.setupProvider(mockProvider)
			}
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crdScope1}, labels.ComputeClassLabel)
			inner := &fakeEstimationLimiter{}
			wrapper := NewEstimationLimiterWrapper(inner, mockProvider, mockLister, tc.experimentsManager)

			wrapper.StartEstimation(context.Background(), nil, tc.nodeGroup, nil)

			assert.True(t, inner.startCalled)
			assert.Equal(t, tc.wantBudgetUsed, wrapper.scopeAwaitTimeoutUsed)
			mockProvider.AssertExpectations(t)
		})
	}
}

func TestEstimationLimiterWrapper_BudgetLifecycleAcrossNodeGroupsAndScaleUpEnd(t *testing.T) {
	crdScope1 := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "scope-1"}}, "", false, nil, nil)
	ngNoCcc := newTestNodeGroup("mig-no-ccc", "")
	ngScope1A := newTestNodeGroup("mig-scope-1-a", "scope-1")
	ngScope1B := newTestNodeGroup("mig-scope-1-b", "scope-1")

	mockProvider := new(instanceavailability.MockProvider)
	var events []string
	mockProvider.On("AwaitFlexibilityScopeWithTimeout", "scope-1", 3*time.Second).Run(func(_ mock.Arguments) {
		events = append(events, "await")
	}).Return(nil).Twice()

	mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crdScope1}, labels.ComputeClassLabel)
	inner := &fakeEstimationLimiter{
		onStart: func() {
			events = append(events, "inner_start")
		},
	}
	wrapper := NewEstimationLimiterWrapper(inner, mockProvider, mockLister, binpackerBlockTestManager(true, nil))

	// 1. Node group without CCC does not consume budget.
	wrapper.StartEstimation(context.Background(), nil, ngNoCcc, nil)
	assert.False(t, wrapper.scopeAwaitTimeoutUsed)

	// 2. First CCC node group in pass 1 awaits BEFORE inner.StartEstimation and consumes budget.
	wrapper.StartEstimation(context.Background(), nil, ngScope1A, nil)
	assert.True(t, wrapper.scopeAwaitTimeoutUsed)

	// 3. Second CCC node group in pass 1 does not await because budget is already used.
	wrapper.StartEstimation(context.Background(), nil, ngScope1B, nil)
	assert.True(t, wrapper.scopeAwaitTimeoutUsed)

	// 4. OnScaleUpEnd resets the budget for pass 2.
	wrapper.OnScaleUpEnd()
	assert.False(t, wrapper.scopeAwaitTimeoutUsed)

	// 5. First CCC node group in pass 2 awaits again.
	wrapper.StartEstimation(context.Background(), nil, ngScope1A, nil)
	assert.True(t, wrapper.scopeAwaitTimeoutUsed)

	assert.Equal(t, []string{
		"inner_start",
		"await", "inner_start",
		"inner_start",
		"await", "inner_start",
	}, events)
	mockProvider.AssertExpectations(t)
}
