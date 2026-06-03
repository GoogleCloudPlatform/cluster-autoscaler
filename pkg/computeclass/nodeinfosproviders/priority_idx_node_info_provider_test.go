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

package nodeinfosproviders

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

type fakeTemplateNodeInfoProvider struct {
	nodeinfosprovider.TemplateNodeInfoProvider
	returnedNodeInfos map[string]*framework.NodeInfo
	returnedError     errors.AutoscalerError
}

func (m *fakeTemplateNodeInfoProvider) Process(ctx *context.AutoscalingContext, nodes []*apiv1.Node, daemonsets []*appsv1.DaemonSet, taintConfig taints.TaintConfig, now time.Time) (map[string]*framework.NodeInfo, errors.AutoscalerError) {
	return m.returnedNodeInfos, m.returnedError
}

func (m *fakeTemplateNodeInfoProvider) CleanUp() {
}

func TestPriorityIdxNodeInfoProvider_Process(t *testing.T) {
	testCases := []struct {
		name                 string
		baseNodeInfos        map[string]*framework.NodeInfo
		nodeGroup            cloudprovider.NodeGroup
		ccc                  crd.CRD
		baseProviderErr      errors.AutoscalerError
		wantPriorityIdxLabel string
	}{
		{
			name:            "base provider error",
			baseNodeInfos:   nil,
			baseProviderErr: errors.NewAutoscalerError(errors.InternalError, "base error"),
		},
		{
			name: "no node groups",
			baseNodeInfos: map[string]*framework.NodeInfo{
				"node1": framework.NewTestNodeInfo(&apiv1.Node{}),
			},
			nodeGroup: nil, // CloudProvider returns error or nil
		},
		{
			name: "no CCC for node group",
			baseNodeInfos: map[string]*framework.NodeInfo{
				"node1": framework.NewTestNodeInfo(&apiv1.Node{}),
			},
			nodeGroup: gke.NewTestGkeMigBuilder().Build(), // empty labels will return found=false
			ccc:       nil,
		},
		{
			name: "rule not matched",
			baseNodeInfos: map[string]*framework.NodeInfo{
				"node1": framework.NewTestNodeInfo(&apiv1.Node{}),
			},
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{gkelabels.ComputeClassLabel: "not-matched"},
				}).
				Build(),
			ccc:                  crd.NewTestCrd(crd.WithName("not-matched"), crd.WithLabel(gkelabels.ComputeClassLabel)),
			wantPriorityIdxLabel: "-1",
		},
		{
			name: "rule matched, label injected",
			baseNodeInfos: map[string]*framework.NodeInfo{
				"node1": framework.NewTestNodeInfo(&apiv1.Node{}),
			},
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					Labels: map[string]string{gkelabels.ComputeClassLabel: "matched"},
				}).
				SetNodePoolName("matched-np").
				Build(),
			ccc: crd.NewTestCrd(
				crd.WithName("matched"),
				crd.WithLabel(gkelabels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"other-1"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"other-2"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"other-3"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"matched-np"})),
				}),
			),
			wantPriorityIdxLabel: "3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeBaseProvider := &fakeTemplateNodeInfoProvider{}

			var crds []crd.CRD
			if tc.ccc != nil {
				crds = append(crds, tc.ccc)
			}
			mockLister := lister.NewMockCrdListerWithLabel(crds, gkelabels.ComputeClassLabel)
			if tc.ccc != nil {
				mockLister.SetDefaultCrdName(tc.ccc.Name())
			}

			matcher := computeclass.NewMatcher(mockLister, &computeclass.MockGKEProvider{})
			provider := NewPriorityIdxNodeInfoProvider(fakeBaseProvider, matcher, mockLister)

			ctx := &context.AutoscalingContext{}
			nodes := []*apiv1.Node{}
			daemonsets := []*appsv1.DaemonSet{}
			taintConfig := taints.TaintConfig{}
			now := time.Now()

			mockCloudProvider := &gke.GkeCloudProviderMock{}
			ctx.CloudProvider = mockCloudProvider

			fakeBaseProvider.returnedNodeInfos = tc.baseNodeInfos
			fakeBaseProvider.returnedError = tc.baseProviderErr

			if tc.baseProviderErr == nil && tc.baseNodeInfos != nil {
				for _, nodeInfo := range tc.baseNodeInfos {
					mockCloudProvider.On("NodeGroupForNode", nodeInfo.Node()).Return(tc.nodeGroup, nil)
				}
			}

			res, err := provider.Process(ctx, nodes, daemonsets, taintConfig, now)

			if tc.baseProviderErr != nil {
				assert.Equal(t, tc.baseProviderErr, err, "Base provider error should be returned")
			} else {
				assert.NoError(t, err, "Base provider error should not be returned")
				for _, nodeInfo := range res {
					assert.Equal(t, tc.wantPriorityIdxLabel, nodeInfo.Node().Labels[labels.ComputeClassPriorityIdxLabel], "Priority idx label must match expected value")
				}
			}
		})
	}
}
