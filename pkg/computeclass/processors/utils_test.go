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

package processors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestGetFirstMatchingRule(t *testing.T) {
	testCrdLabel := "test-crd-label"
	testCases := []struct {
		name          string
		crds          []crd.CRD
		nodeGroup     cloudprovider.NodeGroup
		wantErr       bool
		wantRuleIndex int
	}{
		{
			name: "default",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),
			},
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetNodePoolName("nodepool-2").
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-object-1"},
					MachineType: "machine-type",
					Spot:        true}).
				Build(),
			wantRuleIndex: 1,
		},
		{
			name:    "nil nodegroup",
			wantErr: true,
		},
		{
			name:      "crd label not present",
			nodeGroup: buildMigWithCrdLabelError(),
			wantErr:   true,
		},
		{
			name: "crd not found",
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetNodePoolName("nodepool-1").
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-object-1"},
					MachineType: "machine-type",
					Spot:        true}).
				Build(),
			wantErr: true,
		},
		{
			name: "nodepool should not scale up",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					})),
			},
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetNodePoolName("nodepool-3").
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-object-1"},
					MachineType: "machine-type",
					Spot:        true}).
				Build(),
			wantErr: true,
		},
		{
			name: "crd without priorties - ScaleUpAnyway",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			},
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetNodePoolName("nodepool-3").
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-object-1"},
					MachineType: "machine-type",
					Spot:        true}).
				Build(),
			wantRuleIndex: 0,
		},
		{
			name: "no matching rule - ScaleUpAnyway",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),
			},
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetNodePoolName("nodepool-3").
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-object-1"},
					MachineType: "machine-type",
					Spot:        true}).
				Build(),
			wantRuleIndex: 2, // total rules (rules from 0-1)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			mockLister.SetCrdLabel(testCrdLabel)
			mockProvider := NewMockCloudProvider()
			mockProvider.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
			mockProvider.On("IsAutopilotEnabled").Return(false)
			matcher := computeclass.NewMatcher(mockLister, mockProvider)
			ruleIndex, _, err := getRuleIndexForMetrics(tc.nodeGroup, mockLister, matcher)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, ruleIndex, tc.wantRuleIndex)
			}
		})
	}
}

func buildMigWithCrdLabelError() *gke.GkeMig {
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)
	mig := gke.NewTestGkeMigBuilder().
		SetGkeManager(gkeManager).
		Build()
	node := v1.Node{}
	gkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), errors.New("error"))
	return mig
}

func buildMigWithDefaultCrd() *gke.GkeMig {
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)
	mig := gke.NewTestGkeMigBuilder().
		SetGkeManager(gkeManager).
		Build()
	node := v1.Node{}
	gkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), nil)
	return mig
}
