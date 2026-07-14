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

package conditions

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateNodePoolNotExistReason(t *testing.T) {
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)
	testCases := []struct {
		name          string
		crd           crd.CRD
		migs          []*gke.GkeMig
		wantCondition bool
		wantMessage   string
	}{
		{
			name: "crd without rules",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "no migs in cluster",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantCondition: true,
			wantMessage:   fmt.Sprintf(NodePoolNotExistMessage, "nodepool-1"),
		},
		{
			name: "nodepool doesn't exist in cluster",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
			wantMessage:   fmt.Sprintf(NodePoolNotExistMessage, "nodepool-1"),
		},
		{
			name: "all nodepools exists in cluster",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "only one nodepool is present",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
			wantMessage:   fmt.Sprintf(NodePoolNotExistMessage, "nodepool-2"),
		},
		{
			name: "2 nodepools rule but only one nodepool present",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
			wantMessage:   fmt.Sprintf(NodePoolNotExistMessage, "nodepool-2"),
		},
		{
			name: "2 nodepools rule and both nodepools present",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-1": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			migsMap := map[string]*gke.GkeMig{}
			for _, mig := range tc.migs {
				migsMap[mig.NodePoolName()] = mig
			}
			checker := &nodePoolNotExistCheck{}
			condition := checker.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, CrdMisconfiguredCondition, condition.Type)
				assert.Equal(t, NodePoolNotExistReason, condition.Reason)
				assert.Equal(t, tc.wantMessage, condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
