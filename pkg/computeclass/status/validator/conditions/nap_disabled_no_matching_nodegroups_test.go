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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateNapDisabledAndNoMatchingNodegroupsReason(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)
	testCases := []struct {
		name          string
		crd           crd.CRD
		migs          []*gke.GkeMig
		wantCondition bool
	}{
		{
			name: "crd with nap enabled",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "crd with nap disabled and no migs in cluster",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway()),
			wantCondition: true,
		},
		{
			name: "crd with DoNotScaleUp, nap disabled and no migs matching",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}),
			),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
		},
		{
			name: "crd with ScaleUpAnyway, no matching migs",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-xyz").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-non-matching"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
		},
		{
			name: "crd with ScaleUpAnyway, no rule matching but nodepool matches with ScaleUpAnyway",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "crd with nap disabled and 1 mig matching",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
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
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			provider := newTestProvider().
				Build()
			matcher := computeclass.NewMatcher(mockCrdLister, provider)
			migsMap := map[string]*gke.GkeMig{}
			for _, mig := range tc.migs {
				migsMap[mig.NodePoolName()] = mig
			}
			ch := &napDisabledAndNoMatchingNodegroupsCheck{matcher: matcher}
			condition := ch.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, CrdMisconfiguredCondition, condition.Type)
				assert.Equal(t, NapDisabledAndNoMatchingNodegroupsReason, condition.Reason)
				assert.Equal(t, NapDisabledAndNoMatchingNodegroupsMessage, condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
