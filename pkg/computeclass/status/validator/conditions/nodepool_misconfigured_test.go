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
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateNoRuleMatchingReason(t *testing.T) {
	testCrdLabel := "test-crd-label"
	machineFamily := "e2"
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)
	testCases := []struct {
		name          string
		crd           crd.CRD
		migs          []*gke.GkeMig
		wantCondition bool
	}{
		{
			name: "crd without rules - but ScaleUpAnyway matches",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
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
			name: "crd without rules - DoNotScaleUp",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{})),
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
			wantCondition: true,
		},
		{
			name: "no migs in cluster",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway()),
			wantCondition: false,
		},
		{
			name: "migs related to crd matches rule",
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
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "1 mig doesn't match to any rule - but ScaleUpAnyway matches",
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
		{
			name: "1 mig with machine family doesn't match to any rule - DoNotScaleUp",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&machineFamily, nil, nil, nil)})),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "e2-standard-4",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "n2-standard-8",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
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
			checker := &noRuleMatchingCheck{matcher: matcher}
			condition := checker.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, NodepoolMisconfiguredCondition, condition.Type)
				assert.Equal(t, NoRuleMatchingReason, condition.Reason)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}

func TestValidateCrdLabelNotMatchingReason(t *testing.T) {
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
			name: "crd without rules",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
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
			name: "crd label not matching",
			crd: crd.NewTestCrd(crd.WithLabel("crd-2"),
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
			},
			wantCondition: true,
		},
		{
			name: "crd label matching",
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
			},
			wantCondition: false,
		},
		{
			name: "mig without crd label",
			crd: crd.NewTestCrd(crd.WithLabel("crd-2"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
		},
		{
			name: "only one mig matches",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
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
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-2": "crd-object-1"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := v1.Node{}
			migsMap := map[string]*gke.GkeMig{}
			for _, mig := range tc.migs {
				defaultGkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), nil)
				migsMap[mig.NodePoolName()] = mig
			}
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			provider := newTestProvider().
				Build()
			matcher := computeclass.NewMatcher(mockCrdLister, provider)
			checker := &crdLabelNotMatchingCheck{matcher: matcher}
			condition := checker.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, NodepoolMisconfiguredCondition, condition.Type)
				assert.Equal(t, CrdLabelNotMatchingReason, condition.Reason)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}

func TestValidatemultipleCrdTaintsReason(t *testing.T) {
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
			name: "1 mig with no crd taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
					}).
					SetNodePoolName("nodepool-1").
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "1 mig with 1 crd taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
					}).
					SetNodePoolName("nodepool-1").
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "1 mig with 2 different crd taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
					}).
					SetNodePoolName("nodepool-1").
					Build(),
			},
			wantCondition: true,
		},
		{
			name: "1 mig with 2 similar crd taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
					}).
					SetNodePoolName("nodepool-1").
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "2 migs where 1 has crd taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
					}).
					SetNodePoolName("nodepool-1").
					Build(),
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: "non-crd-taint", Value: "crd-object-1"},
						},
					}).
					SetNodePoolName("nodepool-2").
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "2 migs - one with single crd taint and other with multiple crd taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
					}).
					SetNodePoolName("nodepool-1").
					Build(),
				gke.NewTestGkeMigBuilder().
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
					}).
					SetNodePoolName("nodepool-2").
					Build(),
			},
			wantCondition: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := v1.Node{}
			migsMap := map[string]*gke.GkeMig{}
			for _, mig := range tc.migs {
				defaultGkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), nil)
				migsMap[mig.NodePoolName()] = mig
			}
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			provider := newTestProvider().
				Build()
			checker := &multipleCrdTaintsCheck{
				matcher: computeclass.NewMatcher(mockCrdLister, provider),
			}
			condition := checker.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, NodepoolMisconfiguredCondition, condition.Type)
				assert.Equal(t, MultipleCrdTaintsReason, condition.Reason)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}

func TestValidateTaintMissingReason(t *testing.T) {
	testCrdLabel := "test-crd"
	defaultCrdName := "test-default-crd"
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)
	testCases := []struct {
		name          string
		crd           crd.CRD
		migs          []*gke.GkeMig
		wantCondition bool
	}{
		{
			name: "default crd - missing label and taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "default crd - default label, no taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: defaultCrdName},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "taint missing in mig",
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
			},
			wantCondition: true,
		},
		{
			name: "all migs have crd taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "one mig missing taint",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
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
			wantCondition: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := v1.Node{}
			migsMap := map[string]*gke.GkeMig{}
			for _, mig := range tc.migs {
				defaultGkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), nil)
				migsMap[mig.NodePoolName()] = mig
			}
			mockLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockLister.SetCrdLabel(testCrdLabel)
			mockLister.SetDefaultCrdName(defaultCrdName)
			provider := newTestProvider().
				Build()
			matcher := computeclass.NewMatcher(mockLister, provider)
			ch := &taintMissingCheck{lister: mockLister, matcher: matcher}
			condition := ch.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, NodepoolMisconfiguredCondition, condition.Type)
				assert.Equal(t, TaintMissingReason, condition.Reason)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}

func TestValidateTaintValueNotMatchingReason(t *testing.T) {
	testCrdLabel := "test-crd"
	defaultCrdName := "test-default-crd"
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)
	testCases := []struct {
		name          string
		crd           crd.CRD
		migs          []*gke.GkeMig
		wantCondition bool
	}{
		{
			name: "default crd - missing label",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "default crd - default label",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: defaultCrdName},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "taint value not matching",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
		},
		{
			name: "all migs crd taint matches to crd name",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
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
		{
			name: "only one mig crd taint matches to crd name",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := v1.Node{}
			migsMap := map[string]*gke.GkeMig{}
			for _, mig := range tc.migs {
				defaultGkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), nil)
				migsMap[mig.NodePoolName()] = mig
			}
			mockLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockLister.SetCrdLabel(testCrdLabel)
			mockLister.SetDefaultCrdName(defaultCrdName)
			provider := newTestProvider().
				Build()
			matcher := computeclass.NewMatcher(mockLister, provider)
			checker := &taintValueNotMatchingCheck{
				lister:  mockLister,
				matcher: matcher,
			}
			condition := checker.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, NodepoolMisconfiguredCondition, condition.Type)
				assert.Equal(t, TaintValueNotMatchingReason, condition.Reason)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}

func TestValidateNodepoolWillNeverScaleUpReason(t *testing.T) {
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
			name: "crd without rules",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-2"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "no migs in cluster",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			wantCondition: false,
		},
		{
			name: "migs related to crd matches rule",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway()),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-2"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: false,
		},
		{
			name: "1 mig doesn't match to any rule and crd with DoNotScaleUp",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}),
			),
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-1"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			wantCondition: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			provider := newTestProvider().
				Build()
			migsMap := map[string]*gke.GkeMig{}
			for _, mig := range tc.migs {
				migsMap[mig.NodePoolName()] = mig
			}
			matcher := computeclass.NewMatcher(mockCrdLister, provider)
			ch := &nodepoolWillNeverScaleUpCheck{matcher: matcher}
			condition := ch.checkCrd(tc.crd, migsMap)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, NodepoolMisconfiguredCondition, condition.Type)
				assert.Equal(t, NodepoolWillNeverScaleUpReason, condition.Reason)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
