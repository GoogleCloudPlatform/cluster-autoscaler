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

package validator

import (
	"fmt"
	"testing"
	"time"

	ccc_clientset "github.com/googlecloudplatform/compute-class-api/client/clientset/versioned"
	fakecccclientset "github.com/googlecloudplatform/compute-class-api/client/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/validator/conditions"
)

const emptyConfig = ""

type fakeClient struct{}

func (c *fakeClient) CccClient() ccc_clientset.Interface {
	return fakecccclientset.NewSimpleClientset()
}

func TestValidateDefaultCrd(t *testing.T) {
	testCrdLabel := "test-crd"
	defaultCrdName := "default-test-crd"
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name           string
		migs           []*gke.GkeMig
		crd            crd.CRD
		wantConditions []metav1.Condition
	}{
		{
			name: "nodepool doesn't exist in cluster",
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
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2"})),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			wantConditions: []metav1.Condition{
				*conditions.NodePoolNotExistCondition("nodepool-2"),
			},
		},
		{
			name: "nodepool with default Crd label matches ScaleUpAnyway",
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
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
		},
		{
			name: "nodepool without label and empty Crd - DoNotScaleUp",
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithAutoprovisioningEnabled()),
			wantConditions: []metav1.Condition{
				// Only first condition of given type (NodePoolMisconfigured) is emitted.
				*conditions.NoRuleMatchingCondition("nodepool-1"),
			},
		},
		{
			name: "no migs",
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			wantConditions: []metav1.Condition{
				*conditions.NapDisabledAndNoMatchingNodegroupsCondition(),
			},
		},
		{
			name: "nodepool without label not matching PR - CRD with ScaleUpAnyway",
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: defaultCrdName},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-3"})),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
		},
		{
			name: "default Crd with DoNotScaleUp",
			migs: []*gke.GkeMig{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName(defaultCrdName),
				crd.WithRules([]rules.Rule{}), crd.WithAutoprovisioningEnabled()),
			wantConditions: []metav1.Condition{
				// Only first condition of given type (NodePoolMisconfigured) is emitted.
				*conditions.NoRuleMatchingCondition("nodepool-1"),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := v1.Node{}
			for _, mig := range tc.migs {
				defaultGkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), nil)
			}
			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{tc.crd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			mockCrdLister.SetDefaultCrdName(defaultCrdName)
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningDefaultFamily(machinetypes.E2).
				WithAutoprovisioningEnabled(true).
				Build()
			validator, _ := NewValidator(nil, mockCrdLister, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, nil, false)
			migsMap := make(map[string]*gke.GkeMig)
			for _, mig := range tc.migs {
				migsMap[mig.NodePoolName()] = mig
			}
			conditions := []metav1.Condition{}
			conditions = append(conditions, validator.evaluator.GetCRDConditions(tc.crd, migsMap)...)
			now := time.Now()
			for i := range conditions {
				conditions[i].LastTransitionTime = metav1.NewTime(now)
			}
			for i := range tc.wantConditions {
				tc.wantConditions[i].LastTransitionTime = metav1.NewTime(now)
			}
			assert.ElementsMatch(t, conditions, tc.wantConditions)
		})
	}
}

func TestLoopUpdatesConditions(t *testing.T) {
	testCrdLabel := "test-crd"
	defaultCrdName := "default-test-crd"
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)
	otherConditions := []metav1.Condition{
		{
			Type:               "other-condition",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "abc",
			Message:            "xyz",
		},
	}
	testCases := []struct {
		name           string
		migs           []*gke.GkeMig
		crds           []crd.CRD
		wantConditions [][]metav1.Condition // for every crd
		napEnabled     bool
	}{
		{
			name: "default case",
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
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-2"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-taint-not-matching"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-4").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-2"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-2"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-5").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-3"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: "crd-object-3"},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-6").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-7").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "non-matching"},
						Taints: []v1.Taint{
							{Key: testCrdLabel, Value: ""},
						},
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-8").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "machine-type",
						Spot:        true}).
					SetGkeManager(defaultGkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),

				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-2"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-3"})),
					}), crd.WithScaleUpAnyway()),

				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-3"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-5"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-6"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-7", "nodepool-9"})),
					}), crd.WithScaleUpAnyway()),

				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultCrdName),
					crd.WithRules([]rules.Rule{})),
			},
			wantConditions: [][]metav1.Condition{
				{
					*conditions.CrdHealthyCondition(),
				},
				{
					*conditions.TaintValueNotMatchingCondition("nodepool-3"),
					*conditions.CrdNotHealthyCondition(),
				},
				{
					// Only first condition of given type (CRDMisconfigured) is emitted.
					*conditions.CrdLabelNotMatchingCondition("nodepool-7"),
					*conditions.NodePoolNotExistCondition("nodepool-9"),
					*conditions.CrdNotHealthyCondition(),
				},
				{
					// Only first condition of given type (CRDMisconfigured) is emitted.
					*conditions.NoRuleMatchingCondition("nodepool-8"),
					*conditions.NapDisabledAndNoMatchingNodegroupsCondition(),
					*conditions.CrdNotHealthyCondition(),
				},
			},
			napEnabled: true,
		},
		{
			name: "NAP not enabled in cluster but in Crd",
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
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantConditions: [][]metav1.Condition{
				{
					*conditions.CrdNotHealthyCondition(),
					*conditions.NapCannotBeEnabledCondition(),
				},
			},
			napEnabled: false,
		},
		{
			name: "crd conditions set by other component",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithConditions(otherConditions)),
			},
			wantConditions: [][]metav1.Condition{
				{
					*conditions.CrdNotHealthyCondition(),
					*conditions.NapDisabledAndNoMatchingNodegroupsCondition(),
					otherConditions[0],
				},
			},
			napEnabled: true,
		},
		{
			name: "crd with existing health condition is not duplicated",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithConditions([]metav1.Condition{
						*conditions.CrdHealthyCondition(),
					})),
			},
			wantConditions: [][]metav1.Condition{
				{
					*conditions.CrdNotHealthyCondition(),
					*conditions.NapDisabledAndNoMatchingNodegroupsCondition(),
				},
			},
			napEnabled: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := v1.Node{}
			for _, mig := range tc.migs {
				defaultGkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(&node), nil)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningDefaultFamily(machinetypes.E2).
				WithGkeMigs(tc.migs).
				WithAutoprovisioningEnabled(tc.napEnabled).
				Build()
			mockCrdLister := lister.NewMockCrdLister(tc.crds)
			mockCrdLister.SetCrdLabel(testCrdLabel)
			mockCrdLister.SetDefaultCrdName(defaultCrdName)
			client := &fakeClient{}
			validator, _ := NewValidator(client, mockCrdLister, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, nil, false)
			time := metav1.Now()
			validator.loop()
			for i := range tc.crds {
				gotConditions := tc.crds[i].Conditions()
				assert.Equal(t, len(tc.wantConditions[i]), len(gotConditions))
				for j := range tc.wantConditions[i] {
					tc.wantConditions[i][j].LastTransitionTime = time
					gotConditions[j].LastTransitionTime = time
				}
				assert.ElementsMatch(t, tc.wantConditions[i], gotConditions)
			}
		})
	}
}

func TestAnyConditionsChanged(t *testing.T) {
	testCases := []struct {
		name          string
		crd           crd.CRD
		newConditions []metav1.Condition
		wantChange    bool
	}{
		{
			name:       "empty new conditions",
			crd:        crd.NewTestCrd(),
			wantChange: false,
		},
		{
			name: "new condition added",
			crd:  crd.NewTestCrd(),
			newConditions: []metav1.Condition{
				*conditions.CrdNotHealthyCondition(),
			},
			wantChange: true,
		},
		{
			name: "condition changed",
			crd: crd.NewTestCrd(crd.WithConditions([]metav1.Condition{
				*conditions.CrdHealthyCondition(),
			})),
			newConditions: []metav1.Condition{
				*conditions.CrdNotHealthyCondition(),
			},
			wantChange: true,
		},
		{
			name: "condition not changed",
			crd: crd.NewTestCrd(crd.WithConditions([]metav1.Condition{
				*conditions.CrdNotHealthyCondition(),
			})),
			newConditions: []metav1.Condition{
				*conditions.CrdNotHealthyCondition(),
			},
			wantChange: false,
		},
		{
			name: "conditions with same type but different reason",
			crd: crd.NewTestCrd(crd.WithConditions([]metav1.Condition{
				*conditions.NapCannotBeEnabledCondition(),
			})),
			newConditions: []metav1.Condition{
				*conditions.NodePoolNotExistCondition("nodepool-1"),
			},
			wantChange: true,
		},
		{
			name: "conditions with same type and reason but different message",
			crd: crd.NewTestCrd(crd.WithConditions([]metav1.Condition{
				*conditions.NodePoolNotExistCondition("nodepool-1"),
			})),
			newConditions: []metav1.Condition{
				*conditions.NodePoolNotExistCondition("nodepool-2"),
			},
			wantChange: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningEnabled(true).
				Build()
			validator, _ := NewValidator(nil, nil, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, nil, false)
			assert.Equal(t, tc.wantChange, validator.anyConditionsChanged(tc.crd, tc.newConditions))
		})
	}
}

func TestAnyRuleConditionsChanged(t *testing.T) {
	ruleIdx := "1"
	stableCondition := *conditions.NodePoolNotExistCondition("np1")
	testCases := []struct {
		name          string
		crd           crd.CRD
		newConditions []metav1.Condition
		wantChange    bool
	}{
		{
			name:          "empty new conditions, empty existing",
			crd:           crd.NewTestCrd(),
			newConditions: []metav1.Condition{},
			wantChange:    false,
		},
		{
			name: "new condition added",
			crd:  crd.NewTestCrd(),
			newConditions: []metav1.Condition{
				*conditions.NodePoolNotExistCondition("np1"),
			},
			wantChange: true,
		},
		{
			name: "condition changed status",
			crd: crd.NewTestCrd(crd.WithRuleConditions(ruleIdx, []metav1.Condition{
				{
					Type:   conditions.NodepoolMisconfiguredCondition,
					Status: metav1.ConditionFalse,
					Reason: conditions.NodePoolNotExistReason,
				},
			})),
			newConditions: []metav1.Condition{
				{
					Type:   conditions.NodepoolMisconfiguredCondition,
					Status: metav1.ConditionTrue,
					Reason: conditions.NodePoolNotExistReason,
				},
			},
			wantChange: true,
		},
		{
			name: "condition not changed",
			crd: crd.NewTestCrd(crd.WithRuleConditions(ruleIdx, []metav1.Condition{
				stableCondition,
			})),
			newConditions: []metav1.Condition{
				stableCondition,
			},
			wantChange: false,
		},
		{
			name: "conditions with same type but different reason",
			crd: crd.NewTestCrd(crd.WithRuleConditions(ruleIdx, []metav1.Condition{
				{
					Type:   conditions.NodepoolMisconfiguredCondition,
					Status: metav1.ConditionTrue,
					Reason: "OldReason",
				},
			})),
			newConditions: []metav1.Condition{
				{
					Type:   conditions.NodepoolMisconfiguredCondition,
					Status: metav1.ConditionTrue,
					Reason: conditions.NodePoolNotExistReason,
				},
			},
			wantChange: true,
		},
		{
			name: "only other component conditions",
			crd: crd.NewTestCrd(crd.WithRuleConditions(ruleIdx, []metav1.Condition{
				{
					Type:   conditions.NodepoolMisconfiguredCondition,
					Status: metav1.ConditionTrue,
					Reason: "OldReason",
				},
			})),
			newConditions: []metav1.Condition{},
			wantChange:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			validator, _ := NewValidator(nil, nil, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, nil, false)
			assert.Equal(t, tc.wantChange, validator.anyRuleConditionsChanged(ruleIdx, tc.newConditions, tc.crd))
		})
	}
}

func TestRuleConditionsEmittedCorrectly(t *testing.T) {
	testCrdLabel := "test-crd"
	defaultGkeManager := &gke.GkeManagerMock{}
	defaultGkeManager.On("IsDataplaneV2Enabled").Return(true)
	badSysctlValue := "invalid-value"
	sysctls := map[string]string{"net.ipv4.tcp_rmem": badSysctlValue}

	testCases := []struct {
		name                   string
		rules                  []rules.Rule
		existingRuleConditions map[string][]metav1.Condition
		wantUpdate             bool
		wantConditionType      string
		wantConditionReason    string
		wantConditionMessage   string
	}{
		{
			name: "invalid sysctl emits condition",
			rules: []rules.Rule{
				rules.NewRule(rules.WithSysctlsRule(sysctls)),
			},
			existingRuleConditions: nil,
			wantUpdate:             true,
			wantConditionType:      conditions.RuleMisconfiguredCondition,
			wantConditionReason:    conditions.UnsupportedNodeSystemConfigFormatReason,
			wantConditionMessage:   badSysctlValue,
		},
		{
			name: "existing identical condition does not emit update",
			rules: []rules.Rule{
				rules.NewRule(rules.WithSysctlsRule(sysctls)),
			},
			existingRuleConditions: map[string][]metav1.Condition{
				"0": {
					{
						Type:    conditions.RuleMisconfiguredCondition,
						Status:  metav1.ConditionTrue,
						Reason:  conditions.UnsupportedNodeSystemConfigFormatReason,
						Message: fmt.Sprintf(conditions.UnsupportedSysctlsFormatMessage, "net.ipv4.tcp_rmem", badSysctlValue, "min,default,max"),
					},
				},
			},
			wantUpdate: false,
		},
		{
			name: "existing unrelated condition is preserved and new one added",
			rules: []rules.Rule{
				rules.NewRule(rules.WithSysctlsRule(sysctls)),
			},
			existingRuleConditions: map[string][]metav1.Condition{
				"0": {
					{
						Type:    "OtherCondition",
						Status:  metav1.ConditionTrue,
						Reason:  "OtherReason",
						Message: "OtherMessage",
					},
				},
			},
			wantUpdate:           true,
			wantConditionType:    conditions.RuleMisconfiguredCondition,
			wantConditionReason:  conditions.UnsupportedNodeSystemConfigFormatReason,
			wantConditionMessage: badSysctlValue,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := []crd.TestCrdOption{
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules(tc.rules),
				crd.WithScaleUpAnyway(),
				crd.WithAutoprovisioningEnabled(),
			}

			if tc.existingRuleConditions != nil {
				for ruleIdx, conditions := range tc.existingRuleConditions {
					opts = append(opts, crd.WithRuleConditions(ruleIdx, conditions))
				}
			}

			testCrd := crd.NewTestCrd(opts...)

			mockCrdLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
			mockCrdLister.SetCrdLabel(testCrdLabel)
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningDefaultFamily(machinetypes.E2).
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			updatesCh := make(chan status.UpdateMessage, 10)
			validator, _ := NewValidator(nil, mockCrdLister, provider, computeclass.NewMockMetrics(), nil, nil, nil, emptyConfig, updatesCh, true)

			go validator.loop()

			timeout := time.After(2 * time.Second)
			if tc.wantUpdate {
				found := false
				for !found {
					select {
					case msg := <-updatesCh:
						mockStatus := crd.NewMockCRDStatus(tc.existingRuleConditions)
						msg.Mutate(mockStatus)
						if mockStatus.UpdateRuleConditionsCalled {
							if mockStatus.RuleIdx == "0" {
								// Check preservation of existing conditions
								if tc.existingRuleConditions != nil {
									for _, existing := range tc.existingRuleConditions["0"] {
										assert.Contains(t, mockStatus.Conditions, existing, "Expected existing condition to be preserved")
									}
								}
								// Check new condition
								for _, cond := range mockStatus.Conditions {
									if cond.Type == tc.wantConditionType &&
										cond.Reason == tc.wantConditionReason &&
										len(cond.Message) > 0 &&
										(len(tc.wantConditionMessage) == 0 || contains(cond.Message, tc.wantConditionMessage)) {
										found = true
									}
								}
							}
						}
					case <-timeout:
						t.Fatal("Expected update message, got none or timed out waiting for rule condition")
					}
				}
			} else {
				select {
				case msg := <-updatesCh:
					mockStatus := crd.NewMockCRDStatus(nil)
					msg.Mutate(mockStatus)
					if mockStatus.UpdateRuleConditionsCalled && mockStatus.RuleIdx == "0" {
						t.Fatalf("Expected no update message, but got one: %v", mockStatus.Conditions)
					}
				case <-time.After(1 * time.Second):
					// No message received as expected
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[0:len(substr)] == substr || (len(s) > len(substr) && contains(s[1:], substr))
}
