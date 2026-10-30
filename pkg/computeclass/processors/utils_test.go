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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

func TestGetRuleIndex(t *testing.T) {
	testCrdLabel := "test-crd-label"
	testCases := []struct {
		name          string
		crds          []crd.CRD
		nodeGroup     cloudprovider.NodeGroup
		wantErr       bool
		wantNilCrd    bool
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
			name:       "nodegroup without crd label returns zero and nil crd",
			nodeGroup:  buildMigWithDefaultCrd(),
			wantNilCrd: true,
		},
		{
			name:    "nil nodegroup",
			wantErr: true,
		},
		{
			name:      "crd label error",
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
			name: "crd without priorities - ScaleUpAnyway",
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
			ruleIndex, matchedCrd, err := getRuleIndexForMetrics(tc.nodeGroup, mockLister, matcher)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, matchedCrd)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantRuleIndex, ruleIndex)
				if tc.wantNilCrd {
					assert.Nil(t, matchedCrd)
				} else {
					assert.Equal(t, tc.crds[0], matchedCrd)
				}
			}

			rawRuleIndex, rawMatchedCrd, rawErr := GetRuleIndex(tc.nodeGroup, mockLister, matcher)
			if tc.wantErr {
				assert.Error(t, rawErr)
				assert.Nil(t, rawMatchedCrd)
			} else {
				assert.NoError(t, rawErr)
				assert.Equal(t, tc.wantRuleIndex, rawRuleIndex)
				if tc.wantNilCrd {
					assert.Nil(t, rawMatchedCrd)
				} else {
					assert.Equal(t, tc.crds[0], rawMatchedCrd)
				}
			}
		})
	}
}

func TestGetRuleIndexClamping(t *testing.T) {
	testCrdLabel := "test-crd-label"
	manyRules := make([]rules.Rule, maxRuleIndex+2)
	for i := range manyRules {
		manyRules[i] = rules.NewRule(rules.WithNodePoolsRule([]string{"other-nodepool"}))
	}
	manyRules[maxRuleIndex-1] = rules.NewRule(rules.WithNodePoolsRule([]string{"pool-below-max"}))
	manyRules[maxRuleIndex] = rules.NewRule(rules.WithNodePoolsRule([]string{"pool-at-max"}))
	manyRules[maxRuleIndex+1] = rules.NewRule(rules.WithNodePoolsRule([]string{"pool-above-max"}))

	crds := []crd.CRD{
		crd.NewTestCrd(
			crd.WithLabel(testCrdLabel),
			crd.WithName("crd-many-rules"),
			crd.WithRules(manyRules),
			crd.WithScaleUpAnyway(),
		),
	}

	mockLister := lister.NewMockCrdLister(crds)
	mockLister.SetCrdLabel(testCrdLabel)
	mockProvider := NewMockCloudProvider()
	mockProvider.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
	mockProvider.On("IsAutopilotEnabled").Return(false)
	matcher := computeclass.NewMatcher(mockLister, mockProvider)

	testCases := []struct {
		name           string
		nodePoolName   string
		wantRawIdx     int
		wantMetricsIdx int
	}{
		{
			name:           "below maxRuleIndex",
			nodePoolName:   "pool-below-max",
			wantRawIdx:     maxRuleIndex - 1,
			wantMetricsIdx: maxRuleIndex - 1,
		},
		{
			name:           "at maxRuleIndex",
			nodePoolName:   "pool-at-max",
			wantRawIdx:     maxRuleIndex,
			wantMetricsIdx: maxRuleIndex,
		},
		{
			name:           "above maxRuleIndex clamped",
			nodePoolName:   "pool-above-max",
			wantRawIdx:     maxRuleIndex + 1,
			wantMetricsIdx: maxRuleIndex,
		},
		{
			name:           "ScaleUpAnyway fallback clamped",
			nodePoolName:   "unmatched-nodepool",
			wantRawIdx:     len(manyRules),
			wantMetricsIdx: maxRuleIndex,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeGroup := gke.NewTestGkeMigBuilder().
				SetNodePoolName(tc.nodePoolName).
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{testCrdLabel: "crd-many-rules"},
					MachineType: "machine-type",
					Spot:        true,
				}).
				Build()

			rawIdx, matchedCrd, err := GetRuleIndex(nodeGroup, mockLister, matcher)
			assert.NoError(t, err)
			assert.Equal(t, crds[0], matchedCrd)
			assert.Equal(t, tc.wantRawIdx, rawIdx)

			metricsIdx, matchedCrd, err := getRuleIndexForMetrics(nodeGroup, mockLister, matcher)
			assert.NoError(t, err)
			assert.Equal(t, crds[0], matchedCrd)
			assert.Equal(t, tc.wantMetricsIdx, metricsIdx)
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

func TestBuildFakePod(t *testing.T) {
	t.Run("top-level CCC fake pod uses default namespace", func(t *testing.T) {
		pod := buildFakePod("my-ccc", nil, 0)
		assert.Equal(t, metav1.NamespaceDefault, pod.Namespace)
		assert.Equal(t, "min-nodes-fake-ccc-pod-my-ccc-0", pod.Name)
		assert.True(t, IsMinCapacityFakePod(pod))
		assert.Equal(t, "my-ccc", pod.Spec.NodeSelector[labels.ComputeClassLabel])
		assert.NotContains(t, pod.Spec.NodeSelector, labels.ComputeClassPriorityIdxLabel)
	})

	t.Run("priority rule fake pod uses default namespace", func(t *testing.T) {
		priorityIdx := 1
		pod := buildFakePod("my-ccc", &priorityIdx, 2)
		assert.Equal(t, metav1.NamespaceDefault, pod.Namespace)
		assert.Equal(t, "min-nodes-fake-priority-pod-my-ccc-1-2", pod.Name)
		assert.True(t, IsMinCapacityFakePod(pod))
		assert.Equal(t, "my-ccc", pod.Spec.NodeSelector[labels.ComputeClassLabel])
		assert.Equal(t, "1", pod.Spec.NodeSelector[labels.ComputeClassPriorityIdxLabel])
	})
}
