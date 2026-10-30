/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourcequota

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
)

const cccPriorityIndexAnnotationKey = labels.CCCPriorityIndexAnnotationKey

func TestRuleMinCapacityQuotaProvider_Quotas(t *testing.T) {
	testCases := []struct {
		name           string
		crds           []crd.CRD
		expectedQuotas []struct {
			id     string
			limits map[string]int64
		}
	}{
		{
			name: "spec-level CCC min capacity is not a quota",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			expectedQuotas: []struct {
				id     string
				limits map[string]int64
			}{},
		},
		{
			name: "rule-level CCC target",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("priority-ccc"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(4))),
					}),
				),
			},
			expectedQuotas: []struct {
				id     string
				limits map[string]int64
			}{
				{
					id:     "cc-min-nodes-priority-ccc-rule-0",
					limits: map[string]int64{resourcequotas.ResourceNodes: 4},
				},
			},
		},
		{
			name: "nothing specified",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("empty-ccc"),
				),
			},
			expectedQuotas: []struct {
				id     string
				limits map[string]int64
			}{},
		},
		{
			name: "only the rule-level min capacity of both is a quota",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("both-ccc"),
					crd.WithTargetNodeCount(intPtr(5)),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(3))),
					}),
				),
			},
			expectedQuotas: []struct {
				id     string
				limits map[string]int64
			}{
				{
					id:     "cc-min-nodes-both-ccc-rule-0",
					limits: map[string]int64{resourcequotas.ResourceNodes: 3},
				},
			},
		},
		{
			name: "multiple rules with targets",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("multi-rule-ccc"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(3))),
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(4))),
					}),
				),
			},
			expectedQuotas: []struct {
				id     string
				limits map[string]int64
			}{
				{
					id:     "cc-min-nodes-multi-rule-ccc-rule-0",
					limits: map[string]int64{resourcequotas.ResourceNodes: 3},
				},
				{
					id:     "cc-min-nodes-multi-rule-ccc-rule-1",
					limits: map[string]int64{resourcequotas.ResourceNodes: 4},
				},
			},
		},
		{
			name: "invalid target count",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("invalid-ccc"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithTargetNodeCountRule(intPtr(-1))),
					}),
				),
			},
			expectedQuotas: []struct {
				id     string
				limits map[string]int64
			}{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ccLister := lister.NewMockCrdLister(tc.crds)
			provider := NewRuleMinCapacityQuotaProvider(ccLister, nil)

			quotas, err := provider.Quotas(t.Context())
			assert.NoError(t, err)
			if !assert.Len(t, quotas, len(tc.expectedQuotas)) {
				return
			}
			for i, eq := range tc.expectedQuotas {
				assert.Equal(t, eq.id, quotas[i].ID())
				assert.Equal(t, eq.limits, quotas[i].Limits())
			}
		})
	}
}

func TestRuleMinCapacityQuota_AppliesTo(t *testing.T) {
	ruleQuota := &RuleMinCapacityQuota{
		id:              "test-rule-quota",
		crdName:         "my-ccc",
		targetNodeCount: 5,
		ruleIdxStr:      "0",
	}

	testCases := []struct {
		name          string
		node          *apiv1.Node
		expectedMatch bool
	}{
		{
			name:          "nil node",
			node:          nil,
			expectedMatch: false,
		},
		{
			name:          "match",
			node:          buildNodeWithLabelsAndAnnotations("my-ccc", "0"),
			expectedMatch: true,
		},
		{
			name:          "mismatch priority",
			node:          buildNodeWithLabelsAndAnnotations("my-ccc", "1"),
			expectedMatch: false,
		},
		{
			name:          "special -1 priority ignores rule",
			node:          buildNodeWithLabelsAndAnnotations("my-ccc", "-1"),
			expectedMatch: false,
		},
		{
			name:          "mismatch CC",
			node:          buildNodeWithLabelsAndAnnotations("other-ccc", "0"),
			expectedMatch: false,
		},
		{
			name:          "partial match (missing priority) fails",
			node:          buildNodeWithLabelsAndAnnotations("my-ccc", ""),
			expectedMatch: false,
		},
		{
			name:          "malformed priority fails",
			node:          buildNodeWithLabelsAndAnnotations("my-ccc", "abc"),
			expectedMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedMatch, ruleQuota.AppliesTo(tc.node))
		})
	}
}

func buildNodeWithLabelsAndAnnotations(cc, priority string) *apiv1.Node {
	n := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
		},
	}
	if cc != "" {
		n.Labels[labels.ComputeClassLabel] = cc
	}
	if priority != "" {
		n.Annotations[cccPriorityIndexAnnotationKey] = priority
	}
	return n
}

func intPtr(v int) *int { return &v }

func TestRuleMinCapacityQuotaProvider_Quotas_DisabledByExperiment(t *testing.T) {
	crds := []crd.CRD{
		crd.NewTestCrd(
			crd.WithName("my-ccc"),
			crd.WithRules([]rules.Rule{
				rules.NewRule(rules.WithTargetNodeCountRule(intPtr(5))),
			}),
		),
	}
	ccLister := lister.NewMockCrdLister(crds)
	mockManager := experiments.NewMockManagerWithOptions(
		version.Version{},
		map[string]bool{experiments.ComputeClassMinCapacityEnabledFlag: false},
		map[string]string{},
	)

	provider := NewRuleMinCapacityQuotaProvider(ccLister, mockManager)
	quotas, err := provider.Quotas(t.Context())
	assert.NoError(t, err)
	assert.Empty(t, quotas)
}
