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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
)

const cccPriorityIndexAnnotationKey = labels.CCCPriorityIndexAnnotationKey

func TestTargetNodeCountProvider_Quotas(t *testing.T) {
	testCases := []struct {
		name            string
		crds            []crd.CRD
		excludeTopLevel bool
		expectedQuotas  []struct {
			id     string
			limits map[string]int64
		}
	}{
		{
			name: "top-level CCC target",
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("my-ccc"),
					crd.WithTargetNodeCount(intPtr(5)),
				),
			},
			expectedQuotas: []struct {
				id     string
				limits map[string]int64
			}{
				{
					id:     "cc-min-nodes-my-ccc",
					limits: map[string]int64{resourcequotas.ResourceNodes: 5},
				},
			},
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
			name: "both priority and high-level quotas",
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
					id:     "cc-min-nodes-both-ccc",
					limits: map[string]int64{resourcequotas.ResourceNodes: 5},
				},
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
					crd.WithTargetNodeCount(intPtr(-1)),
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
			provider := NewTargetNodeCountProvider(ccLister, tc.excludeTopLevel, nil, nil, false)

			quotas, err := provider.Quotas(context.TODO())
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedQuotas), len(quotas))

			for i, eq := range tc.expectedQuotas {
				assert.Equal(t, eq.id, quotas[i].ID())
				assert.Equal(t, eq.limits, quotas[i].Limits())
			}
		})
	}
}

func TestTargetNodeCountProvider_Quotas_WithExclusion(t *testing.T) {
	crds := []crd.CRD{
		crd.NewTestCrd(
			crd.WithName("both-ccc"),
			crd.WithTargetNodeCount(intPtr(5)),
			crd.WithRules([]rules.Rule{
				rules.NewRule(rules.WithTargetNodeCountRule(intPtr(3))),
			}),
		),
	}
	ccLister := lister.NewMockCrdLister(crds)
	provider := NewTargetNodeCountProvider(ccLister, true, nil, nil, false)

	quotas, err := provider.Quotas(context.TODO())
	assert.NoError(t, err)
	// We expect ONLY rule-level quota, spec-level should be excluded
	assert.Equal(t, 1, len(quotas))
	assert.Equal(t, "cc-min-nodes-both-ccc-rule-0", quotas[0].ID())
}

func TestTargetNodeCountQuota_AppliesTo(t *testing.T) {
	topLevelQuota := &TargetNodeCountQuota{
		id:              "test-quota",
		crdName:         "my-ccc",
		targetNodeCount: 5,
		ruleIdxStr:      "",
	}

	ruleQuota := &TargetNodeCountQuota{
		id:              "test-rule-quota",
		crdName:         "my-ccc",
		targetNodeCount: 5,
		ruleIdxStr:      "0",
	}

	topLevelTestCases := []struct {
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
			node:          buildNodeWithLabelsAndAnnotations("my-ccc", ""),
			expectedMatch: true,
		},
		{
			name:          "mismatch CC",
			node:          buildNodeWithLabelsAndAnnotations("other-ccc", ""),
			expectedMatch: false,
		},
		{
			name:          "special -1 priority matches",
			node:          buildNodeWithLabelsAndAnnotations("my-ccc", "-1"),
			expectedMatch: true,
		},
		{
			name:          "missing labels fails",
			node:          buildNodeWithLabelsAndAnnotations("", ""),
			expectedMatch: false,
		},
	}

	ruleLevelTestCases := []struct {
		name          string
		node          *apiv1.Node
		expectedMatch bool
	}{
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

	for _, tc := range topLevelTestCases {
		t.Run("TopLevel_"+tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedMatch, topLevelQuota.AppliesTo(tc.node))
		})
	}

	for _, tc := range ruleLevelTestCases {
		t.Run("RuleLevel_"+tc.name, func(t *testing.T) {
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

// fakeMigProvider is a test double for MigProvider.
type fakeMigProvider struct {
	mig *gke.GkeMig
	err error
}

func (f *fakeMigProvider) GkeMigForNode(_ *apiv1.Node) (*gke.GkeMig, error) {
	return f.mig, f.err
}

func atomicTestMig() *gke.GkeMig {
	spec := gke.NewTestMigSpecBuilder().
		SetTpuType("tpu7x").
		SetTpuTopology("4x4x4").
		SetTpuMultiHost(true).
		SpecBuild()
	return gke.NewTestGkeMigBuilder().SetSpec(spec).Build()
}

func TestTargetNodeCountQuota_AppliesTo_AtomicExemption(t *testing.T) {
	matchingNode := buildNodeWithLabelsAndAnnotations("my-ccc", "")

	testCases := []struct {
		name          string
		migProvider   MigProvider
		exempt        bool
		node          *apiv1.Node
		expectedMatch bool
	}{
		{
			name:          "atomic node is exempt",
			migProvider:   &fakeMigProvider{mig: atomicTestMig()},
			exempt:        true,
			node:          matchingNode,
			expectedMatch: false,
		},
		{
			name:          "atomic node not exempt when flag disabled",
			migProvider:   &fakeMigProvider{mig: atomicTestMig()},
			exempt:        false,
			node:          matchingNode,
			expectedMatch: true,
		},
		{
			name:          "no mig provider applies quota",
			migProvider:   nil,
			exempt:        true,
			node:          matchingNode,
			expectedMatch: true,
		},
		{
			name:          "nil mig applies quota",
			migProvider:   &fakeMigProvider{mig: nil},
			exempt:        true,
			node:          matchingNode,
			expectedMatch: true,
		},
		{
			name:          "mig lookup error applies quota",
			migProvider:   &fakeMigProvider{err: assert.AnError},
			exempt:        true,
			node:          matchingNode,
			expectedMatch: true,
		},
		{
			name:          "atomic node with wrong CC still does not match",
			migProvider:   &fakeMigProvider{mig: atomicTestMig()},
			exempt:        true,
			node:          buildNodeWithLabelsAndAnnotations("other-ccc", ""),
			expectedMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			quota := &TargetNodeCountQuota{
				id:                     "test-quota",
				crdName:                "my-ccc",
				targetNodeCount:        5,
				ruleIdxStr:             "",
				migProvider:            tc.migProvider,
				exemptAtomicNodeGroups: tc.exempt,
			}
			assert.Equal(t, tc.expectedMatch, quota.AppliesTo(tc.node))
		})
	}
}

func intPtr(v int) *int { return &v }

func TestTargetNodeCountProvider_Quotas_DisabledByExperiment(t *testing.T) {
	crds := []crd.CRD{
		crd.NewTestCrd(
			crd.WithName("my-ccc"),
			crd.WithTargetNodeCount(intPtr(5)),
		),
	}
	ccLister := lister.NewMockCrdLister(crds)
	mockManager := experiments.NewMockManagerWithOptions(
		version.Version{},
		map[string]bool{experiments.ComputeClassMinCapacityEnabledFlag: false},
		map[string]string{},
	)

	provider := NewTargetNodeCountProvider(ccLister, false, mockManager, nil, false)
	quotas, err := provider.Quotas(context.TODO())
	assert.NoError(t, err)
	assert.Empty(t, quotas)
}
