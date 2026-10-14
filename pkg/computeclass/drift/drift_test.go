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

package drift

import (
	"fmt"
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

// fakeNodeGroup embeds the interface so that any method other than Id() panics,
// asserting that the Evaluator only ever identifies node groups by ID.
type fakeNodeGroup struct {
	cloudprovider.NodeGroup
	id string
}

func (f *fakeNodeGroup) Id() string { return f.id }

type fakeLister struct {
	c    crd.CRD
	name string
	err  error
	// calls counts NodeGroupCrd invocations per node group ID.
	calls map[string]int
}

func (l *fakeLister) NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error) {
	if l.calls == nil {
		l.calls = make(map[string]int)
	}
	l.calls[nodeGroup.Id()]++
	return l.c, l.name, l.err
}

type fakeMatcher struct {
	matchesLabel  bool
	matchesConfig bool
	ruleMatches   bool
}

func (m *fakeMatcher) MatchesCrdLabel(cloudprovider.NodeGroup, crd.CRD) bool  { return m.matchesLabel }
func (m *fakeMatcher) MatchesCrdConfig(cloudprovider.NodeGroup, crd.CRD) bool { return m.matchesConfig }
func (m *fakeMatcher) FirstMatchedRule(cloudprovider.NodeGroup, crd.CRD) (bool, int, rules.Rule) {
	return m.ruleMatches, 0, nil
}

func TestEvaluateNodeGroup(t *testing.T) {
	withRules := crd.NewTestCrd(
		crd.WithName("cc-rules"),
		crd.WithConfigDrift(true),
		crd.WithRules([]rules.Rule{rules.NewRule()}),
	)
	withRulesScaleUpAnyway := crd.NewTestCrd(
		crd.WithName("cc-anyway"),
		crd.WithConfigDrift(true),
		crd.WithRules([]rules.Rule{rules.NewRule()}),
		crd.WithScaleUpAnyway(),
	)
	noRules := crd.NewTestCrd(
		crd.WithName("cc-no-rules"),
		crd.WithConfigDrift(true),
	)
	driftDisabled := crd.NewTestCrd(
		crd.WithName("cc-off"),
		crd.WithConfigDrift(false),
	)

	testCases := []struct {
		name    string
		lister  *fakeLister
		matcher *fakeMatcher
		want    Result
	}{
		{
			name:    "no compute class assigned",
			lister:  &fakeLister{c: nil, name: ""},
			matcher: &fakeMatcher{},
			want:    Result{},
		},
		{
			name:    "compute class present but name empty",
			lister:  &fakeLister{c: noRules, name: ""},
			matcher: &fakeMatcher{},
			want:    Result{},
		},
		{
			name:    "lister error is reported as not drifted",
			lister:  &fakeLister{c: noRules, name: "cc-no-rules", err: fmt.Errorf("boom")},
			matcher: &fakeMatcher{matchesLabel: true},
			want:    Result{},
		},
		{
			name:    "config drift disabled is never drifted",
			lister:  &fakeLister{c: driftDisabled, name: "cc-off"},
			matcher: &fakeMatcher{matchesLabel: true, matchesConfig: false},
			want:    Result{CRD: driftDisabled, CRDName: "cc-off", MigrationEnabled: false, Drifted: false},
		},
		{
			name:    "node group of a different compute class is not drifted",
			lister:  &fakeLister{c: noRules, name: "cc-no-rules"},
			matcher: &fakeMatcher{matchesLabel: false, matchesConfig: false},
			want:    Result{CRD: noRules, CRDName: "cc-no-rules", MigrationEnabled: true, Drifted: false},
		},
		{
			name:    "no rules and config matches is compliant",
			lister:  &fakeLister{c: noRules, name: "cc-no-rules"},
			matcher: &fakeMatcher{matchesLabel: true, matchesConfig: true},
			want:    Result{CRD: noRules, CRDName: "cc-no-rules", MigrationEnabled: true, Drifted: false},
		},
		{
			name:    "no rules and config differs is drifted",
			lister:  &fakeLister{c: noRules, name: "cc-no-rules"},
			matcher: &fakeMatcher{matchesLabel: true, matchesConfig: false},
			want:    Result{CRD: noRules, CRDName: "cc-no-rules", MigrationEnabled: true, Drifted: true},
		},
		{
			name:    "with rules a matching rule is compliant",
			lister:  &fakeLister{c: withRules, name: "cc-rules"},
			matcher: &fakeMatcher{matchesLabel: true, ruleMatches: true, matchesConfig: false},
			want:    Result{CRD: withRules, CRDName: "cc-rules", MigrationEnabled: true, Drifted: false},
		},
		{
			name:    "with rules no matching rule is drifted",
			lister:  &fakeLister{c: withRules, name: "cc-rules"},
			matcher: &fakeMatcher{matchesLabel: true, ruleMatches: false, matchesConfig: true},
			want:    Result{CRD: withRules, CRDName: "cc-rules", MigrationEnabled: true, Drifted: true},
		},
		{
			// ScaleUpAnyway means rules are advisory, so compliance falls back
			// to the ComputeClass-wide config even though rules are declared.
			name:    "scale up anyway falls back to config match",
			lister:  &fakeLister{c: withRulesScaleUpAnyway, name: "cc-anyway"},
			matcher: &fakeMatcher{matchesLabel: true, ruleMatches: false, matchesConfig: true},
			want:    Result{CRD: withRulesScaleUpAnyway, CRDName: "cc-anyway", MigrationEnabled: true, Drifted: false},
		},
		{
			name:    "scale up anyway drifts when config differs",
			lister:  &fakeLister{c: withRulesScaleUpAnyway, name: "cc-anyway"},
			matcher: &fakeMatcher{matchesLabel: true, ruleMatches: true, matchesConfig: false},
			want:    Result{CRD: withRulesScaleUpAnyway, CRDName: "cc-anyway", MigrationEnabled: true, Drifted: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := NewEvaluator(tc.lister, tc.matcher)
			got := e.EvaluateNodeGroup(&fakeNodeGroup{id: "ng-1"})
			if got != tc.want {
				t.Errorf("EvaluateNodeGroup() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCacheEvaluatesEachNodeGroupOnce(t *testing.T) {
	c := crd.NewTestCrd(crd.WithName("cc"), crd.WithConfigDrift(true))
	lister := &fakeLister{c: c, name: "cc"}
	cache := NewCache(NewEvaluator(lister, &fakeMatcher{matchesLabel: true, matchesConfig: false}))

	ng1 := &fakeNodeGroup{id: "ng-1"}
	ng2 := &fakeNodeGroup{id: "ng-2"}

	// Simulate many nodes spread across two node groups.
	for i := 0; i < 5; i++ {
		if got := cache.EvaluateNodeGroup(ng1); !got.Drifted {
			t.Fatalf("EvaluateNodeGroup(ng-1) reported not drifted on call %d", i)
		}
		if got := cache.EvaluateNodeGroup(ng2); !got.Drifted {
			t.Fatalf("EvaluateNodeGroup(ng-2) reported not drifted on call %d", i)
		}
	}

	for _, id := range []string{"ng-1", "ng-2"} {
		if got := lister.calls[id]; got != 1 {
			t.Errorf("NodeGroupCrd called %d times for %s, want 1", got, id)
		}
	}
}

func TestCacheCachesNegativeResults(t *testing.T) {
	lister := &fakeLister{c: nil, name: ""}
	cache := NewCache(NewEvaluator(lister, &fakeMatcher{}))

	ng := &fakeNodeGroup{id: "ng-1"}
	cache.EvaluateNodeGroup(ng)
	cache.EvaluateNodeGroup(ng)

	if got := lister.calls["ng-1"]; got != 1 {
		t.Errorf("NodeGroupCrd called %d times, want 1: node groups with no compute class must also be cached", got)
	}
}
