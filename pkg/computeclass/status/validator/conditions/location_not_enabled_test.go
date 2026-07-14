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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateLocationNotEnabledForAutoprovisioningReason(t *testing.T) {
	testCases := []struct {
		name          string
		crd           crd.CRD
		wantCondition bool
		wantMessage   string
	}{
		{
			name: "1 rule, location config zone outside of autoprovisioning locations",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithLocationRule([]string{"zone3"})),
				}),
			),
			wantCondition: true,
			wantMessage:   fmt.Sprintf(LocationNotEnabledForAutoprovisioningMessage, "zone3", "[zone1 zone2]"),
		},
		{
			name: "1 rule, location config zone correct with autoprovisioning locations",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithLocationRule([]string{"zone1"})),
				}),
			),
			wantCondition: false,
		},
		{
			name: "zone in location config outside of autoprovisioning locations",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithLocationRule([]string{"zone3"})),
					rules.NewRule(rules.WithLocationRule([]string{"zone1", "zone2"})),
				}),
			),
			wantCondition: true,
			wantMessage:   fmt.Sprintf(LocationNotEnabledForAutoprovisioningMessage, "zone3", "[zone1 zone2]"),
		},
		{
			name: "zones in location config correct with autoprovisioning locations",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithLocationRule([]string{"zone1"})),
					rules.NewRule(rules.WithLocationRule([]string{})),
					rules.NewRule(),
					rules.NewRule(rules.WithLocationRule([]string{"zone1", "zone2"})),
				}),
			),
			wantCondition: false,
		},
		{
			name: "last rule contains a zone outside of autoprovisioning locations",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithLocationRule([]string{"zone1", "zone2"})),
					rules.NewRule(rules.WithLocationRule([]string{"zone3"})),
				}),
			),
			wantCondition: true,
			wantMessage:   fmt.Sprintf(LocationNotEnabledForAutoprovisioningMessage, "zone3", "[zone1 zone2]"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAllZones("zone1", "zone2").
				WithAutoprovisioningLocations("zone1").
				Build()
			checker := &locationNotEnabledForAutoprovisioningCheck{provider: provider}
			condition := checker.checkCrd(tc.crd, nil)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, CrdMisconfiguredCondition, condition.Type)
				assert.Equal(t, LocationNotEnabledForAutoprovisioningReason, condition.Reason)
				assert.Equal(t, tc.wantMessage, condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
