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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateNapCannotBeEnabledReason(t *testing.T) {
	testCases := []struct {
		name          string
		crd           crd.CRD
		napEnabled    bool
		wantCondition bool
	}{
		{
			name: "both crd and cluster nap disabled",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{})),
			napEnabled:    false,
			wantCondition: false,
		},
		{
			name: "crd nap disabled and cluster nap enabled",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			napEnabled:    true,
			wantCondition: false,
		},
		{
			name: "both crd and cluster nap enabled",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			napEnabled:    true,
			wantCondition: false,
		},
		{
			name: "crd with nap enabled and cluster nap disabled",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			napEnabled:    false,
			wantCondition: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAutoprovisioningEnabled(tc.napEnabled).
				Build()
			checker := &napCannotBeEnabledCheck{provider: provider}
			condition := checker.checkCrd(tc.crd, nil)
			if tc.wantCondition {
				assert.NotNil(t, condition)
				assert.Equal(t, CrdMisconfiguredCondition, condition.Type)
				assert.Equal(t, NapCannotBeEnabledReason, condition.Reason)
				assert.Equal(t, NapCannotBeEnabledMessage, condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
