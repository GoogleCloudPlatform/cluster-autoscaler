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

package rules

import (
	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"testing"
)

type mockNodeGroup struct {
	cloudprovider.NodeGroup
}

func TestMinimumCapacityRule_Matches(t *testing.T) {
	rule := &minimumCapacityRule{}
	assert.True(t, rule.Matches(&mockNodeGroup{}))
}

func TestMinimumCapacityRule_TargetNodeCount(t *testing.T) {
	val := 15

	// Test set value
	rule := &minimumCapacityRule{targetNodeCount: &val}
	assert.NotNil(t, rule.TargetNodeCount())
	assert.Equal(t, 15, *rule.TargetNodeCount())

	// Test nil rule
	var nRule *minimumCapacityRule
	assert.Nil(t, nRule.TargetNodeCount())

	// Test nil value
	rRule := &minimumCapacityRule{}
	assert.Nil(t, rRule.TargetNodeCount())
}

func TestWithTargetNodeCountRule(t *testing.T) {
	val := 20
	r := &rule{}
	opt := WithTargetNodeCountRule(&val)
	opt(r)

	assert.NotNil(t, r.minimumCapacityRule.TargetNodeCount())
	assert.Equal(t, 20, *r.minimumCapacityRule.TargetNodeCount())
}
