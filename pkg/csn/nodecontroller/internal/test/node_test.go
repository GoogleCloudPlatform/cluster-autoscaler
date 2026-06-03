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

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
)

func TestCreateNode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name           string
		opts           []func(*v1.Node)
		extraAssertion func(*testing.T, *v1.Node)
	}{
		{
			name: "no_opts",
		},
		{
			name: "one_opt",
			opts: []func(*v1.Node){
				func(n *v1.Node) {
					n.Labels["foo"] = "bar"
				},
			},
			extraAssertion: func(t *testing.T, n *v1.Node) {
				assert.Equal(t, "bar", n.Labels["foo"])
			},
		},
		{
			name: "two_opts",
			opts: []func(node *v1.Node){
				func(n *v1.Node) {
					n.Spec.Unschedulable = true
				},
				func(n *v1.Node) {
					n.Annotations = map[string]string{"abc": "xyz"}
				},
			},
			extraAssertion: func(t *testing.T, n *v1.Node) {
				assert.True(t, n.Spec.Unschedulable)
				assert.Equal(t, "xyz", n.Annotations["abc"])
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodeName := "sample-node-name"
			node := CreateNode(nodeName, tc.opts...)

			assert.Equal(t, nodeName, node.Name)

			// GCE reference should be computable
			ref, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
			assert.NoError(t, err)

			assert.Equal(t, gce.GceRef{
				Project: "project",
				Zone:    "zone",
				Name:    nodeName,
			}, ref)

			if tc.extraAssertion != nil {
				tc.extraAssertion(t, node)
			}
		})
	}
}

func TestCreateNodeStateOpt(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name         string
		state        csn.NodeState
		expectNonCSN bool
	}{
		{
			name:  "suspended_node_can_be_created",
			state: csn.NodeStateSuspended,
		},
		{
			name:  "chilling_node_can_be_created",
			state: csn.NodeStateSuspended,
		},
		{
			name:         "state_opt_should_be_ignored_with_invalid_state",
			state:        "invalidState123",
			expectNonCSN: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var opts []func(*v1.Node)
			if s := tc.state; s != "" {
				opts = append(opts, StateOpt(s))
			}

			node := CreateNode("test-node", opts...)

			if tc.expectNonCSN {
				assert.False(t, csn.IsCSNNode(node))
				return
			}

			assert.Equal(t, tc.state, csn.ClassifyNode(node))
		})
	}
}
