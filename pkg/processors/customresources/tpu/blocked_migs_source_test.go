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

package tpu

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	testcloudprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

func applyTpuResource(node *v1.Node, tr TpuResource) {
	node.Labels[gkelabels.TPULabel] = tr.TpuType
	node.Labels[gkelabels.TPUTopologyLabel] = tr.Topology
	node.Status.Capacity[tpu.ResourceGoogleTPU] = *resource.NewQuantity(tr.Count, resource.DecimalSI)
	node.Status.Allocatable[tpu.ResourceGoogleTPU] = *resource.NewQuantity(tr.Count, resource.DecimalSI)
}

func buildTpuNode(name string, tr TpuResource) *v1.Node {
	node := test.BuildTestNode(name, 1000, 1000)
	applyTpuResource(node, tr)
	return node
}

// implements NodeGroup interface used in blockedMigs()
type testMig struct {
	*testcloudprovider.TestNodeGroup

	id       string
	status   string
	template *framework.NodeInfo
}

func newTestMig(id, status string, templateNode *v1.Node) testMig {
	nodeInfo := framework.NewTestNodeInfo(templateNode)

	return testMig{
		id:       id,
		status:   status,
		template: nodeInfo,
	}
}

func (m testMig) Id() string {
	return m.id
}

func (m testMig) Status() string {
	return m.status
}

func (m testMig) TemplateNodeInfo() (*framework.NodeInfo, error) {
	if m.template != nil {
		return m.template, nil
	}
	return nil, fmt.Errorf("no node template in mig %v", m.id)
}

func (m testMig) IsStable() (bool, error) {
	return true, nil
}

type testMigConfig struct {
	template *v1.Node
	status   string
}

func TestBlockedMigs(t *testing.T) {
	// create template nodes to reuse in tests
	multiHostTpuNode := buildTpuNode("multi-host", TpuResource{gkelabels.TpuV4PodsliceValue, "2x2x4", 4})
	singleHostTpuNode := buildTpuNode("single-host", TpuResource{gkelabels.TpuV4PodsliceValue, "2x2x1", 4})
	brokenTpuNode := buildTpuNode("broken-config", TpuResource{"", "", 8})
	noTpuNode := test.BuildTestNode("no-tpu", 1000, 1000)

	for _, tc := range []struct {
		desc          string
		migs          map[string]testMigConfig
		expectBlocked []string
	}{
		{
			desc: "a mig in a non-reconciling multi-host node pool is ready to scale",
			migs: map[string]testMigConfig{
				"multi-host-running": {
					template: multiHostTpuNode,
					status:   "RUNNING",
				},
			},
		},
		{
			desc: "a mig in a reconciling multi-host node pool is blocked",
			migs: map[string]testMigConfig{
				"multi-host-reconciling": {
					template: multiHostTpuNode,
					status:   "RECONCILING",
				},
			},
			expectBlocked: []string{"multi-host-reconciling"},
		},
		{
			desc: "fail open if tpu config can't be determined",
			migs: map[string]testMigConfig{
				"broken-tpu-reconciling": {
					template: brokenTpuNode,
					status:   "RECONCILING",
				},
			},
		},
		{
			desc: "migs in non-multi-host node pools are ready to scale regardless of status",
			migs: map[string]testMigConfig{
				"single-host-reconciling": {
					template: singleHostTpuNode,
					status:   "RECONCILING",
				},
				"no-tpu-reconciling": {
					template: noTpuNode,
					status:   "RECONCILING",
				},
				"single-host-running": {
					template: singleHostTpuNode,
					status:   "RUNNING",
				},
				"no-tpu-stopping": {
					template: noTpuNode,
					status:   "STOPPING",
				},
				"broken-tpu-error": {
					template: brokenTpuNode,
					status:   "ERROR",
				},
				"no-tpu-new-status": {
					template: noTpuNode,
					status:   "some new gke status value",
				},
			},
		},
		{
			desc: "multiple migs in multi-host reconciling node pools are all blocked",
			migs: map[string]testMigConfig{
				"multi-host-reconciling-1": {
					template: multiHostTpuNode,
					status:   "RECONCILING",
				},
				"multi-host-reconciling-2": {
					template: multiHostTpuNode,
					status:   "RECONCILING",
				},
				"multi-host-reconciling-3": {
					template: multiHostTpuNode,
					status:   "RECONCILING",
				},
				"multi-host-running": {
					template: multiHostTpuNode,
					status:   "RUNNING",
				},
			},
			expectBlocked: []string{
				"multi-host-reconciling-1",
				"multi-host-reconciling-2",
				"multi-host-reconciling-3",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var migs []NodeGroup
			for id, mc := range tc.migs {
				migs = append(migs, newTestMig(id, mc.status, mc.template))
			}

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			source := NewBlockedMigsSource(provider)
			blocked := source.blockedMigs(migs)

			if len(blocked) != len(tc.expectBlocked) {
				t.Errorf("unexpected blocked migs size, got: %v, want: %v", blocked, tc.expectBlocked)
			}
			for _, mig := range tc.expectBlocked {
				if _, found := blocked[mig]; !found {
					t.Errorf("missing mig %v that should be blocked, got: %v, want: %v", mig, blocked, tc.expectBlocked)
				}
			}
		})
	}
}
