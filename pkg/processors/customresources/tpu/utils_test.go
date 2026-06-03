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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

func TestGetTpuFromTemplate(t *testing.T) {
	for _, tc := range []struct {
		desc           string
		tpuType        string
		topology       string
		count          int64
		brokenTemplate bool
		expectErr      bool
		expectFound    bool
	}{
		{
			desc:        "no tpu on the node",
			expectFound: false,
		},
		{
			desc:        "all tpu fields found",
			tpuType:     "type of tpu",
			topology:    "topology",
			count:       13,
			expectFound: true,
		},
		{
			desc:        "missing tpu type",
			topology:    "perfectly valid topology",
			count:       42,
			expectFound: false,
		},
		{
			desc:        "missing topology",
			tpuType:     "some tpu type",
			count:       44,
			expectFound: false,
		},
		{
			desc:        "missing count",
			topology:    "non-euclidean",
			count:       1,
			expectFound: false,
		},
		{
			desc:           "error rendering template",
			tpuType:        gkelabels.TpuV4LiteDeviceValue,
			topology:       "2x2",
			count:          4,
			brokenTemplate: true,
			expectErr:      true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			template := test.BuildTestNode("n1", 1000, 1000)
			if tc.tpuType != "" {
				template.Labels[gkelabels.TPULabel] = tc.tpuType
			}
			if tc.topology != "" {
				template.Labels[gkelabels.TPUTopologyLabel] = tc.topology
			}
			if tc.count != 0 {
				template.Status.Capacity[tpu.ResourceGoogleTPU] = *resource.NewQuantity(tc.count, resource.DecimalSI)
			}
			tni := framework.NewTestNodeInfo(template)
			var machineTemplates map[string]*framework.NodeInfo
			if !tc.brokenTemplate {
				machineTemplates = map[string]*framework.NodeInfo{

					"tng": tni,
				}
			}
			provider := testprovider.NewTestCloudProviderBuilder().WithMachineTemplates(machineTemplates).Build()
			provider.AddNodeGroup("tng", 1, 10, 1)
			provider.AddNode("tng", template)
			ng1, _ := provider.NodeGroupForNode(template)

			tr, found, err := getTpuFromTemplate(ng1)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, found, tc.expectFound)
			if found {
				assert.Equal(t, tr.TpuType, tc.tpuType)
				assert.Equal(t, tr.Topology, tc.topology)
				assert.Equal(t, tr.Count, tc.count)
			}
		})
	}
}
