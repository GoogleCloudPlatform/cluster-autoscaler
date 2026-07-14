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

package mppn

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

var (
	optionsSortFunc = func(x, y expander.Option) bool {
		return fmt.Sprintf("%v", x) < fmt.Sprintf("%v", y)
	}
	optionsSliceIgnoreOrderOpt = cmpopts.SortSlices(func(x, y []expander.Option) bool {
		sort.Slice(x, func(i, j int) bool {
			return optionsSortFunc(x[i], x[j])
		})
		sort.Slice(y, func(i, j int) bool {
			return optionsSortFunc(y[i], y[j])
		})
		var xSig, ySig bytes.Buffer
		for _, el := range x {
			xSig.WriteString(fmt.Sprintf("%v", el))
		}
		for _, el := range y {
			ySig.WriteString(fmt.Sprintf("%v", el))
		}
		return ySig.String() < xSig.String()
	})
)

func TestGroupOptions(t *testing.T) {
	p1 := []*v1.Pod{test_util.BuildTestPod("p1", 1, 1)}
	p2 := []*v1.Pod{test_util.BuildTestPod("p2", 1, 1)}
	basicNG := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{}).Build()
	basicNg32Mppn := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 32}).Build()
	basicNg64Mppn := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 64}).Build()
	ng1kDiskSize := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{DiskSize: 1000}).Build()
	ng2kDiskSize := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{DiskSize: 2000}).Build()
	acceleratorA1Ng := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{Accelerators: []*gke_api_beta.AcceleratorConfig{
		{
			AcceleratorCount: 2,
			AcceleratorType:  "a1",
			GpuPartitionSize: "p1",
		},
	}}).Build()
	acceleratorA2Ng := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{Accelerators: []*gke_api_beta.AcceleratorConfig{
		{
			AcceleratorCount: 2,
			AcceleratorType:  "a2",
			GpuPartitionSize: "p1",
		},
	}}).Build()
	localSsd1Ng := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{LocalSSDConfig: &gkeclient.LocalSSDConfig{
		LocalSsdCount: 1,
	}}).Build()
	localSsd2Ng := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{LocalSSDConfig: &gkeclient.LocalSSDConfig{
		LocalSsdCount: 2,
	}}).Build()
	e2MachineType := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-medium"}).Build()
	n1MachineType := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-2"}).Build()
	tpuT1Ng := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		TpuMultiHost: true,
		TpuType:      "t1",
		TpuTopology:  "t1",
	}).Build()
	tpuT2Ng := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		TpuMultiHost: true,
		TpuType:      "t2",
		TpuTopology:  "t2",
	}).Build()
	labelsNg := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		Labels: map[string]string{"val1": "key1"},
	}).Build()
	for desc, tc := range map[string]struct {
		options            []expander.Option
		wantGroupedOptions [][]expander.Option
	}{
		"same node groups, different pods": {
			options: []expander.Option{
				{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				},
				{
					Pods:      p2,
					NodeGroup: basicNG,
					NodeCount: 1,
				},
			},
			wantGroupedOptions: [][]expander.Option{
				{{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				}},
				{{
					Pods:      p2,
					NodeGroup: basicNG,
					NodeCount: 1,
				}},
			},
		},
		"same node groups, different node count": {
			options: []expander.Option{
				{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 2,
				},
				{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				},
			},
			wantGroupedOptions: [][]expander.Option{
				{{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 2,
				}},
				{{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				}},
			},
		},
		"same options": {
			options: []expander.Option{
				{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				},
			},
			wantGroupedOptions: [][]expander.Option{
				{{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				}},
			},
		},
		"node groups with different spec grouped correctly": {
			options: []expander.Option{
				{
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: basicNg32Mppn,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: basicNg64Mppn,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: localSsd1Ng,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: localSsd2Ng,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: acceleratorA1Ng,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: acceleratorA2Ng,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: ng1kDiskSize,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: ng2kDiskSize,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: e2MachineType,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: n1MachineType,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: tpuT2Ng,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: tpuT1Ng,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: labelsNg,
					NodeCount: 1,
				}},
			wantGroupedOptions: [][]expander.Option{
				{{
					Pods:      p1,
					NodeGroup: basicNg64Mppn,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: basicNg32Mppn,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: basicNG,
					NodeCount: 1,
				}, {
					Pods:      p1,
					NodeGroup: labelsNg,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: localSsd1Ng,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: localSsd2Ng,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: acceleratorA1Ng,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: acceleratorA2Ng,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: ng1kDiskSize,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: ng2kDiskSize,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: e2MachineType,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: n1MachineType,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: tpuT2Ng,
					NodeCount: 1,
				}}, {{
					Pods:      p1,
					NodeGroup: tpuT1Ng,
					NodeCount: 1,
				}},
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotOptions := groupOptions(tc.options)
			if diff := cmp.Diff(tc.wantGroupedOptions, gotOptions, cmpopts.IgnoreUnexported(gke.FakeGkeManager{}), cmp.AllowUnexported(gke.GkeMig{}), cmp.AllowUnexported(gke.GkeNodePool{}), optionsSliceIgnoreOrderOpt); diff != "" {
				t.Errorf("Unexcpected groupOptions(); diff: %v", diff)
			}
		})
	}
}
