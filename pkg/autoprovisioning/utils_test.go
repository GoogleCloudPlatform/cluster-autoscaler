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

package autoprovisioning

import (
	"fmt"
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	test_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"

	"github.com/stretchr/testify/assert"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
)

func TestAutoprovisionedNodeGroupsCount(t *testing.T) {
	for tn, tc := range map[string]struct {
		nonAutoprovisionedCount int
		autoprovisionedCount    int
		expected                int
	}{
		"no node groups": {
			nonAutoprovisionedCount: 0,
			autoprovisionedCount:    0,
			expected:                0,
		},
		"only non-autoprovisioned": {
			nonAutoprovisionedCount: 13,
			autoprovisionedCount:    0,
			expected:                0,
		},
		"only autoprovisioned": {
			nonAutoprovisionedCount: 0,
			autoprovisionedCount:    37,
			expected:                37,
		},
		"both": {
			nonAutoprovisionedCount: 13,
			autoprovisionedCount:    37,
			expected:                37,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			var nodeGroups []cloudprovider.NodeGroup
			for i := 0; i < tc.nonAutoprovisionedCount; i++ {
				nodeGroups = append(nodeGroups, test.NewTestNodeGroup(fmt.Sprintf("ng-%d", tc.nonAutoprovisionedCount), 1, 1, 1, true, false, "", nil, nil))
			}
			for i := 0; i < tc.autoprovisionedCount; i++ {
				nodeGroups = append(nodeGroups, test.NewTestNodeGroup(fmt.Sprintf("ng-%d", tc.nonAutoprovisionedCount), 1, 1, 1, false, true, "", nil, nil))
			}
			got := autoprovisionedNodeGroupsCount(nodeGroups)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestConfiguredMaxPodsPerNodeFromLabels(t *testing.T) {
	for tn, tc := range map[string]struct {
		systemLabels map[string]string
		expected     int
		expectErr    bool
	}{
		"nil labels": {
			systemLabels: nil,
			expected:     0,
			expectErr:    false,
		},
		"empty labels": {
			systemLabels: map[string]string{},
			expected:     0,
			expectErr:    false,
		},
		"negative mppn": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "-1",
			},
			expected:  0,
			expectErr: true,
		},
		"Invalid mppn - Invalid number": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "__Not_a_Number__",
			},
			expected:  0,
			expectErr: true,
		},
		"Invalid mppn - Floating point number": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "3.14",
			},
			expected:  0,
			expectErr: true,
		},
		"Valid mppn": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "120",
			},
			expected:  120,
			expectErr: false,
		},
		"mppn is set to 0": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "0",
			},
			expected:  0,
			expectErr: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got, err := configuredMaxPodsPerNodeFromLabels(tc.systemLabels)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestGetEstimatedNumberOfPods(t *testing.T) {
	maxMppn := 256
	for desc, tc := range map[string]struct {
		req              nodeGroupRequirements
		machineType      string
		wantNumberOfPods int
		wantErr          error
	}{
		"a lot of small cpu pods, e2-standard-32, 200 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 160, 100, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 200,
		},
		"average cpu pods, e2-standard-32, 50 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 640, 100, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 50,
		},
		"large cpu pods, e2-standard-32, 3 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 10000, 100, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 3,
		},
		"large memory pods, e2-standard-32, 3 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 100, 40*units.GiB, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 3,
		},
		"differently sized workloads, e2-standard-16, 10 estimated pods": {
			req: nodeGroupRequirements{
				pods: append(getPods(10, 640, 100, 0, 0), getPods(10, 2500, 100, 10, 0)...),
			},
			machineType:      "e2-standard-16",
			wantNumberOfPods: 10,
		},
		"gpu pod, 8 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 160, 100, 1, 0),
				gpuRequest: machinetypes.GpuRequest{
					Count: 8,
				},
			},
			machineType:      "a2-ultragpu-8g",
			wantNumberOfPods: 8,
		},
		"tpu pod, 1 estimated pods": {
			req: nodeGroupRequirements{
				pods:       getPods(10, 160, 100, 0, 4),
				tpuRequest: TpuRequest{ChipsPerNode: 4},
			},
			machineType:      "ct3-hightpu-4t",
			wantNumberOfPods: 1,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotNumberOfPods := 0
			machineTypeInfo, gotErr := machinetypes.NewMachineConfigProvider(nil).ToMachineType(tc.machineType)
			if gotErr == nil {
				gotNumberOfPods = getEstimatedNumberOfPods(maxMppn, tc.req, machineTypeInfo)
			}
			if tc.wantErr != nil {
				assert.Error(t, gotErr)
				assert.Equal(t, gotErr, tc.wantErr)
			} else {
				assert.NoError(t, gotErr)
				assert.Equal(t, tc.wantNumberOfPods, gotNumberOfPods)
			}
		})
	}
}

func getPods(numPods int, cpuMilli int64, memoryMilli int64, gpuResource int64, tpuResource int64) []*apiv1.Pod {
	var result []*apiv1.Pod
	for i := 0; i < numPods; i++ {
		pod := test_util.BuildTestPod(fmt.Sprintf("pod-%d", i), cpuMilli, memoryMilli, test_util.MarkUnschedulable())
		if gpuResource > 0 {
			pod.Spec.Containers[0].Resources.Requests[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(gpuResource, resource.DecimalSI)
		}
		if tpuResource > 0 {
			pod.Spec.Containers[0].Resources.Requests[tpu.ResourceGoogleTPU] = *resource.NewQuantity(tpuResource, resource.DecimalSI)
		}
		result = append(result, pod)
	}
	return result
}

func TestPlacementGroupSpec(t *testing.T) {
	for desc, tc := range map[string]struct {
		labelReq podrequirements.LabelRequirements
		req      nodeGroupRequirements
		wantSpec placement.Spec
	}{
		"placement group and policy from requirements without policy rule": {
			labelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				labels.PlacementGroupLabel: podrequirements.NewValues("group"),
				labels.PolicyLabel:         podrequirements.NewValues("policy"),
			}),
			wantSpec: placement.Spec{GroupId: "group", Policy: "policy"},
		},
		"placement group from requirements with policy rule": {
			labelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				labels.PlacementGroupLabel: podrequirements.NewValues("group"),
			}),
			req: nodeGroupRequirements{
				computeClassRule: rules.NewRule(
					rules.WithPlacementPolicyRule("policy"),
				),
			},
			wantSpec: placement.Spec{GroupId: "group", Policy: "policy"},
		},
		"placement policy from policy rule": {
			req: nodeGroupRequirements{
				computeClassRule: rules.NewRule(
					rules.WithPlacementPolicyRule("policy"),
				),
			},
			wantSpec: placement.Spec{Policy: "policy"},
		},
		"placement group from requirements with policy rule, no override": {
			labelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				labels.PlacementGroupLabel: podrequirements.NewValues("group"),
				labels.PolicyLabel:         podrequirements.NewValues("policy"),
			}),
			req: nodeGroupRequirements{
				computeClassRule: rules.NewRule(
					rules.WithPlacementPolicyRule("policy rule"),
				),
			},
			wantSpec: placement.Spec{GroupId: "group", Policy: "policy"},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotSpec := placementGroupSpec(&tc.req, tc.labelReq)
			assert.Equal(t, tc.wantSpec, gotSpec)
		})
	}
}
