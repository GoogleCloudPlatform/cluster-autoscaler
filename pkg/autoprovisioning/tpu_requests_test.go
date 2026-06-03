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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	computeclass_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestTpuRequestSignature(t *testing.T) {
	for tn, tc := range map[string]struct {
		request  TpuRequest
		expected string
	}{
		"empty request": {
			request:  TpuRequest{},
			expected: `type: "", count: 0, topology: ""`,
		},
		"no topology specified": {
			request: TpuRequest{
				TpuType:      "tpu-type-1",
				ChipsPerNode: 4,
			},
			expected: `type: "tpu-type-1", count: 4, topology: ""`,
		},
		"everything specified": {
			request: TpuRequest{
				TpuType:      "tpu-type-1",
				ChipsPerNode: 4,
				Topology:     "2x2x2",
			},
			expected: `type: "tpu-type-1", count: 4, topology: "2x2x2"`,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.request.Signature())
		})
	}
}

func TestTpuPodsRequirements(t *testing.T) {
	plain1 := test.BuildTestPod("plain1", 0, 1000)
	plain2 := test.BuildTestPod("plain2", 0, 1000)
	tpuDevicePod := buildTpuPod("TPUDevice", labels.TpuV4LiteDeviceValue, 4, "")
	tpuv3DeviceSinglePod := buildTpuPod("TPUv3DeviceSinglePod", labels.TpuV3DeviceValue, 4, "2x2")
	tpuNoLimitSetPod := buildTpuPodGeneric("TPUv3DeviceSinglePod", labels.TpuV3DeviceValue, 4, "2x2", false)
	tpuv3PodsliceMultiPod1 := buildTpuPod("TPUv3PodsliceMultiPod1", labels.TpuV3SliceValue, 4, "4x4")
	tpuv3PodsliceMultiPod2 := buildTpuPod("TPUv3PodsliceMultiPod2", labels.TpuV3SliceValue, 4, "4x8")
	tpuv3PodsliceMultiPod3 := buildTpuPod("TPUv3PodsliceMultiPod3", labels.TpuV3SliceValue, 4, "8x8")
	tpuPodsliceMultiPod1 := buildTpuPod("TPUPodsliceMultiHost1", labels.TpuV4PodsliceValue, 4, "2x2x2")
	tpuPodsliceMultiPod2 := buildTpuPod("TPUPodsliceMultiHost2", labels.TpuV4PodsliceValue, 4, "2x2x4")
	tpuPodsliceMultiPod3 := buildTpuPod("TPUPodsliceMultiHost3", labels.TpuV4PodsliceValue, 4, "2x2x4")
	tpuPodsliceSinglePod := buildTpuPod("TPUPodsliceSingleHost", labels.TpuV4PodsliceValue, 4, "2x2x1")
	tpuv5sliceMultiPod1 := buildTpuPod("TPUv5sliceMultiHost1", labels.TpuV5PSliceValue, 4, "2x2x2")
	tpuv5sliceMultiPod2 := buildTpuPod("TPUv5sliceMultiHost2", labels.TpuV5PSliceValue, 4, "2x2x4")
	tpuv5sliceMultiPod3 := buildTpuPod("TPUv5sliceMultiHost3", labels.TpuV5PSliceValue, 4, "2x2x4")
	tpuv5sliceSinglePod := buildTpuPod("TPUv5sliceSingleHost", labels.TpuV5PSliceValue, 4, "2x2x1")
	tpuv5LiteDevicePod1 := buildTpuPod("Tpuv5LiteDevice1x1", labels.TpuV5LiteDeviceValue, 1, "1x1")
	tpuv5LiteDevicePod2 := buildTpuPod("Tpuv5LiteDevice2x2", labels.TpuV5LiteDeviceValue, 4, "2x2")
	tpuv5LiteDevicePod3 := buildTpuPod("Tpuv5LiteDevice2x4", labels.TpuV5LiteDeviceValue, 8, "2x4")
	tpuv5LitePodsliceSinglePod1x1 := buildTpuPod("TPUv5LitePodscliceSingleHost-1x1", labels.TpuV5LitePodsliceValue, 1, "1x1")
	tpuv5LitePodsliceSinglePod2x2 := buildTpuPod("TPUv5LitePodscliceSingleHost-2x2", labels.TpuV5LitePodsliceValue, 4, "2x2")
	tpuv5LitePodsliceSinglePod2x4 := buildTpuPod("TPUv5LitePodscliceSingleHost-2x4", labels.TpuV5LitePodsliceValue, 8, "2x4")
	tpuv5LitePodsliceMultiPod2x4 := buildTpuPod("TPUv5LitePodscliceMultiHost-2x4", labels.TpuV5LitePodsliceValue, 4, "2x4")
	tpuv5LitePodsliceMultiPod4x4 := buildTpuPod("TPUv5LitePodscliceMultiHost-4x4", labels.TpuV5LitePodsliceValue, 4, "4x4")
	tpu7xPod1x1x1 := buildTpuPod("TPU7x-1x1x1", labels.Tpu7xValue, 1, "1x1x1")
	tpu7xPod2x2x1 := buildTpuPod("TPU7x-2x2x1", labels.Tpu7xValue, 4, "2x2x1")
	tpu7xMultiPod4x4x4 := buildTpuPod("TPU7x-4x4x4", labels.Tpu7xValue, 4, "4x4x4")
	tpu7Pod1x1x1 := buildTpuPod("TPU7-1x1x1", labels.Tpu7Value, 1, "1x1x1")
	tpu7Pod2x2x1 := buildTpuPod("TPU7-2x2x1", labels.Tpu7Value, 4, "2x2x1")
	ccLabel := "autoscaling.gke.io/cc-label"
	tpuCCName := "tpu-cc"
	tpuV4Rule := rules.NewRule(rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2x2"))
	tpuV5eRule := rules.NewRule(rules.WithTpuRule(labels.TpuV5LitePodsliceValue, 4, "2x2"))
	incorrectTpuRule := rules.NewRule(rules.WithTpuRule("incorrect-tpu", 10, "6x6"))
	singleTpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{tpuV4Rule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	multipleTpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{tpuV4Rule, tpuV5eRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	incorrectTPUCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{incorrectTpuRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	draTpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{tpuV4Rule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithTpuDriverMode(computeclass.TpuDriverModeDynamicResourceAllocation),
	)
	draTpuCCScaleUpAnyway := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{tpuV4Rule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithTpuDriverMode(computeclass.TpuDriverModeDynamicResourceAllocation),
		computeclass.WithScaleUpAnyway(),
	)

	ccTpuPod := buildTpuPod("cc-tpu", "", 4, "")
	ccTpuPod = addSeparation(ccTpuPod, ccLabel, tpuCCName, true)

	draTpuPod := test.BuildTestPod("dra-tpu", 0, 0)
	draTpuPod = addSeparation(draTpuPod, ccLabel, tpuCCName, true)
	draTpuPod.Spec.ResourceClaims = []apiv1.PodResourceClaim{{}}

	defaultLimits := map[string]int64{
		labels.TpuV3DeviceValue:       4,
		labels.TpuV3SliceValue:        16,
		labels.TpuV4LiteDeviceValue:   8,
		labels.TpuV4PodsliceValue:     8,
		labels.TpuV5LiteDeviceValue:   16,
		labels.TpuV5LitePodsliceValue: 16,
		labels.TpuV5PSliceValue:       16,
		labels.Tpu7xValue:             4,
		labels.Tpu7Value:              4,
	}

	podMissingTpuLabel := test.BuildTestPod("pod-missing-tpu-label", 0, 1000)
	podMissingTpuLabel.Spec.NodeSelector = map[string]string{
		labels.AcceleratorCountLabel: "4",
	}

	tests := []struct {
		name                 string
		pods                 []*apiv1.Pod
		expectedRequirements []nodeGroupRequirements // Only pods and TpuRequest are checked.
		expectedPodStatuses  map[types.UID]PodProcessingStatus
		cc                   computeclass.CRD
	}{
		{
			name:                 "no tpu pods",
			pods:                 []*apiv1.Pod{plain1, plain2},
			expectedRequirements: []nodeGroupRequirements{},
		},
		{
			name:                 "TPU Pod with AcceleratorCount but missing TPULabel",
			pods:                 []*apiv1.Pod{podMissingTpuLabel},
			expectedRequirements: []nodeGroupRequirements{},
		},
		{
			name: "TPU Device Pod without resource limit set for TPU",
			pods: []*apiv1.Pod{tpuNoLimitSetPod},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3DeviceValue,
						ChipsPerNode: 4,
						Topology:     "2x2",
					},
					pods: []*apiv1.Pod{tpuNoLimitSetPod},
				},
			},
		},
		{
			name: "TPU v3 Device SingleHost Pod",
			pods: []*apiv1.Pod{tpuv3DeviceSinglePod},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3DeviceValue,
						ChipsPerNode: 4,
						Topology:     "2x2",
					},
					pods: []*apiv1.Pod{tpuv3DeviceSinglePod},
				},
			},
		},
		{
			name: "TPU v3 Podslice MultiHost Pod 4x4",
			pods: []*apiv1.Pod{tpuv3PodsliceMultiPod1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3SliceValue,
						ChipsPerNode: 4,
						Topology:     "4x4",
					},
					pods: []*apiv1.Pod{tpuv3PodsliceMultiPod1},
				},
			},
		},
		{
			name: "TPU v3 Podslice MultiHost Pod 4x8",
			pods: []*apiv1.Pod{tpuv3PodsliceMultiPod2},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3SliceValue,
						ChipsPerNode: 4,
						Topology:     "4x8",
					},
					pods: []*apiv1.Pod{tpuv3PodsliceMultiPod2},
				},
			},
		},
		{
			name: "TPU v3 Podslice MultiHost Pod 8x8",
			pods: []*apiv1.Pod{tpuv3PodsliceMultiPod3},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3SliceValue,
						ChipsPerNode: 4,
						Topology:     "8x8",
					},
					pods: []*apiv1.Pod{tpuv3PodsliceMultiPod3},
				},
			},
		},
		{
			name: "TPU Device Pod",
			pods: []*apiv1.Pod{tpuDevicePod},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4LiteDeviceValue,
						ChipsPerNode: 4,
					},
					pods: []*apiv1.Pod{tpuDevicePod},
				},
			},
		},
		{
			name: "TPU Device Pod with plain pod",
			pods: []*apiv1.Pod{tpuDevicePod, plain1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4LiteDeviceValue,
						ChipsPerNode: 4,
					},
					pods: []*apiv1.Pod{tpuDevicePod},
				},
			},
		},
		{
			name: "TPU Podslice MultiHost Pod",
			pods: []*apiv1.Pod{tpuPodsliceMultiPod1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuPodsliceMultiPod1},
				},
			},
		},
		{
			name: "TPU Podslice SingleHost Pod",
			pods: []*apiv1.Pod{tpuPodsliceSinglePod},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpuPodsliceSinglePod},
				},
			},
		},
		{
			name: "Both TPU Podslice Multi and Single Host Pods",
			pods: []*apiv1.Pod{tpuPodsliceMultiPod1, tpuPodsliceSinglePod},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuPodsliceMultiPod1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpuPodsliceSinglePod},
				},
			},
		},
		{
			name: "Two TPU Podslice Multihost with different topologies",
			pods: []*apiv1.Pod{tpuPodsliceMultiPod1, tpuPodsliceMultiPod2},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuPodsliceMultiPod1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x4",
					},
					pods: []*apiv1.Pod{tpuPodsliceMultiPod2},
				},
			},
		},
		{
			name: "Two TPU Podslice Multihost with same topology",
			pods: []*apiv1.Pod{tpuPodsliceMultiPod2, tpuPodsliceMultiPod3},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x4",
					},
					pods: []*apiv1.Pod{tpuPodsliceMultiPod2, tpuPodsliceMultiPod3},
				},
			},
		},
		{
			name: "TPU v5 Podslice MultiHost Pod",
			pods: []*apiv1.Pod{tpuv5sliceMultiPod1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuv5sliceMultiPod1},
				},
			},
		},
		{
			name: "TPU Podslice SingleHost Pod",
			pods: []*apiv1.Pod{tpuv5sliceSinglePod},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpuv5sliceSinglePod},
				},
			},
		},
		{
			name: "Both TPU v5 Podslice Multi and Single Host Pods",
			pods: []*apiv1.Pod{tpuv5sliceMultiPod1, tpuv5sliceSinglePod},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuv5sliceMultiPod1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpuv5sliceSinglePod},
				},
			},
		},
		{
			name: "Two TPU v5 Podslice Multihost with different topologies",
			pods: []*apiv1.Pod{tpuv5sliceMultiPod1, tpuv5sliceMultiPod2},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuv5sliceMultiPod1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x4",
					},
					pods: []*apiv1.Pod{tpuv5sliceMultiPod2},
				},
			},
		},
		{
			name: "Two TPU v5 Podslice Multihost with same topology",
			pods: []*apiv1.Pod{tpuv5sliceMultiPod2, tpuv5sliceMultiPod3},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x4",
					},
					pods: []*apiv1.Pod{tpuv5sliceMultiPod2, tpuv5sliceMultiPod3},
				},
			},
		},
		{
			name: "TPU v5 Lite Device Pod 1x1",
			pods: []*apiv1.Pod{tpuv5LiteDevicePod1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LiteDeviceValue,
						ChipsPerNode: 1,
						Topology:     "1x1",
					},
					pods: []*apiv1.Pod{tpuv5LiteDevicePod1},
				},
			},
		},
		{
			name: "TPU v5 Lite Device Pod 2x2",
			pods: []*apiv1.Pod{tpuv5LiteDevicePod2},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LiteDeviceValue,
						ChipsPerNode: 4,
						Topology:     "2x2",
					},
					pods: []*apiv1.Pod{tpuv5LiteDevicePod2},
				},
			},
		},
		{
			name: "TPU v5 Lite Device Pod 2x4",
			pods: []*apiv1.Pod{tpuv5LiteDevicePod3},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LiteDeviceValue,
						ChipsPerNode: 8,
						Topology:     "2x4",
					},
					pods: []*apiv1.Pod{tpuv5LiteDevicePod3},
				},
			},
		},
		{
			name: "TPU v5 lite Podslice Singlehost",
			pods: []*apiv1.Pod{tpuv5LitePodsliceSinglePod1x1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LitePodsliceValue,
						ChipsPerNode: 1,
						Topology:     "1x1",
					},
					pods: []*apiv1.Pod{tpuv5LitePodsliceSinglePod1x1},
				},
			},
		},
		{
			name: "Two TPU v5 lite Podslice Singlehost pods",
			pods: []*apiv1.Pod{tpuv5LitePodsliceSinglePod1x1, tpuv5LitePodsliceSinglePod2x2},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LitePodsliceValue,
						ChipsPerNode: 1,
						Topology:     "1x1",
					},
					pods: []*apiv1.Pod{tpuv5LitePodsliceSinglePod1x1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LitePodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2",
					},
					pods: []*apiv1.Pod{tpuv5LitePodsliceSinglePod2x2},
				},
			},
		},
		{
			name: "TPU v5 lite Podslice Singlehost and Multihost same topology",
			pods: []*apiv1.Pod{tpuv5LitePodsliceSinglePod2x4, tpuv5LitePodsliceMultiPod2x4},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LitePodsliceValue,
						ChipsPerNode: 8,
						Topology:     "2x4",
					},
					pods: []*apiv1.Pod{tpuv5LitePodsliceSinglePod2x4},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LitePodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x4",
					},
					pods: []*apiv1.Pod{tpuv5LitePodsliceMultiPod2x4},
				},
			},
		},
		{
			name: "TPU v5 lite Podslice Multihost",
			pods: []*apiv1.Pod{tpuv5LitePodsliceMultiPod4x4},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LitePodsliceValue,
						ChipsPerNode: 4,
						Topology:     "4x4",
					},
					pods: []*apiv1.Pod{tpuv5LitePodsliceMultiPod4x4},
				},
			},
		},
		{
			name: "TPU7x 1x1x1 topology",
			pods: []*apiv1.Pod{tpu7xPod1x1x1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.Tpu7xValue,
						ChipsPerNode: 1,
						Topology:     "1x1x1",
					},
					pods: []*apiv1.Pod{tpu7xPod1x1x1},
				},
			},
		},
		{
			name: "TPU7x 2x2x1 topology",
			pods: []*apiv1.Pod{tpu7xPod2x2x1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.Tpu7xValue,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpu7xPod2x2x1},
				},
			},
		},
		{
			name: "TPU7x 4x4x4 topology",
			pods: []*apiv1.Pod{tpu7xMultiPod4x4x4},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.Tpu7xValue,
						ChipsPerNode: 4,
						Topology:     "4x4x4",
					},
					pods: []*apiv1.Pod{tpu7xMultiPod4x4x4},
				},
			},
		},
		{
			name: "TPU7 1x1x1 topology",
			pods: []*apiv1.Pod{tpu7Pod1x1x1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.Tpu7Value,
						ChipsPerNode: 1,
						Topology:     "1x1x1",
					},
					pods: []*apiv1.Pod{tpu7Pod1x1x1},
				},
			},
		},
		{
			name: "TPU7 2x2x1 topology",
			pods: []*apiv1.Pod{tpu7Pod2x2x1},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.Tpu7Value,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpu7Pod2x2x1},
				},
			},
		},
		{
			name: "TPU requested through CRD",
			pods: []*apiv1.Pod{ccTpuPod},
			cc:   singleTpuCC,
			expectedRequirements: []nodeGroupRequirements{
				{
					computeClass: singleTpuCC,
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{ccTpuPod},
				},
			},
		}, {
			name: "TPU requested through CRD with multiple rules",
			pods: []*apiv1.Pod{ccTpuPod},
			cc:   multipleTpuCC,
			expectedRequirements: []nodeGroupRequirements{
				{
					computeClass: multipleTpuCC,
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{ccTpuPod},
				}, {
					computeClass: multipleTpuCC,
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5LitePodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2",
					},
					pods: []*apiv1.Pod{ccTpuPod},
				},
			},
		}, {
			name:                 "TPU requested through CRD with incorrect rule",
			pods:                 []*apiv1.Pod{ccTpuPod},
			cc:                   incorrectTPUCC,
			expectedRequirements: []nodeGroupRequirements{},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				ccTpuPod.UID: {
					Err: NewTpuTypeNotSupportedError("incorrect-tpu"),
				},
			},
		},
		{
			name: "DRA TPU Pod",
			pods: []*apiv1.Pod{draTpuPod},
			cc:   draTpuCC,
			expectedRequirements: []nodeGroupRequirements{
				{
					computeClass: draTpuCC,
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{draTpuPod},
				},
			},
		},
		{
			name: "DRA TPU Pod with ScaleUpAnyway, no default requirement",
			pods: []*apiv1.Pod{draTpuPod},
			cc:   draTpuCCScaleUpAnyway,
			expectedRequirements: []nodeGroupRequirements{
				{
					computeClass: draTpuCCScaleUpAnyway,
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{draTpuPod},
				},
			},
		},
		{
			name: "All TPU and plain pods",
			pods: []*apiv1.Pod{
				plain1,
				plain2,
				tpuv3DeviceSinglePod,
				tpuv3PodsliceMultiPod1,
				tpuv3PodsliceMultiPod2,
				tpuv3PodsliceMultiPod3,
				tpuDevicePod,
				tpuPodsliceMultiPod1,
				tpuPodsliceMultiPod2,
				tpuPodsliceMultiPod3,
				tpuPodsliceSinglePod,
				tpuv5sliceSinglePod,
				tpuv5sliceMultiPod1,
				tpuv5sliceMultiPod2,
				tpuv5sliceMultiPod3,
			},
			expectedRequirements: []nodeGroupRequirements{
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3DeviceValue,
						ChipsPerNode: 4,
						Topology:     "2x2",
					},
					pods: []*apiv1.Pod{tpuv3DeviceSinglePod},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3SliceValue,
						ChipsPerNode: 4,
						Topology:     "4x4",
					},
					pods: []*apiv1.Pod{tpuv3PodsliceMultiPod1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3SliceValue,
						ChipsPerNode: 4,
						Topology:     "4x8",
					},
					pods: []*apiv1.Pod{tpuv3PodsliceMultiPod2},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV3SliceValue,
						ChipsPerNode: 4,
						Topology:     "8x8",
					},
					pods: []*apiv1.Pod{tpuv3PodsliceMultiPod3},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4LiteDeviceValue,
						ChipsPerNode: 4,
					},
					pods: []*apiv1.Pod{tpuDevicePod},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpuPodsliceSinglePod},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuPodsliceMultiPod1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV4PodsliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x4",
					},
					pods: []*apiv1.Pod{tpuPodsliceMultiPod2, tpuPodsliceMultiPod3},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x1",
					},
					pods: []*apiv1.Pod{tpuv5sliceSinglePod},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x2",
					},
					pods: []*apiv1.Pod{tpuv5sliceMultiPod1},
				},
				{
					tpuRequest: TpuRequest{
						TpuType:      labels.TpuV5PSliceValue,
						ChipsPerNode: 4,
						Topology:     "2x2x4",
					},
					pods: []*apiv1.Pod{tpuv5sliceMultiPod2, tpuv5sliceMultiPod3},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := defaultLimits
			zones := []string{"us-central2-b", "us-west1-b"}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutoprovisioningEnabled(true).Build()
			lister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{tc.cc})
			lister.SetCrdLabel(ccLabel)
			em := experiments.NewMockManager()
			manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider: provider,
				Lister:        lister,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					TpuAutoprovisioningEnabled: true,
				},
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			})
			ctx := &injectionContext{
				status:          NewProcessingStatus(),
				zones:           zones,
				resourceLimiter: cloudprovider.NewResourceLimiter(nil, limits),
				clusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}

			gotRequirements := manager.tpuPodsRequirements(ctx, tc.pods)
			if diff := cmp.Diff(tc.expectedRequirements, gotRequirements, requirementsIgnoreOrderOpt, requirementsCompareOnlyPodsAndTpuOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("expected requirements differ (only pods and tpuRequests are compared), diff (-want +got):\n%s", diff)
			}
			selErrCmp := cmp.Comparer(func(e1, e2 *machineselection.Error) bool {
				if e1.Type() == machineselection.TpuIncompatibleError {
					tpuNamesMatch := e1.TpuName == e2.TpuName
					return e1.MachineGroupName == e2.MachineGroupName && tpuNamesMatch
				}
				return e1 == e2 || cmp.Equal(e1, e2)
			})
			napErrCmp := cmp.Comparer(func(e1, e2 *Error) bool {
				return e1 == e2 || cmp.Equal(e1, e2)
			})
			if diff := cmp.Diff(tc.expectedPodStatuses, ctx.status.PodStatuses, selErrCmp, napErrCmp, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("expected pod statuses differ, diff (-want + got):\n%s", diff)
			}
		})
	}
}
