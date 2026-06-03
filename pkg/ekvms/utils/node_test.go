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

package utils

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
)

const (
	giBToKiB = int64(1024 * 1024)
)

func TestUpdateAllocatable(t *testing.T) {
	emptyAllocatableNode := ekvms_test.EkNode32("node1", 1000, 1024)
	emptyAllocatableNode.Status.Allocatable = nil

	testCases := []struct {
		desc         string
		node         *v1.Node
		size         size.Allocatable
		expectedNode *v1.Node
	}{
		{
			desc: "Updates existing allocatable",
			node: ekvms_test.EkNode32("node1", 1000, 1024),
			size: size.Allocatable{
				MilliCpus: 1234,
				KBytes:    5678 * giBToKiB},
			expectedNode: ekvms_test.EkNode32("node1", 1234, 5678*giBToKiB*size.KiB),
		},
		{
			desc: "Updates empty allocatable",
			node: emptyAllocatableNode,
			size: size.Allocatable{
				MilliCpus: 1234,
				KBytes:    5678 * giBToKiB},
			expectedNode: ekvms_test.EkNode32("node1", 1234, 5678*giBToKiB*size.KiB),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			UpdateAllocatable(tc.node, tc.size)
			assert.Equal(t, trimToCpuAndMemory(tc.expectedNode), trimToCpuAndMemory(tc.node))
		})
	}
}

func TestIsEkMachine(t *testing.T) {
	testCases := []struct {
		desc              string
		instanceTypeLabel string
		wantIsEk          bool
		wantErr           string
	}{
		{
			desc:              "Node with EK Instance type",
			instanceTypeLabel: "ek-standard-32",
			wantIsEk:          true,
		},
		{
			desc:              "Node with a non-EK Instance type",
			instanceTypeLabel: "xx-standard-32",
			wantIsEk:          false,
		},
		{
			desc:              "Node with a wrong-formatted Instance type",
			instanceTypeLabel: "ek",
			wantErr:           "unable to parse machine type",
		},
		{
			desc:              "Node without an instance type",
			instanceTypeLabel: "",
			wantErr:           "does not have instance type label set",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := ekvms_test.NewResizableNodeBuilder("node-1", 1000, 1024).Build()
			if tc.instanceTypeLabel != "" {
				node.SetLabels(map[string]string{v1.LabelInstanceTypeStable: tc.instanceTypeLabel})
			}
			isEk, err := IsEkMachine(node)
			if tc.wantErr == "" {
				assert.Equal(t, tc.wantIsEk, isEk)
			} else if assert.Error(t, err) {
				match, matchErr := regexp.MatchString(tc.wantErr, err.Error())
				assert.NoError(t, matchErr)
				assert.True(t, match, "Expected error [%s], but got [%s]", tc.wantErr, err.Error())
			}
		})
	}
}

func trimToCpuAndMemory(node *v1.Node) *v1.Node {
	if node == nil || node.Status.Allocatable == nil {
		return node
	}
	for res := range node.Status.Allocatable {
		if res == v1.ResourceCPU || res == v1.ResourceMemory {
			continue
		}
		delete(node.Status.Allocatable, res)
	}
	return node
}

func TestGetMaxResizableVmSize(t *testing.T) {
	testCases := []struct {
		desc              string
		instanceTypeLabel string
		wantVmSize        size.VmSize
		wantErr           string
	}{
		{
			desc:              "EK standard 32",
			instanceTypeLabel: "ek-standard-32",
			wantVmSize:        size.VmSize{MilliCpus: 32000, KBytes: 134217728},
		},
		{
			desc:              "E4A standard 16",
			instanceTypeLabel: "e4a-standard-16",
			wantVmSize:        size.VmSize{MilliCpus: 16000, KBytes: 67108864},
		},
		{
			desc:              "E2 standard 32",
			instanceTypeLabel: "e2-standard-32",
			wantErr:           "machine family \"e2\" is not resizable",
		},
		{
			desc:              "Node without an instance type",
			instanceTypeLabel: "",
			wantErr:           "does not have instance type label set",
		},
		{
			desc:              "Node with an invalid instance type",
			instanceTypeLabel: "invalid-instance",
			wantErr:           "unsupported machine family \"invalid\"",
		},
		{
			desc:              "Node with an unknown ek type",
			instanceTypeLabel: "ek-standard-xx",
			wantErr:           "unknown machine type \"ek-standard-xx\"",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := ekvms_test.NewResizableNodeBuilder("node-1", 1000, 1024).Build()
			if tc.instanceTypeLabel != "" {
				node.SetLabels(map[string]string{v1.LabelInstanceTypeStable: tc.instanceTypeLabel})
			}
			vmSize, err := GetMaxResizableVmSize(machinetypes.NewMachineConfigProvider(nil), node)
			if tc.wantErr == "" {
				assert.Equal(t, tc.wantVmSize, vmSize)
			} else if assert.Error(t, err) {
				match, matchErr := regexp.MatchString(tc.wantErr, err.Error())
				assert.NoError(t, matchErr)
				assert.True(t, match, "Expected error [%s], but got [%s]", tc.wantErr, err.Error())
			}
		})
	}
}

func TestGetMachineTypeFromLabels(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		labels          map[string]string
		wantMachineType string
		wantFound       bool
	}{
		{
			desc:            "found ek-standard-32 as stable label",
			labels:          map[string]string{v1.LabelInstanceTypeStable: "ek-standard-32", "random-label": "random-value"},
			wantMachineType: "ek-standard-32",
			wantFound:       true,
		},
		{
			desc:            "found ek-standard-32 as beta label",
			labels:          map[string]string{"random-label": "random-value", v1.LabelInstanceType: "ek-standard-32"},
			wantMachineType: "ek-standard-32",
			wantFound:       true,
		},
		{
			desc:            "stable label prioritized over beta label",
			labels:          map[string]string{v1.LabelInstanceType: "beta-label", v1.LabelInstanceTypeStable: "stable-label"},
			wantMachineType: "stable-label",
			wantFound:       true,
		},
		{
			desc:      "no machine type label",
			labels:    map[string]string{"random-label": "random-value"},
			wantFound: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			machineType, found := GetMachineTypeFromLabels(tc.labels)
			assert.Equal(t, tc.wantMachineType, machineType)
			assert.Equal(t, tc.wantFound, found)
		})
	}
}

func TestGetMachineFamilyName(t *testing.T) {
	testCases := []struct {
		desc              string
		instanceTypeLabel string
		wantFamily        string
		wantErr           string
	}{
		{
			desc:              "EK standard 32",
			instanceTypeLabel: "ek-standard-32",
			wantFamily:        "ek",
		},
		{
			desc:              "E2 standard 32",
			instanceTypeLabel: "e2-standard-32",
			wantFamily:        "e2",
		},
		{
			desc:              "Node without an instance type",
			instanceTypeLabel: "",
			wantErr:           "does not have instance type label set",
		},
		{
			desc:              "Node with an invalid instance type",
			instanceTypeLabel: "invalid",
			wantErr:           "unable to parse machine type",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := test.BuildTestNode("node-1", 1000, 1024)
			if tc.instanceTypeLabel != "" {
				node.SetLabels(map[string]string{v1.LabelInstanceTypeStable: tc.instanceTypeLabel})
			}
			family, err := GetMachineFamilyName(node)

			if tc.wantErr == "" {
				assert.Equal(t, tc.wantFamily, family)
			} else if assert.Error(t, err) {
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

func TestIsResizableMachineType(t *testing.T) {
	testCases := []struct {
		desc        string
		machineType string
		wantResult  bool
		wantErr     string
	}{
		{
			desc:        "EK standard 32 is resizable",
			machineType: "ek-standard-32",
			wantResult:  true,
		},
		{
			desc:        "E4A standard 8 is resizable",
			machineType: "e4a-standard-8",
			wantResult:  true,
		},
		{
			desc:        "E2 standard 32 is not resizable",
			machineType: "e2-standard-32",
			wantResult:  false,
		},
		{
			desc:        "N1 standard 1 is not resizable",
			machineType: "n1-standard-1",
			wantResult:  false,
		},
		{
			desc:        "Invalid machine type returns error",
			machineType: "invalid-machine-type",
			wantErr:     "unsupported machine family \"invalid\"",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := IsResizableMachineType(machinetypes.NewMachineConfigProvider(nil), tc.machineType)
			if tc.wantErr == "" {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantResult, result)
			} else if assert.Error(t, err) {
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}
