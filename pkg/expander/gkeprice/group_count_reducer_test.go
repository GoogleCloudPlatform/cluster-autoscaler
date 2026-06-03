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

package gkeprice

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func TestGroupCountPenalty(t *testing.T) {
	testCases := []struct {
		migSize          int64
		expectedResult   float64
		useExactEquality bool
	}{
		{migSize: 0, expectedResult: 1.0004, useExactEquality: true},
		{migSize: 1, expectedResult: 1.0008, useExactEquality: true},
		{migSize: 5, expectedResult: 1.01},
		{migSize: 10, expectedResult: 1.04},
		{migSize: 17, expectedResult: 1.12},
		{migSize: 20, expectedResult: 1.16},
		{migSize: 30, expectedResult: 1.36},
		{migSize: 35, expectedResult: 1.49},
		{migSize: 40, expectedResult: 1.64},
		{migSize: 50, expectedResult: 2},
		{migSize: 52, expectedResult: 2.08},
		{migSize: 60, expectedResult: 2.44},
		{migSize: 70, expectedResult: 2.96},
		{migSize: 80, expectedResult: 3.56},
		{migSize: 87, expectedResult: 4.03},
		{migSize: 90, expectedResult: 4.24},
		{migSize: 100, expectedResult: 5},
		{migSize: 150, expectedResult: 10},
		{migSize: 200, expectedResult: 17},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("MIGs count %d", tc.migSize), func(t *testing.T) {
			result := groupCountPenalty(tc.migSize)
			if tc.useExactEquality {
				assert.Equal(t, tc.expectedResult, result)
			} else {
				assert.InEpsilon(t, tc.expectedResult, result, 0.01,
					"MIGs %v, result %v, expected %v", tc.migSize, result, tc.expectedResult)
			}
		})
	}
}

func TestProgressiveGroupCountReducer(t *testing.T) {
	testCases := []struct {
		locations      int
		poolCount      int
		hasGpu         bool
		expectedResult float64
	}{
		{1, 1, false, 1.5 * 1},
		{1, 10, false, 1.5 * 1.12},
		{1, 30, false, 1.5 * 2.08},
		{1, 50, false, 1.5 * 4.03},
		{1, 58, false, 1.5 * 5.08},
		{1, 1, true, 1.1 * 1},
		{1, 10, true, 1.1 * 1.12},
		{1, 30, true, 1.1 * 2.08},
		{1, 50, true, 1.1 * 4.03},
		{3, 1, false, 1.5 * 1},
		{3, 10, false, 1.5 * 1.36},
		{3, 30, false, 1.5 * 4.24},
		{3, 1, true, 1.1 * 1},
		{3, 10, true, 1.1 * 1.36},
		{3, 30, true, 1.1 * 4.24},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("locations=%d, poolCount=%d, hasGpu=%v", tc.locations, tc.poolCount, tc.hasGpu), func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			for i := 0; i < tc.poolCount; i++ {
				machineType := fmt.Sprintf("n1-standard-%d", i+1)
				for j := 0; j < tc.locations; j++ {
					groupId := fmt.Sprintf("%s-zone%d", machineType, j)
					provider.AddAutoprovisionedGkeNodeGroup(machineType, groupId, 0, true, false, machineType, false, true)
				}
			}
			reducer := NewProgressiveGroupCountReducer(provider)
			result := reducer.GroupCreationPenalty(tc.hasGpu)
			assert.InEpsilon(t, tc.expectedResult, result, 0.01,
				"hasGpu %v, pools %v, MIGs %v, result %v, expected %v",
				tc.hasGpu, tc.poolCount, len(provider.NodeGroups()), result, tc.expectedResult)
		})
	}
}

func TestProgressiveGroupCountReducerOnAutopilot(t *testing.T) {

	type poolStruct struct {
		locations       int
		poolCount       int
		autoprovisioned bool
	}

	testCases := []struct {
		pools          []poolStruct
		expectedResult float64
	}{
		{
			[]poolStruct{
				{1, 10, true},
			},
			1.5 * 1.12,
		},
		{
			[]poolStruct{
				{1, 10, true},
				{1, 10, false},
			},
			1.5 * 1.12,
		},
		{
			[]poolStruct{{1, 30, true}},
			1.5 * 2.08,
		},
		{
			[]poolStruct{
				{1, 30, true},
				{1, 10, false},
				{2, 2, false},
			},
			1.5 * 2.08,
		},
	}
	for idx, tc := range testCases {
		provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutopilotEnabled(true).Build()
		for k, pool := range tc.pools {
			for i := 0; i < pool.poolCount; i++ {
				machineType := fmt.Sprintf("n1-standard-%d", i+1)
				for j := 0; j < pool.locations; j++ {
					groupId := fmt.Sprintf("%s-zone%d-%d", machineType, j, k)
					provider.AddAutoprovisionedGkeNodeGroup(machineType, groupId, 0, true, pool.autoprovisioned, machineType, true, true)
				}
			}
		}
		reducer := NewProgressiveGroupCountReducer(provider)
		result := reducer.GroupCreationPenalty(false)
		assert.InEpsilon(t, tc.expectedResult, result, 0.01,
			"test #%v: MIGs %v, result %v, expected %v",
			idx, len(provider.NodeGroups()), result, tc.expectedResult)
	}
}
