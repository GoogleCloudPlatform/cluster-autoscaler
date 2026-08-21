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

package localssdsize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
)

func TestSSDSizeInGiB(t *testing.T) {
	testCases := map[string]struct {
		machineType           string
		diskSizeMap           map[string]uint64
		simpleSSDSizeProvider localssdsize.LocalSSDSizeProvider
		expectedSSDSize       uint64
	}{
		"Machine type is not present - Machine family is not present as well": {
			machineType: "my-custom-machine-type",
			simpleSSDSizeProvider: &mockSSDSizeProvider{
				sizeMap: map[string]uint64{"my-custom-machine-type": 512},
			},
			expectedSSDSize: 512,
		},
		"Machine type is not present - Machine family is present": {
			machineType: "my-custom-machine-type",
			diskSizeMap: map[string]uint64{
				"my": 123,
			},
			expectedSSDSize: 123,
		},
		"Machine type is not present - Invalid machine family": {
			machineType: "abcd",
			simpleSSDSizeProvider: &mockSSDSizeProvider{
				sizeMap: map[string]uint64{"abcd": 512},
			},
			expectedSSDSize: 512,
		},
		"Machine type is present - Machine family is present": {
			machineType: "my-custom-machine-type",
			diskSizeMap: map[string]uint64{
				"my-custom-machine-type": 512,
				"my":                     123,
			},
			expectedSSDSize: 512,
		},
	}

	for desc, tc := range testCases {
		t.Run(desc, func(t *testing.T) {
			provider := &DynamicLocalSSDDiskSizeProvider{
				machineToDiskSize: tc.diskSizeMap,
				simpleSSDProvider: tc.simpleSSDSizeProvider,
			}
			assert.Equal(t, tc.expectedSSDSize, provider.SSDSizeInGiB(tc.machineType))
		})
	}
}

type mockSSDSizeProvider struct {
	sizeMap map[string]uint64
}

func (m *mockSSDSizeProvider) SSDSizeInGiB(machineType string) uint64 {
	return m.sizeMap[machineType]
}
