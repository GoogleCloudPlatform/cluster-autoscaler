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

package selfservice

import (
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
)

// TestNodepoolMetadataBeforeSetCloudProvider mirrors the start-up order of the
// autoscaler: features are registered before the cloud provider exists, and
// node pool specs are built before SetCloudProvider is called. The metadata
// must be complete at that point, otherwise every node pool looks like it does
// not match its ComputeClass until the next cluster refresh.
func TestNodepoolMetadataBeforeSetCloudProvider(t *testing.T) {
	t.Cleanup(initDefaultSelfService)

	InitSelfService(defaultExperimentsManager())

	got := NodepoolMetadata(&container.NodePool{
		Config: &container.NodeConfig{
			ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
				EnableSecureBoot:          false,
				EnableIntegrityMonitoring: true,
			},
			AdvancedMachineFeatures: &container.AdvancedMachineFeatures{EnableNestedVirtualization: true},
		},
		NetworkConfig: &container.NodeNetworkConfig{EnablePrivateNodes: true},
	})

	assert.Equal(t, "false", got[secureBootMetadataKey])
	assert.Equal(t, "true", got[integrityMonitoringMetadataKey])
	assert.Equal(t, "true", got[nestedVirtualizationMetadataKey])
	assert.Equal(t, "true", got[privateNodeFromLabel])
	assert.Equal(t, "true", got[privateNodeFromCcc])
}

func TestSetCloudProvider(t *testing.T) {
	t.Cleanup(initDefaultSelfService)

	InitSelfService(defaultExperimentsManager())
	spec := v1.ComputeClassSpec{NodePoolConfig: &v1.NodePoolConfig{IPType: privateIPType}}

	// Before SetCloudProvider is called, features depending on it fall back to
	// defaults instead of panicking.
	assert.NotContains(t, ComputeClassSpecMetadata(spec), privateNodeFromCcc)

	SetCloudProvider(&mockCloudProvider{isPSC: true})
	assert.Equal(t, privateNodeTrue, ComputeClassSpecMetadata(spec)[privateNodeFromCcc])
}

func TestNodepoolMetadataBeforeInitSelfService(t *testing.T) {
	t.Cleanup(initDefaultSelfService)

	supportedFeatures = nil

	assert.Nil(t, NodepoolMetadata(&container.NodePool{
		Config: &container.NodeConfig{
			ShieldedInstanceConfig: &container.ShieldedInstanceConfig{EnableIntegrityMonitoring: true},
		},
	}))
}
