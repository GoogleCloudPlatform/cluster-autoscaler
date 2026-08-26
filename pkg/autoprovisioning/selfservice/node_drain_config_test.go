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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/utils/ptr"
)

func TestNodeDrainConfigFromNodepool(t *testing.T) {
	testCases := []struct {
		name         string
		nodepool     *container.NodePool
		wantMetadata Metadata
	}{
		{
			name:         "Nodepool is nil",
			nodepool:     nil,
			wantMetadata: nil,
		},
		{
			name:         "Nodepool is empty",
			nodepool:     &container.NodePool{},
			wantMetadata: nil,
		},
		{
			name: "Nodepool with nil NodeDrainConfig",
			nodepool: &container.NodePool{
				Name: "test-nodepool",
			},
			wantMetadata: nil,
		},
		{
			name: "Nodepool with full NodeDrainConfig (RespectPdb=true)",
			nodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					PdbTimeoutDuration:               "60s",
					GraceTerminationDuration:         "120s",
					RespectPdbDuringNodePoolDeletion: true,
				},
			},
			wantMetadata: Metadata{
				pdbTimeoutDurationMetadataKey:               "60s",
				graceTerminationDurationMetadataKey:         "120s",
				respectPdbDuringNodePoolDeletionMetadataKey: "true",
			},
		},
		{
			name: "Nodepool with full NodeDrainConfig (RespectPdb=false)",
			nodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					PdbTimeoutDuration:               "1800s",
					GraceTerminationDuration:         "3600s",
					RespectPdbDuringNodePoolDeletion: false,
				},
			},
			wantMetadata: Metadata{
				pdbTimeoutDurationMetadataKey:               "1800s",
				graceTerminationDurationMetadataKey:         "3600s",
				respectPdbDuringNodePoolDeletionMetadataKey: "false",
			},
		},
		{
			name: "Nodepool with only PdbTimeoutDuration",
			nodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					PdbTimeoutDuration: "60s",
				},
			},
			wantMetadata: Metadata{
				pdbTimeoutDurationMetadataKey:               "60s",
				respectPdbDuringNodePoolDeletionMetadataKey: "false",
			},
		},
		{
			name: "Nodepool with only GraceTerminationDuration",
			nodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					GraceTerminationDuration: "120s",
				},
			},
			wantMetadata: Metadata{
				graceTerminationDurationMetadataKey:         "120s",
				respectPdbDuringNodePoolDeletionMetadataKey: "false",
			},
		},
		{
			name: "Nodepool with only RespectPdbDuringNodePoolDeletion=true",
			nodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					RespectPdbDuringNodePoolDeletion: true,
				},
			},
			wantMetadata: Metadata{
				respectPdbDuringNodePoolDeletionMetadataKey: "true",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodeDrainConfig()
			gotMetadata := f.FromNodepool(tc.nodepool)
			assert.Equal(t, tc.wantMetadata, gotMetadata)
		})
	}
}

func TestNodeDrainConfigFromCccSpec(t *testing.T) {
	testCases := []struct {
		name         string
		spec         v1.ComputeClassSpec
		wantMetadata Metadata
	}{
		{
			name:         "Empty spec",
			spec:         v1.ComputeClassSpec{},
			wantMetadata: nil,
		},
		{
			name: "Nil NodePoolConfig",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: nil,
			},
			wantMetadata: nil,
		},
		{
			name: "Nil NodeDrainConfig",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					NodeDrainConfig: nil,
				},
			},
			wantMetadata: nil,
		},
		{
			name: "Empty NodeDrainConfig",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					NodeDrainConfig: &v1.NodeDrainConfig{},
				},
			},
			wantMetadata: nil,
		},
		{
			name: "PdbTimeoutDuration specified",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					NodeDrainConfig: &v1.NodeDrainConfig{
						PdbTimeoutDuration: ptr.To("60s"),
					},
				},
			},
			wantMetadata: Metadata{
				pdbTimeoutDurationMetadataKey: "60s",
			},
		},
		{
			name: "GraceTerminationDuration specified",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					NodeDrainConfig: &v1.NodeDrainConfig{
						GraceTerminationDuration: ptr.To("120s"),
					},
				},
			},
			wantMetadata: Metadata{
				graceTerminationDurationMetadataKey: "120s",
			},
		},
		{
			name: "RespectPdbDuringNodePoolDeletion is true",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					NodeDrainConfig: &v1.NodeDrainConfig{
						RespectPdbDuringNodePoolDeletion: ptr.To(true),
					},
				},
			},
			wantMetadata: Metadata{
				respectPdbDuringNodePoolDeletionMetadataKey: "true",
			},
		},
		{
			name: "RespectPdbDuringNodePoolDeletion is false",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					NodeDrainConfig: &v1.NodeDrainConfig{
						RespectPdbDuringNodePoolDeletion: ptr.To(false),
					},
				},
			},
			wantMetadata: Metadata{
				respectPdbDuringNodePoolDeletionMetadataKey: "false",
			},
		},
		{
			name: "All fields specified",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					NodeDrainConfig: &v1.NodeDrainConfig{
						PdbTimeoutDuration:               ptr.To("60s"),
						GraceTerminationDuration:         ptr.To("120s"),
						RespectPdbDuringNodePoolDeletion: ptr.To(true),
					},
				},
			},
			wantMetadata: Metadata{
				pdbTimeoutDurationMetadataKey:               "60s",
				graceTerminationDurationMetadataKey:         "120s",
				respectPdbDuringNodePoolDeletionMetadataKey: "true",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodeDrainConfig()
			gotMetadata := f.FromCccSpec(tc.spec)
			assert.Equal(t, tc.wantMetadata, gotMetadata)
		})
	}
}

func TestNodeDrainConfigFromPriority(t *testing.T) {
	f := newNodeDrainConfig()
	assert.Nil(t, f.FromPriority(v1.Priority{}))
}

func TestNodeDrainConfigFromLabelRequirements(t *testing.T) {
	f := newNodeDrainConfig()
	assert.Nil(t, f.FromLabelRequirements(podrequirements.LabelRequirements{}))
}

func TestNodeDrainConfigToNodePoolLabels(t *testing.T) {
	f := newNodeDrainConfig()
	labels := make(map[string]string)
	f.ToNodePoolLabels(labels, Metadata{
		pdbTimeoutDurationMetadataKey:               "60s",
		graceTerminationDurationMetadataKey:         "120s",
		respectPdbDuringNodePoolDeletionMetadataKey: "true",
	})
	assert.Empty(t, labels)
}

func TestNodeDrainConfigToNodepool(t *testing.T) {
	testCases := []struct {
		name         string
		initialPool  *container.NodePool
		metadata     Metadata
		wantNodepool *container.NodePool
	}{
		{
			name:         "Empty metadata does nothing",
			initialPool:  &container.NodePool{},
			metadata:     Metadata{},
			wantNodepool: &container.NodePool{},
		},
		{
			name:        "PdbTimeoutDuration is set",
			initialPool: &container.NodePool{},
			metadata: Metadata{
				pdbTimeoutDurationMetadataKey: "60s",
			},
			wantNodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					PdbTimeoutDuration: "60s",
					ForceSendFields:    []string{"PdbTimeoutDuration"},
				},
			},
		},
		{
			name:        "GraceTerminationDuration is set",
			initialPool: &container.NodePool{},
			metadata: Metadata{
				graceTerminationDurationMetadataKey: "120s",
			},
			wantNodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					GraceTerminationDuration: "120s",
					ForceSendFields:          []string{"GraceTerminationDuration"},
				},
			},
		},
		{
			name:        "RespectPdbDuringNodePoolDeletion is true",
			initialPool: &container.NodePool{},
			metadata: Metadata{
				respectPdbDuringNodePoolDeletionMetadataKey: "true",
			},
			wantNodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					RespectPdbDuringNodePoolDeletion: true,
					ForceSendFields:                  []string{"RespectPdbDuringNodePoolDeletion"},
				},
			},
		},
		{
			name:        "RespectPdbDuringNodePoolDeletion is false",
			initialPool: &container.NodePool{},
			metadata: Metadata{
				respectPdbDuringNodePoolDeletionMetadataKey: "false",
			},
			wantNodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					RespectPdbDuringNodePoolDeletion: false,
					ForceSendFields:                  []string{"RespectPdbDuringNodePoolDeletion"},
				},
			},
		},
		{
			name:        "All fields set on new NodeDrainConfig",
			initialPool: &container.NodePool{},
			metadata: Metadata{
				pdbTimeoutDurationMetadataKey:               "60s",
				graceTerminationDurationMetadataKey:         "120s",
				respectPdbDuringNodePoolDeletionMetadataKey: "true",
			},
			wantNodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					PdbTimeoutDuration:               "60s",
					GraceTerminationDuration:         "120s",
					RespectPdbDuringNodePoolDeletion: true,
					ForceSendFields:                  []string{"PdbTimeoutDuration", "GraceTerminationDuration", "RespectPdbDuringNodePoolDeletion"},
				},
			},
		},
		{
			name: "Appending to existing NodeDrainConfig with preexisting ForceSendFields",
			initialPool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					PdbTimeoutDuration: "30s",
					ForceSendFields:    []string{"PdbTimeoutDuration"},
				},
			},
			metadata: Metadata{
				pdbTimeoutDurationMetadataKey:               "60s",
				graceTerminationDurationMetadataKey:         "120s",
				respectPdbDuringNodePoolDeletionMetadataKey: "false",
			},
			wantNodepool: &container.NodePool{
				NodeDrainConfig: &container.NodeDrainConfig{
					PdbTimeoutDuration:               "60s",
					GraceTerminationDuration:         "120s",
					RespectPdbDuringNodePoolDeletion: false,
					ForceSendFields:                  []string{"PdbTimeoutDuration", "GraceTerminationDuration", "RespectPdbDuringNodePoolDeletion"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newNodeDrainConfig()
			f.ToNodepool(tc.initialPool, tc.metadata)
			assert.Equal(t, tc.wantNodepool, tc.initialPool)
		})
	}
}
