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
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestMaintenanceExclusionFromNodepool(t *testing.T) {
	testCases := []struct {
		name         string
		np           *container.NodePool
		wantMetadata Metadata
	}{
		{
			name:         "nil nodepool",
			np:           nil,
			wantMetadata: nil,
		},
		{
			name:         "empty nodepool",
			np:           &container.NodePool{},
			wantMetadata: nil,
		},
		{
			name: "native eos exclusion enabled",
			np: &container.NodePool{
				MaintenancePolicy: &container.NodePoolMaintenancePolicy{
					ExclusionUntilEndOfSupport: &container.ExclusionUntilEndOfSupport{
						Enabled: true,
					},
				},
			},
			wantMetadata: Metadata{
				gkelabels.MaintenanceExclusionLabelKey: string(v1.MaintenanceExclusionUntilEndOfSupport),
			},
		},
		{
			name: "native eos exclusion disabled",
			np: &container.NodePool{
				MaintenancePolicy: &container.NodePoolMaintenancePolicy{
					ExclusionUntilEndOfSupport: &container.ExclusionUntilEndOfSupport{
						Enabled: false,
					},
				},
			},
			wantMetadata: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newMaintenanceExclusion()
			gotMetadata := f.FromNodepool(tc.np)
			assert.Equal(t, tc.wantMetadata, gotMetadata)
		})
	}
}

func TestMaintenanceExclusionFromCccSpec(t *testing.T) {
	exclusionVal := v1.MaintenanceExclusionUntilEndOfSupport
	unsupportedExclusionVal := v1.MaintenanceExclusionType("UNSUPPORTED")
	testCases := []struct {
		name         string
		spec         v1.ComputeClassSpec
		wantMetadata Metadata
	}{
		{
			name:         "empty spec",
			spec:         v1.ComputeClassSpec{},
			wantMetadata: nil,
		},
		{
			name: "nil nodepool config",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: nil,
			},
			wantMetadata: nil,
		},
		{
			name: "nil maintenance exclusion",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					MaintenanceExclusion: nil,
				},
			},
			wantMetadata: nil,
		},
		{
			name: "until end of support exclusion",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					MaintenanceExclusion: &exclusionVal,
				},
			},
			wantMetadata: Metadata{
				gkelabels.MaintenanceExclusionLabelKey: string(v1.MaintenanceExclusionUntilEndOfSupport),
			},
		},
		{
			name: "unsupported exclusion type",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{
					MaintenanceExclusion: &unsupportedExclusionVal,
				},
			},
			wantMetadata: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newMaintenanceExclusion()
			gotMetadata := f.FromCccSpec(tc.spec)
			assert.Equal(t, tc.wantMetadata, gotMetadata)
		})
	}
}

func TestMaintenanceExclusionToNodepool(t *testing.T) {
	testCases := []struct {
		name     string
		metadata Metadata
		wantNp   *container.NodePool
	}{
		{
			name:     "empty metadata",
			metadata: Metadata{},
			wantNp:   &container.NodePool{},
		},
		{
			name: "maintenance exclusion mapped to native field",
			metadata: Metadata{
				gkelabels.MaintenanceExclusionLabelKey: string(v1.MaintenanceExclusionUntilEndOfSupport),
			},
			wantNp: &container.NodePool{
				MaintenancePolicy: &container.NodePoolMaintenancePolicy{
					ExclusionUntilEndOfSupport: &container.ExclusionUntilEndOfSupport{
						Enabled: true,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newMaintenanceExclusion()
			gotNp := &container.NodePool{}
			f.ToNodepool(gotNp, tc.metadata)
			assert.Equal(t, tc.wantNp, gotNp)
		})
	}
}
