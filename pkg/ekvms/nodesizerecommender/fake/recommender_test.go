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

package fake

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

func testNode(name, machineType, zone string) *corev1.Node {
	labels := map[string]string{
		corev1.LabelInstanceTypeStable: machineType,
	}
	if zone != "" {
		labels[corev1.LabelTopologyZone] = zone
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

func testNodeWithProviderID(name, machineType, providerID string) *corev1.Node {
	node := testNode(name, machineType, "")
	node.Spec.ProviderID = providerID
	return node
}

func TestRecommenderMaxSize(t *testing.T) {
	mcp := machinetypes.NewMachineConfigProvider(nil)
	size4vCPU := size.VmSize{MilliCpus: 4000, KBytes: 16 * size.GiBToKiB}
	size8vCPU := size.VmSize{MilliCpus: 8000, KBytes: 32 * size.GiBToKiB}
	size12vCPU := size.VmSize{MilliCpus: 12000, KBytes: 48 * size.GiBToKiB}
	size16vCPU := size.VmSize{MilliCpus: 16000, KBytes: 64 * size.GiBToKiB}
	fixedTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name              string
		node              *corev1.Node
		defaultMax        *size.VmSize
		zoneMax           map[string]size.VmSize
		nodeMax           *size.VmSize
		nonResizable      bool
		customRec         *nodesizerecommender.MaxSizeRecommendation
		clearNodeOverride bool
		wantSize          size.VmSize
		wantExactTime     time.Time
	}{
		{
			name:     "ek_physical_max_default",
			node:     testNode("node-ek", "ek-standard-8", "us-central1-a"),
			wantSize: size8vCPU,
		},
		{
			name:     "e4_physical_max_default",
			node:     testNode("node-e4", "e4-standard-16", "us-central1-a"),
			wantSize: size16vCPU,
		},
		{
			name:     "e4a_physical_max_default",
			node:     testNode("node-e4a", "e4a-standard-8", "us-central1-a"),
			wantSize: size8vCPU,
		},
		{
			name:       "default_max_size_overrides_physical_max",
			node:       testNode("node-default", "e4-standard-16", "us-central1-a"),
			defaultMax: &size12vCPU,
			wantSize:   size12vCPU,
		},
		{
			name:       "zone_max_size_overrides_default_max",
			node:       testNode("node-zone-a", "e4-standard-16", "us-central1-a"),
			defaultMax: &size12vCPU,
			zoneMax:    map[string]size.VmSize{"us-central1-a": size8vCPU},
			wantSize:   size8vCPU,
		},
		{
			name:     "unmatched_zone_falls_back_to_physical_max",
			node:     testNode("node-zone-b", "e4-standard-16", "us-central1-b"),
			zoneMax:  map[string]size.VmSize{"us-central1-a": size8vCPU},
			wantSize: size16vCPU,
		},
		{
			name:       "zone_max_size_resolved_from_provider_id",
			node:       testNodeWithProviderID("node-provider-zone", "e4-standard-16", "gce://test-project/us-central1-a/node-provider-zone"),
			defaultMax: &size12vCPU,
			zoneMax:    map[string]size.VmSize{"us-central1-a": size8vCPU},
			wantSize:   size8vCPU,
		},
		{
			name:     "missing_zone_and_provider_id_falls_back_to_physical_max",
			node:     testNode("node-no-zone", "e4-standard-16", ""),
			zoneMax:  map[string]size.VmSize{"us-central1-a": size8vCPU},
			wantSize: size16vCPU,
		},
		{
			name:       "invalid_provider_id_without_zone_falls_back_to_default_max",
			node:       testNodeWithProviderID("node-bad-provider", "e4-standard-16", "invalid-provider-id"),
			defaultMax: &size12vCPU,
			zoneMax:    map[string]size.VmSize{"us-central1-a": size8vCPU},
			wantSize:   size12vCPU,
		},
		{
			name:     "node_override_takes_precedence_over_zone",
			node:     testNode("node-cap", "e4-standard-16", "us-central1-a"),
			zoneMax:  map[string]size.VmSize{"us-central1-a": size8vCPU},
			nodeMax:  &size4vCPU,
			wantSize: size4vCPU,
		},
		{
			name:         "non_resizable_spillover_returns_zero_size",
			node:         testNode("node-spillover", "e4-standard-16", "us-central1-a"),
			nonResizable: true,
			wantSize:     size.VmSize{},
		},
		{
			name: "explicit_recommendation_preserves_creation_time",
			node: testNode("node-custom", "e4-standard-16", "us-central1-a"),
			customRec: &nodesizerecommender.MaxSizeRecommendation{
				VmSize:       size4vCPU,
				CreationTime: fixedTime,
			},
			wantSize:      size4vCPU,
			wantExactTime: fixedTime,
		},
		{
			name:              "cleared_node_override_falls_back_to_physical_max",
			node:              testNode("node-cleared", "e4-standard-16", "us-central1-a"),
			nodeMax:           &size4vCPU,
			clearNodeOverride: true,
			wantSize:          size16vCPU,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := NewRecommender(mcp)
			if tc.defaultMax != nil {
				rec.SetDefaultMaxSize(*tc.defaultMax)
			}
			for z, zMax := range tc.zoneMax {
				rec.SetZoneMaxSize(z, zMax)
			}
			if tc.nodeMax != nil {
				rec.SetNodeMaxSize(tc.node.Name, *tc.nodeMax)
			}
			if tc.nonResizable {
				rec.SetNodeNonResizable(tc.node.Name)
			}
			if tc.customRec != nil {
				rec.SetNodeRecommendation(tc.node.Name, tc.customRec)
			}
			if tc.clearNodeOverride {
				rec.ClearNodeOverride(tc.node.Name)
			}

			got := rec.MaxSize(tc.node)
			if got == nil {
				t.Fatalf("MaxSize(%q) = nil, want VmSize %+v", tc.node.Name, tc.wantSize)
			}
			if diff := cmp.Diff(tc.wantSize, got.VmSize); diff != "" {
				t.Errorf("MaxSize(%q).VmSize diff (-want +got):\n%s", tc.node.Name, diff)
			}
			if !tc.wantExactTime.IsZero() && !got.CreationTime.Equal(tc.wantExactTime) {
				t.Errorf("MaxSize(%q).CreationTime = %v, want %v", tc.node.Name, got.CreationTime, tc.wantExactTime)
			} else if got.CreationTime.IsZero() {
				t.Errorf("MaxSize(%q).CreationTime = zero, want non-zero timestamp", tc.node.Name)
			}
			if !rec.HasSeenNode(tc.node.Name) {
				t.Errorf("HasSeenNode(%q) = false, want true", tc.node.Name)
			}
		})
	}
}

func TestRecommenderMaxSize_NilCases(t *testing.T) {
	mcp := machinetypes.NewMachineConfigProvider(nil)
	tests := []struct {
		name        string
		node        *corev1.Node
		explicitNil bool
	}{
		{
			name: "nil_node",
		},
		{
			name: "classic_non_resizable_machine_type",
			node: testNode("e2-node", "e2-standard-4", "us-central1-a"),
		},
		{
			name: "missing_instance_type_label",
			node: testNode("unlabeled-node", "", "us-central1-a"),
		},
		{
			name:        "explicit_nil_node_recommendation",
			node:        testNode("nil-override-node", "e4-standard-16", "us-central1-a"),
			explicitNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := NewRecommender(mcp)
			if tc.explicitNil && tc.node != nil {
				rec.SetNodeRecommendation(tc.node.Name, nil)
			}
			if got := rec.MaxSize(tc.node); got != nil {
				t.Errorf("MaxSize(%v) = %+v, want nil", tc.node, got)
			}
		})
	}
}

func TestRecommenderSetLocations(t *testing.T) {
	tests := []struct {
		name      string
		locations []string
	}{
		{
			name:      "single_zone",
			locations: []string{"us-central1-a"},
		},
		{
			name:      "multiple_zones",
			locations: []string{"us-central1-a", "us-central1-b"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := NewRecommender(nil)
			input := append([]string(nil), tc.locations...)
			rec.SetLocations(input)
			input[0] = "mutated"

			got := rec.Locations()
			if diff := cmp.Diff(tc.locations, got); diff != "" {
				t.Errorf("Locations() diff (-want +got):\n%s", diff)
			}
		})
	}
}
