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

package ccc

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
)

var (
	highCpuCrd    = crd.NewTestCrd(crd.WithName("high-cpu"), crd.WithLabel(gkelabels.ComputeClassLabel))
	defaultCccCrd = crd.NewTestCrd(crd.WithName("default-ccc"), crd.WithLabel(gkelabels.ComputeClassLabel))
	provider      = lister.NewMockCrdListerWithLabel([]crd.CRD{highCpuCrd, defaultCccCrd}, gkelabels.ComputeClassLabel)
	p             = NewNodeGroupChangePerCCCMetricsProducer(provider)
)

func TestGetCrdNameForNodeGroup(t *testing.T) {
	provider.SetDefaultCrdName("default-ccc")
	testCases := []struct {
		name         string
		computeClass string
		want         string
	}{
		{
			name:         "NodeGroup with ComputeClass in Spec",
			computeClass: "high-cpu",
			want:         "high-cpu",
		},
		{
			name:         "NodeGroup with empty Spec (falls back to default)",
			computeClass: "",
			want:         "default-ccc",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spec := gke.NewTestMigSpecBuilder()
			if tc.computeClass != "" {
				spec.SetLabels(map[string]string{gkelabels.ComputeClassLabel: tc.computeClass})
			}
			ng := gke.NewTestGkeMigBuilder().
				SetSpec(spec.SpecBuild()).
				Build()
			assert.Equal(t, tc.want, p.getCrdNameForNodeGroup(ng))
		})
	}
}

func TestRegisterScaleUp(t *testing.T) {
	scaleUpAttempts.Reset()
	provider.SetDefaultCrdName("default-ccc")
	now := time.Now()

	ng1 := gke.NewTestGkeMigBuilder().
		SetSpec(gke.NewTestMigSpecBuilder().SetLabels(map[string]string{gkelabels.ComputeClassLabel: "high-cpu"}).SpecBuild()).
		Build()
	p.RegisterScaleUp(ng1, 3, now)

	ng2 := gke.NewTestGkeMigBuilder().Build()
	p.RegisterScaleUp(ng2, 2, now)

	want := `# HELP cluster_autoscaler_node_provisioning_attempts_count_per_ccc [ALPHA] Number of node provisioning attempts per CCC.
# TYPE cluster_autoscaler_node_provisioning_attempts_count_per_ccc counter
cluster_autoscaler_node_provisioning_attempts_count_per_ccc{entity_name="default-ccc",entity_type="ccc"} 2
cluster_autoscaler_node_provisioning_attempts_count_per_ccc{entity_name="high-cpu",entity_type="ccc"} 3
`

	if err := testutil.CollectAndCompare(scaleUpAttempts, strings.NewReader(want), "cluster_autoscaler_node_provisioning_attempts_count_per_ccc"); err != nil {
		t.Errorf("unexpected collecting result: %v", err)
	}
}
