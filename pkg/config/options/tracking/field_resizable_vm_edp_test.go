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

package tracking

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestIsResizableVmEdpEnabled(t *testing.T) {
	testCases := []struct {
		testName     string
		clusterProto gkeclient.Cluster
		minVersion   string
		wantValue    bool
	}{
		{
			testName:     "Cluster version less than min version - Disabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "1.25.10-gke.1000"},
			minVersion:   "1.26.1-gke.0",
			wantValue:    false, // 1.25.x < 1.26.x
		},
		{
			testName:     "Cluster version equals min version - Enabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "1.26.5-gke.100"},
			minVersion:   "1.26.5-gke.100",
			wantValue:    true, // Equal versions: !v.LessThan(vMin) is true
		},
		{
			testName:     "Cluster version greater than min version - Enabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "1.27.1-gke.500"},
			minVersion:   "1.26.10-gke.0",
			wantValue:    true, // 1.27.x > 1.26.x
		},
		{
			testName:     "Patch version greater - Enabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "1.26.10-gke.10"},
			minVersion:   "1.26.10-gke.5",
			wantValue:    true, // 10 > 5
		},
		{
			testName:     "Different version formats (semantic vs GKE) - Enabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "1.27.0"},
			minVersion:   "1.26.0-gke.10",
			wantValue:    true, // 1.27.0 > 1.26.x
		},
		{
			testName:     "Invalid clusterVersion string - Disabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "invalid_version_string"},
			minVersion:   "1.26.1-gke.0",
			wantValue:    false, // Fails version.FromString(m.clusterVersion)
		},
		{
			testName:     "Invalid minVersion string - Disabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "1.27.1-gke.500"},
			minVersion:   "invalid_min_version",
			wantValue:    false, // Fails version.FromString(minGkeVersion)
		},
		{
			testName:     "Both invalid - Disabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "a.b.c"},
			minVersion:   "x.y.z",
			wantValue:    false, // Fails version.FromString(m.clusterVersion) first
		},
		{
			testName:     "Empty cluster version - Disabled",
			clusterProto: gkeclient.Cluster{},
			minVersion:   "1.26.1-gke.0",
			wantValue:    false,
		},
		{
			testName:     "Unset experiment uses failsafe fallback - Disabled",
			clusterProto: gkeclient.Cluster{ClusterVersion: "1.30.0-gke.0"},
			minVersion:   "", // Unset experiment: falls back to 999.999.999-gke.0
			wantValue:    false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			stringExperiments := map[string]string{}
			if tc.minVersion != "" {
				stringExperiments[experiments.EnableResizableVmEdpMinGKEVersionFlag] = tc.minVersion
			}
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, nil, stringExperiments)

			optsFromFlags := internalopts.AutoscalingOptions{}
			flagsToModify := internalopts.AutoscalingOptions{}
			err := resizableVmEdpEnabledField.setValueFromClusterProto(optsFromFlags, experimentsManager, tc.clusterProto, &flagsToModify)
			assert.NoError(t, err)
			gotValue := flagsToModify.ResizableVmEdpEnabled

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)
			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), resizableVmEdpEnabledField.getValueStr(flagsToModify))
		})
	}
}

func TestResizableVmEdpEnabledFieldValueEqual(t *testing.T) {
	for _, tc := range []struct {
		testName string

		optsA internalopts.AutoscalingOptions
		optsB internalopts.AutoscalingOptions

		wantEqual bool
	}{
		{
			testName:  "both_true",
			optsA:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{ResizableVmEdpEnabled: true}},
			optsB:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{ResizableVmEdpEnabled: true}},
			wantEqual: true,
		},
		{
			testName:  "both_false",
			optsA:     internalopts.AutoscalingOptions{},
			optsB:     internalopts.AutoscalingOptions{},
			wantEqual: true,
		},
		{
			testName:  "different_values",
			optsA:     internalopts.AutoscalingOptions{},
			optsB:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{ResizableVmEdpEnabled: true}},
			wantEqual: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			gotEqual := resizableVmEdpEnabledField.valueEqual(tc.optsA, tc.optsB)
			assert.Equal(t, tc.wantEqual, gotEqual)
		})
	}
}
