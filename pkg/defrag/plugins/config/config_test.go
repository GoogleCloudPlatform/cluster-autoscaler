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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestNodeMigration(t *testing.T) {
	testCases := []struct {
		name                         string
		nodeMigration                *NodeMigration
		wantEnabled                  bool
		wantNodesPerCycle            int
		wantSourceMachineFamily      string
		wantTargetMachineFamily      string
		wantForce                    bool
		wantNodeCreationDateLessThan string
	}{
		{
			name: "experiment disabled",
			nodeMigration: NewNodeMigration(
				/* Enabled */ true,
				/* NodesPerCycle */ 3,
				/* SourceMachineFamily */ "n2",
				/* TargetMachineFamily */ "n2d",
				/* Force */ false,
				/* NodeCreationDateLessThan */ "2024-07-11",
				experiments.NewMockManager(),
			),
			wantEnabled:                  true,
			wantNodesPerCycle:            3,
			wantSourceMachineFamily:      "n2",
			wantTargetMachineFamily:      "n2d",
			wantForce:                    false,
			wantNodeCreationDateLessThan: "2024-07-11",
		},
		{
			name: "experiment enabled",
			nodeMigration: NewNodeMigration(
				/* Enabled */ true,
				/* NodesPerCycle */ 3,
				/* SourceMachineFamily */ "n2",
				/* TargetMachineFamily */ "n2d",
				/* Force */ false,
				/* NodeCreationDateLessThan */ "2024-07-11",
				experiments.NewMockManager(experiments.EkForcefulMigrationFromEkToE2MinCAVersionFlag),
			),
			wantEnabled:                  true,
			wantNodesPerCycle:            experimentEnabledNodesPerCycle,
			wantSourceMachineFamily:      "ek",
			wantTargetMachineFamily:      "e2",
			wantForce:                    true,
			wantNodeCreationDateLessThan: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantEnabled, tc.nodeMigration.Enabled())
			assert.Equal(t, tc.wantNodesPerCycle, tc.nodeMigration.NodesPerCycle())
			assert.Equal(t, tc.wantSourceMachineFamily, tc.nodeMigration.SourceMachineFamily())
			assert.Equal(t, tc.wantTargetMachineFamily, tc.nodeMigration.TargetMachineFamily())
			assert.Equal(t, tc.wantForce, tc.nodeMigration.Force())
			assert.Equal(t, tc.wantNodeCreationDateLessThan, tc.nodeMigration.NodeCreationDateLessThan())
		})
	}
}
