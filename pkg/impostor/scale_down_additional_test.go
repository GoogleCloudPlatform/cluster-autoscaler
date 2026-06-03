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

package impostor

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/testutils"
)

const (
	podsPerNode                = 30
	defaultPodsTerminationTime = 2 * time.Second
	nodeUtilization            = 20
	machineType                = "n1-standard-1"
	scaleDownUtilThreshold     = 0.5
	scaleDownUnneededTime      = 1 * time.Minute
)

func TestScaleDown(t *testing.T) {
	testutils.MarkTestManual(t)
	testTimeout := 6 * time.Hour

	parameters := []SdTestParameters{
		{
			nodesInNodeGroup:        10,
			numNodeGroups:           100,
			maxScaleDownParallelism: 50,
			maxDrainParallelism:     10,
		},
		{
			nodesInNodeGroup:        10,
			numNodeGroups:           100,
			maxScaleDownParallelism: 50,
			maxDrainParallelism:     1,
		},
	}
	for _, p := range parameters {
		t.Run("Basic Scale Down", func(t *testing.T) {
			err := basicScaleDown(&p, testTimeout)
			if err != nil {
				t.Errorf("basicScaleDown return err: %s", err)
			}
		})
		t.Run("Scale Down with range of termination time", func(t *testing.T) {
			err := scaleDownWithRangeOfTerminationTime(&p, 1*time.Minute, testTimeout)
			if err != nil {
				t.Errorf("scaleDownWithRangeOfTerminationTime return err: %s", err)
			}
		})
		t.Run("Scale Down with empty and non empty nodes", func(t *testing.T) {
			err := scaleDownWithEmptyAndNonEmptyNodes(&p, 2, testTimeout)
			if err != nil {
				t.Errorf("scaleDownWithEmptyAndNonEmptyNodes return err: %s", err)
			}
		})
		t.Run("Scale Down with pdb", func(t *testing.T) {
			err := scaleDownWithPdb(&p, 10, testTimeout)
			if err != nil {
				t.Errorf("scaleDownWithPdb return err: %s", err)
			}
		})
	}
}

func basicScaleDown(p *SdTestParameters, testTimeout time.Duration) error {
	s := SetUpDefaultSuite(p, scaleDownUnneededTime, scaleDownUtilThreshold, DefaultMaxPodsTerminationSec, "basicScaleDown")
	nodeGroupsName, labelsForNodeGroup := generateNodeGroupsAndLabels(p.numNodeGroups)
	for i, name := range nodeGroupsName {
		s.AddAndFillUpNodePool(name, machineType, p.nodesInNodeGroup, podsPerNode, nodeUtilization, nodeUtilization, "ReplicaSet", defaultPodsTerminationTime, labelsForNodeGroup[i])
	}
	idealNodeCount := (p.nodesInNodeGroup * p.numNodeGroups * nodeUtilization) / 100
	expectedNodeCount := idealNodeCount*2 - 1
	return s.ScaleDownUntilConditionMet(context.Background(), expectedNodeCount, testTimeout, 1*time.Second)
}

func scaleDownWithRangeOfTerminationTime(p *SdTestParameters, maxTerminationTime, testTimeout time.Duration) error {
	// TODO(b/517098458): use 10 minutes instead of DefaultMaxPodsTerminationSec.
	s := SetUpDefaultSuite(p, scaleDownUnneededTime, scaleDownUtilThreshold, DefaultMaxPodsTerminationSec, "scaleDownWithRangeOfTerminationTime")
	nodeGroupsName, labelsForNodeGroup := generateNodeGroupsAndLabels(p.numNodeGroups)
	for i, name := range nodeGroupsName {
		nodes := s.AddAndFillUpNodePool(name, machineType, p.nodesInNodeGroup, 10, 7, 7, "StatefulSet", defaultPodsTerminationTime, labelsForNodeGroup[i])
		s.Cluster.FillUpNodesPartially(nodes, 7, 7, 10, maxTerminationTime/2, "ReplicaSet", 2, "n1", nil)
		s.Cluster.FillUpNodesPartially(nodes, 6, 6, 10, maxTerminationTime, "ReplicaSet", 2, "n1", nil)
	}
	idealNodeCount := (p.nodesInNodeGroup * p.numNodeGroups * nodeUtilization) / 100
	expectedNodeCount := idealNodeCount*2 - 1
	return s.ScaleDownUntilConditionMet(context.Background(), expectedNodeCount, testTimeout, 3*time.Second)
}

func scaleDownWithEmptyAndNonEmptyNodes(p *SdTestParameters, numEmptyNodeGroups int, testTimeout time.Duration) error {
	s := SetUpDefaultSuite(p, scaleDownUnneededTime, scaleDownUtilThreshold, DefaultMaxPodsTerminationSec, "scaleDownWithEmptyAndNonEmptyNodes")
	nodeGroupsName, labelsForNodeGroup := generateNodeGroupsAndLabels(p.numNodeGroups)

	for i, name := range nodeGroupsName {
		if i < numEmptyNodeGroups {
			s.AddAndFillUpNodePool(name, machineType, p.nodesInNodeGroup, 1, 5, 5, "DaemonSet", 0*time.Second, labelsForNodeGroup[i])
		} else {
			s.AddAndFillUpNodePool(name, machineType, p.nodesInNodeGroup, podsPerNode, nodeUtilization, nodeUtilization, "StatefulSet", defaultPodsTerminationTime, labelsForNodeGroup[i])
		}
	}
	idealNodeCount := (p.nodesInNodeGroup * (p.numNodeGroups - numEmptyNodeGroups) * nodeUtilization) / 100
	expectedNodeCount := idealNodeCount*2 - 1
	return s.ScaleDownUntilConditionMet(context.Background(), expectedNodeCount, testTimeout, 3*time.Second)
}

func scaleDownWithPdb(p *SdTestParameters, maxUnavailable int, testTimeout time.Duration) error {
	s := SetUpDefaultSuite(p, scaleDownUnneededTime, scaleDownUtilThreshold, DefaultMaxPodsTerminationSec, "scaleDownWithPdb")
	nodeGroupsName, labelsForNodeGroup := generateNodeGroupsAndLabels(p.numNodeGroups)
	minUnavailable := 1
	unavailableStep := (maxUnavailable - minUnavailable) / p.numNodeGroups
	for i, name := range nodeGroupsName {
		s.AddAndFillUpNodePool(name, machineType, p.nodesInNodeGroup, 10, nodeUtilization, nodeUtilization, "ReplicaSet", defaultPodsTerminationTime, labelsForNodeGroup[i])
		maxUnavail := intstr.FromInt(minUnavailable + i*unavailableStep)
		minAvail := intstr.FromInt(p.numNodeGroups / 2)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvail,
				Selector: &metav1.LabelSelector{
					MatchLabels: labelsForNodeGroup[i],
				},
				MaxUnavailable: &maxUnavail,
			},
		}
		err := s.AddPDB(pdb)
		if err != nil {
			return fmt.Errorf("failed to add pdbs")
		}
	}
	idealNodeCount := (p.nodesInNodeGroup * p.numNodeGroups * nodeUtilization) / 100
	expectedNodeCount := idealNodeCount*2 - 1
	return s.ScaleDownUntilConditionMet(context.Background(), expectedNodeCount, testTimeout, 3*time.Second)
}

func generateNodeGroupsAndLabels(numNodeGroups int) ([]string, []map[string]string) {
	labelsForNodeGroup := []map[string]string{}
	nodeGroupsName := []string{}
	for i := 0; i < numNodeGroups; i++ {
		labels := make(map[string]string)
		name := "ng-" + strconv.Itoa(i)
		labels["ng-name"] = name
		nodeGroupsName = append(nodeGroupsName, name)
		labelsForNodeGroup = append(labelsForNodeGroup, labels)
	}
	return nodeGroupsName, labelsForNodeGroup
}
