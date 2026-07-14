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
	"testing"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/testutils"
)

// DefaultMaxPodsTerminationSec is a default max time for pods eviction.
const DefaultMaxPodsTerminationSec = 100

func TestBasicScaleDown(t *testing.T) {
	testutils.MarkTestLongRunning(t)

	scaleDownUnneededTime := 1 * time.Second
	scaleDownUtilThreshold := 0.5
	s := SetUpDefaultSuite(&SdTestParameters{maxScaleDownParallelism: 1, maxDrainParallelism: 1}, scaleDownUnneededTime, scaleDownUtilThreshold, DefaultMaxPodsTerminationSec, "")
	nodes := s.AddAndFillUpNodePool("default-pool", "n1-standard-1", 10, 20, 10, 10, "StatefulSet", 2*time.Second, nil)
	s.Cluster.FillUpNodesPartially(nodes, 10, 10, 20, 3*time.Second, "ReplicaSet", 2, "n1", nil)
	s.Cluster.AddDS(nodes, 2*time.Second, make(map[string]string), "default", 1, 100)
	expectedNodeCount := 5
	err := s.ScaleDownUntilConditionMet(context.Background(), expectedNodeCount, 20*time.Minute, 3*time.Second)
	if err != nil {
		t.Error(err)
	}
}

func TestScaleDownWithPDBBasic(t *testing.T) {
	testutils.MarkTestLongRunning(t)

	scaleDownUnneededTime := 1 * time.Second
	scaleDownUtilThreshold := 0.5
	labels := make(map[string]string)
	labels["app"] = "foo"
	s := SetUpDefaultSuite(&SdTestParameters{maxScaleDownParallelism: 1, maxDrainParallelism: 1}, scaleDownUnneededTime, scaleDownUtilThreshold, DefaultMaxPodsTerminationSec, "")
	s.AddAndFillUpNodePool("default-pool", "n1-standard-1", 2, 10, 10, 10, "StatefulSet", 2*time.Second, labels)
	maxUnavail := intstr.FromInt(1)
	minAvail := intstr.FromInt(20)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvail,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			MaxUnavailable: &maxUnavail,
		}}
	err := s.AddPDB(pdb)
	if err != nil {
		t.Fatal(err)
	}
	expectedNodeCount := 1
	err = s.ScaleDownUntilConditionMet(context.Background(), expectedNodeCount, 1*time.Minute, 3*time.Second)
	if err == nil {
		t.Error("Expected cluster not to scale down")
	}
}
