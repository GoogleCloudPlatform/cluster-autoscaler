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

package integration

import (
	"context"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/predicate"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	cakubernetes "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
)

// RunScheduler runs a scheduling simulation using CA's internal simulator logic
// and updates the fake K8s API with node assignments.
//
// Assumptions/Limitations:
//   - It behaves like CA simulations by using HintingSimulator to check if pods fit on nodes.
//   - Instead of creating high-fidelity Binding resources (like a real K8s scheduler),
//     it directly bypasses the binding sequence and updates Spec.NodeName and PodScheduled
//     condition to True in the fake K8s client. This is a typical test shortcut.
//   - It assumes no concurrent schedulers are active.
//   - It currently does not support DRA.
func (f *FakeSet) RunScheduler(ctx context.Context, t *testing.T) {
	t.Helper()

	fwHandle := f.fwHandle

	snapshotStore := store.NewBasicSnapshotStore()
	snapshot := predicate.NewPredicateSnapshot(snapshotStore, fwHandle, true, 1, false)

	nodes, scheduledPods, unscheduledPods, err := f.collectClusterState(ctx)
	if err != nil {
		t.Fatalf("Failed to collect cluster state: %v", err)
	}

	if err := snapshot.SetClusterState(nodes, scheduledPods, nil, nil); err != nil {
		t.Fatalf("Failed to set cluster state: %v", err)
	}

	if len(unscheduledPods) == 0 {
		return
	}

	statuses, err := f.simulateScheduling(snapshot, unscheduledPods)
	if err != nil {
		t.Fatalf("Failed to simulate scheduling: %v", err)
	}

	if err := f.updatePodsInAPI(ctx, statuses); err != nil {
		t.Fatalf("Failed to update pods in API: %v", err)
	}
}

func (f *FakeSet) collectClusterState(ctx context.Context) ([]*apiv1.Node, []*apiv1.Pod, []*apiv1.Pod, error) {
	var nodes []*apiv1.Node
	nodeList := f.K8s.Nodes()
	for i := range nodeList.Items {
		nodes = append(nodes, &nodeList.Items[i])
	}

	podList, err := f.KubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, err
	}

	var scheduledPods []*apiv1.Pod
	var unscheduledPods []*apiv1.Pod

	for i := range podList.Items {
		pod := &podList.Items[i]

		if pod.Spec.NodeName == "" && pod.Status.Phase != apiv1.PodSucceeded && pod.Status.Phase != apiv1.PodFailed {
			unscheduledPods = append(unscheduledPods, pod)
		} else if pod.Spec.NodeName != "" {
			scheduledPods = append(scheduledPods, pod)
		}
	}

	return nodes, scheduledPods, unscheduledPods, nil
}

func (f *FakeSet) simulateScheduling(snapshot clustersnapshot.ClusterSnapshot, unscheduledPods []*apiv1.Pod) ([]scheduling.Status, error) {
	simulator := scheduling.NewHintingSimulator()
	opts := clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: func(node *framework.NodeInfo) bool {
			if node.Node() == nil {
				return false
			}
			return cakubernetes.IsNodeReadyAndSchedulable(node.Node())
		},
	}
	statuses, _, err := simulator.TrySchedulePods(snapshot, unscheduledPods, false, opts)
	return statuses, err
}

func (f *FakeSet) updatePodsInAPI(ctx context.Context, statuses []scheduling.Status) error {
	for _, status := range statuses {
		status.Pod.Spec.NodeName = status.NodeName

		var newConditions []apiv1.PodCondition
		for _, cond := range status.Pod.Status.Conditions {
			if cond.Type != apiv1.PodScheduled {
				newConditions = append(newConditions, cond)
			}
		}
		newConditions = append(newConditions, apiv1.PodCondition{
			Type:   apiv1.PodScheduled,
			Status: apiv1.ConditionTrue,
		})
		status.Pod.Status.Conditions = newConditions

		_, err := f.KubeClient.CoreV1().Pods(status.Pod.Namespace).Update(ctx, status.Pod, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		_, err = f.KubeClient.CoreV1().Pods(status.Pod.Namespace).UpdateStatus(ctx, status.Pod, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
