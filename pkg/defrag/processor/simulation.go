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

package processor

import (
	"slices"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// candidatePods keeps info about Candidate pods schedulability
type candidatePods struct {
	schedulableOnExisting []*apiv1.Pod
	schedulableOnUpcoming []*apiv1.Pod
	unschedulable         []*apiv1.Pod
}

// defragSimulator is simulating scheduling of defrag candidate pods
type defragSimulator struct {
	simulator *scheduling.HintingSimulator

	deleteOptions     options.NodeDeleteOptions
	drainabilityRules rules.Rules
	pdbTracker        pdb.RemainingPdbTracker

	clock clock.PassiveClock
}

type simulatorOptions struct {
	DeleteOptions     options.NodeDeleteOptions
	DrainabilityRules rules.Rules
}

// newSimulator returns a new instance of defragSimulator
func newSimulator(opts simulatorOptions) *defragSimulator {
	return &defragSimulator{
		simulator:         scheduling.NewHintingSimulator(),
		deleteOptions:     opts.DeleteOptions,
		drainabilityRules: opts.DrainabilityRules,
		pdbTracker:        pdb.NewBasicRemainingPdbTracker(),
		clock:             clock.RealClock{},
	}
}

// simulateNodeRemovals checks which candidateNodes can be removed by simulating
// pods scheduling on other nodes in the cluster excluding allCandidatesNodes.
// If node can be removed, the scheduling simulation will be saved in the cluster snapshot.
// Returns a list of nodes ready for removal and a list of nodes that can't
// be removed yet.
func (s *defragSimulator) simulateNodeRemovals(
	ctx *context.AutoscalingContext,
	candidateNodes []string,
	allCandidatesNodes map[string]bool,
) (nodesToScaleDown []string, unreadyNodes []string, err error) {
	snapshot := ctx.ClusterSnapshot
	rs := simulator.NewRemovalSimulator(ctx.ListerRegistry, snapshot, s.deleteOptions, s.drainabilityRules, true)
	destinationMap := make(map[string]bool)
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		return nil, nil, err
	}
	for _, nodeInfo := range nodeInfos {
		isUpcoming := nodeInfo.Node().Annotations[annotations.NodeUpcomingAnnotation] == "true"
		if isCandidateNode := allCandidatesNodes[nodeInfo.Node().Name]; !isCandidateNode && !isUpcoming {
			destinationMap[nodeInfo.Node().Name] = true
		}
	}
	for _, node := range candidateNodes {
		removableNode, unremovableNode := rs.SimulateNodeRemoval(node, destinationMap, s.clock.Now(), s.pdbTracker)
		if removableNode != nil {
			nodesToScaleDown = append(nodesToScaleDown, removableNode.Node.Name)
		} else if unremovableNode != nil {
			if unremovableNode.Reason == simulator.NoPlaceToMovePods {
				unreadyNodes = append(unreadyNodes, unremovableNode.Node.Name)
			} else {
				// this is an unexpected scenario, nodes with blocking pods should be
				// filtered out by the processor.
				klog.Warningf("node %s was unexpectedly unremovable, reason: %d", unremovableNode.Node.Name, unremovableNode.Reason)
			}
		}
	}
	return nodesToScaleDown, unreadyNodes, nil
}

// simulatePodsScheduling simulates scheduling of candidate pods and returns info on their scheduling
func (s *defragSimulator) simulatePodsScheduling(snapshot clustersnapshot.ClusterSnapshot, candidateNodes []string, allCandidatesNodes map[string]bool) (*candidatePods, error) {
	// Fork snapshot
	snapshot.Fork()

	// Get recreatable pods
	pods, err := recreatablePods(snapshot, candidateNodes)
	if err != nil {
		snapshot.Revert()
		return nil, err
	}

	// Filter pods schedulable on real nodes
	schedulableOnExisting, otherPods, err := s.schedulePods(snapshot, pods, allCandidatesNodes, false)
	if err != nil {
		snapshot.Revert()
		return nil, err
	}

	// Filter pods schedulable on upcoming nodes
	schedulableOnUpcoming, unschedulable, err := s.schedulePods(snapshot, otherPods, allCandidatesNodes, true)
	if err != nil {
		snapshot.Revert()
		return nil, err
	}

	err = snapshot.Commit()
	if err != nil {
		return nil, err
	}

	return &candidatePods{
		schedulableOnExisting: schedulableOnExisting,
		schedulableOnUpcoming: schedulableOnUpcoming,
		unschedulable:         unschedulable,
	}, nil
}

// schedulePods bin-packs pods on the non-candidate nodes, either real or upcoming ones
func (s *defragSimulator) schedulePods(snapshot clustersnapshot.ClusterSnapshot, pods []*apiv1.Pod, allCandidatesNodes map[string]bool, upcomingNodes bool) ([]*apiv1.Pod, []*apiv1.Pod, error) {
	isNodeAcceptable := func(nodeInfo *framework.NodeInfo) bool {
		_, isUpcoming := nodeInfo.Node().Annotations[annotations.NodeUpcomingAnnotation]
		return isUpcoming == upcomingNodes && !allCandidatesNodes[nodeInfo.Node().Name]
	}
	statuses, _, err := s.simulator.TrySchedulePods(snapshot, pods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: isNodeAcceptable,
	})
	if err != nil {
		return nil, nil, err
	}

	podsWithStatus := make(map[types.UID]bool)
	for _, status := range statuses {
		podsWithStatus[status.Pod.UID] = true
	}

	var schedulable, unschedulable []*apiv1.Pod
	for _, pod := range pods {
		if podsWithStatus[pod.UID] {
			schedulable = append(schedulable, pod)
		} else {
			unschedulable = append(unschedulable, pod)
		}
	}
	return schedulable, unschedulable, nil
}

// recreatablePods returns recreatable pods from given nodes
func recreatablePods(snapshot clustersnapshot.ClusterSnapshot, nodeNames []string) ([]*apiv1.Pod, error) {
	var pods []*apiv1.Pod
	for _, nodeName := range nodeNames {
		nodeInfo, err := snapshot.GetNodeInfo(nodeName)
		if err != nil {
			return nil, err
		}
		for _, podInfo := range nodeInfo.Pods() {
			pods = append(pods, podInfo.Pod)
		}
	}

	// Sort pods to maintain pod order across multiple iterations/runs.
	// Without this defrag can make different decisions wrt. expected node count between different iterations.
	slices.SortFunc(pods, func(podA, podB *apiv1.Pod) int {
		return podA.CreationTimestamp.Compare(podB.CreationTimestamp.Time)
	})

	return podutils.ClearPodNodeNames(podutils.FilterRecreatablePods(pods)), nil
}
