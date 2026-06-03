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

package noscaledown

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// NoScaleDown describes an interface used to obtain information about the reasons behind a lack of scale-down.
type NoScaleDown interface {
	GetNewReasons(scaleDownStatus *vistypes.ScaleDownStatus, now time.Time) *Reasons
	MarkReasonsReported(reasons *Reasons, reportTime time.Time)
}

type throttledNoScaleDown struct {
	recentlyReportedReasons *reasonsReportTracker
}

// MarkReasonsReported is used to indicate that the specified reasons were reported to the user.
func (ns *throttledNoScaleDown) MarkReasonsReported(reasons *Reasons, reportTime time.Time) {
	ns.recentlyReportedReasons.markReported(reasons, reportTime)
}

// GetNewReasons returns reasons that should be reported to the user in the current autoscaling iteration.
func (ns *throttledNoScaleDown) GetNewReasons(scaleDownStatus *vistypes.ScaleDownStatus, now time.Time) *Reasons {
	// Garbage collect old entries.
	ns.recentlyReportedReasons.removeOld(now)

	unremovableReasons := ns.computeUnremovableNodes(scaleDownStatus)
	if len(unremovableReasons) == 0 {
		// If there are no meaningful (e.g. underutilized/scale-down disabled annotation etc.) unremovable nodes, don't provide any reasons, they won't make sense anyway.
		return &Reasons{}
	}

	allCurrentReasons := &Reasons{
		TopLevel:         ns.computeTopLevel(scaleDownStatus.Result),
		UnremovableNodes: unremovableReasons,
	}
	return ns.recentlyReportedReasons.filterOutAlreadyTrackedReasons(allCurrentReasons, now)
}

func (ns *throttledNoScaleDown) computeTopLevel(result status.ScaleDownResult) *vistypes.Message {
	if result == status.ScaleDownError {
		return vistypes.NewNoScaleDownUnexpectedErrorMsg()
	} else if result == status.ScaleDownInCooldown {
		return vistypes.NewNoScaleDownInBackoffMsg()
	} else if result == status.ScaleDownInProgress {
		return vistypes.NewNoScaleDownInProgressMsg()
	} else if result == status.ScaleDownNotTried {
		return vistypes.NewNoScaleDownNotTriedMsg()
	} else {
		return nil
	}
}

func (ns *throttledNoScaleDown) computeUnremovableNodes(scaleDownStatus *vistypes.ScaleDownStatus) []*vistypes.NodeExplanation {
	var result []*vistypes.NodeExplanation

	for _, unremovableNode := range scaleDownStatus.UnremovableNodes {
		reasonMsg := ns.computeNodeReasonMsg(unremovableNode)
		if reasonMsg == nil {
			continue
		}

		result = append(result, &vistypes.NodeExplanation{Node: unremovableNode.Node, Reason: reasonMsg})
	}

	return result
}

func (ns *throttledNoScaleDown) computeNodeReasonMsg(unremovableNode *vistypes.UnremovableNode) *vistypes.Message {
	if unremovableNode.Reason == simulator.ScaleDownDisabledAnnotation {
		return vistypes.NewNoScaleDownNodeScaleDownDisabledAnnotationMsg()
	} else if unremovableNode.Reason == simulator.NodeGroupMinSizeReached {
		return vistypes.NewNoScaleDownNodeNodeGroupMinSizeReachedMsg()
	} else if unremovableNode.Reason == simulator.MinimalResourceLimitExceeded {
		return vistypes.NewNoScaleDownNodeMinimalResourceLimitsExceededMsg()
	} else if unremovableNode.Reason == simulator.NoPlaceToMovePods {
		return vistypes.NewNoScaleDownNodeNoPlaceToMovePodsMsg()
	} else if unremovableNode.Reason == simulator.UnexpectedError {
		return vistypes.NewNoScaleDownNodeUnexpectedErrorMsg()
	} else if unremovableNode.Reason == simulator.BlockedByPod {
		return ns.computeNodeReasonMsgFromBlockingPod(unremovableNode.BlockingPod)
	} else {
		return nil
	}
}

func (ns *throttledNoScaleDown) computeNodeReasonMsgFromBlockingPod(blockingPod *vistypes.BlockingPod) *vistypes.Message {
	if blockingPod.Reason == drain.ControllerNotFound {
		return vistypes.NewNoScaleDownNodePodControllerNotFoundMsg(blockingPod.Pod.Name)
	} else if blockingPod.Reason == drain.MinReplicasReached {
		return vistypes.NewNoScaleDownNodePodMinReplicasReachedMsg(blockingPod.Pod.Name)
	} else if blockingPod.Reason == drain.NotReplicated {
		return vistypes.NewNoScaleDownNodePodNotBackedByControllerMsg(blockingPod.Pod.Name)
	} else if blockingPod.Reason == drain.LocalStorageRequested {
		return vistypes.NewNoScaleDownNodePodHasLocalStorageMsg(blockingPod.Pod.Name)
	} else if blockingPod.Reason == drain.NotSafeToEvictAnnotation {
		return vistypes.NewNoScaleDownNodePodNotSafeToEvictAnnotationMsg(blockingPod.Pod.Name)
	} else if blockingPod.Reason == drain.UnmovableKubeSystemPod {
		return vistypes.NewNoScaleDownNodePodKubeSystemUnmovableMsg(blockingPod.Pod.Name)
	} else if blockingPod.Reason == drain.NotEnoughPdb {
		return vistypes.NewNoScaleDownNodePodNotEnoughPdbMsg(blockingPod.Pod.Name)
	} else {
		return vistypes.NewNoScaleDownNodePodUnexpectedErrorMsg(blockingPod.Pod.Name)
	}
}

// NewNoScaleDown returns a new default instance implementing NoScaleDown.
func NewNoScaleDown(stalenessThreshold time.Duration) NoScaleDown {
	return &throttledNoScaleDown{recentlyReportedReasons: newReasonsReportTracker(stalenessThreshold)}
}
