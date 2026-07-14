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

	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// reasonsReportTracker can be used to track when particular no scale-down reasons were
// last reported.
type reasonsReportTracker struct {
	stalenessThreshold time.Duration
	topLevel           map[vistypes.MessageSignature]time.Time
	unremovableNodes   map[vistypes.NodeExplanationSignature]time.Time
}

// removeOld removes reasons that were reported longer than stalenessThreshold ago.
func (t *reasonsReportTracker) removeOld(now time.Time) {
	for reasonId, reportTime := range t.topLevel {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.topLevel, reasonId)
		}
	}
	for signature, reportTime := range t.unremovableNodes {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.unremovableNodes, signature)
		}
	}
}

// markReported records the given reasons as reported at the given time.
func (t *reasonsReportTracker) markReported(reasons *Reasons, reportTime time.Time) {
	if reasons.TopLevel != nil {
		t.topLevel[reasons.TopLevel.Signature()] = reportTime
	}
	for _, unremovableNode := range reasons.UnremovableNodes {
		t.unremovableNodes[unremovableNode.Signature()] = reportTime
	}
}

// filterOutAlreadyTrackedReasons returns a shallow copy of a Reasons structure, without any references to reasons
// that are present in the tracker.
func (t *reasonsReportTracker) filterOutAlreadyTrackedReasons(reasons *Reasons, now time.Time) *Reasons {
	var freshTopLevel *vistypes.Message
	if reasons.TopLevel != nil {
		if reportTime, found := t.topLevel[reasons.TopLevel.Signature()]; !found || reportTime.Add(t.stalenessThreshold).Before(now) {
			freshTopLevel = reasons.TopLevel
		}
	}

	var freshUnremovableNodes []*vistypes.NodeExplanation
	for _, unremovableNode := range reasons.UnremovableNodes {
		if reportTime, found := t.unremovableNodes[unremovableNode.Signature()]; !found || reportTime.Add(t.stalenessThreshold).Before(now) {
			freshUnremovableNodes = append(freshUnremovableNodes, unremovableNode)
		}
	}

	return &Reasons{
		TopLevel:         freshTopLevel,
		UnremovableNodes: freshUnremovableNodes,
	}
}

func newReasonsReportTracker(stalenessThreshold time.Duration) *reasonsReportTracker {
	return &reasonsReportTracker{
		stalenessThreshold: stalenessThreshold,
		topLevel:           make(map[vistypes.MessageSignature]time.Time),
		unremovableNodes:   make(map[vistypes.NodeExplanationSignature]time.Time),
	}
}
