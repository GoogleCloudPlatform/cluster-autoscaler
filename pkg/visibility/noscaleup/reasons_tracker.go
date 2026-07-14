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

package noscaleup

import (
	"time"

	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// reasonsReportTracker can be used to track when particular no scale-up reasons were
// last reported.
type reasonsReportTracker struct {
	stalenessThreshold time.Duration
	topLevel           map[vistypes.MessageSignature]time.Time
	topLevelNap        map[vistypes.MessageSignature]time.Time
	skippedMigs        map[vistypes.MigExplanationSignature]time.Time
	migsInPodGroup     map[vistypes.MigExplanationPerPodGroupSignature]time.Time
	napInPodGroup      map[vistypes.MessagePerPodGroupSignature]time.Time
	podGroups          map[string]time.Time
}

// removeOld removes reasons that were reported longer than stalenessThreshold ago.
func (t *reasonsReportTracker) removeOld(now time.Time) {
	for reasonId, reportTime := range t.topLevel {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.topLevel, reasonId)
		}
	}
	for reasonId, reportTime := range t.topLevelNap {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.topLevelNap, reasonId)
		}
	}
	for signature, reportTime := range t.skippedMigs {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.skippedMigs, signature)
		}
	}
	for signature, reportTime := range t.migsInPodGroup {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.migsInPodGroup, signature)
		}
	}
	for signature, reportTime := range t.napInPodGroup {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.napInPodGroup, signature)
		}
	}
	for signature, reportTime := range t.podGroups {
		if reportTime.Add(t.stalenessThreshold).Before(now) {
			delete(t.podGroups, signature)
		}
	}
}

// markReported records the given reasons as reported at the given time.
func (t *reasonsReportTracker) markReported(reasons *Reasons, reportTime time.Time) {
	if reasons.TopLevel != nil {
		t.topLevel[reasons.TopLevel.Signature()] = reportTime
	}
	if reasons.TopLevelNap != nil {
		t.topLevelNap[reasons.TopLevelNap.Signature()] = reportTime
	}
	for _, migReason := range reasons.SkippedMigs {
		t.skippedMigs[migReason.Signature()] = reportTime
	}
	for _, podGroup := range reasons.PodGroups {
		t.podGroups[podGroup.SamplePod.ControllerOrPodUid()] = reportTime
		for _, migReason := range podGroup.MigReasons {
			t.migsInPodGroup[migReason.PerPodGroupSignature(podGroup.SamplePod.ControllerOrPodUid())] = reportTime
		}
		for _, napReason := range podGroup.NapReasons {
			t.napInPodGroup[napReason.PerPodGroupSignature(podGroup.SamplePod.ControllerOrPodUid())] = reportTime
		}
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

	var freshTopLevelNap *vistypes.Message
	if reasons.TopLevelNap != nil {
		if reportTime, found := t.topLevelNap[reasons.TopLevelNap.Signature()]; !found || reportTime.Add(t.stalenessThreshold).Before(now) {
			freshTopLevelNap = reasons.TopLevelNap
		}
	}

	var freshSkippedMigs []*vistypes.MigExplanation
	for _, migReason := range reasons.SkippedMigs {
		if reportTime, found := t.skippedMigs[migReason.Signature()]; !found || reportTime.Add(t.stalenessThreshold).Before(now) {
			freshSkippedMigs = append(freshSkippedMigs, migReason)
		}
	}

	var freshPodGroups []*vistypes.PodGroupExplanation
	for _, podGroup := range reasons.PodGroups {
		freshMigReasons := make(map[string]*vistypes.MigExplanation)
		for id, migExplanation := range podGroup.MigReasons {
			if reportTime, found := t.migsInPodGroup[migExplanation.PerPodGroupSignature(podGroup.SamplePod.ControllerOrPodUid())]; !found || reportTime.Add(t.stalenessThreshold).Before(now) {
				freshMigReasons[id] = migExplanation
			}
		}
		freshNapReasons := []*vistypes.Message{}
		for _, napReason := range podGroup.NapReasons {
			if reportTime, found := t.napInPodGroup[napReason.PerPodGroupSignature(podGroup.SamplePod.ControllerOrPodUid())]; !found || reportTime.Add(t.stalenessThreshold).Before(now) {
				freshNapReasons = append(freshNapReasons, napReason)
			}
		}
		// We emit given pod group if it wasn't reported before, it has some unreported mig reasons or unreported nap reasons.
		if reportTime, found := t.podGroups[podGroup.SamplePod.ControllerOrPodUid()]; len(freshMigReasons) > 0 || len(freshNapReasons) > 0 || !found || reportTime.Add(t.stalenessThreshold).Before(now) {
			podGroup.MigReasons = freshMigReasons

			podGroup.NapReasons = freshNapReasons

			freshPodGroups = append(freshPodGroups, podGroup)
		}
	}

	return &Reasons{
		TopLevel:    freshTopLevel,
		TopLevelNap: freshTopLevelNap,
		SkippedMigs: freshSkippedMigs,
		PodGroups:   freshPodGroups,
	}
}

func newReasonsReportTracker(stalenessThreshold time.Duration) *reasonsReportTracker {
	return &reasonsReportTracker{
		stalenessThreshold: stalenessThreshold,
		topLevel:           make(map[vistypes.MessageSignature]time.Time),
		topLevelNap:        make(map[vistypes.MessageSignature]time.Time),
		skippedMigs:        make(map[vistypes.MigExplanationSignature]time.Time),
		migsInPodGroup:     make(map[vistypes.MigExplanationPerPodGroupSignature]time.Time),
		napInPodGroup:      make(map[vistypes.MessagePerPodGroupSignature]time.Time),
		podGroups:          make(map[string]time.Time),
	}
}
