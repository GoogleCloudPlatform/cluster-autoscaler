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

package types

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

// MigExplanationPerPodGroupSignature uniquely identifies a MigExplanation within a PodGroup.
type MigExplanationPerPodGroupSignature string

// MigExplanationSignature uniquely identifies a MigExplanation structure.
type MigExplanationSignature string

// MigExplanation contains a reason why a particular MIG blocked a scale-up.
type MigExplanation struct {
	Mig    *GkeMig
	Reason *Message
}

// PodGroupExplanationSignature uniquely identifies a pod group explanation.
type PodGroupExplanationSignature struct {
	ControllerOrPodUid  string
	MigReasonsSignature string
	NapReasonsSignature string
}

// PodGroupExplanation represents a group of pods sharing the same controller, with reasons explaining decisions made for it.
type PodGroupExplanation struct {
	SamplePod *Pod
	PodCount  int
	// MigReasons contains reasons why particular MIGd couldn't be scaled up to accommodate pods from this group.
	MigReasons map[string]*MigExplanation
	// NapReasons contains reasons why NAP couldn't provisions new node pools to accommodate pods from this group.
	NapReasons []*Message
}

// DisregardedMigInfo contains a NAP disregarded node group key and a reason it was disregarded.
type DisregardedMigInfo struct {
	Key    autoprovisioning.NodeGroupOptions
	Reason autoprovisioning.NodeGroupDisregardedReason
}

// NapStatus is a visibility-specific version of NAP processing status.
type NapStatus struct {
	Result          autoprovisioning.ProcessingResult
	DisregardedMigs map[autoprovisioning.NodeGroupOptions]autoprovisioning.NodeGroupDisregardedReason
	PodStatuses     map[string]autoprovisioning.PodProcessingStatus
}

// Proto converts the mig explanation to its proto representation.
func (me MigExplanation) Proto() *vispb.MigExplanation {
	return &vispb.MigExplanation{
		Mig:    me.Mig.Proto(),
		Reason: me.Reason.Proto(),
	}
}

// Signature returns the migReasons's Signature.
func (me MigExplanation) Signature() MigExplanationSignature {
	return MigExplanationSignature(me.Mig.Id + "," + string(me.Reason.Signature()))
}

// PerPodGroupSignature computes a unique identifier of MigReason within a given PodGroup.
func (me MigExplanation) PerPodGroupSignature(pgUID string) MigExplanationPerPodGroupSignature {
	return MigExplanationPerPodGroupSignature(fmt.Sprintf("%v/%v", pgUID, me.Signature()))
}

// Proto converts the pod group explanation to its proto representation.
func (pg *PodGroupExplanation) Proto() *vispb.PodGroupExplanation {
	explanation := &vispb.PodGroupExplanation{
		PodGroup: &vispb.PodGroup{
			SamplePod:     pg.SamplePod.Proto(),
			TotalPodCount: int32(pg.PodCount),
		},
	}

	if len(pg.NapReasons) > 0 {
		explanation.NapFailureReasons = make([]*vispb.ParametrizedMessage, 0, len(pg.NapReasons))
		for _, napReason := range pg.NapReasons {
			explanation.NapFailureReasons = append(explanation.NapFailureReasons, napReason.Proto())
		}
	}

	if len(pg.MigReasons) > 0 {
		explanation.RejectedMigs = make([]*vispb.MigExplanation, 0, len(pg.MigReasons))
		for _, migReason := range pg.MigReasons {
			explanation.RejectedMigs = append(explanation.RejectedMigs, migReason.Proto())
		}
	}

	return explanation
}

// Signature computes a pod group explanation's signature.
func (pg *PodGroupExplanation) Signature() PodGroupExplanationSignature {
	// We want the order of mig explanations in the Signature to be deterministic, so we have to sort the keys.
	// There shouldn't be more than ~400 MIGs so this shouldn't affect performance.
	sortedMigNames := make([]string, 0, len(pg.MigReasons))
	for migName := range pg.MigReasons {
		sortedMigNames = append(sortedMigNames, migName)
	}
	sort.Strings(sortedMigNames)

	var migReasonsSignature strings.Builder
	for _, migName := range sortedMigNames {
		reason := pg.MigReasons[migName]
		migReasonsSignature.WriteString(string(reason.Signature()))
		migReasonsSignature.WriteRune('\n')
	}

	var napReasonsSignature strings.Builder
	for _, reason := range pg.NapReasons {
		napReasonsSignature.WriteString(string(reason.Signature()))
		napReasonsSignature.WriteRune('\n')
	}

	return PodGroupExplanationSignature{
		ControllerOrPodUid:  pg.SamplePod.ControllerOrPodUid(),
		MigReasonsSignature: migReasonsSignature.String(),
		NapReasonsSignature: napReasonsSignature.String(),
	}
}

// ConvertNapStatus converts a NAP processing status to its visibility-specific version.
func ConvertNapStatus(originalStatus *autoprovisioning.ProcessingStatus) (*NapStatus, error) {
	disregardedMigs := make(map[autoprovisioning.NodeGroupOptions]autoprovisioning.NodeGroupDisregardedReason)
	for key, reason := range originalStatus.DisregardedNodeGroups {
		disregardedMigs[key] = reason
	}

	podStatuses := make(map[string]autoprovisioning.PodProcessingStatus)
	for podUid, podStatus := range originalStatus.PodStatuses {
		podStatuses[string(podUid)] = podStatus
	}

	return &NapStatus{
		Result:          originalStatus.Result,
		DisregardedMigs: disregardedMigs,
		PodStatuses:     podStatuses,
	}, nil
}

// DefaultNapStatus returns a default version of NAP status (processing successful, nothing added, nothing disregarded).
func DefaultNapStatus() *NapStatus {
	return &NapStatus{
		Result:          autoprovisioning.ProcessingOk,
		DisregardedMigs: nil,
		PodStatuses:     nil,
	}
}
