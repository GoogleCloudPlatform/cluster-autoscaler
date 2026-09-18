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

package history

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

const (
	ConditionTypeNodeProvisioningInCooldown        = "ProvisioningSuspended"
	ConditionTypeNodeProvisioningInPartialCooldown = "ProvisioningConstrained"

	// maxErrorDetailLength caps how much of the verbatim cloud provider error is copied into a
	// condition message. The apiserver allows up to 32KiB in Condition.Message, but these
	// messages are shown in `kubectl describe` and are re-sent on every status flush, so a much
	// shorter cap keeps the status readable and the patches small.
	maxErrorDetailLength = 512
)

type ruleBackoffKey struct {
	crdName     string
	ruleIdx     int
	backoffType string // "full" or "partial"
}

// CrdBackoffObserver observes backoff events and updates the status conditions
// of the corresponding NodeProvisioningConfig (CRD). It tracks both full backoffs
// (affecting the entire rule/priority) and partial backoffs (affecting specific
// node pools within a rule).
type CrdBackoffObserver struct {
	updatesCh chan<- npc_status.UpdateMessage
	lister    npc_lister.Lister
	matcher   computeclass.Matcher

	// backedOffRules maps ruleBackoffKey to backoffData.
	//
	// This map is not guarded by a mutex of its own. Async node pool creation reports its
	// failures from a background goroutine (asyncGkeManager.runNodePoolInitalizer), so OnBackoff
	// can be called off the main autoscaling loop while RemoveExpiredBackoffs iterates this map
	// from it. That is safe only because enabling async node groups also forces the synchronized
	// composite backoff (see isSynchronizedBackoffEnabled), which serializes Backoff() and
	// RemoveStaleBackoffData() under a single mutex. Any new caller of the BackoffObserver
	// methods must preserve that serialization.
	backedOffRules map[ruleBackoffKey]backoffData

	includeErrorDetails bool

	now func() time.Time
}

type backoffData struct {
	ruleIdx        int
	crdId          npc_status.CRDId
	expirationTime time.Time
	isFullCooldown bool
}

type backoffCloudProvider interface {
	IsAutopilotEnabled() bool
}

func NewCrdBackoffObserver(updatesCh chan<- npc_status.UpdateMessage, lister npc_lister.Lister, provider backoffCloudProvider, includeErrorDetails bool) *CrdBackoffObserver {
	return &CrdBackoffObserver{
		updatesCh:           updatesCh,
		lister:              lister,
		matcher:             computeclass.NewMatcher(lister, provider),
		backedOffRules:      make(map[ruleBackoffKey]backoffData),
		includeErrorDetails: includeErrorDetails,
		now:                 time.Now,
	}
}

// OnNpcBackoff is called when a full rule/priority level enters backoff.
// It sets a "NodeProvisioningInCooldown" condition on the CRD status for that rule.
func (i *CrdBackoffObserver) OnNpcBackoff(npcCrd crd.CRD, ruleIdx int, errorInfo cloudprovider.InstanceErrorInfo, until time.Time) {
	if i.updatesCh == nil || npcCrd == nil {
		return
	}

	translatedReason := translateErrorCode(errorInfo.ErrorCode)
	crdId := npc_status.CRDId{
		CRDName:  npcCrd.Name(),
		CRDLabel: npcCrd.Label(),
	}

	strIdx := fmt.Sprintf("%d", ruleIdx)

	ruleKey := ruleBackoffKey{crdName: crdId.CRDName, ruleIdx: ruleIdx, backoffType: "full"}
	if existing, ok := i.backedOffRules[ruleKey]; ok {
		if existing.expirationTime.After(until) {
			until = existing.expirationTime
		}
	}
	i.backedOffRules[ruleKey] = backoffData{
		ruleIdx:        ruleIdx,
		crdId:          crdId,
		expirationTime: until,
		isFullCooldown: true,
	}

	npc_status.TrySendRuleUpdate(i.updatesCh, npc_status.UpdateMessage{
		Id: crdId,
		Mutate: func(s crd.CRDStatus) {
			existingConditions := s.GetRuleConditions(strIdx)
			var deduplicated []metav1.Condition
			for _, existing := range existingConditions {
				if existing.Type != ConditionTypeNodeProvisioningInCooldown {
					deduplicated = append(deduplicated, existing)
				}
			}

			deduplicated = append(deduplicated, metav1.Condition{
				Type:               ConditionTypeNodeProvisioningInCooldown,
				Status:             metav1.ConditionTrue,
				Reason:             translatedReason,
				Message:            i.withErrorDetails(fmt.Sprintf("NodeProvisioning associated with this priority failed due to the %v error. Backing off the priority until %v.", translatedReason, until.Format("2006-01-02 15:04:05 MST")), errorInfo),
				LastTransitionTime: metav1.NewTime(i.now()),
			})

			s.UpdateRuleConditions(strIdx, deduplicated)
		},
	}, strIdx)
}

// ensure we implement the interfaces

var _ backoff.BackoffObserver = &CrdBackoffObserver{}

// OnBackoff is called when a specific node group (node pool) enters backoff.
// It identifies the associated CRD and rule index, and sets a "NodeProvisioningInPartialCooldown"
// condition on the CRD status for that rule, unless a full cooldown is already active.
func (i *CrdBackoffObserver) OnBackoff(nodeGroup cloudprovider.NodeGroup, errorInfo cloudprovider.InstanceErrorInfo, until time.Time) {
	if i.updatesCh == nil {
		return
	}

	ruleIdx, c, err := getRuleIndex(nodeGroup, i.lister, i.matcher)
	if err != nil || c == nil {
		return
	}

	translatedReason := translateErrorCode(errorInfo.ErrorCode)
	crdId := npc_status.CRDId{
		CRDName:  c.Name(),
		CRDLabel: c.Label(),
	}

	// Partial backoff
	ruleKey := ruleBackoffKey{crdName: crdId.CRDName, ruleIdx: ruleIdx, backoffType: "partial"}
	if existing, ok := i.backedOffRules[ruleKey]; ok {
		if existing.expirationTime.After(until) {
			until = existing.expirationTime
		}
	}

	i.backedOffRules[ruleKey] = backoffData{
		ruleIdx:        ruleIdx,
		crdId:          crdId,
		expirationTime: until,
		isFullCooldown: false,
	}

	ruleIdxStr := fmt.Sprintf("%d", ruleIdx)
	npc_status.TrySendRuleUpdate(i.updatesCh, npc_status.UpdateMessage{
		Id: crdId,
		Mutate: func(s crd.CRDStatus) {
			existingConditions := s.GetRuleConditions(ruleIdxStr)
			hasFullCooldown := false
			var deduplicated []metav1.Condition
			for _, existing := range existingConditions {
				if existing.Type == ConditionTypeNodeProvisioningInCooldown {
					hasFullCooldown = true
				}
				if existing.Type != ConditionTypeNodeProvisioningInPartialCooldown {
					deduplicated = append(deduplicated, existing)
				}
			}

			if !hasFullCooldown {
				deduplicated = append(deduplicated, metav1.Condition{
					Type:               ConditionTypeNodeProvisioningInPartialCooldown,
					Status:             metav1.ConditionTrue,
					Reason:             translatedReason,
					Message:            i.withErrorDetails(fmt.Sprintf("NodeProvisioning of the node pools associated with this priority failed due to the %v error. In backoff until %v.", translatedReason, until.Format("2006-01-02 15:04:05 MST")), errorInfo),
					LastTransitionTime: metav1.NewTime(i.now()),
				})
			}

			s.UpdateRuleConditions(ruleIdxStr, deduplicated)
		},
	}, ruleIdxStr)
}

// RemoveExpiredBackoffs checks all tracked backoffs and removes those that have expired.
// It also cleans up the corresponding conditions from the CRD status.
func (i *CrdBackoffObserver) RemoveExpiredBackoffs(currentTime time.Time) {
	if i.updatesCh == nil {
		return
	}

	for ruleKey, data := range i.backedOffRules {
		if !currentTime.Before(data.expirationTime) {
			delete(i.backedOffRules, ruleKey)

			ruleIdxStr := fmt.Sprintf("%d", data.ruleIdx)
			npc_status.TrySendRuleUpdate(i.updatesCh, npc_status.UpdateMessage{
				Id: data.crdId,
				Mutate: func(s crd.CRDStatus) {
					existingConditions := s.GetRuleConditions(ruleIdxStr)
					var deduplicated []metav1.Condition
					// CrdBackoffObserver only manages cooldown conditions it emitted (such as QuotaExceeded
					// or InternalError). Preserve any conditions with reason ConditionReasonFilteredOut, as those
					// are managed independently by Flex Advisor.
					for _, existing := range existingConditions {
						if data.isFullCooldown {
							if existing.Type != ConditionTypeNodeProvisioningInCooldown || existing.Reason == flexadvisor.ConditionReasonFilteredOut {
								deduplicated = append(deduplicated, existing)
							}
						} else {
							if existing.Type != ConditionTypeNodeProvisioningInPartialCooldown || existing.Reason == flexadvisor.ConditionReasonFilteredOut {
								deduplicated = append(deduplicated, existing)
							}
						}
					}
					s.UpdateRuleConditions(ruleIdxStr, deduplicated)
				},
			}, ruleIdxStr)
		}
	}
}

// withErrorDetails appends the verbatim cloud provider (GCE/GKE) error message to a condition
// message, if reporting error details is enabled for this cluster.
//
// translateErrorCode collapses every cloud provider error into one of a handful of reasons, which
// tells a user what kind of failure occurred but not why. The underlying message usually carries
// the actionable part (which quota, which reservation, which IP range), so it is appended verbatim
// rather than being parsed.
func (i *CrdBackoffObserver) withErrorDetails(message string, errorInfo cloudprovider.InstanceErrorInfo) string {
	if !i.includeErrorDetails {
		return message
	}
	detail := strings.TrimSpace(errorInfo.ErrorMessage)
	if detail == "" {
		return message
	}
	return message + " Cloud provider error: " + truncateErrorDetail(detail)
}

// truncateErrorDetail shortens detail to maxErrorDetailLength without splitting a multi-byte
// UTF-8 character. Invalid UTF-8 is replaced up front, as Condition.Message has to be valid UTF-8.
func truncateErrorDetail(detail string) string {
	detail = strings.ToValidUTF8(detail, string(utf8.RuneError))
	if len(detail) <= maxErrorDetailLength {
		return detail
	}
	truncated := detail[:maxErrorDetailLength]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated + "..."
}

// translateErrorCode maps cloud provider error codes to human-readable reasons
// used in CRD conditions.
func translateErrorCode(errorCode string) string {
	switch errorCode {
	case gce.ErrorCodeResourcePoolExhausted, "ZONE_RESOURCE_POOL_EXHAUSTED", "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS":
		return "OutOfResources"
	case gce.ErrorCodeQuotaExceeded:
		return "QuotaExceeded"
	case gce.ErrorIPSpaceExhausted:
		return "IpSpaceExhausted"
	case gce.ErrorReservationCapacityExceeded:
		return "ReservationCapacityExceeded"
	case gce.ErrorCodeVmExternalIpAccessPolicyConstraint:
		return "VmExternalIpAccessPolicyConstraint"
	case gce.ErrorInvalidReservation:
		return "InvalidReservation"
	default:
		return "InternalError"
	}
}
