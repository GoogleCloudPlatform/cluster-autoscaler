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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
)

const (
	ConditionTypeNodeProvisioningInCooldown        = "NodeProvisioningInCooldown"
	ConditionTypeNodeProvisioningInPartialCooldown = "NodeProvisioningInPartialCooldown"
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
	backedOffRules map[ruleBackoffKey]backoffData

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

func NewCrdBackoffObserver(updatesCh chan<- npc_status.UpdateMessage, lister npc_lister.Lister, provider backoffCloudProvider) *CrdBackoffObserver {
	return &CrdBackoffObserver{
		updatesCh:      updatesCh,
		lister:         lister,
		matcher:        computeclass.NewMatcher(lister, provider),
		backedOffRules: make(map[ruleBackoffKey]backoffData),
		now:            time.Now,
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

	i.updatesCh <- npc_status.UpdateMessage{
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
				Message:            fmt.Sprintf("NodeProvisioning associated with this priority failed due to the %v error. Backing off the priority until %v.", translatedReason, until.Format("2006-01-02 15:04:05 MST")),
				LastTransitionTime: metav1.NewTime(i.now()),
			})

			s.UpdateRuleConditions(strIdx, deduplicated)
		},
	}
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

	i.updatesCh <- npc_status.UpdateMessage{
		Id: crdId,
		Mutate: func(s crd.CRDStatus) {
			existingConditions := s.GetRuleConditions(fmt.Sprintf("%d", ruleIdx))
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
					Message:            fmt.Sprintf("NodeProvisioning of the node pools associated with this priority failed due to the %v error. In backoff until %v.", translatedReason, until.Format("2006-01-02 15:04:05 MST")),
					LastTransitionTime: metav1.NewTime(i.now()),
				})
			}

			s.UpdateRuleConditions(fmt.Sprintf("%d", ruleIdx), deduplicated)
		},
	}
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

			i.updatesCh <- npc_status.UpdateMessage{
				Id: data.crdId,
				Mutate: func(s crd.CRDStatus) {
					existingConditions := s.GetRuleConditions(fmt.Sprintf("%d", data.ruleIdx))
					var deduplicated []metav1.Condition
					for _, existing := range existingConditions {
						if data.isFullCooldown {
							if existing.Type != ConditionTypeNodeProvisioningInCooldown {
								deduplicated = append(deduplicated, existing)
							}
						} else {
							if existing.Type != ConditionTypeNodeProvisioningInPartialCooldown {
								deduplicated = append(deduplicated, existing)
							}
						}
					}
					s.UpdateRuleConditions(fmt.Sprintf("%d", data.ruleIdx), deduplicated)
				},
			}
		}
	}
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
