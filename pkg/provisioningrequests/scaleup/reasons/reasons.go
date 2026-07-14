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

package reasons

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	klog "k8s.io/klog/v2"
)

const (
	// Reasons and prefixes of messages passed to the Provisioning Requests failed state.
	provisioningRequestSingleZoneInfeasible       = "ProvisioningRequestSingleZoneInfeasible"
	provisioningRequestSingleZoneInfeasiblePrefix = "Provisioning Request cannot be scheduled in a single zone of a single nodepool"

	podsCantBeScheduledInNodepool       = "ProvisioningRequestNotSchedulableInNodepool"
	podsCantBeScheduledInNodepoolPrefix = "Provisioning Request's pods cannot be scheduled in the nodepool"

	clusterNodeLimitExceeded       = "ClusterNodeLimitExceeded"
	clusterNodeLimitExceededPrefix = "Max cluster size reached"

	nodepoolSizeReached       = "NodepoolSizeReached"
	nodepoolSizeReachedPrefix = "Max nodepool size reached"

	outOfResources       = "OutOfResources"
	outOfResourcesPrefix = "Max cluster limit reached"

	nodepoolInBackoff       = "NodepoolInBackoff"
	nodepoolInBackoffPrefix = "Nodepool in backoff after failed scale-up"

	nodepoolsNotReady       = "NodepoolNotReady"
	nodepoolsNotReadyPrefix = "Nodepool not ready for scale-up"

	noQueuedNodepoolAvailable        = "NoQueuedNodepoolAvailable"
	noQueuedNodepoolAvailableMessage = "No nodepool with QueuedProvisioning enabled is available for scale up"

	couldNotParallelizeScaleup = "CannotExecuteObtainabilityStrategy"
	couldNotParallelizePrefix  = "Could not execute OBTAINABILITY capacitySearchStrategy"

	maxNodepoolsInMessage               = 10
	maxPredicateErrorNodepoolsInMessage = 5
	unrecognizedSkippedReason           = "InternalErrorSkippedNodepool"
	unrecognizedSkippedMessagePrefix    = "Unrecognized reasons for skipping nodepools, i.e."
)

var (
	// PodCountNotFoundReason - ProvReq or PodTemplates not found in cache; generally unexpected.
	PodCountNotFoundReason = orchestrator.NewRejectedReasons("internal error, unable to determine Provisioning Request's pod count")
	// NotQueuedNodeGroupSkippedReason - no queued nodepool is available for scale up.
	NotQueuedNodeGroupSkippedReason = orchestrator.NewSkippedReasons("node group is not handling queued nodes")
	// ClusterSizeReachedSkippedReason - max number of nodes is reached in cluster.
	ClusterSizeReachedSkippedReason = orchestrator.NewSkippedReasons("max cluster size reached")
	// CouldNotScheduleAllPodsInSingleZone - while estimating the scale up some of the pods could not be scheduled in the single zone of the single nodepool.
	CouldNotScheduleAllPodsInSingleZone = orchestrator.NewSkippedReasons("Provisioning Request could not be scheduled in a single zone of a single nodepool")

	reasonsPriorities = []reasonsMapEntry{
		createCouldNotParallelizeScaleupEntry(couldNotParallelizePrefix, couldNotParallelizePrefix),
		createSimpleEntry(CouldNotScheduleAllPodsInSingleZone, provisioningRequestSingleZoneInfeasible, provisioningRequestSingleZoneInfeasiblePrefix),
		createSimpleEntry(ClusterSizeReachedSkippedReason, clusterNodeLimitExceeded, clusterNodeLimitExceededPrefix),
		createSimpleEntry(orchestrator.MaxLimitReachedReason, nodepoolSizeReached, nodepoolSizeReachedPrefix),
		createResourceLimitEntry(outOfResources, outOfResourcesPrefix),
		createSimpleEntry(orchestrator.BackoffReason, nodepoolInBackoff, nodepoolInBackoffPrefix),
		createSimpleEntry(orchestrator.NotReadyReason, nodepoolsNotReady, nodepoolsNotReadyPrefix),
		createCouldNotScheduleAnyPodsInNodePoolEntry(podsCantBeScheduledInNodepool, podsCantBeScheduledInNodepoolPrefix),
	}
)

type reasonsMapEntry interface {
	// findMatch goes through the map of ignored node groups and looks for a match.
	// If a match is found it should return true, reason and message to be passed
	// to the Provisioning Request state.
	findMatch(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) (bool, string, string)
}

// GetReasonAndMessage - returns the reason and message to be passed to
// the Provisioning Request's Failed condition.
func GetReasonAndMessage(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) (reason, message string) {
	for _, entry := range reasonsPriorities {
		if found, reason, message := entry.findMatch(skippedNodeGroups); found {
			return reason, message
		}
	}

	if found, reason, message := getUnrecognizedReasonMessage(skippedNodeGroups); found {
		return reason, message
	}
	return noQueuedNodepoolAvailable, noQueuedNodepoolAvailableMessage
}

func getUnrecognizedReasonMessage(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) (bool, string, string) {
	unrecognizedReasonsSkippedNodeGroups := map[string][]string{}
	for ng, reason := range skippedNodeGroups {
		if !IsNodeGroupQueued(ng) {
			continue
		}

		reasons := strings.Join(reason.Reasons(), ", ")
		if unrecognizedReasonsSkippedNodeGroups[reasons] == nil {
			unrecognizedReasonsSkippedNodeGroups[reasons] = []string{}
		}
		unrecognizedReasonsSkippedNodeGroups[reasons] = append(unrecognizedReasonsSkippedNodeGroups[reasons], nodepoolName(ng))
	}

	if len(unrecognizedReasonsSkippedNodeGroups) > 0 {
		return true, unrecognizedSkippedReason, getUnrecognizedSkipMessage(unrecognizedReasonsSkippedNodeGroups)
	}
	return false, "", ""
}

func getUnrecognizedSkipMessage(reasonsNodeGroups map[string][]string) string {
	reasons := make([]string, 0, len(reasonsNodeGroups))
	for reason := range reasonsNodeGroups {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)

	nodepoolsInMessage := 0
	messages := []string{}
	for _, reason := range reasons {
		nodepoolsLeft := maxNodepoolsInMessage - nodepoolsInMessage
		if nodepoolsLeft <= 0 {
			break
		}

		ngs := reasonsNodeGroups[reason]
		sort.Strings(ngs)
		if len(ngs) > nodepoolsLeft {
			ngs = append(ngs[:nodepoolsLeft], "...")
		}
		messages = append(messages, fmt.Sprintf("%q, affected nodepools: %s", reason, strings.Join(ngs, ", ")))
		nodepoolsInMessage += len(ngs)
	}

	if len(reasons) > maxNodepoolsInMessage {
		messages = append(messages, "...")
	}

	return fmt.Sprintf("%s %s", unrecognizedSkippedMessagePrefix, strings.Join(messages, "; "))
}

// IsNodeGroupQueued returns true if node group is able to handle Provisioning Requests.
func IsNodeGroupQueued(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		klog.Errorf("Could not cast node group %q to GkeMig structure!", nodeGroup.Id())
		return false
	}
	return mig.QueuedProvisioning()
}

// TranslateKeysToNames - returns map containing the same values, but uses Id as a key.
// This is used to translate to OSS-compatible object.
func TranslateKeysToNames(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) map[string]status.Reasons {
	result := make(map[string]status.Reasons, len(skippedNodeGroups))
	for ng, r := range skippedNodeGroups {
		result[ng.Id()] = r
	}
	return result
}

type simpleEntry struct {
	skippedReasons       *orchestrator.SkippedReasons
	provreqResponse      string
	provreqMessagePrefix string
}

func createSimpleEntry(skippedReasons *orchestrator.SkippedReasons, provreqResponse, provreqMessagePrefix string) reasonsMapEntry {
	return &simpleEntry{
		skippedReasons:       skippedReasons,
		provreqResponse:      provreqResponse,
		provreqMessagePrefix: provreqMessagePrefix,
	}
}

func (s *simpleEntry) findMatch(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) (bool, string, string) {
	nodepoolsMap := map[string]struct{}{}
	for nodeGroup, reasons := range skippedNodeGroups {
		if reasons == s.skippedReasons {
			nodepoolsMap[nodepoolName(nodeGroup)] = struct{}{}
		}
	}
	if len(nodepoolsMap) == 0 {
		return false, "", ""
	}

	nodepools := make([]string, 0, len(nodepoolsMap))
	for k := range nodepoolsMap {
		nodepools = append(nodepools, k)
	}
	sort.Strings(nodepools)

	// Cap the number of nodepools in one message.
	if len(nodepools) > maxNodepoolsInMessage {
		nodepools = nodepools[:maxNodepoolsInMessage]
		nodepools = append(nodepools, "...")
	}

	message := fmt.Sprintf("%s, affected nodepools: %s", s.provreqMessagePrefix, strings.Join(nodepools, ", "))
	return true, s.provreqResponse, message
}

type resourceLimitEntry struct {
	provreqResponse      string
	provreqMessagePrefix string
}

func createResourceLimitEntry(provreqResponse, provreqMessagePrefix string) reasonsMapEntry {
	return &resourceLimitEntry{
		provreqResponse:      provreqResponse,
		provreqMessagePrefix: provreqMessagePrefix,
	}
}

func (s *resourceLimitEntry) findMatch(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) (bool, string, string) {
	missingResourcesMap := map[string]map[string]struct{}{}
	for nodeGroup, reasons := range skippedNodeGroups {
		mrlr, ok := reasons.(*orchestrator.MaxResourceLimitReached)
		if !ok {
			continue
		}

		if missingResourcesMap[nodepoolName(nodeGroup)] == nil {
			missingResourcesMap[nodepoolName(nodeGroup)] = map[string]struct{}{}
		}
		for _, resource := range mrlr.Resources() {
			missingResourcesMap[nodepoolName(nodeGroup)][resource] = struct{}{}
		}
	}
	if len(missingResourcesMap) == 0 {
		return false, "", ""
	}

	nodepools := make([]string, 0, len(missingResourcesMap))
	for nodepool := range missingResourcesMap {
		nodepools = append(nodepools, nodepool)
	}
	sort.Strings(nodepools)

	nodepoolMessages := make([]string, 0, len(nodepools))
	for _, nodepool := range nodepools {
		resources := make([]string, 0, len(missingResourcesMap[nodepool]))
		for resource := range missingResourcesMap[nodepool] {
			resources = append(resources, resource)
		}
		sort.Strings(resources)

		nodepoolMessages = append(nodepoolMessages, fmt.Sprintf("%s (%s)", nodepool, strings.Join(resources, ", ")))
	}

	// Cap the number of nodepools in one message.
	if len(nodepoolMessages) > maxNodepoolsInMessage {
		nodepoolMessages = nodepoolMessages[:maxNodepoolsInMessage]
		nodepoolMessages = append(nodepoolMessages, "...")
	}

	message := fmt.Sprintf("%s, nodepools out of resources: %s", s.provreqMessagePrefix, strings.Join(nodepoolMessages, ", "))
	return true, s.provreqResponse, message
}

func nodepoolName(ng cloudprovider.NodeGroup) string {
	mig, ok := ng.(*gke.GkeMig)
	if !ok {
		return ""
	}
	return mig.NodePoolName()
}

type CouldNotParallelizeScaleup struct {
	reason status.Reasons
}

func NewCouldNotParallelizeScaleup(reason status.Reasons) *CouldNotParallelizeScaleup {
	return &CouldNotParallelizeScaleup{reason}
}

func (c *CouldNotParallelizeScaleup) Reasons() []string {
	result := []string{}
	if c.reason != nil {
		for _, reason := range c.reason.Reasons() {
			result = append(result, reason)
		}
	}
	return result
}

type CouldNotParallelizeScaleupEntry struct {
	provreqResponse      string
	provreqMessagePrefix string
}

func createCouldNotParallelizeScaleupEntry(provreqResponse, provreqMessagePrefix string) *CouldNotParallelizeScaleupEntry {
	return &CouldNotParallelizeScaleupEntry{
		provreqResponse:      provreqResponse,
		provreqMessagePrefix: provreqMessagePrefix,
	}
}
func (s *CouldNotParallelizeScaleupEntry) findMatch(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) (bool, string, string) {
	for ng, reasons := range skippedNodeGroups {
		couldNotParallelize, ok := reasons.(*CouldNotParallelizeScaleup)
		if !ok {
			continue
		}
		nodePoolName := nodepoolName(ng)
		message := fmt.Sprintf("%s in nodepool %s. %s", couldNotParallelizePrefix, nodePoolName, strings.Join(couldNotParallelize.Reasons(), ","))
		return true, couldNotParallelizeScaleup, message
	}
	return false, "", ""
}

// CouldNotScheduleAnyPodsInNodePool contains information why given node group was skipped.
type CouldNotScheduleAnyPodsInNodePool struct {
	errorMessages []string
}

// Reasons returns a slice of reasons why the node group was not considered for scale up.
func (sr *CouldNotScheduleAnyPodsInNodePool) Reasons() []string {
	return sr.errorMessages
}

// NewCouldNotScheduleAnyPodsInNodePool returns a reason describing which cluster wide resource limits were reached.
func NewCouldNotScheduleAnyPodsInNodePool(errorMessages []string) *CouldNotScheduleAnyPodsInNodePool {
	return &CouldNotScheduleAnyPodsInNodePool{
		errorMessages: errorMessages,
	}
}

type CouldNotScheduleAnyPodsInNodePoolEntry struct {
	provreqResponse      string
	provreqMessagePrefix string
}

func createCouldNotScheduleAnyPodsInNodePoolEntry(provreqResponse, provreqMessagePrefix string) *CouldNotScheduleAnyPodsInNodePoolEntry {
	return &CouldNotScheduleAnyPodsInNodePoolEntry{
		provreqResponse:      provreqResponse,
		provreqMessagePrefix: provreqMessagePrefix,
	}
}
func (s *CouldNotScheduleAnyPodsInNodePoolEntry) findMatch(skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons) (bool, string, string) {
	unschedulableNodesMap := map[string][]string{}
	for nodeGroup, reasons := range skippedNodeGroups {
		noschd, ok := reasons.(*CouldNotScheduleAnyPodsInNodePool)
		if !ok {
			continue
		}

		unschedulableNodesMap[nodepoolName(nodeGroup)] = noschd.errorMessages
	}
	if len(unschedulableNodesMap) == 0 {
		return false, "", ""
	}
	nodepools := make([]string, 0, len(unschedulableNodesMap))
	for unschedulableNode := range unschedulableNodesMap {
		nodepools = append(nodepools, unschedulableNode)
	}
	sort.Strings(nodepools)

	predicateErrorMessages := make([]string, 0, len(unschedulableNodesMap))
	for _, nodepool := range nodepools {
		predicateErrorMessages = append(predicateErrorMessages, fmt.Sprintf("%s (%s)", nodepool, strings.Join(unschedulableNodesMap[nodepool], ", ")))
	}
	if len(predicateErrorMessages) > maxPredicateErrorNodepoolsInMessage {
		predicateErrorMessages = predicateErrorMessages[:maxPredicateErrorNodepoolsInMessage]
		predicateErrorMessages = append(predicateErrorMessages, "...")
	}
	message := fmt.Sprintf("%s. Predicate checking errors: %s", s.provreqMessagePrefix, strings.Join(predicateErrorMessages, ", "))
	return true, s.provreqResponse, message
}
