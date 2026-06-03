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

package queuedwrapper

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/klog/v2"
)

var (
	// DefaultMaxRunDuration - maximum time that the users VMs can be provisioned for.
	DefaultMaxRunDuration = 7 * 24 * time.Hour
)

const (
	// QueuedProvisioningClassName const used to denote QRM specific provisioning class.
	QueuedProvisioningClassName = "queued-provisioning.gke.io"
	// MaxRunDurationSecondsKey key used to denote max run duration for the requested VMs
	MaxRunDurationSecondsKey = "maxRunDurationSeconds"

	nodeGroupNameDetailKey           = "NodeGroupName"
	resizeRequestNameDetailKey       = "ResizeRequestName"
	nodePoolNameDetailKey            = "NodePoolName"
	acceleratorTypeDetailKey         = "AcceleratorType"
	selectedZoneDetailKey            = "SelectedZone"
	nodePoolAutoProvisionedDetailKey = "NodePoolAutoProvisioned"
	podTemplateNameDetailKey         = "PodTemplateName"
	ProvisioningModeDetailKey        = "ProvisioningMode"

	// OBTAINABILITY (i.e. multiple Resize Requests per single Provisioning Request) specific details
	committedZonesDetailKey       = "CommittedZones"
	committedNodeGroupsDetailKey  = "CommittedNodeGroups"
	overprovisionedZonesDetailKey = "OverprovisionedZones"

	ProvisioningModeResizeRequest = "resize_request"
	ProvisioningModeBulkMig       = "bulk_mig"

	// CapacitySearchStrategyKey is used to denote capacity search strategy: OBTAINABILITY (can be missing/empty to denote default single zone).
	CapacitySearchStrategyKey           = "capacitySearchStrategy"
	CapacitySearchStrategyObtainability = "OBTAINABILITY"
)

// MaxRunDurationFromString converts the maxRunDurationSeconds provided as string to a time duration
func MaxRunDurationFromString(mrdInSeconds string) (*time.Duration, error) {
	maxRunDurationSeconds, err := strconv.ParseInt(mrdInSeconds, 10, 0)
	if err != nil {
		return nil, fmt.Errorf("while parsing 'maxRunDurationSeconds' got error: %w", err)
	}
	maxRunDuration := time.Duration(maxRunDurationSeconds) * time.Second
	return &maxRunDuration, nil
}

// MaxRunDurationFromStringOrDefaultWithWarning converts the maxRunDurationSeconds provided as string to a time duration and defaults to 7 days in case the provided string was empty and logs a warning.
// This method shouldn't be used for Provisioning Requests, as we're expected to use the default value there in case the parameter wasn't provided, so the warning log doesn't apply.
func MaxRunDurationFromStringOrDefaultWithWarning(mrdInSeconds string) (*time.Duration, error) {
	if mrdInSeconds == "" {
		klog.Warningf("The provided MaxRunDurationInSeconds string was empty, defaulting to %+v", DefaultMaxRunDuration)
		return &DefaultMaxRunDuration, nil
	}
	return MaxRunDurationFromString(mrdInSeconds)
}

// ProvisioningRequest represents a single structure embedding the v1
// Provisioning Request, additionally containing its corresponding PodTemplates.
type ProvisioningRequest struct {
	provreqwrapper.ProvisioningRequest
}

// ProvisioningClassDetails contains information to be saved in provisioning class details map of Provisioning Request.
type ProvisioningClassDetails struct {
	// NodeGroupName is the name of the MIG.
	NodeGroupName string
	// ResizeRequestName is the name of the corresponding ResizeRequest.
	ResizeRequestName string
	// NodePoolName is the name of the k8s node pool.
	NodePoolName string
	// AcceleratorType is the name of the GPU type.
	AcceleratorType string
	// SelectedZone is the zone in which the single-zone requests will be provisioned, and designates the actually provisioned zone for OBTAINABILITY requests.
	SelectedZone string
	// NodePoolAutoProvisioned defines if a node pool was auto-provisioned for this Provisioning Request
	NodePoolAutoProvisioned AutoprovisionedStatus
	// PodTemplateName is the name of the pod template to provision for.
	PodTemplateName string
	// ProvisioningMode defines whether the request will be provisioned via Resize Request API or Bulk MIG API.
	ProvisioningMode string
	// CommittedZones is the list of zones in which the requested VMs will be attempted to be provisioned, i.e. Resize Requests will be created.
	CommittedZones []string
	// CommittedNodeGroups is the list of node groups in which the requested VMs will be attempted to be provisioned, i.e. MIGs in which Resize Requests will be created.
	CommittedNodeGroups []string
	// OverprovisionedZones is the list of zones in which the requested VMs were provisioned, resulting in overprovisioning (i.e. if multiple zones were provisioned).
	OverprovisionedZones []string
}

// AutoprovisionedStatus represents the NAP mode of a NodePool.
type AutoprovisionedStatus string

const (
	// AutoprovisionedUnset means the state is not set or unknown.
	AutoprovisionedUnset AutoprovisionedStatus = ""
	// Autoprovisioned means the NodePool is autoprovisioned i.e. was created by NAP.
	Autoprovisioned AutoprovisionedStatus = "true"
	// NotAutoprovisioned means the NodePool is not autoprovisioned i.e. wasn't created by NAP.
	NotAutoprovisioned AutoprovisionedStatus = "false"
)

func AutoprovisionedStatusFromBool(nap bool) AutoprovisionedStatus {
	if nap {
		return Autoprovisioned
	}
	return NotAutoprovisioned
}

// NewProvisioningRequest creates new ProvisioningRequest based on v1 CR.
func ToQueuedProvisioningRequest(pr provreqwrapper.ProvisioningRequest) *ProvisioningRequest {
	return &ProvisioningRequest{pr}
}

// ObtainabilityStrategy returns true if the Provisioning Request has the OBTAINABILITY capacitySearchStrategy parameter set,
// i.e. it uses OBTAINABILITY strategy to enqueue in multiple zones with multiple Resize Requests (in different MIGs in the same node pool).
func (pr *ProvisioningRequest) ObtainabilityStrategy() bool {
	detail, found := pr.Spec.Parameters[CapacitySearchStrategyKey]
	return found && string(detail) == CapacitySearchStrategyObtainability
}

// DefaultStrategy returns true if the Provisioning Request doesn't have the OBTAINABILITY capacitySearchStrategy parameter set,
// i.e. it uses default strategy to enqueue in a single zone with a single Resize Request.
func (pr *ProvisioningRequest) DefaultStrategy() bool {
	detail, found := pr.Spec.Parameters[CapacitySearchStrategyKey]
	return !found || string(detail) == ""
}

// MaxRunDuration of the Provisioning Request.
func (pr *ProvisioningRequest) MaxRunDuration() (*time.Duration, error) {
	if detail, found := pr.Spec.Parameters[MaxRunDurationSecondsKey]; found {
		return MaxRunDurationFromString(string(detail))
	}
	return nil, nil
}

// MaxRunDurationOrDefaultWithWarning returns MaxRunDuration of the Provisioning Request or default value
func (pr *ProvisioningRequest) MaxRunDurationOrDefaultWithWarning() (*time.Duration, error) {
	return MaxRunDurationFromStringOrDefaultWithWarning(string(pr.Spec.Parameters[MaxRunDurationSecondsKey]))
}

func (pr *ProvisioningRequest) provisioningRequestDetail(detailKey string) *string {
	if detail, found := pr.Status.ProvisioningClassDetails[detailKey]; found {
		detailString := string(detail)
		return &detailString
	}
	return nil
}

// ResizeRequestName of the Provisioning Request.
func (pr *ProvisioningRequest) ResizeRequestName() *string {
	return pr.provisioningRequestDetail(resizeRequestNameDetailKey)
}

// NodeGroupName of the Provisioning Request.
func (pr *ProvisioningRequest) NodeGroupName() *string {
	return pr.provisioningRequestDetail(nodeGroupNameDetailKey)
}

// NodePoolName of the Provisioning Request.
func (pr *ProvisioningRequest) NodePoolName() *string {
	return pr.provisioningRequestDetail(nodePoolNameDetailKey)
}

// AcceleratorType of the VMs requested via Provisioning Request.
func (pr *ProvisioningRequest) AcceleratorType() *string {
	return pr.provisioningRequestDetail(acceleratorTypeDetailKey)
}

// NodePoolAutoProvisioned returns 'true' if a NAP-injected node pool was created for this Provisioning Request.
func (pr *ProvisioningRequest) NodePoolAutoProvisioned() *string {
	return pr.provisioningRequestDetail(nodePoolAutoProvisionedDetailKey)
}

// SelectedZone is the zone in which the VMs requested via Provisioning Request will get provisioned.
func (pr *ProvisioningRequest) SelectedZone() *string {
	return pr.provisioningRequestDetail(selectedZoneDetailKey)
}

// PodTemplateName is the name of the pod template to provision according to.
func (pr *ProvisioningRequest) PodTemplateName() *string {
	return pr.provisioningRequestDetail(podTemplateNameDetailKey)
}

// ProvisioningMode defines whether the request will be provisioned via Resize Request API or Bulk MIG API.
func (pr *ProvisioningRequest) ProvisioningMode() *string {
	return pr.provisioningRequestDetail(ProvisioningModeDetailKey)
}

// CommittedZones is the list of zones in which the requested VMs will be attempted to be provisioned, i.e. Resize Requests will be created.
func (pr *ProvisioningRequest) CommittedZones() *string {
	return pr.provisioningRequestDetail(committedZonesDetailKey)
}

// CommittedNodeGroups is the list of node groups in which the requested VMs will be attempted to be provisioned, i.e. MIGs in which Resize Requests will be created.
func (pr *ProvisioningRequest) CommittedNodeGroups() *string {
	return pr.provisioningRequestDetail(committedNodeGroupsDetailKey)
}

// OverprovisionedZones is the list of zones in which the requested VMs were provisioned, resulting in overprovisioning (i.e. if multiple zones were provisioned).
func (pr *ProvisioningRequest) OverprovisionedZones() *string {
	return pr.provisioningRequestDetail(overprovisionedZonesDetailKey)
}

// SetProvisioningClassDetails sets the ResizeRequestName, NodeGroupName, NodePoolName, AcceleratorType and SelectedZone fields in ProvisioningRequest provisioning class details.
func (pr *ProvisioningRequest) SetProvisioningClassDetails(details *ProvisioningClassDetails) {
	if pr.Status.ProvisioningClassDetails == nil {
		pr.Status.ProvisioningClassDetails = make(map[string]v1.Detail)
	}
	setDetailIfNotEmpty(pr, nodeGroupNameDetailKey, details.NodeGroupName)
	setDetailIfNotEmpty(pr, resizeRequestNameDetailKey, details.ResizeRequestName)
	setDetailIfNotEmpty(pr, nodePoolNameDetailKey, details.NodePoolName)
	setDetailIfNotEmpty(pr, acceleratorTypeDetailKey, details.AcceleratorType)
	setDetailIfNotEmpty(pr, selectedZoneDetailKey, details.SelectedZone)
	setDetailIfNotEmpty(pr, nodePoolAutoProvisionedDetailKey, string(details.NodePoolAutoProvisioned))
	setDetailIfNotEmpty(pr, podTemplateNameDetailKey, details.PodTemplateName)
	setDetailIfNotEmpty(pr, ProvisioningModeDetailKey, details.ProvisioningMode)
	setDetailIfNotEmpty(pr, committedZonesDetailKey, concatListDetails(details.CommittedZones))
	setDetailIfNotEmpty(pr, committedNodeGroupsDetailKey, concatListDetails(details.CommittedNodeGroups))
	setDetailIfNotEmpty(pr, overprovisionedZonesDetailKey, concatListDetails(details.OverprovisionedZones))
}

func setDetailIfNotEmpty(pr *ProvisioningRequest, key string, value string) {
	if value != "" {
		pr.Status.ProvisioningClassDetails[key] = v1.Detail(value)
	}
}

func concatListDetails(list []string) string {
	return strings.Join(list, ",")
}

// ClearProvisioningClassDetails clears the ResizeRequestName, NodeGroupName, NodePoolName, AcceleratorType and SelectedZone fields in ProvisioningRequest provisioning class details.
func (pr *ProvisioningRequest) ClearProvisioningClassDetails() {
	if pr.Status.ProvisioningClassDetails != nil {
		delete(pr.Status.ProvisioningClassDetails, nodeGroupNameDetailKey)
		delete(pr.Status.ProvisioningClassDetails, resizeRequestNameDetailKey)
		delete(pr.Status.ProvisioningClassDetails, nodePoolNameDetailKey)
		delete(pr.Status.ProvisioningClassDetails, acceleratorTypeDetailKey)
		delete(pr.Status.ProvisioningClassDetails, selectedZoneDetailKey)
		delete(pr.Status.ProvisioningClassDetails, nodePoolAutoProvisionedDetailKey)
		delete(pr.Status.ProvisioningClassDetails, podTemplateNameDetailKey)
		delete(pr.Status.ProvisioningClassDetails, ProvisioningModeDetailKey)
		delete(pr.Status.ProvisioningClassDetails, committedZonesDetailKey)
		delete(pr.Status.ProvisioningClassDetails, committedNodeGroupsDetailKey)
		delete(pr.Status.ProvisioningClassDetails, overprovisionedZonesDetailKey)
	}
}

// QueuedProvisioningRequests return ProvisioningRequests of QueuedProvisioningClass.
func QueuedProvisioningRequests(prs []*provreqwrapper.ProvisioningRequest) []*provreqwrapper.ProvisioningRequest {
	qprs := make([]*provreqwrapper.ProvisioningRequest, 0, len(prs))
	for _, pr := range prs {
		if pr.Spec.ProvisioningClassName == QueuedProvisioningClassName {
			qprs = append(qprs, pr)
		}
	}
	return qprs
}
