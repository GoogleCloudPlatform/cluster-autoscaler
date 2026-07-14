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

package manager

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler/reasons"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	klog "k8s.io/klog/v2"
)

const (
	maxRunDurationParseErrorReason  = "InvalidArgumentMaxRunDurationParseError"
	maxRunDurationParseErrorMessage = "Additional parameter maxRunDurationSeconds could not be parsed."
	failedUnexpectedStateReason     = "InternalErrorUnexpectedState"
	failedUnexpectedStateMessage    = "Reached unexpected state, please retry"
)

// ProvisioningRequestManager is a component that is responsible for reconciling statuses
// of the Resize Requests and matching Provisioning Requests.
type ProvisioningRequestManager interface {
	// Refresh reconciles Provisioning Requests with Resize Requests.
	Refresh(rrMigs, bulkMigs map[gce.GceRef]common.GkeMigWrapper, now time.Time) error
	// CreateResizeRequest creates new instance of Resize Request and updates state of the Provisioning Request.
	CreateResizeRequest(*ProvisioningRequestDetailsSpec, ShouldUpdateProvReqDetails) error
	// CreateQueuedBulkInstances creates new instances and updates state of the Provisioning Request.
	CreateQueuedBulkInstances(mig common.GkeMigWrapper, spec *ProvisioningRequestDetailsSpec) error
	// ResizeRequests returns all Resize Requests for a given node group.
	ResizeRequests(mig gce.GceRef) ([]resizerequestclient.ResizeRequestStatus, error)
	// QueuedProvisioningNodeHasScaleDownImmunity returns true if the provided QueuedProvisioning node still shouldn't get scaled down,
	// i.e. additionalImmunity hasn't ran out yet.
	QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool
}

// ProvisioningRequestDetailsSpec contains details of the Provisioning Request
type ProvisioningRequestDetailsSpec struct {
	// ProjectID the request will be created in.
	ProjectID string
	// Namespace of the corresponding Provisioning Request.
	ProvReqNamespace string
	// Name of the corresponding Provisioning Request.
	ProvReqName string
	// Zone in which the request will be created.
	Zone string
	// Delta is the number of VMs to request.
	Delta int64
	// Name of the MIG in which the request will be created.
	MigName string
	// NodePoolName: name of node pool in which the request should get queued.
	NodePoolName string
	// AcceleratorType: name of accelerator type of the requested machine.
	AcceleratorType string
	// MigAutoProvisioned defines if the MIG in which the request will be created was auto-provisioned.
	MigAutoProvisioned bool
	// ProvisioningMode defines whether the request will be provisioned via Resize Request API or Bulk MIG API.
	ProvisioningMode string
}

type provisioningRequestReconciler interface {
	// ReconcileRequests updates statuses of Provisioning Requests:
	// -  based on states of their corresponding Resize Requests for regular QueuedProvisioning migs
	// -  based on states of their corresponding `bulkInstanceOperation` status for BulkMig QueuedProvisioning migs
	// - and initializes new Provisioning Requests which were not Accepted (i.e. not chosen for scale up) yet, thus were not assigned the ResizeRequest/BulkMig.
	ReconcileRequests(rrMigs, bulkMigs map[gce.GceRef]common.GkeMigWrapper, now time.Time) error
	// QueuedProvisioningNodeHasScaleDownImmunity returns true if the provided QueuedProvisioning node still shouldn't get scaled down,
	// i.e. additionalImmunity hasn't ran out yet.
	QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool
}

type provreqClient interface {
	UpdateProvisioningRequest(*v1.ProvisioningRequest) (*v1.ProvisioningRequest, error)
	DeleteProvisioningRequest(*v1.ProvisioningRequest) error
	ProvisioningRequests() ([]*provreqwrapper.ProvisioningRequest, error)
	ProvisioningRequest(string, string) (*provreqwrapper.ProvisioningRequest, error)
}

// provReqManager manages integration between Provisioning Requests and Resize Requests.
type provReqManager struct {
	queuedResizeRequestService resizerequestclient.ResizeRequestClient
	prClient                   provreqClient
	requestReconciler          provisioningRequestReconciler

	// provisioningRequestStatuses serves as a set of all the Provisioning Request's <state, reason> pairs that have ever existed in the cluster and it's used for metric accounting.
	provisioningRequestStatuses map[prStatusKey]bool
	// podSetCustomResources serves as a set of all the Provisioning Request PodSet's custom resource subsets that have ever existed in the cluster and it's used for metric accounting.
	podSetCustomResources map[string]bool
}

type prStatusKey struct {
	state  provreqstate.ProvisioningRequestState
	reason string
}

type longAcceptedMetricLabels struct {
	acceleratorType string
	zone            string
	napUsed         string
}

type ShouldUpdateProvReqDetails bool

const (
	UpdateProvReqDetails      ShouldUpdateProvReqDetails = true
	DoNotUpdateProvReqDetails ShouldUpdateProvReqDetails = false
)

// NewProvisioningRequestManager returns new instance of the Provisioning Request manager.
func NewProvisioningRequestManager(prClient *provreqclient.ProvisioningRequestClient, queuedResizeRequestService resizerequestclient.ResizeRequestClient, bulkMigClient bulkmig.GceMigClient, projectId string, prCache *provreqcache.QueuedProvisioningCache, experimentsManager experiments.Manager) (*provReqManager, error) {
	requestReconciler, err := reconciler.NewCompositeProvisioningRequestReconciler(prClient, prCache, queuedResizeRequestService, bulkMigClient, experimentsManager, projectId)
	if err != nil {
		return nil, err
	}
	return &provReqManager{
		queuedResizeRequestService:  queuedResizeRequestService,
		prClient:                    prClient,
		requestReconciler:           requestReconciler,
		provisioningRequestStatuses: map[prStatusKey]bool{},
		podSetCustomResources:       map[string]bool{},
	}, nil
}

func (m *provReqManager) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool {
	return m.requestReconciler.QueuedProvisioningNodeHasScaleDownImmunity(node, migSpec, now)
}

// CreateQueuedBulkInstances creates new instances and updates state of the Provisioning Request.
func (m *provReqManager) CreateQueuedBulkInstances(mig common.GkeMigWrapper, spec *ProvisioningRequestDetailsSpec) error {
	pr, err := m.prClient.ProvisioningRequest(spec.ProvReqNamespace, spec.ProvReqName)
	if err != nil {
		return fmt.Errorf("couldn't retrieve Provisioning Request %s/%s: %w", spec.ProvReqNamespace, spec.ProvReqName, err)
	}
	details := detailsFromSpec(pr, spec)
	if err = provreqstate.SetProvisioningClassDetails(pr, details); err != nil {
		return m.handleProvisioningClassDetailsError(pr, err)
	}
	if pr.ProvisioningRequest, err = m.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
		return fmt.Errorf("couldn't update Provisioning Request %s/%s with the name of Bulk MIG: %w", pr.Namespace, pr.Name, err)
	}

	klog.V(4).Infof("Assigned ProvisioningRequest %s/%s to Bulk MIG %q", pr.Namespace, pr.Name, spec.MigName)
	state := provreqstate.AcceptedState
	finalError := utils.NewMultiErr(2)

	if err = updateLabelsIfNeeded(mig, pr); err != nil {
		klog.Errorf("UpdateNodePoolLabels for PR %s/%s failed with %v; spec: %+v", pr.Namespace, pr.Name, err, spec)
		state = provreqstate.FailedState
		err = provreqstate.SetStateCustomReasonMessage(pr, state, reasons.BulkNodePoolUpdateFailedReason, reasons.BulkNodePoolUpdateFailedMessage, metav1.Now())
	} else if err = mig.IncreaseSize(int(spec.Delta)); err != nil {
		klog.Errorf("IncreaseSize for PR %s/%s failed with %v", pr.Namespace, pr.Name, err)
		state = provreqstate.FailedState
		err = provreqstate.SetStateCustomReasonMessage(pr, state, reasons.BulkIncreaseSizeFailedReason, reasons.BulkIncreaseSizeFailedMessage, metav1.Now())
	} else {
		err = provreqstate.SetState(pr, state, metav1.Now())
	}

	return m.finalizeProvisioningRequest(pr, details.ProvisioningMode, err, finalError, UpdateProvReqDetails)
}

// CreateResizeRequest creates new instance of Resize Request and updates state of the Provisioning Request.
func (m *provReqManager) CreateResizeRequest(spec *ProvisioningRequestDetailsSpec, shouldUpdateProvReqDetails ShouldUpdateProvReqDetails) error {
	pr, err := m.prClient.ProvisioningRequest(spec.ProvReqNamespace, spec.ProvReqName)
	if err != nil {
		return fmt.Errorf("couldn't retrieve Provisioning Request %s/%s: %w", spec.ProvReqNamespace, spec.ProvReqName, err)
	}
	maxRunDuration, err := queuedwrapper.ToQueuedProvisioningRequest(*pr).MaxRunDuration()
	if err != nil {
		klog.Errorf("While getting MaxRunDuration for ProvReq %s/%s got error: %v", pr.Namespace, pr.Name, err)
		if shouldUpdateProvReqDetails {
			if err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, maxRunDurationParseErrorReason, maxRunDurationParseErrorMessage, metav1.Now()); err != nil {
				return fmt.Errorf("Error while updating state of Provisioning Request %s/%s to %q with reason %q and message %q: %v", pr.Namespace, pr.Name, provreqstate.FailedState, maxRunDurationParseErrorReason, maxRunDurationParseErrorMessage, err)
			}
			pr.ProvisioningRequest, err = m.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest)
		}
		return err
	}

	details := detailsFromSpec(pr, spec)
	if shouldUpdateProvReqDetails {
		if err = provreqstate.SetProvisioningClassDetails(pr, details); err != nil {
			return m.handleProvisioningClassDetailsError(pr, err)
		}
		if pr.ProvisioningRequest, err = m.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
			return fmt.Errorf("couldn't update Provisioning Request %s/%s with the name of Resize Request and MIG: %w", pr.Namespace, pr.Name, err)
		}

		klog.V(4).Infof("Assigned ProvisioningRequest %s/%s to Resize Request %q and MIG %q", pr.Namespace, pr.Name, details.ResizeRequestName, spec.MigName)
	}
	state := provreqstate.AcceptedState

	createRequest := resizerequestclient.ResizeRequestCreateRequest{
		Name:                 details.ResizeRequestName,
		ResizeBy:             spec.Delta,
		RequestedRunDuration: &queuedwrapper.DefaultMaxRunDuration,
	}
	if maxRunDuration != nil {
		createRequest.RequestedRunDuration = maxRunDuration
	}

	finalError := utils.NewMultiErr(2)
	if err = m.queuedResizeRequestService.CreateResizeRequest(context.Background(), gce.GceRef{Project: spec.ProjectID, Name: spec.MigName, Zone: spec.Zone}, createRequest); err != nil {
		rrErrorInfo, shouldBackOff := reasons.GetDwsErrorInfoFromResizeRequestOperationError(err, spec.MigName, spec.Zone)
		// We don't want to return error on all errors returned on the Resize Request creation,
		// as some might be caused by the user misconfiguration of the specific Provisioning Request.
		if shouldBackOff {
			finalError.Append(err)
		}
		state = provreqstate.FailedState
		if shouldUpdateProvReqDetails {
			err = provreqstate.SetStateCustomReasonMessage(pr, state, rrErrorInfo.Reason, rrErrorInfo.Message, metav1.Now())
		}
	} else if shouldUpdateProvReqDetails {
		err = provreqstate.SetState(pr, state, metav1.Now())
	}

	return m.finalizeProvisioningRequest(pr, details.ProvisioningMode, err, finalError, shouldUpdateProvReqDetails)
}

func (m *provReqManager) handleProvisioningClassDetailsError(pr *provreqwrapper.ProvisioningRequest, err error) error {
	klog.Warningf("Couldn't update details in Provisioning Request %s/%s, marking it as Failed, got error: %v", pr.Namespace, pr.Name, err)

	// attempt to update the state in-memory
	if err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, failedUnexpectedStateReason, failedUnexpectedStateMessage, metav1.Now()); err != nil {
		return fmt.Errorf("while moving the Provisioning Request %s/%s to Failed state got error: %w", pr.Namespace, pr.Name, err)
	}
	// attempt to persist the state to cluster
	if pr.ProvisioningRequest, err = m.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
		return fmt.Errorf("couldn't update Provisioning Request %s/%s to Failed state, due to unexpected state: %w", pr.Namespace, pr.Name, err)
	}
	// successfully marked the Provisioning Request as Failed
	return nil
}

func (m *provReqManager) finalizeProvisioningRequest(pr *provreqwrapper.ProvisioningRequest, provisioningMode string, err error, finalError *utils.MultiError, shouldUpdateProvReqDetails ShouldUpdateProvReqDetails) error {
	if err != nil {
		finalError.Append(err)
	} else if shouldUpdateProvReqDetails {
		pr.ProvisioningRequest, err = m.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest)
		if err != nil {
			finalError.Append(err)
		}
	}
	if finalError.ErrorOrNil() != nil {
		if pr == nil {
			klog.Errorf("While creating %v instances for nil PR got error: %s", provisioningMode, finalError.Error())
		} else {
			klog.Errorf("While creating %v instances for PR %s/%s got error: %s", provisioningMode, pr.Namespace, pr.Name, finalError.Error())
		}
	}
	status := provreqstate.StatusOfProvisioningRequest(pr)
	var nodePoolAutoProvisioned string
	if autoProvisioned := queuedwrapper.ToQueuedProvisioningRequest(*pr).NodePoolAutoProvisioned(); autoProvisioned != nil {
		nodePoolAutoProvisioned = *autoProvisioned
	}
	metrics.Metrics.ObserveProvisioningRequestProcessingLatencySeconds(string(status.State), status.Reason, nodePoolAutoProvisioned, time.Since(pr.CreationTimestamp.Time))
	return finalError.ErrorOrNil()
}

func detailsFromSpec(pr *provreqwrapper.ProvisioningRequest, spec *ProvisioningRequestDetailsSpec) *queuedwrapper.ProvisioningClassDetails {
	details := &queuedwrapper.ProvisioningClassDetails{
		NodeGroupName:           spec.MigName,
		NodePoolName:            spec.NodePoolName,
		AcceleratorType:         spec.AcceleratorType,
		SelectedZone:            spec.Zone,
		NodePoolAutoProvisioned: queuedwrapper.AutoprovisionedStatusFromBool(spec.MigAutoProvisioned),
		PodTemplateName:         PodTemplateNames(pr.PodTemplates),
		ProvisioningMode:        spec.ProvisioningMode,
	}
	if spec.ProvisioningMode == queuedwrapper.ProvisioningModeResizeRequest {
		details.ResizeRequestName = resizerequestclient.ResizeRequestName(pr.Namespace, pr.Name)
	}
	return details
}

func PodTemplateNames(podTemplates []*apiv1.PodTemplate) string {
	var pdNames []string
	for _, podTemplate := range podTemplates {
		pdNames = append(pdNames, podTemplate.Name)
	}
	return strings.Join(pdNames, ", ")
}

func updateLabelsIfNeeded(mig common.GkeMigWrapper, pr *provreqwrapper.ProvisioningRequest) error {
	// Reason for the label update: go/gke-autoscaler-provreq-labels-update
	updatedLabels := make(map[string]string)
	if spec := mig.Spec(); spec != nil {
		maps.Copy(updatedLabels, spec.Labels)
	}
	wantRRName := resizerequestclient.ResizeRequestName(pr.Namespace, pr.Name)
	if foundRRName, found := updatedLabels[gkelabels.ProvisioningRequestLabelKey]; found && foundRRName == wantRRName {
		return nil // the node pool already has this label - no need to update
	}
	updatedLabels[gkelabels.ProvisioningRequestLabelKey] = wantRRName
	return mig.UpdateNodePoolLabels(updatedLabels)
}

// Refresh reconciles Resize Requests with Provisioning Requests and updates metrics.
func (m *provReqManager) Refresh(rrMigs, bulkMigs map[gce.GceRef]common.GkeMigWrapper, now time.Time) error {
	if err := m.requestReconciler.ReconcileRequests(rrMigs, bulkMigs, now); err != nil {
		return err
	}
	if err := m.updateLongUnprocessedProvisioningRequestCountMetric(); err != nil {
		return err
	}
	if err := m.updateLongAcceptedProvisioningRequestCountMetric(); err != nil {
		return err
	}
	return m.updateProvisioningRequestCountMetric()
}

// ResizeRequests returns all Resize Requests for a given node group.
func (m *provReqManager) ResizeRequests(mig gce.GceRef) ([]resizerequestclient.ResizeRequestStatus, error) {
	return m.queuedResizeRequestService.ResizeRequests(context.Background(), mig)
}

func (m *provReqManager) updateProvisioningRequestCountMetric() error {
	prs, err := m.prClient.ProvisioningRequests()
	if err != nil {
		return fmt.Errorf("error while listing Provisioning Requests: %w", err)
	}
	qprs := queuedwrapper.QueuedProvisioningRequests(prs)
	provisioningRequestCount := map[prStatusKey]int{}
	for _, pr := range qprs {
		status := provreqstate.StatusOfProvisioningRequest(pr)
		statusKey := prStatusKey{status.State, status.Reason}
		m.provisioningRequestStatuses[statusKey] = true
		provisioningRequestCount[statusKey]++
	}

	for statusKey := range m.provisioningRequestStatuses {
		metrics.Metrics.UpdateProvisioningRequestCount(string(statusKey.state), statusKey.reason, provisioningRequestCount[statusKey])
	}
	return nil
}

const (
	maxExpectedProcessingTime = 1 * time.Hour
	longStuckInAcceptedTime   = 24 * time.Hour
)

func (m *provReqManager) updateLongUnprocessedProvisioningRequestCountMetric() error {
	longUnprocessedProvisioningRequestCount := map[string]int{}
	unprocessedStates := []provreqstate.ProvisioningRequestState{provreqstate.UninitializedState, provreqstate.PendingState}

	for _, unprocessedState := range unprocessedStates {
		prs, err := provreqstate.ProvisioningRequestsInState(m.prClient, unprocessedState)
		if err != nil {
			return fmt.Errorf("error while listing Provisioning Requests in state %q: %w", unprocessedState, err)
		}

		for _, pr := range prs {
			if time.Since(pr.CreationTimestamp.Time) > maxExpectedProcessingTime {
				podSets, err := pr.PodSets()
				if err != nil {
					// Ignore the error and export metric with empty resource types.
					// We cannot log the error as it would be logged each CA loop.
					podSets = []provreqwrapper.PodSet{}
				}
				customResources := podSetsCustomResourceTypes(podSets)
				m.podSetCustomResources[customResources] = true
				longUnprocessedProvisioningRequestCount[customResources]++
			}
		}
	}

	for resourcesKey := range m.podSetCustomResources {
		metrics.Metrics.UpdateLongUnprocessedProvisioningRequestCount(resourcesKey, longUnprocessedProvisioningRequestCount[resourcesKey])
	}
	return nil
}

func (m *provReqManager) updateLongAcceptedProvisioningRequestCountMetric() error {
	longAcceptedProvisioningRequestCount := map[longAcceptedMetricLabels]int{}
	prs, err := provreqstate.ProvisioningRequestsInState(m.prClient, provreqstate.AcceptedState)
	if err != nil {
		return fmt.Errorf("error while listing Provisioning Requests in state %q: %w", provreqstate.AcceptedState, err)
	}
	for _, pr := range prs {
		qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
		if time.Since(pr.CreationTimestamp.Time) < longStuckInAcceptedTime {
			continue
		}
		var selectedZone string
		var acceleratorType string
		var nodePoolAutoProvisioned string
		// In cases where pr.SelectedZone() or pr.AcceleratorType() is nil, export the
		// metric with an empty string for that dimension. We cannot report it as an
		// error because a single pr without these values would return error in every loop
		if zone := qpr.SelectedZone(); zone != nil {
			selectedZone = *zone
		}
		if accelerator := qpr.AcceleratorType(); accelerator != nil {
			acceleratorType = *accelerator
		}
		if autoProvisioned := qpr.NodePoolAutoProvisioned(); autoProvisioned != nil {
			nodePoolAutoProvisioned = *autoProvisioned
		}
		labels := longAcceptedMetricLabels{acceleratorType: acceleratorType, zone: selectedZone, napUsed: nodePoolAutoProvisioned}
		longAcceptedProvisioningRequestCount[labels]++
	}
	for labels, count := range longAcceptedProvisioningRequestCount {
		metrics.Metrics.UpdateLongAcceptedProvisioningRequestCount(labels.toMap(), count)
	}
	return nil
}

const (
	gpuAcceleratorLabel    = "gpu"
	tpuAcceleratorLabel    = "tpu"
	noCustomResourcesLabel = "none"
)

// podSetsCustomResourceTypes returns all unique custom resource types present in the podSets in lexicographic order concatenated in a single string,
// e.g. "none", "tpu", "gpu,tpu", "none,tpu", "gpu,none,tpu".
func podSetsCustomResourceTypes(podSets []provreqwrapper.PodSet) string {
	resources := map[string]bool{}
	for _, podSet := range podSets {
		resources[podTemplateCustomResourceType(podSet.PodTemplate)] = true
	}

	resourceKeys := []string{}
	for key := range resources {
		resourceKeys = append(resourceKeys, key)
	}
	sort.Strings(resourceKeys)
	return strings.Join(resourceKeys, ",")
}

// podTemplateCustomResourceType returns the custom resource accelerator type present in the PodSet: "gpu", "tpu" or "none".
func podTemplateCustomResourceType(template apiv1.PodTemplateSpec) string {
	for _, container := range template.Spec.Containers {
		if container.Resources.Requests != nil {
			if _, found := container.Resources.Requests[gpu.ResourceNvidiaGPU]; found {
				return gpuAcceleratorLabel
			}
			if _, found := container.Resources.Requests[tpu.ResourceGoogleTPU]; found {
				return tpuAcceleratorLabel
			}
		}
	}
	return noCustomResourcesLabel
}

func (l longAcceptedMetricLabels) toMap() map[string]string {
	return map[string]string{
		"accelerator_type": l.acceleratorType,
		"zone":             l.zone,
		"nap_used":         l.napUsed,
	}
}
