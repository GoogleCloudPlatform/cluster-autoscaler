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

package resizerequests

import (
	"fmt"
	"maps"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	autoscalingcontext "k8s.io/autoscaler/cluster-autoscaler/context"
	nodegroupchange "k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler/reasons"
	"k8s.io/klog/v2"
)

var defaultError = cloudprovider.InstanceErrorInfo{
	ErrorClass:   cloudprovider.OtherErrorClass,
	ErrorCode:    gce.ErrorCodeOther,
	ErrorMessage: "Unknown error",
}

type GkeCloudProvider interface {
	GetGkeMigs() []*gke.GkeMig
	GetAvailableGPUTypes() map[string]struct{}
	GetNodeGpuConfig(*apiv1.Node) *cloudprovider.GpuConfig
}

type ErrorReporter struct {
	ctx              *autoscalingcontext.AutoscalingContext
	gkeCloudProvider GkeCloudProvider

	// TODO(b/381046606): remove FSNQ node pools handling from error_reporter after migration to CreateInstances API
	now                func() time.Time
	experimentsManager experiments.Manager
	// migHadEmptyResizeRequestsList saves whether the last Resize Request List call on Refresh returned empty list (there were no Resize Requests in the MIG)
	// If value is missing, it defaults to false so that we'll Refresh the MIG on the 1st Refresh regardless of scale up status
	migHadEmptyResizeRequestsList map[gce.GceRef]bool
	scaleStateNotifier            *nodegroupchange.NodeGroupChangeObserversList
}

func NewErrorReporter(experimentsManager experiments.Manager) *ErrorReporter {
	return &ErrorReporter{
		now:                           time.Now,
		experimentsManager:            experimentsManager,
		migHadEmptyResizeRequestsList: map[gce.GceRef]bool{},
	}
}

func (r *ErrorReporter) Init(ctx *autoscalingcontext.AutoscalingContext, gkeCloudProvider GkeCloudProvider, scaleStateNotifier *nodegroupchange.NodeGroupChangeObserversList) {
	r.ctx = ctx
	r.gkeCloudProvider = gkeCloudProvider
	r.scaleStateNotifier = scaleStateNotifier
}

func isErrorReportable(mig *gke.GkeMig) bool {
	switch {
	case mig.UsesBulkProvisioning():
		return false
	case mig.QueuedProvisioning(), mig.IsSingleHostTpuMig():
		return false
	case mig.IsUpcoming():
		return false
	case mig.IsMultiHostTpuMig(), mig.FlexStartNonQueued():
		return true
	default:
		return false
	}
}

func (r *ErrorReporter) Refresh() {
	// Disclaimer: When `FlexStartNonQueuedEnabledFlag` experiment is disabled and thus we won't `handleFailedFlexStartScaleUps`,
	// we rely on `flexStartMaxNodeProvisionTime` fallback:
	// the VM placeholders will be marked as `longUnregistered` and deleted,
	// resulting in their corresponding Resize Requests getting cancelled by GCE.
	flexStartNonQueuedExpEnabled := r.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexStartNonQueuedEnabledFlag, false)

	// This experiment migrates non Flex Start Multi-host TPUs to use the new error_reporter path, same one as Flex Start.
	// This fixes lack of deletion of cancelled RRs in the old path, lack of retries for failed RR deletions,
	// and ensures all CA managed RRs in the MIG will be reported and cleaned up.
	// Using the new path also results in compatibility of supporting CapacityCheckWaitTimeSeconds feature.
	ccwtAndNewMultiHostTpuErrorReportingEnabled := r.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag, false)

	migs := r.gkeCloudProvider.GetGkeMigs()
	for _, mig := range migs {
		if !isErrorReportable(mig) {
			continue
		}

		useNewErrorReporting := r.shouldUseNewErrorReporterPath(mig, flexStartNonQueuedExpEnabled, ccwtAndNewMultiHostTpuErrorReportingEnabled)

		scaleUpInProgress := r.ctx.ClusterStateRegistry.HasNodeGroupStartedScaleUp(mig.Id())
		// The old error reporting path is meant to be deprecated, it will be unused when `CapacityCheckWaitTimeSecondsMultiHostTpu::EnabledMinCAVersion` flag is enabled.
		if !useNewErrorReporting && mig.IsMultiHostTpuMig() && scaleUpInProgress {
			r.reportResizeRequestsErrors(mig)
			return
		}

		// Refreshing only when there's active scale up can result in some of the Resize Request not getting cleaned up,
		// but staying in CANCELED state (not deleted until the next scale up in this MIG).
		// They’re no longer Active, so they don’t take up the Active Resize Request quota, but it’d still be best to clean them up and not depend on the next scale up.
		// Thus we also check whether there were any remaining Resize Requests in the MIG when we last checked.
		needToHandleResizeRequestScaleUps := scaleUpInProgress || !r.migHadEmptyResizeRequestsList[mig.GceRef()]
		if useNewErrorReporting && needToHandleResizeRequestScaleUps {
			r.handleFailedResizeRequestScaleUps(mig)
			return
		}
	}
}

func (r *ErrorReporter) shouldUseNewErrorReporterPath(mig *gke.GkeMig, flexEnabled, newTpusReportingEnabled bool) bool {
	switch {
	case mig.FlexStartNonQueued() && flexEnabled:
		return true
	case mig.IsMultiHostTpuMig() && newTpusReportingEnabled:
		return true
	default:
		return false
	}
}

// TO BE DEPRECATED
// This old error reporting path is meant to get deprecated, it will be unused when `CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag` is enabled.
// reportResizeRequestsErrors doesn't delete of cancelled RRs , doesn't retry for failed RR deletions,
// and considers only the latest Resize Request in the MIG for clean up.
// It also lacks option to support CapacityCheckWaitTimeSeconds CCC feature due to lack of explicit Cancelling of Resize Requests.
func (r *ErrorReporter) reportResizeRequestsErrors(mig *gke.GkeMig) {
	latestFailedResizeRequest := getLatestFailedResizeRequest(mig)
	if latestFailedResizeRequest == nil {
		return
	}
	errorInfo := getResizeRequestErrorInfo(mig, latestFailedResizeRequest)
	if errorInfo == nil {
		return
	}
	// Emit ScaleUpFailed event
	r.ctx.AutoscalingKubeClients.LogRecorder.Eventf(
		apiv1.EventTypeWarning,
		"ScaleUpFailed",
		"Failed adding %v nodes to group %v due to %v; source errors: %v",
		latestFailedResizeRequest.ResizeBy,
		mig.Id(),
		errorInfo.ErrorCode,
		errorInfo.ErrorMessage)

	currentTime := r.now()
	r.ctx.ClusterStateRegistry.RegisterScaleUp(mig, -int(latestFailedResizeRequest.ResizeBy), currentTime)
	r.scaleStateNotifier.RegisterFailedScaleUp(mig, int(latestFailedResizeRequest.ResizeBy), *errorInfo, currentTime)
	if err := mig.AdvanceResizeRequestCleanUp(*latestFailedResizeRequest); err != nil {
		klog.Errorf("Error while deleting resize request for mig %q: %v", mig.Id(), err)
	}
}

func getInstanceErrorInfoOrDefault(rrErrorInfo reasons.DwsErrorInfo) *cloudprovider.InstanceErrorInfo {
	if rrErrorInfo.InstanceError != nil {
		return rrErrorInfo.InstanceError
	}
	return &defaultError
}

func (r *ErrorReporter) handleFailedResizeRequestScaleUps(mig *gke.GkeMig) {
	capacityCheckWaitTimeSeconds, err := mig.CapacityCheckWaitTimeSeconds()
	if err != nil {
		klog.Errorf("handleFailedResizeRequestScaleUps: failed to get CapacityCheckWaitTimeSeconds for MIG %+v: %v. This MIG shouldn't be handled by this method, skipping", mig.GceRef(), err)
		return
	}
	klog.Infof("handleFailedResizeRequestScaleUps: using CapacityCheckWaitTimeSeconds %v for MIG %+v", capacityCheckWaitTimeSeconds, mig.GceRef())

	currentTime := r.now()
	backoffTriggered := r.processPartiallyFailedRequestCreates(mig, currentTime)
	r.processExistingRequests(mig, backoffTriggered, capacityCheckWaitTimeSeconds, currentTime)
}

func (r *ErrorReporter) processPartiallyFailedRequestCreates(mig *gke.GkeMig, currentTime time.Time) bool {
	// Multi-host TPUs have a single Resize Request created, so the failed creates just fail the whole scale up,
	// they're not registered as partially failed
	if mig.IsMultiHostTpuMig() {
		return false
	}

	// Process failed Resize Request Create calls - those scale ups were reported as successful because some of the Resize Requests got created,
	// but we need to correct the MIG size in CSR by the number of those failed requests (VMs)
	failedRRCreations := mig.ResetFailedResizeRequestsCreation()
	failedRRCreationsCount := 0
	backoffTriggered := false
	errCntPerMainErr := map[reasons.ErrorReasonMessage]int{}
	for err, count := range failedRRCreations {
		failedRRCreationsCount += count
		rrErrorInfo, shouldBackoff := reasons.GetDwsErrorInfoFromResizeRequestOperationError(err, mig.NodePoolName(), mig.GceRef().Zone)

		// Group multierrors by main error
		errCntPerMainErr[reasons.ErrorReasonMessage{Reason: rrErrorInfo.Reason, Message: rrErrorInfo.Message}] += count

		// Backoff with the first encountered backoff-causing error by calling `RegisterFailedScaleUp`
		instanceErrorInfo := getInstanceErrorInfoOrDefault(rrErrorInfo)
		if !backoffTriggered && shouldBackoff {
			backoffTriggered = true
			r.scaleStateNotifier.RegisterFailedScaleUp(mig, count, *instanceErrorInfo, currentTime)
		}
	}

	for mainErr, count := range errCntPerMainErr {
		// Emit ScaleUpFailed event for every main creation error
		r.ctx.AutoscalingKubeClients.LogRecorder.Eventf(
			apiv1.EventTypeWarning, "FlexScaleUpFailedOnTrigger",
			"Failed adding %v nodes to group %v via Flex scale up due to %q: %s",
			count, mig.Id(), mainErr.Reason, mainErr.Message)
	}

	// Correct the scale up size
	if failedRRCreationsCount > 0 {
		r.ctx.ClusterStateRegistry.RegisterScaleUp(mig, -failedRRCreationsCount, currentTime)
	}
	return backoffTriggered
}

func (r *ErrorReporter) processExistingRequests(mig *gke.GkeMig, backoffTriggered bool, capacityCheckWaitTimeSeconds time.Duration, currentTime time.Time) {
	mode := ""
	if mig.FlexStartNonQueued() {
		mode = "Flex"
	}

	// Process Resize Requests existing in the MIG
	resizeRequests, err := mig.ResizeRequests()
	if err != nil {
		klog.Errorf("Error fetching resize requests for %s group %v, proceeding without checking resize-request errors: %v", mode, mig.GceRef(), err)
		return
	}
	if len(resizeRequests) == 0 {
		r.migHadEmptyResizeRequestsList[mig.GceRef()] = true
		return
	}
	r.migHadEmptyResizeRequestsList[mig.GceRef()] = false

	rrsPerCategory := r.categorizeAndCleanUpResizeRequests(mig, resizeRequests, mode, capacityCheckWaitTimeSeconds, currentTime)

	failedRRs := slices.Collect(maps.Values(rrsPerCategory[reasons.FailedCategory]))
	errCntPerReason, mainErr := reasons.GroupResizeRequestErrors(failedRRs, capacityCheckWaitTimeSeconds, currentTime)

	// Emit ScaleUpFailed event for every Resize Request error
	errCnt := 0
	for errEntry, count := range errCntPerReason {
		r.ctx.AutoscalingKubeClients.LogRecorder.Eventf(
			apiv1.EventTypeWarning,
			fmt.Sprintf("%sScaleUpFailed", mode),
			"Failed adding %v nodes to group %v via %s scale up due to %q: %s",
			count, mig.Id(), mode, errEntry.Reason, errEntry.Message)
		errCnt += count
	}

	if errCnt > 0 {
		// Backoff with the chosen mainErrorEntry reason if there was no backoff added yet by creation errors
		if !backoffTriggered {
			instanceErrorInfo := getInstanceErrorInfoOrDefault(mainErr)
			r.scaleStateNotifier.RegisterFailedScaleUp(mig, errCnt, *instanceErrorInfo, currentTime)
		}
		// Correct the scale up size
		r.ctx.ClusterStateRegistry.RegisterScaleUp(mig, -errCnt, currentTime)
	}

	updateReportStates(mig, rrsPerCategory)
}

func updateReportStates(mig *gke.GkeMig, rrsPerCategory map[reasons.ResizeRequestCategory]map[string]resizerequestclient.ResizeRequestStatus) {
	for _, rr := range rrsPerCategory[reasons.SuccessfulCategory] {
		mig.SetReportState(rr, resizerequestclient.CleanUpOnlyState)
	}
	for _, rr := range rrsPerCategory[reasons.FailedCategory] {
		mig.SetReportState(rr, resizerequestclient.AlreadyReportedState)
	}
}

func (r *ErrorReporter) categorizeAndCleanUpResizeRequests(mig *gke.GkeMig, resizeRequests []resizerequestclient.ResizeRequestStatus, mode string, capacityCheckWaitTimeSeconds time.Duration, currentTime time.Time) map[reasons.ResizeRequestCategory]map[string]resizerequestclient.ResizeRequestStatus {
	rrsPerCategory := r.categorizeResizeRequests(mig, resizeRequests, capacityCheckWaitTimeSeconds, currentTime)

	cleanUpResizeRequests(mig, rrsPerCategory)
	// Recategorize timeouted requests as failed in case cleanUpResizeRequests updated their report state,
	// i.e. when we still fetched Accepted request, but request is actually Cancelled because the operation just finished, so it got marked as `ToBeReported`
	for _, rr := range rrsPerCategory[reasons.TimeoutCategory] {
		if mig.ReportState(rr) == resizerequestclient.ToBeReportedState {
			rrsPerCategory[reasons.FailedCategory][rr.Name] = rr
			delete(rrsPerCategory[reasons.TimeoutCategory], rr.Name)
		}
	}

	klog.Infof("Processed %s Resize Requests in MIG %q in %s: successfulRequests %d, failedRequests %d, timeoutedRequests %d, queueingRequests: %d, cleaningUpRequests: %d, unexpectedRequests: %d", mode, mig.GceRef().Name, mig.GceRef().Zone, len(rrsPerCategory[reasons.SuccessfulCategory]), len(rrsPerCategory[reasons.FailedCategory]), len(rrsPerCategory[reasons.TimeoutCategory]), len(rrsPerCategory[reasons.QueueingCategory]), len(rrsPerCategory[reasons.CleanUpCategory]), len(rrsPerCategory[reasons.UnexpectedCategory]))
	return rrsPerCategory
}

func (r *ErrorReporter) categorizeResizeRequests(mig *gke.GkeMig, resizeRequests []resizerequestclient.ResizeRequestStatus, capacityCheckWaitTimeSeconds time.Duration, currentTime time.Time) map[reasons.ResizeRequestCategory]map[string]resizerequestclient.ResizeRequestStatus {
	loggingQuota := logging.ProvisioningRequestsLoggingQuota()
	rrsPerCategory := map[reasons.ResizeRequestCategory]map[string]resizerequestclient.ResizeRequestStatus{
		reasons.SuccessfulCategory: {},
		reasons.FailedCategory:     {},
		reasons.TimeoutCategory:    {},
		reasons.QueueingCategory:   {},
		reasons.UnexpectedCategory: {},
		reasons.CleanUpCategory:    {},
	}
	for _, rr := range resizeRequests {
		if !resizerequestclient.IsFlexStartNonQueuedScaleUpResizeRequest(rr.Name) && !resizerequestclient.IsAtomicResizeRequest(rr.Name) {
			klogx.V(1).UpTo(loggingQuota).Infof("Skipping non-error-reporter managed Resize Request %q: %+v", rr.Name, rr)
			continue
		}

		reportState := mig.ReportState(rr)
		if reportState == resizerequestclient.AlreadyReportedState || reportState == resizerequestclient.CleanUpOnlyState {
			rrsPerCategory[reasons.CleanUpCategory][rr.Name] = rr
			continue
		}

		category, _ := reasons.ResizeRequestCategoryReasonMessage(rr, capacityCheckWaitTimeSeconds, currentTime)
		rrsPerCategory[category][rr.Name] = rr
	}

	klogx.V(1).Over(loggingQuota).Infof("There are also %v other skipped non-error-reporter managed Resize Requests in MIG %q", -loggingQuota.Left(), mig.GceRef().Name)
	return rrsPerCategory
}

func cleanUpResizeRequests(mig *gke.GkeMig, rrsPerCategory map[reasons.ResizeRequestCategory]map[string]resizerequestclient.ResizeRequestStatus) {
	cleanUpCategories := []reasons.ResizeRequestCategory{reasons.CleanUpCategory, reasons.SuccessfulCategory, reasons.FailedCategory, reasons.TimeoutCategory}
	for _, cat := range cleanUpCategories {
		for _, rr := range rrsPerCategory[cat] {
			// AdvanceResizeRequestCleanUp will (re)tigger/check Cancel/Delete according to state of the Resize Request
			// TODO(b/393112810): refactor this part after AdvanceResizeRequestCleanUp refactor
			if err := mig.AdvanceResizeRequestCleanUp(rr); err != nil {
				klog.Errorf("Error while deleting resize request for MIG %q: %v", mig.GceRef().Name, err)
			}
		}
	}
}

func getResizeRequestErrorInfo(mig *gke.GkeMig, resizeRequest *resizerequestclient.ResizeRequestStatus) *cloudprovider.InstanceErrorInfo {
	var errorInfo *cloudprovider.InstanceErrorInfo
	// We only need the latest error
	for _, err := range resizeRequest.Errors {
		if newErrInfo := gce.GetErrorInfo(err.Code, err.Message, "", nil); newErrInfo != nil {
			errorInfo = newErrInfo
		}
	}

	if errorInfo == nil {
		klog.Warningf("Resize Request %q is in failed state but it has no errors. This should never happen.", resizeRequest.Name)
		size, err := mig.TargetSize()
		if err != nil {
			klog.Errorf("Cannot get target size of mig %v: %v", mig.Id(), err)
		}
		if size != 0 {
			// Assume error is a fluke since the target size hasn't reverted to zero
			klog.V(4).Info("Target size is still nonzero, assuming the node pool is still scaling up")
			return nil
		}
		errorInfo = &cloudprovider.InstanceErrorInfo{
			ErrorClass: cloudprovider.OtherErrorClass,
			ErrorCode:  gce.ErrorCodeOther,
		}
	}
	return errorInfo
}

func getLatestFailedResizeRequest(mig *gke.GkeMig) *resizerequestclient.ResizeRequestStatus {
	resizeRequests, err := mig.ResizeRequests()
	if err != nil {
		klog.Errorf("Error fetching resize requests for TPU group %q, proceeding without checking resize-request errors: %v", mig.Id(), err)
		return nil
	}
	if len(resizeRequests) == 0 {
		return nil
	}
	// mig.ResizeRequests() doesn't return resize requests ordered by the creation time,
	// so we select the most recent one below.
	latestResizeRequest := resizeRequests[0]
	for _, rr := range resizeRequests {
		if rr.CreationTime.After(latestResizeRequest.CreationTime) {
			latestResizeRequest = rr
		}
	}
	// Has this request failed?
	if latestResizeRequest.State != resizerequestclient.ResizeRequestStateFailed {
		return nil
	}
	return &latestResizeRequest
}
