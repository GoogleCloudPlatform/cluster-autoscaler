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

package processors

import (
	"fmt"
	"time"

	"github.com/golang/groupcache/lru"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/pods"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	klog "k8s.io/klog/v2"
)

const (
	missingPRReason     = "MissingProvisioningRequest"
	ignoredReason       = "IgnoredInScaleUp"
	failedPRReason      = "FailedProvisioningRequest"
	internalErrorReason = "InternalError"
	// provisionedUnschedulableTimeout denotes after what duration we should consider pods with Provisioned ProvReq as potentially broken and log an event.
	provisionedUnschedulableTimeout = 10 * time.Minute
	// eventRefreshDuration denotes how often we refresh an event for a given pod.
	eventRefreshDuration = 5 * time.Minute
	// maxPodEventsPerLoop denotes how many ignored pods receive an event each loop to avoid overflowing the kube-api and prolonging the scale-up loop.
	maxPodEventsPerLoop = 25
	// maxControllerEventsPerLoop denotes how many ignored pods' controllers receive an event each loop to avoid overflowing the kube-api and prolonging the scale-up loop.
	maxControllerEventsPerLoop = 25
)

// ProvisioningRequestPodListProcessor injects pods for all Provisioning Requests.
type ProvisioningRequestPodListProcessor struct {
	queuedProvisioningCache                *provreqcache.QueuedProvisioningCache
	ossProvReqInjector                     pods.PodListProcessor
	prClient                               *provreqclient.ProvisioningRequestClient
	now                                    func() time.Time
	maxPodEventsPerLoop                    int // Kept here to allow override for testing
	maxControllerEventsPerLoop             int // Kept here to allow override for testing
	eventsToPodsReportedCurrentLoop        int
	eventsToControllersReportedCurrentLoop int
	cache                                  *eventCache
	experimentsManager                     experiments.Manager
}

// NewProvisioningRequestPodListProcessor creates a new ProvisioningRequestPodListProcessor.
func NewProvisioningRequestPodListProcessor(prClient *provreqclient.ProvisioningRequestClient, queuedProvisioningCache *provreqcache.QueuedProvisioningCache, ossProvReqInjector pods.PodListProcessor, scanInterval time.Duration, experimentsManager experiments.Manager) *ProvisioningRequestPodListProcessor {
	// CA loops are run at least `scanInterval` apart, hence CA needs to remember of `eventRefreshDuration`/`scanInterval` loops.
	// In each there are at most `eventsLoggedPerLoop` pods logged.
	loopsInDuration := int(eventRefreshDuration.Seconds() / scanInterval.Seconds())
	eventsCacheSize := maxPodEventsPerLoop*loopsInDuration + maxControllerEventsPerLoop*loopsInDuration
	return &ProvisioningRequestPodListProcessor{
		queuedProvisioningCache:                queuedProvisioningCache,
		ossProvReqInjector:                     ossProvReqInjector,
		prClient:                               prClient,
		now:                                    time.Now,
		maxPodEventsPerLoop:                    maxPodEventsPerLoop,
		maxControllerEventsPerLoop:             maxControllerEventsPerLoop,
		eventsToPodsReportedCurrentLoop:        0,
		eventsToControllersReportedCurrentLoop: 0,
		cache:                                  newEventCache(eventsCacheSize),
		experimentsManager:                     experimentsManager,
	}
}

// InjectProvisioningRequestPods injects pending pods for all Provisioning Requests that are in pending state.
func (p *ProvisioningRequestPodListProcessor) InjectProvisioningRequestPods(ca_ctx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	loggingQuota := logging.ProvisioningRequestsLoggingQuota()
	for _, pr := range p.queuedProvisioningCache.PendingProvReqs() {
		qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
		// Skip PRs in a partially assigned state, reconciler will clean those.
		if _, found := provreqstate.GetProvisioningClassDetails(qpr); found {
			continue
		}
		// Skip PRs for which the scale ups are already queued in an upcoming MIG
		if p.queuedProvisioningCache.IsUpcomingProvReq(pr_pods.GetProvReqID(pr)) {
			continue
		}
		pods, err := pr_pods.PodsForProvisioningRequest(ca_ctx.CloudProvider, p.experimentsManager, pr)
		if err != nil {
			klog.Errorf("While PodsForProvisioningRequest for ProvReq: %s/%s got error: %v", pr.Namespace, pr.Name, err)
			continue
		}
		klogx.V(1).UpTo(loggingQuota).Infof("ProvisioningRequestPodListProcessor adding %d pods from ProvReq: %s/%s", len(pods), pr.Namespace, pr.Name)
		unschedulablePods = append(unschedulablePods, pods...)
	}
	klogx.V(1).Over(loggingQuota).Infof("There are also %v other ProvReqs for which pods were added", -loggingQuota.Left())
	if p.ossProvReqInjector != nil {
		return p.ossProvReqInjector.Process(ca_ctx, unschedulablePods)
	}
	return unschedulablePods, nil
}

// IgnorePodsConsumingProvisioningRequest ignores all pods that are consuming a Provisioning Request.
func (p *ProvisioningRequestPodListProcessor) IgnorePodsConsumingProvisioningRequest(ca_ctx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) []*apiv1.Pod {
	p.eventsToPodsReportedCurrentLoop = 0
	p.eventsToControllersReportedCurrentLoop = 0
	now := p.now()
	loggingQuota := klogx.PodsLoggingQuota()
	result := make([]*apiv1.Pod, 0, len(unschedulablePods))
	for _, pod := range unschedulablePods {
		prName, found := pr_pods.ProvisioningRequestName(pod)
		if !found {
			result = append(result, pod)
			continue
		}
		klogx.V(1).UpTo(loggingQuota).Infof("Ignoring unschedulable pod %s/%s as it consumes ProvisioningRequest: %s/%s", pod.Namespace, pod.Name, pod.Namespace, prName)
		p.logIgnoredInScaleUpEvent(ca_ctx, now, pod, prName)
	}
	klogx.V(1).Over(loggingQuota).Infof("There are also %v other pods which were ignored", -loggingQuota.Left())
	return result
}

// logIgnoredInScaleUpEvent logs event regarding unschedulable pod to allow users self-service debugging.
func (p *ProvisioningRequestPodListProcessor) logIgnoredInScaleUpEvent(ca_ctx *ca_context.AutoscalingContext, now time.Time, pod *apiv1.Pod, prName string) {
	controllerRef := metav1.GetControllerOf(pod)
	sendEventToPod, sendEventToController := p.shouldSendEventToPod(pod, now), p.shouldSendEventToController(controllerRef, pod.Namespace, now)
	if !sendEventToPod && !sendEventToController {
		return
	}

	var event *eventInfo
	pr, err := p.prClient.ProvisioningRequest(pod.Namespace, prName)
	if err != nil {
		if !errors.IsNotFound(err) {
			klog.Warningf("While fetching Provisioning Request %s/%s got unrecognized error: %v", pod.Namespace, prName, err)
			return
		}
		event = getMissingPREvent(pod.Namespace, prName)
	} else {
		event = getEvent(pr, now)
	}
	if event == nil {
		return
	}

	if sendEventToPod {
		ca_ctx.Recorder.Event(pod, event.kind, event.reason, event.message)
		p.cache.setLastRecorded(pod.Kind, pod.Namespace, pod.Name, now)
		p.eventsToPodsReportedCurrentLoop++
	}
	if sendEventToController && controllerRef != nil {
		controller := &apiv1.ObjectReference{
			APIVersion: controllerRef.APIVersion,
			Kind:       controllerRef.Kind,
			Name:       controllerRef.Name,
			UID:        controllerRef.UID,
			Namespace:  pod.Namespace,
		}
		msg := fmt.Sprintf("Pod: %s/%s: %s", pod.Namespace, pod.Name, event.message)
		ca_ctx.Recorder.Event(controller, event.kind, event.reason, msg)
		p.cache.setLastRecorded(controllerRef.Kind, pod.Namespace, controllerRef.Name, now)
		p.eventsToControllersReportedCurrentLoop++
	} else if sendEventToController {
		klog.Errorf("Unable to find controller of pod: %s/%s. This should never happen, existence of the controller should have been checked earlier in the code", pod.Namespace, pod.Name)
	}
}

func (p *ProvisioningRequestPodListProcessor) shouldSendEventToPod(pod *apiv1.Pod, now time.Time) bool {
	if p.eventsToPodsReportedCurrentLoop >= p.maxPodEventsPerLoop {
		return false
	}
	lastObserved := p.cache.getLastRecorded(pod.Kind, pod.Namespace, pod.Name)
	return lastObserved.Add(eventRefreshDuration).Before(now)
}

func (p *ProvisioningRequestPodListProcessor) shouldSendEventToController(ref *metav1.OwnerReference, namespace string, now time.Time) bool {
	if ref == nil || p.eventsToControllersReportedCurrentLoop >= p.maxControllerEventsPerLoop {
		return false
	}
	lastObserved := p.cache.getLastRecorded(ref.Kind, namespace, ref.Name)
	return lastObserved.Add(eventRefreshDuration).Before(now)
}

// CleanUp cleans up the processor's internal structures.
func (p *ProvisioningRequestPodListProcessor) CleanUp() {
}

// getMissingPREvent returns event info with not found message.
func getMissingPREvent(prNamespace, prName string) *eventInfo {
	return &eventInfo{
		kind:    apiv1.EventTypeWarning,
		reason:  missingPRReason,
		message: fmt.Sprintf("Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest %s/%s that does not exist. Consider creating pods after creation of ProvisioningRequest.", prNamespace, prName),
	}
}

// getEvent returns information about an event and if there is an event to log at all.
func getEvent(pr *provreqwrapper.ProvisioningRequest, now time.Time) *eventInfo {
	event := &eventInfo{
		kind:   apiv1.EventTypeNormal,
		reason: ignoredReason,
	}
	status := provreqstate.StatusOfProvisioningRequest(pr)
	switch status.State {
	case provreqstate.UninitializedState, provreqstate.PendingState, provreqstate.AcceptedState:
		event.message = fmt.Sprintf("Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest %s/%s that is in %s state. Consider creating pods after observing Provisioned condition of ProvisioningRequest.", pr.Namespace, pr.Name, status.State)
	case provreqstate.ProvisionedState:
		if status.LastTransitionTime.Time.Add(provisionedUnschedulableTimeout).After(now) {
			// If the Pod is unschedulable but the ProvReq became Provisioned less than `provisionedUnschedulableTimeout` ago we shouldn't log an event.
			return nil
		}
		event.kind = apiv1.EventTypeWarning
		event.message = fmt.Sprintf("Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest %s/%s that is in Provisioned state. This situation persisted for some time, perhaps pod spec inconsistent with ProvisioningRequest spec or pod arrived too late and will never schedule.", pr.Namespace, pr.Name)
	case provreqstate.BookingExpiredState:
		event.kind = apiv1.EventTypeWarning
		event.message = fmt.Sprintf("Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest %s/%s that is in BookingExpired state. The pod most likely arrived too late and will never schedule as the VM was already scaled-down.", pr.Namespace, pr.Name)
	case provreqstate.CapacityRevokedState:
		event.kind = apiv1.EventTypeWarning
		event.message = fmt.Sprintf("Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest %s/%s that is in CapacityRevoked state. Pod arrived too late and will never schedule.", pr.Namespace, pr.Name)
	case provreqstate.FailedState:
		event.kind = apiv1.EventTypeWarning
		event.reason = failedPRReason
		event.message = fmt.Sprintf("Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest %s/%s that is in Failed state, with a following reason and message: [%s] %q.", pr.Namespace, pr.Name, status.Reason, status.Message)
	default: // provreqstate.InvalidState
		event.kind = apiv1.EventTypeWarning
		event.reason = internalErrorReason
		event.message = fmt.Sprintf("Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest %s/%s that is in Invalid state.", pr.Namespace, pr.Name)
		klog.Warningf("While trying to log event for pod consuming Provisioning Request %s/%s got unrecognized state %q, all conditions: %v", pr.Namespace, pr.Name, status.State, pr.Status.Conditions)
	}
	return event
}

type eventInfo struct {
	kind    string
	reason  string
	message string
}

type eventCache struct {
	cache *lru.Cache
}

func newEventCache(size int) *eventCache {
	return &eventCache{
		cache: lru.New(size),
	}
}

func (c *eventCache) getLastRecorded(kind, namespace, name string) time.Time {
	// A date in 2000 to always send the event if no events were found.
	lastObserved := time.Date(2000, 1, 1, 1, 1, 1, 1, time.UTC)
	value, found := c.cache.Get(cacheKey(kind, namespace, name))
	if !found {
		return lastObserved
	}
	lastObserved = value.(time.Time)
	return lastObserved
}

func (c *eventCache) setLastRecorded(kind, namespace, name string, value time.Time) {
	c.cache.Add(cacheKey(kind, namespace, name), value)
}

func cacheKey(kind, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", kind, namespace, name)
}
