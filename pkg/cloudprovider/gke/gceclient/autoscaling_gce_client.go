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

package gceclient

import (
	"context"
	"fmt"
	"iter"
	"net/http"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/klogx"

	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

const (
	PagelessMigInstanceLimit  = 1000
	PaginatedMigInstanceLimit = 2000

	// GkePersistentOperationError is an error code used in InstanceErrorInfo
	// that signifies this type of error is persistent.
	GkePersistentOperationError    = "GkePersistentOperationError"
	instanceActionPollingFrequency = 5 * time.Second

	// DefaultResumeInstanceActionTimeout is the default timeout waiting for instances to resume.
	DefaultResumeInstanceActionTimeout = 5 * time.Minute
	// DefaultSuspendInstanceActionTimeout is the default timeout waiting for instances to suspend.
	DefaultSuspendInstanceActionTimeout = 20 * time.Minute

	// nonBlockingErrorReportInterval is how long an identical non-blocking error
	// is suppressed for a given instance before being reported again.
	nonBlockingErrorReportInterval = 5 * time.Minute
)

// ManagedInstance represents a GCE managed instance with the fields needed
// by the autoscaler.
type ManagedInstance struct {
	Name           string
	InstanceStatus string
	TargetStatus   string
	CurrentAction  string
}

// InstanceAction is an action GCE reports as currently in flight for a managed instance, as seen in
// ManagedInstance.CurrentAction.
type InstanceAction string

const (
	// ActionNone indicates that the MIG is not currently acting on the instances.
	ActionNone InstanceAction = "NONE"
	// ActionResuming indicates that instances are resuming.
	ActionResuming InstanceAction = "RESUMING"
	// ActionSuspending indicates that instances are suspending.
	ActionSuspending InstanceAction = "SUSPENDING"
)

// actionPollingVerb names the latency metric emitted for each instance polled for action, e.g.
// "resuming_instance_action_polling".
func actionPollingVerb(action InstanceAction) string {
	return strings.ToLower(string(action)) + "_instance_action_polling"
}

// NonBlockingInstanceError is a per-instance error encountered while polling for an instance
// action. It does not fail the whole operation: GCE keeps retrying the action until it succeeds or
// the instance is deleted, so polling continues after one is reported.
type NonBlockingInstanceError struct {
	// Code and Message are the GCE error code and message from the MIG's last attempt.
	Code    string
	Message string
	// InstanceStatus is the status GCE reported for the instance alongside the error.
	InstanceStatus string
}

// Error implements error.
func (e *NonBlockingInstanceError) Error() string {
	return fmt.Sprintf("non-blocking error (status %q): code %q, message %q", e.InstanceStatus, e.Code, e.Message)
}

// PollUpdate is a single observation about one instance, as reported by PollUntilActionStops. It is
// always one of PollCompleted, PollRetrying or PollAborted, so consumers can tell the outcomes apart with
// a type switch, and each of them carries the data that only makes sense for that outcome.
type PollUpdate interface {
	// isPollUpdate keeps the set of outcomes closed to this package, so that a type switch
	// handling the three types below covers everything a poll can report.
	isPollUpdate()
}

// PollCompleted reports that the instance is no longer running the action: it either finished it or
// disappeared. Reported at most once per instance, and nothing is reported for that instance
// afterwards.
type PollCompleted struct{}

func (PollCompleted) isPollUpdate() {}

// PollRetrying reports that the instance is still running the action and that GCE reported a
// recoverable failure on its last attempt. GCE keeps retrying until the action succeeds or the
// instance is deleted, so polling continues and an instance that keeps failing is reported several
// times.
type PollRetrying struct {
	// Err is the recoverable failure GCE reported for the instance. Never nil.
	Err *NonBlockingInstanceError
}

func (PollRetrying) isPollUpdate() {}

// PollAborted reports that waiting stopped before the action did, e.g. on timeout or because the
// caller's context was cancelled. Reported at most once per instance, and nothing is reported for
// that instance afterwards.
type PollAborted struct {
	// Err says why polling is aborted. Never nil. It wraps context.DeadlineExceeded when the wait
	// timed out and context.Canceled when the caller cancelled it, so the two can be told apart
	// with errors.Is.
	Err error
}

func (PollAborted) isPollUpdate() {}

// ActionPollSeq is the stream of per-instance updates returned by PollUntilActionStops.
type ActionPollSeq = iter.Seq2[gce.GceRef, PollUpdate]

// AllReady returns an ActionPollSeq that reports every instance as PollCompleted straight away, for
// the callers and fakes that know the wait is over before it starts.
func AllReady(instances []gce.GceRef) ActionPollSeq {
	return allWithUpdate(instances, PollCompleted{})
}

// AllAborted returns an ActionPollSeq that reports every instance as PollAborted with err, for the
// callers and fakes that cannot poll at all and so fail the whole batch for the same reason.
func AllAborted(instances []gce.GceRef, err error) ActionPollSeq {
	return allWithUpdate(instances, PollAborted{Err: err})
}

// allWithUpdate returns an ActionPollSeq that reports update once for each instance, stopping as
// soon as the consumer breaks out of the range loop.
func allWithUpdate(instances []gce.GceRef, update PollUpdate) ActionPollSeq {
	return func(yield func(gce.GceRef, PollUpdate) bool) {
		for _, ref := range instances {
			if !yield(ref, update) {
				return
			}
		}
	}
}

var (
	requireShieldedVmConstraint = regexp.MustCompile("Constraint constraints/compute.requireShieldedVm violated")
)

// AutoscalingInternalGceClient is used for communicating with GCE API
// it wraps the OSS AutoscalingGceClient and provides additional methods.
type AutoscalingInternalGceClient interface {
	gce.AutoscalingGceClient

	// CreateInstancesWithRecommendation creates instances with a given scale up recommendation.
	CreateInstancesWithRecommendation(migRef gce.GceRef, baseName string, delta int64, existingInstanceProviderIds []string, recommendation string) ([]string, error)

	FetchAcceleratorTypes(zone string) (*gce_api.AcceleratorTypeList, error)
	// FetchFutureReservationsInProject fetches list of future reservations for a project.
	FetchFutureReservationsInProject(projectID string) ([]*GceFutureReservation, error)
	// FetchReservationBlocksInReservation fetches the reservation blocks for a particular reservation, in specfied project and zone.
	FetchReservationBlocksInReservation(reservationRef ReservationRef) ([]*GceReservationBlock, error)
	// FetchReservationSubBlocksInReservationBlock fetches the reservation subblocks for a particular reservation block, in specfied reservation, project and zone.
	FetchReservationSubBlocksInReservationBlock(reservationRef ReservationRef) ([]*GceReservationSubBlock, error)
	// FetchResourcePolicies fetches the resource policies in the provided project and region.
	FetchResourcePolicies(projectId, region string) ([]*GceResourcePolicy, error)
	// FetchNetwork fetches the GCE network resource from the network name and network's project.
	// Note: The network's project will NOT be the same as the cluster's project in a shared VPC topology.
	FetchNetwork(projectId, name string) (*gce_api.Network, error)
	// FetchStandardZones fetches a list standard zones avaliable for the project in a given region.
	FetchStandardZones(region string) ([]string, error)
	// FetchAIZones fetches a list AI zones avaliable for the project in a given region.
	FetchAIZones(region string) ([]string, error)
	// GetHttpTimeout exposes internal HTTP client timeout
	GetHttpTimeout() time.Duration

	// ResumeInstances resumes instances. It returns once GCE has accepted the request, which is
	// before the instances actually reach RUNNING: use PollUntilActionStops with ActionResuming
	// to wait for that.
	ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error
	// PollUntilActionStops polls instances until each one has stopped running action, the poll
	// times out, or ctx is cancelled. An update is yielded only when something happened to an
	// instance: see PollUpdate for the three outcomes and for how often each of them is reported.
	PollUntilActionStops(ctx context.Context, action InstanceAction, migRef gce.GceRef, instances []gce.GceRef) ActionPollSeq
	// SuspendInstances suspends instances. It returns once GCE has accepted the request, which is
	// before the instances actually reach SUSPENDED: use PollUntilActionStops with
	// ActionSuspending to wait for that.
	SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error
	// FetchManagedInstances fetches ManagedInstances for a given MIG.
	FetchManagedInstances(migRef gce.GceRef, filter string) ([]*ManagedInstance, error)

	// SetRecommendationApplier sets the recommendation applier
	SetRecommendationApplier(applier RecommendationApplier)
}

// MigInfoProvider provides information about a mig
type MigInfoProvider interface {
	// CapacityCheckWaitTimeSeconds returns CapacityCheckWaitTimeSeconds for a mig ref.
	CapacityCheckWaitTimeSeconds(migRef gce.GceRef) (time.Duration, error)
	// ScaleUpTime returns ScaleUpTime for a mig ref.
	ScaleUpTime(migRef gce.GceRef) (time.Time, error)
	// FlexStartNonQueued returns FlexStartNonQueued for a mig ref.
	FlexStartNonQueued(migRef gce.GceRef) bool
	// QueuedProvisioning returns if the given mig is coming from queued nodepool.
	QueuedProvisioning(migRef gce.GceRef) bool
	// IsTpuMig returns true if the given mig is a TPU mig.
	IsTpuMig(migRef gce.GceRef) bool
}

// RecommendationApplier is an interface for setting recommendation on the request.
type RecommendationApplier interface {
	ApplyRecommendation(req *gce_api.InstanceGroupManagersCreateInstancesRequest, recommendation string)
}

type NoOpRecommendationApplier struct{}

func (NoOpRecommendationApplier) ApplyRecommendation(req *gce_api.InstanceGroupManagersCreateInstancesRequest, recommendation string) {
	// Do nothing in OSS fallback.
}

const (
	// defaultAPIOperationTimeout defines the default timeout for multi-call or
	// complex API operations, such as paginated fetches. This timeout applies
	// to the entire duration of the logical operation.
	defaultAPIOperationTimeout = 60 * time.Second
	// defaultOperationPerCallTimeout defines the default timeout for the
	// individual, single-HTTP-call, API operations
	defaultOperationPerCallTimeout = 30 * time.Second
)

type autoscalingInternalGceClient struct {
	gce.AutoscalingGceClient
	experimentsManager experiments.Manager
	gceService         *gce_api.Service
	gceBetaService     *gce_api_beta.Service
	migInfoProvider    MigInfoProvider
	projectID          string
	clusterName        string
	domainUrl          string

	httpTimeout                    time.Duration
	waitTimeout                    time.Duration
	pollInterval                   time.Duration
	operationPerCallTimeout        time.Duration
	instanceActionPollingFrequency time.Duration

	recommendationApplier RecommendationApplier
}

const (
	zoneResourcePoolExhaustedWithDetails = "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS"
)

// Option is a functional option for autoscalingInternalGceClient.
type Option func(*autoscalingInternalGceClient)

// WithInstanceActionPollingFrequency sets the polling frequency for instance
// actions.
func WithInstanceActionPollingFrequency(d time.Duration) Option {
	return func(c *autoscalingInternalGceClient) {
		if d > 0 {
			c.instanceActionPollingFrequency = d
		}
	}
}

// WithExperimentsManager sets the experiments manager on the client.
func WithExperimentsManager(em experiments.Manager) Option {
	return func(c *autoscalingInternalGceClient) {
		c.experimentsManager = em
	}
}

// NewAutoscalingInternalGceClient creates a new client for communicating with GCE API.
func NewAutoscalingInternalGceClient(client *http.Client, migInfoProvider MigInfoProvider, projectID string, clusterName string, userAgent string, waitTimeout, pollInterval time.Duration, experimentsManager experiments.Manager, opts ...Option) (*autoscalingInternalGceClient, error) {
	autoscalingGCEClient, err := gce.NewAutoscalingGceClientV1WithTimeout(client, projectID, userAgent, waitTimeout, pollInterval)
	if err != nil {
		return nil, err
	}

	gceService, err := gce_api.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	gceService.UserAgent = userAgent

	gceBetaService, err := gce_api_beta.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	gceBetaService.UserAgent = userAgent

	c := &autoscalingInternalGceClient{
		AutoscalingGceClient:           autoscalingGCEClient,
		gceService:                     gceService,
		gceBetaService:                 gceBetaService,
		migInfoProvider:                migInfoProvider,
		projectID:                      projectID,
		clusterName:                    clusterName,
		waitTimeout:                    waitTimeout,
		pollInterval:                   pollInterval,
		operationPerCallTimeout:        defaultOperationPerCallTimeout,
		experimentsManager:             experimentsManager,
		httpTimeout:                    client.Timeout,
		instanceActionPollingFrequency: instanceActionPollingFrequency,
		recommendationApplier:          NoOpRecommendationApplier{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// NewCustomAutoscalingInternalGceClient creates a new client using custom server url
// for communicating with GCE API.
func NewCustomAutoscalingInternalGceClient(client *http.Client, migInfoProvider MigInfoProvider, projectID, clusterName, gceEndpoint, userAgent string, waitTimeout, pollInterval time.Duration, experimentsManager experiments.Manager, opts ...Option) (*autoscalingInternalGceClient, error) {
	domainUrl := strings.TrimSuffix(gceEndpoint, "/")
	autoscalingGCEClient, err := gce.NewCustomAutoscalingGceClientV1(client, projectID, gceEndpoint, userAgent, domainUrl, waitTimeout, pollInterval)
	if err != nil {
		return nil, err
	}

	gceService, err := gce_api.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	gceService.UserAgent = userAgent
	gceService.BasePath = gceEndpoint

	gceBetaService, err := gce_api_beta.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	gceBetaService.UserAgent = userAgent
	gceBetaService.BasePath = gceEndpoint

	c := &autoscalingInternalGceClient{
		AutoscalingGceClient:           autoscalingGCEClient,
		gceService:                     gceService,
		gceBetaService:                 gceBetaService,
		migInfoProvider:                migInfoProvider,
		projectID:                      projectID,
		clusterName:                    clusterName,
		waitTimeout:                    waitTimeout,
		pollInterval:                   pollInterval,
		operationPerCallTimeout:        defaultOperationPerCallTimeout,
		experimentsManager:             experimentsManager,
		domainUrl:                      domainUrl,
		instanceActionPollingFrequency: instanceActionPollingFrequency,
		recommendationApplier:          NoOpRecommendationApplier{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// SetRecommendationApplier sets the recommendation applier.
func (client *autoscalingInternalGceClient) SetRecommendationApplier(applier RecommendationApplier) {
	client.recommendationApplier = applier
}

// CreateInstancesWithRecommendation duplicates the OSS implementation of CreateInstances
// to inject the GKE-specific "Recommendation" field into the GCE API request without
// modifying the upstream OSS struct.
// See the OSS equivalent here: https://github.com/kubernetes/autoscaler/blob/eec9bc4dc1d26c3956ff941f786f7be52de68882/cluster-autoscaler/cloudprovider/gce/autoscaling_gce_client.go#L310-L331
func (client *autoscalingInternalGceClient) CreateInstancesWithRecommendation(migRef gce.GceRef, baseName string, delta int64, existingInstanceProviderIds []string, recommendation string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.operationPerCallTimeout)
	defer cancel()
	req := gce_api.InstanceGroupManagersCreateInstancesRequest{}
	client.recommendationApplier.ApplyRecommendation(&req, recommendation)
	instanceNames := instanceIdsToNamesMap(existingInstanceProviderIds)
	req.Instances = make([]*gce_api.PerInstanceConfig, 0, delta)
	createdIds := make([]string, delta)
	for i := range delta {
		newInstanceName := generateInstanceName(baseName, instanceNames)
		instanceNames[newInstanceName] = true
		req.Instances = append(req.Instances, &gce_api.PerInstanceConfig{Name: newInstanceName})
		ref := gce.GceRef{Project: migRef.Project, Zone: migRef.Zone, Name: newInstanceName}
		createdIds[i] = ref.ToProviderId()
	}

	start := time.Now()
	op, err := client.gceService.InstanceGroupManagers.CreateInstances(migRef.Project, migRef.Zone, migRef.Name, &req).Context(ctx).Do()
	gke_metrics.EmitGceLatency("instance_group_managers", "create_instances", op, err, start)
	if err != nil {
		return nil, err
	}
	return createdIds, client.WaitForOperation(context.TODO(), op.Name, op.OperationType, migRef.Project, migRef.Zone)
}

func instanceIdsToNamesMap(instanceProviderIds []string) map[string]bool {
	instanceNames := make(map[string]bool, len(instanceProviderIds))
	for _, inst := range instanceProviderIds {
		ref, err := gce.GceRefFromProviderId(inst)
		if err != nil {
			klog.Warningf("Failed to extract instance name from %q: %v", inst, err)
		} else {
			inst = ref.Name
		}
		instanceNames[inst] = true
	}
	return instanceNames
}

func generateInstanceName(baseName string, existingNames map[string]bool) string {
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("%v-%v", baseName, rand.String(4))
		if ok, _ := existingNames[name]; !ok {
			return name
		}
	}
	klog.Warning("Unable to create unique name for a new instance, duplicate name might occur")
	name := fmt.Sprintf("%v-%v", baseName, rand.String(4))
	return name
}

func (client *autoscalingInternalGceClient) FetchAcceleratorTypes(zone string) (*gce_api.AcceleratorTypeList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.operationPerCallTimeout)
	defer cancel()
	start := time.Now()
	resp, err := client.gceService.AcceleratorTypes.List(client.projectID, zone).Context(ctx).Do()
	gke_metrics.EmitGceLatency("accelerator_types", "list", resp, err, start)
	return resp, err
}

func (client *autoscalingInternalGceClient) FetchMigInstances(ctx context.Context, migRef gce.GceRef) ([]gce.GceInstance, error) {
	ignoreStockouts, capacityCheckTimeoutExpired := client.ignoreInstanceCreationStockoutErrors(migRef)
	b := newInstanceListBuilder(migRef, client.migInfoProvider.QueuedProvisioning(migRef), ignoreStockouts, capacityCheckTimeoutExpired)
	return fetchManagedInstances(ctx, client, b, migRef, "")
}

func (client *autoscalingInternalGceClient) FetchFutureReservationsInProject(projectID string) ([]*GceFutureReservation, error) {
	frs := make([]*GceFutureReservation, 0)
	call := client.gceBetaService.FutureReservations.AggregatedList(projectID)
	start := time.Now()
	lastRequestStart := start
	err := call.Pages(context.TODO(), func(ls *gce_api_beta.FutureReservationsAggregatedListResponse) error {
		gke_metrics.EmitGceLatency("future_reservations", "aggregated_list_page", ls, nil, lastRequestStart)
		for _, items := range ls.Items {
			for _, item := range items.FutureReservations {
				fr, err := toGceFutureReservation(item)
				if err != nil {
					klog.Warningf("Failed to convert GCE future reservation name=%s, id=%d to domain object: %v", item.Name, item.Id, err)
					continue
				}
				frs = append(frs, fr)
			}
		}
		lastRequestStart = time.Now()
		return nil
	})
	gke_metrics.EmitGceLatency("future_reservations", "aggregated_list", nil, err, start)
	if err != nil {
		gke_metrics.EmitGceLatency("future_reservations", "aggregated_list_page", nil, err, lastRequestStart)
	}
	return frs, err
}

// ignoreInstanceCreationStockoutErrors returns (ignoreStockouts, capacityCheckTimeoutExpired).
// - ignoreStockouts indicates whether stockout errors should be ignored for this MIG.
// - capacityCheckTimeoutExpired indicates whether the MIG's capacity check wait time has elapsed.
func (client *autoscalingInternalGceClient) ignoreInstanceCreationStockoutErrors(migRef gce.GceRef) (bool, bool) {
	if client.experimentsManager == nil {
		return false, true
	}
	if !client.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag, false) {
		return false, true
	}
	if !client.migInfoProvider.FlexStartNonQueued(migRef) {
		return false, true
	}
	ccwt, err := client.migInfoProvider.CapacityCheckWaitTimeSeconds(migRef)
	if err != nil {
		return true, true
	}
	scaleUpTime, err := client.migInfoProvider.ScaleUpTime(migRef)
	if err != nil {
		return true, true
	}
	return true, time.Now().After(scaleUpTime.Add(ccwt))
}

type listBuilder[T any] interface {
	loadPage(page *gce_api_beta.InstanceGroupManagersListManagedInstancesResponse) error
	build() []T
}

func fetchManagedInstances[T any](ctx context.Context, client *autoscalingInternalGceClient, b listBuilder[T], migRef gce.GceRef, filter string) ([]T, error) {
	start := time.Now()
	lastRequestStart := start
	call := client.gceBetaService.InstanceGroupManagers.ListManagedInstances(migRef.Project, migRef.Zone, migRef.Name)
	if filter != "" {
		call = call.Filter(filter)
	}
	err := call.Pages(
		ctx,
		func(page *gce_api_beta.InstanceGroupManagersListManagedInstancesResponse) error {
			gke_metrics.EmitGceLatency("instance_group_managers", "list_managed_instances_page", page, nil, lastRequestStart)
			err := b.loadPage(page)
			lastRequestStart = time.Now()
			return err
		},
	)
	gke_metrics.EmitGceLatency("instance_group_managers", "list_managed_instances", nil, err, start)
	if err != nil {
		gke_metrics.EmitGceLatency("instance_group_managers", "list_managed_instances_page", nil, err, lastRequestStart)
		klog.V(4).Infof("Failed MIG info request for %s %s %s: %v", migRef.Project, migRef.Zone, migRef.Name, err)
		return nil, err
	}
	return b.build(), nil
}

type instanceListBuilder struct {
	migRef                      gce.GceRef
	ignoreStockouts             bool
	capacityCheckTimeoutExpired bool
	queuedProvisioning          bool
	errorCodeCounts             map[string]int
	errorLoggingQuota           *klogx.Quota
	infos                       []gce.GceInstance
}

func newInstanceListBuilder(migRef gce.GceRef, queuedProvisioning, ignoreStockouts, capacityCheckTimeoutExpired bool) *instanceListBuilder {
	return &instanceListBuilder{
		migRef:                      migRef,
		queuedProvisioning:          queuedProvisioning,
		ignoreStockouts:             ignoreStockouts,
		capacityCheckTimeoutExpired: capacityCheckTimeoutExpired,
		errorCodeCounts:             make(map[string]int),
		errorLoggingQuota:           klogx.NewLoggingQuota(100),
	}
}

func (i *instanceListBuilder) loadPage(page *gce_api_beta.InstanceGroupManagersListManagedInstancesResponse) error {
	if i.infos == nil {
		i.infos = make([]gce.GceInstance, 0, len(page.ManagedInstances))
	}
	for _, gceInstance := range page.ManagedInstances {
		instance := i.gceBetaInstanceToInstance(gceInstance)
		i.infos = append(i.infos, instance)
	}
	return nil
}

func (i *instanceListBuilder) gceBetaInstanceToInstance(gceInstance *gce_api_beta.ManagedInstance) gce.GceInstance {
	ref := gce.GceRef{
		Project: i.migRef.Project,
		Zone:    i.migRef.Zone,
		Name:    gceInstance.Name,
	}

	instance := gce.GceInstance{
		Instance: cloudprovider.Instance{Id: ref.ToProviderId(),
			Status: &cloudprovider.InstanceStatus{
				State: getInstanceState(gceInstance.CurrentAction),
			},
		},
		GCEStatus: gceInstance.InstanceStatus,
		NumericId: gceInstance.Id,
	}

	if gceInstance.Version != nil {
		instanceTemplate, err := gce.InstanceTemplateNameFromUrl(gceInstance.Version.InstanceTemplate)
		if err == nil {
			instance.InstanceTemplateName = instanceTemplate.Name
		}
	}

	// For QueuedProvisioning MIGs we don't want to propagate the last attempt instance errors as VMs
	// requested via Provisioning Request shouldn't be treated as unhelpable and deleted even when there were errors.
	// These errors will be retrieved via the corresponding Resize Request instead.
	if instance.Status.State != cloudprovider.InstanceCreating || i.queuedProvisioning {
		return instance
	}

	var errorInfo *cloudprovider.InstanceErrorInfo
	errorMessages := []string{}
	lastAttemptErrors := getLastAttemptErrors(gceInstance)
	for _, instanceError := range lastAttemptErrors {
		i.errorCodeCounts[instanceError.Code]++
		if newErrorInfo := gce.GetErrorInfo(instanceError.Code, instanceError.Message, gceInstance.InstanceStatus, errorInfo); newErrorInfo != nil {
			// override older error
			errorInfo = newErrorInfo
		} else {
			// no error
			continue
		}
		if instanceError.Message != "" {
			errorMessages = append(errorMessages, instanceError.Message)
		}
	}

	if i.ignoreStockouts && isStockout(errorInfo) {
		if isProvisioned(gceInstance) {
			// Ignore stockout errors that have been reported on the instance before it was provisioned.
			return instance
		}
		if !i.capacityCheckTimeoutExpired {
			// For pending VMs, ignore stockout errors until the CapacityCheckWaitTime is expired.
			return instance
		}
	}

	if errorInfo != nil {
		errorInfo.ErrorMessage = strings.Join(errorMessages, "; ")
		errorInfo.ErrorCode = getGkeErrorCode(errorInfo)

		instance.Status.ErrorInfo = errorInfo
	}

	if len(lastAttemptErrors) > 0 {
		gceInstanceJSONBytes, err := gceInstance.MarshalJSON()
		var gceInstanceJSON string
		if err != nil {
			gceInstanceJSON = fmt.Sprintf("Got error from MarshalJSON; %v", err)
		} else {
			gceInstanceJSON = string(gceInstanceJSONBytes)
		}
		klogx.V(4).UpTo(i.errorLoggingQuota).Infof("Got GCE instance which is being created and has lastAttemptErrors; gceInstance=%v; errorInfo=%#v", gceInstanceJSON, errorInfo)
	}

	return instance
}

func isProvisioned(gceInstance *gce_api_beta.ManagedInstance) bool {
	return gceInstance != nil && (gceInstance.InstanceStatus == "STAGING" || gceInstance.InstanceStatus == "RUNNING")
}

func isStockout(errorInfo *cloudprovider.InstanceErrorInfo) bool {
	return errorInfo != nil && errorInfo.ErrorClass == cloudprovider.OutOfResourcesErrorClass
}

func getGkeErrorCode(errorInfo *cloudprovider.InstanceErrorInfo) string {
	if requireShieldedVmConstraint.MatchString(errorInfo.ErrorMessage) {
		return GkePersistentOperationError
	}
	return errorInfo.ErrorCode
}

func (i *instanceListBuilder) build() []gce.GceInstance {
	klogx.V(4).Over(i.errorLoggingQuota).Infof("Got %v other GCE instances being created with lastAttemptErrors", -i.errorLoggingQuota.Left())
	if len(i.errorCodeCounts) > 0 {
		klog.Warningf("Spotted following instance creation error codes: %#v", i.errorCodeCounts)
	}
	return i.infos
}

// FetchAllInstances fetches all GceInstances of the cluster in the project in the zone
func (client *autoscalingInternalGceClient) FetchAllInstances(ctx context.Context, project, zone, _ string) ([]gce.GceInstance, error) {
	instances, err := client.AutoscalingGceClient.FetchAllInstances(ctx, project, zone, fmt.Sprintf("labels.goog-k8s-cluster-name=%s", client.clusterName))
	return instances, err
}

func (client *autoscalingInternalGceClient) FetchReservationBlocksInReservation(reservationRef ReservationRef) ([]*GceReservationBlock, error) {
	rsbs := make([]*GceReservationBlock, 0)
	call := client.gceService.ReservationBlocks.List(reservationRef.Project, reservationRef.Zone, reservationRef.Name)
	ctx, cancel := context.WithTimeout(context.Background(), defaultAPIOperationTimeout)
	defer cancel()
	start := time.Now()
	lastRequestStart := start
	err := call.Pages(ctx, func(ls *gce_api.ReservationBlocksListResponse) error {
		gke_metrics.EmitGceLatency("reservation_blocks", "list_page", ls, nil, lastRequestStart)
		for _, item := range ls.Items {
			rb, err := toGceReservationBlock(item)
			if err != nil {
				klog.V(5).Infof("Failed to convert GCE reservation block name=%s, id=%d, parent=%s to domain object: %v", item.Name, item.Id, reservationRef.Name, err)
				continue
			}
			rsbs = append(rsbs, rb)
		}
		lastRequestStart = time.Now()
		return nil
	})
	gke_metrics.EmitGceLatency("reservation_blocks", "list", nil, err, start)
	if err != nil {
		gke_metrics.EmitGceLatency("reservation_blocks", "list_page", nil, err, lastRequestStart)
		return nil, fmt.Errorf("failed to fetch reservation blocks: %w", err)
	}
	return rsbs, nil
}

func (client *autoscalingInternalGceClient) FetchReservationSubBlocksInReservationBlock(reservationRef ReservationRef) ([]*GceReservationSubBlock, error) {
	sbs := make([]*GceReservationSubBlock, 0)
	parent := fmt.Sprintf("reservations/%s/reservationBlocks/%s", reservationRef.Name, reservationRef.BlockName)
	call := client.gceBetaService.ReservationSubBlocks.List(reservationRef.Project, reservationRef.Zone, parent)
	ctx, cancel := context.WithTimeout(context.Background(), defaultAPIOperationTimeout)
	defer cancel()
	start := time.Now()
	lastRequestStart := start
	err := call.Pages(ctx, func(ls *gce_api_beta.ReservationSubBlocksListResponse) error {
		gke_metrics.EmitGceLatency("reservation_sub_blocks", "list_page", ls, nil, lastRequestStart)
		for _, item := range ls.Items {
			sb, err := toGceReservationSubBlock(item)
			if err != nil {
				klog.V(5).Infof("Failed to convert GCE reservation subblock name=%s, id=%d, parent=%s to domain object: %v", item.Name, item.Id, parent, err)
				continue
			}
			sbs = append(sbs, sb)
		}
		lastRequestStart = time.Now()
		return nil
	})
	gke_metrics.EmitGceLatency("reservation_sub_blocks", "list", nil, err, start)
	if err != nil {
		gke_metrics.EmitGceLatency("reservation_sub_blocks", "list_page", nil, err, lastRequestStart)
		return nil, fmt.Errorf("failed to fetch reservation subblocks: %w", err)
	}
	return sbs, nil
}

func (client *autoscalingInternalGceClient) FetchResourcePolicies(projectId, region string) ([]*GceResourcePolicy, error) {
	rps := make([]*GceResourcePolicy, 0)
	call := client.gceBetaService.ResourcePolicies.List(projectId, region)
	ctx, cancel := context.WithTimeout(context.Background(), defaultAPIOperationTimeout)
	defer cancel()
	start := time.Now()
	lastRequestStart := start
	err := call.Pages(ctx, func(ls *gce_api_beta.ResourcePolicyList) error {
		gke_metrics.EmitGceLatency("resource_policies", "list_page", ls, nil, lastRequestStart)
		for _, item := range ls.Items {
			rp, err := toGceResourcePolicy(item)
			if err != nil {
				klog.V(5).Infof("Failed to convert GCE resource policy name=%s, id=%d to domain object: %v", item.Name, item.Id, err)
				continue
			}
			rps = append(rps, rp)
		}
		lastRequestStart = time.Now()
		return nil
	})
	gke_metrics.EmitGceLatency("resource_policies", "list", nil, err, start)
	if err != nil {
		gke_metrics.EmitGceLatency("resource_policies", "list_page", nil, err, lastRequestStart)
		return nil, fmt.Errorf("failed to fetch resource policies: %w", err)
	}
	return rps, nil
}

func (client *autoscalingInternalGceClient) FetchNetwork(projectId, name string) (*gce_api.Network, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationPerCallTimeout)
	defer cancel()
	start := time.Now()
	network, err := client.gceService.Networks.Get(projectId, name).Context(ctx).Do()
	gke_metrics.EmitGceLatency("networks", "get", network, err, start)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch network project=%s, name=%s: %w", projectId, name, err)
	}
	return network, nil
}

func (client *autoscalingInternalGceClient) ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error {
	ctx, cancel := context.WithTimeout(context.Background(), client.operationPerCallTimeout)
	defer cancel()

	start := time.Now()
	req := gce_api.InstanceGroupManagersResumeInstancesRequest{}
	for _, inst := range instances {
		req.Instances = append(req.Instances, gce.GenerateInstanceUrl(client.domainUrl, inst))
	}

	op, err := client.gceService.InstanceGroupManagers.ResumeInstances(migRef.Project, migRef.Zone, migRef.Name, &req).Context(ctx).Do()
	gke_metrics.EmitGceLatency("instance_group_managers", "resume_instances", op, err, start)
	if err != nil {
		return fmt.Errorf("failed to call ResumeInstances for mig %q: %v", migRef.String(), err)
	}
	err = client.WaitForOperation(context.TODO(), op.Name, op.OperationType, migRef.Project, migRef.Zone)
	gke_metrics.EmitGceLatency("instance_group_managers", "resume_instances_polling", nil, err, start)
	if err != nil {
		return fmt.Errorf("failed to wait for ResumeInstances operation %s for mig %q: %v", op.Name, migRef.String(), err)
	}
	return nil
}

func (client *autoscalingInternalGceClient) SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), client.operationPerCallTimeout)
	defer cancel()

	start := time.Now()
	req := gce_api.InstanceGroupManagersSuspendInstancesRequest{ForceSuspend: forceSuspend}
	for _, inst := range instances {
		req.Instances = append(req.Instances, gce.GenerateInstanceUrl(client.domainUrl, inst))
	}

	op, err := client.gceService.InstanceGroupManagers.SuspendInstances(migRef.Project, migRef.Zone, migRef.Name, &req).Context(ctx).Do()
	gke_metrics.EmitGceLatency("instance_group_managers", "suspend_instances", op, err, start)
	if err != nil {
		return fmt.Errorf("failed to call SuspendInstances for mig %q: %v", migRef.String(), err)
	}
	err = client.WaitForOperation(context.TODO(), op.Name, op.OperationType, migRef.Project, migRef.Zone)
	gke_metrics.EmitGceLatency("instance_group_managers", "suspend_instances_polling", nil, err, start)
	if err != nil {
		return fmt.Errorf("failed to wait for SuspendInstances operation %s for mig %q: %v", op.Name, migRef.String(), err)
	}
	return nil
}

// instanceActionTimeout returns the timeout for waiting for the given instance action (e.g. RESUMING or SUSPENDING) to finish.
// It evaluates Giraffe flags (ColdStandbyNodes::ResumeTimeoutSeconds, ColdStandbyNodes::SuspendTimeoutSeconds)
// and falls back to DefaultResumeInstanceActionTimeout (5m) or DefaultSuspendInstanceActionTimeout (20m).
func (client *autoscalingInternalGceClient) instanceActionTimeout(action InstanceAction) time.Duration {
	switch action {
	case ActionResuming:
		if client.experimentsManager != nil {
			timeout := client.experimentsManager.EvaluateDurationSecondsFlagOrFailsafe(
				experiments.ColdStandbyNodesResumeTimeoutSecondsFlag, DefaultResumeInstanceActionTimeout)
			if timeout > 0 {
				return timeout
			}
		}
		return DefaultResumeInstanceActionTimeout
	case ActionSuspending:
		if client.experimentsManager != nil {
			timeout := client.experimentsManager.EvaluateDurationSecondsFlagOrFailsafe(
				experiments.ColdStandbyNodesSuspendTimeoutSecondsFlag, DefaultSuspendInstanceActionTimeout)
			if timeout > 0 {
				return timeout
			}
		}
		return DefaultSuspendInstanceActionTimeout
	default:
		return DefaultResumeInstanceActionTimeout
	}
}

// PollUntilActionStops polls instances until each one has stopped running action, the poll
// times out, or ctx is cancelled. An update is yielded only when something happened to an
// instance: see PollUpdate for the three outcomes and for how often each of them is reported.
func (client *autoscalingInternalGceClient) PollUntilActionStops(ctx context.Context, action InstanceAction, migRef gce.GceRef, instanceRefs []gce.GceRef) ActionPollSeq {
	waitEnabled := client.experimentsManager != nil && client.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesWaitForInstanceStatus)
	if len(instanceRefs) == 0 || !waitEnabled {
		// There is nothing to wait for, or waiting is disabled: optimistically treat every
		// instance as done. On an empty slice this reports nothing at all.
		return AllReady(instanceRefs)
	}

	return func(yield func(gce.GceRef, PollUpdate) bool) {
		start := time.Now()

		// Instances are dropped from pending as soon as they are reported ready, so it doubles as
		// the set still being waited on and as the set to report as failed when waiting stops.
		pending := make(map[string]gce.GceRef, len(instanceRefs))
		for _, inst := range instanceRefs {
			pending[inst.Name] = inst
		}

		// Deriving the timeout from ctx puts the deadline and the caller's cancellation on a
		// single termination path, and makes ctx.Err() tell the two apart. This also makes the
		// timeout errors emitted as timeout instead of internal_error.
		ctx, cancel := context.WithTimeout(ctx, client.instanceActionTimeout(action))
		defer cancel()

		pollTimer := time.NewTimer(client.instanceActionPollingFrequency)
		defer pollTimer.Stop()

		shouldReport := newNonBlockingErrorDeduper()

		for {
			select {
			case <-ctx.Done():
				for _, ref := range pending {
					// Wrapping ctx.Err() keeps the cause recognizable by errors.Is, which
					// is what the latency metric relies on to report a timeout as such
					// rather than as an unclassified internal error.
					err := fmt.Errorf("stopped waiting for instance %v to finish action %q: %w", ref, action, ctx.Err())
					gke_metrics.EmitGceLatency("instance_group_managers", actionPollingVerb(action), nil, err, start)
					if !yield(ref, PollAborted{Err: err}) {
						return
					}
				}
				return
			case <-pollTimer.C:
				stopped, err := client.reapFinishedInstances(ctx, action, migRef, pending, shouldReport, start, yield)
				if stopped {
					return
				}
				if err != nil {
					// A failed poll is transient: keep polling until ctx is done.
					klog.Errorf("Fetching instances for MIG %v failed: %v", migRef, err)
				}
				if len(pending) == 0 {
					return
				}
				pollTimer.Reset(client.instanceActionPollingFrequency)
			}
		}
	}
}

// reapFinishedInstances polls the MIG once and reports every instance of pending that is no longer
// running action as PollCompleted, removing it from pending, plus a PollRetrying update for every
// recoverable failure shouldReport lets through for the instances that are still running it.
// pollStart is when the wait began, and is used to measure the latency of each instance that
// stopped running action.
//
// It reports stopped=true when the consumer abandoned the iteration, in which case the caller must
// stop polling immediately and yield nothing more.
func (client *autoscalingInternalGceClient) reapFinishedInstances(ctx context.Context, action InstanceAction, migRef gce.GceRef, pending map[string]gce.GceRef, shouldReport func(ref gce.GceRef, code, msg string) bool, pollStart time.Time, yield func(gce.GceRef, PollUpdate) bool) (stopped bool, err error) {
	var filter string
	if action != "" {
		filter = fmt.Sprintf("currentAction = %s", action)
	}
	instances, err := fetchManagedInstances(ctx, client, newIdentityListBuilder(), migRef, filter)
	if err != nil {
		return false, fmt.Errorf("failed to fetch managed instances for MIG %v: %w", migRef, err)
	}
	instancesByName := make(map[string]*gce_api_beta.ManagedInstance, len(instances))
	for _, inst := range instances {
		if inst != nil {
			instancesByName[inst.Name] = inst
		}
	}

	// Deleting from a map while ranging over it is safe: deleted entries are simply not produced.
	for name, targetInst := range pending {
		inst, found := instancesByName[name]
		// A missing instance was most likely deleted or finished the action, so treat it as done.
		// Any errors from its last attempt are stale by now and deliberately not reported: an
		// instance that made it is not worth backing off.
		if !found || inst.CurrentAction != string(action) {
			delete(pending, name)
			gke_metrics.EmitGceLatency("instance_group_managers", actionPollingVerb(action), nil, nil, pollStart)
			if !yield(targetInst, PollCompleted{}) {
				return true, nil
			}
			continue
		}
		// inst.LastAttempt.Errors holds the non-blocking errors (e.g., stockout, quota issues, or
		// instance configuration failures) of the MIG's most recent attempt on the instance,
		// allowing consumers to track or back off failing instances. GCE keeps retrying the action
		// until success or deletion of the node.
		for _, e := range getLastAttemptErrors(inst) {
			if !shouldReport(targetInst, e.Code, e.Message) {
				continue
			}
			nonBlockingErr := &NonBlockingInstanceError{
				Code:           e.Code,
				Message:        e.Message,
				InstanceStatus: inst.InstanceStatus,
			}
			if !yield(targetInst, PollRetrying{Err: nonBlockingErr}) {
				return true, nil
			}
		}
	}
	return false, nil
}

type deduperKey struct {
	name, code, msg string
}

// newNonBlockingErrorDeduper returns a predicate that lets an identical error for the same instance
// through at most once per nonBlockingErrorReportInterval. Without it every poll would re-report
// the same error for every failing instance, which may congest the consumer's mutexes. The returned
// predicate is stateful and not safe for concurrent use.
func newNonBlockingErrorDeduper() func(ref gce.GceRef, code, msg string) bool {
	lastReportTime := make(map[deduperKey]time.Time)
	return func(ref gce.GceRef, code, msg string) bool {
		key := deduperKey{name: ref.Name, code: code, msg: msg}
		if last, exists := lastReportTime[key]; exists && time.Since(last) < nonBlockingErrorReportInterval {
			return false
		}
		lastReportTime[key] = time.Now()
		return true
	}
}

type identityListBuilder struct {
	infos []*gce_api_beta.ManagedInstance
}

func newIdentityListBuilder() *identityListBuilder {
	return &identityListBuilder{}
}

func (i *identityListBuilder) loadPage(page *gce_api_beta.InstanceGroupManagersListManagedInstancesResponse) error {
	if i.infos == nil {
		i.infos = make([]*gce_api_beta.ManagedInstance, 0, len(page.ManagedInstances))
	}
	for _, gceInstance := range page.ManagedInstances {
		i.infos = append(i.infos, gceInstance)
	}
	return nil
}

func (i *identityListBuilder) build() []*gce_api_beta.ManagedInstance {
	return i.infos
}

type managedInstanceListBuilder struct {
	instances []*ManagedInstance
}

func newManagedInstanceListBuilder() *managedInstanceListBuilder {
	return &managedInstanceListBuilder{}
}

func (b *managedInstanceListBuilder) loadPage(page *gce_api_beta.InstanceGroupManagersListManagedInstancesResponse) error {
	if b.instances == nil {
		b.instances = make([]*ManagedInstance, 0, len(page.ManagedInstances))
	}
	for _, inst := range page.ManagedInstances {
		if inst == nil {
			continue
		}
		b.instances = append(b.instances, &ManagedInstance{
			Name:           inst.Name,
			InstanceStatus: inst.InstanceStatus,
			TargetStatus:   inst.TargetStatus,
			CurrentAction:  inst.CurrentAction,
		})
	}
	return nil
}

func (b *managedInstanceListBuilder) build() []*ManagedInstance {
	return b.instances
}

func (client *autoscalingInternalGceClient) FetchManagedInstances(migRef gce.GceRef, filter string) ([]*ManagedInstance, error) {
	return fetchManagedInstances(context.Background(), client, newManagedInstanceListBuilder(), migRef, filter)
}

func (client *autoscalingInternalGceClient) FetchStandardZones(region string) ([]string, error) {
	filter := fmt.Sprintf(`(name ne ".*-ai.*") (region eq ".*%s")`, region)
	return client.fetchZones(filter)
}

func (client *autoscalingInternalGceClient) FetchAIZones(region string) ([]string, error) {
	filter := fmt.Sprintf(`(name eq ".*-ai.*") (region eq ".*%s")`, region)
	return client.fetchZones(filter)
}

func (client *autoscalingInternalGceClient) fetchZones(filter string) ([]string, error) {
	zones := make([]string, 0)
	ctx, cancel := context.WithTimeout(context.Background(), defaultAPIOperationTimeout)
	defer cancel()
	req := client.gceService.Zones.List(client.projectID).Filter(filter)
	err := req.Pages(ctx, func(page *gce_api.ZoneList) error {
		for _, z := range page.Items {
			zones = append(zones, z.Name)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch zones: %w", err)
	}
	return zones, nil
}

func (client *autoscalingInternalGceClient) GetHttpTimeout() time.Duration {
	return client.httpTimeout
}

func getInstanceState(currentAction string) cloudprovider.InstanceState {
	switch currentAction {
	case "CREATING", "RECREATING", "CREATING_WITHOUT_RETRIES", "QUEUING":
		return cloudprovider.InstanceCreating
	case "ABANDONING", "DELETING":
		return cloudprovider.InstanceDeleting
	default:
		return cloudprovider.InstanceRunning
	}
}

func getLastAttemptErrors(instance *gce_api_beta.ManagedInstance) []*gce_api_beta.ManagedInstanceLastAttemptErrorsErrors {
	if instance.LastAttempt != nil && instance.LastAttempt.Errors != nil {
		return instance.LastAttempt.Errors.Errors
	}
	return nil
}
