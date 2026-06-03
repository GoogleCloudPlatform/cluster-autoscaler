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
	"net/http"
	"regexp"
	"strings"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"

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
	instanceActionTimeout          = 60 * time.Minute
)

var (
	requireShieldedVmConstraint = regexp.MustCompile("Constraint constraints/compute.requireShieldedVm violated")
)

// AutoscalingInternalGceClient is used for communicating with GCE API
// it wraps the OSS AutoscalingGceClient and provides additional methods.
type AutoscalingInternalGceClient interface {
	gce.AutoscalingGceClient

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

	// ResumeInstances resumes instances
	ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error
	// SuspendInstances suspends instances
	SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error
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
	instanceActionTimeout          time.Duration
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

// WithInstanceActionTimeout sets the timeout of waiting for completion of
// an action that changes the status of a GCE instance.
func WithInstanceActionTimeout(d time.Duration) Option {
	return func(c *autoscalingInternalGceClient) {
		if d > 0 {
			c.instanceActionTimeout = d
		}
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
		instanceActionTimeout:          instanceActionTimeout,
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
		instanceActionTimeout:          instanceActionTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (client *autoscalingInternalGceClient) FetchAcceleratorTypes(zone string) (*gce_api.AcceleratorTypeList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.operationPerCallTimeout)
	defer cancel()
	start := time.Now()
	resp, err := client.gceService.AcceleratorTypes.List(client.projectID, zone).Context(ctx).Do()
	gke_metrics.EmitGceLatency("accelerator_types", "list", resp, err, start)
	return resp, err
}

func (client *autoscalingInternalGceClient) FetchMigInstances(migRef gce.GceRef) ([]gce.GceInstance, error) {
	ignoreStockouts, capacityCheckTimeoutExpired := client.ignoreInstanceCreationStockoutErrors(migRef)
	b := newInstanceListBuilder(migRef, client.migInfoProvider.QueuedProvisioning(migRef), ignoreStockouts, capacityCheckTimeoutExpired)
	return fetchMigInstancesBeta[gce.GceInstance](client, b, migRef)
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

func fetchMigInstancesBeta[T any](client *autoscalingInternalGceClient, b listBuilder[T], migRef gce.GceRef) ([]T, error) {
	start := time.Now()
	lastRequestStart := start
	err := client.gceBetaService.InstanceGroupManagers.ListManagedInstances(migRef.Project, migRef.Zone, migRef.Name).Pages(
		context.Background(),
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
func (client *autoscalingInternalGceClient) FetchAllInstances(project, zone, _ string) ([]gce.GceInstance, error) {
	instances, err := client.AutoscalingGceClient.FetchAllInstances(project, zone, fmt.Sprintf("labels.goog-k8s-cluster-name=%s", client.clusterName))
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
	err = client.WaitForOperation(op.Name, op.OperationType, migRef.Project, migRef.Zone)
	gke_metrics.EmitGceLatency("instance_group_managers", "resume_instances_polling", nil, err, start)
	if err != nil {
		return fmt.Errorf("failed to wait for ResumeInstances operation %s for mig %q: %v", op.Name, migRef.String(), err)
	}
	err = client.waitForActionToStopRunning("RESUMING", migRef, instances)
	gke_metrics.EmitGceLatency("instance_group_managers", "resume_instances_action_polling", nil, err, start)
	if err != nil {
		return fmt.Errorf("failed to wait for instances %v to have status RUNNING: %w", instances, err)
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
	err = client.WaitForOperation(op.Name, op.OperationType, migRef.Project, migRef.Zone)
	gke_metrics.EmitGceLatency("instance_group_managers", "suspend_instances_polling", nil, err, start)
	if err != nil {
		return fmt.Errorf("failed to wait for SuspendInstances operation %s for mig %q: %v", op.Name, migRef.String(), err)
	}
	err = client.waitForActionToStopRunning("SUSPENDING", migRef, instances)
	gke_metrics.EmitGceLatency("instance_group_managers", "suspend_instances_action_polling", nil, err, start)
	if err != nil {
		return fmt.Errorf("failed to wait for instances %v to have status SUSPENDED: %w", instances, err)
	}
	return nil
}

func (client *autoscalingInternalGceClient) waitForActionToStopRunning(action string, migRef gce.GceRef, instanceRefs []gce.GceRef) (err error) {
	if !client.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesWaitForInstanceStatus) {
		return nil
	}
	pollTimer := time.NewTimer(client.instanceActionPollingFrequency)
	timeout := time.NewTimer(client.instanceActionTimeout)
	defer func() {
		pollTimer.Stop()
		timeout.Stop()
	}()

	for {
		select {
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for instances %v to finish action %q", instanceRefs, action)
		case <-pollTimer.C:
			if client.actionFinishedForAllInstances(action, migRef, instanceRefs) {
				return nil
			}
			pollTimer.Reset(client.instanceActionPollingFrequency)
		}
	}
}

func (client *autoscalingInternalGceClient) actionFinishedForAllInstances(action string, migRef gce.GceRef, targetInstances []gce.GceRef) bool {
	instances, err := fetchMigInstancesBeta[*gce_api_beta.ManagedInstance](client, newIdentityListBuilder(), migRef)
	if err != nil {
		klog.Errorf("Fetching instances %v failed: %v", targetInstances, err)
		return false
	}
	// name -> instance mapping
	instancesMap := make(map[string]*gce_api_beta.ManagedInstance, len(instances))
	for _, inst := range instances {
		if inst != nil {
			instancesMap[inst.Name] = inst
		}
	}

	for _, targetInst := range targetInstances {
		inst, found := instancesMap[targetInst.Name]
		if !found {
			// If the instance is not found then it could have been deleted,
			// which is why it's not desired to return false in that case.
			continue
		}
		if inst.CurrentAction == action {
			return false
		}
	}
	return true
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
