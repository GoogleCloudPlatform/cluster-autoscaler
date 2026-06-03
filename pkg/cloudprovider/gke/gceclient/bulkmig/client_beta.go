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

package bulkmig

import (
	"context"
	"fmt"
	"net/http"
	"time"

	gce_api_beta "google.golang.org/api/compute/v0.beta"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	rrclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	klog "k8s.io/klog/v2"
)

const (
	defaultOperationWaitTimeout = 30 * time.Second
)

// TODO(b/433668084): deprecate bulkMigClientBeta once igm.Status.BulkInstanceOperation is in V1
type GceMigClient interface {
	BulkMigStatus(migRef gce.GceRef) (Status, error)
	SetZeroTargetSize(migRef gce.GceRef) error
}

// For Mig resizing, we can use existing methods as we don't require Beta API there.
type gceMigClient interface {
	ResizeMig(gce.GceRef, int64) error
}

// For Mig resizing, we need to update the cache.
type GceMigCache interface {
	InvalidateMigTargetSize(migRef gce.GceRef)
	SetMigTargetSize(migRef gce.GceRef, size int64)
}

type bulkMigClientBeta struct {
	migClient            gceMigClient
	migCache             GceMigCache
	gceBetaService       *gce_api_beta.Service
	operationWaitTimeout time.Duration
}

// NewBulkMigClientBeta creates a new client to communicate with MIG API to retrieve `bulkInstanceOperation` data.
// If gceEndpoint is not empty the base path is overridden.
func NewBulkMigClientBeta(client *http.Client, projectID, userAgent, gceEndpoint string, migClient gceMigClient, migCache GceMigCache) (*bulkMigClientBeta, error) {
	if migClient == nil {
		return nil, fmt.Errorf("Cannot create bulkMigClientBeta: autoscalingGceClient used for resizing MIGs was nil")
	}
	gceBetaService, err := gce_api_beta.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	gceBetaService.UserAgent = userAgent
	if len(gceEndpoint) > 0 {
		gceBetaService.BasePath = gceEndpoint
	}

	return &bulkMigClientBeta{
		migClient:            migClient,
		migCache:             migCache,
		gceBetaService:       gceBetaService,
		operationWaitTimeout: defaultOperationWaitTimeout,
	}, nil
}

// Status contains `bulkInstanceOperation` data corresponding to the MIG.
// This data is used to check the status of MIG's Bulk provisioning.
type Status struct {
	// ID: A unique identifier for this resource.
	ID uint64
	// Ref: MIG GCE reference consisting of project, name and zone.
	Ref gce.GceRef
	// InProgress: Informs whether bulk instance operation is in progress.
	InProgress bool
	// LastProgressCheckErrors: Errors encountered during bulk instance operation.
	LastProgressCheckErrors []rrclient.DwsStatusError
	// LastProgressCheckTimestamp: Timestamp of the last progress check of bulk instance operation.
	LastProgressCheckTimestamp time.Time
	// TargetSize: The target number of instances.
	TargetSize int64
}

// BulkMigStatus gets the status of the BulkInstanceOperation for the MIG.
// If MIG doesn't use Bulk provisioning to scale up, `BulkInstanceOperation` will be nil and thus error will be returned.
func (client *bulkMigClientBeta) BulkMigStatus(migRef gce.GceRef) (Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.operationWaitTimeout)
	defer cancel()
	start := time.Now()
	igm, err := client.gceBetaService.InstanceGroupManagers.Get(migRef.Project, migRef.Zone, migRef.Name).Context(ctx).Do()
	gke_metrics.EmitGceLatency("instance_group_managers", "get", igm, err, start)
	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			if err.Code == http.StatusNotFound {
				return Status{}, errors.NewAutoscalerError(errors.NodeGroupDoesNotExistError, err.Error())
			}
		}
		return Status{}, err
	}

	return bulkMigStatusFromGCEBetaIGM(igm, migRef)
}

// SetZeroTargetSize cancels the bulkMig queueing for capacity by setting Mig target size to 0.
func (client *bulkMigClientBeta) SetZeroTargetSize(migRef gce.GceRef) error {
	klog.V(0).Infof("Setting Bulk Mig %s size to 0", migRef)
	client.migCache.InvalidateMigTargetSize(migRef)
	err := client.migClient.ResizeMig(migRef, 0)
	if err != nil {
		return err
	}
	client.migCache.SetMigTargetSize(migRef, 0)
	return nil
}

func bulkMigStatusFromGCEBetaIGM(igm *gce_api_beta.InstanceGroupManager, migRef gce.GceRef) (Status, error) {
	if igm == nil || igm.Status == nil || igm.Status.BulkInstanceOperation == nil {
		return Status{}, fmt.Errorf("cannot resolve BulkMigStatus for mig %s, got nil BulkInstanceOperation", migRef)
	}

	var lastProgressCheckTimestamp time.Time
	var err error
	if igm.Status.BulkInstanceOperation.LastProgressCheck != nil && igm.Status.BulkInstanceOperation.LastProgressCheck.Timestamp != "" {
		lastProgressCheckTimestamp, err = time.Parse(time.RFC3339, igm.Status.BulkInstanceOperation.LastProgressCheck.Timestamp)
		if err != nil {
			return Status{}, fmt.Errorf("while time.Parse(LastProgressCheckTimestamp) got error: %w", err)
		}
	}

	return Status{
		ID:                         igm.Id,
		Ref:                        migRef,
		InProgress:                 igm.Status.BulkInstanceOperation.InProgress,
		LastProgressCheckErrors:    lastProgressCheckErrorsGCEBeta(igm.Status.BulkInstanceOperation.LastProgressCheck),
		LastProgressCheckTimestamp: lastProgressCheckTimestamp,
		TargetSize:                 igm.TargetSize + igm.TargetSuspendedSize,
	}, nil
}

func lastProgressCheckErrorsGCEBeta(gceErrors *gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperationLastProgressCheck) []rrclient.DwsStatusError {
	if gceErrors == nil || gceErrors.Error == nil || len(gceErrors.Error.Errors) == 0 {
		return nil
	}
	result := make([]rrclient.DwsStatusError, 0, len(gceErrors.Error.Errors))
	for _, e := range gceErrors.Error.Errors {
		err := rrclient.DwsStatusError{
			Code:    e.Code,
			Message: e.Message,
		}
		if rrclient.IsResourcePoolExhaustedErrorCode(e.Code) && len(e.ErrorDetails) > 0 {
			errorInfo := e.ErrorDetails[0].ErrorInfo
			if errorInfo != nil && errorInfo.Metadatas != nil && len(errorInfo.Reason) > 0 {
				details := fmt.Sprintf("Reason: %q, VMType: %q, Attachment: %q.", errorInfo.Reason, errorInfo.Metadatas["vmType"], errorInfo.Metadatas["attachment"])
				err.Message = err.Message + " " + details
			}
		}
		result = append(result, err)
	}
	return result
}
