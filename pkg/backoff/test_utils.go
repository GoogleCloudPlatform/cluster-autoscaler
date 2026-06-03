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

package backoff

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/context"

	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	npc_crd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
)

var quotaError = cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OutOfResourcesErrorClass, ErrorCode: gce.ErrorCodeQuotaExceeded, ErrorMessage: "Not enough CPU"}
var stockoutError = cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OutOfResourcesErrorClass, ErrorCode: gce.ErrorCodeResourcePoolExhausted, ErrorMessage: "CPU stockout error"}
var gkePersistentOperationError = cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: gceclient.GkePersistentOperationError, ErrorMessage: "A persistent error occurred in GKE operation"}
var reservationNotReadyError = cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: gce.ErrorReservationNotReady, ErrorMessage: "Reservation is not ready"}
var invalidReservationError = cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: gce.ErrorInvalidReservation, ErrorMessage: "Reservation is invalid"}
var reservationCapacityExceededError = cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: gce.ErrorReservationCapacityExceeded, ErrorMessage: "Reservation capacity exceeded"}
var randomError = cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: gce.ErrorCodeOther, ErrorMessage: "Random error without specified code"}

var noBackoff = backoff.Status{IsBackedOff: false}
var backoffWithQuotaError = backoff.Status{
	IsBackedOff: true,
	ErrorInfo:   quotaError,
}
var backoffWithStockoutError = backoff.Status{
	IsBackedOff: true,
	ErrorInfo:   stockoutError,
}
var backoffWithGkePersistentOperationError = backoff.Status{
	IsBackedOff: true,
	ErrorInfo:   gkePersistentOperationError,
}
var backoffWithReservationNotReady = backoff.Status{
	IsBackedOff: true,
	ErrorInfo:   reservationNotReadyError,
}
var backoffWithInvalidReservation = backoff.Status{
	IsBackedOff: true,
	ErrorInfo:   invalidReservationError,
}
var backoffWithReservationCapacityExceeded = backoff.Status{
	IsBackedOff: true,
	ErrorInfo:   reservationCapacityExceededError,
}
var backoffWithRandomError = backoff.Status{
	IsBackedOff: true,
	ErrorInfo:   randomError,
}

func testNodeGroup(id string, autoprovisioned bool, flex bool) cloudprovider.NodeGroup {
	gkeManager := gke.NewFakeGkeManagerBuilder().
		WithDataplaneV2Enabled(true).
		Build()
	spec := &gkeclient.NodePoolSpec{
		MachineType: "n1-standard-1",
		Labels:      map[string]string{"l1": "l1v"},
		FlexStart:   flex,
	}

	ref := gce.GceRef{
		Project: "chocolatebarsproject",
		Zone:    "us-central1-f",
		Name:    id,
	}
	return gke.NewTestGkeMigBuilder().
		SetGceRef(ref).
		SetMaxSize(10).
		SetAutoprovisioned(autoprovisioned).
		SetExist(true).
		SetNodePoolName(id).
		SetSpec(spec).
		SetGkeManager(gkeManager).
		SetDeploymentType(gke.DeploymentTypeNone).
		Build()
}

func ipSpaceExhaustedError(cidr string) cloudprovider.InstanceErrorInfo {
	return cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OtherErrorClass,
		ErrorCode:    gce.ErrorIPSpaceExhausted,
		ErrorMessage: fmt.Sprintf("Insufficient free IP addresses in the IP range '%s'", cidr),
	}
}

func backoffWithIPSpaceExhaustedError(cidr string) backoff.Status {
	return backoff.Status{
		IsBackedOff: true,
		ErrorInfo:   ipSpaceExhaustedError(cidr),
	}
}

func createGkeBackoff() CompositeBackoff {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	return NewGkeBackoff(Config{
		CustomResourceProcessor: processor,
		NpcLister:               npc_lister.NewMockCrdLister([]npc_crd.CRD{}),
	})
}
