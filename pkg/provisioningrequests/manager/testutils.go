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
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
)

type ProvReqManagerFake struct {
	rrs      map[string]map[string]resizerequestclient.ResizeRequestStatus
	bulkMigs map[gce.GceRef]*bulkmig.Status
	labels   map[gce.GceRef]map[string]string
	prs      map[prpods.ProvReqID]*provreqwrapper.ProvisioningRequest
}

func NewProvReqManagerFake(resizeRequests map[string][]resizerequestclient.ResizeRequestStatus, bulkMigs []gce.GceRef, provReqs []*provreqwrapper.ProvisioningRequest) *ProvReqManagerFake {
	rrs := map[string]map[string]resizerequestclient.ResizeRequestStatus{}
	for migName, migRRs := range resizeRequests {
		rrs[migName] = map[string]resizerequestclient.ResizeRequestStatus{}
		for _, rr := range migRRs {
			rrs[migName][rr.Name] = rr
		}
	}

	bmigs := map[gce.GceRef]*bulkmig.Status{}
	labels := map[gce.GceRef]map[string]string{}
	for _, migRef := range bulkMigs {
		bmigs[migRef] = &bulkmig.Status{
			Ref:        migRef,
			InProgress: false,
			TargetSize: 0,
		}
		labels[migRef] = map[string]string{}
	}

	prs := map[prpods.ProvReqID]*provreqwrapper.ProvisioningRequest{}
	for _, pr := range provReqs {
		prs[prpods.GetProvReqID(pr)] = pr
	}

	return &ProvReqManagerFake{
		rrs:      rrs,
		bulkMigs: bmigs,
		labels:   labels,
		prs:      prs,
	}
}

func (f *ProvReqManagerFake) CreateQueuedBulkInstances(mig common.GkeMigWrapper, spec *ProvisioningRequestDetailsSpec) error {
	ref := gce.GceRef{
		Name:    spec.MigName,
		Zone:    spec.Zone,
		Project: spec.ProjectID,
	}
	if f.bulkMigs[ref] == nil {
		f.bulkMigs[ref] = &bulkmig.Status{
			Ref:        ref,
			InProgress: false,
			TargetSize: 0,
		}
	}
	if f.bulkMigs[ref].InProgress || f.bulkMigs[ref].TargetSize != 0 {
		return fmt.Errorf("BulkMig %v already has scale up in progress", ref)
	}

	if f.acceptProvReq(spec, "") != nil {
		return fmt.Errorf("Couldn't update ProvReq (BulkMig mode) to Accepted state, spec %+v", spec)
	}

	if f.labels[ref] == nil {
		f.labels[ref] = map[string]string{}
	}
	f.labels[ref][gkelabels.ProvisioningRequestLabelKey] = resizerequestclient.ResizeRequestName(spec.ProvReqNamespace, spec.ProvReqName)
	f.bulkMigs[ref].InProgress = true
	f.bulkMigs[ref].TargetSize = spec.Delta

	return nil
}

func (f *ProvReqManagerFake) CreateResizeRequest(spec *ProvisioningRequestDetailsSpec, shouldUpdateProvReqDetails ShouldUpdateProvReqDetails) error {
	if f.rrs[spec.MigName] == nil {
		f.rrs[spec.MigName] = map[string]resizerequestclient.ResizeRequestStatus{}
	}
	rrName := resizerequestclient.ResizeRequestName(spec.ProvReqNamespace, spec.ProvReqName)
	_, found := f.rrs[spec.MigName][rrName]
	if found {
		return fmt.Errorf("Resize Request %q in mig %s already exists", spec.ProvReqName, spec.MigName)
	}

	if shouldUpdateProvReqDetails && f.acceptProvReq(spec, rrName) != nil {
		return fmt.Errorf("Couldn't update ProvReq (ResizeRequest mode) to Accepted state, spec %+v", spec)
	}

	f.rrs[spec.MigName][rrName] = resizerequestclient.ResizeRequestStatus{
		ID:                   0,
		Name:                 rrName,
		CreationTime:         time.Time{},
		ResizeBy:             spec.Delta,
		State:                resizerequestclient.ResizeRequestStateAccepted,
		ProjectID:            spec.ProjectID,
		MigName:              spec.MigName,
		Zone:                 spec.Zone,
		RequestedRunDuration: &queuedwrapper.DefaultMaxRunDuration,
		Errors:               nil,
		LastAttemptErrors:    nil,
	}
	return nil
}

func (f *ProvReqManagerFake) ResizeRequests(mig gce.GceRef) ([]resizerequestclient.ResizeRequestStatus, error) {
	if f.rrs == nil {
		return nil, fmt.Errorf("Couldn't retrieve Resize Requests, got nil")
	}
	if f.rrs[mig.Name] == nil {
		return nil, fmt.Errorf("Couldn't retrieve Resize Requests, mig %s not found.", mig)
	}
	return slices.Collect(maps.Values(f.rrs[mig.Name])), nil
}

func (f *ProvReqManagerFake) acceptProvReq(spec *ProvisioningRequestDetailsSpec, rrName string) error {
	prID := prpods.ProvReqID{Namespace: spec.ProvReqNamespace, Name: spec.ProvReqName}
	pr := f.prs[prID]
	if pr == nil {
		return fmt.Errorf("Provisioning Request %v not found", prID)
	}
	var podTemplateNames []string
	for _, podTemplate := range pr.PodTemplates {
		podTemplateNames = append(podTemplateNames, podTemplate.Name)
	}
	mode := queuedwrapper.ProvisioningModeResizeRequest
	if rrName == "" {
		mode = queuedwrapper.ProvisioningModeBulkMig
	}
	details := &queuedwrapper.ProvisioningClassDetails{
		NodeGroupName:           spec.MigName,
		NodePoolName:            spec.NodePoolName,
		AcceleratorType:         spec.AcceleratorType,
		SelectedZone:            spec.Zone,
		NodePoolAutoProvisioned: queuedwrapper.AutoprovisionedStatusFromBool(spec.MigAutoProvisioned),
		PodTemplateName:         strings.Join(podTemplateNames, ", "),
		ResizeRequestName:       rrName,
		ProvisioningMode:        mode,
	}
	err := provreqstate.SetProvisioningClassDetails(pr, details)
	if err != nil {
		panic(fmt.Sprintf("setting details should succeed, got error: %v", err))
	}
	err = provreqstate.SetState(pr, provreqstate.AcceptedState, v1.Time{})
	if err != nil {
		panic(fmt.Sprintf("setting Accepted state should succeed,, got error: %v", err))
	}
	return nil
}

func (f *ProvReqManagerFake) Refresh(rrMigs, bulkMigs map[gce.GceRef]common.GkeMigWrapper, now time.Time) error {
	// not necessary for current unit tests
	panic("not implemented")
}

func (f *ProvReqManagerFake) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool {
	// not necessary for current unit tests
	panic("not implemented")
}

func (f *ProvReqManagerFake) BulkMigs() []*bulkmig.Status {
	return slices.Collect(maps.Values(f.bulkMigs))
}

func (f *ProvReqManagerFake) MigLabels(migRef gce.GceRef) map[string]string {
	return f.labels[migRef]
}
