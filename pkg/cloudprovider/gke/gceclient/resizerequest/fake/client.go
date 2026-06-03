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

package fake

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

// ResizeRequestClient implements the ResizeRequestClient interface.
type ResizeRequestClient struct {
	sync.Mutex
	requests     map[gce.GceRef]map[string]*resizerequestclient.ResizeRequestStatus
	errors       map[string]error
	createErrors map[gce.GceRef]error
	gceClient    *fakegce.GceClient
}

// NewResizeRequestClient creates a new fake client.
func NewResizeRequestClient(gceClient *fakegce.GceClient) *ResizeRequestClient {
	return &ResizeRequestClient{
		requests:     make(map[gce.GceRef]map[string]*resizerequestclient.ResizeRequestStatus),
		errors:       make(map[string]error),
		createErrors: make(map[gce.GceRef]error),
		gceClient:    gceClient,
	}
}

// --- Implementation of ResizeRequestClient Interface ---

func (r *ResizeRequestClient) CreateResizeRequest(ctx context.Context, migRef gce.GceRef, createRequest resizerequestclient.ResizeRequestCreateRequest) error {
	r.Lock()
	defer r.Unlock()

	if err, ok := r.createErrors[migRef]; ok && err != nil {
		return err
	}

	if _, ok := r.requests[migRef]; !ok {
		r.requests[migRef] = make(map[string]*resizerequestclient.ResizeRequestStatus)
	}
	r.requests[migRef][createRequest.Name] = &resizerequestclient.ResizeRequestStatus{
		ID:                   uint64(rand.Int63()),
		Name:                 createRequest.Name,
		CreationTime:         time.Now(),
		ResizeBy:             createRequest.ResizeBy,
		State:                resizerequestclient.ResizeRequestStateSucceeded,
		ProjectID:            migRef.Project,
		MigName:              migRef.Name,
		Zone:                 migRef.Zone,
		RequestedRunDuration: createRequest.RequestedRunDuration,
		Errors:               nil,
		LastAttemptErrors:    nil,
	}
	if r.gceClient == nil {
		return fmt.Errorf("ResizeRequestClient not fully initialized with GceClient")
	}

	tName, err := r.gceClient.FetchMigTemplateName(migRef)
	if err != nil {
		return fmt.Errorf("could not fetch template name for mig %s: %v", migRef.Name, err)
	}
	templateName := path.Base(tName.Name)

	_, err = r.gceClient.FetchMigTemplate(migRef, templateName, tName.Regional)
	if err != nil {
		return fmt.Errorf("instance Template %s not found: %v", templateName, err)
	}

	baseName := migRef.Name

	// Add nodes via CreateInstances
	names := make([]string, createRequest.ResizeBy)
	for i := 0; i < int(createRequest.ResizeBy); i++ {
		names[i] = fmt.Sprintf("%s-%s", baseName, strings.ToLower(randString(4)))
	}

	err = r.gceClient.ResizeAtomically(migRef, templateName, names)
	if err != nil {
		return fmt.Errorf("failed creating instances in GceClient: %v", err)
	}

	return nil
}

func (r *ResizeRequestClient) ResizeRequest(ctx context.Context, migRef gce.GceRef, name string) (resizerequestclient.ResizeRequestStatus, error) {
	r.Lock()
	defer r.Unlock()

	if r.requests[migRef] == nil {
		return resizerequestclient.ResizeRequestStatus{}, fmt.Errorf("couldn't retrieve Resize Request %q, mig %v not found", name, migRef)
	}
	rr, found := r.requests[migRef][name]
	if !found {
		return resizerequestclient.ResizeRequestStatus{}, fmt.Errorf("resize Request %q in mig %v not found", name, migRef)
	}
	return *rr, nil
}

func (r *ResizeRequestClient) ResizeRequests(ctx context.Context, migRef gce.GceRef) ([]resizerequestclient.ResizeRequestStatus, error) {
	r.Lock()
	defer r.Unlock()

	if r.requests[migRef] == nil {
		return []resizerequestclient.ResizeRequestStatus{}, nil
	}
	var requests []resizerequestclient.ResizeRequestStatus
	for _, req := range r.requests[migRef] {
		requests = append(requests, *req)
	}
	return requests, nil
}

func (r *ResizeRequestClient) RegisterFailedResizeRequestsCreation(migRef gce.GceRef, err error, count int) {
	panic("unimplemented")
}
func (r *ResizeRequestClient) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	panic("unimplemented")
}
func (r *ResizeRequestClient) AdvanceResizeRequestCleanUp(ctx context.Context, rr resizerequestclient.ResizeRequestStatus) error {
	panic("unimplemented")
}
func (r *ResizeRequestClient) ReportState(rr resizerequestclient.ResizeRequestStatus) resizerequestclient.ResizeRequestReportState {
	panic("unimplemented")
}
func (r *ResizeRequestClient) SetReportState(rr resizerequestclient.ResizeRequestStatus, state resizerequestclient.ResizeRequestReportState) {
	panic("unimplemented")
}

// SetResizeRequest allows setting or updating a Resize Request in the fake client.
func (r *ResizeRequestClient) SetResizeRequest(migRef gce.GceRef, rr *resizerequestclient.ResizeRequestStatus) {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.requests[migRef]; !ok {
		r.requests[migRef] = make(map[string]*resizerequestclient.ResizeRequestStatus)
	}
	r.requests[migRef][rr.Name] = rr
}

// SetCreateError allows configuring a specific error to be returned when CreateResizeRequest is called for a MIG.
func (r *ResizeRequestClient) SetCreateError(migRef gce.GceRef, err error) {
	r.Lock()
	defer r.Unlock()

	r.createErrors[migRef] = err
}

// --- Helpers ---

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
