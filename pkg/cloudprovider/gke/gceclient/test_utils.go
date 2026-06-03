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
	"time"

	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

type autoscalingInternalGceClientMock struct {
	gce.AutoscalingGceClient
	getZoneGpuCounts                            func() (map[string]map[string]int64, error)
	fetchReservationsInProject                  func(project string) ([]*gce_api.Reservation, error)
	fetchReservationBlocksInReservation         func(reservationRef ReservationRef) ([]*GceReservationBlock, error)
	fetchReservationSubBlocksInReservationBlock func(reservationRef ReservationRef) ([]*GceReservationSubBlock, error)
	fetchResourcePolicies                       func(projectId, region string) ([]*GceResourcePolicy, error)
	fetchNetwork                                func(projectId, name string) (*gce_api.Network, error)
	fetchZones                                  func(region string) ([]string, error)
	fetchStandardZones                          func(region string) ([]string, error)
	fetchAIZones                                func(region string) ([]string, error)
	resumeInstances                             func(migRef gce.GceRef, instances []gce.GceRef) error
	suspendInstances                            func(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error
	httpTimeout                                 time.Duration
}

func BuildAutoscalingInternalGceClientMock() *autoscalingInternalGceClientMock {
	return &autoscalingInternalGceClientMock{}
}

// WithGetZoneGpuCounts sets getZoneGpuCounts handler.
func (a *autoscalingInternalGceClientMock) WithGetZoneGpuCounts(getZoneGpuCounts func() (map[string]map[string]int64, error)) *autoscalingInternalGceClientMock {
	a.getZoneGpuCounts = getZoneGpuCounts
	return a
}

// WithFetchNetwork sets FetchNetwork handler.
func (a *autoscalingInternalGceClientMock) WithFetchNetwork(fetchNetwork func(projectId, name string) (*gce_api.Network, error)) *autoscalingInternalGceClientMock {
	a.fetchNetwork = fetchNetwork
	return a
}

// WithResumeInstances sets ResumeInstances handler.
func (a *autoscalingInternalGceClientMock) WithResumeInstances(resumeInstances func(migRef gce.GceRef, instances []gce.GceRef) error) *autoscalingInternalGceClientMock {
	a.resumeInstances = resumeInstances
	return a
}

// WithSuspendInstances sets SuspendInstances handler.
func (a *autoscalingInternalGceClientMock) WithSuspendInstances(suspendInstances func(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error) *autoscalingInternalGceClientMock {
	a.suspendInstances = suspendInstances
	return a
}

// WithHttpTimeout sets the HTTP timeout
func (a *autoscalingInternalGceClientMock) WithHttpTimeout(httpTimeout time.Duration) *autoscalingInternalGceClientMock {
	a.httpTimeout = httpTimeout
	return a
}

func (a *autoscalingInternalGceClientMock) WithFetchZones(fetchZones func(region string) ([]string, error)) *autoscalingInternalGceClientMock {
	a.fetchZones = fetchZones
	return a
}

func (a *autoscalingInternalGceClientMock) WithFetchReservationsInProject(fetchReservationsInProject func(project string) ([]*gce_api.Reservation, error)) *autoscalingInternalGceClientMock {
	a.fetchReservationsInProject = fetchReservationsInProject
	return a
}

func (a *autoscalingInternalGceClientMock) WithFetchStandardZones(fetchStandardZones func(region string) ([]string, error)) *autoscalingInternalGceClientMock {
	a.fetchStandardZones = fetchStandardZones
	return a
}

func (a *autoscalingInternalGceClientMock) WithFetchAIZones(fetchAIZones func(region string) ([]string, error)) *autoscalingInternalGceClientMock {
	a.fetchAIZones = fetchAIZones
	return a
}

func (client *autoscalingInternalGceClientMock) FetchAcceleratorTypes(zone string) (*gce_api.AcceleratorTypeList, error) {
	result := gce_api.AcceleratorTypeList{}
	zoneGpuCounts, err := client.getZoneGpuCounts()
	if err != nil {
		return nil, err
	}
	cachedZone := zoneGpuCounts[zone]
	for gpuName, maxCount := range cachedZone {
		result.Items = append(result.Items, &gce_api.AcceleratorType{
			Name:                    gpuName,
			MaximumCardsPerInstance: maxCount,
		})
	}
	return &result, nil
}

func (a *autoscalingInternalGceClientMock) FetchFutureReservationsInProject(_ string) ([]*GceFutureReservation, error) {
	// not necessary for current unit tests
	panic("not implemented")
}

func (a *autoscalingInternalGceClientMock) FetchReservationBlocksInReservation(reservationRef ReservationRef) ([]*GceReservationBlock, error) {
	return a.fetchReservationBlocksInReservation(reservationRef)
}

func (a *autoscalingInternalGceClientMock) FetchReservationSubBlocksInReservationBlock(reservationRef ReservationRef) ([]*GceReservationSubBlock, error) {
	return a.fetchReservationSubBlocksInReservationBlock(reservationRef)
}

func (a *autoscalingInternalGceClientMock) FetchResourcePolicies(projectId, region string) ([]*GceResourcePolicy, error) {
	return a.fetchResourcePolicies(projectId, region)
}

func (a *autoscalingInternalGceClientMock) FetchNetwork(projectId string, name string) (*gce_api.Network, error) {
	return a.fetchNetwork(projectId, name)
}

func (a *autoscalingInternalGceClientMock) ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error {
	return a.resumeInstances(migRef, instances)
}

func (a *autoscalingInternalGceClientMock) SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error {
	return a.suspendInstances(migRef, instances, forceSuspend)
}

func (a *autoscalingInternalGceClientMock) FetchZones(region string) ([]string, error) {
	return a.fetchZones(region)
}

func (a autoscalingInternalGceClientMock) FetchReservationsInProject(projectID string) ([]*gce_api.Reservation, error) {
	return a.fetchReservationsInProject(projectID)
}

func (a *autoscalingInternalGceClientMock) GetHttpTimeout() time.Duration {
	return a.httpTimeout
}

func (a *autoscalingInternalGceClientMock) FetchStandardZones(region string) ([]string, error) {
	return a.fetchStandardZones(region)
}

func (a *autoscalingInternalGceClientMock) FetchAIZones(region string) ([]string, error) {
	return a.fetchAIZones(region)
}
