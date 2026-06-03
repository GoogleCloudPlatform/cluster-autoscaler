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
	"fmt"

	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api "google.golang.org/api/compute/v1"
)

// GceReservationBlock is a GKE cluster autoscaler domain object abstracting GCE reservation block.
// Only data used in cluster autoscaler is defined and populated from GCE API.
type GceReservationBlock struct {
	Id            uint64
	Name          string
	Count         int64
	InUseCount    int64
	Status        string
	Zone          string
	SubBlocks     []*GceReservationSubBlock
	SubBlockCount int64
}

// GceReservationSubBlock is a domain object abstracting GCE reservation block
// Only data used in cluster autoscaler is defined and populated from GCE API.
type GceReservationSubBlock struct {
	Id         uint64
	Name       string
	Count      int64
	InUseCount int64
	Status     string
	Zone       string
}

func toGceReservationBlock(item *gce_api.ReservationBlock) (*GceReservationBlock, error) {
	if item == nil {
		return nil, fmt.Errorf("GCE reservation block is nil")
	}

	return &GceReservationBlock{
		Id:         item.Id,
		Name:       item.Name,
		Count:      item.Count,
		InUseCount: item.InUseCount,
		Status:     item.Status,
		Zone:       item.Zone,
	}, nil
}

func toGceReservationSubBlock(item *gce_api_beta.ReservationSubBlock) (*GceReservationSubBlock, error) {
	if item == nil {
		return nil, fmt.Errorf("GCE reservation subblock is nil")
	}
	return &GceReservationSubBlock{
		Id:         item.Id,
		Name:       item.Name,
		Count:      item.Count,
		InUseCount: item.InUseCount,
		Status:     item.Status,
		Zone:       item.Zone,
	}, nil
}
