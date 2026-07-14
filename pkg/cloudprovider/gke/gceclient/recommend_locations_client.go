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
	"time"
)

// TargetShape represents strategy for distributing VMs across zones in a region.
type TargetShape string

// ZonePreference for recommendations in a given location.
type ZonePreference string

// RespectedLimits indicates what may limit placement of VMs.
type RespectedLimits string

// RecommendationType represents the type of recommendation, which corresponds to the
// exact action that will be taken.
type RecommendationType string

const (
	// TargetShapeAny VMs can be scheduled in any zone.
	TargetShapeAny TargetShape = "ANY"
	// TargetShapeAnySingleZone VMs can be scheduled in any singe zone.
	TargetShapeAnySingleZone TargetShape = "ANY_SINGLE_ZONE"
	// TargetShapeBalanced VMs should be scheuled balaning between different zones.
	TargetShapeBalanced TargetShape = "BALANCED"
	// TargetShapeSoftBalanced VMs should be scheduled balancing between different zones.
	// including capacity-driven preferences.
	TargetShapeSoftBalanced TargetShape = "SOFT_BALANCED"
	// TargetShapeUnspecified the target shape is not defined.
	TargetShapeUnspecified TargetShape = "TARGET_SHAPE_UNSPECIFIED"

	// ZonePreferenceAllow VMs can be scheluded in given zone.
	ZonePreferenceAllow ZonePreference = "ALLOW"
	// ZonePreferenceDeny VMs canot be scheduled in given zone.
	ZonePreferenceDeny ZonePreference = "DENY"

	// RespectedLimitsAll All limits, including project specific quota and
	// zone capacity.
	RespectedLimitsAll RespectedLimits = "ALL"
	// RespectedLimitsCapacityOnly Use only zone capacity.
	RespectedLimitsCapacityOnly RespectedLimits = "CAPACITY_ONLY"

	// RecommendationTypeAdd is used when the intent is to create new instances.
	// The recommendation should reflect the capacity available currently.
	RecommendationTypeAdd RecommendationType = "ADD_INSTANCES"
	// RecommendationTypeQueue is used when the intent is to queue for resources.
	// The recommendation should reflect the expected waiting time.
	RecommendationTypeQueue RecommendationType = "QUEUE_INSTANCES"

	// The average call takes about 4-5s, with 99 percentile reaching 7.5s,
	// for details see 'Latency' section in go/nodepool-level-location-flexibility-dd.
	// To be on the safer side we set 8s as a timeout.
	RecommendLocationsCallTimeout = 8 * time.Second

	// OnHostMaintenanceTerminate tells GCE to terminate and (optionally) restart the instance away
	// from the maintenance activity. It's supported by all machine types except of bare metal machine types.
	OnHostMaintenanceTerminate = "TERMINATE"
	// OnHostMaintenanceMigrate tells GCE to automatically migrate instances out of the way of maintenance events.
	// It's not supported by GPU/TPU machine types and possibly others
	OnHostMaintenanceMigrate = "MIGRATE"
)

// RecommendLocationsClient is used for communicating with GCE API
// providing additional methods over OSS AutoscalingGceClient.
type RecommendLocationsClient interface {
	RecommendLocations(ctx context.Context, region string, request RecommendLocationsRequest) (*RecommendLocationsResponse, error)
}

// RecommendLocationsRequest contains data needed to make Request to Recommend Locations API
type RecommendLocationsRequest struct {
	// Count is the maximum number of instances to create.
	Count int
	// InstanceProperties specifies the instance template properties used to create instances.
	InstanceProperties *InstanceProperties
	// SourceInstanceTemplate specifies the instance template from which to create instances.
	// Used when InstanceProperties are not specified.
	SourceInstanceTemplate string
	// TargetShape contains strategy for distributing VMs across zones in a region.
	TargetShape TargetShape
	// LocationSettings indicates what zones are allowed to have VMs scheduled.
	LocationSettings map[string]ZoneSetting
	// RespectedLimits indicates what may limit placement of VMs.
	RespectedLimits RespectedLimits
	// RecommendationType indicates the intended action.
	RecommendationType RecommendationType
	// MaxRunDuration specifies the max run duration of VMs.
	MaxRunDuration *time.Duration
}

type InstanceProperties struct {
	MachineType                string
	MinCpuPlatform             string
	GuestAccelerators          []*AcceleratorConfig
	NetworkInterfaces          []*NetworkInterface
	Disks                      []*AttachedDisk
	Scheduling                 *Scheduling
	ReservationAffinity        *ReservationAffinity
	ConfidentialInstanceConfig *ConfidentialInstanceConfig
	ResourcePolicies           []string
}

type AcceleratorConfig struct {
	AcceleratorType  string
	AcceleratorCount int64
}

type ConfidentialInstanceConfig struct {
	ConfidentialInstanceType string
}

type NetworkInterface struct {
	Name              string
	Network           string
	Subnetwork        string
	NetworkAttachment string
	AccessConfig      []*NetworkAccessConfig
}

type NetworkAccessConfig struct {
	Kind  string
	Type  string
	Name  string
	NatIP string
}

type AttachedDisk struct {
	DiskSizeGb       int64
	AutoDelete       bool
	Boot             bool
	Type             string
	Interface        string
	InitializeParams *AttachedDiskInitializeParams
}

type AttachedDiskInitializeParams struct {
	DiskSizeGb  int64
	DiskType    string
	SourceImage string
}

type Scheduling struct {
	Preemptible               bool
	ProvisioningModel         string
	MaxRunDuration            *Duration
	OnHostMaintenance         string
	InstanceTerminationAction string
}

type Duration struct {
	Seconds int64
}

type ReservationAffinity struct {
	ConsumeAllocationType string
	Key                   string
	Values                []string
}

// ZoneSetting contains preference for the specific zone.
type ZoneSetting struct {
	// ZonePreference for a given location: ALLOW or DENY.
	ZonePreference ZonePreference
	// MaxScaleUpSize that can be recommended for a given zone.
	MaxScaleUpSize int64
}

// RecommendLocationsResponse contains Response from Recommend Locations API
type RecommendLocationsResponse struct {
	// Recommendation contains recoomended size for each zone.
	Recommendation map[string]int
	// RecommendationID assigned by GCE to this specific decision.
	RecommendationID string
	// SpecKey maps the scale-up request back to the specification generated by CA.
	SpecKey string
}

type AlwaysErrorRecommendLocationsClient struct {
}

// RecommendLocations always returns an error.
func (client AlwaysErrorRecommendLocationsClient) RecommendLocations(ctx context.Context, region string, request RecommendLocationsRequest) (*RecommendLocationsResponse, error) {
	return nil, fmt.Errorf("AlwaysErrorRecommendLocationsClient.RecommendLocations() is a stub that always returns an error")
}

// NewAlwaysErrorRecommendLocationsClient creates a RecommendLocationsClient stub that always returns an error.
func NewAlwaysErrorRecommendLocationsClient() RecommendLocationsClient {
	return AlwaysErrorRecommendLocationsClient{}
}
