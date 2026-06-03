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

package locationpolicy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	internal_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

const (
	recommendLocationsBalanceKey metrics.FunctionLabel = "scaleUp:recommendLocationsBalance"
)

// locationPolicyAnyBalancer balances scale up for location policy ANY node-pools
type locationPolicyAnyBalancer struct {
	provider           internal_processors.ProcessorsCloudProvider
	experimentsManager experiments.Manager
}

// NewLocationPolicyAnyBalancer returns a new balancer for location policy ANY node-pools
func NewLocationPolicyAnyBalancer(provider internal_processors.ProcessorsCloudProvider, experimentsManager experiments.Manager) *locationPolicyAnyBalancer {
	return &locationPolicyAnyBalancer{
		provider:           provider,
		experimentsManager: experimentsManager,
	}
}

// Balance returns a scale-up based on Recommend Locations API.
// Recommend Locations API call is expected to take less than 8s.
func (b *locationPolicyAnyBalancer) Balance(gkeNodeGroups []gke.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, error) {
	if len(gkeNodeGroups) == 0 {
		return nil, errors.New("got empty gke.NodeGroup slice")
	}

	startTime := time.Now()
	defer metrics.UpdateDurationFromStart(recommendLocationsBalanceKey, startTime)

	// Each gkeNodeGroup has an equivalent instance template, so we pick the first one.
	gkeNodeGroup := gkeNodeGroups[0]
	var instanceProperties *gceclient.InstanceProperties
	templateLink := ""
	if gkeNodeGroup.IsUpcoming() {
		// Upcoming gkeNodeGroup has no template link
		instanceProperties = instanceTemplatePropertiesFromGkeNodeGroup(gkeNodeGroup)
	}
	if instanceProperties == nil {
		var err error
		templateLink, err = b.provider.GetMigInstanceTemplateSelfLink(gkeNodeGroup.GetMig())
		if err != nil {
			return nil, fmt.Errorf("could not get instance template link for gkeNodeGroup: %v", gkeNodeGroup)
		}
	}

	gkeNodeGroupsMap := make(map[string]gke.NodeGroup, len(gkeNodeGroups))
	for _, group := range gkeNodeGroups {
		gkeNodeGroupsMap[group.GceRef().Zone] = group
	}

	allZones, err := b.provider.GetAllZones()
	if err != nil {
		return nil, fmt.Errorf("while fetchin all zones got error: %v", err)
	}

	rec, err := b.consultRecommendLocations(templateLink, instanceProperties, allZones, gkeNodeGroups, newNodes)
	if err != nil {
		return nil, fmt.Errorf("while consulting RecommendLocations got error: %v", err)
	}

	// Compute the result based on received recommendation.
	var result []nodegroupset.ScaleUpInfo
	for loc, nodesResize := range rec {
		if nodesResize <= 0 {
			continue
		}
		gkeNodeGroup, ok := gkeNodeGroupsMap[loc]
		if !ok {
			return nil, fmt.Errorf("did not found gkeNodeGroup for zone: %q", loc)
		}
		targetSize, err := gkeNodeGroup.TargetSize()
		if err != nil {
			return nil, fmt.Errorf("could not get target size for gkeNodeGroup: %+v", gkeNodeGroup)
		}
		if targetSize+nodesResize > gkeNodeGroup.MaxSize() {
			return nil, fmt.Errorf("error in RecLoc API as new size: %d is bigger than max size: %d for gkeNodeGroup: %+v", targetSize+nodesResize, gkeNodeGroup.MaxSize(), gkeNodeGroup)
		}

		result = append(result, nodegroupset.ScaleUpInfo{
			Group:       gkeNodeGroup,
			CurrentSize: targetSize,
			NewSize:     targetSize + nodesResize,
			MaxSize:     gkeNodeGroup.MaxSize(),
		})
	}
	return result, nil
}

func (b *locationPolicyAnyBalancer) consultRecommendLocations(templateLink string, instanceProperties *gceclient.InstanceProperties, allZones []string, gkeNodeGroups []gke.NodeGroup, newNodes int) (map[string]int, error) {
	if len(gkeNodeGroups) == 0 {
		return nil, errors.New("received empty gke.NodeGroup slice")
	}

	if (templateLink != "" && instanceProperties != nil) || (templateLink == "" && instanceProperties == nil) {
		return nil, errors.New("invalid confing. Either templateLink or instanceProperties should be set when consulting RLA, but not both")
	}
	// Setup location policy that allows only zones the gkeNodeGroups are in.
	locationSettings := make(map[string]gceclient.ZoneSetting, len(allZones))
	for _, z := range allZones {
		locationSettings[z] = gceclient.ZoneSetting{
			ZonePreference: gceclient.ZonePreferenceDeny,
		}
	}
	for _, gkeNodeGroup := range gkeNodeGroups {
		targetSize, err := gkeNodeGroup.TargetSize()
		if err != nil {
			return nil, fmt.Errorf("could not get target size for gkeNodeGroup: %+v", gkeNodeGroup)
		}
		maxScaleUpSize := int64(gkeNodeGroup.MaxSize() - targetSize)
		if maxScaleUpSize > 0 {
			locationSettings[gkeNodeGroup.GceRef().Zone] = gceclient.ZoneSetting{
				ZonePreference: gceclient.ZonePreferenceAllow,
				MaxScaleUpSize: maxScaleUpSize,
			}
		}
	}

	request := gceclient.RecommendLocationsRequest{
		Count:                  newNodes,
		SourceInstanceTemplate: templateLink,
		InstanceProperties:     instanceProperties,
		TargetShape:            gceclient.TargetShapeAny,
		LocationSettings:       locationSettings,
	}

	// Since all gkeNodeGroups should have the same nodepool we just pick value from the first one.
	if gkeNodeGroups[0].FlexStartNonQueued() {
		request.RecommendationType = gceclient.RecommendationTypeQueue
		if b.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.RecommendLocationsFlexAddInstancesFlag, false) {
			request.RecommendationType = gceclient.RecommendationTypeAdd
		}

		// `QUEUED_INSTANCES` RecommendLocations API call uses implicitly `ANY_SINGLE_ZONE` target shape,
		// but apparently target shape is not permitted to be explicitly passed for `QUEUED_INSTANCES` type call.
		// Thus we're setting empty target shape for the request (GCE will fix the validation to allow setting `ANY_SINGLE_ZONE` explicitly in b/406224578)
		if request.RecommendationType == gceclient.RecommendationTypeQueue {
			request.TargetShape = ""
		}

		duration, err := queuedwrapper.MaxRunDurationFromStringOrDefaultWithWarning(gkeNodeGroups[0].Spec().MaxRunDurationInSeconds)
		if err != nil {
			return nil, fmt.Errorf("got error while parsing MaxRunDuration GkeMig %+v field, value: %q, error: %v", gkeNodeGroups[0].GceRef(), gkeNodeGroups[0].Spec().MaxRunDurationInSeconds, err)
		}
		request.MaxRunDuration = duration
	}

	ctx, cancel := context.WithTimeout(context.Background(), gceclient.RecommendLocationsCallTimeout)
	defer cancel()
	resp, err := b.provider.RecommendLocations(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("while calling recommendLocations got error: %+v", err)
	}
	return resp.Recommendation, nil
}
