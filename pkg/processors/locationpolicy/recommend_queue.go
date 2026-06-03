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
	"fmt"
	"strings"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
)

type gkeCloudProvider interface {
	GetMigInstanceTemplateSelfLink(*gke.GkeMig) (string, error)
	RecommendLocations(ctx context.Context, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error)
	GetAllZones() ([]string, error)
}

// RecommendZoneForQueuing returs a single zone suggested by the RecommendLocations API for queuing instances.
func RecommendZoneForQueueing(
	provider gkeCloudProvider,
	maxRunDuration *time.Duration,
	nodeCount int,
	selectedMig *gke.GkeMig,
	similarMigs []*gke.GkeMig,
) (string, error) {
	rlaReq, err := recommendQueueRequest(provider, maxRunDuration, nodeCount, selectedMig, similarMigs)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), gceclient.RecommendLocationsCallTimeout)
	defer cancel()

	rlaResponse, err := provider.RecommendLocations(ctx, rlaReq)
	if err != nil {
		return "", fmt.Errorf("error obtaining zone recommendation from RLA: %s", err)
	}
	return recommendLocationsResponse(rlaResponse, nodeCount)
}

func recommendQueueRequest(provider gkeCloudProvider, maxRunDuration *time.Duration, nodeCount int, selectedMig *gke.GkeMig, similarMigs []*gke.GkeMig) (gceclient.RecommendLocationsRequest, error) {
	allZones, err := provider.GetAllZones()
	if err != nil {
		return gceclient.RecommendLocationsRequest{}, fmt.Errorf("while fetching all zones got error: %v", err)
	}
	zoneSettings := map[string]gceclient.ZoneSetting{}
	for _, z := range allZones {
		zoneSettings[z] = gceclient.ZoneSetting{ZonePreference: gceclient.ZonePreferenceDeny}
	}
	for _, mig := range similarMigs {
		zoneSettings[mig.GceRef().Zone] = gceclient.ZoneSetting{ZonePreference: gceclient.ZonePreferenceAllow}
	}
	rlaReq := gceclient.RecommendLocationsRequest{
		Count:              nodeCount,
		LocationSettings:   zoneSettings,
		RecommendationType: gceclient.RecommendationTypeQueue,
		MaxRunDuration:     maxRunDuration,
		// `QUEUED_INSTANCES` RecommendLocations API call uses implicitly `ANY_SINGLE_ZONE` target shape,
		// but apparently target shape is not permitted to be explicitly passed for `QUEUED_INSTANCES` type call.
		// Thus we're setting empty target shape for the request (GCE will fix the validation to allow setting `ANY_SINGLE_ZONE` explicitly in b/406224578)
		TargetShape: "",
	}
	if selectedMig.IsUpcoming() {
		rlaReq.InstanceProperties = instanceTemplatePropertiesFromGkeNodeGroup(selectedMig)
	} else {
		rlaReq.SourceInstanceTemplate, err = provider.GetMigInstanceTemplateSelfLink(selectedMig)
		if err != nil {
			return gceclient.RecommendLocationsRequest{}, fmt.Errorf("couldn't obtain instance template for MIG %s: %s", selectedMig.Id(), err)
		}
	}
	return rlaReq, nil
}

func recommendLocationsResponse(rlaResponse *gceclient.RecommendLocationsResponse, nodeCount int) (string, error) {
	if len(rlaResponse.Recommendation) != 1 {
		zones := []string{}
		for z := range rlaResponse.Recommendation {
			zones = append(zones, z)
		}
		return "", fmt.Errorf("exactly one zone recommmendation expected from RLA, but %v were returned: %q", len(zones), strings.Join(zones, ","))
	}
	for recZone, recCount := range rlaResponse.Recommendation {
		if recCount != nodeCount {
			return "", fmt.Errorf("invalid response from RLA: expected recommendation for %v nodes, but returned recommendation for %v", nodeCount, recCount)
		}
		return recZone, nil
	}
	return "", fmt.Errorf("invalid code path reached (no recommendation from RLA)")
}
