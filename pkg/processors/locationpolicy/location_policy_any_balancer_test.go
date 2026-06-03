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
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	rrclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestLocationPolicyAnyBalancer(t *testing.T) {
	duration := 24 * time.Hour // 86400s

	zones := []string{"us-test1-a", "us-test1-b", "us-test1-c"}
	testCases := []struct {
		name                string
		disabledExperiments []string
		getTemplateErr      bool
		getTargetSizeErr    bool
		migConfigs          []testMigConfig
		newNodes            int
		wantCall            recommendLocationsCall
		wantScaleUpInfo     map[gce.GceRef]nodegroupset.ScaleUpInfo
		wantErr             bool
	}{
		{
			name: "single mig balancing",
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 5,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 5,
				allowedZones: map[string]int64{
					"us-test1-a": 10,
				},
				deniedZones: []string{"us-test1-b", "us-test1-c"},
				recommendation: map[string]int{
					"us-test1-a": 5,
				},
				templateSelfLink: "link-A",
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "mig-1"}: {CurrentSize: 0, NewSize: 5, MaxSize: 10},
			},
		},
		{
			name:                "single DWS Flex mig balancing",
			disabledExperiments: []string{experiments.RecommendLocationsFlexAddInstancesFlag},
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "dws-mig-1", maxSize: 10, templateSelfLink: "link-A",
					locationPolicy: gke.LocationPolicyAny, spec: &gkeclient.NodePoolSpec{FlexStart: true, MaxRunDurationInSeconds: "86400"}},
			},
			newNodes: 5,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:                    gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "dws-mig-1"}.Name,
				unsetTargetShape:           true,
				recommendationTypeOverride: gceclient.RecommendationTypeQueue,
				maxRunDuration:             &duration,
				newNodes:                   5,
				allowedZones: map[string]int64{
					"us-test1-a": 10,
				},
				deniedZones: []string{"us-test1-b", "us-test1-c"},
				recommendation: map[string]int{
					"us-test1-a": 5,
				},
				templateSelfLink: "link-A",
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "dws-mig-1"}: {CurrentSize: 0, NewSize: 5, MaxSize: 10},
			},
		},
		{
			name:                "multiple DWS Flext start migs, always one recommendation",
			disabledExperiments: []string{experiments.RecommendLocationsFlexAddInstancesFlag},
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "dws-mig-1", targetSize: 2, maxSize: 30, templateSelfLink: "link-A",
					locationPolicy: gke.LocationPolicyAny, spec: &gkeclient.NodePoolSpec{FlexStart: true, MaxRunDurationInSeconds: "86400"}},
				{project: "test", zone: "us-test1-b", name: "dws-mig-1", targetSize: 10, maxSize: 10, templateSelfLink: "link-B",
					locationPolicy: gke.LocationPolicyAny, spec: &gkeclient.NodePoolSpec{FlexStart: true, MaxRunDurationInSeconds: "86400"}},
				{project: "test", zone: "us-test1-c", name: "dws-mig-1", targetSize: 0, maxSize: 30, templateSelfLink: "link-C",
					locationPolicy: gke.LocationPolicyAny, spec: &gkeclient.NodePoolSpec{FlexStart: true, MaxRunDurationInSeconds: "86400"}},
				{project: "test", zone: "us-test1-d", name: "dws-mig-1", targetSize: 10, maxSize: 12, templateSelfLink: "link-D",
					locationPolicy: gke.LocationPolicyAny, spec: &gkeclient.NodePoolSpec{FlexStart: true, MaxRunDurationInSeconds: "86400"}},
			},
			newNodes: 10,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:                    gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "dws-mig-1"}.Name,
				unsetTargetShape:           true,
				recommendationTypeOverride: gceclient.RecommendationTypeQueue,
				maxRunDuration:             &duration,
				newNodes:                   10,
				allowedZones: map[string]int64{
					"us-test1-a": 28,
					"us-test1-c": 30,
					"us-test1-d": 2,
				},
				deniedZones: []string{"us-test1-b"},
				recommendation: map[string]int{
					"us-test1-a": 10,
				},
				templateSelfLink: "link-A",
				err:              nil,
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "dws-mig-1"}: {CurrentSize: 2, NewSize: 12, MaxSize: 30},
			},
		},
		{
			name: "multiple migs balancing, under capacity",
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", targetSize: 2, maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-b", name: "mig-1", targetSize: 10, maxSize: 10, templateSelfLink: "link-B", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-c", name: "mig-1", targetSize: 0, maxSize: 10, templateSelfLink: "link-C", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 10,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 10,
				allowedZones: map[string]int64{
					"us-test1-a": 8,
					"us-test1-c": 10,
				},
				deniedZones: []string{"us-test1-b"},
				recommendation: map[string]int{
					"us-test1-a": 6,
					"us-test1-c": 4,
				},
				templateSelfLink: "link-A",
				err:              nil,
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "mig-1"}: {CurrentSize: 2, NewSize: 8, MaxSize: 10},
				{Project: "test", Zone: "us-test1-c", Name: "mig-1"}: {CurrentSize: 0, NewSize: 4, MaxSize: 10},
			},
		},
		{
			name: "multiple migs balancing, zero recommendation for one of the migs",
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", targetSize: 2, maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-b", name: "mig-1", targetSize: 6, maxSize: 10, templateSelfLink: "link-B", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-c", name: "mig-1", targetSize: 0, maxSize: 10, templateSelfLink: "link-C", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 10,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 10,
				allowedZones: map[string]int64{
					"us-test1-a": 8,
					"us-test1-b": 4,
					"us-test1-c": 10,
				},
				deniedZones: []string{},
				recommendation: map[string]int{
					"us-test1-a": 6,
					"us-test1-b": 0,
					"us-test1-c": 4,
				},
				templateSelfLink: "link-A",
				err:              nil,
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "mig-1"}: {CurrentSize: 2, NewSize: 8, MaxSize: 10},
				{Project: "test", Zone: "us-test1-c", Name: "mig-1"}: {CurrentSize: 0, NewSize: 4, MaxSize: 10},
			},
		},
		{
			name: "multiple migs balancing, to capacity",
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", targetSize: 2, maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-b", name: "mig-1", targetSize: 10, maxSize: 10, templateSelfLink: "link-B", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-c", name: "mig-1", maxSize: 10, templateSelfLink: "link-C", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 18,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 18,
				allowedZones: map[string]int64{
					"us-test1-a": 8,
					"us-test1-c": 10,
				},
				deniedZones: []string{"us-test1-b"},
				recommendation: map[string]int{
					"us-test1-a": 8,
					"us-test1-c": 10,
				},
				templateSelfLink: "link-A",
				err:              nil,
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "mig-1"}: {CurrentSize: 2, NewSize: 10, MaxSize: 10},
				{Project: "test", Zone: "us-test1-c", Name: "mig-1"}: {CurrentSize: 0, NewSize: 10, MaxSize: 10},
			},
		},
		{
			name: "multiple migs balancing, recommendation for a zone over capacity (API should never do this)",
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", targetSize: 4, maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-b", name: "mig-1", maxSize: 10, templateSelfLink: "link-B", locationPolicy: gke.LocationPolicyAny},
				{project: "test", zone: "us-test1-c", name: "mig-1", maxSize: 10, templateSelfLink: "link-C", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 20,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 20,
				allowedZones: map[string]int64{
					"us-test1-a": 6,
					"us-test1-b": 10,
					"us-test1-c": 10,
				},
				deniedZones: []string{},
				recommendation: map[string]int{
					"us-test1-a": 10,
					"us-test1-b": 4,
					"us-test1-c": 6,
				},
				templateSelfLink: "link-A",
				err:              nil,
			}),
			wantErr: true,
		},
		{
			name: "Recommend Location API error handling",
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 5,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 5,
				allowedZones: map[string]int64{
					"us-test1-a": 10,
				},
				deniedZones:      []string{"us-test1-b", "us-test1-c"},
				recommendation:   nil,
				templateSelfLink: "link-A",
				err:              errors.New("API test error"),
			}),
			wantErr: true,
		},
		{
			name:           "get instance template error handling",
			getTemplateErr: true,
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 5,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 5,
				allowedZones: map[string]int64{
					"us-test1-a": 10,
				},
				deniedZones: []string{"us-test1-b", "us-test1-c"},
				recommendation: map[string]int{
					"us-test1-a": 5,
				},
				templateSelfLink: "link-A",
			}),
			wantErr: true,
		},
		{
			name:             "get target size error handling",
			getTargetSizeErr: true,
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "mig-1", maxSize: 10, templateSelfLink: "link-A", locationPolicy: gke.LocationPolicyAny},
			},
			newNodes: 5,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:  gce.GceRef{Project: "test", Zone: "us-test1-a", Name: "mig-1"}.Name,
				newNodes: 5,
				allowedZones: map[string]int64{
					"us-test1-a": 10,
				},
				deniedZones: []string{"us-test1-b", "us-test1-c"},
				recommendation: map[string]int{
					"us-test1-a": 5,
				},
				templateSelfLink: "link-A",
			}),
			wantErr: true,
		},
		{
			name:     "no migs",
			newNodes: 5,
			wantErr:  true,
		},
		{
			name:                "fsnq_expDisabled_useQueueInstances_unsetTargetShape",
			disabledExperiments: []string{experiments.RecommendLocationsFlexAddInstancesFlag},
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "dws-mig-1", maxSize: 10, templateSelfLink: "link-A",
					locationPolicy: gke.LocationPolicyAny, spec: &gkeclient.NodePoolSpec{FlexStart: true, MaxRunDurationInSeconds: "86400"}},
			},
			newNodes: 5,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:                    "dws-mig-1",
				unsetTargetShape:           true,
				recommendationTypeOverride: gceclient.RecommendationTypeQueue,
				maxRunDuration:             &duration,
				newNodes:                   5,
				allowedZones:               map[string]int64{"us-test1-a": 10},
				deniedZones:                []string{"us-test1-b", "us-test1-c"},
				recommendation:             map[string]int{"us-test1-a": 5},
				templateSelfLink:           "link-A",
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "dws-mig-1"}: {CurrentSize: 0, NewSize: 5, MaxSize: 10},
			},
		},
		{
			name: "fsnq_expEnabled_useAddInstances_setTargetShape",
			migConfigs: []testMigConfig{
				{project: "test", zone: "us-test1-a", name: "dws-mig-1", maxSize: 10, templateSelfLink: "link-A",
					locationPolicy: gke.LocationPolicyAny, spec: &gkeclient.NodePoolSpec{FlexStart: true, MaxRunDurationInSeconds: "86400"}},
			},
			newNodes: 5,
			wantCall: newRecommendLocationCall(recommendLocationsCallConfig{
				migName:                    "dws-mig-1",
				unsetTargetShape:           false,
				recommendationTypeOverride: gceclient.RecommendationTypeAdd,
				maxRunDuration:             &duration,
				newNodes:                   5,
				allowedZones:               map[string]int64{"us-test1-a": 10},
				deniedZones:                []string{"us-test1-b", "us-test1-c"},
				recommendation:             map[string]int{"us-test1-a": 5},
				templateSelfLink:           "link-A",
			}),
			wantScaleUpInfo: map[gce.GceRef]nodegroupset.ScaleUpInfo{
				{Project: "test", Zone: "us-test1-a", Name: "dws-mig-1"}: {CurrentSize: 0, NewSize: 5, MaxSize: 10},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManager := &gke.GkeManagerMock{}
			gkeManager.On("GetZonesInRegion", "us-test1").Return(zones, nil).Once()
			gkeManager.On("RecommendLocations", mock.Anything, "us-test1", tc.wantCall.req).Return(tc.wantCall.rsp, tc.wantCall.err).Once()
			gkeManager.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.E2)
			gkeManager.On("IsResizeRequestErrorHandlingEnabled").Return(true)
			gkeManager.On("ResizeRequests", mock.Anything).Return([]rrclient.ResizeRequestStatus{}, nil)

			provider, err := gke.BuildGkeCloudProvider(gkeManager, nil, nil, true, "us-test1", nil, false, false, nil, "", nil, nil, nil, 1000)
			if err != nil {
				t.Fatalf("gke.BuildGkeCloudProvider() error = %v", err)
			}

			var gkeNodeGroups []gke.NodeGroup
			var wantScaleUpInfo []nodegroupset.ScaleUpInfo
			for _, config := range tc.migConfigs {
				mig := newTestMig(gkeManager, config, tc.getTemplateErr, tc.getTargetSizeErr)
				gkeNodeGroups = append(gkeNodeGroups, mig)

				scaleUpInfo, found := tc.wantScaleUpInfo[mig.GceRef()]
				if found {
					scaleUpInfo.Group = mig
					wantScaleUpInfo = append(wantScaleUpInfo, scaleUpInfo)
				}
			}

			enabledExperiments := sets.New[string](experiments.RecommendLocationsFlexAddInstancesFlag)
			enabledExperiments.Delete(tc.disabledExperiments...)

			balancer := NewLocationPolicyAnyBalancer(provider, experiments.NewMockManager(enabledExperiments.UnsortedList()...))
			scaleUpInfo, err := balancer.Balance(gkeNodeGroups, tc.newNodes)

			if (err != nil) != tc.wantErr {
				t.Errorf("Balancer.Balance() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			sort.Slice(scaleUpInfo, func(i, j int) bool {
				return scaleUpInfo[i].Group.Id() < scaleUpInfo[j].Group.Id()
			})

			if diff := cmp.Diff(wantScaleUpInfo, scaleUpInfo, compareAllUnexportedOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Balancer.Balance() diff (-want +got):\n%s", diff)
			}
		})
	}
}

type recommendLocationsCallConfig struct {
	migName                    string
	newNodes                   int
	allowedZones               map[string]int64
	deniedZones                []string
	recommendation             map[string]int
	templateSelfLink           string
	unsetTargetShape           bool
	recommendationTypeOverride gceclient.RecommendationType
	maxRunDuration             *time.Duration
	err                        error
}

type recommendLocationsCall struct {
	req gceclient.RecommendLocationsRequest
	rsp *gceclient.RecommendLocationsResponse
	err error
}

func newRecommendLocationCall(config recommendLocationsCallConfig) recommendLocationsCall {
	locationSettings := map[string]gceclient.ZoneSetting{}
	for name, maxScaleUpSize := range config.allowedZones {
		locationSettings[name] = gceclient.ZoneSetting{
			ZonePreference: gceclient.ZonePreferenceAllow,
			MaxScaleUpSize: maxScaleUpSize,
		}
	}
	for _, z := range config.deniedZones {
		locationSettings[z] = gceclient.ZoneSetting{
			ZonePreference: gceclient.ZonePreferenceDeny,
		}
	}

	targetShape := gceclient.TargetShapeAny
	if config.unsetTargetShape {
		targetShape = ""
	}
	return recommendLocationsCall{
		req: gceclient.RecommendLocationsRequest{
			Count:                  config.newNodes,
			SourceInstanceTemplate: config.templateSelfLink,
			TargetShape:            targetShape,
			LocationSettings:       locationSettings,
			RecommendationType:     config.recommendationTypeOverride,
			MaxRunDuration:         config.maxRunDuration,
		},
		rsp: &gceclient.RecommendLocationsResponse{
			Recommendation: config.recommendation,
		},
		err: config.err,
	}
}
