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

package gke

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gcev1 "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"google.golang.org/api/option"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	rrclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
)

const napMaxNodes = 2000

func TestBuildGkeCloudProvider(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}

	resourceLimiter := cloudprovider.NewResourceLimiter(
		map[string]int64{cloudprovider.ResourceNameCores: 1, cloudprovider.ResourceNameMemory: 10000000},
		map[string]int64{cloudprovider.ResourceNameCores: 10, cloudprovider.ResourceNameMemory: 100000000})

	provider, err := BuildGkeCloudProvider(gkeManagerMock, nil, resourceLimiter, false, "us-test1", &gkedebuggingsnapshot.GkeDebuggingSnapshotter{}, false, false, nil, "", nil, nil, nil, napMaxNodes)
	assert.NoError(t, err)
	assert.NotNil(t, provider)
}

func TestNodeGroups(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}
	gke := &gkeCloudProviderImpl{
		gkeManager: gkeManagerMock,
	}
	mig := []*GkeMig{{gceRef: gce.GceRef{Name: "ng1"}}}
	gkeManagerMock.On("GetGkeMigs").Return(mig).Once()
	result := gke.NodeGroups()
	assert.Equal(t, []cloudprovider.NodeGroup{mig[0]}, result)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)
}

func TestNodeGroupsBlockedByServerError(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}
	gke := &gkeCloudProviderImpl{
		gkeManager: gkeManagerMock,
	}
	mig := []*GkeMig{{gceRef: gce.GceRef{Name: "ng1"}}}
	gkeManagerMock.On("GetGkeMigsBlockedByServerError").Return(mig).Once()
	result := gke.NodeGroupsBlockedByServerError()
	assert.Equal(t, []cloudprovider.NodeGroup{mig[0]}, result)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)
}

func TestNodeGroupsBlockedByNotFoundError(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}
	gke := &gkeCloudProviderImpl{
		gkeManager: gkeManagerMock,
	}
	mig := []*GkeMig{{gceRef: gce.GceRef{Name: "ng1"}}}
	gkeManagerMock.On("GetGkeMigsBlockedByNotFoundError").Return(mig).Once()
	result := gke.NodeGroupsBlockedByNotFoundError()
	assert.Equal(t, []cloudprovider.NodeGroup{mig[0]}, result)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)
}

func TestNodeGroupForNode(t *testing.T) {
	validNodePool := "nap-e2-standard"
	migRef := gce.GceRef{
		Project: "project-123",
		Zone:    "us-central1-c",
		Name:    "gke-nap-e2-standard-47c9e542-grp",
	}
	mig := &GkeMig{gceRef: migRef}
	validNodeName := "gke-nap-e2-standard-47c9e542-abcd"
	upcomingInstanceId := "gke-nap-e2-standard-47c9e542-grp-9186834877642524930-upcoming-0"
	validUpcomingNodeName := fmt.Sprintf("template-node-for-https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instanceGroups/%s", migRef.Project, migRef.Zone, upcomingInstanceId)
	tests := []struct {
		testName                                    string
		nodeName                                    string
		cachedInstanceId                            gce.GceRef
		upcoming                                    bool
		providerId                                  string
		disableResolveInstanceRefUsingNodePoolLabel bool
		gkeNodePoolLabel                            string
		cachedMigsForNodePool                       []*GkeMig
		getBasenameForMigErr                        error
		expectedError                               bool
	}{
		{
			testName:         "upcoming node",
			nodeName:         validUpcomingNodeName,
			cachedInstanceId: gce.GceRef{Project: migRef.Project, Zone: migRef.Zone, Name: upcomingInstanceId},
			upcoming:         true,
		},
		// this shouldn't happen in production as nodes have gke-nodepool label set at registration time, but theoretically possible
		{
			testName:      "node without ProviderID, no upcoming annotation, no gke-nodepool label",
			nodeName:      validNodeName,
			upcoming:      false,
			expectedError: true,
		},
		{
			testName:      "invalid upcoming node name",
			nodeName:      validUpcomingNodeName + "/INVALID",
			upcoming:      true,
			expectedError: true,
		},
		{
			testName:         "valid ProviderID",
			nodeName:         validNodeName,
			providerId:       fmt.Sprintf("gce://%s/%s/%s", migRef.Project, migRef.Zone, validNodeName),
			cachedInstanceId: gce.GceRef{Project: migRef.Project, Zone: migRef.Zone, Name: validNodeName},
			upcoming:         false,
		},
		{
			testName:      "invalid ProviderID",
			nodeName:      validNodeName,
			providerId:    fmt.Sprintf("gce://%s/%s/%s/INVALID", migRef.Project, migRef.Zone, validNodeName),
			expectedError: true,
		},
		{
			testName:      "unknown ProviderID",
			nodeName:      validNodeName,
			providerId:    fmt.Sprintf("gce://%s/%s/%s", migRef.Project, migRef.Zone, "unknown"),
			expectedError: true,
		},
		{
			testName:         "node without ProviderID with gke-nodepool label",
			nodeName:         validNodeName,
			gkeNodePoolLabel: validNodePool,
			cachedMigsForNodePool: []*GkeMig{
				{gceRef: gce.GceRef{Project: migRef.Project, Zone: "other-1", Name: "non-matching-1-grp"}},
				mig,
				{gceRef: gce.GceRef{Project: migRef.Project, Zone: "other-2", Name: "non-matching-2-grp"}},
			},
			cachedInstanceId: gce.GceRef{Project: migRef.Project, Zone: migRef.Zone, Name: validNodeName},
			expectedError:    false,
		},
		{
			testName:         "node without ProviderID with gke-nodepool label, but resolving using nodepool disabled",
			nodeName:         validNodeName,
			gkeNodePoolLabel: validNodePool,
			cachedMigsForNodePool: []*GkeMig{
				{gceRef: gce.GceRef{Project: migRef.Project, Zone: "other-1", Name: "non-matching-1-grp"}},
				mig,
				{gceRef: gce.GceRef{Project: migRef.Project, Zone: "other-2", Name: "non-matching-2-grp"}},
			},
			cachedInstanceId: gce.GceRef{Project: migRef.Project, Zone: migRef.Zone, Name: validNodeName},
			disableResolveInstanceRefUsingNodePoolLabel: true,
			expectedError: true,
		},
		{
			testName:              "node without ProviderID with gke-nodepool label, no migs found for nodepool",
			nodeName:              validNodeName,
			gkeNodePoolLabel:      validNodePool,
			cachedMigsForNodePool: nil,
			expectedError:         true,
		},
		{
			testName:              "node without ProviderID with gke-nodepool label, node name doesn't any match migs for nodepool",
			nodeName:              validNodeName,
			gkeNodePoolLabel:      validNodePool,
			cachedMigsForNodePool: []*GkeMig{},
			expectedError:         true,
		},
		{
			testName:              "node without ProviderID with gke-nodepool label, errors when matching migs for nodepool",
			nodeName:              validNodeName,
			gkeNodePoolLabel:      validNodePool,
			cachedMigsForNodePool: []*GkeMig{mig, mig},
			getBasenameForMigErr:  errors.New("cannot get basename"),
			expectedError:         true,
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			gkeManagerMock := &GkeManagerMock{}
			gkeManagerMock.On("GetMigForInstance", mock.AnythingOfType("gce.GceRef")).Return(
				func(instance gce.GceRef) *GkeMig {
					if instance != test.cachedInstanceId {
						return nil
					}
					return mig
				},
				func(instance gce.GceRef) error {
					if instance != test.cachedInstanceId {
						return fmt.Errorf("no mig for instance %s", instance)
					}
					return nil
				},
			)
			gkeManagerMock.On("ExistingMigsInNodePool", test.gkeNodePoolLabel).Return(test.cachedMigsForNodePool)
			gkeManagerMock.On("GetBasenameForMig", mock.AnythingOfType("*gke.GkeMig")).Return(
				func(m *GkeMig) string {
					return strings.TrimSuffix(m.gceRef.Name, "-grp")
				},
				test.getBasenameForMigErr,
			)
			gke := &gkeCloudProviderImpl{gkeManager: gkeManagerMock, resolveInstanceRefUsingNodePoolLabel: !test.disableResolveInstanceRefUsingNodePoolLabel}

			node := BuildTestNode(test.nodeName, 1000, 1000)
			node.Spec.ProviderID = test.providerId
			if test.upcoming {
				node.Annotations = make(map[string]string)
				node.Annotations[annotations.NodeUpcomingAnnotation] = "true"
			}
			if test.gkeNodePoolLabel != "" {
				node.ObjectMeta.Labels[gkelabels.GkeNodePoolLabel] = test.gkeNodePoolLabel
			}

			nodeGroup, err := gke.NodeGroupForNode(node)

			if test.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, mig, nodeGroup)
			}
		})
	}
}

func TestGetResourceLimiter(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}
	resourceLimiter := cloudprovider.NewResourceLimiter(
		map[string]int64{cloudprovider.ResourceNameCores: 1, cloudprovider.ResourceNameMemory: 10000000},
		map[string]int64{cloudprovider.ResourceNameCores: 10, cloudprovider.ResourceNameMemory: 100000000})
	gke := &gkeCloudProviderImpl{
		gkeManager:               gkeManagerMock,
		resourceLimiterFromFlags: resourceLimiter,
	}

	// Return default.
	gkeManagerMock.On("GetResourceLimiter").Return((*cloudprovider.ResourceLimiter)(nil), nil).Once()
	returnedResourceLimiter, err := gke.GetResourceLimiter()
	assert.NoError(t, err)
	assert.Equal(t, resourceLimiter, returnedResourceLimiter)

	// Return for GKE.
	resourceLimiterGKE := cloudprovider.NewResourceLimiter(
		map[string]int64{cloudprovider.ResourceNameCores: 2, cloudprovider.ResourceNameMemory: 20000000},
		map[string]int64{cloudprovider.ResourceNameCores: 5, cloudprovider.ResourceNameMemory: 200000000})
	gkeManagerMock.On("GetResourceLimiter").Return(resourceLimiterGKE, nil).Once()
	returnedResourceLimiterGKE, err := gke.GetResourceLimiter()
	assert.NoError(t, err)
	assert.Equal(t, returnedResourceLimiterGKE, resourceLimiterGKE)

	// Error in GceManager.
	gkeManagerMock.On("GetResourceLimiter").Return((*cloudprovider.ResourceLimiter)(nil), fmt.Errorf("Some error")).Once()
	_, err = gke.GetResourceLimiter()
	assert.Error(t, err)
}

const getInstanceGroupManagerResponse = `{
  "kind": "compute#instanceGroupManager",
  "id": "3213213219",
  "creationTimestamp": "2017-09-15T04:47:24.687-07:00",
  "name": "gke-cluster-1-default-pool",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b",
  "instanceTemplate": "https://www.googleapis.com/compute/v1/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool",
  "instanceGroup": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroups/gke-cluster-1-default-pool",
  "baseInstanceName": "gke-cluster-1-default-pool-f23aac-grp",
  "fingerprint": "kfdsuH",
  "currentActions": {
    "none": 3,
    "creating": 0,
    "creatingWithoutRetries": 0,
    "recreating": 0,
    "deleting": 0,
    "abandoning": 0,
    "restarting": 0,
    "refreshing": 0
  },
  "targetSize": 3,
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool"
}`

func TestMig(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}
	client := &http.Client{}
	gceService, err := gcev1.NewService(context.Background(), option.WithHTTPClient(client))
	assert.NoError(t, err)
	gceService.BasePath = server.URL
	gke := &gkeCloudProviderImpl{
		gkeManager:            gkeManagerMock,
		machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
		napMaxNodes:           napMaxNodes,
	}

	// Test NewNodeGroup.
	gkeManagerMock.On("GetProjectId").Return("project1")
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Once()
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(100))
	gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")

	systemLabels := map[string]string{apiv1.LabelZoneFailureDomain: "us-central1-b"}
	nodeGroup, err := gke.NewNodeGroup("n1-standard-1", nil, systemLabels, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, nodeGroup)
	mig1 := reflect.ValueOf(nodeGroup).Interface().(*GkeMig)
	assert.Equal(t, true, mig1.Autoprovisioned())
	mig1.exist = true
	assert.True(t, strings.HasPrefix(mig1.Id(), "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroups/"+nodeAutoprovisioningPrefix+"-n1-standard-1"))
	assert.Equal(t, true, mig1.Autoprovisioned())
	assert.Equal(t, 0, mig1.MinSize())
	assert.Equal(t, napMaxNodes, mig1.MaxSize())
	assert.Equal(t, false, mig1.TotalSizeLimitEnabled())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test TargetSize.
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(2), nil).Once()
	targetSize, err := mig1.TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 2, targetSize)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test IncreaseSize.
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(2), nil).Once()
	gkeManagerMock.On("CreateInstances", mock.AnythingOfType("*gke.GkeMig"), int64(1)).Return(nil).Once()
	err = mig1.IncreaseSize(1)
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test IncreaseSize - fail on wrong size.
	err = mig1.IncreaseSize(0)
	assert.Error(t, err)
	assert.Equal(t, "size increase must be positive", err.Error())

	// Test IncreaseSize - fail on too big delta.
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(2), nil).Once()
	err = mig1.IncreaseSize(napMaxNodes)
	assert.Error(t, err)
	assert.Equal(t, "size increase too large - desired:2002 max:2000", err.Error())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test DecreaseTargetSize.
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(3), nil).Once()
	gkeManagerMock.On("GetMigNodes", mock.AnythingOfType("*gke.GkeMig")).Return(
		[]gce.GceInstance{
			{
				Instance: cloudprovider.Instance{
					Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-9j4g",
				},
			},
			{
				Instance: cloudprovider.Instance{
					Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-dck1",
				},
			},
		}, nil).Once()
	gkeManagerMock.On("SetMigSize", mock.AnythingOfType("*gke.GkeMig"), int64(2)).Return(nil).Once()
	err = mig1.DecreaseTargetSize(-1)
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test DecreaseTargetSize - fail on positive delta.
	err = mig1.DecreaseTargetSize(1)
	assert.Error(t, err)
	assert.Equal(t, "size decrease must be negative", err.Error())

	// Test DecreaseTargetSize - fail on deleting existing nodes.
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(3), nil).Once()
	gkeManagerMock.On("GetMigNodes", mock.AnythingOfType("*gke.GkeMig")).Return(
		[]gce.GceInstance{
			{
				Instance: cloudprovider.Instance{
					Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-9j4g",
				},
			},
			{
				Instance: cloudprovider.Instance{
					Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-dck1",
				},
			},
		}, nil).Once()

	err = mig1.DecreaseTargetSize(-2)
	assert.Error(t, err)
	assert.Equal(t, "attempt to delete existing nodes targetSize:3 delta:-2 existingNodes: 2", err.Error())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test Belongs - true.
	gkeManagerMock.On("GetMigForInstance", mock.AnythingOfType("gce.GceRef")).Return(mig1, nil).Once()
	node := BuildTestNode("gke-cluster-1-default-pool-f7607aac-dck1", 1000, 1000)
	node.Spec.ProviderID = "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-dck1"

	belongs, err := mig1.Belongs(node)
	assert.NoError(t, err)
	assert.True(t, belongs)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test Belongs - false.
	mig2 := &GkeMig{
		gceRef: gce.GceRef{
			Project: "project1",
			Zone:    "us-central1-b",
			Name:    "default-pool",
		},
		gkeManager:      gkeManagerMock,
		minSize:         0,
		maxSize:         1000,
		autoprovisioned: true,
		exist:           true,
		spec:            nil,
	}
	AddMigsToNodePool("default-pool", mig2)
	gkeManagerMock.On("GetMigForInstance", mock.AnythingOfType("gce.GceRef")).Return(mig2, nil).Once()

	belongs, err = mig1.Belongs(node)
	assert.NoError(t, err)
	assert.False(t, belongs)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test DeleteNodes.
	n1 := BuildTestNode("gke-cluster-1-default-pool-f7607aac-9j4g", 1000, 1000)
	n1.Spec.ProviderID = "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-9j4g"
	n1ref := gce.GceRef{Project: "project1", Zone: "us-central1-b", Name: "gke-cluster-1-default-pool-f7607aac-9j4g"}
	n2 := BuildTestNode("gke-cluster-1-default-pool-f7607aac-dck1", 1000, 1000)
	n2.Spec.ProviderID = "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-dck1"
	n2ref := gce.GceRef{Project: "project1", Zone: "us-central1-b", Name: "gke-cluster-1-default-pool-f7607aac-dck1"}
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(2), nil).Once()
	gkeManagerMock.On("GetMigForInstance", n1ref).Return(mig1, nil).Once()
	gkeManagerMock.On("GetMigForInstance", n2ref).Return(mig1, nil).Once()
	gkeManagerMock.On("DeleteInstances", []gce.GceRef{n1ref, n2ref}).Return(nil).Once()
	err = mig1.DeleteNodes([]*apiv1.Node{n1, n2})
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test DeleteNodes - fail on reaching min size.
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(0), nil).Once()
	err = mig1.DeleteNodes([]*apiv1.Node{n1, n2})
	assert.Error(t, err)
	assert.Equal(t, "min size reached, nodes will not be deleted", err.Error())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test ForceDeleteNodes - don't fail on reaching min size.
	gkeManagerMock.On("GetMigForInstance", n1ref).Return(mig1, nil).Once()
	gkeManagerMock.On("GetMigForInstance", n2ref).Return(mig1, nil).Once()
	gkeManagerMock.On("DeleteInstances", []gce.GceRef{n1ref, n2ref}).Return(nil).Once()
	err = mig1.ForceDeleteNodes([]*apiv1.Node{n1, n2})
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	atomicResizeMig := &GkeMig{
		gceRef: gce.GceRef{
			Project: "project1",
			Zone:    "us-central1-b",
			Name:    "atomicResizeMig",
		},
		gkeManager:      gkeManagerMock,
		minSize:         0,
		maxSize:         1000,
		autoprovisioned: true,
		exist:           true,
		spec:            &gkeclient.NodePoolSpec{TpuMultiHost: true, TpuType: "tpuV5"},
	}
	AddMigsToNodePool("default-pool", atomicResizeMig)

	// Test DeleteNodes - don't fail on partial delete of atomic mig,
	gkeManagerMock.On("GetMigForInstance", mock.AnythingOfType("gce.GceRef")).Return(atomicResizeMig, nil).Once()
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(2), nil).Once()
	gkeManagerMock.On("DeleteInstances", []gce.GceRef{n1ref}).Return(nil).Once()
	err = atomicResizeMig.DeleteNodes([]*apiv1.Node{n1})
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test ForceDeleteNodes - don't fail on non-atomic delete.
	gkeManagerMock.On("GetMigForInstance", n1ref).Return(mig1, nil).Once()
	gkeManagerMock.On("DeleteInstances", []gce.GceRef{n1ref}).Return(nil).Once()
	err = mig1.ForceDeleteNodes([]*apiv1.Node{n1})
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test Nodes.
	gkeManagerMock.On("GetMigNodes", mock.AnythingOfType("*gke.GkeMig")).Return(
		[]gce.GceInstance{
			{
				Instance: cloudprovider.Instance{
					Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-9j4g",
				},
			},
			{
				Instance: cloudprovider.Instance{
					Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-dck1",
				},
			},
		}, nil).Once()
	nodes, err := mig1.Nodes()
	assert.NoError(t, err)
	assert.Equal(t, cloudprovider.Instance{Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-9j4g"}, nodes[0])
	assert.Equal(t, cloudprovider.Instance{Id: "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-dck1"}, nodes[1])
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test Create.
	mig1.exist = false
	gkeManagerMock.On("CreateNodePool", mock.AnythingOfType("*gke.GkeMig")).Return(nil, nil).Once()
	_, err = mig1.AutoprovisionedCreate()
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	gkeManagerMock.On("DeleteNodePool", mock.AnythingOfType("*gke.GkeMig")).Return(nil).Once()
	mig1.exist = true
	err = mig1.Delete()
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test TemaplateNodeLabels
	labels := map[string]string{"a": "1", "b": "2"}
	templateNodeLabelsNode := apiv1.Node{}
	templateNodeLabelsNode.Labels = labels
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&templateNodeLabelsNode), nil).Once()
	templateNodeLabels, err := mig2.TemplateNodeLabels()
	assert.NoError(t, err)
	assert.NotNil(t, templateNodeLabels)
	assert.Equal(t, labels, templateNodeLabels)
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test TemplateNodeInfo.
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}, cloudprovider.BuildKubeProxy(mig2.Id())), nil).Once()
	templateNodeInfo, err := mig2.TemplateNodeInfo()
	assert.NoError(t, err)
	assert.NotNil(t, templateNodeInfo)
	assert.NotNil(t, templateNodeInfo.Node())
	foundKubeProxy := false
	for _, p := range templateNodeInfo.Pods() {
		if strings.Contains(p.Pod.Name, "kube-proxy") {
			foundKubeProxy = true
			break
		}
	}
	assert.True(t, foundKubeProxy, "Unable to find kube proxy static pod.")
	mock.AssertExpectationsForObjects(t, gkeManagerMock)
}

func TestIncreaseSize(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}

	delta := int64(10)

	tests := []struct {
		name            string
		spec            gkeclient.NodePoolSpec
		enabledFeatures []string
		wantCall        string
	}{
		{
			name:     "basic_mig_CreateInstances",
			spec:     gkeclient.NodePoolSpec{},
			wantCall: "CreateInstances",
		},
		{
			name: "single_host_TPU_mig_CreateInstances",
			spec: gkeclient.NodePoolSpec{
				TpuType:      "tpu_type",
				TpuMultiHost: false,
			},
			wantCall: "CreateInstances",
		},
		{
			name: "flex_single_host_TPU_mig_CreateInstances",
			spec: gkeclient.NodePoolSpec{
				FlexStart:    true,
				TpuType:      "tpu_type",
				TpuMultiHost: false,
			},
			wantCall: "CreateInstances",
		},
		{
			name: "flex_no_TPU_mig_CreateFlexResizeRequests",
			spec: gkeclient.NodePoolSpec{
				FlexStart: true,
			},
			wantCall: "CreateFlexResizeRequests",
		},
		{
			name: "flex_trickle_mode_enabled_CreateInstances",
			spec: gkeclient.NodePoolSpec{
				FlexStart: true,
			},
			enabledFeatures: []string{experiments.FlexStartNonQueuedTrickleModeMinCAVersionFlag},
			wantCall:        "CreateInstances",
		},
		{
			name: "multi_host_TPU_mig_CreateResizeRequest",
			spec: gkeclient.NodePoolSpec{
				TpuType:      "tpu_type",
				TpuMultiHost: true,
			},
			wantCall: "CreateResizeRequest",
		},
		{
			name: "flex_multi_host_TPU_mig_CreateResizeRequest",
			spec: gkeclient.NodePoolSpec{
				FlexStart:    true,
				TpuType:      "tpu_type",
				TpuMultiHost: true,
			},
			wantCall: "CreateResizeRequest",
		},
		{
			name: "a4x_CreateInstances",
			spec: gkeclient.NodePoolSpec{
				MachineType:    "a4x-highgpu-4g",
				FlexStart:      false,
				PlacementGroup: placement.Spec{Policy: "a4x-policy"},
			},
			wantCall: "CreateInstances",
		},
		{
			name: "flex_a4x_CreateInstances",
			spec: gkeclient.NodePoolSpec{
				MachineType:    "a4x-highgpu-4g",
				FlexStart:      true,
				PlacementGroup: placement.Spec{Policy: "a4x-policy"},
			},
			wantCall: "CreateInstances",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mig := &GkeMig{
				gceRef: gce.GceRef{
					Name: tc.name,
				},
				gkeManager: gkeManagerMock,
				exist:      true,
				spec:       &tc.spec,
				minSize:    0,
				maxSize:    1000,
			}

			if len(tc.enabledFeatures) > 0 {
				gkeManagerMock.On("ExperimentsManager").Return(experiments.NewMockManager(tc.enabledFeatures...))
			}

			gkeManagerMock.On("GetMigSize", mig).Return(int64(0), nil)
			gkeManagerMock.On(tc.wantCall, mig, delta).Return(nil).Once()

			err := mig.IncreaseSize(int(delta))

			assert.NoError(t, err)
			mock.AssertExpectationsForObjects(t, gkeManagerMock)
		})
	}
}

func TestMigTargetSize(t *testing.T) {
	type rrSpec struct {
		rrclient.ResizeRequestStatus
		isAlreadyReported bool
	}

	allRRs := []rrSpec{
		{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-1", ResizeBy: 1, State: rrclient.ResizeRequestStateAccepted}},
		{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-2", ResizeBy: 2, State: rrclient.ResizeRequestStateCreating}},
		{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-3", ResizeBy: 4, State: rrclient.ResizeRequestStateDeleting}},
		{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-4", ResizeBy: 8, State: rrclient.ResizeRequestStateCancelled}},
		{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-5", ResizeBy: 16, State: rrclient.ResizeRequestStateFailed}},
		{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-6", ResizeBy: 32, State: rrclient.ResizeRequestStateProvisioning}},
		{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-7", ResizeBy: 64, State: rrclient.ResizeRequestStateSucceeded}},
		{isAlreadyReported: true, ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-8", ResizeBy: 128, State: rrclient.ResizeRequestStateAccepted}},
		{isAlreadyReported: true, ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-9", ResizeBy: 256, State: rrclient.ResizeRequestStateCreating}},
		{isAlreadyReported: true, ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-10", ResizeBy: 512, State: rrclient.ResizeRequestStateDeleting}},
		{isAlreadyReported: true, ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-11", ResizeBy: 1024, State: rrclient.ResizeRequestStateCancelled}},
		{isAlreadyReported: true, ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-12", ResizeBy: 2048, State: rrclient.ResizeRequestStateFailed}},
		{isAlreadyReported: true, ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-13", ResizeBy: 4096, State: rrclient.ResizeRequestStateProvisioning}},
		{isAlreadyReported: true, ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-14", ResizeBy: 8192, State: rrclient.ResizeRequestStateSucceeded}},
	}

	tests := map[string]struct {
		tpuType                string
		tpuMultiHost           bool
		rrErrorHandlingEnabled bool
		queuedProvisioning     bool
		flexStart              bool
		migSize                int64
		resizeRequests         []rrSpec
		expectedSize           int
	}{
		"No resize request error handling": {
			rrErrorHandlingEnabled: false,
			tpuType:                "",
			tpuMultiHost:           false,
			migSize:                3,
			resizeRequests:         nil,
			expectedSize:           3,
		},
		"Resize request error handling enabled but not a tpu mig": {
			rrErrorHandlingEnabled: true,
			tpuType:                "",
			tpuMultiHost:           false,
			migSize:                3,
			resizeRequests: []rrSpec{
				{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test", ResizeBy: int64(5), State: rrclient.ResizeRequestStateFailed}},
			},
			expectedSize: 3,
		},
		"Resize request error handling disabled on a tpu mig": {
			rrErrorHandlingEnabled: false,
			tpuType:                "test-tpu",
			tpuMultiHost:           true,
			migSize:                3,
			resizeRequests: []rrSpec{
				{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test", ResizeBy: int64(5), State: rrclient.ResizeRequestStateFailed}},
			},
			expectedSize: 3,
		},
		"Resize request error handling enabled on a tpu mig": {
			rrErrorHandlingEnabled: true,
			tpuType:                "test-tpu",
			tpuMultiHost:           true,
			migSize:                3,
			resizeRequests: []rrSpec{
				{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test", ResizeBy: int64(5), State: rrclient.ResizeRequestStateFailed}},
			},
			expectedSize: 8,
		},
		"Multiple resize request errors on a tpu mig": {
			rrErrorHandlingEnabled: true,
			tpuType:                "test-tpu",
			tpuMultiHost:           true,
			migSize:                3,
			resizeRequests: []rrSpec{
				{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-1", ResizeBy: int64(5), State: rrclient.ResizeRequestStateFailed}},
				{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-2", ResizeBy: int64(5), State: rrclient.ResizeRequestStateAccepted}},
				{ResizeRequestStatus: rrclient.ResizeRequestStatus{Name: "rr-test-3", ResizeBy: int64(5), State: rrclient.ResizeRequestStateCancelled}},
			},
			expectedSize: 8,
		},
		"nonFlexStartMig_noOverwrite": {
			rrErrorHandlingEnabled: true,
			flexStart:              false,
			migSize:                0,
			resizeRequests:         allRRs,
			expectedSize:           0,
		},
		"queuedMig_noOverwrite": {
			rrErrorHandlingEnabled: true,
			flexStart:              false,
			queuedProvisioning:     true,
			migSize:                0,
			resizeRequests:         allRRs,
			expectedSize:           0,
		},
		"queuedFlexStartMig_noOverwrite": {
			rrErrorHandlingEnabled: true,
			flexStart:              true,
			queuedProvisioning:     true,
			migSize:                0,
			resizeRequests:         allRRs,
			expectedSize:           0,
		},
		"nonQueuedFlexStartMig_errorHandlingDisabled_noOverwrite": {
			rrErrorHandlingEnabled: false,
			flexStart:              true,
			queuedProvisioning:     false,
			migSize:                0,
			resizeRequests:         allRRs,
			expectedSize:           0,
		},
		"nonQueuedFlexStartMig_errorHandlingEnabled_overwriteWithFailedAndCancelled_ignoreRRsBeingDeleted": {
			rrErrorHandlingEnabled: true,
			flexStart:              true,
			queuedProvisioning:     false,
			migSize:                0,
			resizeRequests:         allRRs,
			expectedSize:           24,
		},
	}
	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			gkeManagerMock := &GkeManagerMock{}
			mig := GkeMig{
				gkeManager: gkeManagerMock,
				exist:      true,
				spec: &gkeclient.NodePoolSpec{
					TpuType:      tc.tpuType,
					TpuMultiHost: tc.tpuMultiHost,
					FlexStart:    tc.flexStart,
				},
				queuedProvisioning: tc.queuedProvisioning,
			}
			gkeManagerMock.On("IsResizeRequestErrorHandlingEnabled").Return(tc.rrErrorHandlingEnabled)
			gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(tc.migSize, nil)

			var mockRRs []rrclient.ResizeRequestStatus
			for _, rr := range tc.resizeRequests {
				mockRRs = append(mockRRs, rr.ResizeRequestStatus)
				if rr.isAlreadyReported {
					gkeManagerMock.SetReportState(rr.ResizeRequestStatus, rrclient.AlreadyReportedState)
				} else {
					gkeManagerMock.SetReportState(rr.ResizeRequestStatus, rrclient.UnspecifiedReportState)
				}
			}
			gkeManagerMock.On("ResizeRequests", mock.AnythingOfType("*gke.GkeMig")).Return(mockRRs, nil)

			gotSize, err := mig.TargetSize()
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedSize, gotSize)
		})
	}
}

func TestMaxSize(t *testing.T) {
	type extraMig struct {
		zone        string
		currentSize int
	}

	type migFieldsAndExpectation struct {
		zone              string
		maxSizeField      int
		totalMaxSizeField int
		currentSize       int
		otherMigs         []*extraMig
		wantMaxSize       int
	}

	testCases := map[string]migFieldsAndExpectation{
		"MaxSize_returns_2000_when_maxSize_is_higher_than_2000": {
			zone:         "us-central1-a",
			maxSizeField: 2001,
			currentSize:  1,
			wantMaxSize:  2000,
		},
		"MaxSize_returns_maxSize_when_it_is_not_higher_than_2000": {
			zone:         "us-central1-a",
			maxSizeField: 1999,
			currentSize:  1,
			wantMaxSize:  1999,
		},
		"MaxSize_returns_totalMaxSize_minus_otherMigsSizes": {
			zone:              "us-central1-a",
			maxSizeField:      10000,
			totalMaxSizeField: 2001,
			currentSize:       1,
			otherMigs: []*extraMig{
				{
					zone:        "us-central1-b",
					currentSize: 500,
				},
				{
					zone:        "us-central1-c",
					currentSize: 300,
				},
			},
			wantMaxSize: 1201, // 2001 - 500 - 300
		},
		"MaxSize_returns_totalMaxSize_minus_otherMigsSizes_even_if_zero": {
			zone:              "us-central1-a",
			maxSizeField:      10000,
			totalMaxSizeField: 2000,
			currentSize:       1,
			otherMigs: []*extraMig{
				{
					zone:        "us-central1-b",
					currentSize: 2000,
				},
			},
			wantMaxSize: 0,
		},
		"MaxSize_returns_2000_when_totalMaxSize_minus_otherMigSizes_exceeds_2000": {
			zone:              "us-central1-a",
			maxSizeField:      10000,
			totalMaxSizeField: 3000,
			currentSize:       1,
			otherMigs: []*extraMig{
				{
					zone:        "us-central1-b",
					currentSize: 500,
				},
			},
			wantMaxSize: 2000,
		},
		"MaxSize_returns_totalMaxSize_and_ignores_maxSize": {
			zone:              "us-central1-a",
			maxSizeField:      500,
			totalMaxSizeField: 1800,
			currentSize:       1,
			wantMaxSize:       1800,
		},
	}

	for testName, testCase := range testCases {
		t.Name()
		t.Logf("\n\n\n\nRunning %q\n\n\n\n", testName)

		server := NewHttpServerMock()
		defer server.Close()
		gkeManagerMock := &GkeManagerMock{}

		migs := []*GkeMig{}
		migSizesSum := testCase.currentSize

		mig := &GkeMig{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    testCase.zone,
				Name:    "default-pool",
			},
			gkeManager:   gkeManagerMock,
			maxSize:      testCase.maxSizeField,
			totalMaxSize: testCase.totalMaxSizeField,
			exist:        true,
		}
		migs = append(migs, mig)

		if testCase.totalMaxSizeField != 0 {
			for _, extraMigInfo := range testCase.otherMigs {
				extraMig := &GkeMig{
					gceRef: gce.GceRef{
						Project: "project1",
						Zone:    extraMigInfo.zone,
						Name:    "default-pool",
					},
					maxSize:    10000,
					gkeManager: gkeManagerMock,
					exist:      true,
				}
				migs = append(migs, extraMig)
				migSizesSum += extraMigInfo.currentSize
			}
			gkeManagerMock.On("GetMigSize", mig).Return(int64(testCase.currentSize), nil).Once()
			gkeManagerMock.On("GetMigsTargetSize", mock.AnythingOfType("[]gce.GceRef")).Return(int64(migSizesSum), nil).Once()
		}
		AddMigsToNodePool("default-pool", migs...)

		// Check the result of MaxSize() on the main MIG.
		assert.Equal(t, testCase.wantMaxSize, mig.MaxSize())
		mock.AssertExpectationsForObjects(t, gkeManagerMock)
		t.Logf("\n\n\n\n\nPASSED %v\n\n\n\n\n", testName)
	}
}

func TestTotalMigSize(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}

	migs := []*GkeMig{
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-a",
				Name:    "default-pool",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			maxSize:      napMaxNodes,
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-b",
				Name:    "default-pool",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			maxSize:      napMaxNodes,
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-c",
				Name:    "default-pool",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			maxSize:      napMaxNodes,
		},
	}

	AddMigsToNodePool("default-pool", migs...)

	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", migs[0]).Return(0).Times(1)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", migs[1]).Return(1).Times(1)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", migs[2]).Return(0).Times(1)
	gkeManagerMock.On("GetMigSize", migs[0]).Return(int64(4), nil).Times(3)
	gkeManagerMock.On("GetMigSize", migs[1]).Return(int64(6), nil).Times(3)
	gkeManagerMock.On("GetMigSize", migs[2]).Return(int64(2), nil).Times(3)
	gkeManagerMock.On("GetMigsTargetSize", mock.AnythingOfType("[]gce.GceRef")).Return(int64(12), nil).Times(6)

	targetSize0, err := migs[0].TargetSize()
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, targetSize0)
	targetSize1, err := migs[1].TargetSize()
	assert.Equal(t, nil, err)
	assert.Equal(t, 6, targetSize1)
	targetSize2, err := migs[2].TargetSize()
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, targetSize2)

	assert.Equal(t, true, migs[0].TotalSizeLimitEnabled())
	assert.Equal(t, true, migs[1].TotalSizeLimitEnabled())
	assert.Equal(t, true, migs[2].TotalSizeLimitEnabled())
	assert.Equal(t, 1000, migs[0].TotalMaxSize())
	assert.Equal(t, 1000, migs[1].TotalMaxSize())
	assert.Equal(t, 1000, migs[2].TotalMaxSize())
	assert.Equal(t, 3, migs[0].TotalMinSize())
	assert.Equal(t, 3, migs[1].TotalMinSize())
	assert.Equal(t, 3, migs[2].TotalMinSize())

	assert.Equal(t, 0, migs[0].MinSize())
	assert.Equal(t, 1, migs[1].MinSize())
	assert.Equal(t, 0, migs[2].MinSize())
	assert.Equal(t, 992, migs[0].MaxSize())
	assert.Equal(t, 994, migs[1].MaxSize())
	assert.Equal(t, 990, migs[2].MaxSize())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test scenario where total size is greater than total max size by 300.
	gkeManagerMock.On("GetMigSize", migs[0]).Return(int64(200), nil).Once()
	gkeManagerMock.On("GetMigSize", migs[1]).Return(int64(500), nil).Once()
	gkeManagerMock.On("GetMigSize", migs[2]).Return(int64(600), nil).Once()
	gkeManagerMock.On("GetMigsTargetSize", mock.AnythingOfType("[]gce.GceRef")).Return(int64(1300), nil).Times(3)
	assert.Equal(t, 0, migs[0].MaxSize())
	assert.Equal(t, 200, migs[1].MaxSize())
	assert.Equal(t, 300, migs[2].MaxSize())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test GetMigsTargetSize errors scenario.
	gkeManagerMock.On("GetMigsTargetSize", mock.AnythingOfType("[]gce.GceRef")).Return(int64(0), errors.New("test error 4")).Times(6)
	assert.Equal(t, 0, migs[0].MaxSize())
	assert.Equal(t, 0, migs[1].MaxSize())
	assert.Equal(t, 0, migs[2].MaxSize())
	assert.Equal(t, napMaxNodes, migs[0].MinSize())
	assert.Equal(t, napMaxNodes, migs[1].MinSize())
	assert.Equal(t, napMaxNodes, migs[2].MinSize())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)

	// Test GetMigSize errors scenario.
	gkeManagerMock.On("GetMigSize", migs[0]).Return(int64(0), errors.New("test error 1")).Twice()
	gkeManagerMock.On("GetMigSize", migs[1]).Return(int64(0), errors.New("test error 2")).Twice()
	gkeManagerMock.On("GetMigSize", migs[2]).Return(int64(0), errors.New("test error 3")).Twice()
	gkeManagerMock.On("GetMigsTargetSize", mock.AnythingOfType("[]gce.GceRef")).Return(int64(42), nil).Times(6)
	assert.Equal(t, 0, migs[0].MaxSize())
	assert.Equal(t, 0, migs[1].MaxSize())
	assert.Equal(t, 0, migs[2].MaxSize())
	assert.Equal(t, napMaxNodes, migs[0].MinSize())
	assert.Equal(t, napMaxNodes, migs[1].MinSize())
	assert.Equal(t, napMaxNodes, migs[2].MinSize())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)
}

func TestTotalMigSizeRegularBlueGreen(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}

	blueGreenMigs := []*GkeMig{
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-a",
				Name:    "default-pool-blue",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				Color: BlueMig,
				Phase: gkeclient.PhaseUnspecified,
			},
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-b",
				Name:    "default-pool-blue",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				Color: BlueMig,
				Phase: gkeclient.PhaseUnspecified,
			},
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-a",
				Name:    "default-pool-green",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				Color: GreenMig,
				Phase: gkeclient.PhaseUnspecified,
			},
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-b",
				Name:    "default-pool-green",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				Color: GreenMig,
				Phase: gkeclient.PhaseUnspecified,
			},
		},
	}
	AddMigsToNodePool("default-pool", blueGreenMigs...)
	blueMigsGceRefs := []gce.GceRef{
		blueGreenMigs[0].GceRef(),
		blueGreenMigs[1].GceRef(),
	}
	greenMigsGceRefs := []gce.GceRef{
		blueGreenMigs[2].GceRef(),
		blueGreenMigs[3].GceRef(),
	}

	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", blueGreenMigs[0]).Return(0).Times(1)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", blueGreenMigs[1]).Return(1).Times(1)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[0]).Return(int64(4), nil).Times(3)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[1]).Return(int64(6), nil).Times(3)
	gkeManagerMock.On("GetMigsTargetSize", blueMigsGceRefs).Return(int64(10), nil).Times(4)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", blueGreenMigs[2]).Return(0).Times(1)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", blueGreenMigs[3]).Return(0).Times(1)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[2]).Return(int64(3), nil).Times(3)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[3]).Return(int64(9), nil).Times(3)
	gkeManagerMock.On("GetMigsTargetSize", greenMigsGceRefs).Return(int64(12), nil).Times(4)

	targetSize0, err := blueGreenMigs[0].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 4, targetSize0)
	targetSize1, err := blueGreenMigs[1].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 6, targetSize1)
	targetSize2, err := blueGreenMigs[2].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 3, targetSize2)
	targetSize3, err := blueGreenMigs[3].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 9, targetSize3)

	assert.True(t, blueGreenMigs[0].TotalSizeLimitEnabled())
	assert.True(t, blueGreenMigs[1].TotalSizeLimitEnabled())
	assert.True(t, blueGreenMigs[2].TotalSizeLimitEnabled())
	assert.True(t, blueGreenMigs[3].TotalSizeLimitEnabled())
	assert.Equal(t, 1000, blueGreenMigs[0].TotalMaxSize())
	assert.Equal(t, 1000, blueGreenMigs[1].TotalMaxSize())
	assert.Equal(t, 1000, blueGreenMigs[2].TotalMaxSize())
	assert.Equal(t, 1000, blueGreenMigs[3].TotalMaxSize())
	assert.Equal(t, 3, blueGreenMigs[0].TotalMinSize())
	assert.Equal(t, 3, blueGreenMigs[1].TotalMinSize())
	assert.Equal(t, 3, blueGreenMigs[2].TotalMinSize())
	assert.Equal(t, 3, blueGreenMigs[3].TotalMinSize())

	assert.Equal(t, 0, blueGreenMigs[0].MinSize())
	assert.Equal(t, 1, blueGreenMigs[1].MinSize())
	assert.Equal(t, 0, blueGreenMigs[2].MinSize())
	assert.Equal(t, 0, blueGreenMigs[3].MinSize())
	assert.Equal(t, 994, blueGreenMigs[0].MaxSize())
	assert.Equal(t, 996, blueGreenMigs[1].MaxSize())
	assert.Equal(t, 991, blueGreenMigs[2].MaxSize())
	assert.Equal(t, 997, blueGreenMigs[3].MaxSize())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)
}

func TestTotalMigSizeAutoscaledBlueGreen(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}

	blueGreenMigs := []*GkeMig{
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-a",
				Name:    "default-pool-blue",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				IsAutoScaled: true,
				Color:        BlueMig,
				// This blue pool should be enabled for scaling down to zero nodes.
				Phase: gkeclient.PhaseWaitingToDrainBluePool,
			},
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-b",
				Name:    "default-pool-blue",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				IsAutoScaled: true,
				Color:        BlueMig,
				Phase:        gkeclient.PhaseCordoningBluePool,
			},
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-a",
				Name:    "default-pool-green",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				IsAutoScaled: true,
				Color:        GreenMig,
				Phase:        gkeclient.PhaseWaitingToDrainBluePool,
			},
		},
		{
			gceRef: gce.GceRef{
				Project: "project1",
				Zone:    "us-central1-b",
				Name:    "default-pool-green",
			},
			gkeManager:   gkeManagerMock,
			totalMinSize: 3,
			totalMaxSize: 1000,
			exist:        true,
			blueGreenInfo: &MigBlueGreenInfo{
				IsAutoScaled: true,
				Color:        GreenMig,
				Phase:        gkeclient.PhaseCordoningBluePool,
			},
		},
	}
	AddMigsToNodePool("default-pool", blueGreenMigs...)
	blueMigsGceRefs := []gce.GceRef{
		blueGreenMigs[0].GceRef(),
		blueGreenMigs[1].GceRef(),
	}
	greenMigsGceRefs := []gce.GceRef{
		blueGreenMigs[2].GceRef(),
		blueGreenMigs[3].GceRef(),
	}

	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", blueGreenMigs[1]).Return(1).Times(1)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[0]).Return(int64(4), nil).Times(2)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[1]).Return(int64(6), nil).Times(3)
	gkeManagerMock.On("GetMigsTargetSize", blueMigsGceRefs).Return(int64(10), nil).Times(3)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", blueGreenMigs[2]).Return(0).Times(1)
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", blueGreenMigs[3]).Return(0).Times(1)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[2]).Return(int64(3), nil).Times(3)
	gkeManagerMock.On("GetMigSize", blueGreenMigs[3]).Return(int64(9), nil).Times(3)
	gkeManagerMock.On("GetMigsTargetSize", greenMigsGceRefs).Return(int64(12), nil).Times(4)

	targetSize0, err := blueGreenMigs[0].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 4, targetSize0)
	targetSize1, err := blueGreenMigs[1].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 6, targetSize1)
	targetSize2, err := blueGreenMigs[2].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 3, targetSize2)
	targetSize3, err := blueGreenMigs[3].TargetSize()
	assert.NoError(t, err)
	assert.Equal(t, 9, targetSize3)

	assert.True(t, blueGreenMigs[0].TotalSizeLimitEnabled())
	assert.True(t, blueGreenMigs[1].TotalSizeLimitEnabled())
	assert.True(t, blueGreenMigs[2].TotalSizeLimitEnabled())
	assert.True(t, blueGreenMigs[3].TotalSizeLimitEnabled())
	assert.Equal(t, 1000, blueGreenMigs[0].TotalMaxSize())
	assert.Equal(t, 1000, blueGreenMigs[1].TotalMaxSize())
	assert.Equal(t, 1000, blueGreenMigs[2].TotalMaxSize())
	assert.Equal(t, 1000, blueGreenMigs[3].TotalMaxSize())
	assert.Equal(t, 0, blueGreenMigs[0].TotalMinSize())
	assert.Equal(t, 3, blueGreenMigs[1].TotalMinSize())
	assert.Equal(t, 3, blueGreenMigs[2].TotalMinSize())
	assert.Equal(t, 3, blueGreenMigs[3].TotalMinSize())

	assert.Equal(t, 0, blueGreenMigs[0].MinSize())
	assert.Equal(t, 1, blueGreenMigs[1].MinSize())
	assert.Equal(t, 0, blueGreenMigs[2].MinSize())
	assert.Equal(t, 0, blueGreenMigs[3].MinSize())
	assert.Equal(t, 994, blueGreenMigs[0].MaxSize())
	assert.Equal(t, 996, blueGreenMigs[1].MaxSize())
	assert.Equal(t, 991, blueGreenMigs[2].MaxSize())
	assert.Equal(t, 997, blueGreenMigs[3].MaxSize())
	mock.AssertExpectationsForObjects(t, gkeManagerMock)
}

type placementSetter struct {
	puller placement.ResourcePolicyPuller
}

func (cp placementSetter) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	spec.PlacementGroup = placement.FromLabels(systemLabels)
	if cp.puller != nil && systemLabels[gkelabels.PolicyLabel] != "" {
		spec.PlacementGroup.ResourcePolicy = cp.puller.GetResourcePolicy(systemLabels[gkelabels.PolicyLabel])
	}
	return nil
}

func TestNewNodeNameWithCompactPlacement(t *testing.T) {
	defaultFamily := machinetypes.E2
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}
	client := &http.Client{}
	gceService, err := gcev1.NewService(context.Background(), option.WithHTTPClient(client))
	assert.NoError(t, err)
	gceService.BasePath = server.URL
	gke := &gkeCloudProviderImpl{
		gkeManager:              gkeManagerMock,
		compactPlacementEnabled: true,
		nodePoolSpecBuilders:    []napcloudprovider.NodePoolSpecBuilder{placementSetter{}},
		machineConfigProvider:   machinetypes.NewMachineConfigProvider(nil),
	}

	zone := "ss-moon-1"
	machineType := "e2-medium"
	gkeManagerMock.On("GetProjectId").Return("project1")
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Once()
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	gkeManagerMock.On("GetAutoprovisioningLocations").Return([]string{zone}).Once()
	gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(100))
	gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(defaultFamily)

	systemLabels := map[string]string{
		apiv1.LabelZoneFailureDomain:  zone,
		gkelabels.PlacementGroupLabel: "placement-id",
	}

	nodeGroup, err := gke.NewNodeGroup(machineType, nil, systemLabels, nil, nil)
	assert.NoError(t, err)
	mig := nodeGroup.(*GkeMig)
	assert.Equal(t, "placement-id", mig.NodePoolName())
}

type MockSpotNodePoolSpecBuilder struct {
	mock.Mock
}

func (s *MockSpotNodePoolSpecBuilder) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	spec.Spot = true
	return nil
}

func TestNewNodeLocationPolicyDefaultsToAnyForSpot(t *testing.T) {
	defaultFamily := machinetypes.E2
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}
	client := &http.Client{}
	gceService, err := gcev1.NewService(context.Background(), option.WithHTTPClient(client))
	assert.NoError(t, err)
	gceService.BasePath = server.URL
	mockSpotNodePoolSpecBuilder := &MockSpotNodePoolSpecBuilder{}
	gke := &gkeCloudProviderImpl{
		gkeManager:            gkeManagerMock,
		nodePoolSpecBuilders:  []napcloudprovider.NodePoolSpecBuilder{mockSpotNodePoolSpecBuilder},
		machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
	}

	zone := "ss-moon-1"
	machineType := "e2-medium"
	gkeManagerMock.On("GetProjectId").Return("project1")
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Once()
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	gkeManagerMock.On("GetAutoprovisioningLocations").Return([]string{zone}).Once()
	gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(100))
	gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(defaultFamily)

	systemLabels := map[string]string{
		apiv1.LabelZoneFailureDomain:  zone,
		gkelabels.PlacementGroupLabel: "placement-id",
	}

	nodeGroup, err := gke.NewNodeGroup(machineType, nil, systemLabels, nil, nil)
	assert.NoError(t, err)
	mig := nodeGroup.(*GkeMig)
	assert.Equal(t, LocationPolicyAny, mig.locationPolicy)
}

type MockSelfServiceNodePoolSpecBuilder struct {
	mock.Mock
}

func (s *MockSelfServiceNodePoolSpecBuilder) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	spec.SelfServiceMetadata = selfservice.Metadata{gkelabels.LocationPolicyLabelKey: systemLabels[gkelabels.LocationPolicyLabelKey]}
	return nil
}

func TestNewNodeLocationPolicyUsesSelfService(t *testing.T) {
	defaultFamily := machinetypes.E2
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}
	client := &http.Client{}
	gceService, err := gcev1.NewService(context.Background(), option.WithHTTPClient(client))
	assert.NoError(t, err)
	gceService.BasePath = server.URL
	mockSelfServiceNodePoolSpecBuilder := &MockSelfServiceNodePoolSpecBuilder{}
	gke := &gkeCloudProviderImpl{
		gkeManager:            gkeManagerMock,
		nodePoolSpecBuilders:  []napcloudprovider.NodePoolSpecBuilder{mockSelfServiceNodePoolSpecBuilder},
		machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
	}

	zone := "ss-moon-1"
	machineType := "e2-medium"
	gkeManagerMock.On("GetProjectId").Return("project1")
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Once()
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	gkeManagerMock.On("GetAutoprovisioningLocations").Return([]string{zone}).Once()
	gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(100))
	gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(defaultFamily)

	systemLabels := map[string]string{
		apiv1.LabelZoneFailureDomain:     zone,
		gkelabels.LocationPolicyLabelKey: "ANY",
	}

	nodeGroup, err := gke.NewNodeGroup(machineType, nil, systemLabels, nil, nil)
	assert.NoError(t, err)
	mig := nodeGroup.(*GkeMig)
	assert.Equal(t, LocationPolicyAny, mig.locationPolicy)
}

func TestGceRefFromProviderId(t *testing.T) {
	ref, err := gce.GceRefFromProviderId("gce://project1/us-central1-b/name1")
	assert.NoError(t, err)
	assert.Equal(t, gce.GceRef{Project: "project1", Zone: "us-central1-b", Name: "name1"}, ref)
}

func TestGetClusterInfo(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}
	gke := &gkeCloudProviderImpl{
		gkeManager: gkeManagerMock,
	}
	gkeManagerMock.On("GetProjectId").Return("project1")
	gkeManagerMock.On("GetLocation").Return("location1").Once()
	gkeManagerMock.On("GetClusterName").Return("cluster1").Once()

	project, location, cluster := gke.GetClusterInfo()
	assert.Equal(t, "project1", project)
	assert.Equal(t, "location1", location)
	assert.Equal(t, "cluster1", cluster)
}

func TestCropAutoprovisionedMachineTypes(t *testing.T) {
	// this test verifies that all autoprovisioned machine types are cropped correctly
	for _, machineFamily := range machinetypes.NewMachineConfigProvider(nil).AllMachineFamilies() {
		for _, machineType := range machineFamily.AutoprovisionedMachineTypes(machinetypes.NoConstraints) {
			assert.True(t, len(cropMachineType(machineType.Name)) <= maxMachineTypeLength)
		}
	}
}

func TestCropMachineType(t *testing.T) {
	// this test verifies that the cropMachineType function works as expected
	tests := []struct {
		original string
		expected string
	}{
		{"n1-standard-64", "n1-standard-64"},
		{"n123-standard-64", "n123-standa-64"},
		{"n1-standard-1024", "n1-standa-1024"},
		{"n123-standard-1024", "n123-stan-1024"},
		{"n123456-s-1048576", "3456-s-1048576"},
		{"n123456-standard-1048576", "andard-1048576"},
		{"someunknownlongformatting", "longformatting"},
	}
	for _, tt := range tests {
		t.Run(tt.original, func(t *testing.T) {
			cropped := cropMachineType(tt.original)
			assert.Equal(t, tt.expected, cropped)
		})
	}
}

func TestNodePoolNameWithAutopilotForNewNodeGroup(t *testing.T) {
	// this test verifies if NewNodeGroup() creates the correct nodepoolname for autopilot
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}
	client := &http.Client{}
	gceService, err := gcev1.NewService(context.Background(), option.WithHTTPClient(client))
	assert.NoError(t, err)
	gceService.BasePath = server.URL
	gke := &gkeCloudProviderImpl{
		gkeManager:            gkeManagerMock,
		autopilotEnabled:      true,
		machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
	}

	zone := "us-west1-b"
	machineType := "n1-standard-1"

	// Test NewNodeGroup.
	gkeManagerMock.On("GetProjectId").Return("project1")
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Once()
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	gkeManagerMock.On("GetAutoprovisioningLocations").Return([]string{zone}).Once()
	gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(100))
	gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")

	systemLabels := map[string]string{
		apiv1.LabelZoneFailureDomain: zone,
	}

	nodeGroup, err := gke.NewNodeGroup(machineType, make(map[string]string), systemLabels, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, nodeGroup)
	mig1 := reflect.ValueOf(nodeGroup).Interface().(*GkeMig)
	assert.Regexp(t, "^nap-[a-z0-9]{8}", mig1.NodePoolName())
}

func TestReservationAffinityDefaulting(t *testing.T) {
	testCases := []struct {
		desc             string
		autopilotEnabled bool
		managedNodeLabel bool
		expected         *gke_api_beta.ReservationAffinity
	}{
		{
			desc:             "no autopilot",
			autopilotEnabled: false,
		}, {
			desc:             "autopilot",
			autopilotEnabled: true,
			expected: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinityNone,
			},
		}, {
			desc:             "autopilot managed node-pool",
			autopilotEnabled: false,
			managedNodeLabel: true,
			expected: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinityNone,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()
			gkeManagerMock := &GkeManagerMock{}
			client := &http.Client{}
			gceService, err := gcev1.NewService(context.Background(), option.WithHTTPClient(client))
			assert.NoError(t, err)
			gceService.BasePath = server.URL
			gke := &gkeCloudProviderImpl{
				gkeManager:            gkeManagerMock,
				autopilotEnabled:      tc.autopilotEnabled,
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			}

			zone := "us-west1-b"
			machineType := "n1-standard-1"

			// Test NewNodeGroup.
			gkeManagerMock.On("GetProjectId").Return("project1")
			gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Once()
			gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
			gkeManagerMock.On("GetAutoprovisioningLocations").Return([]string{zone}).Once()
			gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(100))
			gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")

			systemLabels := map[string]string{
				apiv1.LabelZoneFailureDomain: zone,
			}
			if tc.managedNodeLabel {
				systemLabels[gkelabels.ManagedNodeLabel] = "true"
			}

			nodeGroup, err := gke.NewNodeGroup(machineType, make(map[string]string), systemLabels, nil, nil)
			assert.NoError(t, err)
			assert.NotNil(t, nodeGroup)
			mig := reflect.ValueOf(nodeGroup).Interface().(*GkeMig)
			assert.Equal(t, tc.expected, mig.Spec().ReservationAffinity)
		})
	}
}

func TestNodePoolNameForNewNodeGroup(t *testing.T) {
	// this test verifies if NewNodeGroup() creates the correct nodepoolname
	server := NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &GkeManagerMock{}
	client := &http.Client{}
	gceService, err := gcev1.NewService(context.Background(), option.WithHTTPClient(client))
	assert.NoError(t, err)
	gceService.BasePath = server.URL
	gke := &gkeCloudProviderImpl{
		gkeManager:            gkeManagerMock,
		machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
	}

	zone := "us-west1-b"
	machineType := "n1-standard-1"

	// Test NewNodeGroup.
	gkeManagerMock.On("GetProjectId").Return("project1")
	gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Once()
	gkeManagerMock.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	gkeManagerMock.On("GetAutoprovisioningLocations").Return([]string{zone}).Once()
	gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(100))
	gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")

	systemLabels := map[string]string{
		apiv1.LabelZoneFailureDomain: zone,
	}

	nodeGroup, err := gke.NewNodeGroup(machineType, make(map[string]string), systemLabels, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, nodeGroup)
	mig1 := reflect.ValueOf(nodeGroup).Interface().(*GkeMig)
	assert.Regexp(t, "^nap-n1-standard-1-[a-z0-9]{8}", mig1.NodePoolName())
}

type migForNodeGkeManagerMock struct {
	*gkeManagerImpl
	mig gce.Mig
}

func (m *migForNodeGkeManagerMock) GetMigForInstance(instance gce.GceRef) (gce.Mig, error) {
	return m.mig, nil
}

func TestGkeMigForNode(t *testing.T) {
	type notMigType struct {
		*GkeMig
	}
	notMig := &notMigType{}
	gkeMig := &GkeMig{gceRef: gce.GceRef{Name: "test-mig"}}

	for tn, tc := range map[string]struct {
		gceMig  gce.Mig
		wantMig *GkeMig
		wantErr error
	}{
		"(*GkeMig, nil) -> nil": {
			gceMig:  (*GkeMig)(nil),
			wantMig: nil,
		},
		"(*GkeMig, non-nil) -> GkeMig": {
			gceMig:  gkeMig,
			wantMig: gkeMig,
		},
		"(notMigType, nil) -> nil": {
			gceMig:  (*notMigType)(nil),
			wantMig: nil,
		},
		"(notMigType, non-nil) -> error": {
			gceMig:  notMig,
			wantErr: cmpopts.AnyError,
		},
		"(nil, nil) -> nil": {
			gceMig:  nil,
			wantMig: nil,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			manager := &migForNodeGkeManagerMock{mig: tc.gceMig}
			provider := &gkeCloudProviderImpl{gkeManager: manager}
			node := BuildTestNode("n1", 1000, 1000)
			node.Spec.ProviderID = "gce://project1/us-central1-b/n1"
			gotMig, gotErr := provider.GkeMigForNode(node)
			compareAllUnexportedOpt := cmp.Exporter(func(r reflect.Type) bool { return true })
			if diff := cmp.Diff(tc.wantMig, gotMig, compareAllUnexportedOpt); diff != "" {
				t.Errorf("MigForNode diff (-want +got): %s", diff)
			}
			if diff := cmp.Diff(tc.wantErr, gotErr, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("MigForNode error diff (-want +got): %s", diff)
			}
		})
	}
}

func TestGetNodeGpuConfig(t *testing.T) {
	gke := &gkeCloudProviderImpl{}

	for tn, tc := range map[string]struct {
		resourceName     apiv1.ResourceName
		acceleratorLabel string
		acceleratorType  string
		draLabel         string
		expectedConfig   *cloudprovider.GpuConfig
	}{
		"Node with no accelerator": {
			resourceName:     "",
			acceleratorLabel: "",
			acceleratorType:  "",
			expectedConfig:   nil,
		},
		"Node with GPU accelerator": {
			resourceName:     gpu.ResourceNvidiaGPU,
			acceleratorLabel: gkelabels.GPULabel,
			acceleratorType:  machinetypes.NvidiaTeslaK80.Name(),
			expectedConfig:   &cloudprovider.GpuConfig{Label: gke.GPULabel(), Type: machinetypes.NvidiaTeslaK80.Name(), ExtendedResourceName: gpu.ResourceNvidiaGPU},
		},
		"Node with GPU accelerator via DRA": {
			acceleratorLabel: gkelabels.GPULabel,
			acceleratorType:  machinetypes.NvidiaTeslaK80.Name(),
			draLabel:         gkelabels.DraGpuNodeLabel,
			expectedConfig:   &cloudprovider.GpuConfig{Label: gke.GPULabel(), Type: machinetypes.NvidiaTeslaK80.Name(), DraDriverName: dynamicresources.GpuDriver},
		},
		"Node with TPU accelerator": {
			resourceName:     tpu.ResourceGoogleTPU,
			acceleratorLabel: gkelabels.TPULabel,
			acceleratorType:  gkelabels.TpuV4LiteDeviceValue,
			expectedConfig:   &cloudprovider.GpuConfig{Label: gkelabels.TPULabel, Type: gkelabels.TpuV4LiteDeviceValue, ExtendedResourceName: tpu.ResourceGoogleTPU},
		},
		"Node with TPU accelerator via DRA": {
			acceleratorLabel: gkelabels.TPULabel,
			acceleratorType:  gkelabels.TpuV4LiteDeviceValue,
			draLabel:         gkelabels.DraTpuNodeLabel,
			expectedConfig:   &cloudprovider.GpuConfig{Label: gkelabels.TPULabel, Type: gkelabels.TpuV4LiteDeviceValue, DraDriverName: dynamicresources.TpuDriver},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			node := BuildTestNode("testNode", 1000, 1000)
			if tc.resourceName != "" {
				node.Status.Capacity[tc.resourceName] = *resource.NewQuantity(1, resource.DecimalSI)
				node.Status.Allocatable[tc.resourceName] = *resource.NewQuantity(1, resource.DecimalSI)
			}
			if tc.acceleratorLabel != "" {
				node.Labels[tc.acceleratorLabel] = tc.acceleratorType
			}
			if tc.draLabel != "" {
				node.Labels[tc.draLabel] = "true"
			}
			gotConfig := gke.GetNodeGpuConfig(node)
			assert.Equal(t, tc.expectedConfig, gotConfig)
		})
	}
}

func TestGkeMigCreateQueuedInstances(t *testing.T) {
	tests := []struct {
		name           string
		managerErr     error
		migSize        int64
		maxSize        int
		bulkMig        bool
		delta          int
		wantErr        bool
		wantErrMessage string
	}{
		{
			name:    "simple scale up",
			migSize: 2,
			maxSize: 100,
			bulkMig: false,
			delta:   1,
		},
		{
			name:           "fail on wrong size",
			migSize:        2,
			maxSize:        100,
			bulkMig:        false,
			delta:          0,
			wantErr:        true,
			wantErrMessage: "size increase must be positive",
		},
		{
			name:           "fail on too big delta",
			migSize:        2,
			delta:          1000,
			maxSize:        1000,
			bulkMig:        false,
			wantErr:        true,
			wantErrMessage: "size increase too large - desired:1002 max:1000",
		},
		{
			name:           "CreateResizeRequest fails",
			managerErr:     errors.New("test error"),
			migSize:        2,
			delta:          1,
			bulkMig:        false,
			maxSize:        100,
			wantErr:        true,
			wantErrMessage: "test error",
		},
		{
			name:           "CreateResizeRequest fails service account deleted",
			managerErr:     errors.New("Service account test-account does not exist"),
			migSize:        2,
			delta:          1,
			bulkMig:        false,
			maxSize:        100,
			wantErr:        true,
			wantErrMessage: "service account deleted: Service account test-account does not exist",
		},
		{
			name:           "CreateResizeRequest fails service account deleted",
			managerErr:     errors.New("out of cpu quota"),
			migSize:        2,
			delta:          1,
			bulkMig:        false,
			maxSize:        100,
			wantErr:        true,
			wantErrMessage: "insufficient regional quota for project: out of cpu quota",
		},
		{
			name:    "bulk",
			migSize: 0,
			maxSize: 100,
			bulkMig: true,
			delta:   5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gkeManagerMock := &GkeManagerMock{}
			mig := &GkeMig{
				gceRef: gce.GceRef{
					Project: "test-project",
					Zone:    "test-zone",
					Name:    "test-name",
				},
				gkeManager: gkeManagerMock,
				maxSize:    tt.maxSize,
				exist:      true,
				spec: &gkeclient.NodePoolSpec{
					MachineType: "n1-standard-2",
				},
			}
			AddMigsToNodePool("test-nodepool", mig)

			if tt.bulkMig {
				mig.spec.FlexStart = true
				mig.spec.MachineType = "a4x-highgpu-4g"
				mig.spec.PlacementGroup.Policy = "a4x-policy"
			}
			assert.Equal(t, tt.bulkMig, mig.UsesBulkProvisioning())

			gkeManagerMock.On("GetMigSize", mig).Return(tt.migSize, nil).Once()
			pr := prpods.ProvReqID{Namespace: "prNamespace", Name: "prName"}
			gkeManagerMock.On("CreateQueuedInstances", pr, mig, int64(tt.delta)).Return(tt.managerErr).Once()
			gkeManagerMock.On("GetGkeMigs").Return([]*GkeMig{mig}).Once()
			gkeManagerMock.On("GetMigsTargetSize", []gce.GceRef{mig.gceRef}).Return(tt.migSize, nil)

			err := mig.CreateQueuedInstances(pr, tt.delta, manager.UpdateProvReqDetails)
			if tt.wantErr != (err != nil) {
				t.Fatalf("wantedError: %t, but got: %v", tt.wantErr, err)
			}
			if len(tt.wantErrMessage) > 0 && tt.wantErrMessage != err.Error() {
				t.Errorf("wantErrMessage: %s, but got: %s", tt.wantErrMessage, err.Error())
			}
		})
	}
}

var (
	queuedScaledownUnreadyTime          = 66 * time.Minute
	npcScaledownUnneededTime            = time.Minute
	npcScaleDownUtilizationThreshold    = 0.5
	npcScaleDownGpuUtilizationThreshold = 0.6
)

func TestGkeMigGetOptions(t *testing.T) {
	defaults := config.NodeGroupAutoscalingOptions{
		ScaleDownUtilizationThreshold:    0.5,
		ScaleDownGpuUtilizationThreshold: 0.5,
		ScaleDownUnneededTime:            10 * time.Minute,
		ScaleDownUnreadyTime:             20 * time.Minute,
		MaxNodeProvisionTime:             15 * time.Minute,
		ZeroOrMaxNodeScaling:             false,
		AllowNonAtomicScaleUpToMax:       false,
	}
	tests := []struct {
		name                                     string
		mig                                      *GkeMig
		scaleDownUnreadyTimeOverride             *time.Duration
		scaleDownUnneededTimeOverride            *time.Duration
		scaleDownUtilizationThresholdOverride    *float64
		scaleDownGpuUtilizationThresholdOverride *float64
		maxNodeProvisionTimeOverride             *time.Duration
		capacityCheckWaitTime                    *time.Duration
		capacityCheckWaitTimeErr                 error
		want                                     *config.NodeGroupAutoscalingOptions
	}{
		{
			name: "Default mig - default options",
			mig:  &GkeMig{},
			want: &defaults,
		},
		{
			name: "non QueuedProvisioning Mig - default options",
			mig: &GkeMig{
				queuedProvisioning: false,
			},
			want: &defaults,
		},
		{
			name: "QueuedProvisioning Mig - 3 days MaxNodeProvisionTime",
			mig: &GkeMig{
				queuedProvisioning: true,
			},
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				MaxNodeProvisionTime:             queuedProvisioningMaxNodeProvisionTime,
				ZeroOrMaxNodeScaling:             false,
			},
		},
		{
			name: "Bulk Mig - ZeroOrMaxNodeScaling override",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:      true,
					MachineType:    "a4x-highgpu-4g",
					PlacementGroup: placement.Spec{Policy: placement.Compact},
				},
			},
			capacityCheckWaitTime: ptr.To(17 * time.Minute),
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				MaxNodeProvisionTime:             17*time.Minute + maxNodeProvisionTimeOffset,
				ZeroOrMaxNodeScaling:             true,
				AllowNonAtomicScaleUpToMax:       true,
			},
		},
		{
			name: "QueuedProvisioning Mig - ScaleDownUnreadyTime override",
			mig: &GkeMig{
				queuedProvisioning: true,
			},
			scaleDownUnreadyTimeOverride: &queuedScaledownUnreadyTime,
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUnreadyTime:             queuedScaledownUnreadyTime,
				MaxNodeProvisionTime:             queuedProvisioningMaxNodeProvisionTime,
				ZeroOrMaxNodeScaling:             false,
			},
		},
		{
			name: "DWS Flex Start Mig - MaxNodeProvisionTime 15 min longer than CapacityCheckWaitTime",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
				},
			},
			capacityCheckWaitTime: ptr.To(17 * time.Minute),
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             17*time.Minute + maxNodeProvisionTimeOffset,
				ZeroOrMaxNodeScaling:             false,
			},
		},
		{
			name: "DWS Flex Start Mig - CapacityCheckWaitTime error - default flex MaxNodeProvisionTime 30min",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
				},
			},
			capacityCheckWaitTimeErr: fmt.Errorf("Failed to get ccwt"),
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             DefaultFlexStartCapacityCheckWaitTime + maxNodeProvisionTimeOffset,
				ZeroOrMaxNodeScaling:             false,
			},
		},
		{
			name: "DWS Flex Start Multi host TPU Mig - MaxNodeProvisionTime 15 min longer than CapacityCheckWaitTime",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    true,
					TpuType:      "ct4p-hightpu-4t",
					TpuMultiHost: true,
				},
			},
			capacityCheckWaitTime: ptr.To(7 * time.Minute),
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             7*time.Minute + maxNodeProvisionTimeOffset,
				ZeroOrMaxNodeScaling:             true,
			},
		},
		{
			name: "Empty TPU Type Mig - default options",
			mig: &GkeMig{
				queuedProvisioning: false,
				spec: &gkeclient.NodePoolSpec{
					TpuType: "",
				},
			},
			want: &defaults,
		},
		{
			name: "Tpu Podslice mig - MaxNodeProvisionTime based on capacityCheckWaitTime",
			mig: &GkeMig{
				queuedProvisioning: false,
				spec: &gkeclient.NodePoolSpec{
					TpuType:      "ct4p-hightpu-4t",
					TpuMultiHost: true,
				},
			},
			capacityCheckWaitTime: ptr.To(13 * time.Minute),
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             13*time.Minute + maxNodeProvisionTimeOffset,
				ZeroOrMaxNodeScaling:             true,
			},
		},
		{
			name: "Tpu Podslice mig -  capacityCheckWaitTime error - default MaxNodeProvisionTime",
			mig: &GkeMig{
				queuedProvisioning: false,
				spec: &gkeclient.NodePoolSpec{
					TpuType:      "ct4p-hightpu-4t",
					TpuMultiHost: true,
				},
			},
			capacityCheckWaitTimeErr: fmt.Errorf("Failed to get ccwt"),
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             tpuMigMaxNodeProvisionTime,
				ZeroOrMaxNodeScaling:             true,
			},
		},
		{
			name: "Tpu Device mig - MaxNodeProvisionTime",
			mig: &GkeMig{
				queuedProvisioning: false,
				spec: &gkeclient.NodePoolSpec{
					TpuType:      "ct4l-hightpu-4t",
					TpuMultiHost: false,
				},
			},
			maxNodeProvisionTimeOverride: ptr.To(3 * time.Minute),
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             3 * time.Minute,
				ZeroOrMaxNodeScaling:             false,
			},
		},
		{
			name:                                     "NPC mig - consolidation controls",
			mig:                                      &GkeMig{},
			scaleDownUnneededTimeOverride:            &npcScaledownUnneededTime,
			scaleDownUtilizationThresholdOverride:    &npcScaleDownUtilizationThreshold,
			scaleDownGpuUtilizationThresholdOverride: &npcScaleDownGpuUtilizationThreshold,
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    npcScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: npcScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            npcScaledownUnneededTime,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             defaults.MaxNodeProvisionTime,
				ZeroOrMaxNodeScaling:             defaults.ZeroOrMaxNodeScaling,
			},
		},
		{
			name:                          "NPC mig - consolidation controls, partial delay",
			mig:                           &GkeMig{},
			scaleDownUnneededTimeOverride: &npcScaledownUnneededTime,
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUnneededTime:            npcScaledownUnneededTime,
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             defaults.MaxNodeProvisionTime,
				ZeroOrMaxNodeScaling:             defaults.ZeroOrMaxNodeScaling,
			},
		},
		{
			name:                                  "NPC mig - consolidation controls, partial threshold",
			mig:                                   &GkeMig{},
			scaleDownUtilizationThresholdOverride: &npcScaleDownUtilizationThreshold,
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownUtilizationThreshold:    npcScaleDownUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownGpuUtilizationThreshold: defaults.ScaleDownGpuUtilizationThreshold,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             defaults.MaxNodeProvisionTime,
				ZeroOrMaxNodeScaling:             defaults.ZeroOrMaxNodeScaling,
			},
		},
		{
			name:                                     "NPC mig - consolidation controls, partial gpu threshold",
			mig:                                      &GkeMig{},
			scaleDownGpuUtilizationThresholdOverride: &npcScaleDownGpuUtilizationThreshold,
			want: &config.NodeGroupAutoscalingOptions{
				ScaleDownGpuUtilizationThreshold: npcScaleDownGpuUtilizationThreshold,
				ScaleDownUnneededTime:            defaults.ScaleDownUnneededTime,
				ScaleDownUtilizationThreshold:    defaults.ScaleDownUtilizationThreshold,
				ScaleDownUnreadyTime:             defaults.ScaleDownUnreadyTime,
				MaxNodeProvisionTime:             defaults.MaxNodeProvisionTime,
				ZeroOrMaxNodeScaling:             defaults.ZeroOrMaxNodeScaling,
			},
		},
	}

	var noError error
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gkeManagerMock := GkeManagerMock{haveAutoscalingOptsOverrides: true}
			if tt.scaleDownUnreadyTimeOverride != nil {
				gkeManagerMock.On("ScaleDownUnreadyTimeOverride", tt.mig).Return(*tt.scaleDownUnreadyTimeOverride, true)
			} else {
				gkeManagerMock.On("ScaleDownUnreadyTimeOverride", tt.mig).Return(time.Duration(0), false)
			}

			if tt.scaleDownUnneededTimeOverride != nil {
				gkeManagerMock.On("ScaleDownUnneededTimeOverride", tt.mig).Return(*tt.scaleDownUnneededTimeOverride, true, noError)
			} else {
				gkeManagerMock.On("ScaleDownUnneededTimeOverride", tt.mig).Return(time.Duration(0), false, noError)
			}

			if tt.scaleDownUtilizationThresholdOverride != nil {
				gkeManagerMock.On("ScaleDownUtilizationThresholdOverride", tt.mig).Return(*tt.scaleDownUtilizationThresholdOverride, true, noError)
			} else {
				gkeManagerMock.On("ScaleDownUtilizationThresholdOverride", tt.mig).Return(0.0, false, noError)
			}

			if tt.scaleDownGpuUtilizationThresholdOverride != nil {
				gkeManagerMock.On("ScaleDownGpuUtilizationThresholdOverride", tt.mig).Return(*tt.scaleDownGpuUtilizationThresholdOverride, true, noError)
			} else {
				gkeManagerMock.On("ScaleDownGpuUtilizationThresholdOverride", tt.mig).Return(0.0, false, noError)
			}

			if tt.capacityCheckWaitTime != nil {
				gkeManagerMock.On("CapacityCheckWaitTimeSeconds", tt.mig).Return(*tt.capacityCheckWaitTime, nil)
			}
			if tt.capacityCheckWaitTimeErr != nil {
				gkeManagerMock.On("CapacityCheckWaitTimeSeconds", tt.mig).Return(time.Duration(0), tt.capacityCheckWaitTimeErr)
			}

			if tt.maxNodeProvisionTimeOverride != nil {
				gkeManagerMock.On("GetMaxNodeProvisioningTimeOverride", tt.mig).Return(*tt.maxNodeProvisionTimeOverride, true)
			} else {
				gkeManagerMock.On("GetMaxNodeProvisioningTimeOverride", tt.mig).Return(time.Duration(0), false)
			}

			tt.mig.gkeManager = &gkeManagerMock

			got, err := tt.mig.GetOptions(defaults)
			assert.NoError(t, err)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("GkeMig.GetOptions() options diff (-want +got): %s", diff)
			}
		})
	}
}

func TestGetNVMELocalSSDCount(t *testing.T) {
	tests := []struct {
		name string
		mig  *GkeMig
		want int
	}{
		{
			name: "Empty mig - zero count",
			mig:  &GkeMig{},
			want: 0,
		},
		{
			name: "Temporary mig - zero count",
			mig: &GkeMig{
				exist: false,
			},
			want: 0,
		},
		{
			name: "Mig with LocalSSDConfig.EphemeralStorageConfig only - count eq EphemeralStorageConfig.LocalSsdCount",
			mig: &GkeMig{
				exist: true,
				spec: &gkeclient.NodePoolSpec{
					LocalSSDConfig: &gkeclient.LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
						LocalSsdCount: 7,
					}},
				},
			},
			want: 7,
		},
		{
			name: "Mig with LocalSSDConfig.EphemeralStorageLocalSsdConfig only - count eq EphemeralStorageLocalSsdConfig.LocalSsdCount",
			mig: &GkeMig{
				exist: true,
				spec: &gkeclient.NodePoolSpec{
					LocalSSDConfig: &gkeclient.LocalSSDConfig{
						EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{LocalSsdCount: 10},
					},
				},
			},
			want: 10,
		},
		{
			name: "Mig with LocalSSDConfig.LocalNvmeSsdBlockConfig only - count eq LocalNvmeSsdBlockConfig.LocalSsdCount",
			mig: &GkeMig{
				exist: true,
				spec: &gkeclient.NodePoolSpec{
					LocalSSDConfig: &gkeclient.LocalSSDConfig{
						LocalNvmeSsdBlockConfig: &gke_api_beta.LocalNvmeSsdBlockConfig{LocalSsdCount: 25},
					},
				},
			},
			want: 25,
		},
		{
			name: "Mig with all LocalSSDConfig defined - count eq sum of LocalSsdCount",
			mig: &GkeMig{
				exist: true,
				spec: &gkeclient.NodePoolSpec{
					LocalSSDConfig: &gkeclient.LocalSSDConfig{
						EphemeralStorageConfig:         &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: 7},
						EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{LocalSsdCount: 10},
						LocalNvmeSsdBlockConfig:        &gke_api_beta.LocalNvmeSsdBlockConfig{LocalSsdCount: 25},
					},
				},
			},
			want: 42,
		},
		{
			name: "Mig with swap dedicated",
			mig: &GkeMig{
				exist: true,
				spec: &gkeclient.NodePoolSpec{
					LocalSSDConfig: &gkeclient.LocalSSDConfig{
						EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{LocalSsdCount: 5},
					},
					LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
						SwapConfig: &gkeclient.SwapConfig{
							Enabled: true,
							DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
								DiskCount: 2,
							},
						},
					},
				},
			},
			want: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.mig.GetNVMELocalSSDCount()
			if tt.want != got {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestBootDiskSizeForNewNodeGroup(t *testing.T) {
	ephStorageNodepoolSpec := gkeclient.NodePoolSpec{
		LocalSSDConfig: &gkeclient.LocalSSDConfig{
			EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
				LocalSsdCount: 42,
			},
		},
	}
	tests := []struct {
		name      string
		spec      gkeclient.NodePoolSpec
		autopilot bool
		want      int64
	}{
		{
			name: "ephemeral storage on local ssd",
			spec: ephStorageNodepoolSpec,
			want: int64(123),
		},
		{
			name: "dynamic sizing enabled with local ssd",
			spec: ephStorageNodepoolSpec,
			want: int64(123),
		},
		{
			name: "managed node pool spec, dynamic boot disk disabled",
			spec: gkeclient.NodePoolSpec{
				AutopilotManaged: true,
			},
			want: int64(123),
		},
		{
			name: "managed node pool spec, dynamic boot disk enabled",
			spec: gkeclient.NodePoolSpec{
				AutopilotManaged: true,
				Labels: map[string]string{
					gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey: "true",
				},
			},
			want: machinetypes.MaxBootDiskSizeNonSharedCoreMachinesGb,
		},
		{
			name: "cluster autopilot enabled",
			spec: gkeclient.NodePoolSpec{
				ComputeClass: "Accelerator",
			},
			autopilot: true,
			want:      machinetypes.MaxBootDiskSizeNonSharedCoreMachinesGb,
		},
		{
			name: "cluster autopilot disabled",
			spec: gkeclient.NodePoolSpec{
				ComputeClass: "Accelerator",
			},
			autopilot: false,
			want:      int64(123),
		},
		{
			name: "boot disk size already set",
			spec: gkeclient.NodePoolSpec{
				DiskSize: 200,
			},
			autopilot: true,
			want:      int64(200),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gkeManagerMock := &GkeManagerMock{}
			gkeManagerMock.On("GetDefaultNodePoolDiskSizeGB").Return(int64(123)).Once()
			gkeManagerMock.On("GetDefaultNodePoolDiskType").Return("pd-balanced")
			gke := &gkeCloudProviderImpl{
				gkeManager:            gkeManagerMock,
				autopilotEnabled:      tt.autopilot,
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			}
			got := gke.bootDiskSizeForNewNodeGroup(tt.spec)
			if got != tt.want {
				t.Errorf("bootDiskSizeForNewNodeGroup(%+v) = %v, want = %v", tt.spec, got, tt.want)
			}
		})
	}
}

func TestBootDiskTypeForNewNodeGroup(t *testing.T) {
	tests := []struct {
		name        string
		machineType string
		diskType    string
		want        string
	}{
		{
			name:        "disk type for e2-",
			machineType: "e2-standard-16",
			want:        "pd-balanced",
		},
		{
			name:        "disk type for n4- - pd-balanced is not supported",
			machineType: "n4-standard-80",
			want:        "hyperdisk-balanced",
		},
		{
			name:        "disk type for c3- metal",
			machineType: "c3-standard-192-metal",
			want:        "pd-balanced", // should be "hyperdisk-balanced", but c3- metal is currently not supported
		},
		{
			name:        "disk type for ek-",
			machineType: "ek-standard-4",
			want:        "pd-balanced",
		},
		{
			name:        "invalid machine-type - fallback to default",
			machineType: "in__valid",
			want:        "pd-balanced",
		},
		{
			name:        "disk type already specified",
			machineType: "e2-standard-4",
			diskType:    "pd-ssd",
			want:        "pd-ssd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gkeManagerMock := &GkeManagerMock{}
			gkeManagerMock.On("GetDefaultNodePoolDiskType").Return(machinetypes.DiskTypeBalanced)

			gke := &gkeCloudProviderImpl{
				gkeManager:            gkeManagerMock,
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			}
			got := gke.bootDiskTypeForNewNodeGroup(tt.machineType, tt.diskType)
			if got != tt.want {
				t.Errorf("bootDiskTypeForNewNodeGroup(%+v) = %v, want = %v", tt.machineType, got, tt.want)
			}
		})
	}
}

func TestDefaultBootDiskTypeForNewAutopilotNodeGroup(t *testing.T) {
	tests := []struct {
		name        string
		machineType string
		diskType    string
		want        string
	}{
		{
			name:        "disk type for e2-",
			machineType: "e2-standard-16",
			want:        "pd-balanced",
		},
		{
			name:        "disk type for n4- - pd-balanced is not supported",
			machineType: "n4-standard-80",
			want:        "hyperdisk-balanced",
		},
		{
			name:        "disk type for ek-",
			machineType: "ek-standard-4",
			want:        "pd-balanced",
		},
		{
			name:        "invalid machine-type - fallback to empty",
			machineType: "in__valid",
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gkeManagerMock := &GkeManagerMock{}
			gkeManagerMock.On("GetDefaultNodePoolDiskType").Return(machinetypes.DiskTypeStandard)

			gke := &gkeCloudProviderImpl{
				gkeManager:            gkeManagerMock,
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			}
			got := gke.defaultBootDiskTypeForNewAutopilotNodeGroup(tt.machineType)
			if got != tt.want {
				t.Errorf("defaultBootDiskTypeForNewAutopilotNodeGroup(%+v) = %v, want = %v", tt.machineType, got, tt.want)
			}
		})
	}
}

func TestHasInstance(t *testing.T) {
	validNode := BuildTestNode("n1", 1000, 1000)
	validNode.Spec.ProviderID = "gce://project1/us-central1-b/n1"

	validNodeBeingDeleted := validNode.DeepCopy()
	validNodeBeingDeleted.Spec.Taints = append(validNodeBeingDeleted.Spec.Taints, apiv1.Taint{
		Key:    taints.ToBeDeletedTaint,
		Value:  fmt.Sprint(time.Now()),
		Effect: apiv1.TaintEffectPreferNoSchedule,
	})

	malformedNode := validNodeBeingDeleted.DeepCopy()
	malformedNode.Spec.ProviderID = ""

	testCases := []struct {
		name          string
		node          *apiv1.Node
		instance      *gce.GceInstance
		want          bool
		wantCacheCall bool
		wantErr       error
	}{
		{
			name:          "node begin delete - has instance",
			node:          validNodeBeingDeleted,
			instance:      &gce.GceInstance{},
			want:          true,
			wantCacheCall: true,
			wantErr:       nil,
		},
		{
			name:          "node begin delete - no instance",
			node:          validNodeBeingDeleted,
			instance:      nil,
			want:          false,
			wantCacheCall: true,
			wantErr:       nil,
		},
		{
			name:          "running node do not trigger cache calls",
			node:          validNode,
			instance:      nil,
			want:          true,
			wantCacheCall: false,
			wantErr:       nil,
		},
		{
			name:    "malformed node",
			node:    malformedNode,
			want:    false,
			wantErr: fmt.Errorf("wrong id: expected format gce://<project-id>/<zone>/<name>, got nil"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManagerMock := &GkeManagerMock{}
			if tc.wantCacheCall {
				gkeManagerMock.On("InstanceByRef", mock.AnythingOfType("gce.GceRef")).Return(tc.instance).Once()
			}
			gke := &gkeCloudProviderImpl{
				gkeManager: gkeManagerMock,
			}

			got, err := gke.HasInstance(tc.node)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantErr, err)
			gkeManagerMock.AssertExpectations(t)
		})
	}
}

func TestGetHugepageSizeBytes(t *testing.T) {
	for tn, tc := range map[string]struct {
		mig                         *GkeMig
		expectedHugepageSize2mBytes int64
		expectedHugepageSize1gBytes int64
	}{
		"no spec": {
			mig: &GkeMig{},
		},
		"no linux node config": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{},
			},
		},
		"no hugepages": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					LinuxNodeConfig: &gkeclient.LinuxNodeConfig{},
				},
			},
		},
		"hugepages2m": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
						Hugepages: &gkeclient.HugepagesConfig{
							HugepageSize2m: 100,
						},
					},
				},
			},
			expectedHugepageSize2mBytes: 2 * 100 * units.MiB,
		},
		"hugepages1g": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
						Hugepages: &gkeclient.HugepagesConfig{
							HugepageSize1g: 100,
						},
					},
				},
			},
			expectedHugepageSize1gBytes: 100 * units.GiB,
		},
		"both hugepages 1g and 2m": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
						Hugepages: &gkeclient.HugepagesConfig{
							HugepageSize1g: 1,
							HugepageSize2m: 2,
						},
					},
				},
			},
			expectedHugepageSize1gBytes: 1 * units.GiB,
			expectedHugepageSize2mBytes: 2 * 2 * units.MiB,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			gotHugepageSize1gBytes := tc.mig.GetHugepageSize1gBytes()
			gotHugepageSize2mBytes := tc.mig.GetHugepageSize2mBytes()
			assert.Equal(t, tc.expectedHugepageSize2mBytes, gotHugepageSize2mBytes)
			assert.Equal(t, tc.expectedHugepageSize1gBytes, gotHugepageSize1gBytes)
		})
	}
}

func TestAcceleratorSliceNodeGroupMaxSize(t *testing.T) {
	const (
		zone = "us-west1-b"
	)
	gkeManager := NewFakeGkeManager([]string{zone})

	test := []struct {
		topology     string
		machineType  string
		multiHostTpu bool
		wantMaxSize  int
	}{
		{
			topology:    "1x72",
			machineType: "a4x-highgpu-4g",
			wantMaxSize: 18,
		},
		{
			topology:    "2x36",
			machineType: "a4x-highgpu-4g",
			wantMaxSize: 18,
		},
		{
			topology:    "1x64",
			machineType: "a4x-highgpu-4g",
			wantMaxSize: 16,
		},
	}

	for _, tc := range test {
		t.Run(tc.topology, func(t *testing.T) {
			systemLabels := map[string]string{
				apiv1.LabelZoneFailureDomain: zone,
				gkelabels.PolicyLabel:        "policy",
			}
			policy := gceclient.GceResourcePolicy{
				Name:           "policy",
				WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: tc.topology},
			}

			gke := &gkeCloudProviderImpl{
				gkeManager: gkeManager,
				nodePoolSpecBuilders: []napcloudprovider.NodePoolSpecBuilder{
					placementSetter{puller: placement.NewFakeResourcePolicyPullerProvider([]*gceclient.GceResourcePolicy{&policy}, nil)},
				},
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			}
			nodeGroup, err := gke.NewNodeGroup(tc.machineType, make(map[string]string), systemLabels, nil, nil)
			assert.NoError(t, err)
			assert.NotNil(t, nodeGroup)

			mig := nodeGroup.(*GkeMig)
			assert.Equal(t, tc.wantMaxSize, mig.maxSize)
		})
	}
}

func TestResizeAtomically(t *testing.T) {
	tests := []struct {
		name                 string
		isTpuMultiHost       bool
		isBulkProvisioning   bool
		machineType          string
		tpuType              string
		placementPolicy      string
		wantResizeAtomically bool
	}{
		{
			name:                 "ResizeAtomically - single host TPU",
			machineType:          "single_host_tpu_vm",
			isTpuMultiHost:       false,
			tpuType:              "tpuV5",
			wantResizeAtomically: false,
		},
		{
			name:                 "ResizeAtomically - multi host TPU",
			isTpuMultiHost:       true,
			tpuType:              "tpuV5",
			machineType:          "multi_host_tpu_vm",
			wantResizeAtomically: true,
		},
		{
			name:                 "ResizeAtomically - non-slice GPU",
			isTpuMultiHost:       false,
			machineType:          "n1-standard-64",
			wantResizeAtomically: false,
		},
		{
			name:                 "ResizeAtomically - slice GPU",
			isTpuMultiHost:       false,
			machineType:          "a4x-highgpu-4g",
			placementPolicy:      "a-placement-policy",
			wantResizeAtomically: true,
		},
		{
			name:                 "ResizeAtomically - bulk MIG",
			isBulkProvisioning:   true,
			machineType:          "nvidia-tesla-t4",
			wantResizeAtomically: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mig := &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					TpuMultiHost: tt.isTpuMultiHost,
					TpuType:      tt.tpuType,
					MachineType:  tt.machineType,
					PlacementGroup: placement.Spec{
						Policy: tt.placementPolicy,
					},
				},
				gkeManager: &gkeManagerImpl{
					machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
				},
			}
			if tt.isBulkProvisioning {
				mig.spec.FlexStart = true
				mig.spec.MachineType = "a4x-highgpu-4g"
				mig.spec.PlacementGroup.Policy = "a4x-policy"
			}
			assert.Equal(t, tt.isBulkProvisioning, mig.UsesBulkProvisioning())
			assert.Equal(t, tt.isTpuMultiHost, mig.IsMultiHostTpuMig())

			assert.Equal(t, tt.wantResizeAtomically, mig.ResizeAtomically())
		})
	}
}

func TestUsesBulkProvisioning(t *testing.T) {
	testCases := []struct {
		name         string
		mig          *GkeMig
		wantUsesBulk bool
	}{
		{
			name:         "No spec",
			mig:          &GkeMig{},
			wantUsesBulk: false,
		},
		{
			name:         "non existent machine type",
			mig:          &GkeMig{spec: &gkeclient.NodePoolSpec{MachineType: "invalid-type"}},
			wantUsesBulk: false,
		},
		{
			name:         "unsupported machine family - n1",
			mig:          &GkeMig{spec: &gkeclient.NodePoolSpec{MachineType: "n1-standard-1", FlexStart: true, PlacementGroup: placement.Spec{Policy: "a4x-policy"}}},
			wantUsesBulk: false,
		},
		{
			name:         "supported family with placement group, but no flex start",
			mig:          &GkeMig{spec: &gkeclient.NodePoolSpec{MachineType: "a4x-highgpu-4g", PlacementGroup: placement.Spec{Policy: "a4x-policy"}}},
			wantUsesBulk: false,
		},
		{
			name:         "supported family and flex start, but no placement group",
			mig:          &GkeMig{spec: &gkeclient.NodePoolSpec{MachineType: "a4x-highgpu-4g", FlexStart: true}},
			wantUsesBulk: false,
		},
		{
			name:         "supported family and flex star and placement group",
			mig:          &GkeMig{spec: &gkeclient.NodePoolSpec{MachineType: "a4x-highgpu-4g", FlexStart: true, PlacementGroup: placement.Spec{Policy: "a4x-policy"}}},
			wantUsesBulk: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testMig := tc.mig
			testMig.gkeManager = &GkeManagerMock{}
			assert.Equal(t, tc.wantUsesBulk, testMig.UsesBulkProvisioning())
		})
	}
}

func TestMigVersion(t *testing.T) {
	gkeManagerMock := &GkeManagerMock{}
	tests := []struct {
		name            string
		autoprovisioned bool
		spec            *gkeclient.NodePoolSpec
		nodeConfig      *NodeConfig
		clusterVersion  string
		wantVersion     string
	}{
		{
			name:            "autoprovisioned with node version in spec",
			autoprovisioned: true,
			spec: &gkeclient.NodePoolSpec{
				NodeVersion: "1.32.9-gke.1726000",
			},
			wantVersion: "1.32.9-gke.1726000",
		},
		{
			name:            "autoprovisioned without node version in spec, fallback to cluster version",
			autoprovisioned: true,
			spec:            &gkeclient.NodePoolSpec{},
			clusterVersion:  "1.31.0-gke.100",
			wantVersion:     "1.31.0-gke.100",
		},
		{
			name:            "non-autoprovisioned with node config version",
			autoprovisioned: false,
			nodeConfig: &NodeConfig{
				Version: "1.30.0-gke.200",
			},
			wantVersion: "1.30.0-gke.200",
		},
		{
			name:            "non-autoprovisioned without node config",
			autoprovisioned: false,
			nodeConfig:      nil,
			wantVersion:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mig := &GkeMig{
				autoprovisioned: tc.autoprovisioned,
				spec:            tc.spec,
				nodeConfig:      tc.nodeConfig,
				gkeManager:      gkeManagerMock,
			}
			if tc.clusterVersion != "" {
				gkeManagerMock.On("GetClusterVersion").Return(tc.clusterVersion).Once()
			}
			assert.Equal(t, tc.wantVersion, mig.Version())
			mock.AssertExpectationsForObjects(t, gkeManagerMock)
		})
	}
}

func TestCloudProviderSuspendInstances(t *testing.T) {
	migRef := gce.GceRef{Name: "mig1"}
	instances := []gce.GceRef{{Name: "inst1"}, {Name: "inst2"}}

	testCases := []struct {
		desc  string
		force bool
	}{
		{
			desc:  "with_force",
			force: true,
		},
		{
			desc: "without_force",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			fakeGkeManager := NewFakeGkeManager([]string{"zone-a"})
			gke := &gkeCloudProviderImpl{
				gkeManager: fakeGkeManager,
			}

			err := gke.SuspendInstances(migRef, instances, tc.force)
			assert.NoError(t, err)

			for _, instRef := range instances {
				assert.Equal(t, SuspensionStatus{ForceUsed: tc.force, Suspended: true}, fakeGkeManager.GetSuspensionStatus(migRef, instRef))
			}
		})
	}
}

func TestCloudProviderResumeInstances(t *testing.T) {
	fakeGkeManager := NewFakeGkeManager([]string{"zone-a"})
	gke := &gkeCloudProviderImpl{
		gkeManager: fakeGkeManager,
	}
	migRef := gce.GceRef{Name: "mig1"}
	instances := []gce.GceRef{{Name: "inst1"}, {Name: "inst2"}}
	err := fakeGkeManager.SuspendInstances(migRef, instances, true)
	assert.NoError(t, err)
	for _, instRef := range instances {
		assert.Equal(t, SuspensionStatus{Suspended: true, ForceUsed: true}, fakeGkeManager.GetSuspensionStatus(migRef, instRef))
	}

	err = gke.ResumeInstances(migRef, instances)
	assert.NoError(t, err)
	for _, instRef := range instances {
		assert.Equal(t, SuspensionStatus{}, fakeGkeManager.GetSuspensionStatus(migRef, instRef))
	}
}

func TestIsReservationCompatible(t *testing.T) {
	tests := []struct {
		name                   string
		affinityType           string
		affinityName           string
		reservationName        string
		specificReservationReq bool
		nilAffinity            bool
		want                   bool
	}{
		{
			name:                   "No specific reservation required, Any affinity",
			affinityType:           gkeclient.ReservationAffinityAny,
			affinityName:           "",
			reservationName:        "res-1",
			specificReservationReq: false,
			want:                   true,
		},
		{
			name:                   "No specific reservation required, Any-then-fail affinity",
			affinityType:           gkeclient.ReservationAffinityAnyThenFail,
			affinityName:           "",
			reservationName:        "res-1",
			specificReservationReq: false,
			want:                   true,
		},
		{
			name:                   "Specific reservation required, Any affinity",
			affinityType:           gkeclient.ReservationAffinityAny,
			affinityName:           "",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   false,
		},
		{
			name:                   "Specific reservation required, Any-then-fail affinity",
			affinityType:           gkeclient.ReservationAffinityAny,
			affinityName:           "",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   false,
		},
		{
			name:                   "Specific reservation required, Specific affinity matching name",
			affinityType:           gkeclient.ReservationAffinitySpecific,
			affinityName:           "res-1",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   true,
		},
		{
			name:                   "Specific reservation required, Specific affinity mismatch name",
			affinityType:           gkeclient.ReservationAffinitySpecific,
			affinityName:           "res-2",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   false,
		},
		{
			name:                   "Specific reservation required, Specific affinity matching shared reservation path",
			affinityType:           gkeclient.ReservationAffinitySpecific,
			affinityName:           "projects/shared-project/reservations/res-1",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   true,
		},
		{
			name:                   "Specific reservation required, Specific affinity matching reservation block path",
			affinityType:           gkeclient.ReservationAffinitySpecific,
			affinityName:           "projects/shared-project/reservations/res-1/reservationBlocks/block-1",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   true,
		},
		{
			name:                   "Specific reservation required, Specific affinity matching reservation sub-block path",
			affinityType:           gkeclient.ReservationAffinitySpecific,
			affinityName:           "projects/shared-project/reservations/res-1/reservationBlocks/block-1/reservationSubBlocks/sub-block-1",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   true,
		},
		{
			name:                   "Specific reservation required, Specific affinity matching local reservation block path",
			affinityType:           gkeclient.ReservationAffinitySpecific,
			affinityName:           "res-1/reservationBlocks/block-1",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   true,
		},
		{
			name:                   "Specific reservation required, Specific affinity with substring collision",
			affinityType:           gkeclient.ReservationAffinitySpecific,
			affinityName:           "projects/shared-project/reservations/res-1-other/reservationBlocks/block-res-1/reservationSubBlocks/sub-block-1",
			reservationName:        "res-1",
			specificReservationReq: true,
			want:                   false,
		},
		{
			name:                   "Nil affinity, Specific reservation required",
			reservationName:        "res-1",
			specificReservationReq: true,
			nilAffinity:            true,
			want:                   false,
		},
		{
			name:                   "Nil affinity, Specific reservation not required",
			reservationName:        "res-1",
			specificReservationReq: false,
			nilAffinity:            true,
			want:                   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mig := &GkeMig{
				spec: &gkeclient.NodePoolSpec{},
			}
			if !tt.nilAffinity {
				mig.spec.ReservationAffinity = &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: tt.affinityType,
					Values:                 []string{tt.affinityName},
				}
			}
			rsv := &gcev1.Reservation{
				Name:                        tt.reservationName,
				SpecificReservationRequired: tt.specificReservationReq,
			}

			got := mig.IsReservationCompatible(rsv)
			assert.Equal(t, tt.want, got)
		})
	}
}
