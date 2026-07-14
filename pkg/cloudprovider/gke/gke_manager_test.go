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
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	gce_api_compute "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	autoscalererrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkeapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/api"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelocalssdsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/utils/ptr"
)

const (
	projectId                   = "project1"
	zoneA                       = "us-central1-a"
	zoneB                       = "us-central1-b"
	zoneC                       = "us-central1-c"
	zoneF                       = "us-central1-f"
	zoneAIA                     = "us-central1-ai1a"
	zoneAIB                     = "us-central1-ai1b"
	machineTypeA                = "n1-standard-1"
	machineTypeTPU              = "ct5lp-hightpu-8t"
	machineTypeB                = "e2-standard-1"
	region                      = "us-central1"
	defaultPoolMigName          = "gke-cluster-1-default-pool"
	defaultPool                 = "default-pool"
	autoprovisionedPoolMigName  = "gke-cluster-1-nodeautoprovisioning-323233232"
	autoprovisionedPool         = "nodeautoprovisioning-323233232"
	clusterName                 = "cluster1"
	defaultClusterVersion       = "22.23.1-gke.0"
	irretrievableMigRefreshTime = 10 * time.Second

	gkeMigA = "gce-mig-a"
	gkeMigB = "gce-mig-b"
)

var (
	zones = []string{zoneA, zoneB, zoneC}
	arch  = gce.DefaultArch
)

const (
	napEnabled  = true
	napDisabled = false
)

const allNodePools1 = `
  "nodePools": [
    {
      "name": "default-pool",
      "config": {
        "machineType": "n1-standard-1",
        "diskSizeGb": 100,
        "oauthScopes": [
          "https://www.googleapis.com/auth/compute",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/logging.write",
          "https://www.googleapis.com/auth/monitoring.write",
          "https://www.googleapis.com/auth/servicecontrol",
          "https://www.googleapis.com/auth/service.management.readonly",
          "https://www.googleapis.com/auth/trace.append"
        ],
        "imageType": "COS",
        "serviceAccount": "default"
      },
      "initialNodeCount": 3,
      "autoscaling": {
         "Enabled": true,
         "MinNodeCount": 1,
         "MaxNodeCount": 11
      },
      "management": {},
      "selfLink": "https://container.googleapis.com/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1/nodePools/default-pool",
      "version": "1.6.9",
      "instanceGroupUrls": [
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool"
      ],
      "status": "RUNNING"
    }
  ]
`

const allNodePoolsRegional = `
  "nodePools": [
    {
      "name": "default-pool",
      "config": {
        "machineType": "n1-standard-1",
        "diskSizeGb": 100,
        "oauthScopes": [
          "https://www.googleapis.com/auth/compute",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/logging.write",
          "https://www.googleapis.com/auth/monitoring.write",
          "https://www.googleapis.com/auth/servicecontrol",
          "https://www.googleapis.com/auth/service.management.readonly",
          "https://www.googleapis.com/auth/trace.append"
        ],
        "imageType": "COS",
        "serviceAccount": "default"
      },
      "initialNodeCount": 3,
      "autoscaling": {
         "Enabled": true,
         "MinNodeCount": 1,
         "MaxNodeCount": 11
      },
      "management": {},
      "selfLink": "https://container.googleapis.com/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1/nodePools/default-pool",
      "version": "1.6.9",
      "instanceGroupUrls": [
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool",
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-c/instanceGroupManagers/gke-cluster-1-default-pool",
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-f/instanceGroupManagers/gke-cluster-1-default-pool"
      ],
      "status": "RUNNING"
    }
  ]
`

const allNodePools2 = `
  "nodePools": [
    {
      "name": "default-pool",
      "config": {
        "machineType": "n1-standard-1",
        "diskSizeGb": 100,
        "oauthScopes": [
          "https://www.googleapis.com/auth/compute",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/logging.write",
          "https://www.googleapis.com/auth/monitoring.write",
          "https://www.googleapis.com/auth/servicecontrol",
          "https://www.googleapis.com/auth/service.management.readonly",
          "https://www.googleapis.com/auth/trace.append"
        ],
        "imageType": "COS",
        "serviceAccount": "default"
      },
      "initialNodeCount": 3,
      "autoscaling": {
         "Enabled": true,
         "MinNodeCount": 1,
         "MaxNodeCount": 11},
      "management": {},
      "selfLink": "https://container.googleapis.com/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1/nodePools/default-pool",
      "version": "1.6.9",
      "instanceGroupUrls": [
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool"
      ],
      "status": "RUNNING"
    },
    {
      "name": "nodeautoprovisioning-323233232",
      "config": {
        "machineType": "n1-standard-1",
        "diskSizeGb": 100,
        "oauthScopes": [
          "https://www.googleapis.com/auth/compute",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/logging.write",
          "https://www.googleapis.com/auth/monitoring.write",
          "https://www.googleapis.com/auth/servicecontrol",
          "https://www.googleapis.com/auth/service.management.readonly",
          "https://www.googleapis.com/auth/trace.append"
        ],
        "imageType": "COS",
        "serviceAccount": "default"
      },
      "initialNodeCount": 3,
      "autoscaling": {
         "Enabled": true,
         "MinNodeCount": 0,
         "MaxNodeCount": 1000
      },
      "management": {},
      "selfLink": "https://container.googleapis.com/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
      "version": "1.6.9",
      "instanceGroupUrls": [
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232"
      ],
      "status": "RUNNING"
    }
  ]
`

const defaultAndTPUNodePool = `
"nodePools": [
    {
      "name": "default-pool",
      "config": {
        "machineType": "n1-standard-1",
        "diskSizeGb": 100,
        "oauthScopes": [
          "https://www.googleapis.com/auth/compute",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/logging.write",
          "https://www.googleapis.com/auth/monitoring.write",
          "https://www.googleapis.com/auth/servicecontrol",
          "https://www.googleapis.com/auth/service.management.readonly",
          "https://www.googleapis.com/auth/trace.append"
        ],
        "imageType": "COS",
        "serviceAccount": "default"
      },
      "initialNodeCount": 3,
      "autoscaling": {
         "Enabled": true,
         "MinNodeCount": 1,
         "MaxNodeCount": 11},
      "management": {},
      "selfLink": "https://container.googleapis.com/v1beta1/projects/project1/locations/us-central1/clusters/cluster-1/nodePools/default-pool",
      "version": "1.6.9",
      "instanceGroupUrls": [
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/instanceGroupManagers/gke-cluster-1-default-pool"
      ],
      "status": "RUNNING"
    },
    {
      "name": "nodeautoprovisioning-323233232",
      "config": {
        "machineType": "ct5lp-hightpu-8t",
        "diskSizeGb": 100,
        "oauthScopes": [
          "https://www.googleapis.com/auth/compute",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/logging.write",
          "https://www.googleapis.com/auth/monitoring.write",
          "https://www.googleapis.com/auth/servicecontrol",
          "https://www.googleapis.com/auth/service.management.readonly",
          "https://www.googleapis.com/auth/trace.append"
        ],
        "imageType": "COS",
        "serviceAccount": "default"
      },
      "initialNodeCount": 3,
      "autoscaling": {
         "Enabled": true,
         "MinNodeCount": 0,
         "MaxNodeCount": 1000
      },
      "management": {},
      "selfLink": "https://container.googleapis.com/v1beta1/projects/project1/locations/us-central1/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
      "version": "1.6.9",
      "instanceGroupUrls": [
        "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232"
      ],
      "status": "RUNNING"
    }
  ]
`

const instanceGroupManager = `{
  "kind": "compute#instanceGroupManager",
  "id": "3213213219",
  "creationTimestamp": "2017-09-15T04:47:24.687-07:00",
  "name": "%s",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s",
  "instanceTemplate": "https://www.googleapis.com/compute/v1/projects/project1/global/instanceTemplates/%s",
  "instanceGroup": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s/instanceGroups/%s",
  "baseInstanceName": "%s",
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
  "targetSize": %d,
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s/instanceGroupManagers/%s"
}
`

const instanceTemplate = `
{
 "kind": "compute#instanceTemplate",
 "id": "28701103232323232",
 "creationTimestamp": "2017-09-15T04:47:21.577-07:00",
 "name": "gke-cluster-1-default-pool",
 "description": "",
 "properties": {
  "tags": {
   "items": [
    "gke-cluster-1-fc0afeeb-node"
   ]
  },
  "machineType": "n1-standard-1",
  "canIpForward": true,
  "networkInterfaces": [
   {
    "kind": "compute#networkInterface",
    "network": "https://www.googleapis.com/compute/v1/projects/project1/global/networks/default",
    "subnetwork": "https://www.googleapis.com/compute/v1/projects/project1/regions/us-central1/subnetworks/default",
    "accessConfigs": [
     {
      "kind": "compute#accessConfig",
      "type": "ONE_TO_ONE_NAT",
      "name": "external-nat"
     }
    ]
   }
  ],
  "disks": [
   {
    "kind": "compute#attachedDisk",
    "type": "PERSISTENT",
    "mode": "READ_WRITE",
    "boot": true,
    "initializeParams": {
     "sourceImage": "https://www.googleapis.com/compute/v1/projects/gke-node-images/global/images/cos-stable-60-9592-84-0",
     "diskSizeGb": "100",
     "diskType": "pd-standard"
    },
    "autoDelete": true
   }
  ],
  "metadata": {
   "kind": "compute#metadata",
   "fingerprint": "F7n_RsHD3ng=",
   "items": [
		{
		 "key": "kube-env",
		 "value": "ALLOCATE_NODE_CIDRS: \"true\"\n"
		},
		{
		 "key": "user-data",
		 "value": "#cloud-config\n\nwrite_files:\n  - path: /etc/systemd/system/kube-node-installation.service\n    "
		},
		{
		 "key": "gci-update-strategy",
		 "value": "update_disabled"
		},
		{
		 "key": "gci-ensure-gke-docker",
		 "value": "true"
		},
		{
		 "key": "configure-sh",
		 "value": "#!/bin/bash\n\n# Copyright 2016 The Kubernetes Authors.\n#\n# Licensed under the Apache License, "
		},
		{
		 "key": "cluster-name",
		 "value": "cluster-1"
		}
	   ]
	  },
  "serviceAccounts": [
   {
    "email": "default",
    "scopes": [
     "https://www.googleapis.com/auth/compute",
     "https://www.googleapis.com/auth/devstorage.read_only",
     "https://www.googleapis.com/auth/logging.write",
     "https://www.googleapis.com/auth/monitoring.write",
     "https://www.googleapis.com/auth/servicecontrol",
     "https://www.googleapis.com/auth/service.management.readonly",
     "https://www.googleapis.com/auth/trace.append"
    ]
   }
  ],
  "scheduling": {
   "onHostMaintenance": "MIGRATE",
   "automaticRestart": true,
   "preemptible": false
  }
 },
 "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool-f7607aac"
}`

const managedInstancesResponse1 = `{
  "managedInstances": [
    {
      "instance": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s/instances/%s-f7607aac-9j4g",
      "name": "%s-f7607aac-9j4g",
      "id": "1974815549671473983",
      "instanceStatus": "RUNNING",
      "currentAction": "NONE"
    },
    {
      "instance": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s/instances/%s-f7607aac-c63g",
      "name": "%s-f7607aac-c63g",
      "currentAction": "RUNNING",
      "id": "197481554967143333",
      "instanceStatus": "RUNNING",
      "currentAction": "NONE"
    },
    {
      "instance": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s/instances/%s-f7607aac-dck1",
      "name": "%s-f7607aac-dck1",
      "id": "4462422841867240255",
      "instanceStatus": "RUNNING",
      "currentAction": "NONE"
    },
    {
      "instance": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s/instances/%s-f7607aac-f1hm",
      "name": "%s-f7607aac-f1hm",
      "id": "6309299611401323327",
      "instanceStatus": "RUNNING",
      "currentAction": "NONE"
    }
  ]
}`

const managedInstancesResponse2 = `{
  "managedInstances": [
    {
      "instance": "https://www.googleapis.com/compute/v1/projects/project1/zones/%s/instances/%s-gdf607aac-9j4g",
      "id": "1974815323221473983",
      "instanceStatus": "RUNNING",
      "currentAction": "NONE"
    }
  ]
}`

const getClusterResponseTemplate = `{
  "name": "usertest",
  "nodeConfig": {
    "machineType": "n1-standard-1",
    "diskSizeGb": 100,
    "oauthScopes": [
      "https://www.googleapis.com/auth/compute",
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring"
    ],
    "imageType": "COS",
    "serviceAccount": "default",
    "diskType": "pd-standard"
  },
  "masterAuth": {
    "username": "admin",
    "password": "pass",
    "clusterCaCertificate": "cer1",
    "clientCertificate": "cer1",
    "clientKey": "cer1=="
  },
  "loggingService": "logging.googleapis.com",
  "monitoringService": "monitoring.googleapis.com",
  "network": "default",
  "clusterIpv4Cidr": "10.32.0.0/14",
  "addonsConfig": {
    "networkPolicyConfig": {
      "disabled": true
    }
  },
  %s,
  "locations": [
    "us-central1-b"
  ],
  "labelFingerprint": "fasdfds",
  "legacyAbac": {},
  "autoscaling": {
    "enableNodeAutoprovisioning": %v,
    "resourceLimits": [
      {
        "resourceType": "cpu",
        "minimum": "2",
        "maximum": "3"
      },
      {
        "resourceType": "memory",
        "minimum": "2000000000",
        "maximum": "3000000000"
      }
    ]
  },
  "networkConfig": {
    "network": "https://www.googleapis.com/compute/v1/projects/project1/global/networks/default"
  },
  "selfLink": "https:///v1beta1/projects/project1/locations/us-central1-c/clusters/usertest",
  "zone": "us-central1-c",
  "endpoint": "xxx",
  "initialClusterVersion": "1.sdafsa",
  "currentMasterVersion": "1fdsfdsfsauser",
  "currentNodeVersion": "xxx",
  "createTime": "2017-10-24T12:20:00+00:00",
  "status": "RUNNING",
  "nodeIpv4CidrSize": 24,
  "servicesIpv4Cidr": "10.35.240.0/20",
  "instanceGroupUrls": [
    "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-c/instanceGroupManagers/gke-usertest-default-pool-323-grp"
  ],
  "currentNodeCount": 1,
	"confidentialNodes": {
		"enabled": %v,
		"confidentialInstanceType": "%v"
	}
}`

func getInstanceGroupManager(zone string) string {
	return getInstanceGroupManagerNamed(defaultPoolMigName, zone, 3)
}

func getInstanceGroupManagerNamed(name, zone string, targetSize int) string {
	return fmt.Sprintf(instanceGroupManager, name, zone, name, zone, name, name, targetSize, zone, name)
}

func getManagedInstancesResponse1(zone string) string {
	return getManagedInstancesResponse1Named(defaultPoolMigName, zone)
}

func getManagedInstancesResponse1Named(name, zone string) string {
	return fmt.Sprintf(managedInstancesResponse1, zone, name, name, zone, name, name, zone, name, name, zone, name, name)
}

func getManagedInstancesResponse2(zone string) string {
	return getManagedInstancesResponse2Named(autoprovisionedPoolMigName, zone)
}

func getManagedInstancesResponse2Named(name, zone string) string {
	return fmt.Sprintf(managedInstancesResponse2, zone, name)
}

func newTestAutoscalingGceClient(t *testing.T, projectId, url string, waitTimeout, pollInterval time.Duration) gceclient.AutoscalingInternalGceClient {
	client := &http.Client{}
	gceClient, err := gceclient.NewCustomAutoscalingInternalGceClient(
		client, &migInfoProviderStub{}, projectId, "", url,
		"cluster-autoscaler", waitTimeout, pollInterval, experiments.NewMockManager(),
		gceclient.WithInstanceActionPollingFrequency(pollInterval),
	)
	if !assert.NoError(t, err) {
		t.Fatalf("fatal error: %v", err)
	}
	return gceClient
}

type migInfoProviderStub struct {
	scaleUpTime                  map[gce.GceRef]time.Time
	capacityCheckWaitTimeSeconds map[gce.GceRef]time.Duration
	queuedProvisioning           map[gce.GceRef]bool
	flexStartNonQueued           map[gce.GceRef]bool
	instanceTemplate             map[gce.GceRef]gce.InstanceTemplateName
	instances                    map[gce.GceRef][]gce.GceInstance
}

func (m *migInfoProviderStub) CapacityCheckWaitTimeSeconds(migRef gce.GceRef) (time.Duration, error) {
	if m.capacityCheckWaitTimeSeconds == nil {
		return 0, fmt.Errorf("CapacityCheckWaitTimeSeconds not found")
	}
	return m.capacityCheckWaitTimeSeconds[migRef], nil
}

func (m *migInfoProviderStub) ScaleUpTime(migRef gce.GceRef) (time.Time, error) {
	if m.scaleUpTime == nil {
		return time.Time{}, fmt.Errorf("ScaleUpTime not found")
	}
	return m.scaleUpTime[migRef], nil
}

func (m *migInfoProviderStub) QueuedProvisioning(migRef gce.GceRef) bool {
	if m.queuedProvisioning == nil {
		return false
	}
	return m.queuedProvisioning[migRef]
}

func (m *migInfoProviderStub) FlexStartNonQueued(migRef gce.GceRef) bool {
	if m.flexStartNonQueued == nil {
		return false
	}
	return m.flexStartNonQueued[migRef]
}

func (m *migInfoProviderStub) GetMigInstanceTemplateName(migRef gce.GceRef) (gce.InstanceTemplateName, error) {
	if m.instanceTemplate == nil {
		return gce.InstanceTemplateName{}, nil
	}
	return m.instanceTemplate[migRef], nil
}

func (m *migInfoProviderStub) GetMigInstances(migRef gce.GceRef) ([]gce.GceInstance, error) {
	if m.instances == nil {
		return nil, nil
	}
	return m.instances[migRef], nil
}

func (m *migInfoProviderStub) IsTpuMig(_ gce.GceRef) bool {
	return false
}

func (m *migInfoProviderStub) GetListManagedInstancesResults(_ gce.GceRef) (string, error) {
	return "", nil
}

func (m *migInfoProviderStub) GetMigForInstance(instanceRef gce.GceRef) (gce.Mig, error) {
	return nil, nil
}

func (m *migInfoProviderStub) RegenerateMigInstancesCache() error {
	return nil
}

func (m *migInfoProviderStub) GetMigTargetSize(_ gce.GceRef) (int64, error) {
	return 0, nil
}

func (m *migInfoProviderStub) GetMigIsStable(_ gce.GceRef) (bool, error) {
	return true, nil
}

func (m *migInfoProviderStub) GetMigBasename(_ gce.GceRef) (string, error) {
	return "", nil
}

func (m *migInfoProviderStub) GetMigInstanceTemplate(_ gce.GceRef) (*gce_api.InstanceTemplate, error) {
	return nil, nil
}

func (m *migInfoProviderStub) GetMigKubeEnv(_ gce.GceRef) (gce.KubeEnv, error) {
	return gce.KubeEnv{}, nil
}

func (m *migInfoProviderStub) GetMigMachineType(_ gce.GceRef) (gce.MachineType, error) {
	return gce.MachineType{}, nil
}

func (m *migInfoProviderStub) RefreshMigInfo(_ gce.GceRef) error {
	return nil
}

const listInstanceGroupManagerResponsePartTemplate = `
  {
   "kind": "compute#instanceGroupManager",
   "id": "9012769713544464023",
   "creationTimestamp": "2019-03-26T07:34:32.082-07:00",
   "name": "%v",
   "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/%v",
   "instanceTemplate": "https://www.googleapis.com/compute/v1/projects/project1/global/instanceTemplates/gke-blah-default-pool-67b773a0",
   "versions": [
    {
     "instanceTemplate": "https://www.googleapis.com/compute/v1/projects/project1/global/instanceTemplates/gke-blah-default-pool-67b773a0",
     "targetSize": {
      "calculated": 1
     }
    }
   ],
   "instanceGroup": "https://www.googleapis.com/compute/v1/projects/my-gke-dev2/zones/%v/instanceGroups/%v",
   "baseInstanceName": "gke-blah-default-pool-67b773a0",
   "fingerprint": "ASJwTpesjDI=",
   "currentActions": {
    "none": 1,
    "creating": 0,
    "creatingWithoutRetries": 0,
    "verifying": 0,
    "recreating": 0,
    "deleting": 0,
    "abandoning": 0,
    "restarting": 0,
    "refreshing": 0
   },
   "status": {
    "isStable": true
   },
   "targetSize": %v,
   "selfLink": "https://www.googleapis.com/compute/v1/projects/my-gke-dev2/zones/us-west1-b/instanceGroupManagers/gke-blah-default-pool-67b773a0-grp",
   "updatePolicy": {
    "type": "OPPORTUNISTIC",
    "minimalAction": "REPLACE",
    "maxSurge": {
     "fixed": 1,
     "calculated": 1
    },
    "maxUnavailable": {
     "fixed": 1,
     "calculated": 1
    }
   }
  }
`

func buildListInstanceGroupManagersResponsePart(name, zone string, targetSize uint64) string {
	return fmt.Sprintf(listInstanceGroupManagerResponsePartTemplate, name, zone, zone, name, targetSize)
}

func buildListInstanceGroupManagersResponse(listInstanceGroupManagerResponseParts ...string) string {
	return `{
 "kind": "compute#instanceGroupManagerList",
 "id": "blah",
 "items": [` +
		strings.Join(listInstanceGroupManagerResponseParts, ",") +
		`], "selfLink": "https://blah"}`
}

func addDefaultListMigsMocks(server *HttpServerMock, cache *GkeCache) {
	for _, zone := range []string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"} {
		path := fmt.Sprintf("/projects/project1/zones/%s/instanceGroupManagers", zone)
		var migParts []string
		if cache != nil {
			for _, mig := range cache.GetGkeMigs() {
				if mig.gceRef.Zone == zone {
					migParts = append(migParts, buildListInstanceGroupManagersResponsePart(mig.gceRef.Name, zone, 1))
				}
			}
		}
		server.On("handle", path).Return(buildListInstanceGroupManagersResponse(migParts...)).Maybe()
	}
}

func newTestGkeManager(t *testing.T, testServerURL string,
	nodeAutoprovisioningEnabled, regional, autopilotEnabled bool,
	gkeClient gkeclient.AutoscalingGkeClient, autopilotHigherMaxPodsPerNode bool, reservationsPuller *gceclient.ReservationsPuller, clusterVersionOverride ...string,
) *gkeManagerImpl {
	// Override wait for op timeouts.
	waitTimeout := 50 * time.Millisecond
	pollInterval := 1 * time.Millisecond

	gceService := newTestAutoscalingGceClient(t, projectId, testServerURL, waitTimeout, pollInterval)

	provisioningRequestManager := manager.NewProvReqManagerFake(nil, nil, nil)

	autoprovisioningEligibility := &MockAutoprovisioningEligibility{}
	autoprovisioningEligibility.On("SetClusterAutoprovisioningEnabled", nodeAutoprovisioningEnabled).Maybe().Return(true)
	autoprovisioningEligibility.On("IsNodeAutoprovisioningEnabled").Return(nodeAutoprovisioningEnabled)
	autoprovisioningEligibility.On("AreClusterLimitsEnabled").Return(nodeAutoprovisioningEnabled)
	autoprovisioningEligibility.On("UseAutoprovisioningFeaturesForPodRequirements").Return(nodeAutoprovisioningEnabled)
	autoprovisioningEligibility.On("UseAutoprovisioningFeaturesForNodeGroup").Return(nodeAutoprovisioningEnabled)

	gceCache := gce.NewGceCache()
	cache := NewGkeCache(gceCache, nodetemplate.NewCache())
	migLister := NewGkeMigLister(cache, irretrievableMigRefreshTime, irretrievableMigRefreshTime, 2)
	clusterVersion := defaultClusterVersion
	if len(clusterVersionOverride) > 0 {
		clusterVersion = clusterVersionOverride[0]
	}
	manager := &gkeManagerImpl{
		cache:                       cache,
		gceService:                  gceService,
		migLister:                   migLister,
		migInfoProvider:             gce.NewCachingMigInfoProvider(gceCache, migLister, gceService, projectId, 1, 15*time.Minute, false, false),
		projectId:                   projectId,
		clusterName:                 clusterName,
		clusterVersion:              clusterVersion,
		templates:                   &GkeTemplateBuilder{},
		reserved:                    NewGkeReservedForTesting(),
		provisioningRequestManager:  provisioningRequestManager,
		autoprovisioningEligibility: autoprovisioningEligibility,
		gkeConfigurationCache:       gkeConfigurationCache{autoprovisioningLocations: []string{zoneA, zoneB}},
		MachineConfigValidator:      &testMachineConfigValidator{},
		managerOptions: GkeManagerOptions{
			Regional:                      regional,
			AutopilotEnabled:              autopilotEnabled,
			AutopilotHigherMaxPodsPerNode: autopilotHigherMaxPodsPerNode,
			enableUserAnyZoneSelection:    true,
		},

		draResourcePredictor:     dynamicresources.NewResourcePredictor(),
		machineConfigProvider:    machinetypes.NewMachineConfigProvider(nil),
		localSSDDiskSizeProvider: gkelocalssdsize.NewDynamicLocalSSDDiskSizeProvider(machinetypes.LocalSSDDiskSizes),
		gkeMetrics:               internalmetrics.Metrics,
	}
	if regional {
		manager.location = region
	} else {
		manager.location = zoneB
	}

	machineConfigProvider := machinetypes.NewMachineConfigProvider(nil)
	manager.draResourcePredictor.SetCloudProvider(&fakeProvider{machineConfigProvider})
	machinesCache := map[gce.MachineTypeKey]gce.MachineType{}
	for _, zone := range []string{zoneA, zoneB, zoneC, zoneF, zoneAIA, zoneAIB} {
		for _, machineType := range []string{machineTypeA, machineTypeB} {
			machinesCache[gce.MachineTypeKey{Zone: zone, MachineTypeName: machineType}] = gce.MachineType{Name: machineType, CPU: 1, Memory: 1 * units.MiB}
		}
	}
	manager.cache.SetMachines(machinesCache)

	// set zones are available in the cluster region.
	manager.cache.SetZonesInRegion(region, []string{zoneA, zoneB, zoneC, zoneAIA, zoneAIB})

	if gkeClient != nil {
		manager.gkeService = gkeClient
	} else {
		client := &http.Client{}
		gkeclient.GkeAPIEndpoint = &testServerURL
		// TODO(b/485133862): refactor this package to use fake service instead of mocks.
		service, err := gkeapi.NewClient(client, "", *gkeclient.GkeAPIEndpoint)
		assert.NoError(t, err)
		gkeService, err := gkeclient.NewAutoscalingGkeClientV1beta1(service, nil, projectId, manager.location, clusterName, machineConfigProvider, napMaxNodes)
		assert.NoError(t, err)
		manager.gkeService = gkeService
	}

	return manager
}

type fakeProvider struct {
	machineConfigProvider *machinetypes.MachineConfigProvider
}

func (f *fakeProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return f.machineConfigProvider
}

func validateMig(t *testing.T, mig gce.Mig, zone string, name string, minSize int, maxSize int) {
	assert.Equal(t, name, mig.GceRef().Name)
	assert.Equal(t, zone, mig.GceRef().Zone)
	assert.Equal(t, projectId, mig.GceRef().Project)
	assert.Equal(t, minSize, mig.MinSize())
	assert.Equal(t, maxSize, mig.MaxSize())
}

// validateInGkeMigsSet should be used for all validation where more than 1 Migs are present in the cache
func validateInGkeMigsSet(t *testing.T, migs []*GkeMig, zone string, name string, minSize int, maxSize int) {
	gceRef := gce.GceRef{
		Project: projectId,
		Zone:    zone,
		Name:    name,
	}
	for _, mig := range migs {
		if mig.GceRef() == gceRef {
			validateMig(t, mig, zone, name, minSize, maxSize)
			return
		}
	}
}

func TestRefreshNodePools(t *testing.T) {
	server := NewHttpServerMock()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)

	// Fetch one node pool.
	getClusterResponse1 := fmt.Sprintf(getClusterResponseTemplate, allNodePools1, napDisabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(getClusterResponse1).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()

	err := g.refreshGkeResources()
	assert.NoError(t, err)
	migs := g.GetGkeMigs()
	assert.Equal(t, 1, len(migs))
	validateMig(t, migs[0], zoneB, "gke-cluster-1-default-pool", 1, 11)
	mock.AssertExpectationsForObjects(t, server)

	// Fetch three node pools, skip one.
	getClusterResponse2 := fmt.Sprintf(getClusterResponseTemplate, allNodePools2, napDisabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(getClusterResponse2).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()

	err = g.refreshGkeResources()
	assert.NoError(t, err)
	migs = g.GetGkeMigs()
	assert.Equal(t, 2, len(migs))
	validateInGkeMigsSet(t, migs, zoneB, "gke-cluster-1-default-pool", 1, 11)
	validateInGkeMigsSet(t, migs, zoneB, "gke-cluster-1-nodeautoprovisioning-323233232", 0, 1000)
	mock.AssertExpectationsForObjects(t, server)

	// Fetch one node pool, remove node pool registered in previous step.
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(getClusterResponse1).Once()

	err = g.refreshGkeResources()
	assert.NoError(t, err)
	migs = g.GetGkeMigs()
	assert.Equal(t, 1, len(migs))
	validateMig(t, migs[0], zoneB, "gke-cluster-1-default-pool", 1, 11)
	mock.AssertExpectationsForObjects(t, server)
}

func TestFetchAllNodePoolsRegional(t *testing.T) {
	server := NewHttpServerMock()
	g := newTestGkeManager(t, server.URL, napDisabled, true, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)

	// Fetch one node pool.
	cluster := fmt.Sprintf(getClusterResponseTemplate, allNodePoolsRegional, napDisabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1/clusters/cluster1").Return(cluster).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-c/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneC)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-f/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneF)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Times(3)

	err := g.refreshGkeResources()
	assert.NoError(t, err)
	migs := g.GetGkeMigs()
	assert.Equal(t, 3, len(migs))
	validateInGkeMigsSet(t, migs, zoneB, "gke-cluster-1-default-pool", 1, 11)
	validateInGkeMigsSet(t, migs, zoneC, "gke-cluster-1-default-pool", 1, 11)
	validateInGkeMigsSet(t, migs, zoneF, "gke-cluster-1-default-pool", 1, 11)
	mock.AssertExpectationsForObjects(t, server)
}

const migDoesNotExistsError = `{
  "error": {
    "code": 404,
    "message": "The resource 'projects/project1/us-central1-b/instanceGroups/gke-cluster-1-default-pool' was not found",
    "errors": [
      {
        "message": "The resource 'projects/project1/zones/us-central1-b/instanceGroups/gke-cluster-1-default-pool' was not found",
        "domain": "global",
        "reason": "notFound"
      }
    ]
  }
}`

func TestFetchNoExistMigInNodePool(t *testing.T) {
	var err error
	server := NewHttpServerMock(MockFieldStatusCode, MockFieldResponse)
	g := newTestGkeManager(t, server.URL, napDisabled, true, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	cluster := fmt.Sprintf(getClusterResponseTemplate, allNodePools1, napDisabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1/clusters/cluster1").Return(200, cluster).Times(3)
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers").Return(200, buildListInstanceGroupManagersResponse()).Times(4)
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(404, migDoesNotExistsError).Times(4)

	gceRef := gce.GceRef{
		Project: projectId,
		Zone:    zoneB,
		Name:    defaultPoolMigName,
	}

	// mig marked as irretrievable when fetching their instance template
	err = g.refreshGkeResources()
	assert.NoError(t, err)
	assert.Equal(t, 1, g.cache.markedIrretrievableMigs[gceRef])
	assert.False(t, g.cache.IsMigBlocked(gceRef))

	// mig marked as irretrievable when fetching their target size
	migs := g.GetGkeMigs()
	_, _ = g.GetMigSize(migs[0])
	assert.Equal(t, 2, g.cache.markedIrretrievableMigs[gceRef])
	assert.True(t, g.cache.IsMigBlocked(gceRef))

	err = g.refreshGkeResources()
	assert.NoError(t, err)
	assert.NotContains(t, g.migLister.GetGkeMigs(), migs[0])

	g.cache.InvalidateBlockedIrretrievableMigs()
	g.cache.InvalidateMarkedIrretrievableMigs()
	assert.NotContains(t, g.cache.blockedIrretrievableMigs, migs[0])
	assert.NotContains(t, g.cache.markedIrretrievableMigs, migs[0])

	// mig not marked as irretrievable when fetching their instance template, as it is already cached
	err = g.refreshGkeResources()
	assert.NoError(t, err)
	assert.Equal(t, 0, g.cache.markedIrretrievableMigs[gceRef])
	assert.False(t, g.cache.IsMigBlocked(gceRef))

	// mig marked as irretrievable when fetching their target size
	migs = g.GetGkeMigs()
	_, _ = g.GetMigSize(migs[0])
	assert.Equal(t, 1, g.cache.markedIrretrievableMigs[gceRef])
	assert.False(t, g.cache.IsMigBlocked(gceRef))
}

// TestValidateMigTemplateNodeRequestingTpuWithUnsupportedMachineType_ExpectedMigBlocked tests the behaviour of
// blocking a mig because a CloudProviderError, due to tpu request with incorrect machine type, is returned.
func TestValidateMigTemplateNodeRequestingTpuWithUnsupportedMachineType_ExpectedMigBlocked(t *testing.T) {
	const templateRequestingTpu = `
	{
	 "kind": "compute#instanceTemplate",
	 "id": "28701103232323232",
	 "creationTimestamp": "2017-09-15T04:47:21.577-07:00",
	 "name": "gke-cluster-1-default-pool",
	 "description": "",
	 "properties": {
	  "tags": {
	   "items": [
		"gke-cluster-1-fc0afeeb-node"
	   ]
	  },
	  "machineType": "n1-standard-1",
	  "canIpForward": true,
	  "networkInterfaces": [
	   {
		"kind": "compute#networkInterface",
		"network": "https://www.googleapis.com/compute/v1/projects/project1/global/networks/default",
		"subnetwork": "https://www.googleapis.com/compute/v1/projects/project1/regions/us-central1/subnetworks/default",
		"accessConfigs": [
		 {
		  "kind": "compute#accessConfig",
		  "type": "ONE_TO_ONE_NAT",
		  "name": "external-nat"
		 }
		]
	   }
	  ],
	  "disks": [
	   {
		"kind": "compute#attachedDisk",
		"type": "PERSISTENT",
		"mode": "READ_WRITE",
		"boot": true,
		"autoDelete": true
	   }
	  ],
	  "metadata": {
	   "kind": "compute#metadata",
	   "fingerprint": "F7n_RsHD3ng=",
	   "items": [
			{
			 "key": "kube-env",
			 "value": "ALLOCATE_NODE_CIDRS: \"true\"\nAUTOSCALER_ENV_VARS: \"node_labels=cloud.google.com/gke-tpu-accelerator=tpu-v6e-slice\""
			},
			{
			 "key": "cluster-name",
			 "value": "cluster-1"
			}
		   ]
		  },
	  "scheduling": {
	   "onHostMaintenance": "MIGRATE",
	   "automaticRestart": true,
	   "preemptible": false
	  }
	 },
	 "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool-f7607aac"
	}`

	server := NewHttpServerMock()
	defer server.Close()

	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/default-pool").Return(getInstanceGroupManagerResponse).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(templateRequestingTpu).Once()

	g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)

	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneB,
			Name:    "default-pool",
		},
		gkeManager:      g,
		minSize:         0,
		maxSize:         10,
		autoprovisioned: false,
		exist:           true,
		spec: &gkeclient.NodePoolSpec{
			MachineType: "n1-standard-1",
		},
		nodeConfig: &NodeConfig{
			ThreadsPerCore: 1,
		},
	}
	AddMigsToNodePool("default-pool", mig)

	prevBlocked := g.migLister.InvalidateIrretrievableMigsCacheIfExpired()
	assert.Empty(t, prevBlocked)
	g.validateMigTemplateNode(mig)

	assert.Equal(t, 1, g.cache.markedIrretrievableMigs[mig.gceRef], "Mig blocked due to unsupported mt, CloudProviderError returned from addTpuCapacity")
	assert.True(t, g.cache.IsMigBlocked(mig.gceRef))
}

const deleteNodePoolResponse = `{
  "name": "operation-1505732351373-819ed94e",
  "zone": "us-central1-a",
  "operationType": "DELETE_NODE_POOL",
  "status": "RUNNING",
  "selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505732351373-819ed94e",
  "targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T10:59:11.373456931Z"
}`

const deleteNodePoolOperationResponse = `{
  "name": "operation-1505732351373-819ed94e",
  "zone": "us-central1-a",
  "operationType": "DELETE_NODE_POOL",
  "status": "DONE",
  "selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505732351373-819ed94e",
  "targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T10:59:11.373456931Z"
}`

func TestDeleteNodePool(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)

	cluster := fmt.Sprintf(getClusterResponseTemplate, allNodePools2, napEnabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1/nodePools/nodeautoprovisioning-323233232").Return(deleteNodePoolResponse).Once()
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/operations/operation-1505732351373-819ed94e").Return(deleteNodePoolOperationResponse).Once()
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(cluster).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()

	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneB,
			Name:    "nodeautoprovisioning-323233232",
		},
		gkeManager:      g,
		minSize:         0,
		maxSize:         1000,
		autoprovisioned: true,
		exist:           true,
		spec:            nil,
	}
	AddMigsToNodePool("nodeautoprovisioning-323233232", mig)

	err := g.DeleteNodePool(mig)
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, server)
}

const createNodePoolResponse = `{
  "name": "operation-1505728466148-d16f5197",
  "zone": "us-central1-a",
  "operationType": "CREATE_NODE_POOL",
  "status": "RUNNING",
  "selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5197",
  "targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T09:54:26.148507311Z"
}`

const createNodePoolResponseZoneB = `{
  "name": "operation-1505728466148-d16f5197",
  "zone": "us-central1-b",
  "operationType": "CREATE_NODE_POOL",
  "status": "RUNNING",
  "selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5197",
  "targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T09:54:26.148507311Z"
}`

const createNodePoolOperationResponse = `{
  "name": "operation-1505728466148-d16f5197",
  "zone": "us-central1-a",
  "operationType": "CREATE_NODE_POOL",
  "status": "DONE",
  "error": {"code": 1},
  "selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5197",
  "targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T09:54:26.148507311Z",
  "endTime": "2017-09-18T09:54:35.124878859Z"
}`

const createNodePoolOperationResponseSuccess = `{
  "name": "operation-1505728466148-d16f5197",
  "zone": "us-central1-a",
  "operationType": "CREATE_NODE_POOL",
  "status": "DONE",
  "selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5197",
  "targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T09:54:26.148507311Z",
  "endTime": "2017-09-18T09:54:35.124878859Z"
}`

type testMachineConfigValidator struct{}

func (mtv *testMachineConfigValidator) ValidateMachineTypeConfig(machineType, zone string) error {
	// Machine type machineTypeB is not supported in zone zoneB
	if machineType == machineTypeA || machineType == machineTypeTPU || (machineType == machineTypeB && zone != zoneB) {
		return nil
	}
	if zone == zoneAIA || zone == zoneAIB {
		return nil
	}
	return fmt.Errorf("Machine type %s is not supported in zone %v", machineType, zone)
}

func (mtv *testMachineConfigValidator) ValidateGpuConfig(gpuType, _, _, _, _ string, _ int64, zone string, _, _ int64) error {
	if zone != zoneA && zone != zoneC {
		return fmt.Errorf("GPU %s is not supported in zone %v", gpuType, zone)
	}
	return nil
}

func TestNewNodePoolSpec(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napEnabled, true, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	g.MachineConfigValidator = &testMachineConfigValidator{}
	// Set autoprovisioning locations
	autoprovLocations := []string{zoneA, zoneB, zoneC}
	g.gkeConfigurationCache.setAutoprovisioningLocations(autoprovLocations)
	g.cache.SetZonesInRegion(region, []string{zoneA, zoneB, zoneC, zoneAIA, zoneAIB})

	testCases := []struct {
		description  string
		mig          *GkeMig
		expectedSpec *gkeclient.NodePoolSpec
		expectedErr  error
	}{
		{
			description: "spec unspecified, expects could not find mig spec error",
			mig: NewTestGkeMigBuilder().SetGceRef(
				gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    "mig",
				},
			).SetNodePoolName("no-spec").Build(),
			expectedErr: fmt.Errorf("could not find mig spec for mig no-spec"),
		},
		{
			description: "specific reservation required with zone present, expects reservation zone",
			mig: NewTestGkeMigBuilder().SetGceRef(
				gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    "mig",
				},
			).SetSpec(
				NewTestMigSpecBuilder().
					SetMachineType(machineTypeA).
					SetReservationAffinity(gkeclient.ReservationAffinitySpecific, "rsv").
					SetLabels(map[string]string{gkelabels.ReservationZoneLabel: zoneA}).
					SpecBuild()).
				Build(),
			expectedSpec: NewTestMigSpecBuilder().
				SetMachineType(machineTypeA).
				SetReservationAffinity(gkeclient.ReservationAffinitySpecific, "rsv").
				SetLocations([]string{zoneA}).
				SpecBuild(),
		},
		{
			description: "specific reservation required without zone present, node pool locations are not limited to rsv zone",
			mig: NewTestGkeMigBuilder().SetGceRef(
				gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    "mig",
				},
			).SetSpec(
				NewTestMigSpecBuilder().
					SetMachineType(machineTypeA).
					SetReservationAffinity(gkeclient.ReservationAffinitySpecific, "rsv").
					SpecBuild()).
				Build(),
			expectedSpec: NewTestMigSpecBuilder().
				SetMachineType(machineTypeA).
				SetReservationAffinity(gkeclient.ReservationAffinitySpecific, "rsv").
				SetLocations(autoprovLocations).
				SpecBuild(),
		},
		{
			description: "any reservation required, expects autoprovisioning zones",
			mig: NewTestGkeMigBuilder().SetGceRef(
				gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    "mig",
				},
			).SetSpec(
				NewTestMigSpecBuilder().
					SetMachineType(machineTypeA).
					SetReservationAffinity(gkeclient.ReservationAffinityAny, "rsv").
					SpecBuild()).
				Build(),
			expectedSpec: NewTestMigSpecBuilder().
				SetMachineType(machineTypeA).
				SetReservationAffinity(gkeclient.ReservationAffinityAny, "rsv").
				SetLocations(autoprovLocations).
				SpecBuild(),
		},
		{
			description: "compact placement mig, expects main zone",
			mig: NewTestGkeMigBuilder().SetGceRef(
				gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    "mig",
				},
			).SetSpec(
				NewTestMigSpecBuilder().
					SetMachineType(machineTypeA).
					SetPlacementGroup("groupId", "COLLOCATED").
					SpecBuild()).
				Build(),
			expectedSpec: NewTestMigSpecBuilder().
				SetMachineType(machineTypeA).
				SetPlacementGroup("groupId", "COLLOCATED").
				SetLocations([]string{zoneA}).
				SpecBuild(),
		},
		{
			description: "tpu multi host mig, expects main zone",
			mig: NewTestGkeMigBuilder().SetGceRef(
				gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    "mig",
				},
			).SetSpec(
				NewTestMigSpecBuilder().
					SetMachineType(machineTypeA).
					SetTpuType("v6e-lite-device").
					SetTpuTopology("4x4").
					SetTpuMultiHost(true).
					SpecBuild()).
				Build(),
			expectedSpec: NewTestMigSpecBuilder().
				SetMachineType(machineTypeA).
				SetTpuType("v6e-lite-device").
				SetTpuTopology("4x4").
				SetTpuMultiHost(true).
				SetLocations([]string{zoneA}).
				SpecBuild(),
		},
		{
			description: "mig with specified locations",
			mig: NewTestGkeMigBuilder().SetGceRef(
				gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    "mig",
				},
			).SetSpec(
				NewTestMigSpecBuilder().
					SetMachineType(machineTypeA).
					SetLocations([]string{zoneAIA, zoneAIB}).
					SpecBuild()).
				Build(),
			expectedSpec: NewTestMigSpecBuilder().
				SetMachineType(machineTypeA).
				SetLocations([]string{zoneAIA, zoneAIB}).
				SpecBuild(),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			spec, err := g.NewNodePoolSpec(tc.mig)
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.Equal(t, tc.expectedSpec, spec)
			}
		})
	}
}

func TestExistingMigsInNodePool(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, true, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers").Return(buildListInstanceGroupManagersResponse())

	// Fetch one node pool
	getClusterResponse1 := fmt.Sprintf(getClusterResponseTemplate, allNodePools1, napDisabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1/clusters/cluster1").Return(getClusterResponse1).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()
	err := g.refreshGkeResources()
	assert.NoError(t, err)

	gotMigs := g.ExistingMigsInNodePool("default-pool")
	assert.Equal(t, 1, len(gotMigs))
	validateMig(t, gotMigs[0], zoneB, "gke-cluster-1-default-pool", 1, 11)

	// Fetch two node pools
	getClusterResponse2 := fmt.Sprintf(getClusterResponseTemplate, allNodePools2, napDisabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1/clusters/cluster1").Return(getClusterResponse2).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232").Return(getInstanceGroupManager(zoneB)).Once()
	err = g.refreshGkeResources()
	assert.NoError(t, err)

	// first nodepool
	gotMigs = g.ExistingMigsInNodePool("default-pool")
	assert.Equal(t, 1, len(gotMigs))
	validateMig(t, gotMigs[0], zoneB, "gke-cluster-1-default-pool", 1, 11)

	// second nodepool
	gotMigs = g.ExistingMigsInNodePool("nodeautoprovisioning-323233232")
	assert.Equal(t, 1, len(gotMigs))
	validateMig(t, gotMigs[0], zoneB, "gke-cluster-1-nodeautoprovisioning-323233232", 0, 1000)

	// Non-existent nodepool
	gotMigs = g.ExistingMigsInNodePool("non-existent-nodepool")
	assert.Equal(t, 0, len(gotMigs))
}

func TestLimitNodePoolLocations(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()

	availableCpuPlatforms := map[string][]string{
		zoneA:   {"Gruby Jasio", "Cienki Bolek"},
		zoneB:   {"Gruby Jasio"},
		zoneC:   {"Cienki Bolek", "Trala Lala"},
		zoneAIA: {"Hello World"},
		zoneAIB: {"Hello World"},
	}

	availableDiskTypes := map[string][]string{
		zoneA:   {"pd-standard", "pd-balanced", "pd-ssd"},
		zoneB:   {"pd-balanced"},
		zoneC:   {"pd-balanced", "hyperdisk-balanced"},
		zoneAIA: {"pd-balanced"},
		zoneAIB: {"pd-balanced"},
	}

	testCases := []struct {
		scenario           string
		machineType        string
		diskType           string
		gpuCount           int
		minCpuPlatform     string
		zone               string
		allAvailableZones  []string
		locations          []string
		specifiedLocations []string
		compactPlacement   bool
		rsvSpecific        bool
		expectErrorMessage string
		expectedLocations  []string
	}{
		{
			scenario:          "machine type available everywhere",
			machineType:       machineTypeA,
			zone:              zoneA,
			locations:         []string{zoneA, zoneB},
			expectedLocations: []string{zoneA, zoneB},
		},
		{
			scenario:          "machine type location filtered",
			machineType:       machineTypeB,
			zone:              zoneA,
			locations:         []string{zoneA, zoneB},
			expectedLocations: []string{zoneA},
		},
		{
			scenario:           "machine type not available in the main zone",
			machineType:        machineTypeB,
			zone:               zoneB,
			locations:          []string{zoneB},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; Machine type e2-standard-1 is not supported in zone us-central1-b",
			expectedLocations:  nil,
		},
		{
			scenario:          "GPU available in all locations",
			machineType:       machineTypeA,
			diskType:          "pd-balanced",
			gpuCount:          1,
			zone:              zoneA,
			locations:         []string{zoneA, zoneC},
			expectedLocations: []string{zoneA, zoneC},
		},
		{
			scenario:          "location filtered",
			machineType:       machineTypeA,
			diskType:          "pd-balanced",
			gpuCount:          1,
			zone:              zoneC,
			locations:         []string{zoneB, zoneC},
			expectedLocations: []string{zoneC},
		},
		{
			scenario:          "no GPU",
			machineType:       machineTypeA,
			diskType:          "pd-balanced",
			zone:              zoneC,
			locations:         []string{zoneB, zoneC},
			expectedLocations: []string{zoneB, zoneC},
		},
		{
			scenario:           "GPUs not available in the main zone",
			machineType:        machineTypeA,
			diskType:           "pd-balanced",
			gpuCount:           1,
			zone:               zoneB,
			locations:          []string{zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; GPU configuration (gpuType nvidia-tesla-k80, gpuCount 1, machineType n1-standard-1) is not supported in zone us-central1-b",
			expectedLocations:  nil,
		},
		{
			scenario:           "requested min cpu platform not available anywhere",
			machineType:        machineTypeA,
			diskType:           "pd-balanced",
			minCpuPlatform:     "Szacher Macher",
			zone:               zoneC,
			locations:          []string{zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-c; CPU platform Szacher Macher is not supported in zone us-central1-c; supported CPU platforms []string{\"Cienki Bolek\", \"Trala Lala\"}",
			expectedLocations:  nil,
		},
		{
			scenario:           "requested min cpu platform not available in main zone",
			machineType:        machineTypeA,
			diskType:           "pd-balanced",
			minCpuPlatform:     "Gruby Jasio",
			zone:               zoneC,
			locations:          []string{zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-c; CPU platform Gruby Jasio is not supported in zone us-central1-c; supported CPU platforms []string{\"Cienki Bolek\", \"Trala Lala\"}",
			expectedLocations:  nil,
		},
		{
			scenario:          "filtered based on requested min cpu platform",
			machineType:       machineTypeA,
			diskType:          "pd-balanced",
			minCpuPlatform:    "Gruby Jasio",
			zone:              zoneA,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA, zoneB},
		},
		{
			scenario:          "filtered based on GPU and machine type (both zones match)",
			machineType:       machineTypeB,
			diskType:          "pd-balanced",
			gpuCount:          1,
			zone:              zoneA,
			locations:         []string{zoneA, zoneC},
			expectedLocations: []string{zoneA, zoneC},
		},
		{
			scenario:          "filtered based on GPU and machine type (zone B does not match)",
			machineType:       machineTypeB,
			diskType:          "pd-balanced",
			gpuCount:          1,
			zone:              zoneA,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA, zoneC},
		},
		{
			scenario:           "filtered based on GPU and machine type (no machine type and gpu in the main zone)",
			machineType:        machineTypeB,
			diskType:           "pd-balanced",
			gpuCount:           1,
			zone:               zoneB,
			locations:          []string{zoneB},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; Machine type e2-standard-1 is not supported in zone us-central1-b",
			expectedLocations:  nil,
		},
		{
			scenario:          "filtered based on machine type and min cpu platform (both zones match)",
			machineType:       machineTypeB,
			diskType:          "pd-balanced",
			minCpuPlatform:    "Cienki Bolek",
			zone:              zoneA,
			locations:         []string{zoneA, zoneC},
			expectedLocations: []string{zoneA, zoneC},
		},
		{
			scenario:          "filtered based on machine type and min cpu platform (only main zone matches)",
			machineType:       machineTypeB,
			diskType:          "pd-balanced",
			minCpuPlatform:    "Gruby Jasio",
			zone:              zoneA,
			locations:         []string{zoneA, zoneC},
			expectedLocations: []string{zoneA},
		},
		{
			scenario:           "filtered based on machine type and min cpu platform (no cpu platform in main zone)",
			machineType:        machineTypeB,
			diskType:           "pd-balanced",
			minCpuPlatform:     "Trala Lala",
			zone:               zoneA,
			locations:          []string{zoneA, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-a; CPU platform Trala Lala is not supported in zone us-central1-a; supported CPU platforms []string{\"Gruby Jasio\", \"Cienki Bolek\"}",
			expectedLocations:  nil,
		},
		{
			scenario:           "filtered based on machine type and min cpu platform (no machine type in main zone)",
			machineType:        machineTypeB,
			diskType:           "pd-balanced",
			minCpuPlatform:     "Gruby Jasio",
			zone:               zoneB,
			locations:          []string{zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; Machine type e2-standard-1 is not supported in zone us-central1-b",
			expectedLocations:  nil,
		},
		{
			scenario:           "filtered based on machine type and min cpu platform (no machine type and no cpu platform in main zone)",
			machineType:        machineTypeB,
			diskType:           "pd-balanced",
			minCpuPlatform:     "Trala Lala",
			zone:               zoneB,
			locations:          []string{zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; Machine type e2-standard-1 is not supported in zone us-central1-b; CPU platform Trala Lala is not supported in zone us-central1-b; supported CPU platforms []string{\"Gruby Jasio\"}",
			expectedLocations:  nil,
		},
		{
			scenario:          "filtered based on GPU and min cpu platform (both zones match)",
			machineType:       machineTypeA,
			diskType:          "pd-balanced",
			gpuCount:          1,
			minCpuPlatform:    "Cienki Bolek",
			zone:              zoneA,
			locations:         []string{zoneA, zoneC},
			expectedLocations: []string{zoneA, zoneC},
		},
		{
			scenario:          "filtered based on GPU and min cpu platform (only main zone matches)",
			machineType:       machineTypeA,
			diskType:          "pd-balanced",
			gpuCount:          1,
			minCpuPlatform:    "Gruby Jasio",
			zone:              zoneA,
			locations:         []string{zoneA, zoneC},
			expectedLocations: []string{zoneA},
		},
		{
			scenario:           "filtered based on GPU and min cpu platform (no cpu platform in main zone)",
			machineType:        machineTypeA,
			diskType:           "pd-balanced",
			gpuCount:           1,
			minCpuPlatform:     "Trala Lala",
			zone:               zoneA,
			locations:          []string{zoneA, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-a; CPU platform Trala Lala is not supported in zone us-central1-a; supported CPU platforms []string{\"Gruby Jasio\", \"Cienki Bolek\"}",
			expectedLocations:  nil,
		},
		{
			scenario:           "filtered based on GPU and min cpu platform (no GPU in main zone)",
			machineType:        machineTypeA,
			diskType:           "pd-balanced",
			gpuCount:           1,
			minCpuPlatform:     "Gruby Jasio",
			zone:               zoneB,
			locations:          []string{zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; GPU configuration (gpuType nvidia-tesla-k80, gpuCount 1, machineType n1-standard-1) is not supported in zone us-central1-b",
			expectedLocations:  nil,
		},
		{
			scenario:           "filtered based on GPU and min cpu platform (no GPU and no cpu platform in main zone)",
			machineType:        machineTypeA,
			diskType:           "pd-balanced",
			gpuCount:           1,
			minCpuPlatform:     "Trala Lala",
			zone:               zoneB,
			locations:          []string{zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; GPU configuration (gpuType nvidia-tesla-k80, gpuCount 1, machineType n1-standard-1) is not supported in zone us-central1-b; CPU platform Trala Lala is not supported in zone us-central1-b; supported CPU platforms []string{\"Gruby Jasio\"}",
			expectedLocations:  nil,
		},
		{
			scenario:          "Compact Placement: picked initial location",
			machineType:       machineTypeA,
			diskType:          "pd-balanced",
			zone:              zoneB,
			compactPlacement:  true,
			locations:         []string{zoneA, zoneB},
			expectedLocations: []string{zoneB},
		},
		{
			scenario:           "Compact Placement: initial location nod supported",
			machineType:        machineTypeA,
			diskType:           "pd-ssd",
			zone:               zoneB,
			compactPlacement:   true,
			locations:          []string{zoneA, zoneB},
			expectErrorMessage: "Cannot create node pool for master location us-central1-b; Disk type pd-ssd is not supported in zone us-central1-b; supported disk types: []string{\"pd-balanced\"}",
		},
		{
			scenario:          "machine type available everywhere, but disk type only in one zone",
			machineType:       machineTypeA,
			diskType:          "pd-ssd",
			zone:              zoneA,
			locations:         []string{zoneA, zoneB},
			expectedLocations: []string{zoneA},
		},
		{
			scenario:           "specified locations used, all zones supported and spec available everywhere",
			machineType:        machineTypeA,
			zone:               zoneA,
			locations:          []string{zoneA, zoneB, zoneC},
			specifiedLocations: []string{zoneA, zoneB},
			expectedLocations:  []string{zoneA, zoneB},
		},
		{
			scenario:           "specified locations used and can be met (some zones outside of preferences are filtered)",
			machineType:        machineTypeA,
			minCpuPlatform:     "Cienki Bolek",
			zone:               zoneA,
			locations:          []string{zoneA, zoneB, zoneC},
			specifiedLocations: []string{zoneA},
			expectedLocations:  []string{zoneA},
		},
		{
			scenario:           "specified locations used, some zones are not set for autoprovisioning",
			machineType:        machineTypeA,
			zone:               zoneA,
			locations:          []string{zoneA},
			allAvailableZones:  []string{zoneA},
			specifiedLocations: []string{zoneA, zoneB},
			expectErrorMessage: "Cannot create node pool for specified locations: [us-central1-a us-central1-b]; location us-central1-b not configured for autoprovisioning",
			expectedLocations:  nil,
		},
		{
			scenario:           "specified locations used, multiple zones not are not set for autoprovisioning",
			machineType:        machineTypeA,
			zone:               zoneA,
			locations:          []string{zoneA},
			allAvailableZones:  []string{zoneA},
			specifiedLocations: []string{zoneA, zoneB, zoneC},
			expectErrorMessage: "Cannot create node pool for specified locations: [us-central1-a us-central1-b us-central1-c]; location us-central1-b not configured for autoprovisioning; location us-central1-c not configured for autoprovisioning",
			expectedLocations:  nil,
		},
		{
			scenario:           "specified locations used, some zones cannot support cpu",
			machineType:        machineTypeA,
			minCpuPlatform:     "Cienki Bolek",
			zone:               zoneA,
			locations:          []string{zoneA, zoneB, zoneC},
			specifiedLocations: []string{zoneA, zoneB},
			expectErrorMessage: "Cannot create node pool for specified locations: [us-central1-a us-central1-b]; CPU platform Cienki Bolek is not supported in zone us-central1-b; supported CPU platforms []string{\"Gruby Jasio\"}",
			expectedLocations:  nil,
		},
		{
			scenario:           "specified locations used, some zones cannot support cpu, some are not set for autoprovisioning",
			machineType:        machineTypeA,
			minCpuPlatform:     "Cienki Bolek",
			zone:               zoneA,
			locations:          []string{zoneB, zoneC},
			allAvailableZones:  []string{zoneB, zoneC},
			specifiedLocations: []string{zoneA, zoneB},
			expectErrorMessage: "Cannot create node pool for specified locations: [us-central1-a us-central1-b]; location us-central1-a not configured for autoprovisioning; CPU platform Cienki Bolek is not supported in zone us-central1-b; supported CPU platforms []string{\"Gruby Jasio\"}",
			expectedLocations:  nil,
		},
		{
			scenario:          "specific reservation required with, expects reservation zone",
			machineType:       machineTypeA,
			zone:              zoneA,
			rsvSpecific:       true,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA},
		},
		{
			scenario:           "specific reservation required, expects reservation zone even if specified zones from CCC",
			machineType:        machineTypeA,
			zone:               zoneA,
			rsvSpecific:        true,
			locations:          []string{zoneA, zoneB, zoneC},
			specifiedLocations: []string{zoneA, zoneB},
			expectedLocations:  []string{zoneA},
		},
		{
			scenario:          "specific reservation not required, expects autoprovisioning zones",
			machineType:       machineTypeA,
			zone:              zoneA,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA, zoneB, zoneC},
		},
		{
			scenario:           "specified locations are available in cluster region but outside of autoprovisioning locations",
			machineType:        machineTypeA,
			zone:               zoneA,
			locations:          []string{zoneA, zoneB, zoneC},
			allAvailableZones:  []string{zoneA, zoneB, zoneC, zoneAIA, zoneAIB},
			specifiedLocations: []string{zoneAIA, zoneAIB},
			expectedLocations:  []string{zoneAIA, zoneAIB},
		},
		{
			scenario:           "specified locations outside of cluster region",
			machineType:        machineTypeA,
			zone:               zoneA,
			locations:          []string{zoneA, zoneB},
			allAvailableZones:  []string{zoneA, zoneB, zoneC, zoneAIA},
			specifiedLocations: []string{zoneAIB},
			expectErrorMessage: "Cannot create node pool for specified locations: [us-central1-ai1b]; location us-central1-ai1b not configured for autoprovisioning",
			expectedLocations:  nil,
		},
		{
			scenario:          "specified locations are not used - fallback to autoprovisioning locations",
			machineType:       machineTypeA,
			zone:              zoneA,
			locations:         []string{zoneA, zoneB, zoneAIA},
			allAvailableZones: []string{zoneA, zoneB, zoneC, zoneAIA},
			expectedLocations: []string{zoneA, zoneB, zoneAIA},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.scenario, func(t *testing.T) {
			g := newTestGkeManager(t, server.URL, napEnabled, true, false, nil, false, nil)
			addDefaultListMigsMocks(server, g.cache)
			g.MachineConfigValidator = &testMachineConfigValidator{}
			g.gkeConfigurationCache.setAutoprovisioningLocations(tc.locations)
			g.availableDiskTypesProvider = NewStaticAvailableDiskTypesProvider(availableDiskTypes)
			g.availableCpuPlatformsProvider = NewStaticAvailableCpuPlatformsProvider(availableCpuPlatforms)
			// override all available zones.
			if tc.allAvailableZones != nil {
				g.cache.SetZonesInRegion(region, tc.allAvailableZones)
			}

			var acceleratorConfig *gke_api_beta.AcceleratorConfig
			if tc.gpuCount > 0 {
				acceleratorConfig = &gke_api_beta.AcceleratorConfig{
					AcceleratorType:  "nvidia-tesla-k80",
					AcceleratorCount: int64(tc.gpuCount),
				}
			}
			locations, err := g.limitNodePoolLocations(tc.zone, tc.specifiedLocations, tc.machineType, tc.diskType, acceleratorConfig, tc.minCpuPlatform, tc.compactPlacement, tc.rsvSpecific)
			if tc.expectErrorMessage != "" {
				assert.Error(t, err)
				assert.Equal(t, tc.expectErrorMessage, err.Error(), "Unexpected error returned")
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expectedLocations, locations)
		})
	}
	mock.AssertExpectationsForObjects(t, server)
}

func newTestMigSpec(manager GkeManager) *GkeMig {
	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneB,
			Name:    "nodeautoprovisioning-323233232",
		},
		gkeManager:      manager,
		minSize:         0,
		maxSize:         1000,
		autoprovisioned: true,
		exist:           true,
		spec: &gkeclient.NodePoolSpec{
			MachineType: machineTypeA,
			Taints: []apiv1.Taint{
				{
					Key:   gpu.ResourceNvidiaGPU,
					Value: "present",
				},
				{
					Key:   "taint1",
					Value: "value",
				},
			},
			SystemArchitecture: &arch,
		},
		nodeConfig: &NodeConfig{},
	}
	AddMigsToNodePool("nodeautoprovisioning-323233232", mig)
	return mig
}

func TestCreateNodePool(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)

	addDefaultListMigsMocks(server, g.cache)

	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/operations/operation-1505728466148-d16f5197").Return(createNodePoolOperationResponse).Once()
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1/nodePools").Return(createNodePoolResponse).Once()

	cluster := fmt.Sprintf(getClusterResponseTemplate, allNodePools2, napEnabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(cluster).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()

	mig := newTestMigSpec(g)
	newMig, err := g.CreateNodePool(mig)
	assert.NoError(t, err)
	assert.True(t, newMig.MainCreatedMig.Exist())
	migs := g.GetGkeMigs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(migs))
	mock.AssertExpectationsForObjects(t, server)
}

func TestCreateNodePool_TPUMultiHost_DifferentMainMigZone(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napEnabled, true, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1/operations/operation-1505728466148-d16f5197").Return(createNodePoolOperationResponseSuccess).Once()
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1/clusters/cluster1/nodePools").Return(createNodePoolResponseZoneB).Once()

	getClusterResponseTPU := fmt.Sprintf(getClusterResponseTemplate, defaultAndTPUNodePool, napEnabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1/clusters/cluster1").Return(getClusterResponseTPU)
	server.On("handle", "/projects/project1/zones/us-central1-a/instanceGroupManagers").Return(buildListInstanceGroupManagersResponse(buildListInstanceGroupManagersResponsePart(defaultPoolMigName, zoneA, 1)))
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-a/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneA)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()

	mig := newTestMigSpec(g)
	mig.gceRef.Zone = zoneA
	mig.spec.TpuMultiHost = true
	mig.spec.TpuTopology = "4x4"
	mig.spec.MachineType = machineTypeTPU
	newMig, err := g.CreateNodePool(mig)
	assert.NoError(t, err)
	assert.True(t, newMig.MainCreatedMig.Exist())
	migs := g.GetGkeMigs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(migs))
	mock.AssertExpectationsForObjects(t, server)
}

func TestNodePoolSpecForNode(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()

	autoscaledNodePool := "autoscaled-pool"
	nonAutoscaledNodePool := "non-autoscaled-pool"
	nodePoolsResponse := `[
		{
			"name": "autoscaled-pool",
			"instanceGroupUrls": ["https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool"],
			"config": { "machineType": "n1-standard-1" },
			"autoscaling": { "Enabled": true, "MinNodeCount": 1, "MaxNodeCount": 3 },
			"status": "RUNNING"
		},
		{
			"name": "non-autoscaled-pool",
			"instanceGroupUrls": ["https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-other-pool"],
			"config": { "machineType": "e2-standard-2" },
			"status": "RUNNING"
		}
	]`
	clusterResponse := fmt.Sprintf(`{
		"name": "cluster1",
		"location": "us-central1-b",
		"nodePools": %s
	}`, nodePoolsResponse)

	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(clusterResponse).Once()

	listMigsResponse := buildListInstanceGroupManagersResponse(
		buildListInstanceGroupManagersResponsePart("gke-cluster-1-default-pool", "us-central1-b", 1),
		buildListInstanceGroupManagersResponsePart("gke-cluster-1-other-pool", "us-central1-b", 1),
	)
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers").Return(listMigsResponse).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-other-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Maybe()

	m := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, m.cache)
	err := m.refreshGkeResources()
	assert.NoError(t, err)

	tcs := []struct {
		desc      string
		node      *apiv1.Node
		wantError bool
	}{
		{
			desc: "AutoscaledNodePools",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-autoscaled",
					Labels: map[string]string{
						"cloud.google.com/gke-nodepool": autoscaledNodePool,
					},
				},
			},
		},
		{
			desc: "NonAutoscaledNodePools",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-non-autoscaled",
					Labels: map[string]string{
						"cloud.google.com/gke-nodepool": nonAutoscaledNodePool,
					},
				},
			},
		},
		{
			desc: "MissingNodeLabel",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-missing-label",
				},
			},
			wantError: true,
		},
		{
			desc: "UnknownNodePool",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-unknown-pool",
					Labels: map[string]string{
						"cloud.google.com/gke-nodepool": "unknown-pool",
					},
				},
			},
			wantError: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			spec, err := m.NodePoolSpecForNode(tc.node)
			if tc.wantError {
				assert.Error(t, err)
				assert.Nil(t, spec)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, spec)
			}
		})
	}
}

func TestCreateNodePoolFail(t *testing.T) {
	testCases := []struct {
		name                            string
		createNodePoolResponse          string
		createNodePoolOperationResponse string
		deleteNodePoolResponse          string
		deleteNodePoolOperationResponse string
		nodePools                       string
		wantIsPersistentError           bool
	}{
		{
			name: "Error during nodepool creation",
			createNodePoolResponse: `{
				"name": "operation-1505728466148-d16f5198",
				"zone": "us-central1-b",
				"operationType": "CREATE_NODE_POOL",
				"status": "RUNNING",
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5198",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z"
			}`,
			createNodePoolOperationResponse: `{
				"name": "operation-1505728466148-d16f5198",
				"zone": "us-central1-b",
				"operationType": "CREATE_NODE_POOL",
				"status": "DONE",
				"error": {"code": 7, "message": "Permission error"},
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/operations/operation-1505728466148-d16f5198",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z",
				"endTime": "2017-09-18T09:54:35.124878859Z"
			}`,
			deleteNodePoolResponse: `{
				"name": "operation-1505732351373-819ed94a",
				"zone": "us-central1-b",
				"operationType": "DELETE_NODE_POOL",
				"status": "RUNNING",
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/operations/operation-1505732351373-819ed94a",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z"
			}`,
			deleteNodePoolOperationResponse: `{
				"name": "operation-1505732351373-819ed94a",
				"zone": "us-central1-b",
				"operationType": "DELETE_NODE_POOL",
				"status": "DONE",
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/operations/operation-1505732351373-819ed94a",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z"
			}`,
			wantIsPersistentError: true,
		},
		{
			name: "Nodepool created but with error",
			createNodePoolResponse: `{
				"name": "operation-1505728466148-d16f5198",
				"zone": "us-central1-b",
				"operationType": "CREATE_NODE_POOL",
				"status": "RUNNING",
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5198",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z"
			}`,
			createNodePoolOperationResponse: `{
				"name": "operation-1505728466148-d16f5198",
				"zone": "us-central1-b",
				"operationType": "CREATE_NODE_POOL",
				"status": "DONE",
				"error": {"code": 13, "message": "Internal error."},
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/operations/operation-1505728466148-d16f5198",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z",
				"endTime": "2017-09-18T09:54:35.124878859Z"
			}`,
			deleteNodePoolResponse: `{
				"name": "operation-1505732351373-819ed94a",
				"zone": "us-central1-b",
				"operationType": "DELETE_NODE_POOL",
				"status": "RUNNING",
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/operations/operation-1505732351373-819ed94a",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z"
			}`,
			deleteNodePoolOperationResponse: `{
				"name": "operation-1505732351373-819ed94a",
				"zone": "us-central1-b",
				"operationType": "DELETE_NODE_POOL",
				"status": "DONE",
				"selfLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/operations/operation-1505732351373-819ed94a",
				"targetLink": "https://container.googleapis.com/v1beta1/projects/601024681890/locations/us-central1-b/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
				"startTime": "2017-09-18T09:54:26.148507311Z"
			}`,
			wantIsPersistentError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()
			g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)

			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/operations/operation-1505728466148-d16f5198").Return(tc.createNodePoolOperationResponse).Once()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1/nodePools").Return(tc.createNodePoolResponse).Once()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1/nodePools/nodeautoprovisioning-111111111").Return(tc.deleteNodePoolResponse).Once()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/operations/operation-1505732351373-819ed94a").Return(tc.deleteNodePoolOperationResponse).Once()

			if !tc.wantIsPersistentError {
				nodePools := `
					"nodePools": [
						{
							"name": "nodeautoprovisioning-111111111",
							"config": {
							"machineType": "n1-standard-1",
							"diskSizeGb": 100,
							"oauthScopes": [
								"https://www.googleapis.com/auth/compute",
								"https://www.googleapis.com/auth/devstorage.read_only",
								"https://www.googleapis.com/auth/service.management.readonly",
								"https://www.googleapis.com/auth/servicecontrol",
								"https://www.googleapis.com/auth/logging.write",
								"https://www.googleapis.com/auth/monitoring"
							],
							"imageType": "COS",
							"serviceAccount": "default",
							"diskType": "pd-standard"
							},
							"initialNodeCount": 1,
							"autoscaling": {
							"enabled": true,
							"maxNodeCount": 5
							},
							"management": {},
							"selfLink": "https:///v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1/nodePools/nodeautoprovisioning-111111111",
							"version": "1.8.0-gke.1",
							"instanceGroupUrls": [
							"https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/nodeautoprovisioning-111111111"
							],
							"status": "ERROR"
						}
					]`

				clusterResponse := fmt.Sprintf(getClusterResponseTemplate, nodePools, napEnabled, false, "")
				server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(clusterResponse)
				server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers").Return(getInstanceGroupManager(zoneB))
				server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/nodeautoprovisioning-111111111").Return("")
			}

			mig := &GkeMig{
				gceRef: gce.GceRef{
					Project: projectId,
					Zone:    zoneB,
					Name:    "nodeautoprovisioning-111111111",
				},
				gkeManager:      g,
				minSize:         0,
				maxSize:         1000,
				autoprovisioned: true,
				exist:           true,
				spec: &gkeclient.NodePoolSpec{
					MachineType:        machineTypeA,
					SystemArchitecture: &arch,
					Taints: []apiv1.Taint{
						{
							Key:   "taint1",
							Value: "value",
						},
					},
				},
			}
			AddMigsToNodePool("nodeautoprovisioning-111111111", mig)

			_, err := g.CreateNodePool(mig)
			if err == nil {
				t.Errorf("expected error during NodePoolCreate call, got nil")
			}
			if actualIsPersistentError := autoscalererrors.ToAutoscalerError(autoscalererrors.InternalError, err).Type() == gkeclient.GkePersistentOperationError; tc.wantIsPersistentError != actualIsPersistentError {
				wantErrorTypeForLogs := gkeclient.GkePersistentOperationError
				if !tc.wantIsPersistentError {
					wantErrorTypeForLogs = "NOT " + gkeclient.GkePersistentOperationError
				}
				t.Errorf("expected error during NodePoolCreate call of type %s, got: %v\nError: %v", wantErrorTypeForLogs, autoscalererrors.ToAutoscalerError(autoscalererrors.InternalError, err).Type(), err)
			}
			mock.AssertExpectationsForObjects(t, server)
		})
	}
}

const deleteInstancesResponse = `{
  "kind": "compute#operation",
  "id": "8554136016090105726",
  "name": "operation-1505802641136-55984ff86d980-a99e8c2b-0c8aaaaa",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a",
  "operationType": "compute.instanceGroupManagers.deleteInstances",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/instanceGroupManagers/gke-cluster-1-default-pool-f7607aac-grp",
  "targetId": "5382990249302819619",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2017-09-18T23:30:41.612-07:00",
  "startTime": "2017-09-18T23:30:41.618-07:00",
  "endTime": "2017-09-18T23:30:41.618-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/operations/operation-1505802641136-55984ff86d980-a99e8c2b-0c8aaaaa"
}`

const deleteInstancesOperationResponse = `
{
  "kind": "compute#operation",
  "id": "8554136016090105726",
  "name": "operation-1505802641136-55984ff86d980-a99e8c2b-0c8aaaaa",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a",
  "operationType": "compute.instanceGroupManagers.deleteInstances",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/instanceGroupManagers/gke-cluster-1-default-pool-f7607aac-grp",
  "targetId": "5382990249302819619",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2017-09-18T23:30:41.612-07:00",
  "startTime": "2017-09-18T23:30:41.618-07:00",
  "endTime": "2017-09-18T23:30:41.618-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/operations/operation-1505802641136-55984ff86d980-a99e8c2b-0c8aaaaa"
}`

func testZonalNodePool() *nodepoolSetup {
	mig := &GkeMig{
		gceRef: gce.GceRef{
			Name:    defaultPoolMigName,
			Zone:    zoneB,
			Project: projectId,
		},
		exist:           true,
		autoprovisioned: false,
		minSize:         1,
		maxSize:         11,
	}
	AddMigsToNodePool(defaultPool, mig)
	return &nodepoolSetup{
		migs: []*GkeMig{mig},
	}
}

func testRegionalNodePool() *nodepoolSetup {
	var migs []*GkeMig
	for _, zone := range zones {
		mig := &GkeMig{
			gceRef: gce.GceRef{
				Name:    defaultPoolMigName,
				Zone:    zone,
				Project: projectId,
			},
			exist:           true,
			autoprovisioned: false,
			minSize:         1,
			maxSize:         11,
		}
		migs = append(migs, mig)
	}
	AddMigsToNodePool(defaultPool, migs...)
	return &nodepoolSetup{
		migs: migs,
	}
}

func testAutoprovisionedPool() *nodepoolSetup {
	mig := &GkeMig{
		gceRef: gce.GceRef{
			Name:    autoprovisionedPoolMigName,
			Zone:    zoneB,
			Project: projectId,
		},
		exist:           true,
		autoprovisioned: true,
		minSize:         minAutoprovisionedSize,
		maxSize:         napMaxNodes,
	}
	AddMigsToNodePool(autoprovisionedPool, mig)
	return &nodepoolSetup{
		migs: []*GkeMig{mig},
	}
}

type nodepoolSetup struct {
	migs          []*GkeMig
	cacheBaseName bool
	atomicResize  bool
}

func (n *nodepoolSetup) withCachedBaseName() *nodepoolSetup {
	n.cacheBaseName = true
	return n
}

func (n *nodepoolSetup) withAtomicResize() *nodepoolSetup {
	n.atomicResize = true
	return n
}

func (n *nodepoolSetup) configure(manager *gkeManagerImpl) *nodepoolSetup {
	for _, m := range n.migs {
		m.gkeManager = manager
		if n.atomicResize {
			m.spec = &gkeclient.NodePoolSpec{
				TpuMultiHost: true,
				TpuType:      "tpuV5",
			}
		}
		manager.cache.RegisterMig(m)
		if n.cacheBaseName {
			manager.cache.SetMigBasename(m.GceRef(), m.GceRef().Name)
		}
	}
	return n
}

func TestDeleteInstances(t *testing.T) {
	testCases := []struct {
		desc      string
		nodepools []*nodepoolSetup
		instances []gce.GceRef
		wantCalls map[string]string
		wantErr   string
	}{
		{
			desc: "two instances from same MIG",
			nodepools: []*nodepoolSetup{
				testZonalNodePool(),
			},
			instances: []gce.GceRef{
				{
					Project: projectId,
					Zone:    zoneB,
					Name:    "gke-cluster-1-default-pool-f7607aac-f1hm",
				},
				{
					Project: projectId,
					Zone:    zoneB,
					Name:    "gke-cluster-1-default-pool-f7607aac-c63g",
				},
			},
			wantCalls: map[string]string{
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool":                        getInstanceGroupManager(zoneB),
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/listManagedInstances":   getManagedInstancesResponse1(zoneB),
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/deleteInstances":        deleteInstancesResponse,
				"/projects/project1/zones/us-central1-b/operations/operation-1505802641136-55984ff86d980-a99e8c2b-0c8aaaaa/wait": deleteInstancesOperationResponse,
			},
		},
		{
			desc: "two instances from different MIGs",
			nodepools: []*nodepoolSetup{
				testZonalNodePool(),
				testAutoprovisionedPool().withCachedBaseName(),
			},
			instances: []gce.GceRef{
				{
					Project: projectId,
					Zone:    zoneB,
					Name:    "gke-cluster-1-default-pool-f7607aac-f1hm",
				},
				{
					Project: projectId,
					Zone:    zoneB,
					Name:    "gke-cluster-1-nodeautoprovisioning-323233232-gdf607aac-9j4g",
				},
			},
			wantCalls: map[string]string{
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool":                                        getInstanceGroupManager(zoneB),
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/listManagedInstances":                   getManagedInstancesResponse1(zoneB),
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232/listManagedInstances": getManagedInstancesResponse2(zoneB),
			},
			wantErr: "cannot delete instances which don't belong to the same MIG.",
		},
		{
			desc: "atomically resized MIG",
			nodepools: []*nodepoolSetup{
				testZonalNodePool().withAtomicResize(),
			},
			instances: []gce.GceRef{
				{
					Project: projectId,
					Zone:    zoneB,
					Name:    "gke-cluster-1-default-pool-f7607aac-f1hm",
				},
				{
					Project: projectId,
					Zone:    zoneB,
					Name:    "gke-cluster-1-default-pool-f7607aac-c63g",
				},
			},
			wantCalls: map[string]string{
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool":                        getInstanceGroupManager(zoneB),
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/listManagedInstances":   getManagedInstancesResponse1(zoneB),
				"/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/resize":                 setMigSizeResponse,
				"/projects/project1/zones/us-central1-b/operations/operation-1505739408819-5597646964339-eb839c88-28805931/wait": setMigSizeOperationResponse,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()
			g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
			addDefaultListMigsMocks(server, g.cache)
			for _, np := range tc.nodepools {
				np.configure(g)
			}
			for path, response := range tc.wantCalls {
				server.On("handle", path).Return(response).Once()
			}
			err := g.DeleteInstances(tc.instances)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Equal(t, tc.wantErr, err.Error())
			}
			mock.AssertExpectationsForObjects(t, server)
		})
	}
}

const setMigSizeResponse = `{
  "kind": "compute#operation",
  "id": "7558996788000226430",
  "name": "operation-1505739408819-5597646964339-eb839c88-28805931",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a",
  "operationType": "compute.instanceGroupManagers.resize",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/instanceGroupManagers/gke-cluster-1-default-pool-f7607aac-grp",
  "targetId": "5382990249302819619",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2017-09-18T05:56:49.227-07:00",
  "startTime": "2017-09-18T05:56:49.230-07:00",
  "endTime": "2017-09-18T05:56:49.230-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/operations/operation-1505739408819-5597646964339-eb839c88-28805931"
}`

const setMigSizeOperationResponse = `{
  "kind": "compute#operation",
  "id": "7558996788000226430",
  "name": "operation-1505739408819-5597646964339-eb839c88-28805931",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a",
  "operationType": "compute.instanceGroupManagers.resize",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/instanceGroupManagers/gke-cluster-1-default-pool-f7607aac-grp",
  "targetId": "5382990249302819619",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2017-09-18T05:56:49.227-07:00",
  "startTime": "2017-09-18T05:56:49.230-07:00",
  "endTime": "2017-09-18T05:56:49.230-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/operations/operation-1505739408819-5597646964339-eb839c88-28805931"
}`

const createInstancesResponse = `{
  "kind": "compute#operation",
  "id": "2890052495600280364",
  "name": "operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b",
  "operationType": "compute.instanceGroupManagers.createInstances",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool-e25725dc-grp",
  "targetId": "7836594831806456968",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2021-06-22T05:55:31.903-07:00",
  "startTime": "2021-06-22T05:55:31.907-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32"
}`

const createInstancesOperationResponse = `{
  "kind": "compute#operation",
  "id": "2890052495600280364",
  "name": "operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b",
  "operationType": "compute.instanceGroupManagers.createInstances",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool-e25725dc-grp",
  "targetId": "7836594831806456968",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2021-06-22T05:55:31.903-07:00",
  "startTime": "2021-06-22T05:55:31.907-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32"
}`

func TestCreateInstances(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	mig := testZonalNodePool().configure(g).migs[0]
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/listManagedInstances").Return(buildListInstanceGroupManagersResponse(
		buildListInstanceGroupManagersResponsePart(defaultPoolMigName, zoneB, 3),
	)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", fmt.Sprintf("/projects/project1/zones/us-central1-b/instanceGroupManagers/%v/createInstances", mig.gceRef.Name)).Return(createInstancesResponse).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32/wait").Return(createInstancesOperationResponse).Once()
	err := g.CreateInstances(mig, 1)
	assert.NoError(t, err)

	// Cache should be updated during CreateInstances so we do not expect API call on subsequent GetMigSize
	server.On("handle", fmt.Sprintf("/projects/project1/zones/us-central1-b/instanceGroupManagers/%v/createInstances", mig.gceRef.Name)).Return(createInstancesResponse).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32/wait").Return(createInstancesOperationResponse).Once()
	err = g.CreateInstances(mig, 1)
	assert.NoError(t, err)
}

func TestCreateInstancesWithMultipleRequests(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	mig := testZonalNodePool().configure(g).migs[0]
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/listManagedInstances").Return(buildListInstanceGroupManagersResponse(
		buildListInstanceGroupManagersResponsePart(defaultPoolMigName, zoneB, 3),
	)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()

	tests := []struct {
		delta        int
		wantRequests int
	}{
		{
			delta:        1000,
			wantRequests: 1,
		},
		{
			delta:        1001,
			wantRequests: 2,
		},
		{
			delta:        3000,
			wantRequests: 3,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("delta=%v", tt.delta), func(t *testing.T) {
			server.On("handle", fmt.Sprintf("/projects/project1/zones/us-central1-b/instanceGroupManagers/%v/createInstances", mig.gceRef.Name)).Return(createInstancesResponse).Times(tt.wantRequests)
			server.On("handle", "/projects/project1/zones/us-central1-b/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32/wait").Return(createInstancesOperationResponse).Times(tt.wantRequests)
			err := g.CreateInstances(mig, int64(tt.delta))
			assert.NoError(t, err)
		})
	}
}

func TestCreateFlexResizeRequests(t *testing.T) {
	tests := []struct {
		name             string
		managerErr       error
		successfulRRs    int // throw on successfulRRs-th Resize Request
		delta            int
		wantErr          bool
		wantErrMessage   string
		registerFailures int
		isFragmented     bool
	}{
		{
			name:          "simple scale up",
			delta:         7,
			successfulRRs: 7,
		},
		{
			name:           "CreateResizeRequest fails on first call, fail",
			managerErr:     errors.New("test error"),
			successfulRRs:  0,
			delta:          10,
			wantErr:        true,
			wantErrMessage: "test error",
		},
		{
			name:             "CreateResizeRequest fails on 10th call, don't fail",
			managerErr:       errors.New("Service account test-account does not exist"),
			successfulRRs:    10,
			delta:            17,
			wantErr:          false,
			registerFailures: 7,
			wantErrMessage:   "service account deleted: Service account test-account does not exist",
		},
		{
			name:          "Big scale up is capped to max batch size",
			delta:         200,
			successfulRRs: maxFlexStartRRBatchSize,
			isFragmented:  true,
		},
		{
			name:             "Big scale up is capped to max batch size with errors",
			managerErr:       errors.New("Service account test-account does not exist"),
			successfulRRs:    10,
			registerFailures: maxFlexStartRRBatchSize - 10,
			delta:            200,
			isFragmented:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()
			g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)

			mockResReqClient := resizerequestclient.NewResizeRequestClientMock()

			mig := testZonalNodePool().configure(g).migs[0]
			mrd24hString := "86400"
			duration, _ := time.ParseDuration(fmt.Sprintf("%ss", mrd24hString))
			mig.spec = &gkeclient.NodePoolSpec{
				MaxRunDurationInSeconds: mrd24hString,
			}

			for i := range tt.delta {
				rrMatch := mock.MatchedBy(func(rr resizerequestclient.ResizeRequestCreateRequest) bool {
					return strings.HasSuffix(rr.Name, fmt.Sprintf("-%d", i)) &&
						rr.ResizeBy == 1 &&
						rr.RequestedRunDuration.Seconds() == duration.Seconds()
				})

				if i == tt.successfulRRs {
					// Add an extra call only if the test expects an error
					if tt.managerErr != nil {
						mockResReqClient.On("CreateResizeRequest", mock.Anything, mig.gceRef, rrMatch).Return(tt.managerErr).Once()
					}
					break
				}
				mockResReqClient.On("CreateResizeRequest", mock.Anything, mig.gceRef, rrMatch).Return(nil).Once()
			}
			if tt.isFragmented {
				mockResReqClient.On("RegisterFailedResizeRequestsCreation", mig.gceRef, fragmentedResizeRequestWarning(int64(tt.delta), maxFlexStartRRBatchSize), tt.delta-maxFlexStartRRBatchSize).Once()
			}
			if tt.registerFailures > 0 {
				mockResReqClient.On("RegisterFailedResizeRequestsCreation", mig.gceRef, tt.managerErr, tt.registerFailures).Once()
			}

			g.flexResizeRequestService = mockResReqClient

			err := g.CreateFlexResizeRequests(mig, int64(tt.delta))
			if tt.wantErr != (err != nil) {
				t.Fatalf("wantedError: %t, but got: %v", tt.wantErr, err)
			}
			if tt.wantErr && len(tt.wantErrMessage) > 0 && tt.wantErrMessage != err.Error() {
				t.Errorf("wantErrMessage: %s, but got: %s", tt.wantErrMessage, err.Error())
			}

			// We call API again to get MIG sizes after cache invalidation in CreateFlexResizeRequests
			server.On("handle", fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s", mig.gceRef.Project, mig.gceRef.Zone, mig.gceRef.Name)).Return(
				getInstanceGroupManagerNamed(mig.gceRef.Name, mig.gceRef.Zone, tt.successfulRRs),
			).Once()
			size, err := g.GetMigSize(mig)
			assert.Equal(t, int64(tt.successfulRRs), size)
			assert.NoError(t, err)

			mock.AssertExpectationsForObjects(t, mockResReqClient)
			mock.AssertExpectationsForObjects(t, server)
		})
	}
}

func TestCreateQueuedInstances(t *testing.T) {
	npName := "queued-np"
	migRef := gce.GceRef{
		Name:    "queued-mig",
		Zone:    "us-central1-a",
		Project: "project",
	}
	prName := "prov-req-1"
	prNamespace := "default"
	rrName := "gke-default-prov-req-1-347831d881f4f342"
	delta := int64(13)

	tests := []struct {
		name              string
		migSpec           *gkeclient.NodePoolSpec
		wantRR            *resizerequestclient.ResizeRequestStatus
		wantBulkMigStatus *bulkmig.Status
		wantLabel         bool
		wantAccelerator   string
	}{
		{
			name: "gpu queued provisioning mig",
			migSpec: &gkeclient.NodePoolSpec{
				Accelerators: []*gke_api_beta.AcceleratorConfig{{
					AcceleratorType:  "nvidia-tesla-l4",
					AcceleratorCount: int64(1),
				}},
			},
			wantRR: &resizerequestclient.ResizeRequestStatus{
				Name:                 rrName,
				ResizeBy:             delta,
				State:                resizerequestclient.ResizeRequestStateAccepted,
				ProjectID:            migRef.Project,
				MigName:              migRef.Name,
				Zone:                 migRef.Zone,
				RequestedRunDuration: &queuedwrapper.DefaultMaxRunDuration,
			},
			wantAccelerator: "nvidia-tesla-l4",
		},
		{
			name: "tpu queued provisioning mig",
			migSpec: &gkeclient.NodePoolSpec{
				TpuType:     "tpu-v5-lite-podslice",
				TpuTopology: "4x4",
			},
			wantRR: &resizerequestclient.ResizeRequestStatus{
				Name:                 rrName,
				ResizeBy:             delta,
				State:                resizerequestclient.ResizeRequestStateAccepted,
				ProjectID:            migRef.Project,
				MigName:              migRef.Name,
				Zone:                 migRef.Zone,
				RequestedRunDuration: &queuedwrapper.DefaultMaxRunDuration,
			},
			wantAccelerator: "tpu-v5-lite-podslice",
		},
		{
			name: "bulkMig queued provisioning mig",
			migSpec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				Accelerators: []*gke_api_beta.AcceleratorConfig{{
					AcceleratorType:  "nvidia-gb200",
					AcceleratorCount: int64(4),
				}},
				PlacementGroup: placement.Spec{
					Policy: "a4x-policy",
				},
				FlexStart: true,
			},
			wantBulkMigStatus: &bulkmig.Status{
				Ref:        migRef,
				InProgress: true,
				TargetSize: delta,
			},
			wantLabel:       true,
			wantAccelerator: "nvidia-gb200",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()
			g := newTestGkeManager(t, server.URL, false, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			pr := provreqstate.ProvisioningRequestInStateForTests(prNamespace, prName, "", "", provreqstate.PendingState, time.Time{}, time.Second)
			bulkMigRefs := []gce.GceRef{}
			if tt.wantRR == nil {
				bulkMigRefs = append(bulkMigRefs, migRef)
			}
			fakeProvReqManager := manager.NewProvReqManagerFake(nil, bulkMigRefs, []*provreqwrapper.ProvisioningRequest{pr})
			g.provisioningRequestManager = fakeProvReqManager

			mig := &GkeMig{
				gceRef:             migRef,
				queuedProvisioning: true,
				spec:               tt.migSpec,
				gkeManager:         &GkeManagerMock{},
			}
			AddMigsToNodePool(npName, mig)

			err := g.CreateQueuedInstances(prpods.GetProvReqID(pr), mig, delta, manager.UpdateProvReqDetails)
			assert.NoError(t, err)

			if tt.wantRR != nil {
				rrs, err := g.ResizeRequests(mig)
				assert.NoError(t, err)
				assert.Equal(t, 1, len(rrs))
				assert.Equal(t, *tt.wantRR, rrs[0])
			} else {
				bulkMigs := fakeProvReqManager.BulkMigs()
				assert.Equal(t, 1, len(bulkMigs))
				assert.Equal(t, tt.wantBulkMigStatus, bulkMigs[0])
			}

			if tt.wantLabel {
				assert.Equal(t, rrName, fakeProvReqManager.MigLabels(migRef)[gkelabels.ProvisioningRequestLabelKey])
			} else {
				assert.Empty(t, fakeProvReqManager.MigLabels(migRef))
			}

			assert.Equal(t, provreqstate.AcceptedState, provreqstate.StateOfProvisioningRequest(pr))
			qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
			assert.Equal(t, tt.wantAccelerator, *qpr.AcceleratorType())
			assert.Equal(t, migRef.Name, *qpr.NodeGroupName())
			assert.Equal(t, npName, *qpr.NodePoolName())
			assert.Equal(t, migRef.Zone, *qpr.SelectedZone())
			assert.Equal(t, "false", *qpr.NodePoolAutoProvisioned())
			assert.Equal(t, "prov-req-1-pod-template-0", *qpr.PodTemplateName())

			if tt.wantRR != nil {
				assert.Equal(t, rrName, *qpr.ResizeRequestName())
				assert.Equal(t, queuedwrapper.ProvisioningModeResizeRequest, *qpr.ProvisioningMode())
			} else {
				assert.Equal(t, queuedwrapper.ProvisioningModeBulkMig, *qpr.ProvisioningMode())
			}
		})
	}
}

func TestCreateResizeRequest(t *testing.T) {
	delta := 13
	mrd := 24 * time.Hour

	tests := []struct {
		name          string
		migSpec       *gkeclient.NodePoolSpec
		flexMatcher   func(rr resizerequestclient.ResizeRequestCreateRequest) bool
		atomicMatcher func(rr resizerequestclient.ResizeRequestCreateRequest) bool
	}{
		{
			name: "flex mig - flex service, mrd",
			migSpec: &gkeclient.NodePoolSpec{
				FlexStart:               true,
				MaxRunDurationInSeconds: fmt.Sprintf("%.f", mrd.Seconds()),
			},
			flexMatcher: func(rr resizerequestclient.ResizeRequestCreateRequest) bool {
				return strings.HasPrefix(rr.Name, "flex-") &&
					rr.ResizeBy == int64(delta) &&
					rr.RequestedRunDuration.Seconds() == mrd.Seconds()
			},
		},
		{
			name: "regular mig - atomic service",
			migSpec: &gkeclient.NodePoolSpec{
				FlexStart: false,
			},
			atomicMatcher: func(rr resizerequestclient.ResizeRequestCreateRequest) bool {
				return strings.HasPrefix(rr.Name, "rr-") &&
					rr.ResizeBy == int64(delta) &&
					rr.RequestedRunDuration == nil
			},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()
			g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)

			atomicClientMock := resizerequestclient.NewResizeRequestClientMock()
			flexClientMock := resizerequestclient.NewResizeRequestClientMock()
			g.atomicResizeRequestService = atomicClientMock
			g.flexResizeRequestService = flexClientMock

			mig := testZonalNodePool().configure(g).migs[0]
			mig.gceRef.Name = fmt.Sprintf("mig-%d", i)
			mig.spec = tt.migSpec

			if tt.flexMatcher != nil {
				flexClientMock.On("CreateResizeRequest", mock.Anything, mig.gceRef, mock.MatchedBy(func(rr resizerequestclient.ResizeRequestCreateRequest) bool {
					return tt.flexMatcher(rr)
				})).Return(nil).Once()
			}
			if tt.atomicMatcher != nil {
				atomicClientMock.On("CreateResizeRequest", mock.Anything, mig.gceRef, mock.MatchedBy(func(rr resizerequestclient.ResizeRequestCreateRequest) bool {
					return tt.atomicMatcher(rr)
				})).Return(nil).Once()
			}

			err := g.CreateResizeRequest(mig, int64(delta))
			assert.NoError(t, err)

			mock.AssertExpectationsForObjects(t, flexClientMock)
			mock.AssertExpectationsForObjects(t, atomicClientMock)
		})
	}
}

func TestGetSetMigSize(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	mig := testZonalNodePool().configure(g).migs[0]

	// First GetMigSize - expect cache repopulation

	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()

	size, err := g.GetMigSize(mig)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), size)

	// Another get - return value from cache
	size, err = g.GetMigSize(mig)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), size)
	mock.AssertExpectationsForObjects(t, server)

	// SetMigSize - expect API call to update target size
	server.On("handle", fmt.Sprintf("/projects/project1/zones/us-central1-b/instanceGroupManagers/%v/resize", defaultPoolMigName)).Return(setMigSizeResponse).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/operations/operation-1505739408819-5597646964339-eb839c88-28805931/wait").Return(setMigSizeOperationResponse).Once()
	err = g.SetMigSize(mig, 5)
	assert.NoError(t, err)

	// Cache should be updated during SetMigSize so we do not expect API call on subsequent GetMigSize
	size, err = g.GetMigSize(mig)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), size)
	mock.AssertExpectationsForObjects(t, server)

	// Register another mig
	mig2 := testAutoprovisionedPool().configure(g).migs[0]

	// GetMigSize on another mig will trigger cache repopulation

	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232").Return(getInstanceGroupManagerNamed(autoprovisionedPoolMigName, zoneB, 7)).Once()
	size, err = g.GetMigSize(mig2)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), size)
	mock.AssertExpectationsForObjects(t, server)

	// No extra call on another GetMigSize
	size, err = g.GetMigSize(mig2)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), size)
	mock.AssertExpectationsForObjects(t, server)

	// We call API again after cache invalidation
	g.cache.InvalidateAllMigTargetSizes()

	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-nodeautoprovisioning-323233232").Return(getInstanceGroupManagerNamed(autoprovisionedPoolMigName, zoneB, 7)).Once()
	size, err = g.GetMigSize(mig2)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), size)
	mock.AssertExpectationsForObjects(t, server)
}

func TestGetMigForInstance(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

	addDefaultListMigsMocks(server, g.cache)

	testZonalNodePool().configure(g)

	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool/listManagedInstances").Return(getManagedInstancesResponse1(zoneB)).Once()
	gceRef := gce.GceRef{
		Project: projectId,
		Zone:    zoneB,
		Name:    "gke-cluster-1-default-pool-f7607aac-f1hm",
	}

	mig, err := g.GetMigForInstance(gceRef)
	assert.NoError(t, err)
	assert.NotNil(t, mig)
	assert.Equal(t, "gke-cluster-1-default-pool", mig.GceRef().Name)
	mock.AssertExpectationsForObjects(t, server)
}

func TestGetMigsTargetSize(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	migs := testRegionalNodePool().configure(g).migs

	// Set up mock responses
	for _, mig := range migs {
		zone := mig.GceRef().Zone
		server.On("handle", fmt.Sprintf("/projects/project1/zones/%s/instanceGroupManagers", zone)).Return(
			buildListInstanceGroupManagersResponse(
				buildListInstanceGroupManagersResponsePart(defaultPoolMigName, zone, 3),
			)).Once()
	}

	addDefaultListMigsMocks(server, g.cache)

	// First of the GetMigSize should get trigger cache repopulation and another
	// gets should return value from cache.
	for _, mig := range migs {
		size, err := g.GetMigSize(mig)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), size)
		mock.AssertExpectationsForObjects(t, server)
	}

	var migRefs []gce.GceRef
	for _, mig := range migs {
		migRefs = append(migRefs, mig.GceRef())
	}
	migsTargetSize, err := g.GetMigsTargetSize(migRefs)
	assert.Nil(t, err)
	assert.Equal(t, int64(9), migsTargetSize)
	mock.AssertExpectationsForObjects(t, server)
}

func TestGetMigsTargetSizeErrors(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	migs := testRegionalNodePool().configure(g).migs

	// Set up mock response for the error path
	zone := migs[0].GceRef().Zone
	server.On("handle", fmt.Sprintf("/projects/project1/zones/%s/instanceGroupManagers", zone)).Return("", errors.New("test error")).Once()

	addDefaultListMigsMocks(server, g.cache)

	var migRefs []gce.GceRef
	for _, mig := range migs {
		migRefs = append(migRefs, mig.GceRef())
	}
	migsTargetSize, err := g.GetMigsTargetSize(migRefs)
	assert.Error(t, err)
	assert.Equal(t, int64(0), migsTargetSize)
	mock.AssertExpectationsForObjects(t, server)
}

func TestGetMigNodes(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

	addDefaultListMigsMocks(server, g.cache)

	server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/nodeautoprovisioning-323233232/listManagedInstances").Return(getManagedInstancesResponse1(zoneB)).Once()

	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneB,
			Name:    "nodeautoprovisioning-323233232",
		},
		gkeManager:      g,
		minSize:         0,
		maxSize:         1000,
		autoprovisioned: true,
		exist:           true,
		spec:            nil,
	}
	AddMigsToNodePool("nodeautoprovisioning-323233232", mig)

	nodes, err := g.GetMigNodes(mig)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(nodes))
	assert.Equal(t, "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-9j4g", nodes[0].Id)
	assert.Equal(t, "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-c63g", nodes[1].Id)
	assert.Equal(t, "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-dck1", nodes[2].Id)
	assert.Equal(t, "gce://project1/us-central1-b/gke-cluster-1-default-pool-f7607aac-f1hm", nodes[3].Id)
	for i := 0; i < 4; i++ {
		assert.Nil(t, nodes[i].Status.ErrorInfo)
		assert.Equal(t, cloudprovider.InstanceRunning, nodes[i].Status.State)
	}

	mock.AssertExpectationsForObjects(t, server)
}

func TestFetchResourceLimiter(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()

	g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	clusterWithoutNodePools := fmt.Sprintf(getClusterResponseTemplate, `"nodePools": []`, napEnabled, false, "")
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(clusterWithoutNodePools).Once()

	err := g.refreshGkeResources()
	assert.NoError(t, err)
	resourceLimiter, err := g.GetResourceLimiter(nil)
	assert.NoError(t, err)
	assert.NotNil(t, resourceLimiter)

	mock.AssertExpectationsForObjects(t, server)
}

const listMachineTypesResponse = `{
 "kind": "compute#machineTypeList",
 "id": "projects/project1/zones/us-central1-c/machineTypes",
 "items": [
  {
   "kind": "compute#machineType",
   "id": "1000",
   "creationTimestamp": "1969-12-31T16:00:00.000-08:00",
   "name": "f1-micro",
   "description": "1 vCPU (shared physical core) and 0.6 GB RAM",
   "guestCpus": 1,
   "memoryMb": 614,
   "imageSpaceGb": 0,
   "maximumPersistentDisks": 16,
   "maximumPersistentDisksSizeGb": "3072",
   "zone": "us-central1-c",
   "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-c/machineTypes/f1-micro",
   "isSharedCpu": true
  },
  {
   "kind": "compute#machineType",
   "id": "2000",
   "creationTimestamp": "1969-12-31T16:00:00.000-08:00",
   "name": "g1-small",
   "description": "1 vCPU (shared physical core) and 1.7 GB RAM",
   "guestCpus": 1,
   "memoryMb": 1740,
   "imageSpaceGb": 0,
   "maximumPersistentDisks": 16,
   "maximumPersistentDisksSizeGb": "3072",
   "zone": "us-central1-c",
   "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-c/machineTypes/g1-small",
   "isSharedCpu": true
  }
 ],
 "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-c/machineTypes"
}`

func TestRefreshMachinesCache(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()

	g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)
	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneB,
			Name:    "default-pool",
		},
	}
	g.cache.RegisterMig(mig)
	g.gkeConfigurationCache.setAutoprovisioningLocations([]string{})

	server.On("handle", "/projects/project1/zones/us-central1-b/machineTypes").Return(listMachineTypesResponse).Once()
	err := g.refreshMachinesCache()
	assert.NoError(t, err)
	machine, _ := g.cache.GetMachine("f1-micro", zoneB)
	assert.NotNil(t, machine)
	machine, _ = g.cache.GetMachine("g1-small", zoneB)
	assert.NotNil(t, machine)
	mock.AssertExpectationsForObjects(t, server)

	// Skipped refresh.
	err = g.refreshMachinesCache()
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, server)

	// Refresh again.
	server.On("handle", "/projects/project1/zones/us-central1-b/machineTypes").Return(listMachineTypesResponse).Once()
	g.machinesCacheLastRefresh = time.Now().Add(-2 * time.Hour)
	err = g.refreshMachinesCache()
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, server)

	// Get machines for autoprovisioning locations with existing nodes.
	g.gkeConfigurationCache.setAutoprovisioningLocations([]string{zoneC})
	server.On("handle", "/projects/project1/zones/us-central1-b/machineTypes").Return(listMachineTypesResponse).Once()
	server.On("handle", "/projects/project1/zones/us-central1-c/machineTypes").Return(listMachineTypesResponse).Once()
	g.machinesCacheLastRefresh = time.Now().Add(-2 * time.Hour)
	err = g.refreshMachinesCache()
	assert.NoError(t, err)
	machine, _ = g.cache.GetMachine("f1-micro", zoneB)
	assert.NotNil(t, machine)
	machine, _ = g.cache.GetMachine("f1-micro", zoneC)
	assert.NotNil(t, machine)
	mock.AssertExpectationsForObjects(t, server)
}

const getMachineTypeResponse = `{
  "kind": "compute#machineType",
  "id": "3001",
  "creationTimestamp": "2015-01-16T09:25:43.314-08:00",
  "name": "n1-standard-2",
  "description": "2 vCPU, 3.75 GB RAM",
  "guestCpus": 2,
  "memoryMb": 3840,
  "maximumPersistentDisks": 32,
  "maximumPersistentDisksSizeGb": "65536",
  "zone": "us-central1-a",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/krzysztof-jastrzebski-dev/zones/us-central1-a/machineTypes/n1-standard-1",
  "isSharedCpu": false
}`

func TestGetMachineType(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
	addDefaultListMigsMocks(server, g.cache)

	// Custom machine type.
	machine, err := g.GetMachineType("custom-8-2", zoneB)
	assert.NoError(t, err)
	assert.Equal(t, int64(8), machine.CPU)
	assert.Equal(t, int64(2*units.MiB), machine.Memory)
	mock.AssertExpectationsForObjects(t, server)

	// Standard machine type found in cache.
	machine, err = g.GetMachineType(machineTypeA, zoneB)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), machine.CPU)
	assert.Equal(t, int64(1*units.MiB), machine.Memory)
	mock.AssertExpectationsForObjects(t, server)

	// Standard machine type not found in cache.
	server.On("handle", "/projects/project1/zones/"+zoneB+"/machineTypes/n1-standard-2").Return(getMachineTypeResponse).Once()
	machine, err = g.GetMachineType("n1-standard-2", zoneB)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), machine.CPU)
	assert.Equal(t, int64(3840*units.MiB), machine.Memory)
	mock.AssertExpectationsForObjects(t, server)

	// Standard machine type cached.
	machine, err = g.GetMachineType("n1-standard-2", zoneB)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), machine.CPU)
	assert.Equal(t, int64(3840*units.MiB), machine.Memory)
	mock.AssertExpectationsForObjects(t, server)

	machine, err = g.GetMachineType("n1-standard-2", zoneB)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), machine.CPU)
	assert.Equal(t, int64(3840*units.MiB), machine.Memory)
	mock.AssertExpectationsForObjects(t, server)

	machine, err = g.GetMachineType("custom-8-2", zoneB)
	assert.NoError(t, err)
	assert.Equal(t, int64(8), machine.CPU)
	assert.Equal(t, int64(2*units.MiB), machine.Memory)
	mock.AssertExpectationsForObjects(t, server)

	// Standard machine type not found in the zone.
	server.On("handle", "/projects/project1/zones/us-central1-g/machineTypes/n1-standard-1").Return("").Once()
	_, err = g.GetMachineType(machineTypeA, "us-central1-g")
	assert.Error(t, err)
	mock.AssertExpectationsForObjects(t, server)
}

func TestGetMachineTypeErrorCache(t *testing.T) {
	tests := []struct {
		name        string
		statusCode  int
		response    string
		shouldCache bool
	}{
		{
			name:        "404 Not Found is cached",
			statusCode:  http.StatusNotFound,
			response:    "Not Found",
			shouldCache: true,
		},
		{
			name:        "400 Bad Request is cached",
			statusCode:  http.StatusBadRequest,
			response:    "Bad Request",
			shouldCache: true,
		},
		{
			name:        "429 Too Many Requests is cached",
			statusCode:  http.StatusTooManyRequests,
			response:    "Too Many Requests",
			shouldCache: true,
		},
		{
			name:        "503 Service Unavailable is not cached",
			statusCode:  http.StatusServiceUnavailable,
			response:    "Service Unavailable",
			shouldCache: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock(MockFieldStatusCode, MockFieldResponse)
			defer server.Close()
			g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
			addDefaultListMigsMocks(server, g.cache)

			machineName := fmt.Sprintf("n1-%d", tc.statusCode)
			path := fmt.Sprintf("/projects/project1/zones/us-central1-g/machineTypes/%s", machineName)

			// First call: GCE API returns the configured status code.
			server.On("handle", path).Return(tc.statusCode, tc.response).Once()
			_, err1 := g.GetMachineType(machineName, "us-central1-g")
			assert.Error(t, err1)
			mock.AssertExpectationsForObjects(t, server)

			if tc.shouldCache {
				// Second call: Should hit the negative cache and return the EXACT SAME error immediately.
				// Since the mock was configured to run "Once()" and we haven't added more expectations,
				// any call to the GCE API will cause the mock server to fail the test.
				_, err2 := g.GetMachineType(machineName, "us-central1-g")
				assert.Error(t, err2)
				assert.Equal(t, err1, err2)
			} else {
				// Second call: Should NOT hit the cache, so it must call GCE API again. We set up another expectation.
				server.On("handle", path).Return(tc.statusCode, tc.response).Once()
				_, err2 := g.GetMachineType(machineName, "us-central1-g")
				assert.Error(t, err2)
			}
			mock.AssertExpectationsForObjects(t, server)
		})
	}
}

func TestApplyThreadsPerCore(t *testing.T) {
	tests := []struct {
		name           string
		threadsPerCore int64
		cpu            int64
		machineType    string
		result         int
		wantErr        bool
	}{
		{
			name:           "SMT-off, custom machine type",
			threadsPerCore: 1,
			cpu:            8,
			machineType:    "custom-8-2",
			result:         4,
		},
		{
			name:           "SMT-on, n1-standard-2 machine type",
			threadsPerCore: 2,
			cpu:            2,
			machineType:    "n1-standard-2",
			result:         2,
		},
		{
			name:           "SMT unspecified, n1-standard-2 machine type",
			threadsPerCore: 0,
			cpu:            4,
			machineType:    "n1-standard-2",
			result:         4,
		},
		{
			name:           "SMT unspecified, t2d-standard-16 machine type",
			threadsPerCore: 0,
			cpu:            16,
			machineType:    "t2d-standard-16",
			result:         16,
		},
		{
			name:           "SMT-on, t2d-standard-16 machine type",
			threadsPerCore: 2,
			cpu:            16,
			machineType:    "t2d-standard-16",
			result:         32,
		},
		{
			name:           "SMT-off, single core custom VM",
			threadsPerCore: 1,
			cpu:            1,
			machineType:    "custom-1-6656",
			result:         1,
		},
		{
			name:           "SMT-on, ct6e-standard-8t",
			threadsPerCore: 2,
			cpu:            180,
			machineType:    "ct6e-standard-8t",
			result:         360,
		},
		{
			name:           "SMT-off, ct6e-standard-8t",
			threadsPerCore: 1,
			cpu:            180,
			machineType:    "ct6e-standard-8t",
			result:         180,
		},
		{
			name:           "SMT unspecified, ct6e-standard-8t default to SMT=1",
			threadsPerCore: 0,
			cpu:            180,
			machineType:    "ct6e-standard-8t",
			result:         180,
		},
		{
			name:           "Inconsistent threading",
			threadsPerCore: 1,
			cpu:            3,
			machineType:    "custom-3-6656",
			wantErr:        true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gkeMachineType, err := machinetypes.NewMachineConfigProvider(nil).ToMachineType(test.machineType)
			if err != nil {
				t.Errorf("bad test configuration: cannot get GKE machine type for %s", test.machineType)
			}
			gceMachineType := gce.MachineType{
				Name: test.machineType,
				CPU:  test.cpu,
			}
			cpu, err := getMachineTypeCpu(gkeMachineType, gceMachineType, test.threadsPerCore)
			if test.wantErr && err == nil {
				t.Errorf("getMachineTypeCpu(GCE CPU = %d, template threads = %d, machine type = %s) = nil, want error", test.cpu, test.threadsPerCore, test.machineType)
			}
			if !test.wantErr && err != nil {
				t.Errorf("getMachineTypeCpu(GCE CPU = %d, template threads = %d, machine type = %s) = %v, want nil", test.cpu, test.threadsPerCore, test.machineType, err)
			}
			if cpu != int64(test.result) {
				t.Errorf("getMachineTypeCpu(GCE CPU = %d, template threads = %d, machine type = %s) = %d, want: %d", test.cpu, test.threadsPerCore, test.machineType, cpu, test.result)
			}
		})
	}
}

func TestAddPodBucketingCapacity(t *testing.T) {
	tests := []struct {
		name      string
		inputNode *apiv1.Node
		wantNode  *apiv1.Node
		wantErr   bool
	}{
		{
			name: "no pod-capacity label",
			inputNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels:   map[string]string{},
				},
				Status: apiv1.NodeStatus{
					Capacity:    apiv1.ResourceList{},
					Allocatable: apiv1.ResourceList{},
				},
			},
			wantNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels:   map[string]string{},
				},
				Status: apiv1.NodeStatus{
					Capacity:    apiv1.ResourceList{},
					Allocatable: apiv1.ResourceList{},
				},
			},
		},
		{
			name: "pod-capacity label valid",
			inputNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels: map[string]string{
						gkelabels.PodCapacityLabel: "1",
					},
				},
				Status: apiv1.NodeStatus{
					Capacity:    apiv1.ResourceList{},
					Allocatable: apiv1.ResourceList{},
				},
			},
			wantNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels: map[string]string{
						gkelabels.PodCapacityLabel: "1",
					},
				},
				Status: apiv1.NodeStatus{
					Capacity: apiv1.ResourceList{
						gkelabels.PodCapacityLabel: *resource.NewQuantity(1, resource.DecimalSI),
					},
					Allocatable: apiv1.ResourceList{
						gkelabels.PodCapacityLabel: *resource.NewQuantity(1, resource.DecimalSI),
					},
				},
			},
		},
		{
			name: "pod-capacity label valid 2",
			inputNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels: map[string]string{
						gkelabels.PodCapacityLabel: "2",
					},
				},
				Status: apiv1.NodeStatus{
					Capacity:    apiv1.ResourceList{},
					Allocatable: apiv1.ResourceList{},
				},
			},
			wantNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels: map[string]string{
						gkelabels.PodCapacityLabel: "2",
					},
				},
				Status: apiv1.NodeStatus{
					Capacity: apiv1.ResourceList{
						gkelabels.PodCapacityLabel: *resource.NewQuantity(2, resource.DecimalSI),
					},
					Allocatable: apiv1.ResourceList{
						gkelabels.PodCapacityLabel: *resource.NewQuantity(2, resource.DecimalSI),
					},
				},
			},
		},
		{
			name: "pod-capacity label invalid",
			inputNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels: map[string]string{
						gkelabels.PodCapacityLabel: "abc",
					},
				},
				Status: apiv1.NodeStatus{
					Capacity:    apiv1.ResourceList{},
					Allocatable: apiv1.ResourceList{},
				},
			},
			wantNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:     "gk3-nap-1",
					SelfLink: "/api/v1/nodes/gk3-nap-1",
					Labels: map[string]string{
						gkelabels.PodCapacityLabel: "abc",
					},
				},
				Status: apiv1.NodeStatus{
					Capacity:    apiv1.ResourceList{},
					Allocatable: apiv1.ResourceList{},
				},
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotNode, err := addPodBucketingCapacity(test.inputNode)
			if test.wantErr && err == nil {
				t.Errorf("addPodBucketingCapacity(inputNode) = nil, want error")
			}
			if !test.wantErr && err != nil {
				t.Errorf("addPodBucketingCapacity(inputNode) = %v, want nil", err)
			}

			if diff := cmp.Diff(test.wantNode, gotNode); diff != "" {
				t.Errorf("addPodBucketingCapacity() node diff (-want +got): %s", diff)
			}
		})
	}
}

func TestConfidentialNodesEnabled(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)

	testCases := []struct {
		name                     string
		confidentialNodesEnabled bool
		confidentialInstanceType string
		expected                 bool
	}{
		{
			name:                     "confidential nodes disabled",
			confidentialNodesEnabled: false,
			confidentialInstanceType: "",
			expected:                 false,
		},
		{
			name:                     "confidential nodes disabled with UnspecifiedConfidentialNodeTypeValue only",
			confidentialInstanceType: gkelabels.UnspecifiedConfidentialNodeTypeValue,
			expected:                 false,
		},
		{
			name:                     "confidential nodes enabled with SEV",
			confidentialNodesEnabled: true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expected:                 true,
		},
		{
			name:                     "confidential nodes enabled with TDX",
			confidentialNodesEnabled: true,
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expected:                 true,
		},
		{
			name:                     "confidential nodes enabled with unspecified",
			confidentialNodesEnabled: true,
			confidentialInstanceType: gkelabels.UnspecifiedConfidentialNodeTypeValue,
			expected:                 true,
		},
		{
			name:                     "confidential nodes enabled with empty type",
			confidentialNodesEnabled: true,
			confidentialInstanceType: "",
			expected:                 true,
		},
		{
			name:                     "confidential nodes enabled with confidentialInstanceType SEV only",
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expected:                 true,
		},
		{
			name:                     "confidential nodes enabled with confidentialInstanceType TDX only",
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expected:                 true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			response := fmt.Sprintf(getClusterResponseTemplate, `"nodePools": []`, napEnabled, tc.confidentialNodesEnabled, tc.confidentialInstanceType)
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(response).Once()

			assert.NoError(t, g.refreshGkeResources())
			assert.Equal(t, tc.expected, g.AreConfidentialNodesEnabled())
		})
	}
}

func TestGetConfidentialInstanceType(t *testing.T) {
	testCases := []struct {
		name                     string
		confidentialInstanceType string
		enabled                  bool
	}{
		{
			name:                     "SEV",
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			enabled:                  true,
		},
		{
			name:                     "SEV_SNP",
			confidentialInstanceType: gkelabels.SEVSNPConfidentialNodeTypeValue,
			enabled:                  true,
		},
		{
			name:                     "TDX",
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			enabled:                  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)

			// Cluster with confidential nodes enabled and instance type.
			withConfidentialNodesEnabled := fmt.Sprintf(getClusterResponseTemplate, `"nodePools": []`, napEnabled, tc.enabled, tc.confidentialInstanceType)
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(withConfidentialNodesEnabled).Once()

			assert.NoError(t, g.refreshGkeResources())
			assert.Equal(t, tc.confidentialInstanceType, g.GetConfidentialInstanceType())
		})
	}
}

type mockClusterLocationsObserver struct {
	mock.Mock
}

func (mclo *mockClusterLocationsObserver) SetLocations(locations []string) {
	mclo.Called(locations)
}

func TestRefreshAutoprovisioningLocations(t *testing.T) {
	testCases := []struct {
		scenario                          string
		locations                         []string
		autoprovisioningLocations         []string
		expectedAutoprovisioningLocations []string
	}{
		{
			scenario:                          "No autoprovisioning locations",
			locations:                         []string{zoneA, zoneB, zoneC},
			expectedAutoprovisioningLocations: []string{zoneA, zoneB, zoneC},
		},
		{
			scenario:                          "Use autoprovisioning locations",
			locations:                         []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:         []string{zoneA, zoneC},
			expectedAutoprovisioningLocations: []string{zoneA, zoneC},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.scenario, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()

			g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			cluster := gkeclient.Cluster{
				Locations:                 testCase.locations,
				AutoprovisioningLocations: testCase.autoprovisioningLocations,
			}
			g.refreshAutoprovisioningLocations(&cluster)

			assert.Equal(t, testCase.expectedAutoprovisioningLocations, g.GetAutoprovisioningLocations())
			mock.AssertExpectationsForObjects(t, server)
		})
	}
}

type mockResizableVmAutoprovisioningProvider struct {
	mock.Mock
}

func (mekp *mockResizableVmAutoprovisioningProvider) ResizingEnabled(machineFamily string) bool {
	args := mekp.Called(machineFamily)
	return args.Get(0).(bool)
}

func (mekp *mockResizableVmAutoprovisioningProvider) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	args := mekp.Called(machineFamily)
	return args.Get(0).(bool)
}

func (mekp *mockResizableVmAutoprovisioningProvider) IsEkEdpEnabled() bool {
	args := mekp.Called()
	return args.Get(0).(bool)
}

func (mekp *mockResizableVmAutoprovisioningProvider) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	args := mekp.Called(machineFamily)
	return args.Get(0).(bool)
}

func (mekp *mockResizableVmAutoprovisioningProvider) IsExtendedFallbacksEnabled() bool {
	args := mekp.Called()
	return args.Get(0).(bool)
}

func (mekp *mockResizableVmAutoprovisioningProvider) Refresh() {}

func (mekp *mockResizableVmAutoprovisioningProvider) NodesCount(machineFamily string) int {
	args := mekp.Called(machineFamily)
	return args.Int(0)
}

func (mekp *mockResizableVmAutoprovisioningProvider) HasActiveResizableNodes() bool {
	args := mekp.Called()
	return args.Get(0).(bool)
}

func TestRefreshAutoprovisioningLocations_Observer(t *testing.T) {
	testCases := []struct {
		scenario                     string
		locations                    []string
		autoprovisioningLocations    []string
		ekResizingEnabled            bool
		ekNodesCount                 int
		e4aResizingEnabled           bool
		e4aNodesCount                int
		expectedObserverSetLocations []string
	}{
		{
			scenario:                     "EK resizing enabled - no autoprovisioning locations",
			locations:                    []string{zoneA, zoneB, zoneC},
			ekResizingEnabled:            true,
			ekNodesCount:                 1,
			expectedObserverSetLocations: []string{zoneA, zoneB, zoneC},
		},
		{
			scenario:                     "E4A resizing enabled - no autoprovisioning locations",
			locations:                    []string{zoneA, zoneB, zoneC},
			e4aResizingEnabled:           true,
			e4aNodesCount:                1,
			expectedObserverSetLocations: []string{zoneA, zoneB, zoneC},
		},
		{
			scenario:                     "EK resizing enabled - use autoprovisioning locations",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			ekResizingEnabled:            true,
			ekNodesCount:                 3,
			expectedObserverSetLocations: []string{zoneA, zoneC},
		},
		{
			scenario:                     "E4A resizing enabled - use autoprovisioning locations",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			e4aResizingEnabled:           true,
			e4aNodesCount:                3,
			expectedObserverSetLocations: []string{zoneA, zoneC},
		},
		{
			scenario:                     "EK & E4A resizing enabled - use autoprovisioning locations",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			ekResizingEnabled:            true,
			ekNodesCount:                 1,
			e4aResizingEnabled:           true,
			e4aNodesCount:                2,
			expectedObserverSetLocations: []string{zoneA, zoneC},
		},
		{
			scenario:                     "EK resizing disabled",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			ekResizingEnabled:            false,
			ekNodesCount:                 3,
			expectedObserverSetLocations: nil,
		},
		{
			scenario:                     "E4A resizing disabled",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			e4aResizingEnabled:           false,
			e4aNodesCount:                3,
			expectedObserverSetLocations: nil,
		},
		{
			scenario:                     "EK & E4A resizing disabled",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			ekResizingEnabled:            false,
			ekNodesCount:                 3,
			e4aResizingEnabled:           false,
			e4aNodesCount:                3,
			expectedObserverSetLocations: nil,
		},
		{
			scenario:                     "EK resizing enabled - no EK nodes",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			ekResizingEnabled:            true,
			ekNodesCount:                 0,
			expectedObserverSetLocations: nil,
		},
		{
			scenario:                     "E4A resizing enabled - no E4A nodes",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			e4aResizingEnabled:           true,
			e4aNodesCount:                0,
			expectedObserverSetLocations: nil,
		},
		{
			scenario:                     "EK & E4A resizing enabled - no EK or E4A nodes",
			locations:                    []string{zoneA, zoneB, zoneC},
			autoprovisioningLocations:    []string{zoneA, zoneC},
			ekResizingEnabled:            true,
			ekNodesCount:                 0,
			e4aResizingEnabled:           true,
			e4aNodesCount:                0,
			expectedObserverSetLocations: nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.scenario, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()

			g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			locationsObserver := &mockClusterLocationsObserver{}
			resizableVmAutoprovisioningProvider := &mockResizableVmAutoprovisioningProvider{}
			g.resizableVmAutoprovisioningProvider = resizableVmAutoprovisioningProvider
			g.clusterLocationsObserver = locationsObserver

			hasActiveNodes := (testCase.ekResizingEnabled && testCase.ekNodesCount > 0) || (testCase.e4aResizingEnabled && testCase.e4aNodesCount > 0)
			resizableVmAutoprovisioningProvider.On("HasActiveResizableNodes").Once().Return(hasActiveNodes)
			locationsObserver.On("SetLocations", testCase.expectedObserverSetLocations).Once()
			cluster := gkeclient.Cluster{
				Locations:                 testCase.locations,
				AutoprovisioningLocations: testCase.autoprovisioningLocations,
			}
			g.refreshAutoprovisioningLocations(&cluster)

			mock.AssertExpectationsForObjects(t, locationsObserver)
		})
	}
}

func buildTestMatcher(t *testing.T, patterns ...string) *gkelabels.Matcher {
	m, err := gkelabels.NewMatcher(patterns)
	assert.NoError(t, err)
	return m
}

func TestNodeLabelsFiltering(t *testing.T) {
	tcs := []struct {
		desc                           string
		input                          map[string]string
		expected                       map[string]string
		allowlistedSystemLabelsMatcher *gkelabels.Matcher
		bootDiskConfigEnabled          bool
	}{
		{
			desc:     "no gpu labels",
			input:    map[string]string{"f": "1", "g": "2"},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "gpu labels",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.GPULabel: "1"},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "pvm label true",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.PreemptibleLabel: gkelabels.PreemptionValue},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "pvm label false",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.PreemptibleLabel: "false"},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "pvm and gpu labels",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.GPULabel: "1", gkelabels.PreemptibleLabel: "false"},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "spot label true",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.SpotLabel: gkelabels.PreemptionValue},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "spot label false",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.SpotLabel: "false"},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "spot and gpu labels",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.GPULabel: "1", gkelabels.SpotLabel: "false"},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "gvisor labels",
			input:    map[string]string{"f": "1", "g": "2", sandbox.RuntimeLabelKey: sandbox.GVisorLabelValue},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "requested_min_cpu_platform label shouldn't be filtered out because it's added by CA",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.RequestedMinCpuPlatformLabel: "platform"},
			expected: map[string]string{"f": "1", "g": "2", gkelabels.RequestedMinCpuPlatformLabel: "platform"},
		},
		{
			desc:     "compute_class label shouldn't be filtered out because it's added by CA",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.ComputeClassLabel: "test-class"},
			expected: map[string]string{"f": "1", "g": "2", gkelabels.ComputeClassLabel: "test-class"},
		},
		{
			desc:     "accelerator count label shouldn't be filtered out because it's added by CA",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.AcceleratorCountLabel: "3"},
			expected: map[string]string{"f": "1", "g": "2", gkelabels.AcceleratorCountLabel: "3"},
		},
		{
			desc:     "supported-cpu-platform labels shouldn't be filtered out because they're added by CA",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.SupportedCpuPlatformKeyPrefix + "Intel_Haswell": "true"},
			expected: map[string]string{"f": "1", "g": "2", gkelabels.SupportedCpuPlatformKeyPrefix + "Intel_Haswell": "true"},
		},
		{
			desc:     "npc labels shouldn't be filtered out because they're added by CA",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.NodeProvisioningConfigLabel: "npc"},
			expected: map[string]string{"f": "1", "g": "2", gkelabels.NodeProvisioningConfigLabel: "npc"},
		},
		{
			desc:                           "allow explicit listed system label",
			input:                          map[string]string{"f": "1", "g": "2", "cloud.google.com/my-feature": "1"},
			expected:                       map[string]string{"f": "1", "g": "2", "cloud.google.com/my-feature": "1"},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, "cloud.google.com/my-feature"),
		},
		{
			// based on: go/node-labels-for-multiple-sdb
			desc: "allow listed system label regex patterns",
			input: map[string]string{
				"cloud.google.com/gke-secondary-boot-disk-{container-disk-image}": "CONTAINER_IMAGE_CACHE",
				"cloud.google.com/gke-secondary-boot-disk-{model-disk-image}":     "MODE_UNSPECIFIED",
			},
			expected: map[string]string{
				"cloud.google.com/gke-secondary-boot-disk-{container-disk-image}": "CONTAINER_IMAGE_CACHE",
				"cloud.google.com/gke-secondary-boot-disk-{model-disk-image}":     "MODE_UNSPECIFIED",
			},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, `cloud.google.com/gke-secondary-boot-disk-.+`),
		},
		{
			desc:                  "allow boot disk config label when enabled",
			input:                 map[string]string{"f": "1", "g": "2", gkelabels.BootDiskTypeLabelKey: "pd-ssd"},
			expected:              map[string]string{"f": "1", "g": "2", gkelabels.BootDiskTypeLabelKey: "pd-ssd"},
			bootDiskConfigEnabled: true,
		},
		{
			desc:     "disallow boot disk config label when disabled",
			input:    map[string]string{"f": "1", "g": "2", gkelabels.BootDiskTypeLabelKey: "pd-ssd"},
			expected: map[string]string{"f": "1", "g": "2"},
		},
		{
			desc:     "ReservationZone label is correctly filtered out",
			input:    map[string]string{gkelabels.ReservationZoneLabel: "us-central1-a"},
			expected: map[string]string{},
		},
		{
			desc:     "ProvReq label is not filtered out",
			input:    map[string]string{gkelabels.ProvisioningRequestLabelKey: "rr0"},
			expected: map[string]string{gkelabels.ProvisioningRequestLabelKey: "rr0"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			gotGkeLabels := filterOutExternalSystemLabels(tc.input, tc.allowlistedSystemLabelsMatcher, GkeManagerOptions{bootDiskConfigEnabled: tc.bootDiskConfigEnabled})
			assert.Equal(t, tc.expected, gotGkeLabels)
		})
	}
}

func TestNodeTaintsFiltering(t *testing.T) {
	tcs := []struct {
		desc     string
		input    []apiv1.Taint
		expected []apiv1.Taint
	}{
		{
			desc: "no gpu taints",
			input: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
			expected: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
		},
		{
			desc: "gpu taints",
			input: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
				{
					Key:    gpu.ResourceNvidiaGPU,
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
			expected: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
		},
		{
			desc: "gvisor taints",
			input: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
				{
					Key:    sandbox.RuntimeTaintKey,
					Value:  sandbox.GVisorTaintValue,
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
			expected: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
		},
		{
			desc: "tpu taints",
			input: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
				{
					Key:    tpu.ResourceGoogleTPU,
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
			expected: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			gotGkeLabels := filterOutSystemTaints(tc.input)
			assert.Equal(t, gotGkeLabels, tc.expected)
		})
	}
}

func TestMigInstancesCache(t *testing.T) {
	cache := gce.NewGceCache()

	for i := 0; i < 1000; i++ {
		ref := gce.GceRef{Project: "p1", Zone: "z1", Name: fmt.Sprintf("name-%v", i)}
		instance := gce.GceInstance{Instance: cloudprovider.Instance{Id: fmt.Sprintf("gce://p1/z1/inst-%v", i)}}

		go func() {
			err := cache.SetMigInstances(ref, []gce.GceInstance{instance}, time.Now())
			gotInstances, found := cache.GetMigInstances(ref)
			assert.Nil(t, err)
			assert.True(t, found)
			assert.Equal(t, 1, len(gotInstances))
			assert.Equal(t, instance, gotInstances[0])
		}()
	}
}

func TestImageDefaulting(t *testing.T) {
	managerWithImageType := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	managerWithImageType.autoprovisioningNodePoolDefaults = &gke_api_beta.AutoprovisioningNodePoolDefaults{
		ImageType: "cos_containerd",
	}
	tcs := []struct {
		desc     string
		manager  *gkeManagerImpl
		expected string
	}{
		{
			desc:     "no autopilot",
			manager:  newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil),
			expected: "cos_containerd",
		}, {
			desc:     "autopilot",
			manager:  newTestGkeManager(t, "", napEnabled, false, true, nil, false, nil),
			expected: "cos_containerd",
		}, {
			desc:     "cos_containerd from NAP AutoprovisioningDefaults",
			manager:  managerWithImageType,
			expected: managerWithImageType.autoprovisioningNodePoolDefaults.ImageType,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			imageType := tc.manager.GetImageTypeForNap(&GkeMig{})
			assert.Equal(t, imageType, tc.expected)
		})
	}
}

func TestOsDistributionForMig(t *testing.T) {
	manager := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	managerWithUbuntu := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	managerWithUbuntu.autoprovisioningNodePoolDefaults = &gke_api_beta.AutoprovisioningNodePoolDefaults{
		ImageType: "ubuntu_containerd",
	}
	tcs := []struct {
		desc     string
		manager  *gkeManagerImpl
		expected gce.OperatingSystemDistribution
		mig      *GkeMig
	}{
		{
			desc:     "mig spec with cos_containerd, should result in os=cos",
			manager:  manager,
			expected: gce.OperatingSystemDistributionCOS,
			mig: &GkeMig{
				gkeManager: manager,
				spec: &gkeclient.NodePoolSpec{
					ImageType: "cos_containerd",
				},
			},
		},
		{
			desc:     "mig spec empty, manager with cos default, should result in os=cos",
			manager:  manager,
			expected: gce.OperatingSystemDistributionCOS,
			mig: &GkeMig{
				gkeManager: manager,
				spec: &gkeclient.NodePoolSpec{
					ImageType: "",
				},
			},
		},
		{
			desc:     "mig spec empty, manager with ubuntu default, should result in os=ubuntu",
			manager:  managerWithUbuntu,
			expected: gce.OperatingSystemDistributionUbuntu,
			mig: &GkeMig{
				gkeManager: managerWithUbuntu,
				spec: &gkeclient.NodePoolSpec{
					ImageType: "",
				},
			},
		},
		{
			desc:     "mig spec with invalid value, manager with ubuntu default, should result in os=ubuntu",
			manager:  managerWithUbuntu,
			expected: gce.OperatingSystemDistributionUbuntu,
			mig: &GkeMig{
				gkeManager: managerWithUbuntu,
				spec: &gkeclient.NodePoolSpec{
					ImageType: "windows3.1_containerd",
				},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			distribution := tc.manager.GetOsDistributionForNap(tc.mig)
			assert.Equal(t, tc.expected, distribution)
		})
	}
}

func TestDefaultDiskSize(t *testing.T) {
	managerWithDisk := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	managerWithDisk.autoprovisioningNodePoolDefaults = &gke_api_beta.AutoprovisioningNodePoolDefaults{
		DiskSizeGb: 250,
	}
	tcs := []struct {
		desc     string
		manager  *gkeManagerImpl
		expected int64
		isNil    bool
	}{
		{
			desc:     "no autoprovisioningNodePoolDefaults Exist - Standard Cluster",
			manager:  newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil),
			expected: 100,
			isNil:    true,
		},
		{
			desc:     "no autoprovisioningNodePoolDefaults Exist - Autopilot Cluster",
			manager:  newTestGkeManager(t, "", napEnabled, false, true, nil, false, nil),
			expected: 250,
			isNil:    true,
		},
		{
			desc:     "autoprovisioningNodePoolDefaults Exist",
			manager:  managerWithDisk,
			expected: 250,
			isNil:    false,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			diskSize := tc.manager.GetDefaultNodePoolDiskSizeGB()
			if tc.isNil {
				assert.Nil(t, tc.manager.autoprovisioningNodePoolDefaults)
			}
			assert.Equal(t, diskSize, tc.expected)
		})
	}
}

// TODO: Migrate to TestGetMigTemplateNodeInfo.
func TestAutopilotMaxPodsPerNodeDefaulting(t *testing.T) {
	tcs := []struct {
		desc                          string
		autopilot                     bool
		autopilotHigherMaxPodsPerNode bool
		machineName                   string
		cpuCount                      int
		expected                      int64
	}{
		{
			desc:      "no autopilot",
			autopilot: false,
			expected:  110,
		},
		{
			desc:      "autopilot",
			autopilot: true,
			expected:  32,
		},
		{
			desc:                          "autopilot - higher mppn - 2-core",
			autopilot:                     true,
			autopilotHigherMaxPodsPerNode: true,
			machineName:                   "n1-standard-2",
			cpuCount:                      2,
			expected:                      32,
		},
		{
			desc:                          "autopilot - higher mppn - 4-core",
			autopilot:                     true,
			autopilotHigherMaxPodsPerNode: true,
			machineName:                   "n1-standard-4",
			cpuCount:                      4,
			expected:                      32,
		},
		{
			desc:                          "autopilot - higher mppn - 8-core",
			autopilot:                     true,
			autopilotHigherMaxPodsPerNode: true,
			machineName:                   "n1-standard-8",
			cpuCount:                      8,
			expected:                      64,
		},
		{
			desc:                          "autopilot - higher mppn - 16-core",
			autopilot:                     true,
			autopilotHigherMaxPodsPerNode: true,
			machineName:                   "n1-standard-16",
			cpuCount:                      16,
			expected:                      128,
		},
		{
			desc:                          "autopilot - higher mppn - 32-core",
			autopilot:                     true,
			autopilotHigherMaxPodsPerNode: true,
			machineName:                   "n1-standard-32",
			cpuCount:                      32,
			expected:                      128,
		},
		{
			desc:                          "autopilot - higher mppn - 1-core - arm",
			autopilot:                     true,
			autopilotHigherMaxPodsPerNode: true,
			machineName:                   "t2a-standard-1",
			cpuCount:                      1,
			expected:                      32,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			server := NewHttpServerMock()
			g := newTestGkeManager(t, server.URL, napDisabled, false, tc.autopilot, nil, tc.autopilotHigherMaxPodsPerNode, nil)

			addDefaultListMigsMocks(server, g.cache)

			cluster := fmt.Sprintf(getClusterResponseTemplate, allNodePools1, napDisabled, false, "")
			instanceTemplate := instanceTemplate
			getMachineTypeResponse := getMachineTypeResponse
			if tc.autopilotHigherMaxPodsPerNode {
				cluster = strings.Replace(cluster, "n1-standard-1", tc.machineName, 1)
				instanceTemplate = strings.Replace(instanceTemplate, "n1-standard-1", tc.machineName, 1)
				getMachineTypeResponse = strings.Replace(getMachineTypeResponse, "\"guestCpus\": 2", fmt.Sprintf("\"guestCpus\": %d", tc.cpuCount), 1)
			}

			// Fetch one node pool.
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(cluster).Once()
			server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Once()
			server.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(instanceTemplate).Once()
			if tc.autopilotHigherMaxPodsPerNode {
				server.On("handle", "/projects/project1/zones/"+zoneB+"/machineTypes/"+tc.machineName).Return(getMachineTypeResponse).Once()
			}

			err := g.refreshGkeResources()
			assert.NoError(t, err)
			migs := g.GetGkeMigs()

			nodeInfo, err := g.GetMigTemplateNodeInfo(migs[0])
			assert.NoError(t, err)
			allocatable := nodeInfo.Node().Status.Allocatable[apiv1.ResourcePods]
			assert.Equal(t, allocatable.Value(), tc.expected)
			capacity := nodeInfo.Node().Status.Capacity[apiv1.ResourcePods]
			assert.Equal(t, capacity.Value(), tc.expected)
		})
	}
}

func TestGetMigBlueGreenInfo(t *testing.T) {
	for tn, tc := range map[string]struct {
		migUrl   string
		bgInfo   *gkeclient.BlueGreenInfo
		wantInfo *MigBlueGreenInfo
		wantErr  error
	}{
		"blue MIG, valid phase": {
			migUrl: "mig-1",
			bgInfo: &gkeclient.BlueGreenInfo{
				BlueMigUrls:  []string{"mig-1"},
				GreenMigUrls: []string{"mig-2"},
				Phase:        "NODE_POOL_SOAKING",
			},
			wantInfo: &MigBlueGreenInfo{
				Color: BlueMig,
				Phase: gkeclient.PhaseNodePoolSoaking,
			},
		},
		"green MIG, valid phase": {
			migUrl: "mig-2",
			bgInfo: &gkeclient.BlueGreenInfo{
				BlueMigUrls:  []string{"mig-1"},
				GreenMigUrls: []string{"mig-2"},
				Phase:        "DELETING_BLUE_POOL",
			},
			wantInfo: &MigBlueGreenInfo{
				Color: GreenMig,
				Phase: gkeclient.PhaseDeletingBluePool,
			},
		},
		"MIG not green or blue is an error": {
			migUrl: "mig-3",
			bgInfo: &gkeclient.BlueGreenInfo{
				BlueMigUrls:  []string{"mig-1"},
				GreenMigUrls: []string{"mig-2"},
				Phase:        "DELETING_BLUE_POOL",
			},
			wantErr: cmpopts.AnyError,
		},
		"MIG autoscaled enabled": {
			migUrl: "mig-blue",
			bgInfo: &gkeclient.BlueGreenInfo{
				BlueMigUrls:  []string{"mig-blue"},
				GreenMigUrls: []string{"mig-2"},
				Phase:        "DELETING_BLUE_POOL",
				Autoscaled:   true,
			},
			wantInfo: &MigBlueGreenInfo{
				Color:        BlueMig,
				Phase:        gkeclient.PhaseDeletingBluePool,
				IsAutoScaled: true,
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			gotInfo, gotErr := getMigBlueGreenInfo(tc.bgInfo, tc.migUrl)
			if diff := cmp.Diff(tc.wantErr, gotErr, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("getMigBlueGreenInfo error diff (-want +got): %s", diff)
			}
			if diff := cmp.Diff(tc.wantInfo, gotInfo); diff != "" {
				t.Errorf("getMigBlueGreenInfo diff (-want +got): %s", diff)
			}
		})
	}
}

func TestRefreshNodePoolsBlueGreenInfo(t *testing.T) {
	mig1Id := "https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone/instanceGroupManagers/mig-1"
	mig2Id := "https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone/instanceGroupManagers/mig-2"
	mig3Id := "https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone/instanceGroupManagers/mig-3"
	pool1BeforeUpdate := gkeclient.NodePool{Name: "pool-1", InstanceGroupUrls: []string{mig1Id}, Autoscaled: true}
	pool1AfterUpdate := gkeclient.NodePool{Name: "pool-1", InstanceGroupUrls: []string{mig2Id}, Autoscaled: true}
	pool1DuringUpdate := gkeclient.NodePool{
		Name:              "pool-1",
		InstanceGroupUrls: []string{mig1Id, mig2Id},
		BlueGreenInfo: &gkeclient.BlueGreenInfo{
			Phase:        "CREATING_GREEN_POOL",
			BlueMigUrls:  []string{mig1Id},
			GreenMigUrls: []string{mig2Id},
		},
		Autoscaled: true,
	}
	pool1ErrorUpdate := gkeclient.NodePool{
		Name:              "pool-1",
		InstanceGroupUrls: []string{mig1Id, mig2Id, "additional-mig-id"},
		BlueGreenInfo: &gkeclient.BlueGreenInfo{
			Phase:        "NODE_POOL_SOAKING",
			BlueMigUrls:  []string{mig1Id},
			GreenMigUrls: []string{mig2Id},
		},
		Autoscaled: true,
	}
	pool2NoUpdate := gkeclient.NodePool{Name: "pool-2", InstanceGroupUrls: []string{mig3Id}, Autoscaled: true}

	type refreshAssertion struct {
		nodePools      []gkeclient.NodePool
		wantMigsBgInfo map[string]*MigBlueGreenInfo
	}
	type testCase []refreshAssertion
	for tn, tc := range map[string]testCase{
		"no update -> B/G update in one of the node pools -> back to no update": {
			{
				nodePools:      []gkeclient.NodePool{pool1BeforeUpdate, pool2NoUpdate},
				wantMigsBgInfo: map[string]*MigBlueGreenInfo{"mig-1": nil, "mig-3": nil},
			},
			{
				nodePools: []gkeclient.NodePool{pool1DuringUpdate, pool2NoUpdate},
				wantMigsBgInfo: map[string]*MigBlueGreenInfo{
					"mig-1": {Color: BlueMig, Phase: gkeclient.PhaseCreatingGreenPool},
					"mig-2": {Color: GreenMig, Phase: gkeclient.PhaseCreatingGreenPool},
					"mig-3": nil,
				},
			},
			{
				nodePools:      []gkeclient.NodePool{pool1AfterUpdate, pool2NoUpdate},
				wantMigsBgInfo: map[string]*MigBlueGreenInfo{"mig-2": nil, "mig-3": nil},
			},
		},
		"no update -> B/G error blocks autoscaling only in the affected node pool -> autoscaling is restored after the update": {
			{
				nodePools:      []gkeclient.NodePool{pool1BeforeUpdate, pool2NoUpdate},
				wantMigsBgInfo: map[string]*MigBlueGreenInfo{"mig-1": nil, "mig-3": nil},
			},
			{
				nodePools:      []gkeclient.NodePool{pool1ErrorUpdate, pool2NoUpdate},
				wantMigsBgInfo: map[string]*MigBlueGreenInfo{"mig-3": nil},
			},
			{
				nodePools:      []gkeclient.NodePool{pool1AfterUpdate, pool2NoUpdate},
				wantMigsBgInfo: map[string]*MigBlueGreenInfo{"mig-2": nil, "mig-3": nil},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			manager := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
			for i, refreshAssertion := range tc {
				manager.refreshNodePools(refreshAssertion.nodePools, sets.New[string]())
				migsBgInfo := map[string]*MigBlueGreenInfo{}
				for _, mig := range manager.GetGkeMigs() {
					migsBgInfo[mig.GceRef().Name] = mig.BlueGreenInfo()
				}
				if diff := cmp.Diff(refreshAssertion.wantMigsBgInfo, migsBgInfo); diff != "" {
					t.Errorf("refresh %d: migsBgInfo diff (-want +got): %s", i+1, diff)
				}
			}
		})
	}
}

func TestRefreshNodePoolsNonAutoscaled(t *testing.T) {
	mig1Id := "https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone/instanceGroupManagers/mig-1"
	pool1Autoscaled := gkeclient.NodePool{Name: "pool-1", InstanceGroupUrls: []string{mig1Id}, Autoscaled: true}
	pool1NonAutoscaled := gkeclient.NodePool{Name: "pool-1", InstanceGroupUrls: []string{mig1Id}, Autoscaled: false}

	manager := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)

	// MIG should be registered as it's autoscaled
	manager.refreshNodePools([]gkeclient.NodePool{pool1Autoscaled}, sets.New[string]())
	assert.Equal(t, 1, len(manager.GetGkeMigs()))
	assert.Equal(t, "mig-1", manager.GetGkeMigs()[0].GceRef().Name)

	// MIG should be unregistered as its status changed
	manager.refreshNodePools([]gkeclient.NodePool{pool1NonAutoscaled}, sets.New[string]())
	assert.Empty(t, manager.GetGkeMigs())
}

func TestInitializeOnce(t *testing.T) {
	testCases := []struct {
		desc              string
		funcResponses     []error
		alreadyTriggerred bool
		wantCalls         int
		wantError         bool
	}{
		{
			desc: "simple test case",
			funcResponses: []error{
				nil,
				nil,
			},
			wantCalls: 2,
		},
		{
			desc: "test with error",
			funcResponses: []error{
				errors.New("test1"),
				nil,
			},
			wantCalls: 2,
			wantError: true,
		},
		{
			desc: "test with multiple errors",
			funcResponses: []error{
				errors.New("test1"),
				nil,
				errors.New("test2"),
				nil,
				errors.New("test3"),
				nil,
			},
			wantCalls: 6,
			wantError: true,
		},
		{
			desc: "test with multiple errors, already run",
			funcResponses: []error{
				errors.New("test1"),
				errors.New("test2"),
				errors.New("test3"),
				nil,
			},
			alreadyTriggerred: true,
		},
		{
			desc: "test without errors, already run",
			funcResponses: []error{
				nil,
				nil,
				nil,
				nil,
			},
			alreadyTriggerred: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			gkeManager := &gkeManagerImpl{
				initializationFuncs: []InitializationFunc{},
			}
			if tc.alreadyTriggerred {
				gkeManager.initializationOnce.Do(func() {})
			}

			gotFuncCalls := 0
			for _, fr := range tc.funcResponses {
				fr := fr
				gkeManager.RegisterInitializationFunc(func() error {
					gotFuncCalls++
					return fr
				})
			}

			gotError := gkeManager.initializeOnce()
			if tc.wantError != (gotError != nil) {
				t.Errorf("Want error: %t, but got error: %v", tc.wantError, gotError)
			}
			if tc.wantCalls != gotFuncCalls {
				t.Errorf("Expected calls: %d, but got %d", tc.wantCalls, gotFuncCalls)
			}
		})
	}
}

func TestAddMultiNetworkCapacity(t *testing.T) {
	nodeWithNetworkRes := BuildTestNode("node1", 10, 10)
	nodeWithNetworkRes.Status.Allocatable["networking.gke.io.networks/red-net.IP"] = *resource.NewQuantity(1, resource.DecimalSI)
	nodeWithNetworkRes.Status.Capacity["networking.gke.io.networks/red-net.IP"] = *resource.NewQuantity(1, resource.DecimalSI)
	for desc, tc := range map[string]struct {
		mig              *GkeMig
		node             *apiv1.Node
		matcherResources map[string]resource.Quantity
		matcherErr       error
		wantAllocatable  apiv1.ResourceList
		wantCapacity     apiv1.ResourceList
		wantErr          error
	}{
		"empty network config, no changes": {
			mig:  &GkeMig{},
			node: BuildTestNode("node", 10, 10),
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
		},
		"non-empty network config, empty result from matcher, no changes": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					NetworkConfigs: []gkeclient.AdditionalNetworkConfig{
						gkeclient.TestAdditionalNetworkConfig("net1", "subnet", "", 0),
					},
				},
			},
			node: BuildTestNode("node", 10, 10),
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
		},
		"non-empty network config, non-empty result from matcher, changes to resources": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					NetworkConfigs: []gkeclient.AdditionalNetworkConfig{
						gkeclient.TestAdditionalNetworkConfig("net1", "subnet", "", 0),
					},
				},
			},
			matcherResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(10, resource.DecimalSI),
			},
			node: BuildTestNode("node", 10, 10),
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(10, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(10, resource.DecimalSI),
			},
		},
		"non-empty network config, multiple results from matcher, changes to resources": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					NetworkConfigs: []gkeclient.AdditionalNetworkConfig{
						gkeclient.TestAdditionalNetworkConfig("net1", "subnet", "", 0),
						gkeclient.TestAdditionalNetworkConfig("net2", "subnet2", "", 0),
					},
				},
			},
			matcherResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP":  *resource.NewQuantity(10, resource.DecimalSI),
				"networking.gke.io.networks/blue-net.IP": *resource.NewQuantity(10, resource.DecimalSI),
			},
			node: BuildTestNode("node", 10, 10),
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/red-net.IP"):  *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/blue-net.IP"): *resource.NewQuantity(10, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/red-net.IP"):  *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/blue-net.IP"): *resource.NewQuantity(10, resource.DecimalSI),
			},
		},
		"non-empty network config, non empty results from matcher, overriding existing resource, changes to resources": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					NetworkConfigs: []gkeclient.AdditionalNetworkConfig{
						gkeclient.TestAdditionalNetworkConfig("net1", "subnet", "", 0),
						gkeclient.TestAdditionalNetworkConfig("net2", "subnet2", "", 0),
					},
				},
			},
			matcherResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(10, resource.DecimalSI),
			},
			node: nodeWithNetworkRes,
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(10, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
				apiv1.ResourceName("networking.gke.io.networks/red-net.IP"): *resource.NewQuantity(10, resource.DecimalSI),
			},
		},
		"non-empty network config, error from matcher, error from function": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					NetworkConfigs: []gkeclient.AdditionalNetworkConfig{
						gkeclient.TestAdditionalNetworkConfig("net1", "subnet", "", 0),
					},
				},
			},
			matcherErr: errors.New("error"),
			node:       BuildTestNode("node", 10, 10),
			wantErr:    errors.New("error"),
		},
	} {
		t.Run(desc, func(t *testing.T) {
			m := mockMatcher{resources: tc.matcherResources, err: tc.matcherErr}
			node, err := addMultiNetworkCapacity(tc.node, tc.mig, &m)
			if tc.wantErr != nil {
				assert.Equal(t, tc.wantErr, err)
			} else {
				assert.Equal(t, node.Status.Capacity, tc.wantCapacity)
				assert.Equal(t, node.Status.Allocatable, tc.wantAllocatable)
			}
		})
	}
}

type mockMatcher struct {
	resources map[string]resource.Quantity
	err       error
}

func (m *mockMatcher) GetNetworkingResourcesFromNetworkConfig(_ []gkeclient.AdditionalNetworkConfig) (map[string]resource.Quantity, error) {
	return m.resources, m.err
}

func (m *mockMatcher) GetNetworkConfigFromResources(_ map[string]resource.Quantity, _ string) ([]gkeclient.AdditionalNetworkConfig, error) {
	return nil, nil
}

func TestGkeManagerImplQueuedProvisioningMigGceRefs(t *testing.T) {
	bulkMigSpec := &gkeclient.NodePoolSpec{MachineType: "a4x-highgpu-4g", FlexStart: true, PlacementGroup: placement.Spec{Policy: "a4x-policy"}}
	gceRefGen := func(id int) *gce.GceRef {
		return &gce.GceRef{
			Name: fmt.Sprintf("test-%d", id),
			Zone: "zone",
		}
	}
	tests := []struct {
		name         string
		gkeMigs      []*GkeMig
		wantRRMigs   map[gce.GceRef]common.GkeMigWrapper
		wantBulkMigs map[gce.GceRef]common.GkeMigWrapper
	}{
		{
			name: "simple case rr mig",
			gkeMigs: []*GkeMig{
				{
					gceRef:             *gceRefGen(1),
					queuedProvisioning: true,
				},
			},
			wantRRMigs: map[gce.GceRef]common.GkeMigWrapper{
				*gceRefGen(1): &common.FakeGkeMigWrapper{},
			},
		},
		{
			name: "simple case bulk mig",
			gkeMigs: []*GkeMig{
				{
					gceRef:             *gceRefGen(1),
					queuedProvisioning: true,
					spec:               bulkMigSpec,
				},
			},
			wantBulkMigs: map[gce.GceRef]common.GkeMigWrapper{
				*gceRefGen(1): &common.FakeGkeMigWrapper{},
			},
		},
		{
			name: "some non-queued migs present",
			gkeMigs: []*GkeMig{
				{
					gceRef:             *gceRefGen(1),
					queuedProvisioning: true,
				},
				{
					gceRef: *gceRefGen(2),
				},
				{
					gceRef:             *gceRefGen(3),
					queuedProvisioning: true,
				},
				{
					gceRef: *gceRefGen(4),
				},
				{
					gceRef:             *gceRefGen(5),
					queuedProvisioning: true,
					spec:               bulkMigSpec,
				},
				{
					gceRef:             *gceRefGen(6),
					queuedProvisioning: true,
					spec:               bulkMigSpec,
				},
			},
			wantRRMigs: map[gce.GceRef]common.GkeMigWrapper{
				*gceRefGen(1): &common.FakeGkeMigWrapper{},
				*gceRefGen(3): &common.FakeGkeMigWrapper{},
			},
			wantBulkMigs: map[gce.GceRef]common.GkeMigWrapper{
				*gceRefGen(5): &common.FakeGkeMigWrapper{},
				*gceRefGen(6): &common.FakeGkeMigWrapper{},
			},
		},
		{
			name: "all non-queued migs present",
			gkeMigs: []*GkeMig{
				{
					gceRef: *gceRefGen(1),
				},
				{
					gceRef: *gceRefGen(2),
				},
				{
					gceRef: *gceRefGen(3),
				},
				{
					gceRef: *gceRefGen(4),
				},
			},
			wantRRMigs: map[gce.GceRef]common.GkeMigWrapper{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gceCache := gce.NewGceCache()
			cache := NewGkeCache(gceCache, nodetemplate.NewCache())
			migLister := NewGkeMigLister(cache, irretrievableMigRefreshTime, irretrievableMigRefreshTime, 2)
			m := &gkeManagerImpl{
				cache:                 cache,
				migLister:             migLister,
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			}
			for _, gkeMig := range tt.gkeMigs {
				cache.RegisterMig(gkeMig)
				gkeMig.gkeManager = m
			}
			rrMigs, bulkMigs := m.queuedProvisioningMigGceRefs()
			assert.ElementsMatch(t, slices.Collect(maps.Keys(tt.wantRRMigs)), slices.Collect(maps.Keys(rrMigs)))
			assert.ElementsMatch(t, slices.Collect(maps.Keys(tt.wantBulkMigs)), slices.Collect(maps.Keys(bulkMigs)))
		})
	}
}

type mockAutoscalingGceClient struct {
	mock.Mock
	gce.AutoscalingGceClient
}

func (fagc *mockAutoscalingGceClient) FetchMigInstances(migRef gce.GceRef) ([]gce.GceInstance, error) {
	args := fagc.Called(migRef)
	return args.Get(0).([]gce.GceInstance), args.Error(1)
}

func (fagc *mockAutoscalingGceClient) FetchAllMigs(zone string) ([]*gce_api_compute.InstanceGroupManager, error) {
	args := fagc.Called(zone)
	return args.Get(0).([]*gce_api_compute.InstanceGroupManager), args.Error(1)
}

type mockAutoscalingOptionsProvider struct {
	scaleDownUnneededTime            *time.Duration
	scaleDownUtilizationThreshold    *float64
	scaleDownGpuUtilizationThreshold *float64
}

func (m *mockAutoscalingOptionsProvider) ScaleDownUnneededTime(cloudprovider.NodeGroup) (time.Duration, bool, error) {
	if m.scaleDownUnneededTime == nil {
		return 0, false, nil
	}

	return *m.scaleDownUnneededTime, true, nil
}

func (m *mockAutoscalingOptionsProvider) ScaleDownUtilizationThreshold(cloudprovider.NodeGroup) (float64, bool, error) {
	if m.scaleDownUtilizationThreshold == nil {
		return 0, false, nil
	}

	return *m.scaleDownUtilizationThreshold, true, nil
}

func (m *mockAutoscalingOptionsProvider) ScaleDownGpuUtilizationThreshold(cloudprovider.NodeGroup) (float64, bool, error) {
	if m.scaleDownGpuUtilizationThreshold == nil {
		return 0, false, nil
	}

	return *m.scaleDownGpuUtilizationThreshold, true, nil
}

func TestScaleDownUnreadyTimeOverride(t *testing.T) {
	tests := map[string]struct {
		mig                      *GkeMig
		experimentsManager       experiments.Manager
		wantScaleDownUnreadyTime *time.Duration
	}{
		"nilMig_noOverride": {
			experimentsManager: nil,
			mig:                nil,
		},
		"nilexperimentsManager_noOverride": {
			experimentsManager: nil,
			mig:                &GkeMig{queuedProvisioning: false},
		},
		"nonQueuedMig_noOverride": {
			experimentsManager: experiments.NewMockManager(),
			mig:                &GkeMig{queuedProvisioning: false},
		},
		"flagUnset_noOverride": {
			experimentsManager: experiments.NewMockManager(),
			mig:                &GkeMig{queuedProvisioning: true},
		},
		"flagInvalidFormat_timeUnitSuffix_noOverride": {
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, make(map[string]bool),
				map[string]string{experiments.ProvisioningRequestsScaleDownUnreadyFlag: "21m"},
			),
			mig: &GkeMig{queuedProvisioning: true},
		},
		"flagInvalidFormat_decimalPoint_noOverride": {
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, make(map[string]bool),
				map[string]string{experiments.ProvisioningRequestsScaleDownUnreadyFlag: "21.2"},
			),
			mig: &GkeMig{queuedProvisioning: true},
		},
		"flagValidFormat_override": {
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, make(map[string]bool),
				map[string]string{experiments.ProvisioningRequestsScaleDownUnreadyFlag: "21"},
			),
			mig:                      &GkeMig{queuedProvisioning: true},
			wantScaleDownUnreadyTime: ptr.To(21 * time.Second),
		},
	}

	for tcName, tc := range tests {
		t.Run(tcName, func(t *testing.T) {
			manager := gkeManagerImpl{optsTracker: optstracking.FakeOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{}, tc.experimentsManager)}

			gotUnreadyTime, found := manager.ScaleDownUnreadyTimeOverride(tc.mig)
			if tc.wantScaleDownUnreadyTime == nil {
				assert.False(t, found)
			} else {
				assert.Equal(t, *tc.wantScaleDownUnreadyTime, gotUnreadyTime)
				assert.True(t, found)
			}
		})
	}
}

func TestGkeManagerAutoscalingOverrides(t *testing.T) {
	unneeded := time.Hour
	utilization := 0.1
	gpuUtilization := 0.2
	mig := &GkeMig{}

	tests := map[string]struct {
		provider                             AutoscalingOptionsProvider
		wantScaleDownUnneededTime            *time.Duration
		wantScaleDownUtilizationThreshold    *float64
		wantScaleDownGpuUtilizationThreshold *float64
	}{
		"nil provider": {
			provider: nil,
		},
		"nil underlying impl": {
			provider: (*mockAutoscalingOptionsProvider)(nil),
		},
		"with overrides": {
			provider: &mockAutoscalingOptionsProvider{
				scaleDownUnneededTime:            &unneeded,
				scaleDownUtilizationThreshold:    &utilization,
				scaleDownGpuUtilizationThreshold: &gpuUtilization,
			},
			wantScaleDownUnneededTime:            &unneeded,
			wantScaleDownUtilizationThreshold:    &utilization,
			wantScaleDownGpuUtilizationThreshold: &gpuUtilization,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			manager := gkeManagerImpl{autoscalingOptsProvider: test.provider}

			gotUnneededTime, found, err := manager.ScaleDownUnneededTimeOverride(mig)
			assert.NoError(t, err)
			if test.wantScaleDownUnneededTime == nil {
				assert.False(t, found)
			} else {
				assert.Equal(t, *test.wantScaleDownUnneededTime, gotUnneededTime)
			}

			gotThreshold, found, err := manager.ScaleDownUtilizationThresholdOverride(mig)
			assert.NoError(t, err)
			if test.wantScaleDownUtilizationThreshold == nil {
				assert.False(t, found)
			} else {
				assert.Equal(t, *test.wantScaleDownUtilizationThreshold, gotThreshold)
			}

			gotGpuThreshold, found, err := manager.ScaleDownGpuUtilizationThresholdOverride(mig)
			assert.NoError(t, err)
			if test.wantScaleDownGpuUtilizationThreshold == nil {
				assert.False(t, found)
			} else {
				assert.Equal(t, *test.wantScaleDownGpuUtilizationThreshold, gotGpuThreshold)
			}
		})
	}
}

func TestInstanceByRef(t *testing.T) {
	gceRefGen := func(name string) gce.GceRef {
		return gce.GceRef{
			Project: projectId,
			Zone:    zoneA,
			Name:    name,
		}
	}

	instGen := func(name string) gce.GceInstance {
		return gce.GceInstance{Instance: cloudprovider.Instance{Id: gceRefGen(name).ToProviderId()}}
	}

	migRef := gceRefGen(gkeMigA)
	instanceRef := gceRefGen("gce-mig-a-inst3")

	testCases := []struct {
		name               string
		migPresent         bool
		expectedCacheFetch bool
		cachedInstances    []gce.GceInstance
		instances          []gce.GceInstance
		expectedResult     *gce.GceInstance
	}{
		{
			name:               "no mig",
			migPresent:         false,
			expectedCacheFetch: false,
			cachedInstances:    []gce.GceInstance{},
			instances:          []gce.GceInstance{},
			expectedResult:     nil,
		},
		{
			name:               "instance in cache",
			migPresent:         true,
			expectedCacheFetch: false,
			cachedInstances: []gce.GceInstance{
				instGen("gce-mig-a-inst1"),
				instGen("gce-mig-a-inst2"),
				instGen("gce-mig-a-inst3"),
			},
			instances: []gce.GceInstance{
				instGen("gce-mig-a-inst1"),
				instGen("gce-mig-a-inst2"),
				instGen("gce-mig-a-inst3"),
				instGen("gce-mig-a-inst4"),
			},
			expectedResult: &gce.GceInstance{Instance: cloudprovider.Instance{Id: gceRefGen("gce-mig-a-inst3").ToProviderId()}},
		},
		{
			name:               "instance not in cache",
			migPresent:         true,
			expectedCacheFetch: true,
			cachedInstances: []gce.GceInstance{
				instGen("gce-mig-a-inst1"),
				instGen("gce-mig-a-inst2"),
			},
			instances: []gce.GceInstance{
				instGen("gce-mig-a-inst1"),
				instGen("gce-mig-a-inst2"),
				instGen("gce-mig-a-inst3"),
				instGen("gce-mig-a-inst4"),
				instGen("gce-mig-a-inst5"),
			},
			expectedResult: &gce.GceInstance{Instance: cloudprovider.Instance{Id: gceRefGen("gce-mig-a-inst3").ToProviderId()}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setting up objects.
			gceCache := gce.NewGceCache()
			err := gceCache.SetMigInstances(migRef, tc.cachedInstances, time.Now())
			assert.NoError(t, err)
			cache := NewGkeCache(gceCache, nodetemplate.NewCache())

			if tc.migPresent {
				cache.RegisterMig(&GkeMig{gceRef: migRef})
			}

			migLister := NewGkeMigLister(cache, irretrievableMigRefreshTime, irretrievableMigRefreshTime, 2)
			gceClient := &mockAutoscalingGceClient{}

			// set migInstancesMinRefreshWaitTime to 0 to force fetching instances that are not in the cache
			migInfoProvider := gce.NewCachingMigInfoProvider(gceCache, migLister, gceClient, projectId, 1, 0*time.Second, false, false)
			m := &gkeManagerImpl{
				cache:           cache,
				migLister:       migLister,
				migInfoProvider: migInfoProvider,
				gkeService: gkeclient.NewAutoscalingGkeClientMock(func() (gkeclient.Cluster, error) {
					return gkeclient.Cluster{}, errors.New("No cluster")
				}, nil, nil),
			}

			if tc.expectedCacheFetch {
				gceCache.InvalidateMigInstances(migRef)
				gceClient.On("FetchMigInstances", migRef).Return(tc.instances, nil).Once()
				gceClient.On("FetchAllMigs", migRef.Zone).Return(
					[]*gce_api_compute.InstanceGroupManager{
						{
							Name:             migRef.Name,
							BaseInstanceName: migRef.Name,
							SelfLink:         fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instanceGroupManagers/%s", migRef.Project, migRef.Zone, migRef.Name),
							Zone:             fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", migRef.Project, migRef.Zone),
							InstanceGroup:    fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instanceGroups/%s", migRef.Project, migRef.Zone, migRef.Name),
							TargetSize:       int64(len(tc.instances)),
							Status: &gce_api_compute.InstanceGroupManagerStatus{
								IsStable: true,
							},
						},
					},
					nil,
				).Once()
			}

			result := m.InstanceByRef(instanceRef)
			assert.Equal(t, tc.expectedResult, result)
			gceClient.AssertExpectations(t)
		})
	}
}

func TestEvaluateCapacityCheckWaitTimeSeconds(t *testing.T) {
	testCases := []struct {
		name                                    string
		mig                                     *GkeMig
		flexTpuValueOverrideExp                 string
		nonFlexTpuValueOverrideExp              string
		defaultGpuValueOverrideExp              string
		capCheckWaitTimeFlexStartExpDisabled    bool
		capCheckWaitTimeMultiHostTpuExpDisabled bool
		wantCapacityCheckWaitTimeSeconds        time.Duration
		wantErrFmt                              string
	}{
		{
			name: "no flex start, no CapacityCheckWaitTimeSeconds",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: false,
				},
			},
			wantCapacityCheckWaitTimeSeconds: time.Duration(0),
			wantErrFmt:                       "CapacityCheckWaitTimeSeconds not supported for non Flex Start mig %v",
		},
		{
			name: "no flex start, single-host TPU, no CapacityCheckWaitTimeSeconds",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    false,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "1x1",
					TpuMultiHost: false,
					MachineType:  "ct6e-standard-1t",
				},
			},
			wantCapacityCheckWaitTimeSeconds: time.Duration(0),
			wantErrFmt:                       "CapacityCheckWaitTimeSeconds not supported for single host TPU mig %v",
		},
		{
			name: "flex start, single-host TPU, no CapacityCheckWaitTimeSeconds",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    true,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "1x1",
					TpuMultiHost: false,
					MachineType:  "ct6e-standard-1t",
				},
			},
			wantCapacityCheckWaitTimeSeconds: time.Duration(0),
			wantErrFmt:                       "CapacityCheckWaitTimeSeconds not supported for single host TPU mig %v",
		},
		{
			name: "no flex start, multi-host TPU, no label - use default",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    false,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
				},
			},
			wantCapacityCheckWaitTimeSeconds: tpuMigMaxNodeProvisionTime - maxNodeProvisionTimeOffset,
		},
		{
			name: "no flex start, multi-host TPU, label valid, custom value",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    false,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "43200", // 12h
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: 12 * time.Hour,
		},
		{
			name:                       "no flex start, multi-host TPU, label valid, default override, custom value",
			flexTpuValueOverrideExp:    "240", // 4min
			nonFlexTpuValueOverrideExp: "180", // 3min
			defaultGpuValueOverrideExp: "120",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    false,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600", // 1h
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: 1 * time.Hour,
		},
		{
			name:                       "no flex start, multi-host TPU, no label, defult override exp - use exp default",
			flexTpuValueOverrideExp:    "240", // 4min
			nonFlexTpuValueOverrideExp: "180", // 3min
			defaultGpuValueOverrideExp: "120",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    false,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
				},
			},
			wantCapacityCheckWaitTimeSeconds: 3 * time.Minute,
		},
		{
			name:                                    "no flex start, multi host tpu- exp disabled - no CapacityCheckWaitTimeSeconds",
			capCheckWaitTimeMultiHostTpuExpDisabled: true,
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    false,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
				},
			},
			wantErrFmt: "CapacityCheckWaitTimeSeconds not supported for non Flex Start mig %v",
		},
		{
			name: "flex start, multi-host TPU, no label - use default",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    true,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
				},
			},
			wantCapacityCheckWaitTimeSeconds: DefaultFlexStartCapacityCheckWaitTime,
		},
		{
			name:                       "flex start, multi-host TPU, no label, defult override exp - use exp default",
			flexTpuValueOverrideExp:    "240", // 4min
			nonFlexTpuValueOverrideExp: "180", // 3min
			defaultGpuValueOverrideExp: "120",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    true,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
				},
			},
			wantCapacityCheckWaitTimeSeconds: 4 * time.Minute,
		},
		{
			name:                       "flex start, multi-host TPU, label valid, custom value",
			flexTpuValueOverrideExp:    "240", // 4min
			nonFlexTpuValueOverrideExp: "180", // 3min
			defaultGpuValueOverrideExp: "120",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart:    true,
					TpuType:      "tpu-v6e-slice",
					TpuTopology:  "2x4",
					TpuMultiHost: true,
					MachineType:  "ct6e-standard-4t",
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "7200", // 2h
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: 2 * time.Hour,
		},
		{
			name: "flex start, no TPU, no label - use default",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
				},
			},
			wantCapacityCheckWaitTimeSeconds: DefaultFlexStartCapacityCheckWaitTime,
		},
		{
			name:                       "flex start, no TPU, no label, defult override exp - use exp default",
			flexTpuValueOverrideExp:    "240", // 4min
			nonFlexTpuValueOverrideExp: "180", // 3min
			defaultGpuValueOverrideExp: "120",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
				},
			},
			wantCapacityCheckWaitTimeSeconds: 2 * time.Minute,
		},
		{
			name: "label valid - use custom from label",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600", // 1h
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: 1 * time.Hour,
		},
		{
			name:                                 "label valid - exp disabled - use default",
			capCheckWaitTimeFlexStartExpDisabled: true,
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "3600", // 1h
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: DefaultFlexStartCapacityCheckWaitTime,
		},
		{
			name: "invalid label below 1 - use default",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "-127",
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: DefaultFlexStartCapacityCheckWaitTime,
		},
		{
			name: "invalid label value type - use default",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "seventeen",
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: DefaultFlexStartCapacityCheckWaitTime,
		},
		{
			name: "invalid label below default - use default",
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					FlexStart: true,
					Labels: map[string]string{
						gkelabels.CapacityCheckWaitTimeSecondsLabel: "17",
					},
				},
			},
			wantCapacityCheckWaitTimeSeconds: DefaultFlexStartCapacityCheckWaitTime,
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{},
				map[string]bool{
					experiments.CapacityCheckWaitTimeSecondsFlexStartEnabledFlag:    !tc.capCheckWaitTimeFlexStartExpDisabled,
					experiments.CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag: !tc.capCheckWaitTimeMultiHostTpuExpDisabled},
				map[string]string{
					experiments.CapacityCheckWaitTimeSecondsDefaultValueGpuFlag:          tc.defaultGpuValueOverrideExp,
					experiments.CapacityCheckWaitTimeSecondsFlexValueMultiHostTpuFlag:    tc.flexTpuValueOverrideExp,
					experiments.CapacityCheckWaitTimeSecondsNonFlexValueMultiHostTpuFlag: tc.nonFlexTpuValueOverrideExp,
				})
			g.optsTracker = optstracking.FakeOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{}, experimentsManager)

			tc.mig.gceRef = gce.GceRef{Name: fmt.Sprintf("mig-%d", i)}

			got, gotErr := g.EvaluateCapacityCheckWaitTimeSeconds(tc.mig)

			var wantErr error
			if tc.wantErrFmt != "" {
				wantErr = fmt.Errorf(tc.wantErrFmt, tc.mig.gceRef)
			}
			assert.Equal(t, wantErr, gotErr)
			assert.Equal(t, tc.wantCapacityCheckWaitTimeSeconds, got)
		})
	}
}

func TestIsEkEdpEnabled(t *testing.T) {
	testCases := []struct {
		name           string
		clusterVersion string
		minVersion     string
		want           bool
	}{
		{
			name:           "Cluster version less than min version - Disabled",
			clusterVersion: "1.25.10-gke.1000",
			minVersion:     "1.26.1-gke.0",
			want:           false, // 1.25.x < 1.26.x
		},
		{
			name:           "Cluster version equals min version - Enabled",
			clusterVersion: "1.26.5-gke.100",
			minVersion:     "1.26.5-gke.100",
			want:           true, // Equal versions: !v.LessThan(vMin) is true
		},
		{
			name:           "Cluster version greater than min version - Enabled",
			clusterVersion: "1.27.1-gke.500",
			minVersion:     "1.26.10-gke.0",
			want:           true, // 1.27.x > 1.26.x
		},
		{
			name:           "Patch version greater - Enabled",
			clusterVersion: "1.26.10-gke.10",
			minVersion:     "1.26.10-gke.5",
			want:           true, // 10 > 5
		},
		{
			name:           "Different version formats (semantic vs GKE) - Enabled",
			clusterVersion: "1.27.0",
			minVersion:     "1.26.0-gke.10",
			want:           true, // 1.27.0 > 1.26.x
		},
		{
			name:           "Invalid clusterVersion string - Disabled",
			clusterVersion: "invalid_version_string",
			minVersion:     "1.26.1-gke.0",
			want:           false, // Fails version.FromString(m.clusterVersion)
		},
		{
			name:           "Invalid minVersion string - Disabled",
			clusterVersion: "1.27.1-gke.500",
			minVersion:     "invalid_min_version",
			want:           false, // Fails version.FromString(minGkeVersion)
		},
		{
			name:           "Both invalid - Disabled",
			clusterVersion: "a.b.c",
			minVersion:     "x.y.z",
			want:           false, // Fails version.FromString(m.clusterVersion) first
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil, tc.clusterVersion)

			addDefaultListMigsMocks(server, g.cache)
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{},
				map[string]bool{},
				map[string]string{
					experiments.EnableEkEdpMinGKEVersionFlag: tc.minVersion,
				})
			g.optsTracker = optstracking.FakeOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{}, experimentsManager)
			g.refreshEkEdpEnabled()
			got := g.IsEkEdpEnabled()

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRefreshShortLivedUpgradeInProgress(t *testing.T) {
	testCases := []struct {
		name                            string
		flexStart                       bool
		queuedProvisioning              bool
		instanceTemplate                string
		instances                       []gce.GceInstance
		wantShortLivedUpgradeInProgress bool
	}{
		{
			name:               "no queued provisioning nor flex start - no upgrade",
			flexStart:          false,
			queuedProvisioning: false,
			instanceTemplate:   "instance-template1",
			instances: []gce.GceInstance{
				{
					NumericId:            17,
					InstanceTemplateName: "instance-template1",
				},
				{
					NumericId:            127,
					InstanceTemplateName: "instance-template1",
				},
			},
			wantShortLivedUpgradeInProgress: false,
		},
		{
			name:                            "queued provisioning no instances - no upgrade",
			queuedProvisioning:              true,
			instanceTemplate:                "instance-template1",
			instances:                       []gce.GceInstance{},
			wantShortLivedUpgradeInProgress: false,
		},
		{
			name:               "queued provisioning, instances have the same IT - no upgrade",
			queuedProvisioning: true,
			instanceTemplate:   "instance-template1",
			instances: []gce.GceInstance{
				{
					NumericId:            17,
					InstanceTemplateName: "instance-template1",
				},
				{
					NumericId:            127,
					InstanceTemplateName: "instance-template1",
				},
				{
					NumericId:            9,
					InstanceTemplateName: "instance-template1",
				},
			},
			wantShortLivedUpgradeInProgress: false,
		},
		{
			name:               "queued provisioning, instances have empty IT (enqueued only) - no upgrade",
			queuedProvisioning: true,
			instanceTemplate:   "instance-template1",
			instances: []gce.GceInstance{
				{
					NumericId:            17,
					InstanceTemplateName: "",
				},
				{
					NumericId:            127,
					InstanceTemplateName: "",
				},
				{
					NumericId:            9,
					InstanceTemplateName: "",
				},
			},
			wantShortLivedUpgradeInProgress: false,
		},
		{
			name:               "queued provisioning, instances have the same IT or no IT (enqueued) - no upgrade",
			queuedProvisioning: true,
			instanceTemplate:   "instance-template1",
			instances: []gce.GceInstance{
				{
					NumericId:            17,
					InstanceTemplateName: "instance-template1",
				},
				{
					NumericId:            127,
					InstanceTemplateName: "",
				},
				{
					NumericId:            9,
					InstanceTemplateName: "instance-template1",
				},
			},
			wantShortLivedUpgradeInProgress: false,
		},
		{
			name:               "queued provisioning, instances have different IT - upgrade in progress",
			queuedProvisioning: true,
			instanceTemplate:   "instance-template1",
			instances: []gce.GceInstance{
				{
					NumericId:            17,
					InstanceTemplateName: "instance-template1",
				},
				{
					NumericId:            127,
					InstanceTemplateName: "",
				},
				{
					NumericId:            128,
					InstanceTemplateName: "",
				},
				{
					NumericId:            9,
					InstanceTemplateName: "instance-template2",
				},
				{
					NumericId:            5,
					InstanceTemplateName: "instance-template1",
				},
			},
			wantShortLivedUpgradeInProgress: true,
		},
		{
			name:               "flex start, no queued provisioning, instances have different IT - upgrade in progress",
			flexStart:          true,
			queuedProvisioning: false,
			instanceTemplate:   "instance-template1",
			instances: []gce.GceInstance{
				{
					NumericId:            17,
					InstanceTemplateName: "instance-template1",
				},
				{
					NumericId:            127,
					InstanceTemplateName: "",
				},
				{
					NumericId:            128,
					InstanceTemplateName: "",
				},
				{
					NumericId:            9,
					InstanceTemplateName: "instance-template2",
				},
				{
					NumericId:            5,
					InstanceTemplateName: "instance-template1",
				},
			},
			wantShortLivedUpgradeInProgress: true,
		},
	}

	for i, tc := range testCases {
		server := NewHttpServerMock()
		g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

		addDefaultListMigsMocks(server, g.cache)

		mig := &GkeMig{
			gceRef: gce.GceRef{
				Project: "project",
				Zone:    "zone",
				Name:    fmt.Sprintf("mig-name-%d", i),
			},
			queuedProvisioning: tc.queuedProvisioning,
			spec: &gkeclient.NodePoolSpec{
				FlexStart: tc.flexStart,
			},
		}
		g.migInfoProvider = &migInfoProviderStub{
			queuedProvisioning: map[gce.GceRef]bool{
				mig.GceRef(): tc.queuedProvisioning,
			},
			instanceTemplate: map[gce.GceRef]gce.InstanceTemplateName{
				mig.GceRef(): {Name: tc.instanceTemplate, Regional: false},
			},
			instances: map[gce.GceRef][]gce.GceInstance{
				mig.GceRef(): tc.instances,
			},
		}
		g.migLister.cache.gkeMigs = map[gce.GceRef]*GkeMig{mig.GceRef(): mig}

		g.refreshShortLivedUpgradeInProgress()

		if tc.wantShortLivedUpgradeInProgress != g.migLister.GetGkeMigs()[0].shortLivedUpgradeInProgress {
			t.Fatalf("mig %q got unexpected shortLivedUpgradeInProgress, want: %v, got: %v", mig.gceRef.Name, tc.wantShortLivedUpgradeInProgress, g.migLister.GetGkeMigs()[0].shortLivedUpgradeInProgress)
		}
	}
}

func TestGetDeploymentType(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
		WithFetchZones(func(region string) ([]string, error) { return []string{zoneB}, nil })

	reservations := []*gce_api.Reservation{
		{
			Id:             1234,
			Name:           "dense-res",
			DeploymentType: "DENSE",
			SelfLink:       "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/reservations/dense-res",
		},
		{
			Id:             12345,
			Name:           "unspecified-res",
			DeploymentType: "UNSPECIFIED",
			SelfLink:       "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/reservations/unspecified-res",
		},
	}
	reservationsPuller, err := gceclient.NewReservationsPuller(mGceClient, nil, experiments.NewMockManager(), projectId, true, zoneB)
	assert.NoError(t, err)
	reservationsPuller.SetReservations(reservations)

	g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)

	testCases := []struct {
		name               string
		gceRef             gce.GceRef
		spec               *gkeclient.NodePoolSpec
		reservationsPuller *gceclient.ReservationsPuller
		want               DeploymentTypeEnum
	}{
		{
			name:               "nil reservations puller",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Key: "dense-res"}},
			reservationsPuller: nil,
			want:               DeploymentTypeNone,
		},
		{
			name:               "nil node pool spec",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               nil,
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeNone,
		},
		{
			name:               "nil reservation affinity",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeNone,
		},
		{
			name:               "no_reservation reservation affinity",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{ConsumeReservationType: gkeclient.ReservationAffinityNone}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeNone,
		},
		{
			name:               "reservation not found",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"not-found"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeUnspecified,
		},
		{
			name:               "dense reservation",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"dense-res"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeDense,
		},
		{
			name:               "unspecified reservation",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"unspecified-res"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeUnspecified,
		},
		{
			name:               "unspecified_reservation_with_full_path",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"projects/other-project/reservations/uspecified-reservation"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeUnspecified,
		},
		{
			name:               "dense_reservation_with_block",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"dense-res/reservationBlocks/block-1"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeDense,
		},
		{
			name:               "dense_reservation_with_block_full_path",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"/projects/other-project/dense-res/reservationBlocks/block-1"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeDense,
		},
		{
			name:               "dense_reservation_with_block_and_subblock",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"dense-res/reservationBlocks/block-1/reservationSubBlocks/res-sub-block"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeDense,
		},
		{
			name:               "dense_reservation_with_block_and_subblock_full_path",
			gceRef:             gce.GceRef{Project: projectId, Zone: zoneB, Name: "mig"},
			spec:               &gkeclient.NodePoolSpec{ReservationAffinity: &gke_api_beta.ReservationAffinity{Values: []string{"/projects/other-project/dense-res/reservationBlocks/block-1/reservationSubBlocks/res-sub-block"}}},
			reservationsPuller: reservationsPuller,
			want:               DeploymentTypeDense,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g.reservationsPuller = tc.reservationsPuller
			got := g.GetDeploymentType(tc.gceRef, tc.spec)
			if got != tc.want {
				t.Errorf("GetDeploymentType(%v, %v) = %v, want %v", tc.gceRef, tc.spec, got, tc.want)
			}
		})
	}
}

func TestAddTpuCapacity(t *testing.T) {
	testCases := []struct {
		name        string
		nodeLabels  map[string]string
		machineType string
		wantError   bool
		errMsg      string
	}{
		{
			name: "TPU type not found",
			nodeLabels: map[string]string{
				gkelabels.GPULabel: "gpu-label",
			},
			machineType: "ct6e-standard-4t",
			wantError:   false,
			errMsg:      "",
		},
		{
			name: "Supported machine type",
			nodeLabels: map[string]string{
				gkelabels.TPULabel: "accelerator",
			},
			machineType: "ct6e-standard-4t",
			wantError:   false,
			errMsg:      "",
		},
		{
			name: "Unsupported machine type",
			nodeLabels: map[string]string{
				gkelabels.TPULabel: "accelerator",
			},
			machineType: "fake-machine-type",
			wantError:   true,
			errMsg:      "Can't get TPU count for machine type fake-machine-type",
		},
	}

	nodeName := "test-node"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   nodeName,
					Labels: tc.nodeLabels,
				},
				Spec: apiv1.NodeSpec{
					ProviderID: nodeName,
				},
				Status: apiv1.NodeStatus{
					Capacity: apiv1.ResourceList{
						apiv1.ResourcePods: *resource.NewQuantity(100, resource.DecimalSI),
					},
					Allocatable: apiv1.ResourceList{
						apiv1.ResourcePods: *resource.NewQuantity(100, resource.DecimalSI),
					},
				},
			}

			m := newTestGkeManager(t, "", true, true, true, nil, false, nil)
			newNode, err := m.addTpuCapacity(node, tc.machineType)
			if tc.wantError {
				assert.Error(t, err)
				assert.Equal(t, tc.errMsg, err.Error())
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, newNode)
			}
		})
	}
}

func TestAddHugepagesCapacity(t *testing.T) {
	for desc, tc := range map[string]struct {
		mig             *GkeMig
		wantAllocatable apiv1.ResourceList
		wantCapacity    apiv1.ResourceList
	}{
		"no spec": {
			mig: &GkeMig{},
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
		},
		"no linux node config": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{},
			},
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
		},
		"no hugepages": {
			mig: &GkeMig{
				spec: &gkeclient.NodePoolSpec{
					LinuxNodeConfig: &gkeclient.LinuxNodeConfig{},
				},
			},
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
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
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:          *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory:       *resource.NewQuantity(10*units.GiB-2*100*units.MiB, resource.DecimalSI),
				apiv1.ResourcePods:         *resource.NewQuantity(100, resource.DecimalSI),
				HugepageSize2mResourceName: *resource.NewQuantity(2*100*units.MiB, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:          *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory:       *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:         *resource.NewQuantity(100, resource.DecimalSI),
				HugepageSize2mResourceName: *resource.NewQuantity(2*100*units.MiB, resource.DecimalSI),
			},
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
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:          *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory:       *resource.NewQuantity(0, resource.DecimalSI),
				apiv1.ResourcePods:         *resource.NewQuantity(100, resource.DecimalSI),
				HugepageSize1gResourceName: *resource.NewQuantity(100*units.GiB, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:          *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory:       *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:         *resource.NewQuantity(100, resource.DecimalSI),
				HugepageSize1gResourceName: *resource.NewQuantity(100*units.GiB, resource.DecimalSI),
			},
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
			wantAllocatable: apiv1.ResourceList{
				apiv1.ResourceCPU:          *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory:       *resource.NewQuantity(10*units.GiB-1*units.GiB-2*2*units.MiB, resource.DecimalSI),
				apiv1.ResourcePods:         *resource.NewQuantity(100, resource.DecimalSI),
				HugepageSize1gResourceName: *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
				HugepageSize2mResourceName: *resource.NewQuantity(2*2*units.MiB, resource.DecimalSI),
			},
			wantCapacity: apiv1.ResourceList{
				apiv1.ResourceCPU:          *resource.NewMilliQuantity(10, resource.DecimalSI),
				apiv1.ResourceMemory:       *resource.NewQuantity(10*units.GiB, resource.DecimalSI),
				apiv1.ResourcePods:         *resource.NewQuantity(100, resource.DecimalSI),
				HugepageSize1gResourceName: *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
				HugepageSize2mResourceName: *resource.NewQuantity(2*2*units.MiB, resource.DecimalSI),
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			node := BuildTestNode("node", 10, 10*1024*1024*1024)
			node = addHugepagesCapacity(node, tc.mig)
			assert.Equal(t, node.Status.Capacity, tc.wantCapacity)
			assert.Equal(t, node.Status.Allocatable, tc.wantAllocatable)
		})
	}
}

func TestGetNodesScaleDownAllowedFromCache(t *testing.T) {
	tests := []struct {
		name                   string
		nodeNames              []string
		cachedScaleDownAllowed map[string]bool
		wantScaleDownAllowed   map[string]bool
	}{
		{
			name: "no node names",
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{},
		},
		{
			name:                 "cache is empty",
			nodeNames:            []string{"node-1", "node-2"},
			wantScaleDownAllowed: map[string]bool{},
		},
		{
			name:      "node names are cached",
			nodeNames: []string{"node-1", "node-2"},
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
		},
		{
			name:      "node names are not cached",
			nodeNames: []string{"node-3", "node-4"},
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{},
		},
		{
			name:      "node names are partly cached",
			nodeNames: []string{"node-2", "node-3"},
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{
				"node-2": false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()

			g := newTestGkeManager(t, server.URL, true, true, true, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			g.cache.nodesScaleDownAllowed = tc.cachedScaleDownAllowed
			assert.Equal(t, tc.wantScaleDownAllowed, g.GetNodesScaleDownAllowedFromCache(tc.nodeNames))
		})
	}
}

func TestUpdateNodesScaleDownAllowedCache(t *testing.T) {
	tests := []struct {
		name                   string
		newScaleDownAllowed    map[string]bool
		cachedScaleDownAllowed map[string]bool
		wantScaleDownAllowed   map[string]bool
	}{
		{
			name: "no newScaleDownAllowed",
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
		},
		{
			name: "cache is empty",
			newScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
		},
		{
			name: "newScaleDownAllowed updates existing",
			newScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": true,
			},
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": true,
			},
		},
		{
			name: "newScaleDownAllowed addes new nodes",
			newScaleDownAllowed: map[string]bool{
				"node-3": true,
				"node-4": false,
			},
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
				"node-3": true,
				"node-4": false,
			},
		},
		{
			name: "newScaleDownAllowed partly updates existing partly addes new nodes",
			newScaleDownAllowed: map[string]bool{
				"node-2": true,
				"node-3": false,
			},
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
			wantScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": true,
				"node-3": false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()

			g := newTestGkeManager(t, server.URL, true, true, true, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			g.cache.nodesScaleDownAllowed = tc.cachedScaleDownAllowed
			g.UpdateNodesScaleDownAllowedCache(tc.newScaleDownAllowed)
			assert.Equal(t, tc.wantScaleDownAllowed, g.cache.nodesScaleDownAllowed)
		})
	}
}

func TestInvalidateNodesScaleDownAllowedCache(t *testing.T) {
	tests := []struct {
		name                   string
		cachedScaleDownAllowed map[string]bool
	}{
		{
			name: "cache is empty",
		},
		{
			name: "cache is populated",
			cachedScaleDownAllowed: map[string]bool{
				"node-1": true,
				"node-2": false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()

			g := newTestGkeManager(t, server.URL, true, true, true, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			g.cache.nodesScaleDownAllowed = tc.cachedScaleDownAllowed
			g.InvalidateNodesScaleDownAllowedCache()
			assert.Equal(t, map[string]bool{}, g.cache.nodesScaleDownAllowed)
		})
	}
}

func TestGetMaxNodeProvisioningTimeOverride(t *testing.T) {
	tests := []struct {
		name                string
		experimentFlags     map[string]string
		spec                *gkeclient.NodePoolSpec
		queued              bool
		expectedValue       time.Duration
		expectedHasOverride bool
	}{
		{
			name:                "no override present, fallback to default",
			spec:                NewTestMigSpecBuilder().SetTpuType("v6e-lite-device").SetTpuMultiHost(false).SpecBuild(),
			expectedValue:       tpuMigMaxNodeProvisionTime,
			expectedHasOverride: true,
		},
		{
			name:                "no override present, fallback to default (flex)",
			spec:                NewTestMigSpecBuilder().SetTpuType("v6e-lite-device").SetTpuMultiHost(false).SetFlexStart(true).SpecBuild(),
			expectedValue:       30 * time.Minute,
			expectedHasOverride: true,
		},
		{
			name:                "override present, use it",
			experimentFlags:     map[string]string{experiments.NodeProvisionTimeSingleHostTPU: "1234"},
			spec:                NewTestMigSpecBuilder().SetTpuType("v6e-lite-device").SetTpuMultiHost(false).SpecBuild(),
			expectedValue:       1234 * time.Second,
			expectedHasOverride: true,
		},
		{
			name:                "override present, use it (flex)",
			experimentFlags:     map[string]string{experiments.NodeProvisionTimeSingleHostTPU: "1234"},
			spec:                NewTestMigSpecBuilder().SetTpuType("v6e-lite-device").SetTpuMultiHost(false).SetFlexStart(true).SpecBuild(),
			expectedValue:       1234 * time.Second,
			expectedHasOverride: true,
		},
		{
			name:            "do not override for queued provisioning",
			experimentFlags: map[string]string{experiments.NodeProvisionTimeSingleHostTPU: "1234"},
			spec:            NewTestMigSpecBuilder().SetTpuType("v6e-lite-device").SetTpuMultiHost(false).SpecBuild(),
			queued:          true,
		},
		{
			name:            "do not override for multihosts",
			experimentFlags: map[string]string{experiments.NodeProvisionTimeSingleHostTPU: "1234"},
			spec:            NewTestMigSpecBuilder().SetTpuType("v6e-lite-device").SetTpuMultiHost(true).SpecBuild(),
		},
		{
			name:            "do not override for other migs",
			experimentFlags: map[string]string{experiments.NodeProvisionTimeSingleHostTPU: "1234"},
			spec:            NewTestMigSpecBuilder().SpecBuild(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()

			g := newTestGkeManager(t, server.URL, true, true, true, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, tc.experimentFlags)
			g.optsTracker = optstracking.FakeOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{}, experimentsManager)

			mig := NewTestGkeMigBuilder().SetSpec(tc.spec).SetQueuedProvisioning(tc.queued).Build()
			overrideValue, hasOverride := g.GetMaxNodeProvisioningTimeOverride(mig)
			assert.Equal(t, tc.expectedHasOverride, hasOverride)
			if hasOverride {
				assert.Equal(t, tc.expectedValue, overrideValue)
			}
		})
	}
}

func TestGetClusterNetwork(t *testing.T) {
	mockFetchNetwork := func(projectId, name string) (*gce_api.Network, error) {
		return &gce_api.Network{
			Name:           "default",
			SelfLink:       "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/default",
			SelfLinkWithId: "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/12345",
		}, nil
	}
	gceClientMock := gceclient.BuildAutoscalingInternalGceClientMock().WithFetchNetwork(mockFetchNetwork)

	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, true, false, false, nil, false, nil)

	addDefaultListMigsMocks(server, g.cache)

	g.networkPath = "projects/test-project/global/networks/default"
	g.gceService = gceClientMock

	assert.Nil(t, g.network)
	// First call, should fetch from API
	network, err := g.GetClusterNetwork()
	assert.NoError(t, err)
	assert.NotNil(t, g.network)
	assert.Equal(t, "default", network.Name)
	assert.Equal(t, "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/default", network.SelfLink)
	assert.Equal(t, "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/12345", network.SelfLinkWithId)

	// Second call, should use cache
	network2, err := g.GetClusterNetwork()
	assert.NoError(t, err)
	assert.Same(t, network, network2, "Expected network object to be cached")
}

func TestSuspendInstances(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

	addDefaultListMigsMocks(server, g.cache)

	migName := "mig-1"
	zone := zoneB
	opName := "operation-suspend-1"

	// SuspendInstances URL
	suspendUrl := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/suspendInstances", projectId, zone, migName)

	// Operation URL (Wait)
	opWaitUrl := fmt.Sprintf("/projects/%s/zones/%s/operations/%s/wait", projectId, zone, opName)

	// Operation self link (standard)
	opSelfLink := fmt.Sprintf("http://%s/projects/%s/zones/%s/operations/%s", server.Listener.Addr().String(), projectId, zone, opName)
	migSelfLink := fmt.Sprintf("http://%s/projects/%s/zones/%s/instanceGroupManagers/%s", server.Listener.Addr().String(), projectId, zone, migName)

	suspendResponse := fmt.Sprintf(`{
        "name": "%s",
        "zone": "https://www.googleapis.com/compute/v1/projects/%s/zones/%s",
        "operationType": "SUSPEND_INSTANCES",
        "status": "RUNNING",
        "selfLink": "%s",
        "targetLink": "%s"
    }`, opName, projectId, zone, opSelfLink, migSelfLink)

	opResponse := fmt.Sprintf(`{
        "name": "%s",
        "zone": "https://www.googleapis.com/compute/v1/projects/%s/zones/%s",
        "operationType": "SUSPEND_INSTANCES",
        "status": "DONE",
        "selfLink": "%s",
        "targetLink": "%s"
    }`, opName, projectId, zone, opSelfLink, migSelfLink)

	server.On("handle", suspendUrl).Return(suspendResponse).Once()
	server.On("handle", opWaitUrl).Return(opResponse).Once()

	listUrl := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/listManagedInstances", projectId, zone, migName)
	listResponse := fmt.Sprintf(`{
        "managedInstances": [
            {
                "name": "inst-1",
                "instanceStatus": "SUSPENDED"
            }
        ]
    }`)
	server.On("handle", listUrl).Return(listResponse).Once()

	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zone,
			Name:    migName,
		},
		gkeManager: g,
	}

	instances := []gce.GceRef{
		{Project: projectId, Zone: zone, Name: "inst-1"},
	}

	err := g.SuspendInstances(mig.gceRef, instances, true)
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, server)
}

func TestResumeInstances(t *testing.T) {
	server := NewHttpServerMock()
	defer server.Close()
	g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

	addDefaultListMigsMocks(server, g.cache)

	migName := "mig-1"
	zone := zoneB
	opName := "operation-resume-1"

	// ResumeInstances URL
	resumeUrl := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/resumeInstances", projectId, zone, migName)

	// Operation URL (Wait)
	opWaitUrl := fmt.Sprintf("/projects/%s/zones/%s/operations/%s/wait", projectId, zone, opName)

	// Operation self link (standard)
	opSelfLink := fmt.Sprintf("http://%s/projects/%s/zones/%s/operations/%s", server.Listener.Addr().String(), projectId, zone, opName)
	migSelfLink := fmt.Sprintf("http://%s/projects/%s/zones/%s/instanceGroupManagers/%s", server.Listener.Addr().String(), projectId, zone, migName)

	resumeResponse := fmt.Sprintf(`{
        "name": "%s",
        "zone": "https://www.googleapis.com/compute/v1/projects/%s/zones/%s",
        "operationType": "RESUME_INSTANCES",
        "status": "RUNNING",
        "selfLink": "%s",
        "targetLink": "%s"
    }`, opName, projectId, zone, opSelfLink, migSelfLink)

	opResponse := fmt.Sprintf(`{
        "name": "%s",
        "zone": "https://www.googleapis.com/compute/v1/projects/%s/zones/%s",
        "operationType": "RESUME_INSTANCES",
        "status": "DONE",
        "selfLink": "%s",
        "targetLink": "%s"
    }`, opName, projectId, zone, opSelfLink, migSelfLink)

	server.On("handle", resumeUrl).Return(resumeResponse).Once()
	server.On("handle", opWaitUrl).Return(opResponse).Once()

	listUrl := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/listManagedInstances", projectId, zone, migName)
	listResponse := fmt.Sprintf(`{
        "managedInstances": [
            {
                "name": "inst-1",
                "instanceStatus": "RUNNING"
            }
        ]
    }`)
	server.On("handle", listUrl).Return(listResponse).Once()

	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zone,
			Name:    migName,
		},
		gkeManager: g,
	}

	instances := []gce.GceRef{
		{Project: projectId, Zone: zone, Name: "inst-1"},
	}

	err := g.ResumeInstances(mig.gceRef, instances)
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, server)
}

type AutoscalingGkeClientMock struct {
	mock.Mock
}

func (m *AutoscalingGkeClientMock) GetCluster() (gkeclient.Cluster, error) {
	args := m.Called()
	return args.Get(0).(gkeclient.Cluster), args.Error(1)
}

func (m *AutoscalingGkeClientMock) DeleteNodePool(pool string) error {
	args := m.Called(pool)
	return args.Error(0)
}

func (m *AutoscalingGkeClientMock) CreateNodePool(name string, spec *gkeclient.NodePoolSpec) error {
	args := m.Called(name, spec)
	return args.Error(0)
}

func (m *AutoscalingGkeClientMock) UpdateNodePoolLabels(name string, labels map[string]string) error {
	args := m.Called(name, labels)
	return args.Error(0)
}

type mockGkeMetrics struct {
	mock.Mock
}

func (m *mockGkeMetrics) UpdateCSNEnabled(enabled bool) {
	m.Called(enabled)
}

func (m *mockGkeMetrics) UpdateNapEnabled(enabled bool) {
	m.Called(enabled)
}

func TestGetStandardZonesInRegion(t *testing.T) {
	testCases := []struct {
		name            string
		region          string
		preCached       []string
		mockResponse    []string
		mockErr         error
		want            []string
		wantErr         bool
		wantClientCalls int
	}{
		{
			name:            "cache_miss",
			region:          "us-central1",
			mockResponse:    []string{"us-central1-a", "us-central1-b"},
			want:            []string{"us-central1-a", "us-central1-b"},
			wantClientCalls: 1,
		},
		{
			name:            "cache_hit",
			region:          "us-central1",
			preCached:       []string{"us-central1-c", "us-central1-d"},
			mockResponse:    []string{"gibberish"},
			want:            []string{"us-central1-c", "us-central1-d"},
			wantClientCalls: 0,
		},
		{
			name:            "gce_error_propagated",
			region:          "us-east1",
			mockErr:         errors.New("network error"),
			wantErr:         true,
			wantClientCalls: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var callCount int
			mockFetchStandardZones := func(region string) ([]string, error) {
				callCount++
				if region != tc.region {
					return nil, fmt.Errorf("unexpected region: %s", region)
				}
				return tc.mockResponse, tc.mockErr
			}

			gceClientMock := gceclient.BuildAutoscalingInternalGceClientMock().WithFetchStandardZones(mockFetchStandardZones)
			g := newTestGkeManager(t, "", true, false, false, nil, false, nil)
			g.gceService = gceClientMock

			if tc.preCached != nil {
				g.cache.SetStandardZonesInRegion(tc.region, tc.preCached)
			}

			got, err := g.GetStandardZonesInRegion(tc.region)

			if (err != nil) != tc.wantErr || tc.wantErr == (err == nil) {
				t.Errorf("GetStandardZonesInRegion() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantClientCalls, callCount)
		})
	}
}

func TestGetAIZonesInRegion(t *testing.T) {
	mockFetchAIZones := func(region string) ([]string, error) {
		if region == "us-central1" {
			return []string{"us-central1-ai1", "us-central1-ai2"}, nil
		}
		return []string{"zone-a", "zone-b"}, nil
	}
	gceClientMock := gceclient.BuildAutoscalingInternalGceClientMock().WithFetchAIZones(mockFetchAIZones)
	g := newTestGkeManager(t, "", true, false, false, nil, false, nil)

	g.gceService = gceClientMock

	// First call, should fetch from API
	aiZones, err := g.GetAIZonesInRegion("us-central1")
	assert.NoError(t, err)
	assert.Equal(t, []string{"us-central1-ai1", "us-central1-ai2"}, aiZones)
}

func TestTrimLocationsForMachineConfig(t *testing.T) {
	const (
		AMD                = "amd"
		INTEL              = "intel"
		NVIDIA             = "nvidia"
		PD_STANDARD        = "pd-standard"
		PD_BALANCED        = "pd-balanced"
		PD_SSD             = "pd-ssd"
		HYPERDISK_BALANCED = "hyperdisk-balanced"
	)
	server := NewHttpServerMock()
	defer server.Close()

	availableCpuPlatforms := map[string][]string{
		zoneA: {AMD, INTEL},
		zoneB: {AMD},
		zoneC: {AMD, NVIDIA},
	}

	availableDiskTypes := map[string][]string{
		zoneA: {PD_STANDARD, PD_BALANCED, PD_SSD},
		zoneB: {PD_BALANCED},
		zoneC: {PD_BALANCED, HYPERDISK_BALANCED},
	}

	testCases := []struct {
		scenario          string
		machineType       string
		diskType          string
		minCpuPlatform    string
		accelerator       *gke_api_beta.AcceleratorConfig
		locations         []string
		expectedLocations []string
	}{
		{
			scenario:          "all_locations_available",
			machineType:       machineTypeA,
			diskType:          PD_BALANCED,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA, zoneB, zoneC},
		},
		{
			scenario:          "filtered_by_machine_type",
			machineType:       machineTypeB,
			diskType:          PD_BALANCED,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA, zoneC},
		},
		{
			scenario:          "filtered_by_disk_type",
			machineType:       machineTypeA,
			diskType:          PD_SSD,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA},
		},
		{
			scenario:          "filtered_by_cpu_platform",
			machineType:       machineTypeA,
			diskType:          PD_BALANCED,
			minCpuPlatform:    NVIDIA,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneC},
		},
		{
			scenario:          "filtered_by_accelerator",
			machineType:       machineTypeA,
			diskType:          PD_BALANCED,
			accelerator:       &gke_api_beta.AcceleratorConfig{AcceleratorType: "nvidia-tesla-k80", AcceleratorCount: 1},
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA, zoneC},
		},
		{
			scenario:          "filtered_by_multiple_factors",
			machineType:       machineTypeB,
			diskType:          PD_STANDARD,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{zoneA},
		},
		{
			scenario:          "location_not_included",
			machineType:       machineTypeB,
			diskType:          PD_STANDARD,
			locations:         []string{zoneB, zoneC},
			expectedLocations: []string{},
		},
		{
			scenario:          "no_locations_available",
			machineType:       machineTypeB,
			diskType:          PD_STANDARD,
			minCpuPlatform:    NVIDIA,
			locations:         []string{zoneA, zoneB, zoneC},
			expectedLocations: []string{},
		},
		{
			scenario:          "location_does_not_exist",
			machineType:       machineTypeA,
			diskType:          PD_BALANCED,
			locations:         []string{"zoneD"},
			expectedLocations: []string{},
		},
		{
			scenario:          "empty_locations",
			machineType:       machineTypeA,
			locations:         []string{},
			expectedLocations: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.scenario, func(t *testing.T) {
			g := newTestGkeManager(t, server.URL, napEnabled, true, false, nil, false, nil)
			g.MachineConfigValidator = &testMachineConfigValidator{}
			g.availableDiskTypesProvider = NewStaticAvailableDiskTypesProvider(availableDiskTypes)
			g.availableCpuPlatformsProvider = NewStaticAvailableCpuPlatformsProvider(availableCpuPlatforms)

			trimmedLocations := g.TrimLocationsForMachineConfig(tc.locations, tc.machineType, tc.accelerator, tc.minCpuPlatform, tc.diskType)
			assert.Equal(t, tc.expectedLocations, trimmedLocations)
		})
	}
}

func TestRefreshGkeResourcesCSNMetrics(t *testing.T) {
	testCases := []struct {
		desc      string
		csnStatus internalopts.CSNStatus
		expected  bool
	}{
		{
			desc:      "CSN enabled",
			csnStatus: internalopts.CSNEnabled,
			expected:  true,
		},
		{
			desc:      "CSN disabled",
			csnStatus: internalopts.CSNDisabled,
			expected:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			server := NewHttpServerMock()
			defer server.Close()

			mockMetrics := &mockGkeMetrics{}
			mockMetrics.On("UpdateCSNEnabled", tc.expected).Return()
			mockMetrics.On("UpdateNapEnabled", mock.Anything).Return()

			g := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)

			addDefaultListMigsMocks(server, g.cache)
			g.gkeMetrics = mockMetrics
			g.optsTracker = optstracking.NewOptionsTracker(internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					CSNCAFlag: tc.csnStatus,
				},
			}, experiments.NewMockManager())

			getClusterResponse := fmt.Sprintf(getClusterResponseTemplate, allNodePools1, napDisabled, false, "")
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster1").Return(getClusterResponse).Maybe()
			server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers").Return(buildListInstanceGroupManagersResponse(
				buildListInstanceGroupManagersResponsePart("gke-cluster-1-default-pool", zoneB, 3),
			)).Maybe()
			server.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool").Return(getInstanceGroupManager(zoneB)).Maybe()
			server.On("handle", "/projects/project1/global/instanceTemplates/gke-blah-default-pool-67b773a0").Return(instanceTemplate).Maybe()

			err := g.refreshGkeResources()
			assert.NoError(t, err)

			mockMetrics.AssertExpectations(t)
		})
	}
}

func TestAddSupportedDiskTypeLabelsToNode(t *testing.T) {
	tests := []struct {
		name           string
		machineType    string
		isConfidential bool
		expectedLabels map[string]string
	}{
		{
			name:           "a4-highgpu-8g non-confidential",
			machineType:    "a4-highgpu-8g",
			isConfidential: false,
			expectedLabels: map[string]string{
				"disk-type.gke.io/hyperdisk-balanced": "true",
				"disk-type.gke.io/hyperdisk-extreme":  "true",
				"disk-type.gke.io/hyperdisk-ml":       "true",
			},
		},
		{
			name:           "n2-standard-96 confidential",
			machineType:    "n2-standard-96",
			isConfidential: true,
			expectedLabels: map[string]string{
				"disk-type.gke.io/pd-balanced":                          "true",
				"disk-type.gke.io/pd-standard":                          "true",
				"disk-type.gke.io/pd-ssd":                               "true",
				"disk-type.gke.io/hyperdisk-balanced":                   "true",
				"disk-type.gke.io/hyperdisk-balanced-high-availability": "true",
				"disk-type.gke.io/hyperdisk-ml":                         "true",
				"disk-type.gke.io/pd-extreme":                           "true",
			},
		},
	}

	g := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	arch := gce.DefaultArch

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			}
			mig := &GkeMig{
				gkeManager: g,
				spec: &gkeclient.NodePoolSpec{
					MachineType:        tc.machineType,
					SystemArchitecture: &arch,
				},
			}
			if tc.isConfidential {
				mig.nodeConfig = &NodeConfig{
					IsConfidentialNode: true,
				}
			}

			err := g.addSupportedDiskTypeLabelsToNode(node, mig)
			assert.NoError(t, err)

			if diff := cmp.Diff(tc.expectedLabels, node.Labels); diff != "" {
				t.Errorf("Labels mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
