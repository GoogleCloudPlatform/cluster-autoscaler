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
	"testing"

	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/utils/ptr"
	gceinternal "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce"
	fakek8s "sigs.k8s.io/cluster-autoscaler/pkg/utils/fake"
)

func TestCreateInstancesWithRecommendation(t *testing.T) {
	migRef := gceinternal.GceRef{
		Project: "test-project",
		Zone:    "us-central1-a",
		Name:    "test-mig",
	}
	template := &gcev1.InstanceTemplate{
		Name: "test-template",
		Properties: &gcev1.InstanceProperties{
			MachineType: "e2-standard-4",
			Disks: []*gcev1.AttachedDisk{
				{
					InitializeParams: &gcev1.AttachedDiskInitializeParams{
						DiskSizeGb: 100,
					},
				},
			},
			Metadata: &gcev1.Metadata{
				Items: []*gcev1.MetadataItems{
					{
						Key:   "kube-env",
						Value: ptr.To("AUTOSCALER_ENV_VARS: os=linux;os_distribution=cos;arch=amd64\n"),
					},
				},
			},
		},
	}
	mig := &gcev1.InstanceGroupManager{
		Name:             "test-mig",
		Zone:             "us-central1-a",
		InstanceTemplate: "test-template",
	}

	kubeClient := k8sfake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	k8s := fakek8s.NewKubernetes(kubeClient, informerFactory)

	client := NewGceClient(t, k8s).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		WithDefaultZones().
		WithDefaultMachineTypes().
		WithTemplates(template).
		WithMigs(mig)

	names, err := client.CreateInstancesWithRecommendation(migRef, "test-template", 2, nil, "1/rec1//spec1")
	assert.NoError(t, err)
	assert.Len(t, names, 2)

	recs := client.GetRecordedRecommendations(migRef)
	assert.Equal(t, []string{"1/rec1//spec1"}, recs)
}
