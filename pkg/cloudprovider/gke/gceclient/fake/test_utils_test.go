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

	gcev1 "google.golang.org/api/compute/v1"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	gceinternal "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/utils/ptr"
	fakek8s "sigs.k8s.io/cluster-autoscaler/pkg/utils/fake"
)

const (
	testProject     = "test-project"
	testZone        = "us-central1-a"
	testMachineType = "e2-standard-4"
	testMig         = "test-mig"
	testTemplate    = "test-template"
)

// newDefaultGceClient returns a fake GCE client with default zones and machine
// types, and no MIGs or reservations.
func newDefaultGceClient(t *testing.T) *GceClient {
	t.Helper()
	kubeClient := k8sfake.NewSimpleClientset()
	k8s := fakek8s.NewKubernetes(kubeClient, informers.NewSharedInformerFactory(kubeClient, 0))
	return NewGceClient(t, k8s).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		WithDefaultZones().
		WithDefaultMachineTypes()
}

// testMIG is a MIG with its instance template, and the number of instances the
// test creates in it.
type testMIG struct {
	mig      *gcev1.InstanceGroupManager
	template *gcev1.InstanceTemplate
	size     int64
}

type migOption func(*testMIG)

// withReservationAffinity sets the reservation affinity of the MIG's instance
// template.
func withReservationAffinity(consumeReservationType string, values ...string) migOption {
	return func(m *testMIG) {
		m.template.Properties.ReservationAffinity = &gcev1.ReservationAffinity{
			ConsumeReservationType: consumeReservationType,
			Values:                 values,
		}
	}
}

// newDefaultMig returns a testMIG named testMig of testMachineType machines in
// testZone, using the instance template testTemplate. The template has no
// reservation affinity unless set with withReservationAffinity.
func newDefaultMig(size int64, opts ...migOption) testMIG {
	m := testMIG{
		mig: &gcev1.InstanceGroupManager{
			Name:             testMig,
			Zone:             testZone,
			InstanceTemplate: testTemplate,
		},
		template: &gcev1.InstanceTemplate{
			Name: testTemplate,
			Properties: &gcev1.InstanceProperties{
				MachineType: testMachineType,
				Disks: []*gcev1.AttachedDisk{
					{InitializeParams: &gcev1.AttachedDiskInitializeParams{DiskSizeGb: 100}},
				},
				Metadata: &gcev1.Metadata{
					Items: []*gcev1.MetadataItems{
						{Key: "kube-env", Value: ptr.To("AUTOSCALER_ENV_VARS: os=linux;os_distribution=cos;arch=amd64\n")},
					},
				},
			},
		},
		size: size,
	}
	for _, opt := range opts {
		opt(&m)
	}
	return m
}

func testMigRef() gceinternal.GceRef {
	return gceinternal.GceRef{Project: testProject, Zone: testZone, Name: testMig}
}
