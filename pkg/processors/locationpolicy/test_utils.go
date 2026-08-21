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
	"reflect"

	"github.com/google/go-cmp/cmp"
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

var (
	compareAllUnexportedOpt = cmp.Exporter(func(t reflect.Type) bool { return true })
)

type testMigConfig struct {
	project          string
	zone             string
	name             string
	targetSize       int
	maxSize          int
	templateSelfLink string
	locationPolicy   gke.LocationPolicyEnum
	spec             *gkeclient.NodePoolSpec
}

func newTestMig(mockManager *gke.GkeManagerMock, config testMigConfig, getInstanceTemplateErr, getSizeErr bool) *gke.GkeMig {
	mig := gke.NewTestGkeMigBuilder().
		SetGkeManager(mockManager).
		SetGceRef(gce.GceRef{Project: config.project, Zone: config.zone, Name: config.name}).
		SetNodePoolName(config.name).
		SetMaxSize(config.maxSize).
		SetLocationPolicy(config.locationPolicy).
		SetSpec(config.spec).
		SetExist(true).
		Build()

	if getInstanceTemplateErr {
		mockManager.On("GetMigInstanceTemplate", mig).Return(nil, errors.New("mig template get error")).Once()
	} else {
		mockManager.On("GetMigInstanceTemplate", mig).Return(&gce_api.InstanceTemplate{SelfLink: config.templateSelfLink}, nil)
	}
	if getSizeErr {
		mockManager.On("GetMigSize", mig).Return(int64(0), errors.New("mig size get error")).Once()
	} else {
		mockManager.On("GetMigSize", mig).Return(int64(config.targetSize), nil)
	}

	return mig
}
