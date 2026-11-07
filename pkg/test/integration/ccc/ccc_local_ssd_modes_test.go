/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccc_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	google_api_container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestComputeClassLocalSSDModes(t *testing.T) {
	testCases := []struct {
		name                    string
		machineType             string
		storage                 v1.Storage
		ephemeralStorageRequest string
		wantBlockConfig         *google_api_container.LocalNvmeSsdBlockConfig
		wantEphemeralConfig     *google_api_container.EphemeralStorageLocalSsdConfig
		expectedLabels          map[string]string
		forbiddenLabels         []string
	}{
		{
			name:        "raw block mode with explicit localSSDCount",
			machineType: "n2d-standard-4",
			storage: v1.Storage{
				LocalNvmeSsdBlockConfig: &v1.LocalNvmeSsdBlockConfig{
					LocalSSDCount: ptr.To(4),
				},
			},
			wantBlockConfig: &google_api_container.LocalNvmeSsdBlockConfig{
				LocalSsdCount: 4,
			},
			expectedLabels: map[string]string{
				gkelabels.LocalNvmeSsdBlockLabel: gkelabels.LocalNvmeSsdBlockValue,
			},
			forbiddenLabels: []string{
				gkelabels.EphemeralLocalSsdLabel,
				gkelabels.LocalNvmeSsdPartitionLabel,
			},
		},
		{
			name:        "raw block mode with omitted localSSDCount on fixed-LSSD machine",
			machineType: "c3-standard-4-lssd",
			storage: v1.Storage{
				LocalNvmeSsdBlockConfig: &v1.LocalNvmeSsdBlockConfig{},
			},
			wantBlockConfig: &google_api_container.LocalNvmeSsdBlockConfig{
				LocalSsdCount: 1,
			},
			expectedLabels: map[string]string{
				gkelabels.LocalNvmeSsdBlockLabel: gkelabels.LocalNvmeSsdBlockValue,
			},
			forbiddenLabels: []string{
				gkelabels.EphemeralLocalSsdLabel,
				gkelabels.LocalNvmeSsdPartitionLabel,
			},
		},
		{
			name:        "mixed mode with ephemeralCapacityGb",
			machineType: "c3-standard-4-lssd",
			storage: v1.Storage{
				EphemeralStorageLocalSsdConfig: &v1.EphemeralStorageLocalSsdConfig{
					EphemeralCapacityGb: ptr.To[int64](200),
				},
			},
			ephemeralStorageRequest: "50Gi",
			wantEphemeralConfig: &google_api_container.EphemeralStorageLocalSsdConfig{
				LocalSsdCount: 1,
			},
			expectedLabels: map[string]string{
				gkelabels.EphemeralLocalSsdLabel:     gkelabels.EphemeralLocalSsdEnabledValue,
				gkelabels.LocalNvmeSsdPartitionLabel: gkelabels.LocalNvmeSsdPartitionValue,
			},
			forbiddenLabels: []string{
				gkelabels.LocalNvmeSsdBlockLabel,
			},
		},
		{
			name:        "plain ephemeralStorageLocalSsdConfig without capacity",
			machineType: "c3-standard-4-lssd",
			storage: v1.Storage{
				EphemeralStorageLocalSsdConfig: &v1.EphemeralStorageLocalSsdConfig{},
			},
			ephemeralStorageRequest: "50Gi",
			wantEphemeralConfig: &google_api_container.EphemeralStorageLocalSsdConfig{
				LocalSsdCount: 1,
			},
			expectedLabels: map[string]string{
				gkelabels.EphemeralLocalSsdLabel: gkelabels.EphemeralLocalSsdEnabledValue,
			},
			forbiddenLabels: []string{
				gkelabels.LocalNvmeSsdBlockLabel,
				gkelabels.LocalNvmeSsdPartitionLabel,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ccName := "lssd-ccc"
			cc := ccc.NewComputeClassBuilder(ccName).
				WithNapEnabled().
				WithWhenUnsatisfiable("ScaleUpAnyway").
				WithPriorities(v1.Priority{
					MachineType: ptr.To(tc.machineType),
					Storage:     &tc.storage,
				}).
				Build()

			testConfig := integration.NewTestConfig().
				WithOverrides(integration.WithAutoProvisioningEnabled()).
				WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
				WithCccCrds(cc)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				require.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				pendingPod := tu.BuildTestPod("lssd-pod", 500, 500, tu.MarkUnschedulable(), pod.WithCCC(ccName))
				if tc.ephemeralStorageRequest != "" {
					pod.WithResource(apiv1.ResourceEphemeralStorage, tc.ephemeralStorageRequest, tc.ephemeralStorageRequest)(pendingPod)
				}
				infra.Fakes.K8s.AddPod(pendingPod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				np := infra.Fakes.GkeService.MustGetSingleAutoprovisionedNodePool(t)
				require.NotNil(t, np.Config)
				assert.Equal(t, tc.wantBlockConfig, np.Config.LocalNvmeSsdBlockConfig)
				assert.Equal(t, tc.wantEphemeralConfig, np.Config.EphemeralStorageLocalSsdConfig)
				assert.Nil(t, np.Config.EphemeralStorageConfig)
				for _, forbidden := range tc.forbiddenLabels {
					assert.NotContains(t, np.Config.Labels, forbidden)
				}

				foundNapGroup := false
				for _, ng := range autoscaler.AutoscalingContext.CloudProvider.NodeGroups(ctx) {
					if ng.Autoprovisioned(ctx) {
						foundNapGroup = true
						gkeMig, ok := ng.(*gke.GkeMig)
						require.True(t, ok)
						injectedMig := gkeMig.GetInjectedMig()
						require.NotNil(t, injectedMig)
						tplInfo, err := injectedMig.TemplateNodeInfo(ctx)
						require.NoError(t, err)
						assertLabelsMatch(t, tplInfo.Node().Labels, ccName, tc.expectedLabels, tc.forbiddenLabels)
					}
				}
				require.True(t, foundNapGroup)

				infra.Fakes.RunScheduler(ctx, t)
				scheduledPod, err := infra.Fakes.KubeClient.CoreV1().Pods(pendingPod.Namespace).Get(ctx, pendingPod.Name, metav1.GetOptions{})
				require.NoError(t, err)
				assert.NotEmpty(t, scheduledPod.Spec.NodeName)
			})
		})
	}
}

func assertLabelsMatch(t *testing.T, labels map[string]string, ccName string, expected map[string]string, forbidden []string) {
	t.Helper()
	assert.Equal(t, ccName, labels[gkelabels.ComputeClassLabel])
	for k, v := range expected {
		assert.Equal(t, v, labels[k])
	}
	for _, k := range forbidden {
		assert.NotContains(t, labels, k)
	}
}
