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

package computeclass

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	npc_crd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/utils/ptr"
)

func TestAutoscalingOptionsProvider(t *testing.T) {
	var (
		consolidationDelay               = 5 * time.Minute
		scaleDownUnneededTime            = consolidationDelay
		consolidationThreshold           = 50
		scaleDownUtilizationThreshold    = float64(consolidationThreshold) / 100.0
		gpuConsolidationThreshold        = 60
		scaleDownGpuUtilizationThreshold = float64(gpuConsolidationThreshold) / 100.0
	)

	crdLabel := "crd-label"
	crdName := "crd-name"

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil)

	tests := map[string]struct {
		crd                                               npc_crd.CRD
		withNodeSelector                                  bool
		withDefaultCrd                                    bool
		withMigConsolidationDelay                         string
		wantScaleDownUnneededTime                         *time.Duration
		wantScaleDownUtilizationThreshold                 *float64
		wantScaleDownGpuUtilizationThreshold              *float64
		wantError                                         bool
		disableNodePoolConsolidationDelayMinCAVersionFlag bool
	}{
		"optional params": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
			),
			withNodeSelector:                     true,
			wantScaleDownUnneededTime:            nil,
			wantScaleDownUtilizationThreshold:    nil,
			wantScaleDownGpuUtilizationThreshold: nil,
		},
		"unneeded time": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(consolidationDelay),
			),
			withNodeSelector:                     true,
			wantScaleDownUnneededTime:            &scaleDownUnneededTime,
			wantScaleDownUtilizationThreshold:    nil,
			wantScaleDownGpuUtilizationThreshold: nil,
		},
		"utilization time": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationThreshold(consolidationThreshold),
			),
			withNodeSelector:                     true,
			wantScaleDownUnneededTime:            nil,
			wantScaleDownUtilizationThreshold:    &scaleDownUtilizationThreshold,
			wantScaleDownGpuUtilizationThreshold: nil,
		},
		"gpu utilization time": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithGPUConsolidationThreshold(gpuConsolidationThreshold),
			),
			withNodeSelector:                     true,
			wantScaleDownUnneededTime:            nil,
			wantScaleDownUtilizationThreshold:    nil,
			wantScaleDownGpuUtilizationThreshold: &scaleDownGpuUtilizationThreshold,
		},
		"combined policy": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(consolidationDelay),
				npc_crd.WithConsolidationThreshold(consolidationThreshold),
				npc_crd.WithGPUConsolidationThreshold(gpuConsolidationThreshold),
			),
			withNodeSelector:                     true,
			wantScaleDownUnneededTime:            &scaleDownUnneededTime,
			wantScaleDownUtilizationThreshold:    &scaleDownUtilizationThreshold,
			wantScaleDownGpuUtilizationThreshold: &scaleDownGpuUtilizationThreshold,
		},
		"no crd selected": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(consolidationDelay),
				npc_crd.WithConsolidationThreshold(consolidationThreshold),
				npc_crd.WithGPUConsolidationThreshold(gpuConsolidationThreshold),
			),
		},
		"crd does not exist": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName("other"),
				npc_crd.WithConsolidationDelay(consolidationDelay),
				npc_crd.WithConsolidationThreshold(consolidationThreshold),
				npc_crd.WithGPUConsolidationThreshold(gpuConsolidationThreshold),
			),
			withNodeSelector: true,
			wantError:        true,
		},
		"default set and specified": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(consolidationDelay),
				npc_crd.WithConsolidationThreshold(consolidationThreshold),
				npc_crd.WithGPUConsolidationThreshold(gpuConsolidationThreshold),
			),
			withNodeSelector:                     true,
			withDefaultCrd:                       true,
			wantScaleDownUnneededTime:            &scaleDownUnneededTime,
			wantScaleDownUtilizationThreshold:    &scaleDownUtilizationThreshold,
			wantScaleDownGpuUtilizationThreshold: &scaleDownGpuUtilizationThreshold,
		},
		"default set and not specified": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(consolidationDelay),
				npc_crd.WithConsolidationThreshold(consolidationThreshold),
				npc_crd.WithGPUConsolidationThreshold(gpuConsolidationThreshold),
			),
			withDefaultCrd:                       true,
			wantScaleDownUnneededTime:            &scaleDownUnneededTime,
			wantScaleDownUtilizationThreshold:    &scaleDownUtilizationThreshold,
			wantScaleDownGpuUtilizationThreshold: &scaleDownGpuUtilizationThreshold,
		},
		"non-existing default set and not specified": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName("other-crd"),
				npc_crd.WithConsolidationDelay(consolidationDelay),
				npc_crd.WithConsolidationThreshold(consolidationThreshold),
				npc_crd.WithGPUConsolidationThreshold(gpuConsolidationThreshold),
			),
			withDefaultCrd: true,
			wantError:      true,
		},
		"with crd delay and no mig, chooses crd delay": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(woErr(time.ParseDuration("100s"))),
			),
			withDefaultCrd:            true,
			wantError:                 false,
			wantScaleDownUnneededTime: ptr.To(woErr(time.ParseDuration("100s"))),
		},
		"with mig delay and no crd, chooses mig delay": {
			withMigConsolidationDelay: "123",
			withDefaultCrd:            false,
			wantError:                 false,
			wantScaleDownUnneededTime: ptr.To(woErr(time.ParseDuration("123s"))),
		},
		"with mig delay and crd delay, chooses mig delay": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(woErr(time.ParseDuration("100s"))),
			),
			withMigConsolidationDelay: "123",
			withDefaultCrd:            true,
			wantError:                 false,
			wantScaleDownUnneededTime: ptr.To(woErr(time.ParseDuration("123s"))),
		},
		"with mig delay and crd delay, and blocked experiment, chooses crd delay": {
			crd: npc_crd.NewTestCrd(
				npc_crd.WithLabel(crdLabel),
				npc_crd.WithName(crdName),
				npc_crd.WithConsolidationDelay(woErr(time.ParseDuration("100s"))),
			),
			withMigConsolidationDelay: "123",
			withDefaultCrd:            true,
			wantError:                 false,
			wantScaleDownUnneededTime: ptr.To(woErr(time.ParseDuration("100s"))),
			disableNodePoolConsolidationDelayMinCAVersionFlag: true,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			crds := []npc_crd.CRD{}
			if test.crd != nil {
				crds = append(crds, test.crd)
			}
			mockCrdLister := lister.NewMockCrdLister(crds)
			mockCrdLister.SetCrdLabel(crdLabel)
			if test.withDefaultCrd {
				mockCrdLister.SetDefaultCrdName(crdName)
			}
			exps := []string{}
			if !test.disableNodePoolConsolidationDelayMinCAVersionFlag {
				exps = append(exps, experiments.NodePoolConsolidationDelayMinCAVersionFlag)
			}
			provider := NewAutoscalingOptionsProvider(mockCrdLister, experiments.NewMockManager(exps...))

			spec := &gkeclient.NodePoolSpec{}
			if test.withMigConsolidationDelay != "" {
				spec.ConsolidationDelayInSeconds = test.withMigConsolidationDelay
			}
			if test.withNodeSelector {
				spec.Labels = map[string]string{crdLabel: crdName}
			}

			mig := gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetSpec(spec).Build()

			threshold, found, err := provider.ScaleDownUtilizationThreshold(mig)
			if test.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assertEqualAutoscalingOpt(t, test.wantScaleDownUtilizationThreshold, threshold, found)
			}

			gpuThreshold, found, err := provider.ScaleDownGpuUtilizationThreshold(mig)
			if test.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assertEqualAutoscalingOpt(t, test.wantScaleDownGpuUtilizationThreshold, gpuThreshold, found)
			}

			unneededTime, found, err := provider.ScaleDownUnneededTime(mig)
			if test.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assertEqualAutoscalingOpt(t, test.wantScaleDownUnneededTime, unneededTime, found)
			}
		})
	}
}

func assertEqualAutoscalingOpt[T any](t *testing.T, wantOption *T, gotOption T, found bool) {
	t.Helper()

	if wantOption == nil {
		assert.False(t, found)
		return
	}

	assert.Equal(t, *wantOption, gotOption)
}
