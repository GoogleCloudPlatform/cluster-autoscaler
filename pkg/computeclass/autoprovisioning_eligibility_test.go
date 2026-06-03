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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

func TestSetClusterAutoprovisioningEnabled(t *testing.T) {
	eligibility := NewAutoprovisioningEligibility(nil, false)

	changed := eligibility.SetClusterAutoprovisioningEnabled(false)
	assert.Equal(t, changed, false)
	assert.Equal(t, eligibility.IsNodeAutoprovisioningEnabled(), false)

	changed = eligibility.SetClusterAutoprovisioningEnabled(true)
	assert.Equal(t, changed, true)
	assert.Equal(t, eligibility.IsNodeAutoprovisioningEnabled(), true)

	changed = eligibility.SetClusterAutoprovisioningEnabled(true)
	assert.Equal(t, changed, false)
	assert.Equal(t, eligibility.IsNodeAutoprovisioningEnabled(), true)
}

func TestIsNodeAutoprovisioningEnabled(t *testing.T) {
	testCases := []struct {
		name              string
		clusterNapEnabled bool
		cccNapEnabled     bool
		wantEnabled       bool
	}{
		{
			name:        "cluster NAP disabled, ccc NAP disabled",
			wantEnabled: false,
		},
		{
			name:          "cluster NAP disabled, ccc NAP enabled",
			cccNapEnabled: true,
			wantEnabled:   true,
		},
		{
			name:              "cluster NAP enabled, ccc NAP disabled",
			clusterNapEnabled: true,
			wantEnabled:       true,
		},
		{
			name:              "cluster NAP enabled, ccc NAP enabled",
			clusterNapEnabled: true,
			cccNapEnabled:     true,
			wantEnabled:       true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eligibility := NewAutoprovisioningEligibility(nil, tc.cccNapEnabled)
			changed := eligibility.SetClusterAutoprovisioningEnabled(tc.clusterNapEnabled)
			assert.Equal(t, changed, tc.clusterNapEnabled)
			assert.Equal(t, tc.wantEnabled, eligibility.IsNodeAutoprovisioningEnabled())
		})
	}
}

func TestAreClusterLimitsEnabled(t *testing.T) {
	testCases := []struct {
		name              string
		clusterNapEnabled bool
		cccNapEnabled     bool
		wantEnabled       bool
	}{
		{
			name:        "cluster NAP disabled, ccc NAP disabled",
			wantEnabled: false,
		},
		{
			name:          "cluster NAP disabled, ccc NAP enabled",
			cccNapEnabled: true,
			wantEnabled:   false,
		},
		{
			name:              "cluster NAP enabled, ccc NAP disabled",
			clusterNapEnabled: true,
			wantEnabled:       true,
		},
		{
			name:              "cluster NAP enabled, ccc NAP enabled",
			clusterNapEnabled: true,
			cccNapEnabled:     true,
			wantEnabled:       true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eligibility := NewAutoprovisioningEligibility(nil, tc.cccNapEnabled)
			changed := eligibility.SetClusterAutoprovisioningEnabled(tc.clusterNapEnabled)
			assert.Equal(t, changed, tc.clusterNapEnabled)
			assert.Equal(t, tc.wantEnabled, eligibility.AreClusterLimitsEnabled())
		})
	}
}

func TestUseAutoprovisioningFeaturesForPodRequirements(t *testing.T) {
	testLabel := "test-ccc-label"
	cccs := []crd.CRD{
		crd.NewTestCrd(crd.WithLabel(testLabel), crd.WithCrdType(ccc.CrdType), crd.WithName("ccc-ap"), crd.WithAutoprovisioningEnabled()),
		crd.NewTestCrd(crd.WithLabel(testLabel), crd.WithCrdType(ccc.CrdType), crd.WithName("ccc-no-ap")),
	}

	testCases := []struct {
		name              string
		clusterNapEnabled bool
		cccNapEnabled     bool
		requirements      *podrequirements.Requirements
		wantEnabled       bool
	}{
		{
			name:         "cluster & ccc NAP disabled, no ccc requested",
			requirements: &podrequirements.Requirements{},
			wantEnabled:  false,
		},
		{
			name: "cluster & ccc NAP disabled, autoprovisioning ccc requested",
			requirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					testLabel: podrequirements.NewValues("ccc-ap"),
				}),
			},
			wantEnabled: false,
		},
		{
			name: "cluster & ccc NAP disabled, no-autoprovisioning ccc requested",
			requirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					testLabel: podrequirements.NewValues("ccc-no-ap"),
				}),
			},
			wantEnabled: false,
		},
		{
			name:              "cluster NAP enabled, no ccc requested",
			clusterNapEnabled: true,
			requirements:      &podrequirements.Requirements{},
			wantEnabled:       true,
		},
		{
			name:              "cluster NAP enabled, autoprovisioning ccc requested",
			clusterNapEnabled: true,
			requirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					testLabel: podrequirements.NewValues("ccc-ap"),
				}),
			},
			wantEnabled: true,
		},
		{
			name:              "cluster NAP enabled, no-autoprovisioning ccc requested",
			clusterNapEnabled: true,
			requirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					testLabel: podrequirements.NewValues("ccc-no-ap"),
				}),
			},
			wantEnabled: false,
		},
		{
			name:          "ccc NAP enabled, no ccc requested",
			cccNapEnabled: true,
			requirements:  &podrequirements.Requirements{},
			wantEnabled:   false,
		},
		{
			name:          "ccc NAP enabled, autoprovisioning ccc requested",
			cccNapEnabled: true,
			requirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					testLabel: podrequirements.NewValues("ccc-ap"),
				}),
			},
			wantEnabled: true,
		},
		{
			name:          "ccc NAP enabled, no-autoprovisioning ccc requested",
			cccNapEnabled: true,
			requirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					testLabel: podrequirements.NewValues("ccc-no-ap"),
				}),
			},
			wantEnabled: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := lister.NewMockCrdLister(cccs)
			l.SetCrdLabel(testLabel)
			eligibility := NewAutoprovisioningEligibility(l, tc.cccNapEnabled)
			eligibility.SetClusterAutoprovisioningEnabled(tc.clusterNapEnabled)
			assert.Equal(t, tc.wantEnabled, eligibility.UseAutoprovisioningFeaturesForPodRequirements(tc.requirements))
		})
	}
}

func TestUseAutoprovisioningFeaturesForNodeGroup(t *testing.T) {
	testLabel := "test-ccc-label"
	cccs := []crd.CRD{
		crd.NewTestCrd(crd.WithLabel(testLabel), crd.WithCrdType(ccc.CrdType), crd.WithName("ccc-ap"), crd.WithAutoprovisioningEnabled()),
		crd.NewTestCrd(crd.WithLabel(testLabel), crd.WithCrdType(ccc.CrdType), crd.WithName("ccc-no-ap")),
	}

	// manager is used as fallback in NodeGroupCrdLabel
	manager := &gke.GkeManagerMock{}
	manager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&v1.Node{}), nil)

	testCases := []struct {
		name              string
		clusterNapEnabled bool
		cccNapEnabled     bool
		nodeGroup         cloudprovider.NodeGroup
		wantEnabled       bool
	}{
		{
			name:        "cluster & ccc NAP disabled, no ccc requested",
			nodeGroup:   gke.NewTestGkeMigBuilder().SetGkeManager(manager).Build(),
			wantEnabled: false,
		},
		{
			name: "cluster & ccc NAP disabled, autoprovisioning ccc requested",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{
					testLabel: "ccc-ap",
				},
			}).Build(),
			wantEnabled: false,
		},
		{
			name: "cluster & ccc NAP disabled, no-autoprovisioning ccc requested",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{
					testLabel: "ccc-no-ap",
				},
			}).Build(),
			wantEnabled: false,
		},
		{
			name:              "cluster NAP enabled, no ccc requested",
			clusterNapEnabled: true,
			nodeGroup:         gke.NewTestGkeMigBuilder().SetGkeManager(manager).Build(),
			wantEnabled:       true,
		},
		{
			name:              "cluster NAP enabled, autoprovisioning ccc requested",
			clusterNapEnabled: true,
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{
					testLabel: "ccc-ap",
				},
			}).Build(),
			wantEnabled: true,
		},
		{
			name:              "cluster NAP enabled, no-autoprovisioning ccc requested",
			clusterNapEnabled: true,
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{
					testLabel: "ccc-no-ap",
				},
			}).Build(),
			wantEnabled: false,
		},
		{
			name:          "ccc NAP enabled, no ccc requested",
			cccNapEnabled: true,
			nodeGroup:     gke.NewTestGkeMigBuilder().SetGkeManager(manager).Build(),
			wantEnabled:   false,
		},
		{
			name:          "ccc NAP enabled, autoprovisioning ccc requested",
			cccNapEnabled: true,
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{
					testLabel: "ccc-ap",
				},
			}).Build(),
			wantEnabled: true,
		},
		{
			name:          "ccc NAP enabled, no-autoprovisioning ccc requested",
			cccNapEnabled: true,
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{
					testLabel: "ccc-no-ap",
				},
			}).Build(),
			wantEnabled: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := lister.NewMockCrdLister(cccs)
			l.SetCrdLabel(testLabel)
			eligibility := NewAutoprovisioningEligibility(l, tc.cccNapEnabled)
			eligibility.SetClusterAutoprovisioningEnabled(tc.clusterNapEnabled)
			assert.Equal(t, tc.wantEnabled, eligibility.UseAutoprovisioningFeaturesForNodeGroup(tc.nodeGroup))
		})
	}
}
