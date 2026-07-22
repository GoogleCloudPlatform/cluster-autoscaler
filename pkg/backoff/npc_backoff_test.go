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

package backoff

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	npc_crd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	npc_rules "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestBackoffDecisions(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultCrdName := "crd-default"
	machineFamily := machinetypes.E2.Name()
	otherFamily := machinetypes.N2.Name()

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil)

	nonCrdPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("non-crd").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		SetGkeManager(gkeManager).
		Build()
	nonCrdMatchingPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("non-crd-matching").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		SetGkeManager(gkeManager).
		Build()
	nonCrdNonMatchingPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("non-crd-non-matching").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: fmt.Sprintf("%s-standard-2", otherFamily)}).
		SetGkeManager(gkeManager).
		Build()

	defaultPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("default").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		Build()
	matchingPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("matching").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		Build()
	nonMatchingPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("non-matching").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-2"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		Build()
	differentCrdPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("matching-family-different-crd").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-2"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		Build()
	multipleReservationsCrd := gke.NewTestGkeMigBuilder().
		SetNodePoolName("matching-family-different-crd").
		SetSpec(&gkeclient.NodePoolSpec{
			ReservationAffinity: &container.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    gkeclient.ReservationNameKey,
				Values:                 []string{"some-name"},
			},
			Labels: map[string]string{
				testCrdLabel:                    "crd-object-1",
				labels.ReservationAffinityLabel: "specific",
				labels.ReservationNameLabel:     "some-name",
			},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		Build()

	refA := gce.GceRef{Project: "p", Zone: "us-central1-a", Name: "pool-zone-a"}
	poolZoneA := gke.NewTestGkeMigBuilder().
		SetGceRef(refA).
		SetNodePoolName("pool-zone-a").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		SetDeploymentType(gke.DeploymentTypeNone).
		Build()

	refB := gce.GceRef{Project: "p", Zone: "us-central1-b", Name: "pool-zone-b"}
	poolZoneB := gke.NewTestGkeMigBuilder().
		SetGceRef(refB).
		SetNodePoolName("pool-zone-b").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		SetDeploymentType(gke.DeploymentTypeNone).
		Build()

	testCases := []struct {
		name                string
		groupFailingScaleUp cloudprovider.NodeGroup
		crd                 npc_crd.CRD
		extraCrd            npc_crd.CRD
		defaultCrd          bool
		errorInfo           cloudprovider.InstanceErrorInfo
		wantBackoff         []cloudprovider.NodeGroup
		wantNoBackoff       []cloudprovider.NodeGroup
	}{
		{
			name:                "Non CRD pool, default crd not set - no backoff",
			groupFailingScaleUp: nonCrdPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName(defaultCrdName),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			errorInfo:     quotaError,
			wantNoBackoff: []cloudprovider.NodeGroup{nonCrdPool, nonCrdMatchingPool, nonCrdNonMatchingPool},
		},
		{
			name:                "Non CRD pool, default set but does not exist - no backoff",
			groupFailingScaleUp: nonCrdPool,
			defaultCrd:          true,
			errorInfo:           quotaError,
			wantNoBackoff:       []cloudprovider.NodeGroup{nonCrdPool, nonCrdMatchingPool, nonCrdNonMatchingPool},
		},
		{
			name:                "Non CRD pool, default set and exist - backoff matching",
			groupFailingScaleUp: nonCrdPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName(defaultCrdName),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			defaultCrd:    true,
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{nonCrdPool, nonCrdMatchingPool},
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, nonCrdNonMatchingPool},
		},
		{
			name:                "Wrong error type - no backoff",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{defaultPool, matchingPool},
			wantNoBackoff: []cloudprovider.NodeGroup{nonMatchingPool},
		},
		{
			name:                "nil nodegroup - no backoff",
			groupFailingScaleUp: nil,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			errorInfo:     quotaError,
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool},
		},
		{
			name:                "Can't list NPC - no backoff",
			groupFailingScaleUp: defaultPool,
			errorInfo:           quotaError,
			wantNoBackoff:       []cloudprovider.NodeGroup{defaultPool},
		},
		{
			name:                "No priorities - no backoff",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{})),
			errorInfo:     quotaError,
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool},
		},
		{
			name:                "No matching rule (outside ScaleUpAnyway) - no backoff",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			errorInfo:     quotaError,
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, matchingPool, nonMatchingPool},
		},
		{
			name:                "No matching rule (and DoNotScaleUp) - no backoff",
			groupFailingScaleUp: defaultPool,
			errorInfo:           quotaError,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, matchingPool, nonMatchingPool},
		},
		{
			name:                "Single rule + DoNotScaleUp - no backoff",
			groupFailingScaleUp: defaultPool,
			errorInfo:           quotaError,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithAutoprovisioningEnabled()),
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, matchingPool, nonMatchingPool},
		},
		{
			name:                "Backoff applies to groups based on whether they match rules",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{defaultPool, matchingPool},
			wantNoBackoff: []cloudprovider.NodeGroup{nonMatchingPool},
		},
		{
			name:                "Backoff doesn't apply to other crds",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			extraCrd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-2"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{defaultPool, matchingPool},
			wantNoBackoff: []cloudprovider.NodeGroup{differentCrdPool},
		},
		{
			name:                "Backoff first rule in multi-rule crd",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{defaultPool, matchingPool},
			wantNoBackoff: []cloudprovider.NodeGroup{nonMatchingPool},
		},
		{
			name:                "Backoff second rule in multi-rule NPC",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{defaultPool, matchingPool},
			wantNoBackoff: []cloudprovider.NodeGroup{nonMatchingPool},
		},
		{
			name:                "Last rule in multi-rule NPC + DoNotScaleUp - no backoff",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				})),
			errorInfo:     quotaError,
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, matchingPool, nonMatchingPool},
		},
		{
			name:                "Nodegroup rule - no backoff",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewRule(npc_rules.WithNodePoolsRule([]string{"default"})),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			errorInfo:     quotaError,
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, matchingPool, nonMatchingPool},
		},
		{
			name:                "Nodegroup rule followed by matching rule - no backoff",
			groupFailingScaleUp: defaultPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewRule(npc_rules.WithNodePoolsRule([]string{"default"})),
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			errorInfo:     quotaError,
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, matchingPool, nonMatchingPool},
		},
		{
			name:                "Nodegroup rule followed by backed off rule - no backoff",
			groupFailingScaleUp: matchingPool,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewRule(npc_rules.WithNodePoolsRule([]string{"default"})),
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{matchingPool},
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, nonMatchingPool},
		},
		{
			name:                "Multiple reservations CRD - no backoff",
			groupFailingScaleUp: multipleReservationsCrd,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewRule(
						npc_rules.WithReservationsRule(npc_rules.NewReservation().WithReservationName("some-name").WithReservationAffinity(reservations.SpecificAffinity).WithReservationPath("some-name")),
						npc_rules.WithReservationsRule(npc_rules.NewReservation().WithReservationName("some-other-name").WithReservationAffinity(reservations.SpecificAffinity).WithReservationPath("some-other-name")),
						npc_rules.WithMachineFamilyRule(&machineFamily),
					),
				}), npc_crd.WithScaleUpAnyway(), npc_crd.WithAutoprovisioningEnabled()),
			errorInfo:     quotaError,
			wantBackoff:   []cloudprovider.NodeGroup{},
			wantNoBackoff: []cloudprovider.NodeGroup{defaultPool, nonMatchingPool, multipleReservationsCrd},
		},
		{
			name:                "Stockout in zone A - backs off zone A but NOT zone B",
			groupFailingScaleUp: poolZoneA,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			errorInfo:     stockoutError,
			wantBackoff:   []cloudprovider.NodeGroup{poolZoneA},
			wantNoBackoff: []cloudprovider.NodeGroup{poolZoneB},
		},
		{
			name:                "Quota error in zone A - triggers regional backoff for both zone A and zone B",
			groupFailingScaleUp: poolZoneA,
			crd: npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()),
			errorInfo:   quotaError,
			wantBackoff: []cloudprovider.NodeGroup{poolZoneA, poolZoneB},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			var crds []npc_crd.CRD
			if tc.crd != nil {
				crds = append(crds, tc.crd)
			}
			if tc.extraCrd != nil {
				crds = append(crds, tc.extraCrd)
			}
			lister := npc_lister.NewMockCrdLister(crds)
			lister.SetCrdLabel(testCrdLabel)
			if tc.defaultCrd {
				lister.SetDefaultCrdName(defaultCrdName)
			}
			provider := &mockProvider{}
			provider.On("IsAutopilotEnabled").Return(false)
			backoff := NewNpcCrdBackoff(5*time.Minute, lister, provider)
			backoff.Backoff(tc.groupFailingScaleUp, nil, tc.errorInfo, now)
			for _, ng := range tc.wantBackoff {
				if !backoff.BackoffStatus(ng, nil, now).IsBackedOff {
					t.Errorf("Expected MIG %s to be backed off, but it isn't", ng.Id())
				}
			}
			for _, ng := range tc.wantNoBackoff {
				if backoff.BackoffStatus(ng, nil, now).IsBackedOff {
					t.Errorf("Unexpected backoff for MIG %s", ng.Id())
				}
			}
		})
	}
}

func TestRuleBackoffStatus(t *testing.T) {
	testCrdLabel := "test-crd-label"
	machineFamily1 := machinetypes.E2.Name()
	machineFamily2 := machinetypes.N2.Name()

	np := gke.NewTestGkeMigBuilder().
		SetNodePoolName("np").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily1)}).
		Build()

	refA := gce.GceRef{Project: "p", Zone: "us-central1-a", Name: "pool-zone-a"}
	poolZoneA := gke.NewTestGkeMigBuilder().
		SetGceRef(refA).
		SetNodePoolName("pool-zone-a").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily1)}).
		SetDeploymentType(gke.DeploymentTypeNone).
		Build()

	crd1 := npc_crd.NewTestCrd(
		npc_crd.WithLabel(testCrdLabel),
		npc_crd.WithName("crd-object-1"),
		npc_crd.WithRules([]npc_rules.Rule{
			npc_rules.NewMachineSpecRule(&machineFamily1, nil, nil, nil),
			npc_rules.NewMachineSpecRule(&machineFamily2, nil, nil, nil),
		}),
		npc_crd.WithScaleUpAnyway(),
	)
	crd2 := npc_crd.NewTestCrd(
		npc_crd.WithLabel(testCrdLabel),
		npc_crd.WithName("crd-object-2"),
		npc_crd.WithRules([]npc_rules.Rule{
			npc_rules.NewMachineSpecRule(&machineFamily1, nil, nil, nil),
		}),
	)

	testCases := []struct {
		name                  string
		npcCrds               []npc_crd.CRD
		npcCrdToCheck         npc_crd.CRD
		ruleIdx               int
		nodeGroup             cloudprovider.NodeGroup
		errorInfo             cloudprovider.InstanceErrorInfo
		wantRuleBackoffStatus base_backoff.Status
	}{
		{
			name:          "Backed off node pool rule check - backoff",
			npcCrds:       []npc_crd.CRD{crd1},
			npcCrdToCheck: crd1,
			ruleIdx:       0,
			wantRuleBackoffStatus: base_backoff.Status{
				IsBackedOff: true,
				ErrorInfo:   quotaError,
			},
		},
		{
			name:          "Zonal stockout backs off specific rule for that CRD",
			npcCrds:       []npc_crd.CRD{crd1},
			npcCrdToCheck: crd1,
			ruleIdx:       0,
			nodeGroup:     poolZoneA,
			errorInfo:     stockoutError,
			wantRuleBackoffStatus: base_backoff.Status{
				IsBackedOff: true,
				ErrorInfo:   stockoutError,
			},
		},
		{
			name:          "Invalid rule id check - no backoff",
			npcCrds:       []npc_crd.CRD{crd1},
			npcCrdToCheck: crd1,
			ruleIdx:       3,
			wantRuleBackoffStatus: base_backoff.Status{
				IsBackedOff: false,
			},
		},
		{
			name:          "Different rule check - no backoff",
			npcCrds:       []npc_crd.CRD{crd1, crd2},
			npcCrdToCheck: crd1,
			ruleIdx:       1,
			wantRuleBackoffStatus: base_backoff.Status{
				IsBackedOff: false,
			},
		},
		{
			name:          "Different crd check - no backoff",
			npcCrds:       []npc_crd.CRD{crd1, crd2},
			npcCrdToCheck: crd2,
			ruleIdx:       0,
			wantRuleBackoffStatus: base_backoff.Status{
				IsBackedOff: false,
			},
		},
		{
			name:          "Nil crd check - no backoff",
			npcCrds:       []npc_crd.CRD{crd1},
			npcCrdToCheck: nil,
			ruleIdx:       0,
			wantRuleBackoffStatus: base_backoff.Status{
				IsBackedOff: false,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			lister := npc_lister.NewMockCrdLister(tc.npcCrds)
			lister.SetCrdLabel(testCrdLabel)
			provider := &mockProvider{}
			provider.On("IsAutopilotEnabled").Return(false)
			backoff := NewNpcCrdBackoff(5*time.Minute, lister, provider)
			nodeGroup := cloudprovider.NodeGroup(np)
			if tc.nodeGroup != nil {
				nodeGroup = tc.nodeGroup
			}
			errInfo := tc.errorInfo
			if errInfo.ErrorCode == "" {
				errInfo = quotaError
			}
			backoff.Backoff(nodeGroup, nil, errInfo, now)
			ruleBackoffStatus := backoff.RuleBackoffStatus(tc.npcCrdToCheck, tc.ruleIdx, now)
			assert.Equal(t, tc.wantRuleBackoffStatus, ruleBackoffStatus)
		})
	}
}

type opType int

const (
	opTypeBackoff opType = iota
	opTypeIsBackedOff
)

type backoffOp struct {
	op          opType
	tplus       time.Duration
	nodegroup   cloudprovider.NodeGroup
	errorInfo   cloudprovider.InstanceErrorInfo
	wantBackoff bool
}

func TestBackoffDurations(t *testing.T) {
	testCrdLabel := "test-crd-label"
	duration := 5 * time.Minute
	machineFamily := machinetypes.E2.Name()
	otherFamily := machinetypes.N2.Name()

	defaultPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("default").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-4", machineFamily)}).
		Build()
	otherPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("other").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-2", otherFamily)}).
		Build()
	differentCrdPool := gke.NewTestGkeMigBuilder().
		SetNodePoolName("matching-family-different-crd").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-2"},
			MachineType: fmt.Sprintf("%s-standard-2", machineFamily)}).
		Build()

	refA := gce.GceRef{Project: "p", Zone: "us-central1-a", Name: "pool-zone-a"}
	poolZoneA := gke.NewTestGkeMigBuilder().
		SetGceRef(refA).
		SetNodePoolName("pool-zone-a").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-4", machineFamily)}).
		SetDeploymentType(gke.DeploymentTypeNone).
		Build()

	refB := gce.GceRef{Project: "p", Zone: "us-central1-b", Name: "pool-zone-b"}
	poolZoneB := gke.NewTestGkeMigBuilder().
		SetGceRef(refB).
		SetNodePoolName("pool-zone-b").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels:      map[string]string{testCrdLabel: "crd-object-1"},
			MachineType: fmt.Sprintf("%s-standard-4", machineFamily)}).
		SetDeploymentType(gke.DeploymentTypeNone).
		Build()

	testCases := []struct {
		name       string
		operations []backoffOp
	}{
		{
			name: "Backoff expires",
			operations: []backoffOp{
				{
					op:          opTypeBackoff,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: false,
				},
			},
		},
		{
			name: "Backoff refreshes correctly",
			operations: []backoffOp{
				{
					op:          opTypeBackoff,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeBackoff,
					tplus:       3 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       7 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       8*time.Minute + 1*time.Second,
					nodegroup:   defaultPool,
					wantBackoff: false,
				},
			},
		},
		{
			name: "Backoff - expire - backoff again",
			operations: []backoffOp{
				{
					op:          opTypeBackoff,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: false,
				},
				{
					op:          opTypeBackoff,
					tplus:       6 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       7 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       11 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       11*time.Minute + 1*time.Second,
					nodegroup:   defaultPool,
					wantBackoff: false,
				},
			},
		},
		{
			name: "Backoff on a different rule refreshes duration",
			operations: []backoffOp{
				{
					op:          opTypeBackoff,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       time.Minute,
					nodegroup:   otherPool,
					wantBackoff: false,
				},
				{
					op:          opTypeBackoff,
					tplus:       3 * time.Minute,
					nodegroup:   otherPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6 * time.Minute,
					nodegroup:   otherPool,
					wantBackoff: true,
				},
			},
		},
		{
			name: "No interaction between backoffs on different NPCs",
			// npc-1 backoff from 0m to 5m, npc-2 from 3m to 8m
			// probe both npc at 2m interval
			operations: []backoffOp{
				{
					op:          opTypeBackoff,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       2 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       2 * time.Minute,
					nodegroup:   differentCrdPool,
					wantBackoff: false,
				},
				{
					op:          opTypeBackoff,
					tplus:       3 * time.Minute,
					nodegroup:   differentCrdPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       4 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       4 * time.Minute,
					nodegroup:   differentCrdPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6 * time.Minute,
					nodegroup:   defaultPool,
					wantBackoff: false,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6 * time.Minute,
					nodegroup:   differentCrdPool,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       8*time.Minute + 1*time.Second,
					nodegroup:   defaultPool,
					wantBackoff: false,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       8*time.Minute + 1*time.Second,
					nodegroup:   differentCrdPool,
					wantBackoff: false,
				},
			},
		},
		{
			name: "Zonal backoff extends the whole CRD backoff",
			operations: []backoffOp{
				{
					op:          opTypeBackoff,
					nodegroup:   poolZoneA,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeBackoff,
					tplus:       time.Minute,
					nodegroup:   poolZoneB,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       2 * time.Minute,
					nodegroup:   poolZoneA,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       2 * time.Minute,
					nodegroup:   poolZoneB,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       5*time.Minute + 1*time.Second,
					nodegroup:   poolZoneA,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       5*time.Minute + 1*time.Second,
					nodegroup:   poolZoneB,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6*time.Minute + 1*time.Second,
					nodegroup:   poolZoneB,
					wantBackoff: false,
				},
			},
		},
		{
			name: "Zonal backoff maintans full priorities iteration",
			operations: []backoffOp{
				{
					op:          opTypeBackoff,
					nodegroup:   poolZoneA,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeBackoff,
					tplus:       time.Minute,
					nodegroup:   poolZoneB,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       2 * time.Minute,
					nodegroup:   poolZoneA,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       2 * time.Minute,
					nodegroup:   poolZoneB,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       5*time.Minute + 1*time.Second,
					nodegroup:   poolZoneA,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       5*time.Minute + 1*time.Second,
					nodegroup:   poolZoneB,
					errorInfo:   stockoutError,
					wantBackoff: true,
				},
				{
					op:          opTypeIsBackedOff,
					tplus:       6*time.Minute + 1*time.Second,
					nodegroup:   poolZoneB,
					errorInfo:   stockoutError,
					wantBackoff: false,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			crds := []npc_crd.CRD{}
			crds = append(crds, npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-1"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()))
			crds = append(crds, npc_crd.NewTestCrd(npc_crd.WithLabel(testCrdLabel),
				npc_crd.WithName("crd-object-2"),
				npc_crd.WithRules([]npc_rules.Rule{
					npc_rules.NewMachineSpecRule(&machineFamily, nil, nil, nil),
					npc_rules.NewMachineSpecRule(&otherFamily, nil, nil, nil),
				}), npc_crd.WithScaleUpAnyway()))
			lister := npc_lister.NewMockCrdLister(crds)
			lister.SetCrdLabel(testCrdLabel)
			provider := &mockProvider{}
			provider.On("IsAutopilotEnabled").Return(false)
			backoff := NewNpcCrdBackoff(duration, lister, provider)

			for opIndx, op := range tc.operations {
				// Validate that intermediate RemoveStaleBackoffData calls have no impact on tests
				backoff.RemoveStaleBackoffData(now.Add(op.tplus))
				errInfo := op.errorInfo
				if errInfo.ErrorCode == "" {
					errInfo = quotaError
				}
				switch op.op {
				case opTypeBackoff:
					until := backoff.Backoff(op.nodegroup, nil, errInfo, now.Add(op.tplus))
					want := now.Add(op.tplus)
					if op.wantBackoff {
						want = want.Add(duration)
					}
					if !until.Equal(want) {
						t.Errorf("Backoff() returned time %v, expected %v", until, want)
					}
				case opTypeIsBackedOff:
					want := base_backoff.Status{IsBackedOff: op.wantBackoff}
					if op.wantBackoff {
						want.ErrorInfo = errInfo
					}
					assert.Equal(t, want, backoff.BackoffStatus(op.nodegroup, nil, now.Add(op.tplus)), fmt.Sprintf("Unexpected backoff status after operation %v", opIndx))
				}
			}
			// Verify stale data is eventually cleaned-up
			backoff.RemoveStaleBackoffData(now.Add(time.Hour))
			if len(backoff.backoffs) != 0 {
				t.Errorf("Not all backoff data cleaned at the end of the test")
			}
		})
	}
}
