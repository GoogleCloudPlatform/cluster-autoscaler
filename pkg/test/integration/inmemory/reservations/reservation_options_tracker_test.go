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

package reservations

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestReservationGeneratorOptionsTracker verifies that ReservationGenerator correctly
// picks up configuration overrides dynamically from OptionsTracker across various
// combinations of static CLI flags and experiment overrides.
func TestReservationGeneratorOptionsTracker(t *testing.T) {
	const (
		testReservationName = "res-1"
		testPodName         = "res-pod"
	)

	testCases := []struct {
		name                        string
		flagMatchEnabled            bool
		flagWithoutMatchEnabled     bool
		flagAnyLocationOverride     bool
		boolExperiments             map[string]bool
		stringExperiments           map[string]string
		hasMatchingReservationInGCE bool
		wantScaleUp                 bool
		wantLocationPolicyAny       bool
	}{
		{
			name:                    "flags_all_false,_experiments_override_all_to_true_with_matching_reservation_in_GCE",
			flagMatchEnabled:        false,
			flagWithoutMatchEnabled: false,
			flagAnyLocationOverride: false,
			boolExperiments: map[string]bool{
				experiments.SpecificTypeReservationMatchEnabledFlag:          true,
				experiments.SpecificTypeReservationWithoutMatchEnabledFlag:   true,
				experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag: true,
			},
			stringExperiments: map[string]string{
				experiments.SpecificTypeReservationMatchMinCAVersionFlag:          "0.0.0",
				experiments.SpecificTypeReservationWithoutMatchMinCAVersionFlag:   "0.0.0",
				experiments.ReservationsAnyLocationPolicyOverrideMinCAVersionFlag: "0.0.0",
			},
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 true,
			wantLocationPolicyAny:       true,
		},
		{
			name:                    "flags_all_false,_experiments_enable_match_without_matching_reservation_in_GCE_->_scaleup_rejected",
			flagMatchEnabled:        false,
			flagWithoutMatchEnabled: false,
			flagAnyLocationOverride: false,
			boolExperiments: map[string]bool{
				experiments.SpecificTypeReservationMatchEnabledFlag:        true,
				experiments.SpecificTypeReservationWithoutMatchEnabledFlag: false,
			},
			stringExperiments: map[string]string{
				experiments.SpecificTypeReservationMatchMinCAVersionFlag: "0.0.0",
			},
			hasMatchingReservationInGCE: false,
			wantScaleUp:                 false,
			wantLocationPolicyAny:       false,
		},
		{
			name:                    "flags_all_true,_experiments_override_all_to_false_->_scaleup_rejected",
			flagMatchEnabled:        true,
			flagWithoutMatchEnabled: true,
			flagAnyLocationOverride: true,
			boolExperiments: map[string]bool{
				experiments.SpecificTypeReservationMatchEnabledFlag:          false,
				experiments.SpecificTypeReservationWithoutMatchEnabledFlag:   false,
				experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag: false,
			},
			stringExperiments: map[string]string{
				experiments.SpecificTypeReservationMatchMinCAVersionFlag:          "0.0.0",
				experiments.SpecificTypeReservationWithoutMatchMinCAVersionFlag:   "0.0.0",
				experiments.ReservationsAnyLocationPolicyOverrideMinCAVersionFlag: "0.0.0",
			},
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 false,
			wantLocationPolicyAny:       false,
		},
		{
			name:                    "without_match_mode_enabled_via_experiment_allows_scaleup_without_GCE_reservation",
			flagMatchEnabled:        false,
			flagWithoutMatchEnabled: false,
			flagAnyLocationOverride: false,
			boolExperiments: map[string]bool{
				experiments.SpecificTypeReservationMatchEnabledFlag:          false,
				experiments.SpecificTypeReservationWithoutMatchEnabledFlag:   true,
				experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag: true,
			},
			stringExperiments: map[string]string{
				experiments.SpecificTypeReservationWithoutMatchMinCAVersionFlag:   "0.0.0",
				experiments.ReservationsAnyLocationPolicyOverrideMinCAVersionFlag: "0.0.0",
			},
			hasMatchingReservationInGCE: false,
			wantScaleUp:                 true,
			wantLocationPolicyAny:       true,
		},
		{
			name:                    "flags_true,_experiment_disables_location_policy_override_only",
			flagMatchEnabled:        true,
			flagWithoutMatchEnabled: false,
			flagAnyLocationOverride: true,
			boolExperiments: map[string]bool{
				experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag: false,
			},
			stringExperiments: map[string]string{
				experiments.ReservationsAnyLocationPolicyOverrideMinCAVersionFlag: "0.0.0",
			},
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 true,
			wantLocationPolicyAny:       false,
		},
		{
			name:                    "min_ca_version_unsupported_falls_back_to_static_flags",
			flagMatchEnabled:        true,
			flagWithoutMatchEnabled: false,
			flagAnyLocationOverride: true,
			boolExperiments: map[string]bool{
				experiments.SpecificTypeReservationMatchEnabledFlag:          false,
				experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag: false,
			},
			stringExperiments: map[string]string{
				experiments.SpecificTypeReservationMatchMinCAVersionFlag:          "99.0.0",
				experiments.ReservationsAnyLocationPolicyOverrideMinCAVersionFlag: "99.0.0",
			},
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 true,
			wantLocationPolicyAny:       true,
		},
		{
			name:                        "flags_all_false,_no_experiments_->_scaleup_rejected",
			flagMatchEnabled:            false,
			flagWithoutMatchEnabled:     false,
			flagAnyLocationOverride:     false,
			boolExperiments:             nil,
			stringExperiments:           nil,
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 false,
			wantLocationPolicyAny:       false,
		},
		{
			name:                        "flags_all_true,_no_experiments_falls_back_to_flags_->_scaleup_succeeds",
			flagMatchEnabled:            true,
			flagWithoutMatchEnabled:     true,
			flagAnyLocationOverride:     true,
			boolExperiments:             nil,
			stringExperiments:           nil,
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 true,
			wantLocationPolicyAny:       true,
		},
		{
			name:                    "no_MinCAVersion_flag_set:_experiment_true_overrides_flag_false",
			flagMatchEnabled:        false,
			flagWithoutMatchEnabled: false,
			flagAnyLocationOverride: false,
			boolExperiments: map[string]bool{
				experiments.SpecificTypeReservationMatchEnabledFlag:          true,
				experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag: true,
			},
			stringExperiments:           nil,
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 true,
			wantLocationPolicyAny:       true,
		},
		{
			name:                    "no_MinCAVersion_flag_set:_experiment_false_overrides_flag_true",
			flagMatchEnabled:        true,
			flagWithoutMatchEnabled: false,
			flagAnyLocationOverride: true,
			boolExperiments: map[string]bool{
				experiments.SpecificTypeReservationMatchEnabledFlag:          false,
				experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag: false,
			},
			stringExperiments:           nil,
			hasMatchingReservationInGCE: true,
			wantScaleUp:                 false,
			wantLocationPolicyAny:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithBalanceSimilarNodeGroups(),
					integration.WithSpecificTypeReservationMatchEnabled(tc.flagMatchEnabled),
					integration.WithSpecificTypeReservationWithoutMatchEnabled(tc.flagWithoutMatchEnabled),
					integration.WithReservationsAnyLocationPolicyOverride(tc.flagAnyLocationOverride),
				).
				WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled())

			if tc.boolExperiments != nil || tc.stringExperiments != nil {
				testConfig.WithExperimentOverrides(tc.boolExperiments, tc.stringExperiments)
			}

			if tc.hasMatchingReservationInGCE {
				testConfig.AddReservation(integration.DefaultProject(), reservations.New(testReservationName, reservationZone, reservations.WithMachine("n1-standard-4"), reservations.WithProject("test-project"), reservations.WithCounts(0, 2)))
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				pod := tu.BuildTestPod(testPodName, 3000, 10000, tu.MarkUnschedulable())
				pod.Spec.NodeSelector = map[string]string{
					gkelabels.ReservationNameLabel:     testReservationName,
					gkelabels.ReservationAffinityLabel: "specific",
				}
				infra.Fakes.K8s.AddPod(pod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
				assert.NoError(t, err)

				if tc.wantScaleUp {
					assert.Equal(t, 1, len(cluster.NodePools), "Expected exactly 1 NAP node pool to be created")
					np := cluster.NodePools[0]
					assert.NotNil(t, np.Config, "Expected node pool config to be non-nil")
					assert.NotNil(t, np.Config.ReservationAffinity, "Expected node pool reservation affinity to be non-nil")
					assert.Equal(t, gkeclient.ReservationAffinitySpecific, np.Config.ReservationAffinity.ConsumeReservationType)
					assert.Equal(t, gkeclient.ReservationNameKey, np.Config.ReservationAffinity.Key)
					assert.Equal(t, []string{testReservationName}, np.Config.ReservationAffinity.Values)

					if tc.wantLocationPolicyAny {
						assert.NotNil(t, np.Autoscaling, "Expected node pool autoscaling config to be non-nil")
						assert.Equal(t, "ANY", np.Autoscaling.LocationPolicy, "Expected LocationPolicy to be ANY")
					} else {
						if np.Autoscaling != nil {
							assert.NotEqual(t, "ANY", np.Autoscaling.LocationPolicy, "Expected LocationPolicy NOT to be ANY")
						}
					}

					nodeGroups := autoscaler.CloudProvider.NodeGroups(t.Context())
					assert.NotEmpty(t, nodeGroups, "Expected node groups to be registered with the cloud provider")
					totalTargetSize := 0
					for _, ng := range nodeGroups {
						size, err := ng.TargetSize(t.Context())
						assert.NoError(t, err)
						totalTargetSize += size
					}
					assert.Equal(t, 1, totalTargetSize, "Expected total target size across node groups to scale up to 1")
				} else {
					assert.Equal(t, 0, len(cluster.NodePools), "Expected no NAP node pool to be created")
					assert.Empty(t, autoscaler.CloudProvider.NodeGroups(t.Context()), "Expected no node groups to be registered")
				}
			})
		})
	}
}
