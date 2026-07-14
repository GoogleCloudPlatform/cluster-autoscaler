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

package ccc

import (
	"fmt"
	"os"
	"testing"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/utils/ptr"
)

func TestMain(m *testing.M) {
	selfservice.InitSelfService(nil)
	os.Exit(m.Run())
}

func TestNewCccCrd(t *testing.T) {
	familyName := "e2"
	mppn := 85
	count := 25
	podFamily := "general-purpose"

	testCases := []struct {
		name             string
		ccc              *v1.ComputeClass
		cccProject       string
		autopilotEnabled bool
		optsModifier     func(*internalopts.InternalOptions)
		wantCrd          crd.CRD
	}{
		{
			name: "nil ccc",
			ccc:  nil,
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
			),
		},
		{
			name: "ccc name",
			ccc: &v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ccc",
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithName("test-ccc"),
			),
		},
		{
			name: "ccc rules",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							Spot:        proto.Bool(true),
							MinMemoryGb: &count,
						},
						{
							Nodepools: []string{"np1", "np2"},
						},
						{
							MachineFamily: &familyName,
							MinCores:      &count,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(nil, proto.Bool(true), nil, &count),
					rules.NewRule(rules.WithNodePoolsRule([]string{"np1", "np2"})),
					rules.NewMachineSpecRule(&familyName, nil, &count, nil),
				}),
			),
		},
		{
			name: "ccc nap enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						Enabled: true,
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithAutoprovisioningEnabled(),
			),
		},
		{
			name: "ccc nap disabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						Enabled: false,
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
			),
		},
		{
			name: "ccc scale up with general purpose enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					WhenUnsatisfiable: scaleUpAnyway,
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithScaleUpAnyway(),
			),
		},
		{
			name: "ccc scale up with general pupose disabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					WhenUnsatisfiable: doNotScaleUp,
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
			),
		},
		{
			name: "ccc defrag enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					ActiveMigration: &v1.ActiveMigration{
						OptimizeRulePriority: true,
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithOptimizeRulePriority(),
			),
		},
		{
			name: "ccc defrag disabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					ActiveMigration: &v1.ActiveMigration{
						OptimizeRulePriority: false,
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
			),
		},
		{
			name: "ccc mppn set",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{MaxPodsPerNode: &mppn},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules(
					[]rules.Rule{
						rules.NewRule(
							rules.WithMaxPodsPerNodeRule(&mppn),
						),
					},
				),
			),
		},
		{
			name: "ccc conditions",
			ccc: &v1.ComputeClass{
				Status: v1.ComputeClassStatus{
					Conditions: []metav1.Condition{
						{Message: "Hello from"},
						{Message: "the other side."},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithConditions([]metav1.Condition{
					{Message: "Hello from"},
					{Message: "the other side."},
				}),
			),
		},
		{
			name: "ccc no reservations",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{},
								Affinity: v1.SpecificAffinity,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
					),
				}),
			),
		},
		{
			name: "ccc with specific reservations",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{
									{
										Name: "local",
										ReservationBlock: &v1.ReservationBlock{
											Name: "",
										},
									},
									{
										Name:    "shared",
										Project: "other-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "",
										},
									},
									{
										Name:    "with-block",
										Project: "other-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "res-block",
										},
									},
									{
										Name:    "with-sub-block",
										Project: "other-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "res-block",
											ReservationSubBlock: &v1.ReservationSubBlock{
												Name: "res-sub-block",
											},
										},
									},
								},
								Affinity: v1.SpecificAffinity,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{rules.NewRule(
					rules.WithMachineFamilyRule(&familyName),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("local").
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationPath("local")),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("shared").
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject("other-project").
						WithReservationPath("projects/other-project/reservations/shared")),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("with-block").
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject("other-project").
						WithReservationPath("projects/other-project/reservations/with-block/reservationBlocks/res-block").
						WithReservationBlock("res-block")),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("with-sub-block").
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject("other-project").
						WithReservationPath("projects/other-project/reservations/with-sub-block/reservationBlocks/res-block/reservationSubBlocks/res-sub-block").
						WithReservationBlock("res-block").
						WithReservationSubBlock("res-sub-block")),
				)},
				),
			),
		},
		{
			name: "ccc with specific reservations with zones",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{
									{
										Name:  "local",
										Zones: []string{"us-central1-a"},
										ReservationBlock: &v1.ReservationBlock{
											Name: "",
										},
									},
									{
										Name:    "shared",
										Project: "other-project",
										Zones:   []string{"us-central1-a", "us-central1-b"},
										ReservationBlock: &v1.ReservationBlock{
											Name: "",
										},
									},
									{
										Name:    "with-block",
										Project: "other-project",
										Zones:   []string{"us-central1-a", "us-central1-b"},
										ReservationBlock: &v1.ReservationBlock{
											Name: "res-block",
										},
									},
									{
										Name:    "with-sub-block",
										Project: "other-project",
										Zones:   []string{"us-central1-a", "us-central1-b"},
										ReservationBlock: &v1.ReservationBlock{
											Name: "res-block",
											ReservationSubBlock: &v1.ReservationSubBlock{
												Name: "res-sub-block",
											},
										},
									},
								},
								Affinity: v1.SpecificAffinity,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{rules.NewRule(
					rules.WithMachineFamilyRule(&familyName),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("local").
						WithReservationZones([]string{"us-central1-a"}).
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationPath("local")),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("shared").
						WithReservationZones([]string{"us-central1-a", "us-central1-b"}).
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject("other-project").
						WithReservationPath("projects/other-project/reservations/shared")),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("with-block").
						WithReservationZones([]string{"us-central1-a", "us-central1-b"}).
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject("other-project").
						WithReservationPath("projects/other-project/reservations/with-block/reservationBlocks/res-block").
						WithReservationBlock("res-block")),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("with-sub-block").
						WithReservationZones([]string{"us-central1-a", "us-central1-b"}).
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject("other-project").
						WithReservationPath("projects/other-project/reservations/with-sub-block/reservationBlocks/res-block/reservationSubBlocks/res-sub-block").
						WithReservationBlock("res-block").
						WithReservationSubBlock("res-sub-block")),
				)},
				),
			),
		},
		{
			name:       "ccc with local reservation in same project",
			cccProject: "cluster-project",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{
									{
										Name:    "local",
										Project: "cluster-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "",
										},
									},
								},
								Affinity: v1.SpecificAffinity,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{rules.NewRule(
					rules.WithMachineFamilyRule(&familyName),
					rules.WithReservationsRule(rules.NewReservation().
						WithReservationName("local").
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject("cluster-project").
						WithReservationPath("local")),
				)},
				),
			),
		},
		{
			name:       "ccc with any reservations with specific not considered",
			cccProject: "cluster-project",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{
									{
										Name:    "local",
										Project: "cluster-project",
									},
								},
								Affinity: v1.AnyBestEffortAffinity,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{rules.NewRule(
					rules.WithMachineFamilyRule(&familyName),
					rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)),
				)},
				),
			),
		},
		{
			name:       "ccc with none reservation affinity",
			cccProject: "cluster-project",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Affinity: v1.NoneAffinity,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{rules.NewRule(
					rules.WithMachineFamilyRule(&familyName),
					rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.NoneAffinity)),
				)},
				),
			),
		},
		{
			name:       "ccc with none reservation affinity with specific not considered",
			cccProject: "cluster-project",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{
									{
										Name:    "local",
										Project: "cluster-project",
									},
								},
								Affinity: v1.NoneAffinity,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{rules.NewRule(
					rules.WithMachineFamilyRule(&familyName),
					rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.NoneAffinity)),
				)},
				),
			),
		},
		{
			name:       "ccc with any-then-fail affinity",
			cccProject: "cluster-project",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{
									{
										Name:    "local",
										Project: "cluster-project",
									},
								},
								Affinity: v1.AnyThenFail,
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{rules.NewRule(
					rules.WithMachineFamilyRule(&familyName),
					rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyThenFail)),
				)},
				),
			),
		},
		{
			name: "ccc autopilot managed enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
					Priorities: []v1.Priority{
						{
							PodFamily: &podFamily,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&podFamily),
					),
				}),
				crd.WithAutopilotManaged(),
				crd.WithDynamicBootDiskSizeEnabled(),   // This setting is enabled for CCC by default if autopilot flag is true
				crd.WithDynamicMaxPodsPerNodeEnabled(), // This setting is enabled for CCC by default if autopilot flag is true
			),
		},
		{
			name: "ccc autopilot managed disabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: false,
					},
					Priorities: []v1.Priority{
						{
							PodFamily: &podFamily,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithPodFamilyRule(&podFamily),
					),
				}),
			),
		},
		{
			name: "node system config in rule takes priority over default",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									Sysctls: &v1.SysctlsConfig{
										Net_core_netdev_max_backlog: int64Ptr(1234),
									},
								},
							},
						},
					},
					PriorityDefaults: &v1.PriorityDefaults{
						NodeSystemConfig: &v1.NodeSystemConfig{
							LinuxNodeConfig: &v1.LinuxNodeConfig{
								Sysctls: &v1.SysctlsConfig{
									Vm_max_map_count: int64Ptr(262144),
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
						rules.WithSysctlsRule(map[string]string{"net.core.netdev_max_backlog": "1234"}),
					),
				}),
			),
		},
		{
			name: "node system config in rule with kubelet config takes priority over default with only sysctls",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							NodeSystemConfig: &v1.NodeSystemConfig{
								KubeletConfig: &v1.KubeletConfig{
									CpuCfsQuotaPeriod: stringPtr("10ms"),
								},
							},
						},
					},
					PriorityDefaults: &v1.PriorityDefaults{
						NodeSystemConfig: &v1.NodeSystemConfig{
							LinuxNodeConfig: &v1.LinuxNodeConfig{
								Sysctls: &v1.SysctlsConfig{
									Vm_max_map_count: int64Ptr(262144),
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
						rules.WithCpuCfsQuotaPeriodRule("10ms"),
					),
				}),
			),
		},
		{
			name: "node system config in rule with hugespages takes priority over default with only sysctls",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									Hugepages: &v1.HugepagesConfig{
										HugepageSize1g: int64Ptr(3),
									},
								},
							},
						},
					},
					PriorityDefaults: &v1.PriorityDefaults{
						NodeSystemConfig: &v1.NodeSystemConfig{
							LinuxNodeConfig: &v1.LinuxNodeConfig{
								Sysctls: &v1.SysctlsConfig{
									Vm_max_map_count: int64Ptr(262144),
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
						rules.WithHugepageSize1gRule(3),
					),
				}),
			),
		},
		{
			name: "default node system config not present",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									Sysctls: &v1.SysctlsConfig{
										Net_core_netdev_max_backlog: int64Ptr(1234),
									},
								},
							},
						},
					},
					PriorityDefaults: &v1.PriorityDefaults{
						NodeSystemConfig: &v1.NodeSystemConfig{},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
						rules.WithSysctlsRule(map[string]string{"net.core.netdev_max_backlog": "1234"}),
					),
				}),
			),
		},
		{
			name: "rule 2 without node system config gets default",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									Sysctls: &v1.SysctlsConfig{
										Net_core_netdev_max_backlog: int64Ptr(1234),
									},
								},
							},
						},
						{
							MachineFamily: &familyName,
						},
					},
					PriorityDefaults: &v1.PriorityDefaults{
						NodeSystemConfig: &v1.NodeSystemConfig{
							LinuxNodeConfig: &v1.LinuxNodeConfig{
								Sysctls: &v1.SysctlsConfig{
									Net_core_rmem_max: int64Ptr(5678),
								},
							},
							KubeletConfig: &v1.KubeletConfig{
								CpuCfsQuota:       boolPtr(true),
								CpuCfsQuotaPeriod: stringPtr("10ms"),
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
						rules.WithSysctlsRule(map[string]string{"net.core.netdev_max_backlog": "1234"}),
					),
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
						rules.WithSysctlsRule(map[string]string{"net.core.rmem_max": "5678"}),
						rules.WithCpuCfsQuotaRule(true),
						rules.WithCpuCfsQuotaPeriodRule("10ms"),
					),
				}),
			),
		},
		{
			name: "only 1 rule with node system config, default absent",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									Sysctls: &v1.SysctlsConfig{
										Net_core_netdev_max_backlog: int64Ptr(1234),
									},
								},
							},
						},
						{
							MachineFamily: &familyName,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
						rules.WithSysctlsRule(map[string]string{"net.core.netdev_max_backlog": "1234"}),
					),
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
					),
				}),
			),
		},
		{
			name: "ccc with autopilot and dynamic boot disk size enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						DynamicBootDiskSize: proto.Bool(true),
					},
					Priorities: []v1.Priority{
						{
							PodFamily: &podFamily,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&podFamily),
					),
				}),
				crd.WithAutopilotManaged(),
				crd.WithDynamicBootDiskSizeEnabled(),
				crd.WithDynamicMaxPodsPerNodeEnabled(), // This setting is enabled for CCC by default if autopilot flag is true
			),
		},
		{
			name: "ccc with autopilot enabled and dynamic boot disk size disabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						DynamicBootDiskSize: proto.Bool(false),
					},
					Priorities: []v1.Priority{
						{
							PodFamily: &podFamily,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&podFamily),
					),
				}),
				crd.WithAutopilotManaged(),
				crd.WithDynamicMaxPodsPerNodeEnabled(), // This setting is enabled for CCC by default if autopilot flag is true
			),
		},
		{
			name: "ccc with autopilot disabled and dynamic boot disk size enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						DynamicBootDiskSize: proto.Bool(true),
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
			),
		},
		{
			name: "ccc with autopilot and dynamic max pods per node enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						DynamicMaxPodsPerNode: proto.Bool(true),
					},
					Priorities: []v1.Priority{
						{
							PodFamily: &podFamily,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&podFamily),
					),
				}),
				crd.WithAutopilotManaged(),
				crd.WithDynamicBootDiskSizeEnabled(), // This setting is enabled for CCC by default if autopilot flag is true
				crd.WithDynamicMaxPodsPerNodeEnabled(),
			),
		},
		{
			name: "ccc with autopilot enabled and dynamic max pods per node disabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						DynamicMaxPodsPerNode: proto.Bool(false),
					},
					Priorities: []v1.Priority{
						{
							PodFamily: &podFamily,
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&podFamily),
					),
				}),
				crd.WithAutopilotManaged(),
				crd.WithDynamicBootDiskSizeEnabled(), // This setting is enabled for CCC by default if autopilot flag is true
			),
		},
		{
			name: "ccc with autopilot disabled and dynamic max pods per node enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						DynamicMaxPodsPerNode: proto.Bool(true),
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
			),
		},
		{
			name:       "ccc with everything",
			cccProject: "cluster-project",
			ccc: &v1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ccc",
				},
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							Spot:        proto.Bool(true),
							MinMemoryGb: &count,
						},
						{
							Nodepools: []string{"np1", "np2"},
						},
						{
							MachineFamily: &familyName,
							MinCores:      &count,
						},
						{
							MachineFamily:  &familyName,
							MaxPodsPerNode: &mppn,
						},
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Specific: []v1.SpecificReservation{
									{
										Name:    "local",
										Project: "cluster-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "",
										},
									},
									{
										Name:    "shared",
										Project: "other-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "",
										},
									},
									{
										Name:    "with-block",
										Project: "other-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "res-block",
										},
									},
									{
										Name:    "with-sub-block",
										Project: "other-project",
										ReservationBlock: &v1.ReservationBlock{
											Name: "res-block",
											ReservationSubBlock: &v1.ReservationSubBlock{
												Name: "res-sub-block",
											},
										},
									},
								},
								Affinity: v1.SpecificAffinity,
							},
						},
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Affinity: v1.NoneAffinity,
							},
						},
						{
							MachineFamily: &familyName,
							Reservations: &v1.Reservations{
								Affinity: v1.AnyBestEffortAffinity,
							},
						},
					},
					NodePoolAutoCreation: &v1.NodePoolAutoCreation{
						Enabled: true,
					},
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
					ActiveMigration: &v1.ActiveMigration{
						OptimizeRulePriority: true,
					},
					WhenUnsatisfiable: scaleUpAnyway,
				},
				Status: v1.ComputeClassStatus{
					Conditions: []metav1.Condition{
						{Message: "Hello from"},
						{Message: "the other side."},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithName("test-ccc"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithSpotRule(proto.Bool(true)),
						rules.WithMinMemoryGbRule(&count),
					),
					rules.NewRule(
						rules.WithNodePoolsRule([]string{"np1", "np2"}),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithMachineFamilyRule(&familyName),
						rules.WithMinCoresRule(&count),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithMachineFamilyRule(&familyName),
						rules.WithMaxPodsPerNodeRule(&mppn),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithMachineFamilyRule(&familyName),
						rules.WithReservationsRule(rules.NewReservation().
							WithReservationName("local").
							WithReservationAffinity(reservations.SpecificAffinity).
							WithReservationProject("cluster-project").
							WithReservationPath("local")),
						rules.WithReservationsRule(rules.NewReservation().
							WithReservationName("shared").
							WithReservationAffinity(reservations.SpecificAffinity).
							WithReservationProject("other-project").
							WithReservationPath("projects/other-project/reservations/shared")),
						rules.WithReservationsRule(rules.NewReservation().
							WithReservationName("with-block").
							WithReservationAffinity(reservations.SpecificAffinity).
							WithReservationProject("other-project").
							WithReservationPath("projects/other-project/reservations/with-block/reservationBlocks/res-block").
							WithReservationBlock("res-block")),
						rules.WithReservationsRule(rules.NewReservation().
							WithReservationName("with-sub-block").
							WithReservationAffinity(reservations.SpecificAffinity).
							WithReservationProject("other-project").
							WithReservationPath("projects/other-project/reservations/with-sub-block/reservationBlocks/res-block/reservationSubBlocks/res-sub-block").
							WithReservationBlock("res-block").
							WithReservationSubBlock("res-sub-block")),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithMachineFamilyRule(&familyName),
						rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.NoneAffinity)),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithMachineFamilyRule(&familyName),
						rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)),
					),
				}),
				crd.WithAutoprovisioningEnabled(),
				crd.WithAutopilotManaged(),
				crd.WithDynamicBootDiskSizeEnabled(),   // This setting is enabled for CCC by default if autopilot flag is true
				crd.WithDynamicMaxPodsPerNodeEnabled(), // This setting is enabled for CCC by default if autopilot flag is true
				crd.WithScaleUpAnyway(),
				crd.WithOptimizeRulePriority(),
				crd.WithConditions([]metav1.Condition{
					{Message: "Hello from"},
					{Message: "the other side."},
				}),
			),
		},
		{
			name: "ccc nodePoolConfig passed to every rules",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						ServiceAccount: "test@1234.iam.gserviceaccount.com",
						ImageType:      "cos_containerd",
						NodeLabels:     map[string]string{"label-1": "value-1"},
					},
					Priorities: []v1.Priority{
						{MachineFamily: &familyName},
						{MachineFamily: &familyName},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
					),
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
					),
				}),
				crd.WithServiceAccount("test@1234.iam.gserviceaccount.com"),
				crd.WithImageType("cos_containerd"),
				crd.WithUserDefinedLabels(map[string]string{"label-1": "value-1"}),
			),
		},
		{
			name: "ccc nodeVersion passed to crd",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						NodeVersion: "1.32.9-gke.1726000",
					},
					Priorities: []v1.Priority{
						{MachineFamily: &familyName},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
					),
				}),
				crd.WithNodeVersion("1.32.9-gke.1726000"),
				crd.WithUserDefinedLabels(map[string]string{}),
				crd.WithUserDefinedTaints([]apiv1.Taint(nil)),
				crd.WithResourceManagerTags([]crd.Tag(nil)),
			),
		},
		{
			name: "ccc in autopilot cluster has autopilot managed rules",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
						},
						{
							MachineFamily: &familyName,
						},
					},
				},
			},
			autopilotEnabled: true,
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithMachineFamilyRule(&familyName),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithMachineFamilyRule(&familyName),
					),
				}),
			),
		},
		{
			name: "ccc with zonal preferences",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					PriorityDefaults: &v1.PriorityDefaults{
						Location: &v1.Location{
							Zones: []string{"zone4"},
						},
					},
					Priorities: []v1.Priority{
						{
							Location: &v1.Location{Zones: []string{"zone1", "zone2"}},
						},
						{
							Location: &v1.Location{Zones: []string{"zone3"}},
						},
						{},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithLocationRule([]string{"zone1", "zone2"}),
					),
					rules.NewRule(
						rules.WithLocationRule([]string{"zone3"}),
					),
					rules.NewRule(
						rules.WithLocationRule([]string{"zone4"}),
					),
				}),
			),
		},
		{
			name: "ccc with zone type preferences - zone types enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							Location: &v1.Location{ZoneTypes: []v1.ZoneType{"STANDARD", "AI"}},
						},
						{
							Location: &v1.Location{ZoneTypes: []v1.ZoneType{"CLUSTER_DEFAULT"}},
						},
					},
				},
			},
			optsModifier: func(io *internalopts.InternalOptions) { io.ZoneTypesEnabled = true },
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD", "AI"}, crd.TestDefaultDataProvider()),
					),
					rules.NewRule(
						rules.WithLocationZoneTypesRule([]v1.ZoneType{"CLUSTER_DEFAULT"}, crd.TestDefaultDataProvider()),
					),
				}),
			),
		},
		{
			name: "ccc with zone type preferences - zone types disabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("g2-standard-12"),
							Location:    &v1.Location{ZoneTypes: []v1.ZoneType{"STANDARD", "AI"}},
						},
					},
				},
			},
			optsModifier: func(io *internalopts.InternalOptions) { io.ZoneTypesEnabled = false },
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("g2-standard-12")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Count:            1,
							PhysicalGPUCount: 1,
							Config: machinetypes.GpuConfig{
								GpuType: "nvidia-l4",
							},
						}),
					),
				},
				)),
		},
		{
			name: "GPU machine type, no GPU request - GPU request defaulted",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("g2-standard-12"),
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("g2-standard-12")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Count:            1,
							PhysicalGPUCount: 1,
							Config: machinetypes.GpuConfig{
								GpuType: "nvidia-l4",
							},
						}),
					),
				}),
			),
		},
		{
			name: "GPU machine type, driver version set - defaults GPU type and count",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("g2-standard-12"),
							Gpu: &v1.GPU{
								DriverVersion: "latest",
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("g2-standard-12")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Count:            1,
							PhysicalGPUCount: 1,
							Config: machinetypes.GpuConfig{
								GpuType:       "nvidia-l4",
								DriverVersion: "latest",
							},
						}),
					),
				},
				),
			),
		},
		{
			name: "GPU machine type, autoinstall-disabled driver version set - cascades correctly",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("g2-standard-12"),
							Gpu: &v1.GPU{
								DriverVersion: "autoinstall-disabled",
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("g2-standard-12")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Count:            1,
							PhysicalGPUCount: 1,
							Config: machinetypes.GpuConfig{
								GpuType:       "nvidia-l4",
								DriverVersion: "autoinstall-disabled",
							},
						}),
					),
				}),
			),
		},
		{name: "GPU machine type, GPU request set - GPU request should not be overridden",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("g2-standard-12"),
							Gpu: &v1.GPU{
								Type:          "nvidia-tesla-a100",
								Count:         2,
								DriverVersion: "latest",
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("g2-standard-12")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Count:            2,
							PhysicalGPUCount: 2,
							Config: machinetypes.GpuConfig{
								GpuType:       "nvidia-tesla-a100",
								DriverVersion: "latest",
							},
						}),
					),
				},
				),
			),
		},
		{
			name: "CCC with user-defined node labels and taints",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						NodeLabels: map[string]string{
							"label-1": "label-key-1",
							"label-2": "label-key-2",
						},
						Taints: []v1.TaintConfig{
							{
								Key:    "taint-1",
								Value:  "taint-key-1",
								Effect: "NoSchedule",
							},
							{
								Key:    "taint-2",
								Value:  "taint-key-2",
								Effect: "NoExecute",
							},
							{
								Key:    "taint-3",
								Value:  "taint-key-3",
								Effect: "PreferNoSchedule",
							},
						},
					},
					Priorities: []v1.Priority{
						{
							NodeLabels: map[string]string{
								"p1-label-1": "p1-label-key-1",
								"p1-label-2": "p1-label-key-2",
							},
							Taints: []v1.TaintConfig{
								{
									Key:    "p1-taint-1",
									Value:  "p1-taint-key-1",
									Effect: "NoSchedule",
								},
								{
									Key:    "p1-taint-2",
									Value:  "p1-taint-key-2",
									Effect: "NoExecute",
								},
								{
									Key:    "p1-taint-3",
									Value:  "p1-taint-key-3",
									Effect: "PreferNoSchedule",
								},
							},
						},
						{
							NodeLabels: map[string]string{
								"p2-label-1": "p2-label-key-1",
								"p2-label-2": "p2-label-key-2",
							},
							Taints: []v1.TaintConfig{
								{
									Key:    "p2-taint-1",
									Value:  "p2-taint-key-1",
									Effect: "NoSchedule",
								},
								{
									Key:    "p2-taint-2",
									Value:  "p2-taint-key-2",
									Effect: "NoExecute",
								},
								{
									Key:    "p2-taint-3",
									Value:  "p2-taint-key-3",
									Effect: "PreferNoSchedule",
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithUserDefinedLabels(map[string]string{
					"label-1": "label-key-1",
					"label-2": "label-key-2",
				}),
				crd.WithUserDefinedTaints([]apiv1.Taint{
					{
						Key:    "taint-1",
						Value:  "taint-key-1",
						Effect: "NoSchedule",
					},
					{
						Key:    "taint-2",
						Value:  "taint-key-2",
						Effect: "NoExecute",
					},
					{
						Key:    "taint-3",
						Value:  "taint-key-3",
						Effect: "PreferNoSchedule",
					},
				}),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithLabelsRule(map[string]string{
							"p1-label-1": "p1-label-key-1",
							"p1-label-2": "p1-label-key-2",
						}),
						rules.WithTaintsRule([]apiv1.Taint{
							{
								Key:    "p1-taint-1",
								Value:  "p1-taint-key-1",
								Effect: "NoSchedule",
							},
							{
								Key:    "p1-taint-2",
								Value:  "p1-taint-key-2",
								Effect: "NoExecute",
							},
							{
								Key:    "p1-taint-3",
								Value:  "p1-taint-key-3",
								Effect: "PreferNoSchedule",
							},
						}),
					),
					rules.NewRule(
						rules.WithLabelsRule(map[string]string{
							"p2-label-1": "p2-label-key-1",
							"p2-label-2": "p2-label-key-2",
						}),
						rules.WithTaintsRule([]apiv1.Taint{
							{
								Key:    "p2-taint-1",
								Value:  "p2-taint-key-1",
								Effect: "NoSchedule",
							},
							{
								Key:    "p2-taint-2",
								Value:  "p2-taint-key-2",
								Effect: "NoExecute",
							},
							{
								Key:    "p2-taint-3",
								Value:  "p2-taint-key-3",
								Effect: "PreferNoSchedule",
							},
						}),
					),
				}),
			),
		},
		{
			name: "Resource policy set - Expected placement policy name",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("n2-standard-2"),
							Placement: &v1.Placement{
								PolicyName: "policy",
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("n2-standard-2")),
						rules.WithPlacementPolicyRule("policy"),
					),
				},
				),
			),
		},
		{
			name: "GPU sharing set, but empty fields - ignores the values",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("g2-standard-12"),
							Gpu: &v1.GPU{
								GpuSharing: &v1.GpuSharing{},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("g2-standard-12")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Count:            1,
							PhysicalGPUCount: 1,
							Config: machinetypes.GpuConfig{
								GpuType: "nvidia-l4",
							},
						}),
					),
				},
				),
			),
		},
		{
			name: "GPU sharing set - time based sharing",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("g2-standard-12"),
							Gpu: &v1.GPU{
								GpuSharing: &v1.GpuSharing{
									SharingStrategy:        "MPS",
									MaxSharedClientsPerGPU: 45,
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("g2-standard-12")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							PhysicalGPUCount: 1,
							Count:            45,
							Config: machinetypes.GpuConfig{
								GpuType:          "nvidia-l4",
								MaxSharedClients: "45",
								SharingStrategy:  "mps",
							},
						}),
					),
				},
				),
			),
		},
		{
			name: "GPU sharing set - partition based sharing",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("a2-highgpu-2g"),
							Gpu: &v1.GPU{
								GpuSharing: &v1.GpuSharing{
									GpuPartitionSize: "1g.5gb", // 7 partitions
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("a2-highgpu-2g")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							PhysicalGPUCount: 2,
							Count:            14, // 2 * 7 partitions
							Config: machinetypes.GpuConfig{
								GpuType:       "nvidia-tesla-a100",
								PartitionSize: "1g.5gb",
							},
						}),
					),
				},
				),
			),
		},
		{
			name: "GPU sharing set - time + partition sharing",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("a2-highgpu-4g"),
							Gpu: &v1.GPU{
								GpuSharing: &v1.GpuSharing{
									SharingStrategy:        "TIME_SHARING",
									MaxSharedClientsPerGPU: 31,
									GpuPartitionSize:       "2g.10gb", // 3 partitions
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("a2-highgpu-4g")),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							PhysicalGPUCount: 4,
							Count:            372, // 4 gpus * 3 partitions * 31 clients
							Config: machinetypes.GpuConfig{
								GpuType:          "nvidia-tesla-a100",
								PartitionSize:    "2g.10gb",
								MaxSharedClients: "31",
								SharingStrategy:  "time-sharing",
							},
						}),
					),
				},
				),
			),
		},
		{
			name: "CCC with swap enabled",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("n2-standard-2"),
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									SwapConfig: &v1.SwapConfig{
										Enabled: true,
									},
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("n2-standard-2")),
						rules.WithSwapConfigRule(v1.SwapConfig{
							Enabled: true,
						}),
					),
				}),
			),
		},
		{
			name: "CCC with swap enabled on ephemeral lssd profile",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("n2-standard-2"),
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									SwapConfig: &v1.SwapConfig{
										Enabled: true,
										EphemeralLocalSsdProfile: &v1.SwapConfigEphemeralLocalSsdProfile{
											SwapSizeGib: int64Ptr(10),
										},
									},
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("n2-standard-2")),
						rules.WithSwapConfigRule(v1.SwapConfig{
							Enabled: true,
							EphemeralLocalSsdProfile: &v1.SwapConfigEphemeralLocalSsdProfile{
								SwapSizeGib: int64Ptr(10),
							},
						}),
					),
				}),
			),
		},
		{
			name: "CCC with swap enabled on dedicated lssd profile ",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("c4-standard-32-lssd"),
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									SwapConfig: &v1.SwapConfig{
										Enabled: true,
										DedicatedLocalSsdProfile: &v1.SwapConfigDedicatedLocalSsdProfile{
											DiskCount: 2,
										},
									},
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("c4-standard-32-lssd")),
						rules.WithStorageRule(nil, nil, nil, nil),
						rules.WithSwapConfigRule(v1.SwapConfig{
							Enabled: true,
							DedicatedLocalSsdProfile: &v1.SwapConfigDedicatedLocalSsdProfile{
								DiskCount: 2,
							},
						}),
					),
				}),
			),
		},
		{
			name: "CCC with swap enabled on dedicated lssd profile + ephemeral storage local ssd",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("c4-standard-32-lssd"),
							Storage: &v1.Storage{
								LocalSSDCount: ptr.To(1),
							},
							NodeSystemConfig: &v1.NodeSystemConfig{
								LinuxNodeConfig: &v1.LinuxNodeConfig{
									SwapConfig: &v1.SwapConfig{
										Enabled: true,
										DedicatedLocalSsdProfile: &v1.SwapConfigDedicatedLocalSsdProfile{
											DiskCount: 1,
										},
									},
								},
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("c4-standard-32-lssd")),
						rules.WithStorageRule(nil, nil, nil, ptr.To(1)),
						rules.WithSwapConfigRule(v1.SwapConfig{
							Enabled: true,
							DedicatedLocalSsdProfile: &v1.SwapConfigDedicatedLocalSsdProfile{
								DiskCount: 1,
							},
						}),
					),
				}),
			),
		},
		{
			name: "ccc minimum capacity set",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					MinimumCapacity: &v1.MinimumCapacity{
						TargetNodeCount: intPtr(5),
					},
					Priorities: []v1.Priority{
						{
							MachineFamily: &familyName,
							MinimumCapacity: &v1.MinimumCapacity{
								TargetNodeCount: intPtr(3),
							},
						},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithTargetNodeCount(intPtr(5)),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineFamilyRule(stringPtr(familyName)), rules.WithTargetNodeCountRule(intPtr(3))),
				}),
			),
		},
		{
			name: "ccc taintConfig architectureTaintBehavior set",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						TaintConfig: &v1.NodePoolTaintConfig{
							ArchitectureTaintBehavior: "NONE",
						},
					},
					Priorities: []v1.Priority{
						{MachineFamily: &familyName},
					},
				},
			},
			wantCrd: crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithMachineFamilyRule(&familyName),
					),
				}),
				crd.WithArchitectureTaintBehavior("NONE"),
				crd.WithUserDefinedLabels(map[string]string{}),
				crd.WithUserDefinedTaints([]apiv1.Taint(nil)),
				crd.WithResourceManagerTags([]crd.Tag(nil)),
			),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ccc := NewCccCrd(tc.ccc, tc.cccProject, tc.autopilotEnabled, crd.TestDefaultDataProvider(), testOptionsTracker(tc.optsModifier))
			crd.CompareCrd(t, tc.wantCrd, ccc)
		})
	}
}

func TestCccAutoscalingPolicy(t *testing.T) {
	var (
		consolidationDelayMinutes        = 5
		scaleDownUnneededTime            = time.Duration(consolidationDelayMinutes) * time.Minute
		consolidationThreshold           = 50
		scaleDownUtilizationThreshold    = consolidationThreshold
		gpuConsolidationThreshold        = 60
		scaleDownGpuUtilizationThreshold = gpuConsolidationThreshold
	)

	tests := map[string]struct {
		policy                               *v1.AutoscalingPolicy
		wantScaleDownUnneededTime            *time.Duration
		wantScaleDownUtilizationThreshold    *int
		wantScaleDownGpuUtilizationThreshold *int
	}{
		"optional params": {
			policy:                               nil,
			wantScaleDownUnneededTime:            nil,
			wantScaleDownUtilizationThreshold:    nil,
			wantScaleDownGpuUtilizationThreshold: nil,
		},
		"unneeded time": {
			policy: &v1.AutoscalingPolicy{
				ConsolidationDelayMinutes: &consolidationDelayMinutes,
			},
			wantScaleDownUnneededTime:            &scaleDownUnneededTime,
			wantScaleDownUtilizationThreshold:    nil,
			wantScaleDownGpuUtilizationThreshold: nil,
		},
		"utilization time": {
			policy: &v1.AutoscalingPolicy{
				ConsolidationThreshold: &consolidationThreshold,
			},
			wantScaleDownUnneededTime:            nil,
			wantScaleDownUtilizationThreshold:    &scaleDownUtilizationThreshold,
			wantScaleDownGpuUtilizationThreshold: nil,
		},
		"gpu utilization time": {
			policy: &v1.AutoscalingPolicy{
				GPUConsolidationThreshold: &gpuConsolidationThreshold,
			},
			wantScaleDownUnneededTime:            nil,
			wantScaleDownUtilizationThreshold:    nil,
			wantScaleDownGpuUtilizationThreshold: &scaleDownGpuUtilizationThreshold,
		},
		"combined policy": {
			policy: &v1.AutoscalingPolicy{
				ConsolidationDelayMinutes: &consolidationDelayMinutes,
				ConsolidationThreshold:    &consolidationThreshold,
				GPUConsolidationThreshold: &gpuConsolidationThreshold,
			},
			wantScaleDownUnneededTime:            &scaleDownUnneededTime,
			wantScaleDownUtilizationThreshold:    &scaleDownUtilizationThreshold,
			wantScaleDownGpuUtilizationThreshold: &scaleDownGpuUtilizationThreshold,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			ccc := &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					AutoscalingPolicy: test.policy,
				},
			}

			gotCrd := NewCccCrd(ccc, "", false, nil, testOptionsTracker(nil))
			assert.Equal(t, gotCrd.ConsolidationDelay(), test.wantScaleDownUnneededTime)
			assert.Equal(t, gotCrd.ConsolidationThreshold(), test.wantScaleDownUtilizationThreshold)
			assert.Equal(t, gotCrd.GPUConsolidationThreshold(), test.wantScaleDownGpuUtilizationThreshold)
		})
	}
}

func TestCCCEnsureAllDaemonSetPodsRunning(t *testing.T) {
	trueVal := true
	falseVal := false

	tests := map[string]struct {
		ccc                               *v1.ComputeClass
		wantEnsureAllDaemonSetPodsRunning bool
	}{
		"defaults to false in unmanaged nodes": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: false,
					},
				},
			},
			wantEnsureAllDaemonSetPodsRunning: false,
		},
		"defaults to true in managed nodes": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
				},
			},
			wantEnsureAllDaemonSetPodsRunning: true,
		},
		"explicit setting takes precedence over the default for unmanaged node": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: false,
					},
					ActiveMigration: &v1.ActiveMigration{
						EnsureAllDaemonSetPodsRunning: &trueVal,
					},
				},
			},
			wantEnsureAllDaemonSetPodsRunning: true,
		},
		"explicit setting takes precedence over the default for managed node": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Autopilot: &v1.Autopilot{
						Enabled: true,
					},
					ActiveMigration: &v1.ActiveMigration{
						EnsureAllDaemonSetPodsRunning: &falseVal,
					},
				},
			},
			wantEnsureAllDaemonSetPodsRunning: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gotCrd := NewCccCrd(tc.ccc, "", false, nil, testOptionsTracker(nil))

			if got := gotCrd.EnsureAllDaemonSetPodsRunning(); got != tc.wantEnsureAllDaemonSetPodsRunning {
				t.Errorf("invalid EnsureAllDaemonSetPodsRunning, want: %t, got: %t", tc.wantEnsureAllDaemonSetPodsRunning, got)
			}
		})
	}
}

func TestPriorities(t *testing.T) {
	fleetStrategy := v1.AllocationStrategyFleetEfficiency
	costStrategy := v1.AllocationStrategyLowestCost

	nodeSystemConfigDefault := &v1.NodeSystemConfig{
		LinuxNodeConfig: &v1.LinuxNodeConfig{
			Sysctls: &v1.SysctlsConfig{
				Net_core_netdev_max_backlog: int64Ptr(1234),
			},
		},
	}
	nodeSystemConfigA := &v1.NodeSystemConfig{
		KubeletConfig: &v1.KubeletConfig{
			CpuCfsQuotaPeriod: stringPtr("10ms"),
		},
	}
	nodeSystemConfigB := &v1.NodeSystemConfig{
		LinuxNodeConfig: &v1.LinuxNodeConfig{
			Hugepages: &v1.HugepagesConfig{
				HugepageSize1g: int64Ptr(3),
			},
		},
	}

	locationConfigDefault := &v1.Location{
		Zones: []string{"zone1", "zone2"},
	}
	locationConfigA := &v1.Location{
		Zones: []string{"zone3"},
	}

	tests := map[string]struct {
		ccc                 *v1.ComputeClass
		exopectedPriorities []v1.Priority
	}{
		"ccc is nil": {
			ccc:                 nil,
			exopectedPriorities: nil,
		},
		"priorties are nil": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: nil,
				},
			},
			exopectedPriorities: nil,
		},
		"PriorityDefaults are nil": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: proto.String("m2"),
						},
					},
					PriorityDefaults: nil,
				},
			},
			exopectedPriorities: []v1.Priority{
				{
					MachineFamily: proto.String("m2"),
				},
			},
		},
		"node system config - fallback to default in some priorities": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: proto.String("m2"),
						},
						{
							Spot:             proto.Bool(true),
							NodeSystemConfig: nodeSystemConfigA,
						},
						{
							NodeSystemConfig: nodeSystemConfigB,
						},
						{
							Gpu: &v1.GPU{
								Type:  "abc",
								Count: 10,
							},
						},
						{
							Location: locationConfigA,
						},
					},
					PriorityDefaults: &v1.PriorityDefaults{
						NodeSystemConfig: nodeSystemConfigDefault,
						Location:         locationConfigDefault,
					},
				},
			},
			exopectedPriorities: []v1.Priority{
				{
					MachineFamily:    proto.String("m2"),
					NodeSystemConfig: nodeSystemConfigDefault,
					Location:         locationConfigDefault,
				},
				{
					Spot:             proto.Bool(true),
					NodeSystemConfig: nodeSystemConfigA,
					Location:         locationConfigDefault,
				},
				{
					NodeSystemConfig: nodeSystemConfigB,
					Location:         locationConfigDefault,
				},
				{
					Gpu: &v1.GPU{
						Type:  "abc",
						Count: 10,
					},
					NodeSystemConfig: nodeSystemConfigDefault,
					Location:         locationConfigDefault,
				},
				{
					NodeSystemConfig: nodeSystemConfigDefault,
					Location:         locationConfigA,
				},
			},
		},
		"allocation strategy defaults": {
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineFamily: proto.String("m2"), // OnDemand
						},
						{
							Spot: proto.Bool(true), // Spot
						},
						{
							FlexStart: &v1.FlexStart{Enabled: true}, // FlexStart
						},
						{
							MachineFamily:      proto.String("n2"),
							AllocationStrategy: &costStrategy, // Explicitly set, overrides default
						},
					},
					AllocationStrategyDefaults: &v1.AllocationStrategyDefaults{
						OnDemand:  &fleetStrategy,
						Spot:      &costStrategy,
						FlexStart: &fleetStrategy,
					},
				},
			},
			exopectedPriorities: []v1.Priority{
				{
					MachineFamily:      proto.String("m2"),
					AllocationStrategy: &fleetStrategy,
				},
				{
					Spot:               proto.Bool(true),
					AllocationStrategy: &costStrategy,
				},
				{
					FlexStart:          &v1.FlexStart{Enabled: true},
					AllocationStrategy: &fleetStrategy,
				},
				{
					MachineFamily:      proto.String("n2"),
					AllocationStrategy: &costStrategy,
				},
			},
		},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			crd := &cccCrd{
				ComputeClass: test.ccc,
			}
			assert.Equal(t, crd.priorities(), test.exopectedPriorities)
		})
	}
}
func TestCccSpecSelfService(t *testing.T) {
	testCases := []struct {
		name       string
		ccc        *v1.ComputeClass
		wantLabels map[string]string
	}{
		{
			name: "no self service labels",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{},
			},
		},
		{
			name: "self-service node group name",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolGroup: &v1.NodePoolGroup{
						Name: "test-name",
					},
				},
			},
			wantLabels: map[string]string{
				labels.NodePoolGroupNameLabel: "test-name",
			},
		},
		{
			name: "self-service workload type",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						WorkloadType: "HIGH_AVAILABILITY",
					},
				},
			},
			wantLabels: map[string]string{
				labels.WorkloadTypeLabel: "HIGH_AVAILABILITY",
			},
		},

		{
			name: "self-service dra net",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Dra: v1.Dra{
							Networking: v1.NetworkingDra{
								Enabled: true,
							},
						},
					},
				},
			},
			wantLabels: map[string]string{
				labels.DraNetNodeLabel: "true",
			},
		},
		{
			name: "All self-service labels",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolGroup: &v1.NodePoolGroup{
						Name: "test-name",
					},
					NodePoolConfig: &v1.NodePoolConfig{
						WorkloadType: "HIGH_AVAILABILITY",
						Dra: v1.Dra{
							Networking: v1.NetworkingDra{
								Enabled: true,
							},
						},
					},
				},
			},
			wantLabels: map[string]string{
				labels.NodePoolGroupNameLabel: "test-name",
				labels.WorkloadTypeLabel:      "HIGH_AVAILABILITY",
				labels.DraNetNodeLabel:        "true",
			},
		},
	}
	for _, tc := range testCases {
		gotCrd := NewCccCrd(tc.ccc, "", false, nil, testOptionsTracker(nil))
		assert.Equal(t, fmt.Sprint(tc.wantLabels), fmt.Sprint(gotCrd.SelfServiceMetadata()))
	}
}

func TestPrioritySelfService(t *testing.T) {
	testCases := []struct {
		name       string
		ccc        *v1.ComputeClass
		wantLabels []map[string]string
	}{
		{
			name: "no priority self service labels",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{},
			},
		},
		{
			name: "PriorityDefaults contains LocationPolicy BALANCED, but the 3-rd priority overrides it to ANY",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					PriorityDefaults: &v1.PriorityDefaults{
						Location: &v1.Location{
							LocationPolicy: proto.String("BALANCED"),
						},
					},
					Priorities: []v1.Priority{
						{MachineFamily: proto.String("n2")},
						{MachineFamily: proto.String("n1")},
						{MachineFamily: proto.String("n2"), Location: &v1.Location{LocationPolicy: proto.String("ANY")}},
					},
				},
			},
			wantLabels: []map[string]string{
				selfservice.Metadata{"location-policy": "BALANCED"},
				selfservice.Metadata{"location-policy": "BALANCED"},
				selfservice.Metadata{"location-policy": "ANY"},
			},
		},
	}
	for _, tc := range testCases {
		gotCrd := NewCccCrd(tc.ccc, "", false, nil, testOptionsTracker(nil))
		for i, rule := range gotCrd.Rules() {
			assert.Equal(t, tc.wantLabels[i], rule.SelfServiceMetadata())
		}
	}
}

func TestUserDefinedLabels(t *testing.T) {
	testCases := []struct {
		name       string
		ccc        *v1.ComputeClass
		wantLabels map[string]string
	}{
		{
			name: "no user defined labels",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{},
			},
			wantLabels: nil,
		},
		{
			name: "user defined labels",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						NodeLabels: map[string]string{
							"label-1": "label-1",
							"label-2": "label-2",
						},
					},
				},
			},
			wantLabels: map[string]string{
				"label-1": "label-1",
				"label-2": "label-2",
			},
		},
		{
			name: "user defined labels with system labels",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						NodeLabels: map[string]string{
							labels.ComputeClassLabel: "compute-class",
							"label-2":                "label-2",
						},
					},
				},
			},
			wantLabels: map[string]string{
				"label-2": "label-2",
			},
		},
	}
	for _, tc := range testCases {
		gotCrd := NewCccCrd(tc.ccc, "", false, nil, testOptionsTracker(nil))
		assert.Equal(t, tc.wantLabels, gotCrd.UserDefinedLabels())
	}
}

func TestUserDefinedTaints(t *testing.T) {
	testCases := []struct {
		name       string
		ccc        *v1.ComputeClass
		wantTaints []apiv1.Taint
	}{
		{
			name: "no user defined taints",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{},
			},
			wantTaints: nil,
		},
		{
			name: "user defined taints",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Taints: []v1.TaintConfig{
							{
								Key:    "taint-key-1",
								Value:  "taint-value-1",
								Effect: "NoSchedule",
							},
							{
								Key:    "taint-key-2",
								Value:  "taint-value-2",
								Effect: "NoExecute",
							},
						},
					},
				},
			},
			wantTaints: []apiv1.Taint{
				{
					Key:    "taint-key-1",
					Value:  "taint-value-1",
					Effect: "NoSchedule",
				},
				{
					Key:    "taint-key-2",
					Value:  "taint-value-2",
					Effect: "NoExecute",
				},
			},
		},
		{
			name: "user defined taints with system taints",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Taints: []v1.TaintConfig{
							{
								Key:    labels.MachineFamilyLabel,
								Value:  "compute-class",
								Effect: "NoSchedule",
							},
							{
								Key:    "taint-key-2",
								Value:  "taint-value-2",
								Effect: "NoExecute",
							},
						},
					},
				},
			},
			wantTaints: []apiv1.Taint{
				{
					Key:    "taint-key-2",
					Value:  "taint-value-2",
					Effect: "NoExecute",
				},
			},
		},
		{
			name: "user defined taints without effect",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Taints: []v1.TaintConfig{
							{
								Key:   "taint-key-1",
								Value: "taint-value-1",
							},
						},
					},
				},
			},
			wantTaints: []apiv1.Taint{
				{
					Key:   "taint-key-1",
					Value: "taint-value-1",
				},
			},
		},
		{
			name: "user defined taints with key only",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Taints: []v1.TaintConfig{
							{
								Key: "taint-key-1",
							},
						},
					},
				},
			},
			wantTaints: []apiv1.Taint{
				{
					Key: "taint-key-1",
				},
			},
		},
	}
	for _, tc := range testCases {
		gotCrd := NewCccCrd(tc.ccc, "", false, nil, testOptionsTracker(nil))
		assert.Equal(t, tc.wantTaints, gotCrd.UserDefinedTaints())
	}
}

func TestGroupedRules(t *testing.T) {
	testCases := []struct {
		name             string
		ccc              *v1.ComputeClass
		wantGroupedRules [][]rules.Rule
	}{
		{
			name: "Priorities with priority score",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType:   stringPtr("e2-standard-4"),
							PriorityScore: intPtr(10),
						},
						{
							MachineFamily: stringPtr("n2"),
							PriorityScore: intPtr(20),
						},
						{
							MachineFamily: stringPtr("c3"),
							PriorityScore: intPtr(10),
						},
					},
				},
			},
			wantGroupedRules: [][]rules.Rule{
				{
					rules.NewRule(
						rules.WithMachineFamilyRule(stringPtr("n2")),
					),
				},
				{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("e2-standard-4")),
					),
					rules.NewRule(
						rules.WithMachineFamilyRule(stringPtr("c3")),
					),
				},
			},
		},
		{
			name: "Priorities without priority score",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType: stringPtr("e2-standard-4"),
						},
						{
							MachineFamily: stringPtr("n2"),
						},
						{
							MachineFamily: stringPtr("c3"),
						},
					},
				},
			},
			wantGroupedRules: [][]rules.Rule{
				{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("e2-standard-4")),
					),
				},
				{
					rules.NewRule(
						rules.WithMachineFamilyRule(stringPtr("n2")),
					),
				},
				{
					rules.NewRule(
						rules.WithMachineFamilyRule(stringPtr("c3")),
					),
				},
			},
		},
		{
			name: "Mixed priorities (should never happen) - considered without priority score based",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					Priorities: []v1.Priority{
						{
							MachineType:   stringPtr("e2-standard-4"),
							PriorityScore: intPtr(5),
						},
						{
							MachineFamily: stringPtr("n2"),
							PriorityScore: intPtr(5),
						},
						{
							MachineFamily: stringPtr("c3"),
						},
					},
				},
			},
			wantGroupedRules: [][]rules.Rule{
				{
					rules.NewRule(
						rules.WithMachineTypeRule(stringPtr("e2-standard-4")),
					),
				},
				{
					rules.NewRule(
						rules.WithMachineFamilyRule(stringPtr("n2")),
					),
				},
				{
					rules.NewRule(
						rules.WithMachineFamilyRule(stringPtr("c3")),
					),
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crd := NewCccCrd(tc.ccc, "", false, crd.TestDefaultDataProvider(), testOptionsTracker(nil))
			gotGroupedRules := crd.GroupedRules()
			assert.Equal(t, len(tc.wantGroupedRules), len(gotGroupedRules))
			for i, wantRules := range tc.wantGroupedRules {
				assert.ElementsMatch(t, wantRules, gotGroupedRules[i])
			}
		})
	}
}

func TestTpuDriverMode(t *testing.T) {
	testCases := []struct {
		name              string
		ccc               *v1.ComputeClass
		wantTpuDriverMode crd.TpuDriverMode
	}{
		{
			name: "DefaultWhenNodePoolConfigIsNil",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{},
			},
			wantTpuDriverMode: crd.TpuDriverModeDevicePlugin,
		},
		{
			name: "DefaultWhenTpuConfigIsNil",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{},
				},
			},
			wantTpuDriverMode: crd.TpuDriverModeDevicePlugin,
		},
		{
			name: "ExplicitDevicePluginMode",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Tpu: v1.GoogleTpu{
							DriverMode: v1.TpuDriverModeDevicePlugin,
						},
					},
				},
			},
			wantTpuDriverMode: crd.TpuDriverModeDevicePlugin,
		},
		{
			name: "ExplicitDRAMode",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Tpu: v1.GoogleTpu{
							DriverMode: v1.TpuDriverModeDynamicResourceAllocation,
						},
					},
				},
			},
			wantTpuDriverMode: crd.TpuDriverModeDynamicResourceAllocation,
		},
		{
			name: "UnknownModeDefaultsToDevicePlugin",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Tpu: v1.GoogleTpu{
							DriverMode: "unknown-mode",
						},
					},
				},
			},
			wantTpuDriverMode: crd.TpuDriverModeDevicePlugin,
		},
		{
			name: "UnsetModeDefaultsToDevicePlugin",
			ccc: &v1.ComputeClass{
				Spec: v1.ComputeClassSpec{
					NodePoolConfig: &v1.NodePoolConfig{
						Tpu: v1.GoogleTpu{},
					},
				},
			},
			wantTpuDriverMode: crd.TpuDriverModeDevicePlugin,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ccc := NewCccCrd(tc.ccc, "", false, nil, testOptionsTracker(nil))
			assert.Equal(t, tc.wantTpuDriverMode, ccc.TpuDriverMode())
		})
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func stringPtr(v string) *string {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(i int) *int {
	return &i
}

func testOptionsTracker(modifier func(opts *internalopts.InternalOptions)) *optstracking.OptionsTracker {
	opts := internalopts.InternalOptions{}
	if modifier != nil {
		modifier(&opts)
	}

	return optstracking.FakeOptionsTracker(internalopts.AutoscalingOptions{
		InternalOptions: opts,
	}, gkeclient.Cluster{}, experiments.NewMockManager())
}
