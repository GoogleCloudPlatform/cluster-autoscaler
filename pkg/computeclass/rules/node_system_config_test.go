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

package rules

import (
	"fmt"
	"testing"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	"k8s.io/utils/ptr"
)

func TestNodeSystemConfig(t *testing.T) {
	nonDefaultMachineFamilyName := machinetypes.N2.Name()
	nonDefaultMachineType := fmt.Sprintf("%s-standard-8", nonDefaultMachineFamilyName)

	cpuCfsQuota := true
	cpuCfsQuotaPeriod := "100ms"
	cpuManagerPolicy := "none"
	podPidsLimit := int64(1024)
	imageGcLowThresholdPercent := int64(10)
	imageGcHighThresholdPercent := int64(85)
	imageMinimumGcAge := "90s"
	imageMaximumGcAge := "1h30s"
	containerLogMaxFiles := int64(8)
	containerLogMaxSize := "20000Ki"
	allowedUnsafeSysctls := []string{"kernel.shm*", "kernel.msg*", "kernel.sem", "fs.mqueue.*", "net.*"}
	maxParallelImagePulls := int64(5)
	singleProcessOOMKill := true

	hugepageSize1g := int64(3)
	hugepageSize2m := int64(1024)
	thpEnabledAlways := "TRANSPARENT_HUGEPAGE_ENABLED_ALWAYS"
	thpEnabledNever := "TRANSPARENT_HUGEPAGE_ENABLED_NEVER"
	thpDefragAlways := "TRANSPARENT_HUGEPAGE_DEFRAG_ALWAYS"
	thpDefragDefer := "TRANSPARENT_HUGEPAGE_DEFRAG_DEFER_WITH_MADVISE"

	evictionSoftMemoryAvailable := "200Mi"
	evictionSoftNodefsAvailable := "10%"
	evictionSoftImagefsAvailable := "15%"
	evictionSoftImagefsInodesFree := "5%"
	evictionSoftNodefsInodesFree := "5%"
	evictionSoftPidAvailable := "10%"

	evictionSoftGracePeriodMemoryAvailable := "1m30s"
	evictionSoftGracePeriodNodefsAvailable := "2m"
	evictionSoftGracePeriodImagefsAvailable := "2m30s"
	evictionSoftGracePeriodImagefsInodesFree := "3m"
	evictionSoftGracePeriodNodefsInodesFree := "3m30s"
	evictionSoftGracePeriodPidAvailable := "4m"

	evictionMinimumReclaimMemoryAvailable := "5%"
	evictionMinimumReclaimNodefsAvailable := "6%"
	evictionMinimumReclaimImagefsAvailable := "7%"
	evictionMinimumReclaimImagefsInodesFree := "8%"
	evictionMinimumReclaimNodefsInodesFree := "9%"
	evictionMinimumReclaimPidAvailable := "1%"
	evictionMaxPodGracePeriodSeconds := int64(120)

	topologyManagerPolicyBestEffort := "best-effort"
	topologyManagerPolicyRestricted := "restricted"
	topologyManagerScopeContainer := "container"
	topologyManagerScopePod := "pod"
	memoryManagerPolicyStatic := "Static"
	shutdownGracePeriodSeconds := int64(120)
	shutdownGracePeriodCriticalPodsSeconds := int64(60)

	localSSDCount := 1
	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      NodeSystemConfigRule
		expected  bool
	}{
		{
			name: "rule without sysctls, node group without sysctls - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: nil,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(nil),
			),
			expected: true,
		},
		{
			name: "rule with empty sysctls, node group without sysctls - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: nil,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{}),
			),
			expected: true,
		},
		{
			name: "rule with empty sysctls, node group with empty sysctls - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{}),
			),
			expected: true,
		},
		{
			name: "rule with empty sysctls, node group without linux node group config - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{}),
			),
			expected: true,
		},
		{
			name: "rule without linux node config, node group without linux node group config - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule without sysctls, node group with empty sysctls - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:     nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{}),
			),
			expected: true,
		},
		{
			name: "rule without sysctls, node group with sysctls - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{"foo": "bar"},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule with same sysctls as node group - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{"foo": "bar"},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{"foo": "bar"}),
			),
			expected: true,
		},
		{
			name: "rule with some sysctls as node group - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{"foo": "bar", "foo1": "bar1"},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{"foo": "bar"}),
			),
			expected: true,
		},
		{
			name: "rule without linux node config, node group with sysctls - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{"foo": "not-bar"},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule with different sysctls than node group - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{"foo": "not-bar"},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{"foo": "bar"}),
			),
			expected: false,
		},
		{
			name: "rule with sysctls, node group without linux node group config - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSysctlsRule(map[string]string{"foo": "bar"}),
			),
			expected: false,
		},
		{
			name: "rule with kubelet config, node group without kubelet config - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCpuCfsQuotaRule(cpuCfsQuota),
			),
			expected: false,
		},
		{
			name: "rule with kubelet config, node group with different kubelet config - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{"foo": "not-bar"},
				},
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuCfsQuota:       cpuCfsQuota,
					CpuCfsQuotaPeriod: cpuCfsQuotaPeriod,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCpuCfsQuotaRule(cpuCfsQuota),
				WithPodPidsLimitRule(podPidsLimit),
			),
			expected: false,
		},
		{
			name: "rule without kubelet config, node group with cpuCfsQuota true - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuCfsQuota:       true,
					CpuCfsQuotaPeriod: cpuCfsQuotaPeriod,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule without kubelet config, node group with cpuCfsQuota false - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuCfsQuota:       false,
					CpuCfsQuotaPeriod: cpuCfsQuotaPeriod,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule with kubelet config, node group with same kubelet config - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuCfsQuota:                 cpuCfsQuota,
					CpuCfsQuotaPeriod:           cpuCfsQuotaPeriod,
					CpuManagerPolicy:            cpuManagerPolicy,
					ImageGcLowThresholdPercent:  imageGcLowThresholdPercent,
					ImageGcHighThresholdPercent: imageGcHighThresholdPercent,
					ImageMinimumGcAge:           imageMinimumGcAge,
					ImageMaximumGcAge:           imageMaximumGcAge,
					ContainerLogMaxFiles:        containerLogMaxFiles,
					ContainerLogMaxSize:         containerLogMaxSize,
					AllowedUnsafeSysctls:        allowedUnsafeSysctls,
					MaxParallelImagePulls:       maxParallelImagePulls,
					SingleProcessOomKill:        singleProcessOOMKill,
					EvictionSoft: &gke_api_beta.EvictionSignals{
						MemoryAvailable:   evictionSoftMemoryAvailable,
						NodefsAvailable:   evictionSoftNodefsAvailable,
						ImagefsAvailable:  evictionSoftImagefsAvailable,
						ImagefsInodesFree: evictionSoftImagefsInodesFree,
						NodefsInodesFree:  evictionSoftNodefsInodesFree,
						PidAvailable:      evictionSoftPidAvailable,
					},
					EvictionSoftGracePeriod: &gke_api_beta.EvictionGracePeriod{
						MemoryAvailable:   evictionSoftGracePeriodMemoryAvailable,
						NodefsAvailable:   evictionSoftGracePeriodNodefsAvailable,
						ImagefsAvailable:  evictionSoftGracePeriodImagefsAvailable,
						ImagefsInodesFree: evictionSoftGracePeriodImagefsInodesFree,
						NodefsInodesFree:  evictionSoftGracePeriodNodefsInodesFree,
						PidAvailable:      evictionSoftGracePeriodPidAvailable,
					},
					EvictionMinimumReclaim: &gke_api_beta.EvictionMinimumReclaim{
						MemoryAvailable:   evictionMinimumReclaimMemoryAvailable,
						NodefsAvailable:   evictionMinimumReclaimNodefsAvailable,
						ImagefsAvailable:  evictionMinimumReclaimImagefsAvailable,
						ImagefsInodesFree: evictionMinimumReclaimImagefsInodesFree,
						NodefsInodesFree:  evictionMinimumReclaimNodefsInodesFree,
						PidAvailable:      evictionMinimumReclaimPidAvailable,
					},
					EvictionMaxPodGracePeriodSeconds: evictionMaxPodGracePeriodSeconds,
					TopologyManager: &gke_api_beta.TopologyManager{
						Policy: topologyManagerPolicyBestEffort,
						Scope:  topologyManagerScopeContainer,
					},
					MemoryManager: &gke_api_beta.MemoryManager{
						Policy: memoryManagerPolicyStatic,
					},
					ShutdownGracePeriodSeconds:             shutdownGracePeriodSeconds,
					ShutdownGracePeriodCriticalPodsSeconds: shutdownGracePeriodCriticalPodsSeconds,
					CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
						MaxContainerRestartPeriod: "10s",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCpuCfsQuotaRule(cpuCfsQuota),
				WithCpuCfsQuotaPeriodRule(cpuCfsQuotaPeriod),
				WithCpuManagerPolicyRule(cpuManagerPolicy),
				WithImageGcLowThresholdPercentRule(imageGcLowThresholdPercent),
				WithImageGcHighThresholdPercentRule(imageGcHighThresholdPercent),
				WithImageMinimumGcAgeRule(imageMinimumGcAge),
				WithImageMaximumGcAgeRule(imageMaximumGcAge),
				WithContainerLogMaxFilesRule(containerLogMaxFiles),
				WithContainerLogMaxSizeRule(containerLogMaxSize),
				WithAllowedUnsafeSysctlsRule(allowedUnsafeSysctls),
				WithMaxParallelImagePullsRule(maxParallelImagePulls),
				WithSingleProcessOOMKill(singleProcessOOMKill),
				WithEvictionSoftMemoryAvailableRule(evictionSoftMemoryAvailable),
				WithEvictionSoftNodefsAvailableRule(evictionSoftNodefsAvailable),
				WithEvictionSoftImagefsAvailableRule(evictionSoftImagefsAvailable),
				WithEvictionSoftImagefsInodesFreeRule(evictionSoftImagefsInodesFree),
				WithEvictionSoftNodefsInodesFreeRule(evictionSoftNodefsInodesFree),
				WithEvictionSoftPidAvailableRule(evictionSoftPidAvailable),
				WithEvictionSoftGracePeriodMemoryAvailableRule(evictionSoftGracePeriodMemoryAvailable),
				WithEvictionSoftGracePeriodNodefsAvailableRule(evictionSoftGracePeriodNodefsAvailable),
				WithEvictionSoftGracePeriodImagefsAvailableRule(evictionSoftGracePeriodImagefsAvailable),
				WithEvictionSoftGracePeriodImagefsInodesFreeRule(evictionSoftGracePeriodImagefsInodesFree),
				WithEvictionSoftGracePeriodNodefsInodesFreeRule(evictionSoftGracePeriodNodefsInodesFree),
				WithEvictionSoftGracePeriodPidAvailableRule(evictionSoftGracePeriodPidAvailable),
				WithEvictionMinimumReclaimMemoryAvailableRule(evictionMinimumReclaimMemoryAvailable),
				WithEvictionMinimumReclaimNodefsAvailableRule(evictionMinimumReclaimNodefsAvailable),
				WithEvictionMinimumReclaimImagefsAvailableRule(evictionMinimumReclaimImagefsAvailable),
				WithEvictionMinimumReclaimImagefsInodesFreeRule(evictionMinimumReclaimImagefsInodesFree),
				WithEvictionMinimumReclaimNodefsInodesFreeRule(evictionMinimumReclaimNodefsInodesFree),
				WithEvictionMinimumReclaimPidAvailableRule(evictionMinimumReclaimPidAvailable),
				WithEvictionMaxPodGracePeriodSecondsRule(evictionMaxPodGracePeriodSeconds),
				WithTopologyManagerPolicyRule(topologyManagerPolicyBestEffort),
				WithTopologyManagerScopeRule(topologyManagerScopeContainer),
				WithMemoryManagerPolicyRule(memoryManagerPolicyStatic),
				WithShutdownGracePeriodSecondsRule(shutdownGracePeriodSeconds),
				WithShutdownGracePeriodCriticalPodsSecondsRule(shutdownGracePeriodCriticalPodsSeconds),
				WithCrashLoopBackOffMaxContainerRestartPeriodRule("10s"),
			),
			expected: true,
		},
		{
			name: "rule without kubelet config, node group with kubelet config - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuCfsQuota:                 cpuCfsQuota,
					CpuCfsQuotaPeriod:           cpuCfsQuotaPeriod,
					CpuManagerPolicy:            cpuManagerPolicy,
					ImageGcHighThresholdPercent: imageGcHighThresholdPercent,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCpuCfsQuotaRule(cpuCfsQuota),
			),
			expected: true,
		},
		{
			name: "rule with subset of kubelet configs as in node group - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuCfsQuota:       cpuCfsQuota,
					CpuCfsQuotaPeriod: cpuCfsQuotaPeriod,
					CpuManagerPolicy:  cpuManagerPolicy,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule with hugepages, node group without hugepages - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithHugepageSize1gRule(hugepageSize1g),
				WithHugepageSize2mRule(hugepageSize2m),
			),
			expected: false,
		},
		{
			name: "rule with hugepages, node group with different hugepages - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Hugepages: &gkeclient.HugepagesConfig{
						HugepageSize1g: 100000,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithHugepageSize1gRule(hugepageSize1g),
			),
			expected: false,
		},
		{
			name: "rule with hugepages, node group with same hugepages - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Hugepages: &gkeclient.HugepagesConfig{
						HugepageSize1g: hugepageSize1g,
						HugepageSize2m: hugepageSize2m,
					},
					TransparentHugepageEnabled: thpEnabledAlways,
					TransparentHugepageDefrag:  thpDefragAlways,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithHugepageSize1gRule(hugepageSize1g),
				WithHugepageSize2mRule(hugepageSize2m),
				WithTransparentHugepageEnabledRule(thpEnabledAlways),
				WithTransparentHugepageDefragRule(thpDefragAlways),
			),
			expected: true,
		},
		{
			name: "rule with subset of hugepages as in node group - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Hugepages: &gkeclient.HugepagesConfig{
						HugepageSize1g: hugepageSize1g,
						HugepageSize2m: hugepageSize2m,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithHugepageSize2mRule(hugepageSize2m),
			),
			expected: true,
		},
		{
			name: "rule without hugepages, node group with hugepages - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Hugepages: &gkeclient.HugepagesConfig{
						HugepageSize1g: hugepageSize1g,
						HugepageSize2m: hugepageSize2m,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule with transparent hugepage, node group with different linux node group config - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					TransparentHugepageEnabled: thpEnabledNever,
					TransparentHugepageDefrag:  thpDefragDefer,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithTransparentHugepageEnabledRule(thpEnabledAlways),
				WithTransparentHugepageDefragRule(thpDefragAlways),
			),
			expected: false,
		},
		{
			name: "rule with accurate time config, node group with same - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					AccurateTimeConfig: &gkeclient.AccurateTimeConfig{
						EnablePtpKvmTimeSync: true,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithAccurateTimeConfigRule(ccc_api.AccurateTimeConfig{
					EnablePtpKvmTimeSync: ptr.To(true),
				}),
			),
			expected: true,
		},
		{
			name: "rule without accurate time config, node group with - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					AccurateTimeConfig: &gkeclient.AccurateTimeConfig{
						EnablePtpKvmTimeSync: true,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithAccurateTimeConfigRule(ccc_api.AccurateTimeConfig{}),
			),
			expected: false,
		},
		{
			name: "rule with accurate time config, node group without - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					AccurateTimeConfig: &gkeclient.AccurateTimeConfig{},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithAccurateTimeConfigRule(ccc_api.AccurateTimeConfig{
					EnablePtpKvmTimeSync: ptr.To(true),
				}),
			),
			expected: false,
		},
		{
			name: "rule with accurate time config, node group with different - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					AccurateTimeConfig: &gkeclient.AccurateTimeConfig{
						EnablePtpKvmTimeSync: true,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithAccurateTimeConfigRule(ccc_api.AccurateTimeConfig{
					EnablePtpKvmTimeSync: ptr.To(false),
				}),
			),
			expected: false,
		},
		{
			name: "rule with eviction soft, node group with same eviction soft - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					EvictionSoft: &gke_api_beta.EvictionSignals{
						MemoryAvailable:   evictionSoftMemoryAvailable,
						NodefsAvailable:   evictionSoftNodefsAvailable,
						ImagefsAvailable:  evictionSoftImagefsAvailable,
						ImagefsInodesFree: evictionSoftImagefsInodesFree,
						NodefsInodesFree:  evictionSoftNodefsInodesFree,
						PidAvailable:      evictionSoftPidAvailable,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithEvictionSoftMemoryAvailableRule(evictionSoftMemoryAvailable),
				WithEvictionSoftNodefsAvailableRule(evictionSoftNodefsAvailable),
				WithEvictionSoftImagefsAvailableRule(evictionSoftImagefsAvailable),
				WithEvictionSoftImagefsInodesFreeRule(evictionSoftImagefsInodesFree),
				WithEvictionSoftNodefsInodesFreeRule(evictionSoftNodefsInodesFree),
				WithEvictionSoftPidAvailableRule(evictionSoftPidAvailable),
			),
			expected: true,
		},
		{
			name: "rule with eviction soft, node group with different eviction soft - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					EvictionSoft: &gke_api_beta.EvictionSignals{
						MemoryAvailable: "500Mi",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithEvictionSoftMemoryAvailableRule(evictionSoftMemoryAvailable),
			),
			expected: false,
		},
		{
			name: "rule with eviction soft, node group without eviction soft - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:   nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithEvictionSoftMemoryAvailableRule(evictionSoftMemoryAvailable),
			),
			expected: false,
		},
		{
			name: "rule with full eviction config, node group with same eviction config - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					EvictionSoft: &gke_api_beta.EvictionSignals{
						MemoryAvailable:   evictionSoftMemoryAvailable,
						NodefsAvailable:   evictionSoftNodefsAvailable,
						ImagefsAvailable:  evictionSoftImagefsAvailable,
						ImagefsInodesFree: evictionSoftImagefsInodesFree,
						NodefsInodesFree:  evictionSoftNodefsInodesFree,
						PidAvailable:      evictionSoftPidAvailable,
					},
					EvictionSoftGracePeriod: &gke_api_beta.EvictionGracePeriod{
						MemoryAvailable:   evictionSoftGracePeriodMemoryAvailable,
						NodefsAvailable:   evictionSoftGracePeriodNodefsAvailable,
						ImagefsAvailable:  evictionSoftGracePeriodImagefsAvailable,
						ImagefsInodesFree: evictionSoftGracePeriodImagefsInodesFree,
						NodefsInodesFree:  evictionSoftGracePeriodNodefsInodesFree,
						PidAvailable:      evictionSoftGracePeriodPidAvailable,
					},
					EvictionMinimumReclaim: &gke_api_beta.EvictionMinimumReclaim{
						MemoryAvailable:   evictionMinimumReclaimMemoryAvailable,
						NodefsAvailable:   evictionMinimumReclaimNodefsAvailable,
						ImagefsAvailable:  evictionMinimumReclaimImagefsAvailable,
						ImagefsInodesFree: evictionMinimumReclaimImagefsInodesFree,
						NodefsInodesFree:  evictionMinimumReclaimNodefsInodesFree,
						PidAvailable:      evictionMinimumReclaimPidAvailable,
					},
					EvictionMaxPodGracePeriodSeconds:       evictionMaxPodGracePeriodSeconds,
					ShutdownGracePeriodSeconds:             shutdownGracePeriodSeconds,
					ShutdownGracePeriodCriticalPodsSeconds: shutdownGracePeriodCriticalPodsSeconds,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithEvictionSoftMemoryAvailableRule(evictionSoftMemoryAvailable),
				WithEvictionSoftNodefsAvailableRule(evictionSoftNodefsAvailable),
				WithEvictionSoftImagefsAvailableRule(evictionSoftImagefsAvailable),
				WithEvictionSoftImagefsInodesFreeRule(evictionSoftImagefsInodesFree),
				WithEvictionSoftNodefsInodesFreeRule(evictionSoftNodefsInodesFree),
				WithEvictionSoftPidAvailableRule(evictionSoftPidAvailable),
				WithEvictionSoftGracePeriodMemoryAvailableRule(evictionSoftGracePeriodMemoryAvailable),
				WithEvictionSoftGracePeriodNodefsAvailableRule(evictionSoftGracePeriodNodefsAvailable),
				WithEvictionSoftGracePeriodImagefsAvailableRule(evictionSoftGracePeriodImagefsAvailable),
				WithEvictionSoftGracePeriodImagefsInodesFreeRule(evictionSoftGracePeriodImagefsInodesFree),
				WithEvictionSoftGracePeriodNodefsInodesFreeRule(evictionSoftGracePeriodNodefsInodesFree),
				WithEvictionSoftGracePeriodPidAvailableRule(evictionSoftGracePeriodPidAvailable),
				WithEvictionMinimumReclaimMemoryAvailableRule(evictionMinimumReclaimMemoryAvailable),
				WithEvictionMinimumReclaimNodefsAvailableRule(evictionMinimumReclaimNodefsAvailable),
				WithEvictionMinimumReclaimImagefsAvailableRule(evictionMinimumReclaimImagefsAvailable),
				WithEvictionMinimumReclaimImagefsInodesFreeRule(evictionMinimumReclaimImagefsInodesFree),
				WithEvictionMinimumReclaimNodefsInodesFreeRule(evictionMinimumReclaimNodefsInodesFree),
				WithEvictionMinimumReclaimPidAvailableRule(evictionMinimumReclaimPidAvailable),
				WithEvictionMaxPodGracePeriodSecondsRule(evictionMaxPodGracePeriodSeconds),
				WithShutdownGracePeriodSecondsRule(shutdownGracePeriodSeconds),
				WithShutdownGracePeriodCriticalPodsSecondsRule(shutdownGracePeriodCriticalPodsSeconds),
			),
			expected: true,
		},
		{
			name: "rule with topology manager, node group with same topology manager - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					TopologyManager: &gke_api_beta.TopologyManager{
						Policy: topologyManagerPolicyBestEffort,
						Scope:  topologyManagerScopeContainer,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithTopologyManagerPolicyRule(topologyManagerPolicyBestEffort),
				WithTopologyManagerScopeRule(topologyManagerScopeContainer),
			),
			expected: true,
		},
		{
			name: "rule with topology manager, node group with different topology manager - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					TopologyManager: &gke_api_beta.TopologyManager{
						Policy: topologyManagerPolicyRestricted,
						Scope:  topologyManagerScopePod,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithTopologyManagerPolicyRule(topologyManagerPolicyBestEffort),
				WithTopologyManagerScopeRule(topologyManagerScopeContainer),
			),
			expected: false,
		},
		{
			name: "rule with topology manager, node group with same policy but different scope - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					TopologyManager: &gke_api_beta.TopologyManager{
						Policy: topologyManagerPolicyBestEffort,
						Scope:  topologyManagerScopePod,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithTopologyManagerPolicyRule(topologyManagerPolicyBestEffort),
				WithTopologyManagerScopeRule(topologyManagerScopeContainer),
			),
			expected: false,
		},
		{
			name: "rule with topology manager, node group without topology manager - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:   nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithTopologyManagerPolicyRule(topologyManagerPolicyBestEffort),
			),
			expected: false,
		},
		{
			name: "rule with memory manager, node group with same memory manager - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					MemoryManager: &gke_api_beta.MemoryManager{
						Policy: memoryManagerPolicyStatic,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMemoryManagerPolicyRule(memoryManagerPolicyStatic),
			),
			expected: true,
		},
		{
			name: "rule with memory manager, node group with different memory manager - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					MemoryManager: &gke_api_beta.MemoryManager{
						Policy: "None",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMemoryManagerPolicyRule(memoryManagerPolicyStatic),
			),
			expected: false,
		},
		{
			name: "rule with memory manager, node group without memory manager - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:   nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMemoryManagerPolicyRule(memoryManagerPolicyStatic),
			),
			expected: false,
		},
		{
			name: "rule with memory manager policy None, node group without memory manager - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:   nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMemoryManagerPolicyRule("None"),
			),
			expected: true,
		},
		{
			name: "rule with memory manager policy None, node group with memory manager policy None - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					MemoryManager: &gke_api_beta.MemoryManager{Policy: "None"},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMemoryManagerPolicyRule("None"),
			),
			expected: true,
		},
		{
			name: "rule with swap disabled, node group with swap enabled, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: false,
				}),
			),
			expected: false,
		},
		{
			name: "rule with swap enabled, node group without swap enabled, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
				}),
			),
			expected: false,
		},
		{
			name: "rule without swap enabled, node group same, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: false,
				}),
			),
			expected: true,
		},
		{
			name: "rule with swap enabled on boot disk, node group same, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						BootDiskProfile: &gkeclient.BootDiskProfile{
							SwapSizeGib: 10,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
			expected: true,
		},
		{
			name: "rule with swap enabled on boot disk, node group has diff size, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						BootDiskProfile: &gkeclient.BootDiskProfile{
							SwapSizeGib: 20,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
			expected: false,
		},
		{
			name: "rule with swap enabled on boot disk, node group has diff profile, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						EphemeralLocalSsdProfile: &gkeclient.EphemeralLocalSsdProfile{
							SwapSizeGib: 10,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
			expected: false,
		},
		{
			name: "rule with swap enabled on boot disk encrypted, node group not encrypted, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: true,
						},
						BootDiskProfile: &gkeclient.BootDiskProfile{
							SwapSizeGib: 10,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
			expected: false,
		},
		{
			name: "rule with swap enabled on boot disk not encrypted, node group encrypted, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						BootDiskProfile: &gkeclient.BootDiskProfile{
							SwapSizeGib: 10,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					EncryptionConfig: &ccc_api.SwapConfigEncryptionConfig{
						Disabled: true,
					},
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
			expected: false,
		},
		{
			name: "rule with swap enabled on ephemeral storage LSSD, node group same, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						EphemeralLocalSsdProfile: &gkeclient.EphemeralLocalSsdProfile{
							SwapSizeGib: 10,
						},
					},
				},
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{
						LocalSsdCount: 1,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					EphemeralLocalSsdProfile: &ccc_api.SwapConfigEphemeralLocalSsdProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
				WithStorageRule(nil, nil, nil, &localSSDCount),
			),
			expected: true,
		},
		{
			name: "rule with swap enabled on ephemeral storage LSSD, node group diff size, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						EphemeralLocalSsdProfile: &gkeclient.EphemeralLocalSsdProfile{
							SwapSizeGib: 20,
						},
					},
				},
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{
						LocalSsdCount: 1,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					EphemeralLocalSsdProfile: &ccc_api.SwapConfigEphemeralLocalSsdProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
				WithStorageRule(nil, nil, nil, &localSSDCount),
			),
			expected: false,
		},
		{
			name: "rule with swap enabled on dedicated LSSD, node group same, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 2,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
				WithStorageRule(nil, nil, nil, nil),
			),
			expected: true,
		},
		{
			name: "rule with swap enabled on dedicated LSSD with ephemeral storage, node group same, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{
						LocalSsdCount: 1,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 1,
					},
				}),
				WithStorageRule(nil, nil, nil, &localSSDCount),
			),
			expected: true,
		},
		{
			name: "rule with swap enabled on dedicated LSSD, node group has diff count, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
			expected: false,
		},
		{
			name: "rule with swap enabled on dedicated LSSD, node group on boot disk, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						EncryptionConfig: &gkeclient.EncryptionConfig{
							Disabled: false,
						},
						BootDiskProfile: &gkeclient.BootDiskProfile{
							SwapSizeGib: 10,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
			expected: false,
		},
		{
			name: "rule with crashLoopBackOff, node group with same crashLoopBackOff - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
						MaxContainerRestartPeriod: "10s",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCrashLoopBackOffMaxContainerRestartPeriodRule("10s"),
			),
			expected: true,
		},
		{
			name: "rule with crashLoopBackOff, node group with different max container restart period - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
						MaxContainerRestartPeriod: "10s",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCrashLoopBackOffMaxContainerRestartPeriodRule("20s"),
			),
			expected: false,
		},
		{
			name: "rule with crashLoopBackOff, node group without crashLoopBackOff - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:   nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCrashLoopBackOffMaxContainerRestartPeriodRule("10s"),
			),
			expected: false,
		},
		{
			name: "rule without crashLoopBackOff, node group with crashLoopBackOff - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
						MaxContainerRestartPeriod: "10s",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule with AdditionalEtcHosts - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					AdditionalEtcHosts: []*gkeclient.EtcHostsEntry{
						{Ip: "1.2.3.4", Host: "host1"},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithAdditionalEtcHostsRule([]*EtcHostsEntry{
					{Ip: "1.2.3.4", Host: "host1"},
				}),
			),
			expected: true,
		},
		{
			name: "rule with AdditionalEtcResolvConf - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					AdditionalEtcResolvConf: []*gkeclient.ResolvedConfEntry{
						{Key: "nameserver", Value: []string{"8.8.8.8"}},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithAdditionalEtcResolvConfRule([]*ResolvedConfEntry{
					{Key: "nameserver", Value: []string{"8.8.8.8"}},
				}),
			),
			expected: true,
		},
		{
			name: "rule with AdditionalEtcSystemdResolvedConf - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					AdditionalEtcSystemdResolvedConf: []*gkeclient.ResolvedConfEntry{
						{Key: "DNS", Value: []string{"8.8.4.4"}},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithAdditionalEtcSystemdResolvedConfRule([]*ResolvedConfEntry{
					{Key: "DNS", Value: []string{"8.8.4.4"}},
				}),
			),
			expected: true,
		},
		{
			name: "rule with CustomNodeInit - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					CustomNodeInit: &gkeclient.CustomNodeInit{
						InitScript: &gkeclient.InitScript{
							GcsUri: "gs://bucket/script.sh",
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithCustomNodeInitRule(&CustomNodeInit{
					InitScript: &InitScript{
						GcsUri: ptr.To("gs://bucket/script.sh"),
					},
				}),
			),
			expected: true,
		},
		{
			name: "rule with KernelOverrides - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					KernelOverrides: &gkeclient.KernelOverrides{
						KernelCommandlineOverrides: &gkeclient.KernelCommandlineOverrides{
							SpecRstackOverflow: "OFF",
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithKernelOverridesRule(&KernelOverrides{
					KernelCommandlineOverrides: &KernelCommandlineOverrides{
						SpecRstackOverflow: ptr.To("OFF"),
					},
				}),
			),
			expected: true,
		},
		{
			name: "rule with TimeZone - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					TimeZone: "UTC",
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithTimeZoneRule("UTC"),
			),
			expected: true,
		},
		{
			name: "rule with NodeVfioConfig - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					NodeVfioConfig: &gkeclient.NodeVfioConfig{
						DmaEntryLimit: 65536,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithNodeVfioConfigRule(ccc_api.NodeVfioConfig{
					DmaEntryLimit: ptr.To(int32(65536)),
				}),
			),
			expected: true,
		},
		{
			name: "rule with NodeVfioConfig - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					NodeVfioConfig: &gkeclient.NodeVfioConfig{
						DmaEntryLimit: 100000,
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithNodeVfioConfigRule(ccc_api.NodeVfioConfig{
					DmaEntryLimit: ptr.To(int32(65536)),
				}),
			),
			expected: false,
		},
		{
			name: "rule with DiskIoScheduler - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					DiskIoScheduler: &gkeclient.DiskIoScheduler{
						NodeSystemIoScheduler:       "mq-deadline",
						NodeAttachedDiskIoScheduler: "bfq",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithDiskIoSchedulerRule(ccc_api.DiskIoScheduler{
					NodeSystemIoScheduler:       "mq-deadline",
					NodeAttachedDiskIoScheduler: "bfq",
				}),
			),
			expected: true,
		},
		{
			name: "rule with DiskIoScheduler - non matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					DiskIoScheduler: &gkeclient.DiskIoScheduler{
						NodeSystemIoScheduler:       "kyber",
						NodeAttachedDiskIoScheduler: "none",
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithDiskIoSchedulerRule(ccc_api.DiskIoScheduler{
					NodeSystemIoScheduler:       "mq-deadline",
					NodeAttachedDiskIoScheduler: "bfq",
				}),
			),
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.rule.Matches(tc.nodegroup)
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}
