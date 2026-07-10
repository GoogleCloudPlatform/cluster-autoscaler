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
	"testing"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestRuleOptsForNodeSystemConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		nodeSystemConfig *ccc_api.NodeSystemConfig
		expected         []rules.RuleOption
	}{
		{
			desc:             "nil config",
			nodeSystemConfig: nil,
			expected:         nil,
		},
		{
			desc: "empty linux node systemconfig",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{},
			},
			expected: []rules.RuleOption{},
		},
		{
			desc: "full sysctls config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					Sysctls: &ccc_api.SysctlsConfig{
						Net_core_netdev_max_backlog:                        int64Ptr(1234),
						Net_core_rmem_max:                                  int64Ptr(5678),
						Net_core_wmem_default:                              int64Ptr(9101),
						Net_core_wmem_max:                                  int64Ptr(1213),
						Net_core_optmem_max:                                int64Ptr(1415),
						Net_core_somaxconn:                                 int64Ptr(1617),
						Net_ipv4_tcp_rmem:                                  stringPtr("4096 87380 16777216"),
						Net_ipv4_tcp_wmem:                                  stringPtr("4096 65536 16777216"),
						Net_ipv4_tcp_tw_reuse:                              int64Ptr(2),
						Net_core_busy_poll:                                 int64Ptr(0),
						Net_core_busy_read:                                 int64Ptr(1),
						Net_ipv6_conf_all_disable_ipv6:                     boolPtr(true),
						Net_ipv6_conf_default_disable_ipv6:                 boolPtr(false),
						Vm_max_map_count:                                   int64Ptr(262144),
						Kernel_shmmni:                                      int64Ptr(32768),
						Kernel_shmall:                                      stringPtr("18446744073692774399"),
						Kernel_shmmax:                                      stringPtr("0"),
						Net_core_rmem_default:                              int64Ptr(2147483647),
						Net_netfilter_nf_conntrack_max:                     int64Ptr(65536),
						Net_netfilter_nf_conntrack_buckets:                 int64Ptr(524288),
						Net_netfilter_nf_conntrack_acct:                    boolPtr(true),
						Net_netfilter_nf_conntrack_tcp_timeout_established: int64Ptr(86400),
						Net_netfilter_nf_conntrack_tcp_timeout_close_wait:  int64Ptr(60),
						Net_netfilter_nf_conntrack_tcp_timeout_time_wait:   int64Ptr(120),
						Vm_overcommit_memory:                               int64Ptr(1),
						Vm_overcommit_ratio:                                int64Ptr(50),
						Vm_vfs_cache_pressure:                              int64Ptr(100),
						Vm_dirty_background_ratio:                          int64Ptr(10),
						Vm_dirty_ratio:                                     int64Ptr(20),
						Vm_dirty_expire_centisecs:                          int64Ptr(3000),
						Vm_dirty_writeback_centisecs:                       int64Ptr(500),
						Fs_nr_open:                                         int64Ptr(1048576),
						Fs_inotify_max_user_watches:                        int64Ptr(8192),
						Fs_inotify_max_user_instances:                      int64Ptr(8192),
						Fs_aio_max_nr:                                      int64Ptr(65536),
						Fs_file_max:                                        int64Ptr(1048576),
						Net_ipv4_tcp_max_orphans:                           int64Ptr(16384),
						Vm_swappiness:                                      int64Ptr(200),
						Vm_watermark_scale_factor:                          int64Ptr(3000),
						Vm_min_free_kbytes:                                 int64Ptr(1048576),
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithSysctlsRule(map[string]string{
					"net.core.netdev_max_backlog":                        "1234",
					"net.core.rmem_max":                                  "5678",
					"net.core.wmem_default":                              "9101",
					"net.core.wmem_max":                                  "1213",
					"net.core.optmem_max":                                "1415",
					"net.core.somaxconn":                                 "1617",
					"net.ipv4.tcp_rmem":                                  "4096 87380 16777216",
					"net.ipv4.tcp_wmem":                                  "4096 65536 16777216",
					"net.ipv4.tcp_tw_reuse":                              "2",
					"net.core.busy_poll":                                 "0",
					"net.core.busy_read":                                 "1",
					"net.ipv6.conf.all.disable_ipv6":                     "1",
					"net.ipv6.conf.default.disable_ipv6":                 "0",
					"vm.max_map_count":                                   "262144",
					"kernel.shmmni":                                      "32768",
					"kernel.shmall":                                      "18446744073692774399",
					"kernel.shmmax":                                      "0",
					"net.core.rmem_default":                              "2147483647",
					"net.netfilter.nf_conntrack_max":                     "65536",
					"net.netfilter.nf_conntrack_buckets":                 "524288",
					"net.netfilter.nf_conntrack_acct":                    "1",
					"net.netfilter.nf_conntrack_tcp_timeout_established": "86400",
					"net.netfilter.nf_conntrack_tcp_timeout_close_wait":  "60",
					"net.netfilter.nf_conntrack_tcp_timeout_time_wait":   "120",
					"vm.overcommit_memory":                               "1",
					"vm.overcommit_ratio":                                "50",
					"vm.vfs_cache_pressure":                              "100",
					"vm.dirty_background_ratio":                          "10",
					"vm.dirty_ratio":                                     "20",
					"vm.dirty_expire_centisecs":                          "3000",
					"vm.dirty_writeback_centisecs":                       "500",
					"fs.nr_open":                                         "1048576",
					"fs.inotify.max_user_watches":                        "8192",
					"fs.inotify.max_user_instances":                      "8192",
					"fs.aio-max-nr":                                      "65536",
					"fs.file-max":                                        "1048576",
					"net.ipv4.tcp_max_orphans":                           "16384",
					"vm.swappiness":                                      "200",
					"vm.watermark_scale_factor":                          "3000",
					"vm.min_free_kbytes":                                 "1048576",
				}),
			},
		},
		{
			desc: "empty kubelet config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				KubeletConfig: &ccc_api.KubeletConfig{},
			},
			expected: []rules.RuleOption{},
		},
		{
			desc: "full kubelet config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				KubeletConfig: &ccc_api.KubeletConfig{
					CpuCfsQuota:                 boolPtr(true),
					CpuCfsQuotaPeriod:           stringPtr("100ms"),
					CpuManagerPolicy:            stringPtr("none"),
					PodPidsLimit:                int64Ptr(1234),
					ImageGcLowThresholdPercent:  int64Ptr(10),
					ImageGcHighThresholdPercent: int64Ptr(85),
					ImageMinimumGcAge:           stringPtr("1m10s"),
					ImageMaximumGcAge:           stringPtr("10m"),
					ContainerLogMaxFiles:        int64Ptr(6),
					ContainerLogMaxSize:         stringPtr("10Mi"),
					AllowedUnsafeSysctls:        []string{"kernel.shm*", "kernel.msg*", "kernel.sem", "fs.mqueue.*", "net.*"},
					MaxParallelImagePulls:       int64Ptr(2),
					SingleProcessOOMKill:        boolPtr(true),
					EvictionSoft: &ccc_api.EvictionSoft{
						MemoryAvailable:   stringPtr("200Mi"),
						NodefsAvailable:   stringPtr("10%"),
						ImagefsAvailable:  stringPtr("15%"),
						ImagefsInodesFree: stringPtr("5%"),
						NodefsInodesFree:  stringPtr("5%"),
						PidAvailable:      stringPtr("10%"),
					},
					EvictionSoftGracePeriod: &ccc_api.EvictionSoftGracePeriod{
						MemoryAvailable:   stringPtr("1m30s"),
						NodefsAvailable:   stringPtr("2m"),
						ImagefsAvailable:  stringPtr("2m30s"),
						ImagefsInodesFree: stringPtr("3m"),
						NodefsInodesFree:  stringPtr("3m30s"),
						PidAvailable:      stringPtr("4m"),
					},
					EvictionMinimumReclaim: &ccc_api.EvictionMinimumReclaim{
						MemoryAvailable:   stringPtr("5%"),
						NodefsAvailable:   stringPtr("6%"),
						ImagefsAvailable:  stringPtr("7%"),
						ImagefsInodesFree: stringPtr("8%"),
						NodefsInodesFree:  stringPtr("9%"),
						PidAvailable:      stringPtr("1%"),
					},
					EvictionMaxPodGracePeriodSeconds: int64Ptr(120),
					TopologyManager: &ccc_api.TopologyManager{
						Policy: stringPtr("best-effort"),
						Scope:  stringPtr("container"),
					},
					MemoryManager: &ccc_api.MemoryManager{
						Policy: stringPtr("Static"),
					},
					ShutdownGracePeriodSeconds:             int32Ptr(120),
					ShutdownGracePeriodCriticalPodsSeconds: int32Ptr(60),
					CrashLoopBackOff: &ccc_api.CrashLoopBackOff{
						MaxContainerRestartPeriod: stringPtr("10s"),
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithCpuCfsQuotaRule(true),
				rules.WithCpuCfsQuotaPeriodRule("100ms"),
				rules.WithCpuManagerPolicyRule("none"),
				rules.WithPodPidsLimitRule(1234),
				rules.WithImageGcLowThresholdPercentRule(10),
				rules.WithImageGcHighThresholdPercentRule(85),
				rules.WithImageMinimumGcAgeRule("1m10s"),
				rules.WithImageMaximumGcAgeRule("10m"),
				rules.WithContainerLogMaxFilesRule(6),
				rules.WithContainerLogMaxSizeRule("10Mi"),
				rules.WithAllowedUnsafeSysctlsRule([]string{"kernel.shm*", "kernel.msg*", "kernel.sem", "fs.mqueue.*", "net.*"}),
				rules.WithMaxParallelImagePullsRule(2),
				rules.WithSingleProcessOOMKill(true),
				rules.WithEvictionSoftMemoryAvailableRule("200Mi"),
				rules.WithEvictionSoftNodefsAvailableRule("10%"),
				rules.WithEvictionSoftImagefsAvailableRule("15%"),
				rules.WithEvictionSoftImagefsInodesFreeRule("5%"),
				rules.WithEvictionSoftNodefsInodesFreeRule("5%"),
				rules.WithEvictionSoftPidAvailableRule("10%"),
				rules.WithEvictionSoftGracePeriodMemoryAvailableRule("1m30s"),
				rules.WithEvictionSoftGracePeriodNodefsAvailableRule("2m"),
				rules.WithEvictionSoftGracePeriodImagefsAvailableRule("2m30s"),
				rules.WithEvictionSoftGracePeriodImagefsInodesFreeRule("3m"),
				rules.WithEvictionSoftGracePeriodNodefsInodesFreeRule("3m30s"),
				rules.WithEvictionSoftGracePeriodPidAvailableRule("4m"),
				rules.WithEvictionMinimumReclaimMemoryAvailableRule("5%"),
				rules.WithEvictionMinimumReclaimNodefsAvailableRule("6%"),
				rules.WithEvictionMinimumReclaimImagefsAvailableRule("7%"),
				rules.WithEvictionMinimumReclaimImagefsInodesFreeRule("8%"),
				rules.WithEvictionMinimumReclaimNodefsInodesFreeRule("9%"),
				rules.WithEvictionMinimumReclaimPidAvailableRule("1%"),
				rules.WithEvictionMaxPodGracePeriodSecondsRule(120),
				rules.WithTopologyManagerPolicyRule("best-effort"),
				rules.WithTopologyManagerScopeRule("container"),
				rules.WithMemoryManagerPolicyRule("Static"),
				rules.WithShutdownGracePeriodSecondsRule(120),
				rules.WithShutdownGracePeriodCriticalPodsSecondsRule(60),
				rules.WithCrashLoopBackOffMaxContainerRestartPeriodRule("10s"),
			},
		},
		{
			desc: "empty hugepages config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					Hugepages: &ccc_api.HugepagesConfig{},
				},
			},
			expected: []rules.RuleOption{},
		},
		{
			desc: "full hugepages config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					Hugepages: &ccc_api.HugepagesConfig{
						HugepageSize1g: int64Ptr(1234),
						HugepageSize2m: int64Ptr(5678),
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithHugepageSize1gRule(1234),
				rules.WithHugepageSize2mRule(5678),
			},
		},
		{
			desc: "full transparent hugepages config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					TransparentHugepageEnabled: stringPtr("TRANSPARENT_HUGEPAGE_ENABLED_ALWAYS"),
					TransparentHugepageDefrag:  stringPtr("TRANSPARENT_HUGEPAGE_DEFRAG_ALWAYS"),
				},
			},
			expected: []rules.RuleOption{
				rules.WithTransparentHugepageEnabledRule("TRANSPARENT_HUGEPAGE_ENABLED_ALWAYS"),
				rules.WithTransparentHugepageDefragRule("TRANSPARENT_HUGEPAGE_DEFRAG_ALWAYS"),
			},
		},
		{
			desc: "full eviction config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				KubeletConfig: &ccc_api.KubeletConfig{
					EvictionSoft: &ccc_api.EvictionSoft{
						MemoryAvailable:   stringPtr("200Mi"),
						NodefsAvailable:   stringPtr("10%"),
						ImagefsAvailable:  stringPtr("15%"),
						ImagefsInodesFree: stringPtr("5%"),
						NodefsInodesFree:  stringPtr("5%"),
						PidAvailable:      stringPtr("10%"),
					},
					EvictionSoftGracePeriod: &ccc_api.EvictionSoftGracePeriod{
						MemoryAvailable:   stringPtr("1m30s"),
						NodefsAvailable:   stringPtr("2m"),
						ImagefsAvailable:  stringPtr("2m30s"),
						ImagefsInodesFree: stringPtr("3m"),
						NodefsInodesFree:  stringPtr("3m30s"),
						PidAvailable:      stringPtr("4m"),
					},
					EvictionMinimumReclaim: &ccc_api.EvictionMinimumReclaim{
						MemoryAvailable:   stringPtr("5%"),
						NodefsAvailable:   stringPtr("6%"),
						ImagefsAvailable:  stringPtr("7%"),
						ImagefsInodesFree: stringPtr("8%"),
						NodefsInodesFree:  stringPtr("9%"),
						PidAvailable:      stringPtr("1%"),
					},
					EvictionMaxPodGracePeriodSeconds: int64Ptr(120),
				},
			},
			expected: []rules.RuleOption{
				rules.WithEvictionSoftMemoryAvailableRule("200Mi"),
				rules.WithEvictionSoftNodefsAvailableRule("10%"),
				rules.WithEvictionSoftImagefsAvailableRule("15%"),
				rules.WithEvictionSoftImagefsInodesFreeRule("5%"),
				rules.WithEvictionSoftNodefsInodesFreeRule("5%"),
				rules.WithEvictionSoftPidAvailableRule("10%"),
				rules.WithEvictionSoftGracePeriodMemoryAvailableRule("1m30s"),
				rules.WithEvictionSoftGracePeriodNodefsAvailableRule("2m"),
				rules.WithEvictionSoftGracePeriodImagefsAvailableRule("2m30s"),
				rules.WithEvictionSoftGracePeriodImagefsInodesFreeRule("3m"),
				rules.WithEvictionSoftGracePeriodNodefsInodesFreeRule("3m30s"),
				rules.WithEvictionSoftGracePeriodPidAvailableRule("4m"),
				rules.WithEvictionMinimumReclaimMemoryAvailableRule("5%"),
				rules.WithEvictionMinimumReclaimNodefsAvailableRule("6%"),
				rules.WithEvictionMinimumReclaimImagefsAvailableRule("7%"),
				rules.WithEvictionMinimumReclaimImagefsInodesFreeRule("8%"),
				rules.WithEvictionMinimumReclaimNodefsInodesFreeRule("9%"),
				rules.WithEvictionMinimumReclaimPidAvailableRule("1%"),
				rules.WithEvictionMaxPodGracePeriodSecondsRule(120),
			},
		},
		{
			desc: "accurate time config",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					AccurateTimeConfig: &ccc_api.AccurateTimeConfig{
						EnablePtpKvmTimeSync: boolPtr(true),
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithAccurateTimeConfigRule(ccc_api.AccurateTimeConfig{
					EnablePtpKvmTimeSync: boolPtr(true),
				}),
			},
		},
		{
			desc: "swap on boot disk",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					SwapConfig: &ccc_api.SwapConfig{
						Enabled: true,
						BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
							SwapSizeGib: int64Ptr(10),
						},
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: int64Ptr(10),
					},
				}),
			},
		},
		{
			desc: "swap ephemeral local ssd",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					SwapConfig: &ccc_api.SwapConfig{
						Enabled: true,
						EphemeralLocalSsdProfile: &ccc_api.SwapConfigEphemeralLocalSsdProfile{
							SwapSizeGib: int64Ptr(10),
						},
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					EphemeralLocalSsdProfile: &ccc_api.SwapConfigEphemeralLocalSsdProfile{
						SwapSizeGib: int64Ptr(10),
					},
				}),
			},
		},
		{
			desc: "swap dedicated local ssd",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					SwapConfig: &ccc_api.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 1,
					},
				}),
			},
		},
		{
			desc: "new boot time config flags",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					AdditionalEtcHosts: []*ccc_api.EtcHostsEntry{
						{Ip: "1.2.3.4", Host: "host1"},
					},
					AdditionalEtcResolvConf: []*ccc_api.ResolvedConfEntry{
						{Key: "nameserver", Value: []string{"8.8.8.8"}},
					},
					AdditionalEtcSystemdResolvedConf: []*ccc_api.ResolvedConfEntry{
						{Key: "DNS", Value: []string{"8.8.4.4"}},
					},
					CustomNodeInit: &ccc_api.CustomNodeInit{
						InitScript: &ccc_api.InitScript{
							GcsUri:        stringPtr("gs://bucket/script.sh"),
							GcsGeneration: int64Ptr(123),
							Args:          []string{"arg1"},
						},
					},
					KernelOverrides: &ccc_api.KernelOverrides{
						KernelCommandlineOverrides: &ccc_api.KernelCommandlineOverrides{
							SpecRstackOverflow: stringPtr("OFF"),
						},
						LruGen: &ccc_api.LRUGen{
							Enabled:  boolPtr(true),
							MinTtlMs: int32Ptr(1000),
						},
					},
					TimeZone: stringPtr("UTC"),
				},
			},
			expected: []rules.RuleOption{
				rules.WithAdditionalEtcHostsRule([]*rules.EtcHostsEntry{
					{Ip: "1.2.3.4", Host: "host1"},
				}),
				rules.WithAdditionalEtcResolvConfRule([]*rules.ResolvedConfEntry{
					{Key: "nameserver", Value: []string{"8.8.8.8"}},
				}),
				rules.WithAdditionalEtcSystemdResolvedConfRule([]*rules.ResolvedConfEntry{
					{Key: "DNS", Value: []string{"8.8.4.4"}},
				}),
				rules.WithCustomNodeInitRule(&rules.CustomNodeInit{
					InitScript: &rules.InitScript{
						GcsUri:        stringPtr("gs://bucket/script.sh"),
						GcsGeneration: int64Ptr(123),
						Args:          []string{"arg1"},
					},
				}),
				rules.WithKernelOverridesRule(&rules.KernelOverrides{
					KernelCommandlineOverrides: &rules.KernelCommandlineOverrides{
						SpecRstackOverflow: stringPtr("OFF"),
					},
					LruGen: &rules.LRUGen{
						Enabled:  boolPtr(true),
						MinTtlMs: int32Ptr(1000),
					},
				}),
				rules.WithTimeZoneRule("UTC"),
			},
		},
		{
			desc: "linux node config NodeVfioConfig and DiskIoScheduler",
			nodeSystemConfig: &ccc_api.NodeSystemConfig{
				LinuxNodeConfig: &ccc_api.LinuxNodeConfig{
					NodeVfioConfig: &ccc_api.NodeVfioConfig{
						DmaEntryLimit: int32Ptr(65536),
					},
					DiskIoScheduler: &ccc_api.DiskIoScheduler{
						NodeSystemIoScheduler:       "mq-deadline",
						NodeAttachedDiskIoScheduler: "bfq",
					},
				},
			},
			expected: []rules.RuleOption{
				rules.WithNodeVfioConfigRule(ccc_api.NodeVfioConfig{
					DmaEntryLimit: int32Ptr(65536),
				}),
				rules.WithDiskIoSchedulerRule(ccc_api.DiskIoScheduler{
					NodeSystemIoScheduler:       "mq-deadline",
					NodeAttachedDiskIoScheduler: "bfq",
				}),
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			actual, _ := ruleOptsForNodeSystemConfig(tc.nodeSystemConfig)
			if tc.expected == nil {
				assert.Nil(t, actual)
			} else {
				assert.NotNil(t, actual)
				assert.Equal(t, rules.NewRule(tc.expected...), rules.NewRule(actual...))
			}
		})
	}
}
