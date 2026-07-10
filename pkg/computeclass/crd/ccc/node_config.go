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
	"encoding/json"
	"fmt"
	"strconv"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

// ruleOptsForNodeSystemConfig returns node config rule options for node system config.
func ruleOptsForNodeSystemConfig(cccNodeSystemConfig *ccc_api.NodeSystemConfig) ([]rules.RuleOption, error) {
	if cccNodeSystemConfig == nil {
		return nil, nil
	}

	ruleOpts := []rules.RuleOption{}

	if cccNodeSystemConfig.LinuxNodeConfig != nil {
		// check for sysctls
		if cccNodeSystemConfig.LinuxNodeConfig.Sysctls != nil {
			// Marshal the struct to JSON
			sysctlJson, err := json.Marshal(cccNodeSystemConfig.LinuxNodeConfig.Sysctls)
			if err != nil {
				return nil, err
			}

			// Unmarshal the JSON into a map[string]interface{}
			var sysctlsJsonMap map[string]interface{}
			err = json.Unmarshal(sysctlJson, &sysctlsJsonMap)
			if err != nil {
				return nil, &json.InvalidUnmarshalError{}
			}

			// Convert map[string]interface{} to map[string]string
			sysctlsStringMap := make(map[string]string)
			for key, value := range sysctlsJsonMap {
				switch v := value.(type) {
				case string:
					sysctlsStringMap[key] = v
				case float64: // JSON numbers are unmarshaled as float64
					sysctlsStringMap[key] = strconv.FormatFloat(v, 'f', -1, 64)
				case bool:
					sysctlsStringMap[key] = boolToString(v)
				default:
					return nil, fmt.Errorf("error converting sysctl key: %s with value %v of type %T to string. Skipping...", key, value, v)
				}
			}
			ruleOpts = append(ruleOpts, rules.WithSysctlsRule(sysctlsStringMap))
		}

		// check for hugepages
		if cccNodeSystemConfig.LinuxNodeConfig.Hugepages != nil {
			if hugepageSize1g := cccNodeSystemConfig.LinuxNodeConfig.Hugepages.HugepageSize1g; hugepageSize1g != nil {
				ruleOpts = append(ruleOpts, rules.WithHugepageSize1gRule(*hugepageSize1g))
			}
			if hugepageSize2m := cccNodeSystemConfig.LinuxNodeConfig.Hugepages.HugepageSize2m; hugepageSize2m != nil {
				ruleOpts = append(ruleOpts, rules.WithHugepageSize2mRule(*hugepageSize2m))
			}
		}

		// check for transparent hugepage
		if thpEnabled := cccNodeSystemConfig.LinuxNodeConfig.TransparentHugepageEnabled; thpEnabled != nil {
			ruleOpts = append(ruleOpts, rules.WithTransparentHugepageEnabledRule(*thpEnabled))
		}
		if thpDefrag := cccNodeSystemConfig.LinuxNodeConfig.TransparentHugepageDefrag; thpDefrag != nil {
			ruleOpts = append(ruleOpts, rules.WithTransparentHugepageDefragRule(*thpDefrag))
		}

		if accurateTimeConfig := cccNodeSystemConfig.LinuxNodeConfig.AccurateTimeConfig; accurateTimeConfig != nil {
			ruleOpts = append(ruleOpts, rules.WithAccurateTimeConfigRule(*accurateTimeConfig))
		}
		if swapConfig := cccNodeSystemConfig.LinuxNodeConfig.SwapConfig; swapConfig != nil {
			ruleOpts = append(ruleOpts, rules.WithSwapConfigRule(*swapConfig))
		}

		if nodeVfioConfig := cccNodeSystemConfig.LinuxNodeConfig.NodeVfioConfig; nodeVfioConfig != nil {
			ruleOpts = append(ruleOpts, rules.WithNodeVfioConfigRule(*nodeVfioConfig))
		}
		if diskIoScheduler := cccNodeSystemConfig.LinuxNodeConfig.DiskIoScheduler; diskIoScheduler != nil {
			ruleOpts = append(ruleOpts, rules.WithDiskIoSchedulerRule(*diskIoScheduler))
		}

		if cccNodeSystemConfig.LinuxNodeConfig.AdditionalEtcHosts != nil {
			var etcHosts []*rules.EtcHostsEntry
			for _, entry := range cccNodeSystemConfig.LinuxNodeConfig.AdditionalEtcHosts {
				etcHosts = append(etcHosts, &rules.EtcHostsEntry{Ip: entry.Ip, Host: entry.Host})
			}
			ruleOpts = append(ruleOpts, rules.WithAdditionalEtcHostsRule(etcHosts))
		}
		if cccNodeSystemConfig.LinuxNodeConfig.AdditionalEtcResolvConf != nil {
			var resolvConf []*rules.ResolvedConfEntry
			for _, entry := range cccNodeSystemConfig.LinuxNodeConfig.AdditionalEtcResolvConf {
				resolvConf = append(resolvConf, &rules.ResolvedConfEntry{Key: entry.Key, Value: entry.Value})
			}
			ruleOpts = append(ruleOpts, rules.WithAdditionalEtcResolvConfRule(resolvConf))
		}
		if cccNodeSystemConfig.LinuxNodeConfig.AdditionalEtcSystemdResolvedConf != nil {
			var resolvedConf []*rules.ResolvedConfEntry
			for _, entry := range cccNodeSystemConfig.LinuxNodeConfig.AdditionalEtcSystemdResolvedConf {
				resolvedConf = append(resolvedConf, &rules.ResolvedConfEntry{Key: entry.Key, Value: entry.Value})
			}
			ruleOpts = append(ruleOpts, rules.WithAdditionalEtcSystemdResolvedConfRule(resolvedConf))
		}
		if cccNodeSystemConfig.LinuxNodeConfig.CustomNodeInit != nil {
			var customNodeInit *rules.CustomNodeInit
			if script := cccNodeSystemConfig.LinuxNodeConfig.CustomNodeInit.InitScript; script != nil {
				customNodeInit = &rules.CustomNodeInit{
					InitScript: &rules.InitScript{
						GcsUri:                    script.GcsUri,
						GcsGeneration:             script.GcsGeneration,
						Args:                      script.Args,
						GcpSecretManagerSecretUri: script.GcpSecretManagerSecretUri,
					},
				}
			}
			ruleOpts = append(ruleOpts, rules.WithCustomNodeInitRule(customNodeInit))
		}
		if cccNodeSystemConfig.LinuxNodeConfig.KernelOverrides != nil {
			ko := cccNodeSystemConfig.LinuxNodeConfig.KernelOverrides
			ruleKO := &rules.KernelOverrides{}
			if ko.KernelCommandlineOverrides != nil {
				ruleKO.KernelCommandlineOverrides = &rules.KernelCommandlineOverrides{
					SpecRstackOverflow: ko.KernelCommandlineOverrides.SpecRstackOverflow,
					InitOnAlloc:        ko.KernelCommandlineOverrides.InitOnAlloc,
				}
			}
			if ko.LruGen != nil {
				ruleKO.LruGen = &rules.LRUGen{
					Enabled:  ko.LruGen.Enabled,
					MinTtlMs: ko.LruGen.MinTtlMs,
				}
			}
			ruleOpts = append(ruleOpts, rules.WithKernelOverridesRule(ruleKO))
		}
		if cccNodeSystemConfig.LinuxNodeConfig.TimeZone != nil {
			ruleOpts = append(ruleOpts, rules.WithTimeZoneRule(*cccNodeSystemConfig.LinuxNodeConfig.TimeZone))
		}
	}

	if cccNodeSystemConfig.KubeletConfig != nil {
		if cpuCfsQuota := cccNodeSystemConfig.KubeletConfig.CpuCfsQuota; cpuCfsQuota != nil {
			ruleOpts = append(ruleOpts, rules.WithCpuCfsQuotaRule(*cpuCfsQuota))
		}
		if cpuCfsQuotaPeriod := cccNodeSystemConfig.KubeletConfig.CpuCfsQuotaPeriod; cpuCfsQuotaPeriod != nil {
			ruleOpts = append(ruleOpts, rules.WithCpuCfsQuotaPeriodRule(*cpuCfsQuotaPeriod))
		}
		if cpuManagerPolicy := cccNodeSystemConfig.KubeletConfig.CpuManagerPolicy; cpuManagerPolicy != nil {
			ruleOpts = append(ruleOpts, rules.WithCpuManagerPolicyRule(*cpuManagerPolicy))
		}
		if podPidsLimit := cccNodeSystemConfig.KubeletConfig.PodPidsLimit; podPidsLimit != nil {
			ruleOpts = append(ruleOpts, rules.WithPodPidsLimitRule(*podPidsLimit))
		}
		if imageGcLowThresholdPercent := cccNodeSystemConfig.KubeletConfig.ImageGcLowThresholdPercent; imageGcLowThresholdPercent != nil {
			ruleOpts = append(ruleOpts, rules.WithImageGcLowThresholdPercentRule(*imageGcLowThresholdPercent))
		}
		if imageGcHighThresholdPercent := cccNodeSystemConfig.KubeletConfig.ImageGcHighThresholdPercent; imageGcHighThresholdPercent != nil {
			ruleOpts = append(ruleOpts, rules.WithImageGcHighThresholdPercentRule(*imageGcHighThresholdPercent))
		}
		if imageMinimumGcAge := cccNodeSystemConfig.KubeletConfig.ImageMinimumGcAge; imageMinimumGcAge != nil {
			ruleOpts = append(ruleOpts, rules.WithImageMinimumGcAgeRule(*imageMinimumGcAge))
		}
		if imageMaximumGcAge := cccNodeSystemConfig.KubeletConfig.ImageMaximumGcAge; imageMaximumGcAge != nil {
			ruleOpts = append(ruleOpts, rules.WithImageMaximumGcAgeRule(*imageMaximumGcAge))
		}
		if containerLogMaxSize := cccNodeSystemConfig.KubeletConfig.ContainerLogMaxSize; containerLogMaxSize != nil {
			ruleOpts = append(ruleOpts, rules.WithContainerLogMaxSizeRule(*containerLogMaxSize))
		}
		if containerLogMaxFiles := cccNodeSystemConfig.KubeletConfig.ContainerLogMaxFiles; containerLogMaxFiles != nil {
			ruleOpts = append(ruleOpts, rules.WithContainerLogMaxFilesRule(*containerLogMaxFiles))
		}
		if allowedUnsafeSysctls := cccNodeSystemConfig.KubeletConfig.AllowedUnsafeSysctls; allowedUnsafeSysctls != nil {
			ruleOpts = append(ruleOpts, rules.WithAllowedUnsafeSysctlsRule(allowedUnsafeSysctls))
		}
		if maxParallelImagePulls := cccNodeSystemConfig.KubeletConfig.MaxParallelImagePulls; maxParallelImagePulls != nil {
			ruleOpts = append(ruleOpts, rules.WithMaxParallelImagePullsRule(*maxParallelImagePulls))
		}
		if singleProcessOOMKill := cccNodeSystemConfig.KubeletConfig.SingleProcessOOMKill; singleProcessOOMKill != nil {
			ruleOpts = append(ruleOpts, rules.WithSingleProcessOOMKill(*singleProcessOOMKill))
		}
		if evictionSoft := cccNodeSystemConfig.KubeletConfig.EvictionSoft; evictionSoft != nil {
			if memoryAvailable := evictionSoft.MemoryAvailable; memoryAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftMemoryAvailableRule(*memoryAvailable))
			}
			if nodefsAvailable := evictionSoft.NodefsAvailable; nodefsAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftNodefsAvailableRule(*nodefsAvailable))
			}
			if imagefsAvailable := evictionSoft.ImagefsAvailable; imagefsAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftImagefsAvailableRule(*imagefsAvailable))
			}
			if imagefsInodesFree := evictionSoft.ImagefsInodesFree; imagefsInodesFree != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftImagefsInodesFreeRule(*imagefsInodesFree))
			}
			if nodefsInodesFree := evictionSoft.NodefsInodesFree; nodefsInodesFree != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftNodefsInodesFreeRule(*nodefsInodesFree))
			}
			if pidAvailable := evictionSoft.PidAvailable; pidAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftPidAvailableRule(*pidAvailable))
			}
		}
		if evictionSoftGracePeriod := cccNodeSystemConfig.KubeletConfig.EvictionSoftGracePeriod; evictionSoftGracePeriod != nil {
			if memoryAvailable := evictionSoftGracePeriod.MemoryAvailable; memoryAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftGracePeriodMemoryAvailableRule(*memoryAvailable))
			}
			if nodefsAvailable := evictionSoftGracePeriod.NodefsAvailable; nodefsAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftGracePeriodNodefsAvailableRule(*nodefsAvailable))
			}
			if imagefsAvailable := evictionSoftGracePeriod.ImagefsAvailable; imagefsAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftGracePeriodImagefsAvailableRule(*imagefsAvailable))
			}
			if imagefsInodesFree := evictionSoftGracePeriod.ImagefsInodesFree; imagefsInodesFree != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftGracePeriodImagefsInodesFreeRule(*imagefsInodesFree))
			}
			if nodefsInodesFree := evictionSoftGracePeriod.NodefsInodesFree; nodefsInodesFree != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftGracePeriodNodefsInodesFreeRule(*nodefsInodesFree))
			}
			if pidAvailable := evictionSoftGracePeriod.PidAvailable; pidAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionSoftGracePeriodPidAvailableRule(*pidAvailable))
			}
		}
		if evictionMinimumReclaim := cccNodeSystemConfig.KubeletConfig.EvictionMinimumReclaim; evictionMinimumReclaim != nil {
			if memoryAvailable := evictionMinimumReclaim.MemoryAvailable; memoryAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionMinimumReclaimMemoryAvailableRule(*memoryAvailable))
			}
			if nodefsAvailable := evictionMinimumReclaim.NodefsAvailable; nodefsAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionMinimumReclaimNodefsAvailableRule(*nodefsAvailable))
			}
			if imagefsAvailable := evictionMinimumReclaim.ImagefsAvailable; imagefsAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionMinimumReclaimImagefsAvailableRule(*imagefsAvailable))
			}
			if imagefsInodesFree := evictionMinimumReclaim.ImagefsInodesFree; imagefsInodesFree != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionMinimumReclaimImagefsInodesFreeRule(*imagefsInodesFree))
			}
			if nodefsInodesFree := evictionMinimumReclaim.NodefsInodesFree; nodefsInodesFree != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionMinimumReclaimNodefsInodesFreeRule(*nodefsInodesFree))
			}
			if pidAvailable := evictionMinimumReclaim.PidAvailable; pidAvailable != nil {
				ruleOpts = append(ruleOpts, rules.WithEvictionMinimumReclaimPidAvailableRule(*pidAvailable))
			}
		}
		if evictionMaxPodGracePeriodSeconds := cccNodeSystemConfig.KubeletConfig.EvictionMaxPodGracePeriodSeconds; evictionMaxPodGracePeriodSeconds != nil {
			ruleOpts = append(ruleOpts, rules.WithEvictionMaxPodGracePeriodSecondsRule(*evictionMaxPodGracePeriodSeconds))
		}
		if topologyManager := cccNodeSystemConfig.KubeletConfig.TopologyManager; topologyManager != nil {
			if policy := topologyManager.Policy; policy != nil {
				ruleOpts = append(ruleOpts, rules.WithTopologyManagerPolicyRule(*policy))
			}
			if scope := topologyManager.Scope; scope != nil {
				ruleOpts = append(ruleOpts, rules.WithTopologyManagerScopeRule(*scope))
			}
		}
		if memoryManager := cccNodeSystemConfig.KubeletConfig.MemoryManager; memoryManager != nil {
			if policy := memoryManager.Policy; policy != nil {
				ruleOpts = append(ruleOpts, rules.WithMemoryManagerPolicyRule(*policy))
			}
		}
		if shutdownGracePeriodSeconds := cccNodeSystemConfig.KubeletConfig.ShutdownGracePeriodSeconds; shutdownGracePeriodSeconds != nil {
			ruleOpts = append(ruleOpts, rules.WithShutdownGracePeriodSecondsRule(int64(*shutdownGracePeriodSeconds)))
		}
		if shutdownGracePeriodCriticalPodsSeconds := cccNodeSystemConfig.KubeletConfig.ShutdownGracePeriodCriticalPodsSeconds; shutdownGracePeriodCriticalPodsSeconds != nil {
			ruleOpts = append(ruleOpts, rules.WithShutdownGracePeriodCriticalPodsSecondsRule(int64(*shutdownGracePeriodCriticalPodsSeconds)))
		}
		if crashLoopBackOff := cccNodeSystemConfig.KubeletConfig.CrashLoopBackOff; crashLoopBackOff != nil {
			if maxContainerRestartPeriod := crashLoopBackOff.MaxContainerRestartPeriod; maxContainerRestartPeriod != nil {
				ruleOpts = append(ruleOpts, rules.WithCrashLoopBackOffMaxContainerRestartPeriodRule(*maxContainerRestartPeriod))
			}
		}
	}

	return ruleOpts, nil
}

func boolToString(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
