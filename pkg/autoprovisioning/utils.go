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

package autoprovisioning

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
)

const (
	fieldCpuCfsQuota                         = "CpuCfsQuota"
	fieldShutdownGracePeriod                 = "ShutdownGracePeriodSeconds"
	fieldShutdownGracePeriodCriticalPods     = "ShutdownGracePeriodCriticalPodsSeconds"
	fieldCpuCfsQuotaJSON                     = "cpuCfsQuota"
	fieldShutdownGracePeriodJSON             = "shutdownGracePeriodSeconds"
	fieldShutdownGracePeriodCriticalPodsJSON = "shutdownGracePeriodCriticalPodsSeconds"
)

func getResourceBasedBackoff(compositeBackoff gke_backoff.CompositeBackoff) *gke_backoff.ResourceBackoff {
	for _, backoff := range compositeBackoff.GetBackoffs() {
		if resourceBasedBackoff, ok := backoff.(*gke_backoff.ResourceBackoff); ok {
			return resourceBasedBackoff
		}
	}
	return nil
}

func autoprovisionedNodeGroupsCount(nodeGroups []cloudprovider.NodeGroup) int {
	result := 0
	for _, group := range nodeGroups {
		if group.Autoprovisioned() {
			result++
		}
	}
	return result
}

func virtualNodeInfos(nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo) map[string]*framework.NodeInfo {
	var virtualNodeInfos = make(map[string]*framework.NodeInfo)
	for _, nodeG := range nodeGroups {
		if !nodeG.Exist() {
			nodeI := nodeInfos[nodeG.Id()]
			virtualNodeInfos[nodeG.Id()] = nodeI
		}
	}
	return virtualNodeInfos
}

func linuxNodeConfigFromCCRule(rule rules.Rule) *gkeclient.LinuxNodeConfig {
	linuxNodeConfig := &gkeclient.LinuxNodeConfig{}
	if rule.Sysctls() != nil {
		linuxNodeConfig.Sysctls = rule.Sysctls()
	}
	if rule.HugepageSize1g() != nil || rule.HugepageSize2m() != nil {
		linuxNodeConfig.Hugepages = &gkeclient.HugepagesConfig{}
		if hugepageSize1g := rule.HugepageSize1g(); hugepageSize1g != nil {
			linuxNodeConfig.Hugepages.HugepageSize1g = *hugepageSize1g
		}
		if hugepageSize2m := rule.HugepageSize2m(); hugepageSize2m != nil {
			linuxNodeConfig.Hugepages.HugepageSize2m = *hugepageSize2m
		}
	}
	if transparentHugepageEnabled := rule.TransparentHugepageEnabled(); transparentHugepageEnabled != nil {
		linuxNodeConfig.TransparentHugepageEnabled = *transparentHugepageEnabled
	}
	if transparentHugepageDefrag := rule.TransparentHugepageDefrag(); transparentHugepageDefrag != nil {
		linuxNodeConfig.TransparentHugepageDefrag = *transparentHugepageDefrag
	}

	if accurateTimeConfig := rule.AccurateTimeConfig(); accurateTimeConfig != nil {
		linuxNodeConfig.AccurateTimeConfig = &gkeclient.AccurateTimeConfig{}
		if enabled := accurateTimeConfig.EnablePtpKvmTimeSync; enabled != nil {
			linuxNodeConfig.AccurateTimeConfig.EnablePtpKvmTimeSync = *enabled
		}
	}

	if nodeVfioConfig := rule.NodeVfioConfig(); nodeVfioConfig != nil {
		linuxNodeConfig.NodeVfioConfig = &gkeclient.NodeVfioConfig{}
		if dmaEntryLimit := nodeVfioConfig.DmaEntryLimit; dmaEntryLimit != nil {
			linuxNodeConfig.NodeVfioConfig.DmaEntryLimit = int64(*dmaEntryLimit)
		}
	}

	if diskIoScheduler := rule.DiskIoScheduler(); diskIoScheduler != nil {
		linuxNodeConfig.DiskIoScheduler = &gkeclient.DiskIoScheduler{}
		if diskIoScheduler.NodeSystemIoScheduler != "" {
			linuxNodeConfig.DiskIoScheduler.NodeSystemIoScheduler = diskIoScheduler.NodeSystemIoScheduler
		}
		if diskIoScheduler.NodeAttachedDiskIoScheduler != "" {
			linuxNodeConfig.DiskIoScheduler.NodeAttachedDiskIoScheduler = diskIoScheduler.NodeAttachedDiskIoScheduler
		}
	}

	if swapConfig := rule.SwapConfig(); swapConfig != nil {
		linuxNodeConfig.SwapConfig = &gkeclient.SwapConfig{}
		if swapConfig.Enabled {
			linuxNodeConfig.SwapConfig.Enabled = true
		}
		if swapConfig.EncryptionConfig != nil {
			linuxNodeConfig.SwapConfig.EncryptionConfig = &gkeclient.EncryptionConfig{
				Disabled: swapConfig.EncryptionConfig.Disabled,
			}
		}
		if swapConfig.BootDiskProfile != nil {
			linuxNodeConfig.SwapConfig.BootDiskProfile = &gkeclient.BootDiskProfile{}
			if swapConfig.BootDiskProfile.SwapSizeGib != nil {
				linuxNodeConfig.SwapConfig.BootDiskProfile.SwapSizeGib = *swapConfig.BootDiskProfile.SwapSizeGib
			}
			if swapConfig.BootDiskProfile.SwapSizePercent != nil {
				linuxNodeConfig.SwapConfig.BootDiskProfile.SwapSizePercent = int64(*swapConfig.BootDiskProfile.SwapSizePercent)
			}
		}
		if swapConfig.EphemeralLocalSsdProfile != nil {
			linuxNodeConfig.SwapConfig.EphemeralLocalSsdProfile = &gkeclient.EphemeralLocalSsdProfile{}
			if swapConfig.EphemeralLocalSsdProfile.SwapSizeGib != nil {
				linuxNodeConfig.SwapConfig.EphemeralLocalSsdProfile.SwapSizeGib = *swapConfig.EphemeralLocalSsdProfile.SwapSizeGib
			}
			if swapConfig.EphemeralLocalSsdProfile.SwapSizePercent != nil {
				linuxNodeConfig.SwapConfig.EphemeralLocalSsdProfile.SwapSizePercent = int64(*swapConfig.EphemeralLocalSsdProfile.SwapSizePercent)
			}
		}
		if swapConfig.DedicatedLocalSsdProfile != nil {
			linuxNodeConfig.SwapConfig.DedicatedLocalSsdProfile = &gkeclient.DedicatedLocalSsdProfile{
				DiskCount: swapConfig.DedicatedLocalSsdProfile.DiskCount,
			}
		}
	}
	if rule.AdditionalEtcHosts() != nil {
		for _, entry := range rule.AdditionalEtcHosts() {
			linuxNodeConfig.AdditionalEtcHosts = append(linuxNodeConfig.AdditionalEtcHosts, &gkeclient.EtcHostsEntry{Ip: entry.Ip, Host: entry.Host})
		}
	}
	if rule.AdditionalEtcResolvConf() != nil {
		for _, entry := range rule.AdditionalEtcResolvConf() {
			linuxNodeConfig.AdditionalEtcResolvConf = append(linuxNodeConfig.AdditionalEtcResolvConf, &gkeclient.ResolvedConfEntry{Key: entry.Key, Value: entry.Value})
		}
	}
	if rule.AdditionalEtcSystemdResolvedConf() != nil {
		for _, entry := range rule.AdditionalEtcSystemdResolvedConf() {
			linuxNodeConfig.AdditionalEtcSystemdResolvedConf = append(linuxNodeConfig.AdditionalEtcSystemdResolvedConf, &gkeclient.ResolvedConfEntry{Key: entry.Key, Value: entry.Value})
		}
	}
	if rule.CustomNodeInit() != nil {
		linuxNodeConfig.CustomNodeInit = &gkeclient.CustomNodeInit{}
		if rule.CustomNodeInit().InitScript != nil {
			script := rule.CustomNodeInit().InitScript
			linuxNodeConfig.CustomNodeInit.InitScript = &gkeclient.InitScript{
				GcsUri:                    derefString(script.GcsUri),
				GcsGeneration:             derefInt64(script.GcsGeneration),
				Args:                      script.Args,
				GcpSecretManagerSecretUri: derefString(script.GcpSecretManagerSecretUri),
			}
		}
	}
	if rule.KernelOverrides() != nil {
		ko := rule.KernelOverrides()
		linuxNodeConfig.KernelOverrides = &gkeclient.KernelOverrides{}
		if ko.KernelCommandlineOverrides != nil {
			linuxNodeConfig.KernelOverrides.KernelCommandlineOverrides = &gkeclient.KernelCommandlineOverrides{
				SpecRstackOverflow: derefString(ko.KernelCommandlineOverrides.SpecRstackOverflow),
				InitOnAlloc:        derefString(ko.KernelCommandlineOverrides.InitOnAlloc),
			}
		}
		if ko.LruGen != nil {
			linuxNodeConfig.KernelOverrides.LruGen = &gkeclient.LRUGen{
				Enabled:  derefBool(ko.LruGen.Enabled),
				MinTtlMs: int64(derefInt32(ko.LruGen.MinTtlMs)),
			}
		}
	}
	if rule.TimeZone() != nil {
		linuxNodeConfig.TimeZone = *rule.TimeZone()
	}

	if reflect.DeepEqual(linuxNodeConfig, &gkeclient.LinuxNodeConfig{}) {
		return nil
	}
	return linuxNodeConfig
}

func kubeletConfigFromCCRule(rule rules.Rule) *gke_api_beta.NodeKubeletConfig {
	kubeletConfig := &gke_api_beta.NodeKubeletConfig{}
	if cpuCfsQuota := rule.CpuCfsQuota(); cpuCfsQuota != nil {
		kubeletConfig.CpuCfsQuota = *cpuCfsQuota
		kubeletConfig.ForceSendFields = append(kubeletConfig.ForceSendFields, fieldCpuCfsQuota)
	}
	if cpuCfsQuotaPeriod := rule.CpuCfsQuotaPeriod(); cpuCfsQuotaPeriod != nil {
		kubeletConfig.CpuCfsQuotaPeriod = *cpuCfsQuotaPeriod
	}
	if cpuManagerPolicy := rule.CpuManagerPolicy(); cpuManagerPolicy != nil {
		kubeletConfig.CpuManagerPolicy = *cpuManagerPolicy
	}
	if podPidsLimit := rule.PodPidsLimit(); podPidsLimit != nil {
		kubeletConfig.PodPidsLimit = *podPidsLimit
	}
	if imageGcLowThresholdPercent := rule.ImageGcLowThresholdPercent(); imageGcLowThresholdPercent != nil {
		kubeletConfig.ImageGcLowThresholdPercent = *imageGcLowThresholdPercent
	}
	if imageGcHighThresholdPercent := rule.ImageGcHighThresholdPercent(); imageGcHighThresholdPercent != nil {
		kubeletConfig.ImageGcHighThresholdPercent = *imageGcHighThresholdPercent
	}
	if imageMinimumGcAge := rule.ImageMinimumGcAge(); imageMinimumGcAge != nil {
		kubeletConfig.ImageMinimumGcAge = *imageMinimumGcAge
	}
	if imageMaximumGcAge := rule.ImageMaximumGcAge(); imageMaximumGcAge != nil {
		kubeletConfig.ImageMaximumGcAge = *imageMaximumGcAge
	}
	if containerLogMaxSize := rule.ContainerLogMaxSize(); containerLogMaxSize != nil {
		kubeletConfig.ContainerLogMaxSize = *containerLogMaxSize
	}
	if containerLogMaxFiles := rule.ContainerLogMaxFiles(); containerLogMaxFiles != nil {
		kubeletConfig.ContainerLogMaxFiles = *containerLogMaxFiles
	}
	if allowedUnsafeSysctls := rule.AllowedUnsafeSysctls(); allowedUnsafeSysctls != nil {
		kubeletConfig.AllowedUnsafeSysctls = allowedUnsafeSysctls
	}
	if maxParallelImagePulls := rule.MaxParallelImagePulls(); maxParallelImagePulls != nil {
		kubeletConfig.MaxParallelImagePulls = *maxParallelImagePulls
	}
	if singleProcessOomKill := rule.SingleProcessOOMKill(); singleProcessOomKill != nil {
		kubeletConfig.SingleProcessOomKill = *singleProcessOomKill
	}
	if val := rule.EvictionSoftMemoryAvailable(); val != nil {
		if kubeletConfig.EvictionSoft == nil {
			kubeletConfig.EvictionSoft = &gke_api_beta.EvictionSignals{}
		}
		kubeletConfig.EvictionSoft.MemoryAvailable = *val
	}
	if val := rule.EvictionSoftNodefsAvailable(); val != nil {
		if kubeletConfig.EvictionSoft == nil {
			kubeletConfig.EvictionSoft = &gke_api_beta.EvictionSignals{}
		}
		kubeletConfig.EvictionSoft.NodefsAvailable = *val
	}
	if val := rule.EvictionSoftImagefsAvailable(); val != nil {
		if kubeletConfig.EvictionSoft == nil {
			kubeletConfig.EvictionSoft = &gke_api_beta.EvictionSignals{}
		}
		kubeletConfig.EvictionSoft.ImagefsAvailable = *val
	}
	if val := rule.EvictionSoftImagefsInodesFree(); val != nil {
		if kubeletConfig.EvictionSoft == nil {
			kubeletConfig.EvictionSoft = &gke_api_beta.EvictionSignals{}
		}
		kubeletConfig.EvictionSoft.ImagefsInodesFree = *val
	}
	if val := rule.EvictionSoftNodefsInodesFree(); val != nil {
		if kubeletConfig.EvictionSoft == nil {
			kubeletConfig.EvictionSoft = &gke_api_beta.EvictionSignals{}
		}
		kubeletConfig.EvictionSoft.NodefsInodesFree = *val
	}
	if val := rule.EvictionSoftPidAvailable(); val != nil {
		if kubeletConfig.EvictionSoft == nil {
			kubeletConfig.EvictionSoft = &gke_api_beta.EvictionSignals{}
		}
		kubeletConfig.EvictionSoft.PidAvailable = *val
	}
	if val := rule.EvictionSoftGracePeriodMemoryAvailable(); val != nil {
		if kubeletConfig.EvictionSoftGracePeriod == nil {
			kubeletConfig.EvictionSoftGracePeriod = &gke_api_beta.EvictionGracePeriod{}
		}
		kubeletConfig.EvictionSoftGracePeriod.MemoryAvailable = *val
	}
	if val := rule.EvictionSoftGracePeriodNodefsAvailable(); val != nil {
		if kubeletConfig.EvictionSoftGracePeriod == nil {
			kubeletConfig.EvictionSoftGracePeriod = &gke_api_beta.EvictionGracePeriod{}
		}
		kubeletConfig.EvictionSoftGracePeriod.NodefsAvailable = *val
	}
	if val := rule.EvictionSoftGracePeriodImagefsAvailable(); val != nil {
		if kubeletConfig.EvictionSoftGracePeriod == nil {
			kubeletConfig.EvictionSoftGracePeriod = &gke_api_beta.EvictionGracePeriod{}
		}
		kubeletConfig.EvictionSoftGracePeriod.ImagefsAvailable = *val
	}
	if val := rule.EvictionSoftGracePeriodImagefsInodesFree(); val != nil {
		if kubeletConfig.EvictionSoftGracePeriod == nil {
			kubeletConfig.EvictionSoftGracePeriod = &gke_api_beta.EvictionGracePeriod{}
		}
		kubeletConfig.EvictionSoftGracePeriod.ImagefsInodesFree = *val
	}
	if val := rule.EvictionSoftGracePeriodNodefsInodesFree(); val != nil {
		if kubeletConfig.EvictionSoftGracePeriod == nil {
			kubeletConfig.EvictionSoftGracePeriod = &gke_api_beta.EvictionGracePeriod{}
		}
		kubeletConfig.EvictionSoftGracePeriod.NodefsInodesFree = *val
	}
	if val := rule.EvictionSoftGracePeriodPidAvailable(); val != nil {
		if kubeletConfig.EvictionSoftGracePeriod == nil {
			kubeletConfig.EvictionSoftGracePeriod = &gke_api_beta.EvictionGracePeriod{}
		}
		kubeletConfig.EvictionSoftGracePeriod.PidAvailable = *val
	}
	if val := rule.EvictionMinimumReclaimMemoryAvailable(); val != nil {
		if kubeletConfig.EvictionMinimumReclaim == nil {
			kubeletConfig.EvictionMinimumReclaim = &gke_api_beta.EvictionMinimumReclaim{}
		}
		kubeletConfig.EvictionMinimumReclaim.MemoryAvailable = *val
	}
	if val := rule.EvictionMinimumReclaimNodefsAvailable(); val != nil {
		if kubeletConfig.EvictionMinimumReclaim == nil {
			kubeletConfig.EvictionMinimumReclaim = &gke_api_beta.EvictionMinimumReclaim{}
		}
		kubeletConfig.EvictionMinimumReclaim.NodefsAvailable = *val
	}
	if val := rule.EvictionMinimumReclaimImagefsAvailable(); val != nil {
		if kubeletConfig.EvictionMinimumReclaim == nil {
			kubeletConfig.EvictionMinimumReclaim = &gke_api_beta.EvictionMinimumReclaim{}
		}
		kubeletConfig.EvictionMinimumReclaim.ImagefsAvailable = *val
	}
	if val := rule.EvictionMinimumReclaimImagefsInodesFree(); val != nil {
		if kubeletConfig.EvictionMinimumReclaim == nil {
			kubeletConfig.EvictionMinimumReclaim = &gke_api_beta.EvictionMinimumReclaim{}
		}
		kubeletConfig.EvictionMinimumReclaim.ImagefsInodesFree = *val
	}
	if val := rule.EvictionMinimumReclaimNodefsInodesFree(); val != nil {
		if kubeletConfig.EvictionMinimumReclaim == nil {
			kubeletConfig.EvictionMinimumReclaim = &gke_api_beta.EvictionMinimumReclaim{}
		}
		kubeletConfig.EvictionMinimumReclaim.NodefsInodesFree = *val
	}
	if val := rule.EvictionMinimumReclaimPidAvailable(); val != nil {
		if kubeletConfig.EvictionMinimumReclaim == nil {
			kubeletConfig.EvictionMinimumReclaim = &gke_api_beta.EvictionMinimumReclaim{}
		}
		kubeletConfig.EvictionMinimumReclaim.PidAvailable = *val
	}
	if val := rule.EvictionMaxPodGracePeriodSeconds(); val != nil {
		kubeletConfig.EvictionMaxPodGracePeriodSeconds = *val
	}
	if topologyManagerPolicy := rule.TopologyManagerPolicy(); topologyManagerPolicy != nil {
		if kubeletConfig.TopologyManager == nil {
			kubeletConfig.TopologyManager = &gke_api_beta.TopologyManager{}
		}
		kubeletConfig.TopologyManager.Policy = *topologyManagerPolicy
	}
	if topologyManagerScope := rule.TopologyManagerScope(); topologyManagerScope != nil {
		if kubeletConfig.TopologyManager == nil {
			kubeletConfig.TopologyManager = &gke_api_beta.TopologyManager{}
		}
		kubeletConfig.TopologyManager.Scope = *topologyManagerScope
	}
	if memoryManagerPolicy := rule.MemoryManagerPolicy(); memoryManagerPolicy != nil {
		kubeletConfig.MemoryManager = &gke_api_beta.MemoryManager{Policy: *memoryManagerPolicy}
	}
	if shutdownGracePeriodSeconds := rule.ShutdownGracePeriodSeconds(); shutdownGracePeriodSeconds != nil {
		kubeletConfig.ShutdownGracePeriodSeconds = *shutdownGracePeriodSeconds
		// Adding ShutdownGracePeriodSeconds to ForceSendFields as 0 is a valid value for the API
		kubeletConfig.ForceSendFields = append(kubeletConfig.ForceSendFields, fieldShutdownGracePeriod)
	}
	if shutdownGracePeriodCriticalPodsSeconds := rule.ShutdownGracePeriodCriticalPodsSeconds(); shutdownGracePeriodCriticalPodsSeconds != nil {
		kubeletConfig.ShutdownGracePeriodCriticalPodsSeconds = *shutdownGracePeriodCriticalPodsSeconds
		kubeletConfig.ForceSendFields = append(kubeletConfig.ForceSendFields, fieldShutdownGracePeriodCriticalPods)
	}
	if val := rule.CrashLoopBackOffMaxContainerRestartPeriod(); val != nil {
		if kubeletConfig.CrashLoopBackOff == nil {
			kubeletConfig.CrashLoopBackOff = &gke_api_beta.CrashLoopBackOffConfig{}
		}
		kubeletConfig.CrashLoopBackOff.MaxContainerRestartPeriod = *val
	}

	if reflect.DeepEqual(kubeletConfig, &gke_api_beta.NodeKubeletConfig{}) {
		return nil
	}
	return kubeletConfig
}

func serializeLinuxNodeConfig(linuxNodeConfig *gkeclient.LinuxNodeConfig) (string, error) {
	if linuxNodeConfig == nil {
		return "", nil
	}
	linuxNodeConfigJsonBytes, err := json.Marshal(linuxNodeConfig)
	if err != nil {
		return "", err
	}
	return string(linuxNodeConfigJsonBytes), nil
}

func deserializeLinuxNodeConfig(linuxNodeConfig string) (*gkeclient.LinuxNodeConfig, error) {
	if linuxNodeConfig == "" {
		return nil, nil
	}
	var deserializedLinuxNodeConfig *gkeclient.LinuxNodeConfig
	linuxNodeConfigJsonBytes := []byte(linuxNodeConfig)
	err := json.Unmarshal(linuxNodeConfigJsonBytes, &deserializedLinuxNodeConfig)
	if err != nil {
		return nil, err
	}
	return deserializedLinuxNodeConfig, nil
}

func linuxNodeConfigSignature(linuxNodeConfig *gkeclient.LinuxNodeConfig) string {
	if linuxNodeConfig == nil {
		return ""
	}

	var linuxConfigParts []string
	if linuxNodeConfig.Sysctls != nil {
		var pairs []string
		for key, value := range linuxNodeConfig.Sysctls {
			pairs = append(pairs, fmt.Sprintf("%s:%s", key, value))
		}
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("Sysctls: <%s>", strings.Join(pairs, ", ")))
	}

	if linuxNodeConfig.Hugepages != nil {
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("Hugepages: <hugepage_size1g: %d, hugepage_size2m: %d>", linuxNodeConfig.Hugepages.HugepageSize1g, linuxNodeConfig.Hugepages.HugepageSize2m))
	}
	if linuxNodeConfig.TransparentHugepageEnabled != "" {
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("TransparentHugepageEnabled: %q", linuxNodeConfig.TransparentHugepageEnabled))
	}
	if linuxNodeConfig.TransparentHugepageDefrag != "" {
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("TransparentHugepageDefrag: %q", linuxNodeConfig.TransparentHugepageDefrag))
	}

	if accurateTimeConfig := linuxNodeConfig.AccurateTimeConfig; accurateTimeConfig != nil {
		var entries []string
		entries = append(entries, fmt.Sprintf("EnablePtpKvmTimeSync: %t", accurateTimeConfig.EnablePtpKvmTimeSync))
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("AccurateTimeConfig: <%s>", strings.Join(entries, ", ")))
	}

	if nodeVfioConfig := linuxNodeConfig.NodeVfioConfig; nodeVfioConfig != nil {
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("NodeVfioConfig: <dmaEntryLimit: %d>", nodeVfioConfig.DmaEntryLimit))
	}

	if diskIoScheduler := linuxNodeConfig.DiskIoScheduler; diskIoScheduler != nil {
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("DiskIoScheduler: <nodeSystemIoScheduler: %q, nodeAttachedDiskIoScheduler: %q>", diskIoScheduler.NodeSystemIoScheduler, diskIoScheduler.NodeAttachedDiskIoScheduler))
	}

	if swapConfig := linuxNodeConfig.SwapConfig; swapConfig != nil {
		var parts []string
		parts = append(parts, fmt.Sprintf("enabled: %t", swapConfig.Enabled))
		if swapConfig.EncryptionConfig != nil {
			parts = append(parts, fmt.Sprintf("encryptionConfig: <disabled: %t>", swapConfig.EncryptionConfig.Disabled))
		}
		if swapConfig.BootDiskProfile != nil {
			parts = append(parts, fmt.Sprintf("bootDiskProfile: <swapSizeGib: %d, swapSizePercent: %d>", swapConfig.BootDiskProfile.SwapSizeGib, swapConfig.BootDiskProfile.SwapSizePercent))
		}
		if swapConfig.EphemeralLocalSsdProfile != nil {
			parts = append(parts, fmt.Sprintf("ephemeralLocalSsdProfile: <swapSizeGib: %d, swapSizePercent: %d>", swapConfig.EphemeralLocalSsdProfile.SwapSizeGib, swapConfig.EphemeralLocalSsdProfile.SwapSizePercent))
		}
		if swapConfig.DedicatedLocalSsdProfile != nil {
			parts = append(parts, fmt.Sprintf("dedicatedLocalSsdProfile: <diskCount: %d>", swapConfig.DedicatedLocalSsdProfile.DiskCount))
		}
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("swapConfig: <%s>", strings.Join(parts, ", ")))
	}

	if len(linuxNodeConfig.AdditionalEtcHosts) > 0 {
		var entries []string
		for _, entry := range linuxNodeConfig.AdditionalEtcHosts {
			entries = append(entries, fmt.Sprintf("<%s:%s>", entry.Ip, entry.Host))
		}
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("AdditionalEtcHosts: [%s]", strings.Join(entries, ", ")))
	}
	if len(linuxNodeConfig.AdditionalEtcResolvConf) > 0 {
		var entries []string
		for _, entry := range linuxNodeConfig.AdditionalEtcResolvConf {
			entries = append(entries, fmt.Sprintf("<%s:%s>", entry.Key, strings.Join(entry.Value, ",")))
		}
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("AdditionalEtcResolvConf: [%s]", strings.Join(entries, ", ")))
	}
	if len(linuxNodeConfig.AdditionalEtcSystemdResolvedConf) > 0 {
		var entries []string
		for _, entry := range linuxNodeConfig.AdditionalEtcSystemdResolvedConf {
			entries = append(entries, fmt.Sprintf("<%s:%s>", entry.Key, strings.Join(entry.Value, ",")))
		}
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("AdditionalEtcSystemdResolvedConf: [%s]", strings.Join(entries, ", ")))
	}
	if linuxNodeConfig.CustomNodeInit != nil && linuxNodeConfig.CustomNodeInit.InitScript != nil {
		script := linuxNodeConfig.CustomNodeInit.InitScript
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("CustomNodeInit: <GcsUri: %q, GcsGeneration: %d, Args: [%s], GcpSecretManagerSecretUri: %q>", script.GcsUri, script.GcsGeneration, strings.Join(script.Args, ","), script.GcpSecretManagerSecretUri))
	}
	if linuxNodeConfig.KernelOverrides != nil {
		var parts []string
		if ko := linuxNodeConfig.KernelOverrides.KernelCommandlineOverrides; ko != nil {
			parts = append(parts, fmt.Sprintf("KernelCommandlineOverrides: <SpecRstackOverflow: %q, InitOnAlloc: %q>", ko.SpecRstackOverflow, ko.InitOnAlloc))
		}
		if lru := linuxNodeConfig.KernelOverrides.LruGen; lru != nil {
			parts = append(parts, fmt.Sprintf("LruGen: <Enabled: %t, MinTtlMs: %d>", lru.Enabled, lru.MinTtlMs))
		}
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("KernelOverrides: <%s>", strings.Join(parts, ", ")))
	}
	if linuxNodeConfig.TimeZone != "" {
		linuxConfigParts = append(linuxConfigParts, fmt.Sprintf("TimeZone: %q", linuxNodeConfig.TimeZone))
	}

	return fmt.Sprintf("linuxNodeConfig: <%s>", strings.Join(linuxConfigParts, ", "))
}

func serializeKubeletConfig(kubeletConfig *gke_api_beta.NodeKubeletConfig) (string, error) {
	if kubeletConfig == nil {
		return "", nil
	}
	kubeletConfigJsonBytes, err := json.Marshal(kubeletConfig)
	if err != nil {
		return "", err
	}
	return string(kubeletConfigJsonBytes), nil
}

func deserializeKubeletConfig(kubeletConfig string) (*gke_api_beta.NodeKubeletConfig, error) {
	if kubeletConfig == "" {
		return nil, nil
	}
	var deserializedKubeletConfig *gke_api_beta.NodeKubeletConfig
	linuxNodeConfigJsonBytes := []byte(kubeletConfig)
	err := json.Unmarshal(linuxNodeConfigJsonBytes, &deserializedKubeletConfig)
	if err != nil {
		return nil, err
	}
	// Add ShutdownGracePeriodSeconds and ShutdownGracePeriodCriticalPodsSeconds to ForceSendFields if they exists in original JSON
	var deserializedKubeletConfigMap map[string]interface{}
	err = json.Unmarshal(linuxNodeConfigJsonBytes, &deserializedKubeletConfigMap)
	if err != nil {
		return nil, err
	}
	if _, ok := deserializedKubeletConfigMap[fieldCpuCfsQuotaJSON]; ok {
		deserializedKubeletConfig.ForceSendFields = append(deserializedKubeletConfig.ForceSendFields, fieldCpuCfsQuota)
	}
	if _, ok := deserializedKubeletConfigMap[fieldShutdownGracePeriodJSON]; ok {
		deserializedKubeletConfig.ForceSendFields = append(deserializedKubeletConfig.ForceSendFields, fieldShutdownGracePeriod)
	}
	if _, ok := deserializedKubeletConfigMap[fieldShutdownGracePeriodCriticalPodsJSON]; ok {
		deserializedKubeletConfig.ForceSendFields = append(deserializedKubeletConfig.ForceSendFields, fieldShutdownGracePeriodCriticalPods)
	}

	return deserializedKubeletConfig, nil
}

func evictionSignature(evictionSignals any, prefix string) string {
	var parts []string
	var memoryAvailable, nodefsAvailable, imagefsAvailable, imagefsInodesFree, nodefsInodesFree, pidAvailable string

	switch v := evictionSignals.(type) {
	case *gke_api_beta.EvictionSignals:
		if v == nil {
			return ""
		}
		memoryAvailable = v.MemoryAvailable
		nodefsAvailable = v.NodefsAvailable
		imagefsAvailable = v.ImagefsAvailable
		imagefsInodesFree = v.ImagefsInodesFree
		nodefsInodesFree = v.NodefsInodesFree
		pidAvailable = v.PidAvailable
	case *gke_api_beta.EvictionGracePeriod:
		if v == nil {
			return ""
		}
		memoryAvailable = v.MemoryAvailable
		nodefsAvailable = v.NodefsAvailable
		imagefsAvailable = v.ImagefsAvailable
		imagefsInodesFree = v.ImagefsInodesFree
		nodefsInodesFree = v.NodefsInodesFree
		pidAvailable = v.PidAvailable
	case *gke_api_beta.EvictionMinimumReclaim:
		if v == nil {
			return ""
		}
		memoryAvailable = v.MemoryAvailable
		nodefsAvailable = v.NodefsAvailable
		imagefsAvailable = v.ImagefsAvailable
		imagefsInodesFree = v.ImagefsInodesFree
		nodefsInodesFree = v.NodefsInodesFree
		pidAvailable = v.PidAvailable
	default:
		return ""
	}

	if memoryAvailable != "" {
		parts = append(parts, fmt.Sprintf("MemoryAvailable: %q", memoryAvailable))
	}
	if nodefsAvailable != "" {
		parts = append(parts, fmt.Sprintf("NodefsAvailable: %q", nodefsAvailable))
	}
	if imagefsAvailable != "" {
		parts = append(parts, fmt.Sprintf("ImagefsAvailable: %q", imagefsAvailable))
	}
	if imagefsInodesFree != "" {
		parts = append(parts, fmt.Sprintf("ImagefsInodesFree: %q", imagefsInodesFree))
	}
	if nodefsInodesFree != "" {
		parts = append(parts, fmt.Sprintf("NodefsInodesFree: %q", nodefsInodesFree))
	}
	if pidAvailable != "" {
		parts = append(parts, fmt.Sprintf("PidAvailable: %q", pidAvailable))
	}
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf("%s: <%s>", prefix, strings.Join(parts, ", "))
}

func kubeletConfigSignature(kubeletConfig *gke_api_beta.NodeKubeletConfig) string {
	if kubeletConfig == nil {
		return ""
	}

	var kubeletConfigParts []string
	if kubeletConfig.CpuCfsQuota || slices.Contains(kubeletConfig.ForceSendFields, "CpuCfsQuota") {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("CpuCfsQuota: %t", kubeletConfig.CpuCfsQuota))
	}
	if kubeletConfig.CpuCfsQuotaPeriod != "" {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("CpuCfsQuotaPeriod: %q", kubeletConfig.CpuCfsQuotaPeriod))
	}
	if kubeletConfig.CpuManagerPolicy != "" {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("CpuManagerPolicy: %q", kubeletConfig.CpuManagerPolicy))
	}
	if kubeletConfig.PodPidsLimit != 0 {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("PodPidsLimit: %d", kubeletConfig.PodPidsLimit))
	}
	if kubeletConfig.ImageGcLowThresholdPercent != 0 {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ImageGcLowThresholdPercent: %d", kubeletConfig.ImageGcLowThresholdPercent))
	}
	if kubeletConfig.ImageGcHighThresholdPercent != 0 {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ImageGcHighThresholdPercent: %d", kubeletConfig.ImageGcHighThresholdPercent))
	}
	if kubeletConfig.ImageMinimumGcAge != "" {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ImageMinimumGcAge: %q", kubeletConfig.ImageMinimumGcAge))
	}
	if kubeletConfig.ImageMaximumGcAge != "" {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ImageMaximumGcAge: %q", kubeletConfig.ImageMaximumGcAge))
	}
	if kubeletConfig.ContainerLogMaxSize != "" {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ContainerLogMaxSize: %q", kubeletConfig.ContainerLogMaxSize))
	}
	if kubeletConfig.ContainerLogMaxFiles != 0 {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ContainerLogMaxFiles: %d", kubeletConfig.ContainerLogMaxFiles))
	}
	if len(kubeletConfig.AllowedUnsafeSysctls) > 0 {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("AllowedUnsafeSysctls: %v", kubeletConfig.AllowedUnsafeSysctls))
	}
	if kubeletConfig.MaxParallelImagePulls != 0 {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("MaxParallelImagePulls: %d", kubeletConfig.MaxParallelImagePulls))
	}
	if kubeletConfig.SingleProcessOomKill {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("SingleProcessOomKill: %t", kubeletConfig.SingleProcessOomKill))
	}
	if sig := evictionSignature(kubeletConfig.EvictionSoft, "EvictionSoft"); sig != "" {
		kubeletConfigParts = append(kubeletConfigParts, sig)
	}
	if sig := evictionSignature(kubeletConfig.EvictionSoftGracePeriod, "EvictionSoftGracePeriod"); sig != "" {
		kubeletConfigParts = append(kubeletConfigParts, sig)
	}
	if sig := evictionSignature(kubeletConfig.EvictionMinimumReclaim, "EvictionMinimumReclaim"); sig != "" {
		kubeletConfigParts = append(kubeletConfigParts, sig)
	}
	if kubeletConfig.EvictionMaxPodGracePeriodSeconds != 0 {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("EvictionMaxPodGracePeriodSeconds: %d", kubeletConfig.EvictionMaxPodGracePeriodSeconds))
	}
	if tm := kubeletConfig.TopologyManager; tm != nil {
		if tm.Policy != "" {
			kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("TopologyManagerPolicy: %q", tm.Policy))
		}
		if tm.Scope != "" {
			kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("TopologyManagerScope: %q", tm.Scope))
		}
	}
	if mm := kubeletConfig.MemoryManager; mm != nil && mm.Policy != "" {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("MemoryManagerPolicy: %q", mm.Policy))
	}
	if slices.Contains(kubeletConfig.ForceSendFields, "ShutdownGracePeriodSeconds") {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ShutdownGracePeriodSeconds: %d", kubeletConfig.ShutdownGracePeriodSeconds))
	}
	if slices.Contains(kubeletConfig.ForceSendFields, "ShutdownGracePeriodCriticalPodsSeconds") {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("ShutdownGracePeriodCriticalPodsSeconds: %d", kubeletConfig.ShutdownGracePeriodCriticalPodsSeconds))
	}
	if kubeletConfig.CrashLoopBackOff != nil && kubeletConfig.CrashLoopBackOff.MaxContainerRestartPeriod != "" {
		kubeletConfigParts = append(kubeletConfigParts, fmt.Sprintf("CrashLoopBackOff: <maxContainerRestartPeriod: %q>", kubeletConfig.CrashLoopBackOff.MaxContainerRestartPeriod))
	}

	return fmt.Sprintf("kubelet-config: <%s>", strings.Join(kubeletConfigParts, ", "))
}

// configuredMaxPodsPerNodeFromLabels returns the configured MaxPodsPerNode.
// If it's configured it returns MaxPodsPerNode as an int, otherwise returns 0.
func configuredMaxPodsPerNodeFromLabels(systemLabels map[string]string) (int, error) {
	if systemLabels == nil {
		return 0, nil
	}
	if strMPPN, exists := systemLabels[labels.MaxPodsPerNodeLabel]; exists {
		mppn, err := strconv.Atoi(strMPPN)
		if err != nil {
			return 0, err
		}
		if mppn < 0 {
			return 0, fmt.Errorf("Invalid MaxPodsPerNode found, expected an int >= 0, instead found: %v", mppn)
		}
		return mppn, nil
	}
	return 0, nil
}

// getEstimatedNumberOfPods returns an estimated number of pods that could fit given machineType based on node group
// requirements. It's meant to be a very simple approximation, not bullet proof mechanism.
func getEstimatedNumberOfPods(estimatedNumberOfPods int, requirements nodeGroupRequirements, machineTypeInfo machinetypes.MachineType) int {
	if len(requirements.pods) == 0 {
		return 0
	}

	cpuSum := resource.Quantity{}
	memorySum := resource.Quantity{}
	gpuSum := resource.Quantity{}
	tpuSum := resource.Quantity{}
	for _, pod := range requirements.pods {
		podRequests := podutils.PodRequests(pod)
		cpuSum.Add(podRequests[apiv1.ResourceCPU])
		memorySum.Add(podRequests[apiv1.ResourceMemory])
		gpuSum.Add(podRequests[gpu.ResourceNvidiaGPU])
		tpuSum.Add(podRequests[tpu.ResourceGoogleTPU])
	}

	averageCpuMilli := cpuSum.MilliValue() / int64(len(requirements.pods))
	averageMemory := memorySum.Value() / int64(len(requirements.pods))
	averageGpuSum := gpuSum.Value() / int64(len(requirements.pods))
	averageTpuSum := tpuSum.Value() / int64(len(requirements.pods))
	if averageCpuMilli != 0 && int((machineTypeInfo.CPU*1000)/averageCpuMilli) < estimatedNumberOfPods {
		estimatedNumberOfPods = int((machineTypeInfo.CPU * 1000) / averageCpuMilli)
	}
	if averageMemory != 0 && int(machineTypeInfo.Memory/averageMemory) < estimatedNumberOfPods {
		estimatedNumberOfPods = int(machineTypeInfo.Memory / averageMemory)
	}
	if averageGpuSum != 0 && requirements.gpuRequest.Count != 0 && int(int64(requirements.gpuRequest.Count)/averageGpuSum) < estimatedNumberOfPods {
		estimatedNumberOfPods = int(int64(requirements.gpuRequest.Count) / averageGpuSum)
	}
	if averageTpuSum != 0 && int(requirements.tpuRequest.ChipsPerNode/averageTpuSum) < estimatedNumberOfPods {
		estimatedNumberOfPods = int(requirements.tpuRequest.ChipsPerNode / averageTpuSum)
	}

	return estimatedNumberOfPods
}

func capacityCheckWaitTimeSecondsSignature(computeClassRule rules.Rule) string {
	ccwt := "nil"
	if computeClassRule != nil && computeClassRule.SelfServiceMetadata() != nil {
		ccwt = computeClassRule.SelfServiceMetadata()[labels.CapacityCheckWaitTimeSecondsLabel]
	}
	return ccwt
}

func placementGroupSpec(ngReq *nodeGroupRequirements, labelReq podrequirements.LabelRequirements) placement.Spec {
	pgSpec := placement.FromRequirements(labelReq)
	// if there is a placement policy inferred from compute class.
	if ngReq.computeClassRule != nil && ngReq.computeClassRule.PlacementPolicy() != "" && pgSpec.Policy == "" {
		pgSpec.Policy = ngReq.computeClassRule.PlacementPolicy()
	}
	return pgSpec
}

// defaultEphemeralStorageLSSDCount returns the default number of local SSDs
// for ephemeral storage, after subtracting swap dedicated local SSDs, data
// cache, and NVME block local SSDs from the available number of local SSDs.
func defaultEphemeralStorageLSSDCount(ngReq nodeGroupRequirements, total int) int {
	defaultCount := total
	if ngReq.linuxNodeConfig != nil && ngReq.linuxNodeConfig.SwapConfig != nil {
		swapConfig := ngReq.linuxNodeConfig.SwapConfig
		if swapConfig.Enabled && swapConfig.DedicatedLocalSsdProfile != nil {
			defaultCount -= int(swapConfig.DedicatedLocalSsdProfile.DiskCount)
		}
	}
	// TODO(go/ccc-static-local-ssd): Add data cache and NVME block count.
	return defaultCount
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func derefInt32(i *int32) int32 {
	if i == nil {
		return 0
	}
	return *i
}

func derefBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}
