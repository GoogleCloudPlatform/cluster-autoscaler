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
	"reflect"
	"sort"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/klog/v2"
)

type linuxNodeConfig struct {
	sysctls                          map[string]string
	hugepages                        *hugepages
	transparentHugepageEnabled       *string
	transparentHugepageDefrag        *string
	accurateTimeConfig               *AccurateTimeConfig
	swapConfig                       *SwapConfig
	nodeVfioConfig                   *NodeVfioConfig
	diskIoScheduler                  *DiskIoScheduler
	additionalEtcHosts               []*EtcHostsEntry
	additionalEtcResolvConf          []*ResolvedConfEntry
	additionalEtcSystemdResolvedConf []*ResolvedConfEntry
	customNodeInit                   *CustomNodeInit
	kernelOverrides                  *KernelOverrides
	timeZone                         *string
}

type hugepages struct {
	hugepageSize1g *int64
	hugepageSize2m *int64
}

type NodeVfioConfig struct {
	DmaEntryLimit *int32
}

type DiskIoScheduler struct {
	NodeSystemIoScheduler       string
	NodeAttachedDiskIoScheduler string
}

type AccurateTimeConfig struct {
	EnablePtpKvmTimeSync *bool
}

type SwapConfig struct {
	Enabled                  bool
	EncryptionConfig         *SwapConfigEncryptionConfig
	BootDiskProfile          *SwapConfigBootDiskProfile
	EphemeralLocalSsdProfile *SwapConfigEphemeralLocalSsdProfile
	DedicatedLocalSsdProfile *SwapConfigDedicatedLocalSsdProfile
}

type SwapConfigEncryptionConfig struct {
	Disabled bool
}

type SwapConfigBootDiskProfile struct {
	SwapSizeGib     *int64
	SwapSizePercent *int32
}

type SwapConfigEphemeralLocalSsdProfile struct {
	SwapSizeGib     *int64
	SwapSizePercent *int32
}

type SwapConfigDedicatedLocalSsdProfile struct {
	DiskCount int64
}

type evictionThresholds struct {
	memoryAvailable   *string
	nodefsAvailable   *string
	imagefsAvailable  *string
	imagefsInodesFree *string
	nodefsInodesFree  *string
	pidAvailable      *string
}

type crashLoopBackOff struct {
	maxContainerRestartPeriod *string
}

type kubeletConfig struct {
	cpuCfsQuota                            *bool
	cpuCfsQuotaPeriod                      *string
	cpuManagerPolicy                       *string
	podPidsLimit                           *int64
	imageGcLowThresholdPercent             *int64
	imageGcHighThresholdPercent            *int64
	imageMinimumGcAge                      *string
	imageMaximumGcAge                      *string
	containerLogMaxSize                    *string
	containerLogMaxFiles                   *int64
	allowedUnsafeSysctls                   []string
	maxParallelImagePulls                  *int64
	singleProcessOOMKill                   *bool
	evictionSoft                           *evictionThresholds
	evictionSoftGracePeriod                *evictionThresholds
	evictionMinimumReclaim                 *evictionThresholds
	evictionMaxPodGracePeriodSeconds       *int64
	topologyManagerPolicy                  *string
	topologyManagerScope                   *string
	memoryManagerPolicy                    *string
	shutdownGracePeriodSeconds             *int64
	shutdownGracePeriodCriticalPodsSeconds *int64
	crashLoopBackOff                       *crashLoopBackOff
}

// NodeSystemConfigRule is an interface for rules with node system config.
type NodeSystemConfigRule interface {
	BaseRule
	Sysctls() map[string]string
	HugepageSize1g() *int64
	HugepageSize2m() *int64
	TransparentHugepageEnabled() *string
	TransparentHugepageDefrag() *string
	CpuCfsQuota() *bool
	CpuCfsQuotaPeriod() *string
	CpuManagerPolicy() *string
	PodPidsLimit() *int64
	ImageGcLowThresholdPercent() *int64
	ImageGcHighThresholdPercent() *int64
	ImageMinimumGcAge() *string
	ImageMaximumGcAge() *string
	ContainerLogMaxSize() *string
	ContainerLogMaxFiles() *int64
	AllowedUnsafeSysctls() []string
	MaxParallelImagePulls() *int64
	SingleProcessOOMKill() *bool
	EvictionSoftMemoryAvailable() *string
	EvictionSoftNodefsAvailable() *string
	EvictionSoftImagefsAvailable() *string
	EvictionSoftImagefsInodesFree() *string
	EvictionSoftNodefsInodesFree() *string
	EvictionSoftPidAvailable() *string
	EvictionSoftGracePeriodMemoryAvailable() *string
	EvictionSoftGracePeriodNodefsAvailable() *string
	EvictionSoftGracePeriodImagefsAvailable() *string
	EvictionSoftGracePeriodImagefsInodesFree() *string
	EvictionSoftGracePeriodNodefsInodesFree() *string
	EvictionSoftGracePeriodPidAvailable() *string
	EvictionMinimumReclaimMemoryAvailable() *string
	EvictionMinimumReclaimNodefsAvailable() *string
	EvictionMinimumReclaimImagefsAvailable() *string
	EvictionMinimumReclaimImagefsInodesFree() *string
	EvictionMinimumReclaimNodefsInodesFree() *string
	EvictionMinimumReclaimPidAvailable() *string
	EvictionMaxPodGracePeriodSeconds() *int64
	TopologyManagerPolicy() *string
	TopologyManagerScope() *string
	MemoryManagerPolicy() *string
	AccurateTimeConfig() *AccurateTimeConfig
	SwapConfig() *SwapConfig
	NodeVfioConfig() *NodeVfioConfig
	DiskIoScheduler() *DiskIoScheduler
	ShutdownGracePeriodSeconds() *int64
	ShutdownGracePeriodCriticalPodsSeconds() *int64
	CrashLoopBackOffMaxContainerRestartPeriod() *string
	AdditionalEtcHosts() []*EtcHostsEntry
	AdditionalEtcResolvConf() []*ResolvedConfEntry
	AdditionalEtcSystemdResolvedConf() []*ResolvedConfEntry
	CustomNodeInit() *CustomNodeInit
	KernelOverrides() *KernelOverrides
	TimeZone() *string
}

type nodeSystemConfigRule struct {
	linuxNodeConfig *linuxNodeConfig
	kubeletConfig   *kubeletConfig
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *nodeSystemConfigRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}
	// check for sysctls
	var ruleSysctls, npSysctls map[string]string
	if r.linuxNodeConfig != nil {
		ruleSysctls = r.linuxNodeConfig.sysctls
	}
	if mig.Spec() != nil && mig.Spec().LinuxNodeConfig != nil {
		npSysctls = mig.Spec().LinuxNodeConfig.Sysctls
	}
	// Every setting in rule should exist in node pool.
	for key, ruleValue := range ruleSysctls {
		if npValue, exists := npSysctls[key]; !exists || npValue != ruleValue {
			return false
		}
	}

	// check for hugepages
	var npHugepages *gkeclient.HugepagesConfig
	var ruleHugepages *hugepages
	if r.linuxNodeConfig != nil {
		ruleHugepages = r.linuxNodeConfig.hugepages
	}
	if mig.Spec() != nil && mig.Spec().LinuxNodeConfig != nil {
		npHugepages = mig.Spec().LinuxNodeConfig.Hugepages
	}
	if ruleHugepages != nil && npHugepages == nil {
		return false
	}
	if ruleHugepages != nil && npHugepages != nil {
		if ruleHugepages.hugepageSize1g != nil && *ruleHugepages.hugepageSize1g != npHugepages.HugepageSize1g {
			return false
		}
		if ruleHugepages.hugepageSize2m != nil && *ruleHugepages.hugepageSize2m != npHugepages.HugepageSize2m {
			return false
		}
	}

	// Check for transparent hugepage
	var npThpEnabled, npThpDefrag string
	var ruleThpEnabled, ruleThpDefrag *string
	if mig.Spec() != nil && mig.Spec().LinuxNodeConfig != nil {
		npThpEnabled = mig.Spec().LinuxNodeConfig.TransparentHugepageEnabled
		npThpDefrag = mig.Spec().LinuxNodeConfig.TransparentHugepageDefrag
	}
	if r.linuxNodeConfig != nil {
		ruleThpEnabled = r.linuxNodeConfig.transparentHugepageEnabled
		ruleThpDefrag = r.linuxNodeConfig.transparentHugepageDefrag
	}
	if ruleThpEnabled != nil && *ruleThpEnabled != npThpEnabled {
		return false
	}
	if ruleThpDefrag != nil && *ruleThpDefrag != npThpDefrag {
		return false
	}

	// Check for accurate time config
	var ruleEnablePtpKvmTimeSync bool
	var migEnablePtpKvmTimeSync bool
	if r.linuxNodeConfig != nil && r.linuxNodeConfig.accurateTimeConfig != nil && r.linuxNodeConfig.accurateTimeConfig.EnablePtpKvmTimeSync != nil {
		ruleEnablePtpKvmTimeSync = *r.linuxNodeConfig.accurateTimeConfig.EnablePtpKvmTimeSync
	}
	if mig.Spec() != nil && mig.Spec().LinuxNodeConfig != nil && mig.Spec().LinuxNodeConfig.AccurateTimeConfig != nil {
		migEnablePtpKvmTimeSync = mig.Spec().LinuxNodeConfig.AccurateTimeConfig.EnablePtpKvmTimeSync
	}
	if ruleEnablePtpKvmTimeSync != migEnablePtpKvmTimeSync {
		return false
	}

	// Check for swap
	var migSwapConfig *gkeclient.SwapConfig
	var ruleSwapConfig *SwapConfig
	if mig.Spec() != nil && mig.Spec().LinuxNodeConfig != nil && mig.Spec().LinuxNodeConfig.SwapConfig != nil {
		migSwapConfig = mig.Spec().LinuxNodeConfig.SwapConfig
	}
	if r.linuxNodeConfig != nil && r.linuxNodeConfig.swapConfig != nil {
		ruleSwapConfig = r.linuxNodeConfig.swapConfig
	}
	ruleSwapEnabled := ruleSwapConfig != nil && ruleSwapConfig.Enabled
	migSwapEnabled := migSwapConfig != nil && migSwapConfig.Enabled
	if ruleSwapEnabled != migSwapEnabled {
		return false
	}
	if ruleSwapEnabled {
		ruleEncrypted := ruleSwapConfig.EncryptionConfig == nil || !ruleSwapConfig.EncryptionConfig.Disabled
		migEncrypted := migSwapConfig.EncryptionConfig == nil || !migSwapConfig.EncryptionConfig.Disabled
		if ruleEncrypted != migEncrypted {
			return false
		}
		if ruleSwapConfig.BootDiskProfile != nil {
			if migSwapConfig.BootDiskProfile == nil {
				return false
			}
			if ruleSwapConfig.BootDiskProfile.SwapSizeGib != nil {
				if *ruleSwapConfig.BootDiskProfile.SwapSizeGib != migSwapConfig.BootDiskProfile.SwapSizeGib {
					return false
				}
			}
			if ruleSwapConfig.BootDiskProfile.SwapSizePercent != nil {
				if int64(*ruleSwapConfig.BootDiskProfile.SwapSizePercent) != migSwapConfig.BootDiskProfile.SwapSizePercent {
					return false
				}
			}
		}
		if ruleSwapConfig.EphemeralLocalSsdProfile != nil {
			if migSwapConfig.EphemeralLocalSsdProfile == nil {
				return false
			}
			if ruleSwapConfig.EphemeralLocalSsdProfile.SwapSizeGib != nil {
				if *ruleSwapConfig.EphemeralLocalSsdProfile.SwapSizeGib != migSwapConfig.EphemeralLocalSsdProfile.SwapSizeGib {
					return false
				}
			}
			if ruleSwapConfig.EphemeralLocalSsdProfile.SwapSizePercent != nil {
				if int64(*ruleSwapConfig.EphemeralLocalSsdProfile.SwapSizePercent) != migSwapConfig.EphemeralLocalSsdProfile.SwapSizePercent {
					return false
				}
			}
		}
		if ruleSwapConfig.DedicatedLocalSsdProfile != nil {
			if migSwapConfig.DedicatedLocalSsdProfile == nil {
				return false
			}
			if ruleSwapConfig.DedicatedLocalSsdProfile.DiskCount != migSwapConfig.DedicatedLocalSsdProfile.DiskCount {
				return false
			}
		}
	}

	// Check for node vfio config
	var ruleNodeVfioConfig *NodeVfioConfig
	var npNodeVfioConfig *gkeclient.NodeVfioConfig
	if r.linuxNodeConfig != nil {
		ruleNodeVfioConfig = r.linuxNodeConfig.nodeVfioConfig
	}
	if mig.Spec() != nil && mig.Spec().LinuxNodeConfig != nil {
		npNodeVfioConfig = mig.Spec().LinuxNodeConfig.NodeVfioConfig
	}
	if ruleNodeVfioConfig != nil && npNodeVfioConfig == nil {
		return false
	}
	if ruleNodeVfioConfig != nil && npNodeVfioConfig != nil {
		if ruleNodeVfioConfig.DmaEntryLimit != nil && int64(*ruleNodeVfioConfig.DmaEntryLimit) != npNodeVfioConfig.DmaEntryLimit {
			return false
		}
	}

	// Check for disk io scheduler
	var ruleDiskIoScheduler *DiskIoScheduler
	var npDiskIoScheduler *gkeclient.DiskIoScheduler
	if r.linuxNodeConfig != nil {
		ruleDiskIoScheduler = r.linuxNodeConfig.diskIoScheduler
	}
	if mig.Spec() != nil && mig.Spec().LinuxNodeConfig != nil {
		npDiskIoScheduler = mig.Spec().LinuxNodeConfig.DiskIoScheduler
	}
	if ruleDiskIoScheduler != nil && npDiskIoScheduler == nil {
		return false
	}
	if ruleDiskIoScheduler != nil && npDiskIoScheduler != nil {
		if ruleDiskIoScheduler.NodeSystemIoScheduler != "" && ruleDiskIoScheduler.NodeSystemIoScheduler != npDiskIoScheduler.NodeSystemIoScheduler {
			return false
		}
		if ruleDiskIoScheduler.NodeAttachedDiskIoScheduler != "" && ruleDiskIoScheduler.NodeAttachedDiskIoScheduler != npDiskIoScheduler.NodeAttachedDiskIoScheduler {
			return false
		}
	}

	// Check for kubelet config.
	var npKubeletConfig *gke_api_beta.NodeKubeletConfig
	var ruleKubeletConfig *kubeletConfig = r.kubeletConfig
	if mig.Spec() != nil && mig.Spec().KubeletConfig != nil {
		npKubeletConfig = mig.Spec().KubeletConfig
	}
	if ruleKubeletConfig != nil && npKubeletConfig == nil {
		return false
	}
	if ruleKubeletConfig != nil && npKubeletConfig != nil {
		if ruleKubeletConfig.cpuCfsQuota != nil && *ruleKubeletConfig.cpuCfsQuota != npKubeletConfig.CpuCfsQuota {
			return false
		}
		if ruleKubeletConfig.cpuCfsQuotaPeriod != nil && *ruleKubeletConfig.cpuCfsQuotaPeriod != npKubeletConfig.CpuCfsQuotaPeriod {
			return false
		}
		if ruleKubeletConfig.cpuManagerPolicy != nil && *ruleKubeletConfig.cpuManagerPolicy != npKubeletConfig.CpuManagerPolicy {
			return false
		}
		if ruleKubeletConfig.podPidsLimit != nil && *ruleKubeletConfig.podPidsLimit != npKubeletConfig.PodPidsLimit {
			return false
		}
		if ruleKubeletConfig.imageGcLowThresholdPercent != nil && *ruleKubeletConfig.imageGcLowThresholdPercent != npKubeletConfig.ImageGcLowThresholdPercent {
			return false
		}
		if ruleKubeletConfig.imageGcHighThresholdPercent != nil && *ruleKubeletConfig.imageGcHighThresholdPercent != npKubeletConfig.ImageGcHighThresholdPercent {
			return false
		}
		if ruleKubeletConfig.imageMinimumGcAge != nil && *ruleKubeletConfig.imageMinimumGcAge != npKubeletConfig.ImageMinimumGcAge {
			return false
		}
		if ruleKubeletConfig.imageMaximumGcAge != nil && *ruleKubeletConfig.imageMaximumGcAge != npKubeletConfig.ImageMaximumGcAge {
			return false
		}
		if ruleKubeletConfig.containerLogMaxSize != nil && *ruleKubeletConfig.containerLogMaxSize != npKubeletConfig.ContainerLogMaxSize {
			return false
		}
		if ruleKubeletConfig.containerLogMaxFiles != nil && *ruleKubeletConfig.containerLogMaxFiles != npKubeletConfig.ContainerLogMaxFiles {
			return false
		}
		if ruleKubeletConfig.allowedUnsafeSysctls != nil && !stringArrayDeepEqual(ruleKubeletConfig.allowedUnsafeSysctls, npKubeletConfig.AllowedUnsafeSysctls) {
			return false
		}
		if ruleKubeletConfig.maxParallelImagePulls != nil && *ruleKubeletConfig.maxParallelImagePulls != npKubeletConfig.MaxParallelImagePulls {
			return false
		}
		if ruleKubeletConfig.singleProcessOOMKill != nil && *ruleKubeletConfig.singleProcessOOMKill != npKubeletConfig.SingleProcessOomKill {
			return false
		}
		if ruleKubeletConfig.evictionSoft != nil && !compareEvictionSoft(ruleKubeletConfig.evictionSoft, npKubeletConfig.EvictionSoft) {
			return false
		}
		if ruleKubeletConfig.evictionSoftGracePeriod != nil && !compareEvictionSoftGracePeriod(ruleKubeletConfig.evictionSoftGracePeriod, npKubeletConfig.EvictionSoftGracePeriod) {
			return false
		}
		if ruleKubeletConfig.evictionMinimumReclaim != nil && !compareEvictionMinimumReclaim(ruleKubeletConfig.evictionMinimumReclaim, npKubeletConfig.EvictionMinimumReclaim) {
			return false
		}
		if ruleKubeletConfig.evictionMaxPodGracePeriodSeconds != nil && *ruleKubeletConfig.evictionMaxPodGracePeriodSeconds != npKubeletConfig.EvictionMaxPodGracePeriodSeconds {
			return false
		}
		if ruleKubeletConfig.topologyManagerPolicy != nil && (npKubeletConfig.TopologyManager == nil || *ruleKubeletConfig.topologyManagerPolicy != npKubeletConfig.TopologyManager.Policy) {
			return false
		}
		if ruleKubeletConfig.topologyManagerScope != nil && (npKubeletConfig.TopologyManager == nil || *ruleKubeletConfig.topologyManagerScope != npKubeletConfig.TopologyManager.Scope) {
			return false
		}
		if ruleKubeletConfig.memoryManagerPolicy != nil {
			if npKubeletConfig.MemoryManager == nil {
				// The "None" policy is the default, so if the rule specifies "None" and the config is nil, it's a match.
				if *ruleKubeletConfig.memoryManagerPolicy != "None" {
					return false
				}
			} else if *ruleKubeletConfig.memoryManagerPolicy != npKubeletConfig.MemoryManager.Policy {
				return false
			}
		}
		if ruleKubeletConfig.shutdownGracePeriodSeconds != nil && *ruleKubeletConfig.shutdownGracePeriodSeconds != npKubeletConfig.ShutdownGracePeriodSeconds {
			return false
		}
		if ruleKubeletConfig.shutdownGracePeriodCriticalPodsSeconds != nil && *ruleKubeletConfig.shutdownGracePeriodCriticalPodsSeconds != npKubeletConfig.ShutdownGracePeriodCriticalPodsSeconds {
			return false
		}
		if ruleKubeletConfig.crashLoopBackOff != nil {
			if npKubeletConfig.CrashLoopBackOff == nil {
				return false
			}
			if ruleKubeletConfig.crashLoopBackOff.maxContainerRestartPeriod != nil && *ruleKubeletConfig.crashLoopBackOff.maxContainerRestartPeriod != npKubeletConfig.CrashLoopBackOff.MaxContainerRestartPeriod {
				return false
			}
		}
	}

	if r.linuxNodeConfig != nil {
		// Check AdditionalEtcHosts
		if r.linuxNodeConfig.additionalEtcHosts != nil {
			if mig.Spec() == nil || mig.Spec().LinuxNodeConfig == nil || !compareEtcHosts(r.linuxNodeConfig.additionalEtcHosts, mig.Spec().LinuxNodeConfig.AdditionalEtcHosts) {
				return false
			}
		}
		// Check AdditionalEtcResolvConf
		if r.linuxNodeConfig.additionalEtcResolvConf != nil {
			if mig.Spec() == nil || mig.Spec().LinuxNodeConfig == nil || !compareResolvedConf(r.linuxNodeConfig.additionalEtcResolvConf, mig.Spec().LinuxNodeConfig.AdditionalEtcResolvConf) {
				return false
			}
		}
		// Check AdditionalEtcSystemdResolvedConf
		if r.linuxNodeConfig.additionalEtcSystemdResolvedConf != nil {
			if mig.Spec() == nil || mig.Spec().LinuxNodeConfig == nil || !compareResolvedConf(r.linuxNodeConfig.additionalEtcSystemdResolvedConf, mig.Spec().LinuxNodeConfig.AdditionalEtcSystemdResolvedConf) {
				return false
			}
		}
		// Check CustomNodeInit
		if r.linuxNodeConfig.customNodeInit != nil {
			if mig.Spec() == nil || mig.Spec().LinuxNodeConfig == nil || !compareCustomNodeInit(r.linuxNodeConfig.customNodeInit, mig.Spec().LinuxNodeConfig.CustomNodeInit) {
				return false
			}
		}
		// Check KernelOverrides
		if r.linuxNodeConfig.kernelOverrides != nil {
			if mig.Spec() == nil || mig.Spec().LinuxNodeConfig == nil || !compareKernelOverrides(r.linuxNodeConfig.kernelOverrides, mig.Spec().LinuxNodeConfig.KernelOverrides) {
				return false
			}
		}
		// Check TimeZone
		if r.linuxNodeConfig.timeZone != nil {
			if mig.Spec() == nil || mig.Spec().LinuxNodeConfig == nil || *r.linuxNodeConfig.timeZone != mig.Spec().LinuxNodeConfig.TimeZone {
				return false
			}
		}
	}
	return true
}
func compareEvictionThresholds(rule *evictionThresholds, mem, nodefs, imagefs, imagefsInodes, nodefsInodes, pid string) bool {
	if rule.memoryAvailable != nil && *rule.memoryAvailable != mem {
		return false
	}
	if rule.nodefsAvailable != nil && *rule.nodefsAvailable != nodefs {
		return false
	}
	if rule.imagefsAvailable != nil && *rule.imagefsAvailable != imagefs {
		return false
	}
	if rule.imagefsInodesFree != nil && *rule.imagefsInodesFree != imagefsInodes {
		return false
	}
	if rule.nodefsInodesFree != nil && *rule.nodefsInodesFree != nodefsInodes {
		return false
	}
	if rule.pidAvailable != nil && *rule.pidAvailable != pid {
		return false
	}
	return true
}

// compareEvictionSoft compares the rule for EvictionSoft against the node pool's config.
func compareEvictionSoft(rule *evictionThresholds, np *gke_api_beta.EvictionSignals) bool {
	if np == nil {
		return false // Rule exists, but the config doesn't.
	}
	return compareEvictionThresholds(rule, np.MemoryAvailable, np.NodefsAvailable, np.ImagefsAvailable, np.ImagefsInodesFree, np.NodefsInodesFree, np.PidAvailable)
}

// compareEvictionSoftGracePeriod compares the rule for EvictionSoftGracePeriod against the node pool's config.
func compareEvictionSoftGracePeriod(rule *evictionThresholds, np *gke_api_beta.EvictionGracePeriod) bool {
	if np == nil {
		return false
	}
	return compareEvictionThresholds(rule, np.MemoryAvailable, np.NodefsAvailable, np.ImagefsAvailable, np.ImagefsInodesFree, np.NodefsInodesFree, np.PidAvailable)
}

// compareEvictionMinimumReclaim compares the rule for EvictionMinimumReclaim against the node pool's config.
func compareEvictionMinimumReclaim(rule *evictionThresholds, np *gke_api_beta.EvictionMinimumReclaim) bool {
	if np == nil {
		return false
	}
	return compareEvictionThresholds(rule, np.MemoryAvailable, np.NodefsAvailable, np.ImagefsAvailable, np.ImagefsInodesFree, np.NodefsInodesFree, np.PidAvailable)
}

func (r *nodeSystemConfigRule) Sysctls() map[string]string {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.sysctls
}

func (r *nodeSystemConfigRule) HugepageSize1g() *int64 {
	if r.linuxNodeConfig == nil || r.linuxNodeConfig.hugepages == nil {
		return nil
	}
	return r.linuxNodeConfig.hugepages.hugepageSize1g
}

func (r *nodeSystemConfigRule) HugepageSize2m() *int64 {
	if r.linuxNodeConfig == nil || r.linuxNodeConfig.hugepages == nil {
		return nil
	}
	return r.linuxNodeConfig.hugepages.hugepageSize2m
}

func (r *nodeSystemConfigRule) TransparentHugepageEnabled() *string {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.transparentHugepageEnabled
}

func (r *nodeSystemConfigRule) TransparentHugepageDefrag() *string {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.transparentHugepageDefrag
}

func (r *nodeSystemConfigRule) CpuCfsQuota() *bool {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.cpuCfsQuota
}

func (r *nodeSystemConfigRule) CpuCfsQuotaPeriod() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.cpuCfsQuotaPeriod
}

func (r *nodeSystemConfigRule) CpuManagerPolicy() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.cpuManagerPolicy
}

func (r *nodeSystemConfigRule) PodPidsLimit() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.podPidsLimit
}

func (r *nodeSystemConfigRule) ImageGcLowThresholdPercent() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.imageGcLowThresholdPercent
}

func (r *nodeSystemConfigRule) ImageGcHighThresholdPercent() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.imageGcHighThresholdPercent
}

func (r *nodeSystemConfigRule) ImageMinimumGcAge() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.imageMinimumGcAge
}

func (r *nodeSystemConfigRule) ImageMaximumGcAge() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.imageMaximumGcAge
}

func (r *nodeSystemConfigRule) ContainerLogMaxSize() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.containerLogMaxSize
}

func (r *nodeSystemConfigRule) ContainerLogMaxFiles() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.containerLogMaxFiles
}

func (r *nodeSystemConfigRule) AllowedUnsafeSysctls() []string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.allowedUnsafeSysctls
}

func (r *nodeSystemConfigRule) MaxParallelImagePulls() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.maxParallelImagePulls
}

func (r *nodeSystemConfigRule) SingleProcessOOMKill() *bool {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.singleProcessOOMKill
}

func (r *nodeSystemConfigRule) EvictionSoftMemoryAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoft == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoft.memoryAvailable
}

func (r *nodeSystemConfigRule) EvictionSoftNodefsAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoft == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoft.nodefsAvailable
}

func (r *nodeSystemConfigRule) EvictionSoftImagefsAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoft == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoft.imagefsAvailable
}

func (r *nodeSystemConfigRule) EvictionSoftImagefsInodesFree() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoft == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoft.imagefsInodesFree
}

func (r *nodeSystemConfigRule) EvictionSoftNodefsInodesFree() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoft == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoft.nodefsInodesFree
}

func (r *nodeSystemConfigRule) EvictionSoftPidAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoft == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoft.pidAvailable
}

func (r *nodeSystemConfigRule) EvictionSoftGracePeriodMemoryAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoftGracePeriod == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoftGracePeriod.memoryAvailable
}

func (r *nodeSystemConfigRule) EvictionSoftGracePeriodNodefsAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoftGracePeriod == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoftGracePeriod.nodefsAvailable
}

func (r *nodeSystemConfigRule) EvictionSoftGracePeriodImagefsAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoftGracePeriod == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoftGracePeriod.imagefsAvailable
}

func (r *nodeSystemConfigRule) EvictionSoftGracePeriodImagefsInodesFree() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoftGracePeriod == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoftGracePeriod.imagefsInodesFree
}

func (r *nodeSystemConfigRule) EvictionSoftGracePeriodNodefsInodesFree() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoftGracePeriod == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoftGracePeriod.nodefsInodesFree
}

func (r *nodeSystemConfigRule) EvictionSoftGracePeriodPidAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionSoftGracePeriod == nil {
		return nil
	}
	return r.kubeletConfig.evictionSoftGracePeriod.pidAvailable
}

func (r *nodeSystemConfigRule) EvictionMinimumReclaimMemoryAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionMinimumReclaim == nil {
		return nil
	}
	return r.kubeletConfig.evictionMinimumReclaim.memoryAvailable
}

func (r *nodeSystemConfigRule) EvictionMinimumReclaimNodefsAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionMinimumReclaim == nil {
		return nil
	}
	return r.kubeletConfig.evictionMinimumReclaim.nodefsAvailable
}

func (r *nodeSystemConfigRule) EvictionMinimumReclaimImagefsAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionMinimumReclaim == nil {
		return nil
	}
	return r.kubeletConfig.evictionMinimumReclaim.imagefsAvailable
}

func (r *nodeSystemConfigRule) EvictionMinimumReclaimImagefsInodesFree() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionMinimumReclaim == nil {
		return nil
	}
	return r.kubeletConfig.evictionMinimumReclaim.imagefsInodesFree
}

func (r *nodeSystemConfigRule) EvictionMinimumReclaimNodefsInodesFree() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionMinimumReclaim == nil {
		return nil
	}
	return r.kubeletConfig.evictionMinimumReclaim.nodefsInodesFree
}

func (r *nodeSystemConfigRule) EvictionMinimumReclaimPidAvailable() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.evictionMinimumReclaim == nil {
		return nil
	}
	return r.kubeletConfig.evictionMinimumReclaim.pidAvailable
}

func (r *nodeSystemConfigRule) EvictionMaxPodGracePeriodSeconds() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.evictionMaxPodGracePeriodSeconds
}

func (r *nodeSystemConfigRule) TopologyManagerPolicy() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.topologyManagerPolicy
}

func (r *nodeSystemConfigRule) TopologyManagerScope() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.topologyManagerScope
}

func (r *nodeSystemConfigRule) MemoryManagerPolicy() *string {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.memoryManagerPolicy
}

func (r *nodeSystemConfigRule) AccurateTimeConfig() *AccurateTimeConfig {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.accurateTimeConfig
}

func (r *nodeSystemConfigRule) NodeVfioConfig() *NodeVfioConfig {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.nodeVfioConfig
}

func (r *nodeSystemConfigRule) DiskIoScheduler() *DiskIoScheduler {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.diskIoScheduler
}

func (r *nodeSystemConfigRule) SwapConfig() *SwapConfig {
	if r.linuxNodeConfig == nil || r.linuxNodeConfig.swapConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.swapConfig
}

func (r *nodeSystemConfigRule) SwapDedicatedLSSDCount() int64 {
	if r.linuxNodeConfig == nil || r.linuxNodeConfig.swapConfig == nil ||
		r.linuxNodeConfig.swapConfig.DedicatedLocalSsdProfile == nil {
		return 0
	}
	return r.linuxNodeConfig.swapConfig.DedicatedLocalSsdProfile.DiskCount
}

func (r *nodeSystemConfigRule) ShutdownGracePeriodSeconds() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.shutdownGracePeriodSeconds
}

func (r *nodeSystemConfigRule) ShutdownGracePeriodCriticalPodsSeconds() *int64 {
	if r.kubeletConfig == nil {
		return nil
	}
	return r.kubeletConfig.shutdownGracePeriodCriticalPodsSeconds
}

func (r *nodeSystemConfigRule) CrashLoopBackOffMaxContainerRestartPeriod() *string {
	if r.kubeletConfig == nil || r.kubeletConfig.crashLoopBackOff == nil {
		return nil
	}
	return r.kubeletConfig.crashLoopBackOff.maxContainerRestartPeriod
}
func (r *nodeSystemConfigRule) AdditionalEtcHosts() []*EtcHostsEntry {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.additionalEtcHosts
}

func (r *nodeSystemConfigRule) AdditionalEtcResolvConf() []*ResolvedConfEntry {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.additionalEtcResolvConf
}

func (r *nodeSystemConfigRule) AdditionalEtcSystemdResolvedConf() []*ResolvedConfEntry {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.additionalEtcSystemdResolvedConf
}

func (r *nodeSystemConfigRule) CustomNodeInit() *CustomNodeInit {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.customNodeInit
}

func (r *nodeSystemConfigRule) KernelOverrides() *KernelOverrides {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.kernelOverrides
}

func (r *nodeSystemConfigRule) TimeZone() *string {
	if r.linuxNodeConfig == nil {
		return nil
	}
	return r.linuxNodeConfig.timeZone
}

func WithSysctlsRule(sysctls map[string]string) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.sysctls = sysctls
	}
}

func WithHugepageSize1gRule(hugepageSize1g int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		if r.nodeSystemConfigRule.linuxNodeConfig.hugepages == nil {
			r.nodeSystemConfigRule.linuxNodeConfig.hugepages = &hugepages{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.hugepages.hugepageSize1g = &hugepageSize1g
	}
}

func WithHugepageSize2mRule(hugepageSize2m int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		if r.nodeSystemConfigRule.linuxNodeConfig.hugepages == nil {
			r.nodeSystemConfigRule.linuxNodeConfig.hugepages = &hugepages{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.hugepages.hugepageSize2m = &hugepageSize2m
	}
}

func WithTransparentHugepageEnabledRule(thpEnabled string) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.transparentHugepageEnabled = &thpEnabled
	}
}

func WithTransparentHugepageDefragRule(thpDefrag string) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.transparentHugepageDefrag = &thpDefrag
	}
}

func WithCpuCfsQuotaRule(cpuCfsQuota bool) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.cpuCfsQuota = &cpuCfsQuota
	}
}

func WithCpuCfsQuotaPeriodRule(cpuCfsQuotaPeriod string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.cpuCfsQuotaPeriod = &cpuCfsQuotaPeriod
	}
}

func WithCpuManagerPolicyRule(cpuManagerPolicy string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.cpuManagerPolicy = &cpuManagerPolicy
	}
}

func WithPodPidsLimitRule(podPidsLimit int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.podPidsLimit = &podPidsLimit
	}
}

func WithImageGcLowThresholdPercentRule(imageGcLowThresholdPercent int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.imageGcLowThresholdPercent = &imageGcLowThresholdPercent
	}
}

func WithImageGcHighThresholdPercentRule(imageGcHighThresholdPercent int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.imageGcHighThresholdPercent = &imageGcHighThresholdPercent
	}
}

func WithImageMinimumGcAgeRule(imageMinimumGcAge string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.imageMinimumGcAge = &imageMinimumGcAge
	}
}

func WithImageMaximumGcAgeRule(imageMaximumGcAge string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.imageMaximumGcAge = &imageMaximumGcAge
	}
}

func WithContainerLogMaxSizeRule(containerLogMaxSize string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.containerLogMaxSize = &containerLogMaxSize
	}
}

func WithContainerLogMaxFilesRule(containerLogMaxFiles int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.containerLogMaxFiles = &containerLogMaxFiles
	}
}

func WithAllowedUnsafeSysctlsRule(allowedUnsafeSysctls []string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.allowedUnsafeSysctls = allowedUnsafeSysctls
	}
}

func WithMaxParallelImagePullsRule(maxParallelImagePulls int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.maxParallelImagePulls = &maxParallelImagePulls
	}
}
func WithSingleProcessOOMKill(singleProcessOOMKill bool) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.singleProcessOOMKill = &singleProcessOOMKill
	}
}

func WithEvictionSoftMemoryAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoft == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoft = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoft.memoryAvailable = &val
	}
}

func WithEvictionSoftNodefsAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoft == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoft = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoft.nodefsAvailable = &val
	}
}

func WithEvictionSoftImagefsAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoft == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoft = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoft.imagefsAvailable = &val
	}
}

func WithEvictionSoftImagefsInodesFreeRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoft == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoft = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoft.imagefsInodesFree = &val
	}
}

func WithEvictionSoftNodefsInodesFreeRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoft == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoft = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoft.nodefsInodesFree = &val
	}
}

func WithEvictionSoftPidAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoft == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoft = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoft.pidAvailable = &val
	}
}

func WithEvictionSoftGracePeriodMemoryAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod.memoryAvailable = &val
	}
}

func WithEvictionSoftGracePeriodNodefsAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod.nodefsAvailable = &val
	}
}

func WithEvictionSoftGracePeriodImagefsAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod.imagefsAvailable = &val
	}
}

func WithEvictionSoftGracePeriodImagefsInodesFreeRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod.imagefsInodesFree = &val
	}
}

func WithEvictionSoftGracePeriodNodefsInodesFreeRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod.nodefsInodesFree = &val
	}
}

func WithEvictionSoftGracePeriodPidAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionSoftGracePeriod.pidAvailable = &val
	}
}

func WithEvictionMinimumReclaimMemoryAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim.memoryAvailable = &val
	}
}

func WithEvictionMinimumReclaimNodefsAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim.nodefsAvailable = &val
	}
}

func WithEvictionMinimumReclaimImagefsAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim.imagefsAvailable = &val
	}
}

func WithEvictionMinimumReclaimImagefsInodesFreeRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim.imagefsInodesFree = &val
	}
}

func WithEvictionMinimumReclaimNodefsInodesFreeRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim.nodefsInodesFree = &val
	}
}

func WithEvictionMinimumReclaimPidAvailableRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim == nil {
			r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim = &evictionThresholds{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionMinimumReclaim.pidAvailable = &val
	}
}

func WithEvictionMaxPodGracePeriodSecondsRule(val int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.evictionMaxPodGracePeriodSeconds = &val
	}
}

func WithTopologyManagerPolicyRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.topologyManagerPolicy = &val
	}
}

func WithTopologyManagerScopeRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.topologyManagerScope = &val
	}
}

func WithMemoryManagerPolicyRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.memoryManagerPolicy = &val
	}
}

func WithAccurateTimeConfigRule(config ccc_api.AccurateTimeConfig) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		if r.nodeSystemConfigRule.linuxNodeConfig.accurateTimeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig.accurateTimeConfig = &AccurateTimeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.accurateTimeConfig.EnablePtpKvmTimeSync = config.EnablePtpKvmTimeSync
	}
}

func WithSwapConfigRule(config ccc_api.SwapConfig) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		swapConfig := &SwapConfig{
			Enabled: config.Enabled,
		}
		swapConfig.EncryptionConfig = &SwapConfigEncryptionConfig{
			Disabled: false,
		}
		if config.EncryptionConfig != nil {
			swapConfig.EncryptionConfig = &SwapConfigEncryptionConfig{
				Disabled: config.EncryptionConfig.Disabled,
			}
		}
		if config.BootDiskProfile != nil {
			swapConfig.BootDiskProfile = &SwapConfigBootDiskProfile{}
			if config.BootDiskProfile.SwapSizeGib != nil {
				swapConfig.BootDiskProfile.SwapSizeGib = config.BootDiskProfile.SwapSizeGib
			}
			if config.BootDiskProfile.SwapSizePercent != nil {
				swapConfig.BootDiskProfile.SwapSizePercent = config.BootDiskProfile.SwapSizePercent
			}
		}
		if config.EphemeralLocalSsdProfile != nil {
			swapConfig.EphemeralLocalSsdProfile = &SwapConfigEphemeralLocalSsdProfile{}
			if config.EphemeralLocalSsdProfile.SwapSizeGib != nil {
				swapConfig.EphemeralLocalSsdProfile.SwapSizeGib = config.EphemeralLocalSsdProfile.SwapSizeGib
			}
			if config.EphemeralLocalSsdProfile.SwapSizePercent != nil {
				swapConfig.EphemeralLocalSsdProfile.SwapSizePercent = config.EphemeralLocalSsdProfile.SwapSizePercent
			}
		}
		if config.DedicatedLocalSsdProfile != nil {
			swapConfig.DedicatedLocalSsdProfile = &SwapConfigDedicatedLocalSsdProfile{
				DiskCount: config.DedicatedLocalSsdProfile.DiskCount,
			}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.swapConfig = swapConfig
	}
}

func WithNodeVfioConfigRule(config ccc_api.NodeVfioConfig) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		if r.nodeSystemConfigRule.linuxNodeConfig.nodeVfioConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig.nodeVfioConfig = &NodeVfioConfig{}
		}
		if config.DmaEntryLimit != nil {
			r.nodeSystemConfigRule.linuxNodeConfig.nodeVfioConfig.DmaEntryLimit = config.DmaEntryLimit
		}
	}
}

func WithDiskIoSchedulerRule(config ccc_api.DiskIoScheduler) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.diskIoScheduler = &DiskIoScheduler{
			NodeSystemIoScheduler:       config.NodeSystemIoScheduler,
			NodeAttachedDiskIoScheduler: config.NodeAttachedDiskIoScheduler,
		}
	}
}

func WithShutdownGracePeriodSecondsRule(val int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.shutdownGracePeriodSeconds = &val
	}
}

func WithShutdownGracePeriodCriticalPodsSecondsRule(val int64) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		r.nodeSystemConfigRule.kubeletConfig.shutdownGracePeriodCriticalPodsSeconds = &val
	}
}

func WithCrashLoopBackOffMaxContainerRestartPeriodRule(val string) RuleOption {
	return func(r *rule) {
		if r.nodeSystemConfigRule.kubeletConfig == nil {
			r.nodeSystemConfigRule.kubeletConfig = &kubeletConfig{}
		}
		if r.nodeSystemConfigRule.kubeletConfig.crashLoopBackOff == nil {
			r.nodeSystemConfigRule.kubeletConfig.crashLoopBackOff = &crashLoopBackOff{}
		}
		r.nodeSystemConfigRule.kubeletConfig.crashLoopBackOff.maxContainerRestartPeriod = &val
	}
}

func stringArrayDeepEqual(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	copied1 := make([]string, len(s1))
	copied2 := make([]string, len(s2))
	copy(copied1, s1)
	copy(copied2, s2)

	sort.Strings(copied1)
	sort.Strings(copied2)
	return reflect.DeepEqual(copied1, copied2)
}

type EtcHostsEntry struct {
	Ip   string
	Host string
}

type ResolvedConfEntry struct {
	Key   string
	Value []string
}

type InitScript struct {
	GcsUri                    *string
	GcsGeneration             *int64
	Args                      []string
	GcpSecretManagerSecretUri *string
}

type CustomNodeInit struct {
	InitScript *InitScript
}

type KernelCommandlineOverrides struct {
	SpecRstackOverflow *string
	InitOnAlloc        *string
}

type LRUGen struct {
	Enabled  *bool
	MinTtlMs *int32
}

type KernelOverrides struct {
	KernelCommandlineOverrides *KernelCommandlineOverrides
	LruGen                     *LRUGen
}

func WithAdditionalEtcHostsRule(val []*EtcHostsEntry) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.additionalEtcHosts = val
	}
}

func WithAdditionalEtcResolvConfRule(val []*ResolvedConfEntry) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.additionalEtcResolvConf = val
	}
}

func WithAdditionalEtcSystemdResolvedConfRule(val []*ResolvedConfEntry) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.additionalEtcSystemdResolvedConf = val
	}
}

func WithCustomNodeInitRule(val *CustomNodeInit) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.customNodeInit = val
	}
}

func WithKernelOverridesRule(val *KernelOverrides) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.kernelOverrides = val
	}
}

func WithTimeZoneRule(val string) RuleOption {
	return func(r *rule) {
		if r.linuxNodeConfig == nil {
			r.nodeSystemConfigRule.linuxNodeConfig = &linuxNodeConfig{}
		}
		r.nodeSystemConfigRule.linuxNodeConfig.timeZone = &val
	}
}

func compareEtcHosts(rule []*EtcHostsEntry, np []*gkeclient.EtcHostsEntry) bool {
	if len(rule) != len(np) {
		return false
	}
	for i := range rule {
		if rule[i].Ip != np[i].Ip || rule[i].Host != np[i].Host {
			return false
		}
	}
	return true
}

func compareResolvedConf(rule []*ResolvedConfEntry, np []*gkeclient.ResolvedConfEntry) bool {
	if len(rule) != len(np) {
		return false
	}
	for i := range rule {
		if rule[i].Key != np[i].Key || !stringArrayDeepEqual(rule[i].Value, np[i].Value) {
			return false
		}
	}
	return true
}

func compareCustomNodeInit(rule *CustomNodeInit, np *gkeclient.CustomNodeInit) bool {
	if rule == nil && np == nil {
		return true
	}
	if rule == nil || np == nil {
		return false
	}
	return compareInitScript(rule.InitScript, np.InitScript)
}

func compareInitScript(rule *InitScript, np *gkeclient.InitScript) bool {
	if rule == nil && np == nil {
		return true
	}
	if rule == nil || np == nil {
		return false
	}
	if (rule.GcsUri == nil && np.GcsUri != "") || (rule.GcsUri != nil && *rule.GcsUri != np.GcsUri) {
		return false
	}
	if (rule.GcsGeneration == nil && np.GcsGeneration != 0) || (rule.GcsGeneration != nil && *rule.GcsGeneration != np.GcsGeneration) {
		return false
	}
	if (rule.GcpSecretManagerSecretUri == nil && np.GcpSecretManagerSecretUri != "") || (rule.GcpSecretManagerSecretUri != nil && *rule.GcpSecretManagerSecretUri != np.GcpSecretManagerSecretUri) {
		return false
	}
	return stringArrayDeepEqual(rule.Args, np.Args)
}

func compareKernelOverrides(rule *KernelOverrides, np *gkeclient.KernelOverrides) bool {
	if rule == nil && np == nil {
		return true
	}
	if rule == nil || np == nil {
		return false
	}
	if !compareKernelCommandlineOverrides(rule.KernelCommandlineOverrides, np.KernelCommandlineOverrides) {
		return false
	}
	return compareLruGen(rule.LruGen, np.LruGen)
}

func compareKernelCommandlineOverrides(rule *KernelCommandlineOverrides, np *gkeclient.KernelCommandlineOverrides) bool {
	if rule == nil && np == nil {
		return true
	}
	if rule == nil || np == nil {
		return false
	}
	if (rule.SpecRstackOverflow == nil && np.SpecRstackOverflow != "") || (rule.SpecRstackOverflow != nil && *rule.SpecRstackOverflow != np.SpecRstackOverflow) {
		return false
	}
	if (rule.InitOnAlloc == nil && np.InitOnAlloc != "") || (rule.InitOnAlloc != nil && *rule.InitOnAlloc != np.InitOnAlloc) {
		return false
	}
	return true
}

func compareLruGen(rule *LRUGen, np *gkeclient.LRUGen) bool {
	if rule == nil && np == nil {
		return true
	}
	if rule == nil || np == nil {
		return false
	}
	if (rule.Enabled == nil && np.Enabled != false) || (rule.Enabled != nil && *rule.Enabled != np.Enabled) {
		return false
	}
	if (rule.MinTtlMs == nil && np.MinTtlMs != 0) || (rule.MinTtlMs != nil && int64(*rule.MinTtlMs) != np.MinTtlMs) {
		return false
	}
	return true
}
