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

package gkeclient

import gke_api_beta "google.golang.org/api/container/v1beta1"

// LinuxNodeConfig - parameters that can be configured on Linux nodes.
type LinuxNodeConfig struct {
	AccurateTimeConfig               *AccurateTimeConfig      `json:"accurateTimeConfig,omitempty"`
	AdditionalEtcHosts               []*EtcHostsEntry         `json:"additionalEtcHosts,omitempty"`
	AdditionalEtcResolvConf          []*ResolvedConfEntry     `json:"additionalEtcResolvConf,omitempty"`
	AdditionalEtcSystemdResolvedConf []*ResolvedConfEntry     `json:"additionalEtcSystemdResolvedConf,omitempty"`
	CgroupMode                       string                   `json:"cgroupMode,omitempty"`
	CustomNodeInit                   *CustomNodeInit          `json:"customNodeInit,omitempty"`
	Hugepages                        *HugepagesConfig         `json:"hugepages,omitempty"`
	KernelOverrides                  *KernelOverrides         `json:"kernelOverrides,omitempty"`
	NodeKernelModuleLoading          *NodeKernelModuleLoading `json:"nodeKernelModuleLoading,omitempty"`
	SwapConfig                       *SwapConfig              `json:"swapConfig,omitempty"`
	Sysctls                          map[string]string        `json:"sysctls,omitempty"`
	TimeZone                         string                   `json:"timeZone,omitempty"`
	TransparentHugepageDefrag        string                   `json:"transparentHugepageDefrag,omitempty"`
	TransparentHugepageEnabled       string                   `json:"transparentHugepageEnabled,omitempty"`
}

// AccurateTimeConfig - configuration for the accurate time synchronization feature.
type AccurateTimeConfig struct {
	EnablePtpKvmTimeSync bool `json:"enablePtpKvmTimeSync,omitempty"`
}

// EtcHostsEntry - additional entries to be added to /etc/hosts.
type EtcHostsEntry struct {
	Host string `json:"host,omitempty"`
	Ip   string `json:"ip,omitempty"`
}

// ResolvedConfEntry - additional entries to be added to resolved.conf.
type ResolvedConfEntry struct {
	Key   string   `json:"key,omitempty"`
	Value []string `json:"value,omitempty"`
}

// CustomNodeInit - support for running custom init code while bootstrapping nodes.
type CustomNodeInit struct {
	InitScript *InitScript `json:"initScript,omitempty"`
}

// InitScript - provides a simple bash script to be executed on the node.
type InitScript struct {
	Args                      []string `json:"args,omitempty"`
	GcpSecretManagerSecretUri string   `json:"gcpSecretManagerSecretUri,omitempty"`
	GcsGeneration             int64    `json:"gcsGeneration,omitempty,string"`
	GcsUri                    string   `json:"gcsUri,omitempty"`
}

// HugepagesConfig - Hugepages amount in both 2m and 1g size
type HugepagesConfig struct {
	HugepageSize1g int64 `json:"hugepageSize1g,omitempty"`
	HugepageSize2m int64 `json:"hugepageSize2m,omitempty"`
}

// KernelOverrides - parameters that can be configured on the kernel.
type KernelOverrides struct {
	KernelCommandlineOverrides *KernelCommandlineOverrides `json:"kernelCommandlineOverrides,omitempty"`
	LruGen                     *LRUGen                     `json:"lruGen,omitempty"`
}

// KernelCommandlineOverrides - definition of possible additional kernel command line arguments.
type KernelCommandlineOverrides struct {
	InitOnAlloc        string `json:"initOnAlloc,omitempty"`
	SpecRstackOverflow string `json:"specRstackOverflow,omitempty"`
}

// LRUGen - lrugen (Multi-Gen LRU) options.
type LRUGen struct {
	Enabled  bool  `json:"enabled,omitempty"`
	MinTtlMs int64 `json:"minTtlMs,omitempty"`
}

// NodeKernelModuleLoading - configuration for kernel module loading on nodes.
type NodeKernelModuleLoading struct {
	Policy string `json:"policy,omitempty"`
}

// SwapConfig - configuration for swap memory on a node pool.
type SwapConfig struct {
	BootDiskProfile          *BootDiskProfile          `json:"bootDiskProfile,omitempty"`
	DedicatedLocalSsdProfile *DedicatedLocalSsdProfile `json:"dedicatedLocalSsdProfile,omitempty"`
	Enabled                  bool                      `json:"enabled,omitempty"`
	EncryptionConfig         *EncryptionConfig         `json:"encryptionConfig,omitempty"`
	EphemeralLocalSsdProfile *EphemeralLocalSsdProfile `json:"ephemeralLocalSsdProfile,omitempty"`
}

// DedicatedLocalSsdProfile - provisions a new, separate local NVMe SSD
// exclusively for swap.
type DedicatedLocalSsdProfile struct {
	DiskCount int64 `json:"diskCount,omitempty,string"`
}

// BootDiskProfile - swap on the node's boot disk.
type BootDiskProfile struct {
	SwapSizeGib     int64 `json:"swapSizeGib,omitempty,string"`
	SwapSizePercent int64 `json:"swapSizePercent,omitempty"`
}

// EncryptionConfig - defines encryption settings for the swap space.
type EncryptionConfig struct {
	Disabled bool `json:"disabled,omitempty"`
}

// EphemeralLocalSsdProfile - swap on the local SSD shared with pod ephemeral
// storage.
type EphemeralLocalSsdProfile struct {
	SwapSizeGib     int64 `json:"swapSizeGib,omitempty,string"`
	SwapSizePercent int64 `json:"swapSizePercent,omitempty"`
}

func linuxNodeConfig(c *gke_api_beta.LinuxNodeConfig) *LinuxNodeConfig {
	if c == nil {
		return nil
	}
	return &LinuxNodeConfig{
		AccurateTimeConfig:         accurateTimeConfig(c.AccurateTimeConfig),
		CgroupMode:                 c.CgroupMode,
		Hugepages:                  hugepagesConfig(c.Hugepages),
		NodeKernelModuleLoading:    nodeKernelModuleLoading(c.NodeKernelModuleLoading),
		SwapConfig:                 swapConfig(c.SwapConfig),
		Sysctls:                    c.Sysctls,
		TransparentHugepageDefrag:  c.TransparentHugepageDefrag,
		TransparentHugepageEnabled: c.TransparentHugepageEnabled,
	}
}

func accurateTimeConfig(c *gke_api_beta.AccurateTimeConfig) *AccurateTimeConfig {
	if c == nil {
		return nil
	}
	return &AccurateTimeConfig{
		EnablePtpKvmTimeSync: c.EnablePtpKvmTimeSync,
	}
}

func hugepagesConfig(c *gke_api_beta.HugepagesConfig) *HugepagesConfig {
	if c == nil {
		return nil
	}
	return &HugepagesConfig{
		HugepageSize1g: c.HugepageSize1g,
		HugepageSize2m: c.HugepageSize2m,
	}
}

func nodeKernelModuleLoading(c *gke_api_beta.NodeKernelModuleLoading) *NodeKernelModuleLoading {
	if c == nil {
		return nil
	}
	return &NodeKernelModuleLoading{
		Policy: c.Policy,
	}
}

func swapConfig(c *gke_api_beta.SwapConfig) *SwapConfig {
	if c == nil {
		return nil
	}
	return &SwapConfig{
		BootDiskProfile:          bootDiskProfile(c.BootDiskProfile),
		DedicatedLocalSsdProfile: dedicatedLocalSsdProfile(c.DedicatedLocalSsdProfile),
		Enabled:                  c.Enabled,
		EncryptionConfig:         encryptionConfig(c.EncryptionConfig),
		EphemeralLocalSsdProfile: ephemeralLocalSsdProfile(c.EphemeralLocalSsdProfile),
	}
}

func bootDiskProfile(p *gke_api_beta.BootDiskProfile) *BootDiskProfile {
	if p == nil {
		return nil
	}
	return &BootDiskProfile{
		SwapSizeGib:     p.SwapSizeGib,
		SwapSizePercent: p.SwapSizePercent,
	}
}

func dedicatedLocalSsdProfile(p *gke_api_beta.DedicatedLocalSsdProfile) *DedicatedLocalSsdProfile {
	if p == nil {
		return nil
	}
	return &DedicatedLocalSsdProfile{
		DiskCount: p.DiskCount,
	}
}

func encryptionConfig(c *gke_api_beta.EncryptionConfig) *EncryptionConfig {
	if c == nil {
		return nil
	}
	return &EncryptionConfig{
		Disabled: c.Disabled,
	}
}

func ephemeralLocalSsdProfile(p *gke_api_beta.EphemeralLocalSsdProfile) *EphemeralLocalSsdProfile {
	if p == nil {
		return nil
	}
	return &EphemeralLocalSsdProfile{
		SwapSizeGib:     p.SwapSizeGib,
		SwapSizePercent: p.SwapSizePercent,
	}
}

func v1beta1LinuxNodeConfig(c *LinuxNodeConfig) *gke_api_beta.LinuxNodeConfig {
	if c == nil {
		return nil
	}
	return &gke_api_beta.LinuxNodeConfig{
		AccurateTimeConfig:         v1beta1AccurateTimeConfig(c.AccurateTimeConfig),
		CgroupMode:                 c.CgroupMode,
		Hugepages:                  v1beta1HugepagesConfig(c.Hugepages),
		NodeKernelModuleLoading:    v1beta1NodeKernelModuleLoading(c.NodeKernelModuleLoading),
		SwapConfig:                 v1beta1SwapConfig(c.SwapConfig),
		Sysctls:                    c.Sysctls,
		TransparentHugepageDefrag:  c.TransparentHugepageDefrag,
		TransparentHugepageEnabled: c.TransparentHugepageEnabled,
	}
}

func v1beta1AccurateTimeConfig(c *AccurateTimeConfig) *gke_api_beta.AccurateTimeConfig {
	if c == nil {
		return nil
	}
	return &gke_api_beta.AccurateTimeConfig{
		EnablePtpKvmTimeSync: c.EnablePtpKvmTimeSync,
	}
}

func v1beta1HugepagesConfig(c *HugepagesConfig) *gke_api_beta.HugepagesConfig {
	if c == nil {
		return nil
	}
	return &gke_api_beta.HugepagesConfig{
		HugepageSize1g: c.HugepageSize1g,
		HugepageSize2m: c.HugepageSize2m,
	}
}

func v1beta1NodeKernelModuleLoading(c *NodeKernelModuleLoading) *gke_api_beta.NodeKernelModuleLoading {
	if c == nil {
		return nil
	}
	return &gke_api_beta.NodeKernelModuleLoading{
		Policy: c.Policy,
	}
}

func v1beta1SwapConfig(c *SwapConfig) *gke_api_beta.SwapConfig {
	if c == nil {
		return nil
	}
	return &gke_api_beta.SwapConfig{
		BootDiskProfile:          v1beta1BootDiskProfile(c.BootDiskProfile),
		DedicatedLocalSsdProfile: v1beta1DedicatedLocalSsdProfile(c.DedicatedLocalSsdProfile),
		Enabled:                  c.Enabled,
		EncryptionConfig:         v1beta1EncryptionConfig(c.EncryptionConfig),
		EphemeralLocalSsdProfile: v1beta1EphemeralLocalSsdProfile(c.EphemeralLocalSsdProfile),
	}
}

func v1beta1BootDiskProfile(p *BootDiskProfile) *gke_api_beta.BootDiskProfile {
	if p == nil {
		return nil
	}
	return &gke_api_beta.BootDiskProfile{
		SwapSizeGib:     p.SwapSizeGib,
		SwapSizePercent: p.SwapSizePercent,
	}
}

func v1beta1DedicatedLocalSsdProfile(p *DedicatedLocalSsdProfile) *gke_api_beta.DedicatedLocalSsdProfile {
	if p == nil {
		return nil
	}
	return &gke_api_beta.DedicatedLocalSsdProfile{
		DiskCount: p.DiskCount,
	}
}

func v1beta1EncryptionConfig(c *EncryptionConfig) *gke_api_beta.EncryptionConfig {
	if c == nil {
		return nil
	}
	return &gke_api_beta.EncryptionConfig{
		Disabled: c.Disabled,
	}
}

func v1beta1EphemeralLocalSsdProfile(p *EphemeralLocalSsdProfile) *gke_api_beta.EphemeralLocalSsdProfile {
	if p == nil {
		return nil
	}
	return &gke_api_beta.EphemeralLocalSsdProfile{
		SwapSizeGib:     p.SwapSizeGib,
		SwapSizePercent: p.SwapSizePercent,
	}
}
