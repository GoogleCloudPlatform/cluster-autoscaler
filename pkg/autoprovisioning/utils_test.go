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
	"fmt"
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestAutoprovisionedNodeGroupsCount(t *testing.T) {
	for tn, tc := range map[string]struct {
		nonAutoprovisionedCount int
		autoprovisionedCount    int
		expected                int
	}{
		"no node groups": {
			nonAutoprovisionedCount: 0,
			autoprovisionedCount:    0,
			expected:                0,
		},
		"only non-autoprovisioned": {
			nonAutoprovisionedCount: 13,
			autoprovisionedCount:    0,
			expected:                0,
		},
		"only autoprovisioned": {
			nonAutoprovisionedCount: 0,
			autoprovisionedCount:    37,
			expected:                37,
		},
		"both": {
			nonAutoprovisionedCount: 13,
			autoprovisionedCount:    37,
			expected:                37,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			var nodeGroups []cloudprovider.NodeGroup
			for i := 0; i < tc.nonAutoprovisionedCount; i++ {
				nodeGroups = append(nodeGroups, test.NewTestNodeGroup(fmt.Sprintf("ng-%d", tc.nonAutoprovisionedCount), 1, 1, 1, true, false, "", nil, nil))
			}
			for i := 0; i < tc.autoprovisionedCount; i++ {
				nodeGroups = append(nodeGroups, test.NewTestNodeGroup(fmt.Sprintf("ng-%d", tc.nonAutoprovisionedCount), 1, 1, 1, false, true, "", nil, nil))
			}
			got := autoprovisionedNodeGroupsCount(nodeGroups)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestLinuxNodeConfigSignature(t *testing.T) {
	for tn, tc := range map[string]struct {
		linuxNodeConfig *gkeclient.LinuxNodeConfig
		expected        string
	}{
		"nil config": {
			linuxNodeConfig: nil,
			expected:        "",
		},
		"empty config": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{},
			expected:        "linuxNodeConfig: <>",
		},
		"config with sysctls": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
				},
			},
			expected: "linuxNodeConfig: <Sysctls: <net.ipv4.tcp_max_syn_backlog:1024>>",
		},
		"config with hugepages": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
			expected: "linuxNodeConfig: <Hugepages: <hugepage_size1g: 3, hugepage_size2m: 1024>>",
		},
		"config with sysctls and hugepages": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
				},
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
			expected: "linuxNodeConfig: <Sysctls: <net.ipv4.tcp_max_syn_backlog:1024>, Hugepages: <hugepage_size1g: 3, hugepage_size2m: 1024>>",
		},
		"config with all fields": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
				},
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
				TransparentHugepageDefrag:  "TRANSPARENT_HUGEPAGE_DEFRAG_NEVER",
				TransparentHugepageEnabled: "TRANSPARENT_HUGEPAGE_ENABLED_ALWAYS",
			},
			expected: "linuxNodeConfig: <Sysctls: <net.ipv4.tcp_max_syn_backlog:1024>, Hugepages: <hugepage_size1g: 3, hugepage_size2m: 1024>, TransparentHugepageEnabled: \"TRANSPARENT_HUGEPAGE_ENABLED_ALWAYS\", TransparentHugepageDefrag: \"TRANSPARENT_HUGEPAGE_DEFRAG_NEVER\">",
		},
		"config with accurate time": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				AccurateTimeConfig: &gkeclient.AccurateTimeConfig{
					EnablePtpKvmTimeSync: true,
				},
			},
			expected: "linuxNodeConfig: <AccurateTimeConfig: <EnablePtpKvmTimeSync: true>>",
		},
		"config with NodeVfioConfig": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				NodeVfioConfig: &gkeclient.NodeVfioConfig{
					DmaEntryLimit: 65536,
				},
			},
			expected: "linuxNodeConfig: <NodeVfioConfig: <dmaEntryLimit: 65536>>",
		},
		"config with DiskIoScheduler": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				DiskIoScheduler: &gkeclient.DiskIoScheduler{
					NodeSystemIoScheduler:       "mq-deadline",
					NodeAttachedDiskIoScheduler: "bfq",
				},
			},
			expected: "linuxNodeConfig: <DiskIoScheduler: <nodeSystemIoScheduler: \"mq-deadline\", nodeAttachedDiskIoScheduler: \"bfq\">>",
		},
		"config with swap boot disk": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				SwapConfig: &gkeclient.SwapConfig{
					Enabled: true,
					BootDiskProfile: &gkeclient.BootDiskProfile{
						SwapSizeGib: 10,
					},
				},
			},
			expected: "linuxNodeConfig: <swapConfig: <enabled: true, bootDiskProfile: <swapSizeGib: 10, swapSizePercent: 0>>>",
		},
		"config with swap boot disk encryption disabled": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
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
			expected: "linuxNodeConfig: <swapConfig: <enabled: true, encryptionConfig: <disabled: true>, bootDiskProfile: <swapSizeGib: 10, swapSizePercent: 0>>>",
		},
		"config with swap ephemeral local ssd": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				SwapConfig: &gkeclient.SwapConfig{
					Enabled: true,
					EphemeralLocalSsdProfile: &gkeclient.EphemeralLocalSsdProfile{
						SwapSizeGib: 10,
					},
				},
			},
			expected: "linuxNodeConfig: <swapConfig: <enabled: true, ephemeralLocalSsdProfile: <swapSizeGib: 10, swapSizePercent: 0>>>",
		},
		"config with swap dedicated local ssd": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				SwapConfig: &gkeclient.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
						DiskCount: 1,
					},
				},
			},
			expected: "linuxNodeConfig: <swapConfig: <enabled: true, dedicatedLocalSsdProfile: <diskCount: 1>>>",
		},
		"config with new fields": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				AdditionalEtcHosts: []*gkeclient.EtcHostsEntry{
					{Ip: "1.2.3.4", Host: "host1"},
				},
				AdditionalEtcResolvConf: []*gkeclient.ResolvedConfEntry{
					{Key: "nameserver", Value: []string{"8.8.8.8"}},
				},
				AdditionalEtcSystemdResolvedConf: []*gkeclient.ResolvedConfEntry{
					{Key: "DNS", Value: []string{"8.8.4.4"}},
				},
				CustomNodeInit: &gkeclient.CustomNodeInit{
					InitScript: &gkeclient.InitScript{
						GcsUri:                    "gs://bucket/script.sh",
						GcsGeneration:             123,
						Args:                      []string{"arg1"},
						GcpSecretManagerSecretUri: "secret-uri",
					},
				},
				KernelOverrides: &gkeclient.KernelOverrides{
					KernelCommandlineOverrides: &gkeclient.KernelCommandlineOverrides{
						SpecRstackOverflow: "OFF",
						InitOnAlloc:        "OFF",
					},
					LruGen: &gkeclient.LRUGen{
						Enabled:  true,
						MinTtlMs: 1000,
					},
				},
				TimeZone: "UTC",
			},
			expected: "linuxNodeConfig: <AdditionalEtcHosts: [<1.2.3.4:host1>], AdditionalEtcResolvConf: [<nameserver:8.8.8.8>], AdditionalEtcSystemdResolvedConf: [<DNS:8.8.4.4>], CustomNodeInit: <GcsUri: \"gs://bucket/script.sh\", GcsGeneration: 123, Args: [arg1], GcpSecretManagerSecretUri: \"secret-uri\">, KernelOverrides: <KernelCommandlineOverrides: <SpecRstackOverflow: \"OFF\", InitOnAlloc: \"OFF\">, LruGen: <Enabled: true, MinTtlMs: 1000>>, TimeZone: \"UTC\">",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := linuxNodeConfigSignature(tc.linuxNodeConfig)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestSerializeLinuxNodeConfig(t *testing.T) {
	for tn, tc := range map[string]struct {
		linuxNodeConfig *gkeclient.LinuxNodeConfig
		expected        string
		expectErr       bool
	}{
		"nil config": {
			linuxNodeConfig: nil,
			expected:        "",
			expectErr:       false,
		},
		"empty config": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{},
			expected:        `{}`,
			expectErr:       false,
		},
		"config with sysctls": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
				},
			},
			expected:  `{"sysctls":{"net.ipv4.tcp_max_syn_backlog":"1024"}}`,
			expectErr: false,
		},
		"config with hugepages": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
			expected: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024}}`,
		},
		"config with sysctls and hugepages": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
				},
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
			expected: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024},"sysctls":{"net.ipv4.tcp_max_syn_backlog":"1024"}}`,
		},
		"config with all fields": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
				},
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
				TransparentHugepageDefrag:  "TRANSPARENT_HUGEPAGE_DEFRAG_MADVISE",
				TransparentHugepageEnabled: "TRANSPARENT_HUGEPAGE_ENABLED_NEVER",
			},
			expected: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024},"sysctls":{"net.ipv4.tcp_max_syn_backlog":"1024"},"transparentHugepageDefrag":"TRANSPARENT_HUGEPAGE_DEFRAG_MADVISE","transparentHugepageEnabled":"TRANSPARENT_HUGEPAGE_ENABLED_NEVER"}`,
		},
		"config with new fields": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				AdditionalEtcHosts: []*gkeclient.EtcHostsEntry{
					{Ip: "1.2.3.4", Host: "host1"},
				},
				TimeZone: "UTC",
			},
			expected: `{"additionalEtcHosts":[{"host":"host1","ip":"1.2.3.4"}],"timeZone":"UTC"}`,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got, err := serializeLinuxNodeConfig(tc.linuxNodeConfig)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestDeserializeLinuxNodeConfig(t *testing.T) {
	for tn, tc := range map[string]struct {
		linuxNodeConfig string
		expected        *gkeclient.LinuxNodeConfig
		expectErr       bool
	}{
		"nil config": {
			linuxNodeConfig: "",
			expected:        nil,
			expectErr:       false,
		},
		"empty config": {
			linuxNodeConfig: `{}`,
			expected:        &gkeclient.LinuxNodeConfig{},
			expectErr:       false,
		},
		"config with sysctls": {
			linuxNodeConfig: `{"sysctls":{"net.core.somaxconn":"2048","net.ipv4.tcp_max_syn_backlog":"1024"}}`,
			expected: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
					"net.core.somaxconn":           "2048",
				},
			},
			expectErr: false,
		},
		"config with hugepages": {
			linuxNodeConfig: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024}}`,
			expected: &gkeclient.LinuxNodeConfig{
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
		},
		"config with sysctls and hugepages": {
			linuxNodeConfig: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024},"sysctls":{"net.ipv4.tcp_max_syn_backlog":"1024"}}`,
			expected: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.ipv4.tcp_max_syn_backlog": "1024",
				},
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
		},
		"invalid config": {
			linuxNodeConfig: `invalid}`,
			expected:        nil,
			expectErr:       true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got, err := deserializeLinuxNodeConfig(tc.linuxNodeConfig)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestSerializeKubeletConfig(t *testing.T) {
	for tn, tc := range map[string]struct {
		kubeletConfig *gke_api_beta.NodeKubeletConfig
		expected      string
		expectErr     bool
	}{
		"nil config": {
			kubeletConfig: nil,
			expected:      "",
			expectErr:     false,
		},
		"empty config": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			expected:      `{}`,
			expectErr:     false,
		},
		"config with fields": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:                true,
				CpuCfsQuotaPeriod:          "100ms",
				CpuManagerPolicy:           "static",
				PodPidsLimit:               10000,
				ShutdownGracePeriodSeconds: 30,
				CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
					MaxContainerRestartPeriod: "10s",
				},
			},
			expected:  `{"cpuCfsQuota":true,"cpuCfsQuotaPeriod":"100ms","cpuManagerPolicy":"static","crashLoopBackOff":{"maxContainerRestartPeriod":"10s"},"podPidsLimit":"10000","shutdownGracePeriodSeconds":30}`,
			expectErr: false,
		},
		"config with forcesendfields": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:                true,
				CpuCfsQuotaPeriod:          "100ms",
				CpuManagerPolicy:           "static",
				PodPidsLimit:               10000,
				ShutdownGracePeriodSeconds: 0,
				ForceSendFields:            []string{"ShutdownGracePeriodSeconds"},
			},
			expected:  `{"cpuCfsQuota":true,"cpuCfsQuotaPeriod":"100ms","cpuManagerPolicy":"static","podPidsLimit":"10000","shutdownGracePeriodSeconds":0}`,
			expectErr: false,
		},
		"config without forcesendfields": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota: true,
			},
			expected:  `{"cpuCfsQuota":true}`,
			expectErr: false,
		},
		"config without forcesendfields and cpuCfsQuota=false": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota: false,
			},
			expected:  `{}`,
			expectErr: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got, err := serializeKubeletConfig(tc.kubeletConfig)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestKubeletConfigFromCCRuleAndSerialize(t *testing.T) {
	testCases := map[string]struct {
		rule         rules.Rule
		expectedJSON string
	}{
		"CpuCfsQuota explicitly false": {
			rule:         rules.NewRule(rules.WithCpuCfsQuotaRule(false)),
			expectedJSON: `{"cpuCfsQuota":false}`,
		},
		"CpuCfsQuota explicitly true": {
			rule:         rules.NewRule(rules.WithCpuCfsQuotaRule(true)),
			expectedJSON: `{"cpuCfsQuota":true}`,
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			kubeletConfig := kubeletConfigFromCCRule(tc.rule)
			gotJSON, err := serializeKubeletConfig(kubeletConfig)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedJSON, gotJSON)
		})
	}
}

func TestDeserializeKubeletConfig(t *testing.T) {
	for tn, tc := range map[string]struct {
		kubeletConfig string
		expected      *gke_api_beta.NodeKubeletConfig
		expectErr     bool
	}{
		"nil config": {
			kubeletConfig: "",
			expected:      nil,
			expectErr:     false,
		},
		"empty config": {
			kubeletConfig: `{}`,
			expected:      &gke_api_beta.NodeKubeletConfig{},
			expectErr:     false,
		},
		"config with fields": {
			kubeletConfig: `{"cpuCfsQuota":true,"cpuCfsQuotaPeriod":"100ms","cpuManagerPolicy":"static","crashLoopBackOff":{"maxContainerRestartPeriod":"10s"},"podPidsLimit":"10000"}`,
			expected: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:       true,
				CpuCfsQuotaPeriod: "100ms",
				CpuManagerPolicy:  "static",
				CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
					MaxContainerRestartPeriod: "10s",
				},
				PodPidsLimit:    10000,
				ForceSendFields: []string{"CpuCfsQuota"},
			},
			expectErr: false,
		},
		"config with forcesendfields": {
			kubeletConfig: `{"cpuCfsQuota":true,"cpuCfsQuotaPeriod":"100ms","cpuManagerPolicy":"static","podPidsLimit":"10000","shutdownGracePeriodSeconds":0}`,
			expected: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:                true,
				CpuCfsQuotaPeriod:          "100ms",
				CpuManagerPolicy:           "static",
				PodPidsLimit:               10000,
				ShutdownGracePeriodSeconds: 0,
				ForceSendFields:            []string{"CpuCfsQuota", "ShutdownGracePeriodSeconds"},
			},
			expectErr: false,
		},
		"config with forcesendfields and cpuCfsQuota false": {
			kubeletConfig: `{"cpuCfsQuota":false,"cpuCfsQuotaPeriod":"100ms","cpuManagerPolicy":"static","podPidsLimit":"10000"}`,
			expected: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:       false,
				CpuCfsQuotaPeriod: "100ms",
				CpuManagerPolicy:  "static",
				PodPidsLimit:      10000,
				ForceSendFields:   []string{"CpuCfsQuota"},
			},
			expectErr: false,
		},
		"invalid config": {
			kubeletConfig: `invalid}`,
			expected:      nil,
			expectErr:     true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got, err := deserializeKubeletConfig(tc.kubeletConfig)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}
func TestKubeletConfigSignature(t *testing.T) {
	for tn, tc := range map[string]struct {
		kubeletConfig *gke_api_beta.NodeKubeletConfig
		expected      string
	}{
		"nil config": {
			kubeletConfig: nil,
			expected:      "",
		},
		"empty config": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			expected:      "kubelet-config: <>",
		},
		"config with cpuCfsQuota=false": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:     false,
				ForceSendFields: []string{"CpuCfsQuota"},
			},
			expected: "kubelet-config: <CpuCfsQuota: false>",
		},
		"config with all fields": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:                      true,
				CpuCfsQuotaPeriod:                "100ms",
				CpuManagerPolicy:                 "static",
				PodPidsLimit:                     10000,
				ImageGcLowThresholdPercent:       80,
				ImageGcHighThresholdPercent:      90,
				ImageMinimumGcAge:                "2m",
				ImageMaximumGcAge:                "1h",
				ContainerLogMaxSize:              "10M",
				ContainerLogMaxFiles:             5,
				MaxParallelImagePulls:            5,
				AllowedUnsafeSysctls:             []string{"net.ipv4.tcp_max_syn_backlog"},
				SingleProcessOomKill:             true,
				EvictionMaxPodGracePeriodSeconds: 60,
				EvictionSoft: &gke_api_beta.EvictionSignals{
					MemoryAvailable:   "2Gi",
					NodefsAvailable:   "10%",
					ImagefsAvailable:  "15%",
					ImagefsInodesFree: "5%",
					NodefsInodesFree:  "5%",
					PidAvailable:      "10%",
				},
				EvictionSoftGracePeriod: &gke_api_beta.EvictionGracePeriod{
					MemoryAvailable:   "2m",
					NodefsAvailable:   "90s",
					ImagefsAvailable:  "90s",
					ImagefsInodesFree: "90s",
					NodefsInodesFree:  "90s",
					PidAvailable:      "90s",
				},
				EvictionMinimumReclaim: &gke_api_beta.EvictionMinimumReclaim{
					MemoryAvailable:   "5%",
					NodefsAvailable:   "5%",
					ImagefsAvailable:  "5%",
					ImagefsInodesFree: "1%",
					NodefsInodesFree:  "1%",
					PidAvailable:      "1%",
				},
				TopologyManager: &gke_api_beta.TopologyManager{
					Policy: "best-effort",
					Scope:  "container",
				},
				MemoryManager: &gke_api_beta.MemoryManager{
					Policy: "Static",
				},
				ShutdownGracePeriodSeconds:             120,
				ShutdownGracePeriodCriticalPodsSeconds: 0,
				CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
					MaxContainerRestartPeriod: "10s",
				},
				ForceSendFields: []string{"CpuCfsQuota", "ShutdownGracePeriodSeconds", "ShutdownGracePeriodCriticalPodsSeconds"},
			},
			expected: "kubelet-config: <CpuCfsQuota: true, CpuCfsQuotaPeriod: \"100ms\", CpuManagerPolicy: \"static\", PodPidsLimit: 10000, ImageGcLowThresholdPercent: 80, ImageGcHighThresholdPercent: 90, ImageMinimumGcAge: \"2m\", ImageMaximumGcAge: \"1h\", ContainerLogMaxSize: \"10M\", ContainerLogMaxFiles: 5, AllowedUnsafeSysctls: [net.ipv4.tcp_max_syn_backlog], MaxParallelImagePulls: 5, SingleProcessOomKill: true, EvictionSoft: <MemoryAvailable: \"2Gi\", NodefsAvailable: \"10%\", ImagefsAvailable: \"15%\", ImagefsInodesFree: \"5%\", NodefsInodesFree: \"5%\", PidAvailable: \"10%\">, EvictionSoftGracePeriod: <MemoryAvailable: \"2m\", NodefsAvailable: \"90s\", ImagefsAvailable: \"90s\", ImagefsInodesFree: \"90s\", NodefsInodesFree: \"90s\", PidAvailable: \"90s\">, EvictionMinimumReclaim: <MemoryAvailable: \"5%\", NodefsAvailable: \"5%\", ImagefsAvailable: \"5%\", ImagefsInodesFree: \"1%\", NodefsInodesFree: \"1%\", PidAvailable: \"1%\">, EvictionMaxPodGracePeriodSeconds: 60, TopologyManagerPolicy: \"best-effort\", TopologyManagerScope: \"container\", MemoryManagerPolicy: \"Static\", ShutdownGracePeriodSeconds: 120, ShutdownGracePeriodCriticalPodsSeconds: 0, CrashLoopBackOff: <maxContainerRestartPeriod: \"10s\">>",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := kubeletConfigSignature(tc.kubeletConfig)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestConfiguredMaxPodsPerNodeFromLabels(t *testing.T) {
	for tn, tc := range map[string]struct {
		systemLabels map[string]string
		expected     int
		expectErr    bool
	}{
		"nil labels": {
			systemLabels: nil,
			expected:     0,
			expectErr:    false,
		},
		"empty labels": {
			systemLabels: map[string]string{},
			expected:     0,
			expectErr:    false,
		},
		"negative mppn": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "-1",
			},
			expected:  0,
			expectErr: true,
		},
		"Invalid mppn - Invalid number": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "__Not_a_Number__",
			},
			expected:  0,
			expectErr: true,
		},
		"Invalid mppn - Floating point number": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "3.14",
			},
			expected:  0,
			expectErr: true,
		},
		"Valid mppn": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "120",
			},
			expected:  120,
			expectErr: false,
		},
		"mppn is set to 0": {
			systemLabels: map[string]string{
				labels.MaxPodsPerNodeLabel: "0",
			},
			expected:  0,
			expectErr: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got, err := configuredMaxPodsPerNodeFromLabels(tc.systemLabels)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestGetEstimatedNumberOfPods(t *testing.T) {
	maxMppn := 256
	for desc, tc := range map[string]struct {
		req              nodeGroupRequirements
		machineType      string
		wantNumberOfPods int
		wantErr          error
	}{
		"a lot of small cpu pods, e2-standard-32, 200 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 160, 100, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 200,
		},
		"average cpu pods, e2-standard-32, 50 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 640, 100, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 50,
		},
		"large cpu pods, e2-standard-32, 3 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 10000, 100, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 3,
		},
		"large memory pods, e2-standard-32, 3 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 100, 40*units.GiB, 0, 0),
			},
			machineType:      "e2-standard-32",
			wantNumberOfPods: 3,
		},
		"differently sized workloads, e2-standard-16, 10 estimated pods": {
			req: nodeGroupRequirements{
				pods: append(getPods(10, 640, 100, 0, 0), getPods(10, 2500, 100, 10, 0)...),
			},
			machineType:      "e2-standard-16",
			wantNumberOfPods: 10,
		},
		"gpu pod, 8 estimated pods": {
			req: nodeGroupRequirements{
				pods: getPods(10, 160, 100, 1, 0),
				gpuRequest: machinetypes.GpuRequest{
					Count: 8,
				},
			},
			machineType:      "a2-ultragpu-8g",
			wantNumberOfPods: 8,
		},
		"tpu pod, 1 estimated pods": {
			req: nodeGroupRequirements{
				pods:       getPods(10, 160, 100, 0, 4),
				tpuRequest: TpuRequest{ChipsPerNode: 4},
			},
			machineType:      "ct3-hightpu-4t",
			wantNumberOfPods: 1,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotNumberOfPods := 0
			machineTypeInfo, gotErr := machinetypes.NewMachineConfigProvider(nil).ToMachineType(tc.machineType)
			if gotErr == nil {
				gotNumberOfPods = getEstimatedNumberOfPods(maxMppn, tc.req, machineTypeInfo)
			}
			if tc.wantErr != nil {
				assert.Error(t, gotErr)
				assert.Equal(t, gotErr, tc.wantErr)
			} else {
				assert.NoError(t, gotErr)
				assert.Equal(t, tc.wantNumberOfPods, gotNumberOfPods)
			}
		})
	}
}

func getPods(numPods int, cpuMilli int64, memoryMilli int64, gpuResource int64, tpuResource int64) []*apiv1.Pod {
	var result []*apiv1.Pod
	for i := 0; i < numPods; i++ {
		pod := test_util.BuildTestPod(fmt.Sprintf("pod-%d", i), cpuMilli, memoryMilli, test_util.MarkUnschedulable())
		if gpuResource > 0 {
			pod.Spec.Containers[0].Resources.Requests[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(gpuResource, resource.DecimalSI)
		}
		if tpuResource > 0 {
			pod.Spec.Containers[0].Resources.Requests[tpu.ResourceGoogleTPU] = *resource.NewQuantity(tpuResource, resource.DecimalSI)
		}
		result = append(result, pod)
	}
	return result
}

func TestPlacementGroupSpec(t *testing.T) {
	for desc, tc := range map[string]struct {
		labelReq podrequirements.LabelRequirements
		req      nodeGroupRequirements
		wantSpec placement.Spec
	}{
		"placement group and policy from requirements without policy rule": {
			labelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				labels.PlacementGroupLabel: podrequirements.NewValues("group"),
				labels.PolicyLabel:         podrequirements.NewValues("policy"),
			}),
			wantSpec: placement.Spec{GroupId: "group", Policy: "policy"},
		},
		"placement group from requirements with policy rule": {
			labelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				labels.PlacementGroupLabel: podrequirements.NewValues("group"),
			}),
			req: nodeGroupRequirements{
				computeClassRule: rules.NewRule(
					rules.WithPlacementPolicyRule("policy"),
				),
			},
			wantSpec: placement.Spec{GroupId: "group", Policy: "policy"},
		},
		"placement policy from policy rule": {
			req: nodeGroupRequirements{
				computeClassRule: rules.NewRule(
					rules.WithPlacementPolicyRule("policy"),
				),
			},
			wantSpec: placement.Spec{Policy: "policy"},
		},
		"placement group from requirements with policy rule, no override": {
			labelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				labels.PlacementGroupLabel: podrequirements.NewValues("group"),
				labels.PolicyLabel:         podrequirements.NewValues("policy"),
			}),
			req: nodeGroupRequirements{
				computeClassRule: rules.NewRule(
					rules.WithPlacementPolicyRule("policy rule"),
				),
			},
			wantSpec: placement.Spec{GroupId: "group", Policy: "policy"},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotSpec := placementGroupSpec(&tc.req, tc.labelReq)
			assert.Equal(t, tc.wantSpec, gotSpec)
		})
	}
}

func TestKubeletConfigFromCCRule(t *testing.T) {
	for tn, tc := range map[string]struct {
		rule     rules.Rule
		expected *gke_api_beta.NodeKubeletConfig
	}{
		"rule without kubelet config rules": {
			rule:     rules.NewRule(rules.WithAutopilotModeRule()),
			expected: nil,
		},
		"rule with all kubelet config fields": {
			rule: rules.NewRule(
				rules.WithCpuCfsQuotaRule(true),
				rules.WithCpuCfsQuotaPeriodRule("100ms"),
				rules.WithCpuManagerPolicyRule("static"),
				rules.WithPodPidsLimitRule(10000),
				rules.WithImageGcLowThresholdPercentRule(80),
				rules.WithImageGcHighThresholdPercentRule(90),
				rules.WithImageMinimumGcAgeRule("2m"),
				rules.WithImageMaximumGcAgeRule("1h"),
				rules.WithContainerLogMaxSizeRule("10M"),
				rules.WithContainerLogMaxFilesRule(5),
				rules.WithMaxParallelImagePullsRule(5),
				rules.WithAllowedUnsafeSysctlsRule([]string{"net.ipv4.tcp_max_syn_backlog"}),
				rules.WithSingleProcessOOMKill(true),
				rules.WithEvictionMaxPodGracePeriodSecondsRule(60),
				rules.WithEvictionSoftMemoryAvailableRule("2Gi"),
				rules.WithEvictionSoftNodefsAvailableRule("10%"),
				rules.WithEvictionSoftImagefsAvailableRule("15%"),
				rules.WithEvictionSoftImagefsInodesFreeRule("5%"),
				rules.WithEvictionSoftNodefsInodesFreeRule("5%"),
				rules.WithEvictionSoftPidAvailableRule("10%"),
				rules.WithEvictionSoftGracePeriodMemoryAvailableRule("2m"),
				rules.WithEvictionSoftGracePeriodNodefsAvailableRule("90s"),
				rules.WithEvictionSoftGracePeriodImagefsAvailableRule("90s"),
				rules.WithEvictionSoftGracePeriodImagefsInodesFreeRule("90s"),
				rules.WithEvictionSoftGracePeriodNodefsInodesFreeRule("90s"),
				rules.WithEvictionSoftGracePeriodPidAvailableRule("90s"),
				rules.WithEvictionMinimumReclaimMemoryAvailableRule("5%"),
				rules.WithEvictionMinimumReclaimNodefsAvailableRule("5%"),
				rules.WithEvictionMinimumReclaimImagefsAvailableRule("5%"),
				rules.WithEvictionMinimumReclaimImagefsInodesFreeRule("1%"),
				rules.WithEvictionMinimumReclaimNodefsInodesFreeRule("1%"),
				rules.WithEvictionMinimumReclaimPidAvailableRule("1%"),
				rules.WithTopologyManagerPolicyRule("best-effort"),
				rules.WithTopologyManagerScopeRule("container"),
				rules.WithMemoryManagerPolicyRule("Static"),
				rules.WithShutdownGracePeriodSecondsRule(120),
				rules.WithShutdownGracePeriodCriticalPodsSecondsRule(0),
				rules.WithCrashLoopBackOffMaxContainerRestartPeriodRule("10s"),
			),
			expected: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:                      true,
				CpuCfsQuotaPeriod:                "100ms",
				CpuManagerPolicy:                 "static",
				PodPidsLimit:                     10000,
				ImageGcLowThresholdPercent:       80,
				ImageGcHighThresholdPercent:      90,
				ImageMinimumGcAge:                "2m",
				ImageMaximumGcAge:                "1h",
				ContainerLogMaxSize:              "10M",
				ContainerLogMaxFiles:             5,
				MaxParallelImagePulls:            5,
				AllowedUnsafeSysctls:             []string{"net.ipv4.tcp_max_syn_backlog"},
				SingleProcessOomKill:             true,
				EvictionMaxPodGracePeriodSeconds: 60,
				EvictionSoft: &gke_api_beta.EvictionSignals{
					MemoryAvailable:   "2Gi",
					NodefsAvailable:   "10%",
					ImagefsAvailable:  "15%",
					ImagefsInodesFree: "5%",
					NodefsInodesFree:  "5%",
					PidAvailable:      "10%",
				},
				EvictionSoftGracePeriod: &gke_api_beta.EvictionGracePeriod{
					MemoryAvailable:   "2m",
					NodefsAvailable:   "90s",
					ImagefsAvailable:  "90s",
					ImagefsInodesFree: "90s",
					NodefsInodesFree:  "90s",
					PidAvailable:      "90s",
				},
				EvictionMinimumReclaim: &gke_api_beta.EvictionMinimumReclaim{
					MemoryAvailable:   "5%",
					NodefsAvailable:   "5%",
					ImagefsAvailable:  "5%",
					ImagefsInodesFree: "1%",
					NodefsInodesFree:  "1%",
					PidAvailable:      "1%",
				},
				TopologyManager: &gke_api_beta.TopologyManager{
					Policy: "best-effort",
					Scope:  "container",
				},
				MemoryManager: &gke_api_beta.MemoryManager{
					Policy: "Static",
				},
				ShutdownGracePeriodSeconds:             120,
				ShutdownGracePeriodCriticalPodsSeconds: 0,
				CrashLoopBackOff: &gke_api_beta.CrashLoopBackOffConfig{
					MaxContainerRestartPeriod: "10s",
				},
				ForceSendFields: []string{"CpuCfsQuota", "ShutdownGracePeriodSeconds", "ShutdownGracePeriodCriticalPodsSeconds"},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := kubeletConfigFromCCRule(tc.rule)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestLinuxNodeConfigFromCCRule(t *testing.T) {
	for tn, tc := range map[string]struct {
		rule     rules.Rule
		expected *gkeclient.LinuxNodeConfig
	}{
		"empty rule": {
			rule:     rules.NewRule(),
			expected: nil,
		},
		"rule with accurate time config": {
			rule: rules.NewRule(
				rules.WithAccurateTimeConfigRule(ccc_api.AccurateTimeConfig{
					EnablePtpKvmTimeSync: boolPtr(true),
				}),
			),
			expected: &gkeclient.LinuxNodeConfig{
				AccurateTimeConfig: &gkeclient.AccurateTimeConfig{
					EnablePtpKvmTimeSync: true,
				},
			},
		},
		"rule with NodeVfioConfig and DiskIoScheduler": {
			rule: rules.NewRule(
				rules.WithNodeVfioConfigRule(ccc_api.NodeVfioConfig{
					DmaEntryLimit: int32Ptr(65536),
				}),
				rules.WithDiskIoSchedulerRule(ccc_api.DiskIoScheduler{
					NodeSystemIoScheduler:       "mq-deadline",
					NodeAttachedDiskIoScheduler: "bfq",
				}),
			),
			expected: &gkeclient.LinuxNodeConfig{
				NodeVfioConfig: &gkeclient.NodeVfioConfig{
					DmaEntryLimit: 65536,
				},
				DiskIoScheduler: &gkeclient.DiskIoScheduler{
					NodeSystemIoScheduler:       "mq-deadline",
					NodeAttachedDiskIoScheduler: "bfq",
				},
			},
		},
		"rule with new fields": {
			rule: rules.NewRule(
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
						GcsUri:                    stringPtr("gs://bucket/script.sh"),
						GcsGeneration:             int64Ptr(123),
						Args:                      []string{"arg1"},
						GcpSecretManagerSecretUri: stringPtr("secret-uri"),
					},
				}),
				rules.WithKernelOverridesRule(&rules.KernelOverrides{
					KernelCommandlineOverrides: &rules.KernelCommandlineOverrides{
						SpecRstackOverflow: stringPtr("OFF"),
						InitOnAlloc:        stringPtr("OFF"),
					},
					LruGen: &rules.LRUGen{
						Enabled:  boolPtr(true),
						MinTtlMs: int32Ptr(1000),
					},
				}),
				rules.WithTimeZoneRule("UTC"),
			),
			expected: &gkeclient.LinuxNodeConfig{
				AdditionalEtcHosts: []*gkeclient.EtcHostsEntry{
					{Ip: "1.2.3.4", Host: "host1"},
				},
				AdditionalEtcResolvConf: []*gkeclient.ResolvedConfEntry{
					{Key: "nameserver", Value: []string{"8.8.8.8"}},
				},
				AdditionalEtcSystemdResolvedConf: []*gkeclient.ResolvedConfEntry{
					{Key: "DNS", Value: []string{"8.8.4.4"}},
				},
				CustomNodeInit: &gkeclient.CustomNodeInit{
					InitScript: &gkeclient.InitScript{
						GcsUri:                    "gs://bucket/script.sh",
						GcsGeneration:             123,
						Args:                      []string{"arg1"},
						GcpSecretManagerSecretUri: "secret-uri",
					},
				},
				KernelOverrides: &gkeclient.KernelOverrides{
					KernelCommandlineOverrides: &gkeclient.KernelCommandlineOverrides{
						SpecRstackOverflow: "OFF",
						InitOnAlloc:        "OFF",
					},
					LruGen: &gkeclient.LRUGen{
						Enabled:  true,
						MinTtlMs: 1000,
					},
				},
				TimeZone: "UTC",
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := linuxNodeConfigFromCCRule(tc.rule)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func stringPtr(s string) *string { return &s }
func int64Ptr(i int64) *int64    { return &i }
func int32Ptr(i int32) *int32    { return &i }
func boolPtr(b bool) *bool       { return &b }
