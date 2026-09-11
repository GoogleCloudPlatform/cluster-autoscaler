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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
)

func TestLinuxNodeConfigTranslation(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		assert.Nil(t, linuxNodeConfig(nil))
		assert.Nil(t, v1beta1LinuxNodeConfig(nil))
	})

	t.Run("all fields present", func(t *testing.T) {
		apiConfig := &gke_api_beta.LinuxNodeConfig{
			CgroupMode: "CGROUP_MODE_V2",
			Sysctls: map[string]string{
				"net.core.somaxconn": "1024",
			},
			Hugepages: &gke_api_beta.HugepagesConfig{
				HugepageSize1g: 12345,
				HugepageSize2m: 987654321,
			},
			TransparentHugepageEnabled: "always",
			TransparentHugepageDefrag:  "madvise",
			AccurateTimeConfig: &gke_api_beta.AccurateTimeConfig{
				EnablePtpKvmTimeSync: true,
			},
			NodeKernelModuleLoading: &gke_api_beta.NodeKernelModuleLoading{
				Policy: "ALLOWED",
			},
			SwapConfig: &gke_api_beta.SwapConfig{
				Enabled: true,
				EncryptionConfig: &gke_api_beta.EncryptionConfig{
					Disabled: false,
				},
				BootDiskProfile: &gke_api_beta.BootDiskProfile{
					SwapSizeGib:     5,
					SwapSizePercent: 10,
				},
				EphemeralLocalSsdProfile: &gke_api_beta.EphemeralLocalSsdProfile{
					SwapSizeGib:     10,
					SwapSizePercent: 20,
				},
				DedicatedLocalSsdProfile: &gke_api_beta.DedicatedLocalSsdProfile{
					DiskCount: 2,
				},
			},
		}

		expected := &LinuxNodeConfig{
			CgroupMode: "CGROUP_MODE_V2",
			Sysctls: map[string]string{
				"net.core.somaxconn": "1024",
			},
			Hugepages: &HugepagesConfig{
				HugepageSize1g: 12345,
				HugepageSize2m: 987654321,
			},
			TransparentHugepageEnabled: "always",
			TransparentHugepageDefrag:  "madvise",
			AccurateTimeConfig: &AccurateTimeConfig{
				EnablePtpKvmTimeSync: true,
			},
			NodeKernelModuleLoading: &NodeKernelModuleLoading{
				Policy: "ALLOWED",
			},
			SwapConfig: &SwapConfig{
				Enabled: true,
				EncryptionConfig: &EncryptionConfig{
					Disabled: false,
				},
				BootDiskProfile: &BootDiskProfile{
					SwapSizeGib:     5,
					SwapSizePercent: 10,
				},
				EphemeralLocalSsdProfile: &EphemeralLocalSsdProfile{
					SwapSizeGib:     10,
					SwapSizePercent: 20,
				},
				DedicatedLocalSsdProfile: &DedicatedLocalSsdProfile{
					DiskCount: 2,
				},
			},
		}

		got := linuxNodeConfig(apiConfig)
		assert.Equal(t, expected, got)
		assert.Equal(t, apiConfig, v1beta1LinuxNodeConfig(got))
	})
}
