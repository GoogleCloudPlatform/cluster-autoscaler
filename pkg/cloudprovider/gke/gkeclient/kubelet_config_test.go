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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
)

func TestKubeletConfigTranslation(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		assert.Nil(t, nodeKubeletConfig(nil))
		assert.Nil(t, v1beta1NodeKubeletConfig(nil))
	})

	t.Run("all fields present", func(t *testing.T) {
		apiConfig := &gke_api_beta.NodeKubeletConfig{
			AllowedUnsafeSysctls:                   []string{"kernel.msgmax", "kernel.shmmax"},
			ContainerLogMaxFiles:                   5,
			ContainerLogMaxSize:                    "10Mi",
			CpuCfsQuota:                            true,
			CpuCfsQuotaPeriod:                      "100ms",
			CpuManagerPolicy:                       "static",
			CrashLoopBackOff:                       &gke_api_beta.CrashLoopBackOffConfig{MaxContainerRestartPeriod: "300s"},
			EvictionMaxPodGracePeriodSeconds:       30,
			EvictionMinimumReclaim:                 &gke_api_beta.EvictionMinimumReclaim{ImagefsAvailable: "10%", MemoryAvailable: "100Mi"},
			EvictionSoft:                           &gke_api_beta.EvictionSignals{ImagefsAvailable: "15%", MemoryAvailable: "200Mi"},
			EvictionSoftGracePeriod:                &gke_api_beta.EvictionGracePeriod{ImagefsAvailable: "1m", MemoryAvailable: "2m"},
			ImageGcHighThresholdPercent:            80,
			ImageGcLowThresholdPercent:             60,
			ImageMaximumGcAge:                      "720h",
			ImageMinimumGcAge:                      "2m",
			InsecureKubeletReadonlyPortEnabled:     true,
			MaxParallelImagePulls:                  3,
			MemoryManager:                          &gke_api_beta.MemoryManager{Policy: "Static"},
			PodPidsLimit:                           1024,
			ShutdownGracePeriodCriticalPodsSeconds: 10,
			ShutdownGracePeriodSeconds:             30,
			SingleProcessOomKill:                   true,
			TopologyManager:                        &gke_api_beta.TopologyManager{Policy: "single-numa-node", Scope: "pod"},
			ForceSendFields:                        []string{"ShutdownGracePeriodSeconds"},
			NullFields:                             []string{"ContainerLogMaxSize"},
		}

		expected := &NodeKubeletConfig{
			AllowedUnsafeSysctls:                   []string{"kernel.msgmax", "kernel.shmmax"},
			ContainerLogMaxFiles:                   5,
			ContainerLogMaxSize:                    "10Mi",
			CpuCfsQuota:                            true,
			CpuCfsQuotaPeriod:                      "100ms",
			CpuManagerPolicy:                       "static",
			CrashLoopBackOff:                       &CrashLoopBackOffConfig{MaxContainerRestartPeriod: "300s"},
			EvictionMaxPodGracePeriodSeconds:       30,
			EvictionMinimumReclaim:                 &EvictionMinimumReclaim{ImagefsAvailable: "10%", MemoryAvailable: "100Mi"},
			EvictionSoft:                           &EvictionSignals{ImagefsAvailable: "15%", MemoryAvailable: "200Mi"},
			EvictionSoftGracePeriod:                &EvictionGracePeriod{ImagefsAvailable: "1m", MemoryAvailable: "2m"},
			ImageGcHighThresholdPercent:            80,
			ImageGcLowThresholdPercent:             60,
			ImageMaximumGcAge:                      "720h",
			ImageMinimumGcAge:                      "2m",
			InsecureKubeletReadonlyPortEnabled:     true,
			MaxParallelImagePulls:                  3,
			MemoryManager:                          &MemoryManager{Policy: "Static"},
			PodPidsLimit:                           1024,
			ShutdownGracePeriodCriticalPodsSeconds: 10,
			ShutdownGracePeriodSeconds:             30,
			SingleProcessOomKill:                   true,
			TopologyManager:                        &TopologyManager{Policy: "single-numa-node", Scope: "pod"},
			ForceSendFields:                        []string{"ShutdownGracePeriodSeconds"},
			NullFields:                             []string{"ContainerLogMaxSize"},
		}

		got := nodeKubeletConfig(apiConfig)
		assert.Equal(t, expected, got)
		assert.Equal(t, apiConfig, v1beta1NodeKubeletConfig(got))
	})

	t.Run("ForceSendFields JSON marshaling", func(t *testing.T) {
		cfg := &NodeKubeletConfig{
			ShutdownGracePeriodSeconds: 0,
			ForceSendFields:            []string{"ShutdownGracePeriodSeconds"},
		}
		data, err := json.Marshal(cfg)
		assert.NoError(t, err)
		assert.Contains(t, string(data), `"shutdownGracePeriodSeconds":0`)
	})
}

func TestCrashLoopBackOffConfigTranslation(t *testing.T) {
	assert.Nil(t, crashLoopBackOffConfig(nil))
	assert.Nil(t, v1beta1CrashLoopBackOffConfig(nil))

	apiCfg := &gke_api_beta.CrashLoopBackOffConfig{MaxContainerRestartPeriod: "120s"}
	expected := &CrashLoopBackOffConfig{MaxContainerRestartPeriod: "120s"}

	got := crashLoopBackOffConfig(apiCfg)
	assert.Equal(t, expected, got)
	assert.Equal(t, apiCfg, v1beta1CrashLoopBackOffConfig(got))
}

func TestEvictionMinimumReclaimTranslation(t *testing.T) {
	assert.Nil(t, evictionMinimumReclaim(nil))
	assert.Nil(t, v1beta1EvictionMinimumReclaim(nil))

	apiCfg := &gke_api_beta.EvictionMinimumReclaim{
		ImagefsAvailable:  "10%",
		ImagefsInodesFree: "5%",
		MemoryAvailable:   "100Mi",
		NodefsAvailable:   "10%",
		NodefsInodesFree:  "5%",
		PidAvailable:      "10%",
	}
	expected := &EvictionMinimumReclaim{
		ImagefsAvailable:  "10%",
		ImagefsInodesFree: "5%",
		MemoryAvailable:   "100Mi",
		NodefsAvailable:   "10%",
		NodefsInodesFree:  "5%",
		PidAvailable:      "10%",
	}

	got := evictionMinimumReclaim(apiCfg)
	assert.Equal(t, expected, got)
	assert.Equal(t, apiCfg, v1beta1EvictionMinimumReclaim(got))
}

func TestEvictionSignalsTranslation(t *testing.T) {
	assert.Nil(t, evictionSignals(nil))
	assert.Nil(t, v1beta1EvictionSignals(nil))

	apiCfg := &gke_api_beta.EvictionSignals{
		ImagefsAvailable:  "15%",
		ImagefsInodesFree: "10%",
		MemoryAvailable:   "200Mi",
		NodefsAvailable:   "15%",
		NodefsInodesFree:  "10%",
		PidAvailable:      "15%",
	}
	expected := &EvictionSignals{
		ImagefsAvailable:  "15%",
		ImagefsInodesFree: "10%",
		MemoryAvailable:   "200Mi",
		NodefsAvailable:   "15%",
		NodefsInodesFree:  "10%",
		PidAvailable:      "15%",
	}

	got := evictionSignals(apiCfg)
	assert.Equal(t, expected, got)
	assert.Equal(t, apiCfg, v1beta1EvictionSignals(got))
}

func TestEvictionGracePeriodTranslation(t *testing.T) {
	assert.Nil(t, evictionGracePeriod(nil))
	assert.Nil(t, v1beta1EvictionGracePeriod(nil))

	apiCfg := &gke_api_beta.EvictionGracePeriod{
		ImagefsAvailable:  "2m",
		ImagefsInodesFree: "1m",
		MemoryAvailable:   "30s",
		NodefsAvailable:   "2m",
		NodefsInodesFree:  "1m",
		PidAvailable:      "1m",
	}
	expected := &EvictionGracePeriod{
		ImagefsAvailable:  "2m",
		ImagefsInodesFree: "1m",
		MemoryAvailable:   "30s",
		NodefsAvailable:   "2m",
		NodefsInodesFree:  "1m",
		PidAvailable:      "1m",
	}

	got := evictionGracePeriod(apiCfg)
	assert.Equal(t, expected, got)
	assert.Equal(t, apiCfg, v1beta1EvictionGracePeriod(got))
}

func TestMemoryManagerTranslation(t *testing.T) {
	assert.Nil(t, memoryManager(nil))
	assert.Nil(t, v1beta1MemoryManager(nil))

	apiCfg := &gke_api_beta.MemoryManager{Policy: "Static"}
	expected := &MemoryManager{Policy: "Static"}

	got := memoryManager(apiCfg)
	assert.Equal(t, expected, got)
	assert.Equal(t, apiCfg, v1beta1MemoryManager(got))
}

func TestTopologyManagerTranslation(t *testing.T) {
	assert.Nil(t, topologyManager(nil))
	assert.Nil(t, v1beta1TopologyManager(nil))

	apiCfg := &gke_api_beta.TopologyManager{
		Policy: "single-numa-node",
		Scope:  "pod",
	}
	expected := &TopologyManager{
		Policy: "single-numa-node",
		Scope:  "pod",
	}

	got := topologyManager(apiCfg)
	assert.Equal(t, expected, got)
	assert.Equal(t, apiCfg, v1beta1TopologyManager(got))
}
