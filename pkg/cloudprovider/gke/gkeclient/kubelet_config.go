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
	"fmt"
	"reflect"
	"strings"

	gke_api_beta "google.golang.org/api/container/v1beta1"
)

func marshalJSON(schema interface{}, forceSendFields, nullFields []string) ([]byte, error) {
	if len(forceSendFields) == 0 && len(nullFields) == 0 {
		return json.Marshal(schema)
	}
	mustInclude := make(map[string]bool)
	for _, f := range forceSendFields {
		mustInclude[f] = true
	}
	useNull := make(map[string]bool)
	for _, nf := range nullFields {
		useNull[nf] = true
	}

	m := make(map[string]interface{})
	s := reflect.ValueOf(schema)
	st := s.Type()

	for i := 0; i < s.NumField(); i++ {
		jsonTag := st.Field(i).Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}
		parts := strings.Split(jsonTag, ",")
		apiName := parts[0]
		if apiName == "" {
			continue
		}
		stringFormat := false
		for _, part := range parts[1:] {
			if part == "string" {
				stringFormat = true
				break
			}
		}

		v := s.Field(i)
		f := st.Field(i)

		if useNull[f.Name] {
			m[apiName] = nil
			continue
		}

		if !mustInclude[f.Name] && v.IsZero() {
			continue
		}
		if mustInclude[f.Name] && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface || v.Kind() == reflect.Slice || v.Kind() == reflect.Map) && v.IsNil() {
			continue
		}

		if stringFormat {
			m[apiName] = fmt.Sprintf("%v", v.Interface())
		} else {
			m[apiName] = v.Interface()
		}
	}
	return json.Marshal(m)
}

// NodeKubeletConfig - parameters that can be configured on kubelet.
type NodeKubeletConfig struct {
	AllowedUnsafeSysctls                   []string                 `json:"allowedUnsafeSysctls,omitempty"`
	ContainerLogMaxFiles                   int64                    `json:"containerLogMaxFiles,omitempty"`
	ContainerLogMaxSize                    string                   `json:"containerLogMaxSize,omitempty"`
	ContainerLogMaxWorkers                 int64                    `json:"containerLogMaxWorkers,omitempty"`
	ContainerLogMonitorInterval            string                   `json:"containerLogMonitorInterval,omitempty"`
	CpuCfsQuota                            bool                     `json:"cpuCfsQuota,omitempty"`
	CpuCfsQuotaPeriod                      string                   `json:"cpuCfsQuotaPeriod,omitempty"`
	CpuManagerPolicy                       string                   `json:"cpuManagerPolicy,omitempty"`
	CrashLoopBackOff                       *CrashLoopBackOffConfig  `json:"crashLoopBackOff,omitempty"`
	EvictionMaxPodGracePeriodSeconds       int64                    `json:"evictionMaxPodGracePeriodSeconds,omitempty"`
	EvictionMinimumReclaim                 *EvictionMinimumReclaim  `json:"evictionMinimumReclaim,omitempty"`
	EvictionSoft                           *EvictionSignals         `json:"evictionSoft,omitempty"`
	EvictionSoftGracePeriod                *EvictionGracePeriod     `json:"evictionSoftGracePeriod,omitempty"`
	ImageGcHighThresholdPercent            int64                    `json:"imageGcHighThresholdPercent,omitempty"`
	ImageGcLowThresholdPercent             int64                    `json:"imageGcLowThresholdPercent,omitempty"`
	ImageMaximumGcAge                      string                   `json:"imageMaximumGcAge,omitempty"`
	ImageMinimumGcAge                      string                   `json:"imageMinimumGcAge,omitempty"`
	InsecureKubeletReadonlyPortEnabled     bool                     `json:"insecureKubeletReadonlyPortEnabled,omitempty"`
	MaxParallelImagePulls                  int64                    `json:"maxParallelImagePulls,omitempty"`
	MemoryManager                          *MemoryManager           `json:"memoryManager,omitempty"`
	PodPidsLimit                           int64                    `json:"podPidsLimit,omitempty,string"`
	ReservedResourcesConfig                *ReservedResourcesConfig `json:"reservedResourcesConfig,omitempty"`
	ShutdownGracePeriodCriticalPodsSeconds int64                    `json:"shutdownGracePeriodCriticalPodsSeconds,omitempty"`
	ShutdownGracePeriodSeconds             int64                    `json:"shutdownGracePeriodSeconds,omitempty"`
	SingleProcessOomKill                   bool                     `json:"singleProcessOomKill,omitempty"`
	TopologyManager                        *TopologyManager         `json:"topologyManager,omitempty"`
	ForceSendFields                        []string                 `json:"-"`
	NullFields                             []string                 `json:"-"`
}

func (s NodeKubeletConfig) MarshalJSON() ([]byte, error) {
	type NoMethod NodeKubeletConfig
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

// CrashLoopBackOffConfig - configuration for crash loop back off.
type CrashLoopBackOffConfig struct {
	MaxContainerRestartPeriod string   `json:"maxContainerRestartPeriod,omitempty"`
	ForceSendFields           []string `json:"-"`
	NullFields                []string `json:"-"`
}

func (s CrashLoopBackOffConfig) MarshalJSON() ([]byte, error) {
	type NoMethod CrashLoopBackOffConfig
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

// EvictionMinimumReclaim - minimum reclaim for eviction signals.
type EvictionMinimumReclaim struct {
	ImagefsAvailable  string   `json:"imagefsAvailable,omitempty"`
	ImagefsInodesFree string   `json:"imagefsInodesFree,omitempty"`
	MemoryAvailable   string   `json:"memoryAvailable,omitempty"`
	NodefsAvailable   string   `json:"nodefsAvailable,omitempty"`
	NodefsInodesFree  string   `json:"nodefsInodesFree,omitempty"`
	PidAvailable      string   `json:"pidAvailable,omitempty"`
	ForceSendFields   []string `json:"-"`
	NullFields        []string `json:"-"`
}

func (s EvictionMinimumReclaim) MarshalJSON() ([]byte, error) {
	type NoMethod EvictionMinimumReclaim
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

// EvictionSignals - eviction thresholds for signals.
type EvictionSignals struct {
	ImagefsAvailable  string   `json:"imagefsAvailable,omitempty"`
	ImagefsInodesFree string   `json:"imagefsInodesFree,omitempty"`
	MemoryAvailable   string   `json:"memoryAvailable,omitempty"`
	NodefsAvailable   string   `json:"nodefsAvailable,omitempty"`
	NodefsInodesFree  string   `json:"nodefsInodesFree,omitempty"`
	PidAvailable      string   `json:"pidAvailable,omitempty"`
	ForceSendFields   []string `json:"-"`
	NullFields        []string `json:"-"`
}

func (s EvictionSignals) MarshalJSON() ([]byte, error) {
	type NoMethod EvictionSignals
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

// EvictionGracePeriod - grace period for eviction signals.
type EvictionGracePeriod struct {
	ImagefsAvailable  string   `json:"imagefsAvailable,omitempty"`
	ImagefsInodesFree string   `json:"imagefsInodesFree,omitempty"`
	MemoryAvailable   string   `json:"memoryAvailable,omitempty"`
	NodefsAvailable   string   `json:"nodefsAvailable,omitempty"`
	NodefsInodesFree  string   `json:"nodefsInodesFree,omitempty"`
	PidAvailable      string   `json:"pidAvailable,omitempty"`
	ForceSendFields   []string `json:"-"`
	NullFields        []string `json:"-"`
}

func (s EvictionGracePeriod) MarshalJSON() ([]byte, error) {
	type NoMethod EvictionGracePeriod
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

// MemoryManager - memory manager configuration.
type MemoryManager struct {
	Policy          string   `json:"policy,omitempty"`
	ForceSendFields []string `json:"-"`
	NullFields      []string `json:"-"`
}

func (s MemoryManager) MarshalJSON() ([]byte, error) {
	type NoMethod MemoryManager
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

// ReservedResourcesConfig - reserved resources configuration.
type ReservedResourcesConfig struct {
	CpuReservedMillicore          int64    `json:"cpuReservedMillicore,omitempty,string"`
	EffectiveCpuReservedMillicore int64    `json:"effectiveCpuReservedMillicore,omitempty,string"`
	EffectiveMemoryReservedMib    int64    `json:"effectiveMemoryReservedMib,omitempty,string"`
	MemoryReservedMib             int64    `json:"memoryReservedMib,omitempty,string"`
	ForceSendFields               []string `json:"-"`
	NullFields                    []string `json:"-"`
}

func (s ReservedResourcesConfig) MarshalJSON() ([]byte, error) {
	type NoMethod ReservedResourcesConfig
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

// TopologyManager - topology manager configuration.
type TopologyManager struct {
	Policy          string   `json:"policy,omitempty"`
	Scope           string   `json:"scope,omitempty"`
	ForceSendFields []string `json:"-"`
	NullFields      []string `json:"-"`
}

func (s TopologyManager) MarshalJSON() ([]byte, error) {
	type NoMethod TopologyManager
	return marshalJSON(NoMethod(s), s.ForceSendFields, s.NullFields)
}

func nodeKubeletConfig(c *gke_api_beta.NodeKubeletConfig) *NodeKubeletConfig {
	if c == nil {
		return nil
	}
	return &NodeKubeletConfig{
		AllowedUnsafeSysctls:                   c.AllowedUnsafeSysctls,
		ContainerLogMaxFiles:                   c.ContainerLogMaxFiles,
		ContainerLogMaxSize:                    c.ContainerLogMaxSize,
		CpuCfsQuota:                            c.CpuCfsQuota,
		CpuCfsQuotaPeriod:                      c.CpuCfsQuotaPeriod,
		CpuManagerPolicy:                       c.CpuManagerPolicy,
		CrashLoopBackOff:                       crashLoopBackOffConfig(c.CrashLoopBackOff),
		EvictionMaxPodGracePeriodSeconds:       c.EvictionMaxPodGracePeriodSeconds,
		EvictionMinimumReclaim:                 evictionMinimumReclaim(c.EvictionMinimumReclaim),
		EvictionSoft:                           evictionSignals(c.EvictionSoft),
		EvictionSoftGracePeriod:                evictionGracePeriod(c.EvictionSoftGracePeriod),
		ImageGcHighThresholdPercent:            c.ImageGcHighThresholdPercent,
		ImageGcLowThresholdPercent:             c.ImageGcLowThresholdPercent,
		ImageMaximumGcAge:                      c.ImageMaximumGcAge,
		ImageMinimumGcAge:                      c.ImageMinimumGcAge,
		InsecureKubeletReadonlyPortEnabled:     c.InsecureKubeletReadonlyPortEnabled,
		MaxParallelImagePulls:                  c.MaxParallelImagePulls,
		MemoryManager:                          memoryManager(c.MemoryManager),
		PodPidsLimit:                           c.PodPidsLimit,
		ShutdownGracePeriodCriticalPodsSeconds: c.ShutdownGracePeriodCriticalPodsSeconds,
		ShutdownGracePeriodSeconds:             c.ShutdownGracePeriodSeconds,
		SingleProcessOomKill:                   c.SingleProcessOomKill,
		TopologyManager:                        topologyManager(c.TopologyManager),
		ForceSendFields:                        c.ForceSendFields,
		NullFields:                             c.NullFields,
	}
}

func crashLoopBackOffConfig(c *gke_api_beta.CrashLoopBackOffConfig) *CrashLoopBackOffConfig {
	if c == nil {
		return nil
	}
	return &CrashLoopBackOffConfig{
		MaxContainerRestartPeriod: c.MaxContainerRestartPeriod,
		ForceSendFields:           c.ForceSendFields,
		NullFields:                c.NullFields,
	}
}

func evictionMinimumReclaim(c *gke_api_beta.EvictionMinimumReclaim) *EvictionMinimumReclaim {
	if c == nil {
		return nil
	}
	return &EvictionMinimumReclaim{
		ImagefsAvailable:  c.ImagefsAvailable,
		ImagefsInodesFree: c.ImagefsInodesFree,
		MemoryAvailable:   c.MemoryAvailable,
		NodefsAvailable:   c.NodefsAvailable,
		NodefsInodesFree:  c.NodefsInodesFree,
		PidAvailable:      c.PidAvailable,
		ForceSendFields:   c.ForceSendFields,
		NullFields:        c.NullFields,
	}
}

func evictionSignals(c *gke_api_beta.EvictionSignals) *EvictionSignals {
	if c == nil {
		return nil
	}
	return &EvictionSignals{
		ImagefsAvailable:  c.ImagefsAvailable,
		ImagefsInodesFree: c.ImagefsInodesFree,
		MemoryAvailable:   c.MemoryAvailable,
		NodefsAvailable:   c.NodefsAvailable,
		NodefsInodesFree:  c.NodefsInodesFree,
		PidAvailable:      c.PidAvailable,
		ForceSendFields:   c.ForceSendFields,
		NullFields:        c.NullFields,
	}
}

func evictionGracePeriod(c *gke_api_beta.EvictionGracePeriod) *EvictionGracePeriod {
	if c == nil {
		return nil
	}
	return &EvictionGracePeriod{
		ImagefsAvailable:  c.ImagefsAvailable,
		ImagefsInodesFree: c.ImagefsInodesFree,
		MemoryAvailable:   c.MemoryAvailable,
		NodefsAvailable:   c.NodefsAvailable,
		NodefsInodesFree:  c.NodefsInodesFree,
		PidAvailable:      c.PidAvailable,
		ForceSendFields:   c.ForceSendFields,
		NullFields:        c.NullFields,
	}
}

func memoryManager(c *gke_api_beta.MemoryManager) *MemoryManager {
	if c == nil {
		return nil
	}
	return &MemoryManager{
		Policy:          c.Policy,
		ForceSendFields: c.ForceSendFields,
		NullFields:      c.NullFields,
	}
}

func topologyManager(c *gke_api_beta.TopologyManager) *TopologyManager {
	if c == nil {
		return nil
	}
	return &TopologyManager{
		Policy:          c.Policy,
		Scope:           c.Scope,
		ForceSendFields: c.ForceSendFields,
		NullFields:      c.NullFields,
	}
}

func v1beta1NodeKubeletConfig(c *NodeKubeletConfig) *gke_api_beta.NodeKubeletConfig {
	if c == nil {
		return nil
	}
	return &gke_api_beta.NodeKubeletConfig{
		AllowedUnsafeSysctls:                   c.AllowedUnsafeSysctls,
		ContainerLogMaxFiles:                   c.ContainerLogMaxFiles,
		ContainerLogMaxSize:                    c.ContainerLogMaxSize,
		CpuCfsQuota:                            c.CpuCfsQuota,
		CpuCfsQuotaPeriod:                      c.CpuCfsQuotaPeriod,
		CpuManagerPolicy:                       c.CpuManagerPolicy,
		CrashLoopBackOff:                       v1beta1CrashLoopBackOffConfig(c.CrashLoopBackOff),
		EvictionMaxPodGracePeriodSeconds:       c.EvictionMaxPodGracePeriodSeconds,
		EvictionMinimumReclaim:                 v1beta1EvictionMinimumReclaim(c.EvictionMinimumReclaim),
		EvictionSoft:                           v1beta1EvictionSignals(c.EvictionSoft),
		EvictionSoftGracePeriod:                v1beta1EvictionGracePeriod(c.EvictionSoftGracePeriod),
		ImageGcHighThresholdPercent:            c.ImageGcHighThresholdPercent,
		ImageGcLowThresholdPercent:             c.ImageGcLowThresholdPercent,
		ImageMaximumGcAge:                      c.ImageMaximumGcAge,
		ImageMinimumGcAge:                      c.ImageMinimumGcAge,
		InsecureKubeletReadonlyPortEnabled:     c.InsecureKubeletReadonlyPortEnabled,
		MaxParallelImagePulls:                  c.MaxParallelImagePulls,
		MemoryManager:                          v1beta1MemoryManager(c.MemoryManager),
		PodPidsLimit:                           c.PodPidsLimit,
		ShutdownGracePeriodCriticalPodsSeconds: c.ShutdownGracePeriodCriticalPodsSeconds,
		ShutdownGracePeriodSeconds:             c.ShutdownGracePeriodSeconds,
		SingleProcessOomKill:                   c.SingleProcessOomKill,
		TopologyManager:                        v1beta1TopologyManager(c.TopologyManager),
		ForceSendFields:                        c.ForceSendFields,
		NullFields:                             c.NullFields,
	}
}

func v1beta1CrashLoopBackOffConfig(c *CrashLoopBackOffConfig) *gke_api_beta.CrashLoopBackOffConfig {
	if c == nil {
		return nil
	}
	return &gke_api_beta.CrashLoopBackOffConfig{
		MaxContainerRestartPeriod: c.MaxContainerRestartPeriod,
		ForceSendFields:           c.ForceSendFields,
		NullFields:                c.NullFields,
	}
}

func v1beta1EvictionMinimumReclaim(c *EvictionMinimumReclaim) *gke_api_beta.EvictionMinimumReclaim {
	if c == nil {
		return nil
	}
	return &gke_api_beta.EvictionMinimumReclaim{
		ImagefsAvailable:  c.ImagefsAvailable,
		ImagefsInodesFree: c.ImagefsInodesFree,
		MemoryAvailable:   c.MemoryAvailable,
		NodefsAvailable:   c.NodefsAvailable,
		NodefsInodesFree:  c.NodefsInodesFree,
		PidAvailable:      c.PidAvailable,
		ForceSendFields:   c.ForceSendFields,
		NullFields:        c.NullFields,
	}
}

func v1beta1EvictionSignals(c *EvictionSignals) *gke_api_beta.EvictionSignals {
	if c == nil {
		return nil
	}
	return &gke_api_beta.EvictionSignals{
		ImagefsAvailable:  c.ImagefsAvailable,
		ImagefsInodesFree: c.ImagefsInodesFree,
		MemoryAvailable:   c.MemoryAvailable,
		NodefsAvailable:   c.NodefsAvailable,
		NodefsInodesFree:  c.NodefsInodesFree,
		PidAvailable:      c.PidAvailable,
		ForceSendFields:   c.ForceSendFields,
		NullFields:        c.NullFields,
	}
}

func v1beta1EvictionGracePeriod(c *EvictionGracePeriod) *gke_api_beta.EvictionGracePeriod {
	if c == nil {
		return nil
	}
	return &gke_api_beta.EvictionGracePeriod{
		ImagefsAvailable:  c.ImagefsAvailable,
		ImagefsInodesFree: c.ImagefsInodesFree,
		MemoryAvailable:   c.MemoryAvailable,
		NodefsAvailable:   c.NodefsAvailable,
		NodefsInodesFree:  c.NodefsInodesFree,
		PidAvailable:      c.PidAvailable,
		ForceSendFields:   c.ForceSendFields,
		NullFields:        c.NullFields,
	}
}

func v1beta1MemoryManager(c *MemoryManager) *gke_api_beta.MemoryManager {
	if c == nil {
		return nil
	}
	return &gke_api_beta.MemoryManager{
		Policy:          c.Policy,
		ForceSendFields: c.ForceSendFields,
		NullFields:      c.NullFields,
	}
}

func v1beta1TopologyManager(c *TopologyManager) *gke_api_beta.TopologyManager {
	if c == nil {
		return nil
	}
	return &gke_api_beta.TopologyManager{
		Policy:          c.Policy,
		Scope:           c.Scope,
		ForceSendFields: c.ForceSendFields,
		NullFields:      c.NullFields,
	}
}
