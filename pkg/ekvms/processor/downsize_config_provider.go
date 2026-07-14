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

package processor

import (
	"google.golang.org/protobuf/encoding/protojson"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/klog/v2"

	processor_proto "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor/proto"
)

// NewDownsizeConfigProvider returns a provider that combines results from all families that have a ResizableConfig.
func NewDownsizeConfigProvider(provider *machinetypes.MachineConfigProvider, experimentsManager config.StringFlagEvaluator, configFlags map[string]string, experimentFlags map[string]string) config.Provider[map[string]*processor_proto.DownsizeConfig] {
	providers := make(map[string]config.Provider[*processor_proto.DownsizeConfig])
	for _, family := range provider.AllResizableMachineFamilies() {
		providers[family.Name()] = singleDownsizeConfigProvider(experimentsManager, configFlags[family.Name()], experimentFlags[family.Name()])
	}
	return &combinedDownsizeConfigProvider{
		providers: providers,
	}
}

func singleDownsizeConfigProvider(experimentsManager config.StringFlagEvaluator, configJson string, experimentFlag string) config.Provider[*processor_proto.DownsizeConfig] {
	if downsizeConfig, hasConfig := fromJson(configJson); hasConfig {
		return config.SimpleProvider[*processor_proto.DownsizeConfig]{Value: downsizeConfig}
	}
	fallbackDownsizeConfigProvider := config.SimpleProvider[*processor_proto.DownsizeConfig]{Value: DefaultDownsizeConfig()}
	if experimentsManager == nil || len(experimentFlag) == 0 {
		return fallbackDownsizeConfigProvider
	}
	return config.ExperimentProvider[*processor_proto.DownsizeConfig]{
		ExperimentEvaluator: experimentsManager,
		ExperimentFlag:      experimentFlag,
		Convert:             fromJson,
		Fallback:            fallbackDownsizeConfigProvider,
	}
}

type combinedDownsizeConfigProvider struct {
	providers map[string]config.Provider[*processor_proto.DownsizeConfig]
}

func (p *combinedDownsizeConfigProvider) Provide() map[string]*processor_proto.DownsizeConfig {
	result := make(map[string]*processor_proto.DownsizeConfig)
	for family, provider := range p.providers {
		result[family] = provider.Provide()
	}
	return result
}

func fromJson(configJson string) (*processor_proto.DownsizeConfig, bool) {
	if len(configJson) == 0 || configJson == "{}" {
		return nil, false
	}
	downsizeConfig := &processor_proto.DownsizeConfig{}
	err := protojson.Unmarshal([]byte(configJson), downsizeConfig)
	if err != nil {
		klog.Warningf("Unmarshalling of downsize config from JSON failed: %v", err)
		return nil, false
	}
	return downsizeConfig, true
}
