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

package providers

import (
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
)

// NewAllResizableMachineTypesProvider returns a provider that combines results from all families that have a ResizableConfig.
func NewAllResizableMachineTypesProvider(provider *machinetypes.MachineConfigProvider, experimentsManager config.StringFlagEvaluator, machineTypeFlags map[string]string, experimentFlags map[string]string) config.Provider[sets.Set[string]] {
	providers := []config.Provider[sets.Set[string]]{}
	for _, family := range provider.AllResizableMachineFamilies() {
		// If overrides or experimentFlags are nil, the map lookup will safely return the zero value ("").
		providers = append(providers, resizableMachineTypesProvider(experimentsManager, machineTypeFlags[family.Name()], family.ResizableConfig().DefaultMachineTypes, experimentFlags[family.Name()]))
	}
	return &combinedStringSetProvider{
		providers: providers,
	}
}

func resizableMachineTypesProvider(experimentsManager config.StringFlagEvaluator, flagValue string, defaultMachineTypes []string, experimentFlag string) config.Provider[sets.Set[string]] {
	if len(flagValue) > 0 {
		return config.NewCommaSeparatedStringSetProvider(flagValue)
	}
	fallbackProvider := config.NewSimpleStringSetProvider(defaultMachineTypes)
	if experimentsManager == nil {
		return fallbackProvider
	}
	return config.NewExperimentStringSetProvider(experimentsManager, experimentFlag, fallbackProvider)
}

type combinedStringSetProvider struct {
	providers []config.Provider[sets.Set[string]]
}

func (p *combinedStringSetProvider) Provide() sets.Set[string] {
	result := sets.New[string]()
	for _, provider := range p.providers {
		result = result.Union(provider.Provide())
	}
	return result
}
