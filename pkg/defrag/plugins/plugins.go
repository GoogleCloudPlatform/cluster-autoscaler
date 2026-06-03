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

package plugins

import (
	"fmt"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/annotation"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/daemonset"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/ekconsolidation"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/failednodes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/highprioritymigration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/nodepooldrain"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/recycling"
)

var pluginBuildersByName = map[string][]config.PluginBuilder{
	annotation.PluginName:            annotation.PluginBuilders,
	daemonset.PluginName:             {daemonset.NewPlugin},
	nodepooldrain.PluginName:         {nodepooldrain.NewPlugin},
	highprioritymigration.PluginName: {highprioritymigration.NewPlugin},
	ekconsolidation.PluginName:       {ekconsolidation.NewPlugin},
	recycling.PluginName:             {recycling.NewPlugin},
	failednodes.PluginName:           {failednodes.NewPlugin},
}

func BuildPlugins(pluginNames []string, config config.PluginsConfig) ([]defrag.Plugin, error) {
	var plugins []defrag.Plugin
	for _, pluginName := range pluginNames {
		pluginBuilders, found := pluginBuildersByName[pluginName]
		if !found {
			return nil, fmt.Errorf("no defrag plugin named %q", pluginName)
		}
		for _, pluginBuilder := range pluginBuilders {
			plugins = append(plugins, pluginBuilder(config))
		}
	}
	return plugins, nil
}
