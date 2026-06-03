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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
)

func TestBuildPlugins(t *testing.T) {
	config := config.PluginsConfig{}
	testCases := []struct {
		name        string
		pluginNames []string
		wantPlugins []string
		wantErr     bool
	}{
		{
			name:        "no plugins",
			pluginNames: nil,
		},
		{
			name:        "annotation plugin only",
			pluginNames: []string{"annotation"},
			wantPlugins: []string{"annotation-delete-before-create", "annotation-partial", "annotation-create-before-delete"},
		},
		{
			name:        "daemonset plugin only",
			pluginNames: []string{"daemonset"},
			wantPlugins: []string{"daemonset"},
		},
		{
			name:        "nodepool-drain plugin only",
			pluginNames: []string{"nodepool-drain"},
			wantPlugins: []string{"nodepool-drain"},
		},
		{
			name:        "high-priority-migration plugin only",
			pluginNames: []string{"high-priority-migration"},
			wantPlugins: []string{"high-priority-migration"},
		},
		{
			name:        "ek-consolidation plugin only",
			pluginNames: []string{"ek-consolidation"},
			wantPlugins: []string{"ek-consolidation"},
		},
		{
			name:        "failed-nodes plugin only",
			pluginNames: []string{"failed-nodes"},
			wantPlugins: []string{"failed-nodes"},
		},
		{
			name:        "all plugins together",
			pluginNames: []string{"daemonset", "annotation", "nodepool-drain", "high-priority-migration", "ek-consolidation", "failed-nodes"},
			wantPlugins: []string{"daemonset", "annotation-delete-before-create", "annotation-partial", "annotation-create-before-delete", "nodepool-drain", "high-priority-migration", "ek-consolidation", "failed-nodes"},
		},
		{
			name:        "unknown plugin only",
			pluginNames: []string{"--unknown--"},
			wantErr:     true,
		},
		{
			name:        "one known, one unknown plugin",
			pluginNames: []string{"annotation", "--unknown--"},
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugins, err := BuildPlugins(tc.pluginNames, config)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantPlugins, pluginNames(plugins))
			}
		})
	}
}

func pluginNames(plugins []defrag.Plugin) []string {
	var names []string
	for _, plugin := range plugins {
		names = append(names, plugin.String())
	}
	return names
}
