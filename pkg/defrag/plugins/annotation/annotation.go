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

package annotation

import (
	"fmt"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
)

const (
	PluginName = "annotation"

	annotationKey           = "defrag.cluster-autoscaler.kubernetes.io"
	createBeforeDeleteValue = "create-before-delete"
	deleteBeforeCreateValue = "delete-before-create"
	partialValue            = "partial"
)

type plugin struct {
	config   config.PluginsConfig
	mode     defrag.Mode
	isAtomic bool
	name     string
}

// PluginBuilders return builders for 3 annotation plugins.
// Try DeleteBeforeCreate first, since it's the fastest to process,
// then go with Partial, and finally CreateBeforeDelete because it's the slowest.
var PluginBuilders = []config.PluginBuilder{
	newPluginBuilder(deleteBeforeCreateValue, defrag.DeleteBeforeCreate, true),
	newPluginBuilder(partialValue, defrag.CreateBeforeDelete, false),
	newPluginBuilder(createBeforeDeleteValue, defrag.CreateBeforeDelete, true),
}

func newPluginBuilder(name string, mode defrag.Mode, isAtomic bool) config.PluginBuilder {
	return func(config config.PluginsConfig) defrag.Plugin {
		return &plugin{
			config:   config,
			mode:     mode,
			isAtomic: isAtomic,
			name:     name,
		}
	}
}

func (p *plugin) String() string {
	return fmt.Sprintf("%s-%s", PluginName, p.name)
}

func (p *plugin) NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	var annotatedNodes []string
	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			continue
		}

		if m := nodeInfo.Node().Annotations[annotationKey]; m == p.name {
			annotatedNodes = append(annotatedNodes, nodeName)
		}
	}

	if len(annotatedNodes) == 0 {
		return nil
	}
	return defrag.NewCandidateWithLimit(annotatedNodes, p.mode, p.isAtomic, p.config.MaxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string {
	var candidateNodes []string
	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			continue
		}

		if a := nodeInfo.Node().Annotations[annotationKey]; a == p.name {
			candidateNodes = append(candidateNodes, nodeName)
		}
	}
	return candidateNodes
}

func (p *plugin) IsExpansionOptionValid(_ *context.AutoscalingContext, _ *defrag.Candidate, _ expander.Option) bool {
	return true
}

func (p *plugin) BackoffDuration(_ *context.AutoscalingContext, _ *defrag.Candidate) time.Duration {
	return 5 * time.Minute
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

// LatestUnfitNodesCount is used for monitoring defrag_unfit_nodes metric.
// As annotation plugin is used for testing purposes only as of right now
// there is no implementation for this method and it always returns 0
func (p *plugin) LatestUnfitNodesCount() int {
	return 0
}
