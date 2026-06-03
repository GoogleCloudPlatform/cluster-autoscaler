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

package defrag

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
)

// HardTaint is applied to candidate nodes to prevent pods from scheduling.
const HardTaint = "autoscaling.gke.io/defrag-candidate"

// SoftTaint is applied to candidate nodes to minimize the risk of pods with
// wildcard tolerations from scheduling when HardTaint is ignored.
const SoftTaint = "autoscaling.gke.io/defrag-soft-candidate"

// Mode describes requirements for scaling-down the defrag candidate nodes.
type Mode int

const (
	// CreateBeforeDelete means that the candidate nodes should be scaled-down iff.
	// their pods can be scheduled on already existing nodes. This mode is atomic,
	// i.e. scale down will be initiated once per candidate for all candidate
	// nodes once all pods can be rescheduled. This is the default.
	CreateBeforeDelete Mode = iota
	// DeleteBeforeCreate means that the candidate nodes should be scaled-down ASAP,
	// even if there is no place in the cluster to schedule them. This mode should
	// be used iff. there is no possibility of scale-up before the nodes are deleted.
	DeleteBeforeCreate
	// Partial works similarly to CreateBeforeDelete, but defrag can scale down
	// individual candidate nodes once their pods can be scheduled on already
	// existing nodes. This mode allows defrag to scale down part of the nodes,
	// even if it encounters issues further in the processing. Falls back
	// to CreateBeforeDelete if experiment is disabled.
	Partial
)

// Describes plugin type
type PluginType string

// Plugin types
const (
	StandardPluginType    PluginType = "STANDARD"
	ResizesOnlyPluginType PluginType = "RESIZES_ONLY"
)

// Candidate is a set of nodes which should undergo defrag. It is created by Plugin.
type Candidate struct {
	// id is a candidate identifier number, randomized at creation
	id int
	// Plugin is the defrag Plugin that created this candidate
	Plugin Plugin
	// Nodes is a list of names of the nodes belonging to this candidate
	Nodes []string
	// Mode is a mode of scale-down of this candidate
	Mode Mode
}

func NewCandidate(nodes []string, mode Mode) *Candidate {
	return &Candidate{
		id:    rand.Intn(10000),
		Nodes: nodes,
		Mode:  mode,
	}
}

func NewCandidateWithLimit(nodes []string, mode Mode, limit int) *Candidate {
	if limit == 0 {
		return NewCandidate(nodes, mode)
	}
	candidateNodes := make([]string, min(len(nodes), limit))
	copy(candidateNodes, nodes)
	return NewCandidate(candidateNodes, mode)
}

func (c *Candidate) String() string {
	var nodesDesc string
	if len(c.Nodes) <= 3 {
		nodesDesc = fmt.Sprintf("nodes {%s}", strings.Join(c.Nodes, ", "))
	} else {
		nodesDesc = fmt.Sprintf("nodes {%s} and %d others", strings.Join(c.Nodes[:3], ", "), len(c.Nodes)-3)
	}
	return fmt.Sprintf("[%v #%d] %s", c.Plugin, c.id, nodesDesc)
}

// Plugin is an interface responsible for creating Candidates, validating them and
// filtering out expansion options which can help them. Only Candidates created
// by the given Plugin should be passed to it for validation, otherwise the
// behaviour is undefined.
type Plugin interface {
	fmt.Stringer
	// Type() returns plugin type
	Type() PluginType
	// NewCandidate finds a subset of nodes that satisfy the Plugin requirements
	// and creates a new Candidate with them. Only the nodes passed as an argument
	// can be used to construct the Candidate. Returns nil if there is no suitable
	// subset of nodes.
	NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *Candidate
	// ValidCandidateNodes returns a list of nodes suitable for defrag
	// according to the plugin criteria.
	ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string
	// IsExpansionOptionValid checks if the expansion option is viable for the
	// candidate, meaning that the nodes to scale-up are "better" than the
	// currently existing nodes.
	IsExpansionOptionValid(ctx *context.AutoscalingContext, candidate *Candidate, option expander.Option) bool
	// BackoffDuration returns the amount of time for which the candidate should
	// be backed off in case of failed defrag.
	BackoffDuration(ctx *context.AutoscalingContext, candidate *Candidate) time.Duration
	// LatestUnfitNodesCount returns recorded count for the nodes in the cluster that the plugin considers
	// to be requiring defrag for the latest candidate returned by the NewCandidate method
	LatestUnfitNodesCount() int
}
