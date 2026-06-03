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

package config

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const (
	experimentEnabledNodesPerCycle = 1
)

type pluginProvider interface {
	GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily
	IsAutopilotEnabled() bool
}

type NodeMigration struct {
	enabled bool
	// Number of nodes to be migrated per defrag cycle.
	nodesPerCycle int
	// Source machine family to be migrated from (e.g. "e2").
	sourceMachineFamily string
	// Target machine family to be migrated to (e.g. "ek").
	targetMachineFamily string
	// Force migrating nodes by ignoring drainability rules (e.g. PDBs & EDP).
	force bool
	// If specified, only nodes created before this date will be migrated.
	// Format: RFC3339 standard. Example: 2024-07-11T00:00:00Z.
	nodeCreationDateLessThan string

	// Whether AutopilotEk::ForcefulMigrationFromEkToE2MinCAVersion is enabled.
	// It takes priority over the nodeMigration field in cluster proto.
	forcefulMigrationFromEkToE2ExperimentEnabled bool
	experimentsManager                           experiments.Manager
}

func NewNodeMigration(enabled bool, nodesPerCycle int, sourceMachineFamily, targetMachineFamily string, force bool, nodeCreationDateLessThan string, experimentsManager experiments.Manager) *NodeMigration {
	nodeMigration := NodeMigration{
		enabled:                  enabled,
		nodesPerCycle:            nodesPerCycle,
		sourceMachineFamily:      sourceMachineFamily,
		targetMachineFamily:      targetMachineFamily,
		force:                    force,
		nodeCreationDateLessThan: nodeCreationDateLessThan,
		experimentsManager:       experimentsManager,
	}
	nodeMigration.UpdateForcefulMigrationFromEkToE2ExperimentEnabled()
	return &nodeMigration
}

// UpdateForcefulMigrationFromEkToE2ExperimentEnabled updates forcefulMigrationFromEkToE2ExperimentEnabled at the beginning of each CA cycle for consistency.
func (n *NodeMigration) UpdateForcefulMigrationFromEkToE2ExperimentEnabled() {
	n.forcefulMigrationFromEkToE2ExperimentEnabled = n.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.EkForcefulMigrationFromEkToE2MinCAVersionFlag, false)
	klog.V(3).Infof("Updating forcefulMigrationFromEkToE2ExperimentEnabled: %v", n.forcefulMigrationFromEkToE2ExperimentEnabled)
}

// Enabled returns true if node migration is enabled in cluster proto or if AutopilotEk::ForcefulMigrationFromEkToE2MinCAVersion is enabled.
func (n *NodeMigration) Enabled() bool {
	return n.enabled || n.forcefulMigrationFromEkToE2ExperimentEnabled
}

// NodesPerCycle returns the number of nodes to be migrated per defrag cycle
func (n *NodeMigration) NodesPerCycle() int {
	if n.forcefulMigrationFromEkToE2ExperimentEnabled {
		return experimentEnabledNodesPerCycle
	}
	return n.nodesPerCycle
}

// SourceMachineFamily returns the source machine family to be migrated from (e.g. "e2").
func (n *NodeMigration) SourceMachineFamily() string {
	if n.forcefulMigrationFromEkToE2ExperimentEnabled {
		return "ek"
	}
	return n.sourceMachineFamily
}

// TargetMachineFamily returns the target machine family to be migrated to (e.g. "ek").
func (n *NodeMigration) TargetMachineFamily() string {
	if n.forcefulMigrationFromEkToE2ExperimentEnabled {
		return "e2"
	}
	return n.targetMachineFamily
}

// Force migrating nodes by ignoring drainability rules (e.g. PDBs & EDP).
func (n *NodeMigration) Force() bool {
	if n.forcefulMigrationFromEkToE2ExperimentEnabled {
		return true
	}
	return n.force
}

// NodeCreationDateLessThan returns the date which only nodes created before this date will be migrated, or empty string to migrate nodes regardless of when they are created.
// Format: RFC3339 standard. Example: 2024-07-11T00:00:00Z.
func (n *NodeMigration) NodeCreationDateLessThan() string {
	if n.forcefulMigrationFromEkToE2ExperimentEnabled {
		return ""
	}
	return n.nodeCreationDateLessThan
}

type PluginBuilder func(pluginsConfig PluginsConfig) defrag.Plugin

type PluginsConfig struct {
	MaxCandidateNodeCount int
	NPCLister             npc_lister.Lister
	Provider              pluginProvider
	Autopilot             bool
	ResizableVmManager    operationtracker.Manager
	ExperimentsManager    experiments.Manager
}

type Options struct {
	MaxCandidateNodeCount int
	NPCLister             npc_lister.Lister
	Provider              pluginProvider
	Autopilot             bool
	ResizableVmManager    operationtracker.Manager
	ExperimentsManager    experiments.Manager
}

func New(opts Options) PluginsConfig {
	return PluginsConfig{
		MaxCandidateNodeCount: opts.MaxCandidateNodeCount,
		NPCLister:             opts.NPCLister,
		Provider:              opts.Provider,
		Autopilot:             opts.Autopilot,
		ResizableVmManager:    opts.ResizableVmManager,
		ExperimentsManager:    opts.ExperimentsManager,
	}
}
