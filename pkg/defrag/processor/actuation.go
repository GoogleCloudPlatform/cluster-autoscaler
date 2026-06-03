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
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	scaledownstatus "k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// defragActuator is responsible for scaling down candidates and checking scale-down status
type defragActuator struct {
	scaledDownNodes          map[string]time.Time
	scaleDownStatusProcessor status.ScaleDownStatusProcessor
	clock                    clock.PassiveClock
}

type actuatorOptions struct {
	ScaleDownStatusProcessor status.ScaleDownStatusProcessor
	Clock                    clock.PassiveClock
}

// newDefragActuator returns a new instance of defragActuator
func newDefragActuator(opts actuatorOptions) *defragActuator {
	c := opts.Clock
	if c == nil {
		c = clock.RealClock{}
	}
	return &defragActuator{
		scaledDownNodes:          make(map[string]time.Time),
		scaleDownStatusProcessor: opts.ScaleDownStatusProcessor,
		clock:                    c,
	}
}

// startScaleDown starts scale-down of the Candidate, returns scaled down nodes
// returns error if the scale-down fails
func (a *defragActuator) startScaleDown(
	ctx *context.AutoscalingContext,
	candidate *defrag.Candidate,
	candidateCreationTime time.Time,
) ([]string, error) {
	nodes := make([]string, len(candidate.Nodes))
	copy(nodes, candidate.Nodes)
	return a.startScaleDownNodes(ctx, candidate, candidateCreationTime, nodes)
}

func (a *defragActuator) startScaleDownNodes(
	ctx *context.AutoscalingContext,
	candidate *defrag.Candidate,
	candidateCreationTime time.Time,
	nodeNames []string,
) ([]string, error) {
	var nodes []*apiv1.Node
	for _, nodeName := range nodeNames {
		if _, found := a.scaledDownNodes[nodeName]; found {
			continue
		}
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			return []string{}, err
		}
		nodes = append(nodes, nodeInfo.Node())
	}

	var scaleDownResult scaledownstatus.ScaleDownResult
	var scaledDownNodes []*scaledownstatus.ScaleDownNode
	var typedErr errors.AutoscalerError

	scaleDownResult, scaledDownNodes, typedErr = ctx.ScaleDownActuator.StartDeletion(nil, nodes)

	if typedErr != nil {
		return []string{}, typedErr
	}
	if a.scaleDownStatusProcessor != nil {
		// Log only ScaleDownNodeDeleteStarted. The rest of process is handled by static_autoscaler.go.
		if scaleDownResult == scaledownstatus.ScaleDownNodeDeleteStarted {
			scaleDownStatus := &scaledownstatus.ScaleDownStatus{
				Result:          scaleDownResult,
				ScaledDownNodes: scaledDownNodes,
			}
			a.scaleDownStatusProcessor.Process(ctx, scaleDownStatus)
		}
	}

	var names []string
	for _, node := range scaledDownNodes {
		metrics.Metrics.ObserveDefragNodeRemovalDuration(candidate.Plugin.String(), a.clock.Since(candidateCreationTime))
		klog.V(4).Infof("Started scaling down defrag candidate node %v", node.Node.Name)
		a.scaledDownNodes[node.Node.Name] = a.clock.Now()
		names = append(names, node.Node.Name)
	}

	// Update RemainingPdbTracker, making sure that scale-down won't
	// accidentally exceed any PDB remaining disruptions in this loop.
	pods, err := recreatablePods(ctx.ClusterSnapshot, names)
	if err != nil {
		return nil, err
	}
	if len(pods) > 0 {
		metrics.Metrics.IncreaseDefragEvictedPodsTotal(candidate.Plugin.String(), len(pods))
	}
	ctx.RemainingPdbTracker.RemovePods(pods)

	return names, nil
}

// isScaleDownFullyStarted checks if the Candidate scale-down was started
func (a *defragActuator) isScaleDownFullyStarted(candidate *defrag.Candidate) bool {
	for _, nodeName := range candidate.Nodes {
		if _, found := a.scaledDownNodes[nodeName]; !found {
			return false
		}
	}
	return true
}

// isScaleDownTimedOut checks if the Candidate scale-down timed out
func (a *defragActuator) isScaleDownTimedOut(candidate *defrag.Candidate, timeout time.Duration) bool {
	for _, nodeName := range candidate.Nodes {
		if stamp, found := a.scaledDownNodes[nodeName]; found && a.clock.Since(stamp) > timeout {
			return true
		}
	}
	return false
}

// cleanScaleDownInfo cleans scale-down info for non-existing nodes
func (a *defragActuator) cleanScaleDownInfo(candidates []*defrag.Candidate) {
	scaledDownNodes := make(map[string]time.Time)
	for _, candidate := range candidates {
		for _, nodeName := range candidate.Nodes {
			if timestamp, found := a.scaledDownNodes[nodeName]; found {
				scaledDownNodes[nodeName] = timestamp
			}
		}
	}
	a.scaledDownNodes = scaledDownNodes
}
