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

package operationtracker

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/utils/clock"
)

const (
	// metricsExportingInterval is an interval at which metrics are exported.
	metricsExportingInterval = 30 * time.Second
	// metricsGracePeriod defines a period after the application starts during which metrics are not collected or exported.
	// This allows the application to initialize and stabilize before metrics collection begins,
	// preventing potentially misleading data from being recorded during startup.
	metricsGracePeriod = 3 * time.Minute
	// metricsGracePeriod defines a period after the node creation during which metrics for this node are not collected or exported.
	// This allows the recommendation pipeline to initialize and stabilize before metrics collection begins.
	metricsNodeGracePeriod = 3 * time.Minute
)

var (
	zeroTime = time.Time{}
)

type resizableNodeLister interface {
	snapshot(forceRefresh bool) ResizableNodesSnapshot
	isResizingEnabled(family string) bool
	resizableFamilyNames() []string
}

type periodicMetrics interface {
	ObserveMaxSizeRecommendationAge(time.Duration)
}

type metricsExporter struct {
	resizableNodeLister resizableNodeLister
	nodeSizeRecommender nodesizerecommender.NodeSizeRecommender
	metrics             periodicMetrics
	clock               clock.PassiveClock
	enabledSince        map[string]time.Time
}

func newMetricsExporter(nodeLister resizableNodeLister, nodeSizeRecommender nodesizerecommender.NodeSizeRecommender, metrics periodicMetrics, clock clock.PassiveClock) metricsExporter {
	enabledSince := make(map[string]time.Time)
	for _, family := range nodeLister.resizableFamilyNames() {
		if nodeLister.isResizingEnabled(family) {
			enabledSince[family] = clock.Now()
		}
	}
	return metricsExporter{
		resizableNodeLister: nodeLister,
		nodeSizeRecommender: nodeSizeRecommender,
		metrics:             metrics,
		clock:               clock,
		enabledSince:        enabledSince,
	}
}

func (me *metricsExporter) run(ctx context.Context) {
	wait.Until(func() {
		me.observeMaxSizeRecommendationAges()
	}, metricsExportingInterval, ctx.Done())
}

func (me *metricsExporter) observeMaxSizeRecommendationAges() {
	// Cache enabled status to avoid redundant calls and sync enabledSince.
	enabledFamilies := make(map[string]bool)
	for _, family := range me.resizableNodeLister.resizableFamilyNames() {
		enabled := me.resizableNodeLister.isResizingEnabled(family)
		enabledFamilies[family] = enabled
		if !enabled {
			delete(me.enabledSince, family)
		}
	}

	for _, node := range me.resizableNodeLister.snapshot(true) {
		family := node.MachineFamily
		if !enabledFamilies[family] {
			continue
		}
		since, ok := me.enabledSince[family]
		if !ok {
			since = me.clock.Now()
			me.enabledSince[family] = since
		}
		if me.clock.Since(since) < metricsGracePeriod {
			continue
		}
		if me.clock.Since(node.Node.CreationTimestamp.Time) < metricsNodeGracePeriod {
			continue
		}
		me.metrics.ObserveMaxSizeRecommendationAge(me.maxSizeAge(node.Node))
	}
}

func (me *metricsExporter) maxSizeAge(node *v1.Node) time.Duration {
	return me.clock.Now().Sub(me.effectiveMaxSizeRecommendationCreationTime(node))
}

// effectiveMaxSizeRecommendationCreationTime returns the recommendation creation time retrieved from the node,
// or zero time in cases when the creation time is unavailable.
// This way unavailable creation times are treated the same way as very old ones.
func (me *metricsExporter) effectiveMaxSizeRecommendationCreationTime(node *v1.Node) time.Time {
	if me.nodeSizeRecommender == nil {
		return zeroTime
	}
	maxSizeRecommendation := me.nodeSizeRecommender.MaxSize(node)
	if maxSizeRecommendation == nil {
		return zeroTime
	}
	return maxSizeRecommendation.CreationTime
}
