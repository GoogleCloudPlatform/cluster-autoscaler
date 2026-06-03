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

package nodequota

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodequota"
	klog "k8s.io/klog/v2"
)

// NodeQuotaProcessor updates MaxNodesTotal to match GKE node quota at the end of each autoscaler loop.
// Quota is polled asynchronously, calling processor returns the latest value. In case of
// any errors MaxNodesTotal is set to 0 (no limit, failing open).
type NodeQuotaProcessor struct {
	qw nodequota.Watcher
}

// Process sets context.MaxNodesTotal to latest available node quota value.
func (p *NodeQuotaProcessor) Process(context *context.AutoscalingContext, csr *clusterstate.ClusterStateRegistry, now time.Time) error {
	quota := p.qw.GetNodeQuota()
	if quota != context.MaxNodesTotal {
		klog.V(2).Infof("Updating MaxNodesTotal to match nodequota. Previous value = %v, new value = %v", context.MaxNodesTotal, quota)
		context.MaxNodesTotal = quota
	}
	return nil
}

// CleanUp cleans internal state of processor.
func (p *NodeQuotaProcessor) CleanUp() {}

// NewNodeQuotaProcessor returns a NodeQuotaProcessor.
func NewNodeQuotaProcessor(qw nodequota.Watcher) *NodeQuotaProcessor {
	return &NodeQuotaProcessor{qw: qw}
}
