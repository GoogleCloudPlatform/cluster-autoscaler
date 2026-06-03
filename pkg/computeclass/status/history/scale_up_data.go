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

package history

import (
	"fmt"
	"strings"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
)

const (
	// eventExpirationTimeout is the maximum time a scale-up event is kept in memory.
	eventExpirationTimeout = 15 * time.Minute
)

type nodeGroupConfig struct {
	nodePool    string
	machineType string
	gpu         string
	tpu         string
}

func (n nodeGroupConfig) String() string {
	var parts []string
	if n.nodePool != "" {
		parts = append(parts, fmt.Sprintf("NodePool: %s", n.nodePool))
	}
	if n.machineType != "" {
		parts = append(parts, fmt.Sprintf("MachineType: %s", n.machineType))
	}
	if n.gpu != "" {
		parts = append(parts, fmt.Sprintf("GPU: %s", n.gpu))
	}
	if n.tpu != "" {
		parts = append(parts, fmt.Sprintf("TPU: %s", n.tpu))
	}
	return strings.Join(parts, ", ")
}

func getNodeGroupConfigAndZone(nodeGroup cloudprovider.NodeGroup) (nodeGroupConfig, string) {
	var config nodeGroupConfig
	if mig, ok := nodeGroup.(*gke.GkeMig); ok {
		// Include the associated Node Pool this MIG was generated from
		config.nodePool = mig.NodePoolName()

		// Always output the underlying MachineType for clarity
		config.machineType = mig.MachineType()
		if mig.Spec() != nil {
			// Iterate over all accelerators if they exist and dynamically omit GPU if none are present
			if len(mig.Spec().Accelerators) > 0 {
				var gpuStrs []string
				for _, acc := range mig.Spec().Accelerators {
					gpuStrs = append(gpuStrs, fmt.Sprintf("type: %s, count: %d", acc.AcceleratorType, acc.AcceleratorCount))
				}
				config.gpu = strings.Join(gpuStrs, "; ")
			}
			// Only output TPU configuration bits if TpuType is defined, and topology if requested
			if mig.Spec().TpuType != "" {
				tpuInfo := fmt.Sprintf("type: %s", mig.Spec().TpuType)
				if mig.Spec().TpuTopology != "" {
					tpuInfo += fmt.Sprintf(", topology: %s", mig.Spec().TpuTopology)
				}
				config.tpu = tpuInfo
			}
		}

		var zones string
		// Getting zones is a bit trickier, try to get from node template labels, or from gceRef zone
		if templateLabels, err := mig.TemplateNodeLabels(); err == nil {
			if zone, found := templateLabels[apiv1.LabelZoneFailureDomain]; found {
				zones = zone
			}
		}
		if zones == "" {
			zones = mig.GceRef().Zone
		}
		return config, zones
	}
	return nodeGroupConfig{nodePool: "unknown", machineType: "unknown", gpu: "unknown"}, "unknown"
}

// ScaleUpDelta represents a scale-up delta for a node group rule.
type ScaleUpDelta struct {
	crdId         npc_status.CRDId
	ruleIndex     string
	addedNodes    int
	initialSize   int
	targetSize    int
	config        nodeGroupConfig
	zone          string
	isMinCapacity bool
}

type eventInfo struct {
	deltas         []ScaleUpDelta
	expirationTime time.Time
	failed         bool
}

// scaleUpData holds the states of ongoing scale up operations.
// It allows tracking when scaleups originate and verify their success when target state is reached.
type scaleUpData struct {
	lock               sync.Mutex
	unfinishedScaleUps map[string]*eventInfo
}

// NewScaleUpData returns an empty initialized instance of ScaleUpData.
func NewScaleUpData() *scaleUpData {
	return &scaleUpData{
		unfinishedScaleUps: make(map[string]*eventInfo),
	}
}

// registerScaleUp adds a new scale-up delta for the given node group.
func (sd *scaleUpData) registerScaleUp(nodeGroupId string, delta ScaleUpDelta) {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	if info, found := sd.unfinishedScaleUps[nodeGroupId]; found {
		info.deltas = append(info.deltas, delta)
		info.expirationTime = time.Now().Add(eventExpirationTimeout)
	} else {
		sd.unfinishedScaleUps[nodeGroupId] = &eventInfo{
			deltas:         []ScaleUpDelta{delta},
			expirationTime: time.Now().Add(eventExpirationTimeout),
		}
	}
}

// getUnfinishedNodeGroups returns node groups that haven't been resolved yet along with their scale up deltas and failure status.
func (sd *scaleUpData) getUnfinishedNodeGroups() map[string]*eventInfo {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	result := make(map[string]*eventInfo)
	for k, v := range sd.unfinishedScaleUps {
		// Provide a copy of eventInfo to avoid data races
		copyInfo := &eventInfo{
			deltas:         make([]ScaleUpDelta, len(v.deltas)),
			expirationTime: v.expirationTime,
			failed:         v.failed,
		}
		copy(copyInfo.deltas, v.deltas)
		result[k] = copyInfo
	}
	return result
}

// markScaleUpFailed marks the node group scaleup as failed in memory to delay dispatching until target size reached.
func (sd *scaleUpData) markScaleUpFailed(nodeGroupId string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()
	if info, found := sd.unfinishedScaleUps[nodeGroupId]; found {
		info.failed = true
	}
}

// finishScaleUp removes the node group scaleup from records.
func (sd *scaleUpData) finishScaleUp(nodeGroupId string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()
	delete(sd.unfinishedScaleUps, nodeGroupId)
}

// periodicCleanup removes stale scaleups from records to avoid memory leaks.
func (sd *scaleUpData) periodicCleanup() {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	now := time.Now()
	for id, event := range sd.unfinishedScaleUps {
		if now.After(event.expirationTime) {
			delete(sd.unfinishedScaleUps, id)
		}
	}
}
