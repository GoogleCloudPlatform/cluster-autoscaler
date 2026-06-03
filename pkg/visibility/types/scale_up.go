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

package types

import (
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

// ScaleUpMig contains information about a scaled up MIG.
type ScaleUpMig struct {
	Mig         *GkeMig
	CurrentSize int
	NewSize     int
	MaxSize     int
}

// NoScaleUpInfo contains information about a pod that didn't trigger a scale-up.
type NoScaleUpInfo struct {
	Pod                *Pod
	RejectedNodeGroups map[string]status.Reasons
	SkippedNodeGroups  map[string]status.Reasons
}

// ScaleUpStatus contains information about the status of a scale up.
type ScaleUpStatus struct {
	Result               status.ScaleUpResult
	PodsTriggeredScaleUp []*Pod
	PodsAwaitEvaluation  []*Pod
	ConsideredMigs       []*GkeMig
	CreatedNodePools     [][]*GkeMig
	ScaleUpMigs          []*ScaleUpMig
	NoScaleUpInfos       []*NoScaleUpInfo
}

// Proto converts the structure to its proto representation.
func (i *ScaleUpMig) Proto() *vispb.ScaleUpMig {
	return &vispb.ScaleUpMig{
		Mig:            i.Mig.Proto(),
		RequestedNodes: int32(i.NewSize - i.CurrentSize),
	}
}

// ScaleUpDataProto extracts a proto representation of data concerning scaled up MIGs.
func (s *ScaleUpStatus) ScaleUpDataProto() *vispb.ScaleUpData {
	var increasedMigs []*vispb.ScaleUpMig
	for _, scaleUpMig := range s.ScaleUpMigs {
		increasedMigs = append(increasedMigs, scaleUpMig.Proto())
	}

	var triggeringPods []*vispb.Pod
	for i, pod := range s.PodsTriggeredScaleUp {
		if i >= visibility.MaxPodsInEvent {
			break
		}
		triggeringPods = append(triggeringPods, pod.Proto())
	}

	return &vispb.ScaleUpData{
		IncreasedMigs:            increasedMigs,
		TriggeringPods:           triggeringPods,
		TriggeringPodsTotalCount: int32(len(s.PodsTriggeredScaleUp)),
	}
}

// NodePoolCreatedDataProto extracts a proto representation of data concerning created node pools.
func (s *ScaleUpStatus) NodePoolCreatedDataProto() *vispb.NodePoolCreatedData {
	createdNodePools := make([]*vispb.NodePool, 0, len(s.CreatedNodePools))

	for _, createdMigs := range s.CreatedNodePools {
		var protoMigs []*vispb.Mig
		for _, mig := range createdMigs {
			protoMigs = append(protoMigs, mig.Proto())
		}

		if len(protoMigs) == 0 {
			continue
		}

		createdNodePools = append(createdNodePools, &vispb.NodePool{
			Name: createdMigs[0].NodePoolName,
			Migs: protoMigs,
		})
	}

	return &vispb.NodePoolCreatedData{
		NodePools: createdNodePools,
	}
}

// GetMigsById returns a mapping from MIG Id to MIG.
func (s *ScaleUpStatus) GetMigsById() map[string]*GkeMig {
	migsById := make(map[string]*GkeMig)
	for _, mig := range s.ConsideredMigs {
		migsById[mig.Id] = mig
	}
	return migsById
}

// ConvertNoScaleUpInfo converts no scale up info to its visibility-specific counterpart.
func ConvertNoScaleUpInfo(info status.NoScaleUpInfo) *NoScaleUpInfo {
	return &NoScaleUpInfo{
		Pod:                ConvertPod(info.Pod),
		RejectedNodeGroups: info.RejectedNodeGroups,
		SkippedNodeGroups:  info.SkippedNodeGroups,
	}
}

// ConvertScaleUpMig converts a scale up MIG to its visibility-specific counterpart.
func ConvertScaleUpMig(info nodegroupset.ScaleUpInfo) (*ScaleUpMig, error) {
	mig, err := ConvertGkeMig(info.Group)
	if err != nil {
		return nil, err
	}
	return &ScaleUpMig{
		Mig:         mig,
		CurrentSize: info.CurrentSize,
		NewSize:     info.NewSize,
		MaxSize:     info.MaxSize,
	}, nil
}

// ConvertScaleUpStatus converts a scale up status to its visibility-specific counterpart,
// basically replacing all pods, migs etc. with visibility-specific ones.
func ConvertScaleUpStatus(originalStatus *status.ScaleUpStatus) (*ScaleUpStatus, error) {
	var podsTriggeredScaleUp []*Pod
	for _, pod := range originalStatus.PodsTriggeredScaleUp {
		podsTriggeredScaleUp = append(podsTriggeredScaleUp, ConvertPod(pod))
	}

	var podsAwaitEvaluation []*Pod
	for _, pod := range originalStatus.PodsAwaitEvaluation {
		podsAwaitEvaluation = append(podsAwaitEvaluation, ConvertPod(pod))
	}

	var noScaleUpInfos []*NoScaleUpInfo
	for _, noScaleUpInfo := range originalStatus.PodsRemainUnschedulable {
		noScaleUpInfos = append(noScaleUpInfos, ConvertNoScaleUpInfo(noScaleUpInfo))
	}

	var consideredMigs []*GkeMig
	for _, nodeGroup := range originalStatus.ConsideredNodeGroups {
		mig, err := ConvertGkeMig(nodeGroup)
		if err != nil {
			return nil, err
		}
		consideredMigs = append(consideredMigs, mig)
	}

	var scaleUpInfos []*ScaleUpMig
	for _, scaleUpInfo := range originalStatus.ScaleUpInfos {
		convertedInfo, err := ConvertScaleUpMig(scaleUpInfo)
		if err != nil {
			return nil, err
		}
		scaleUpInfos = append(scaleUpInfos, convertedInfo)
	}

	var createdNodePools [][]*GkeMig
	for _, createdNodeGroupResult := range originalStatus.CreateNodeGroupResults {
		if createdNodeGroupResult.MainCreatedNodeGroup == nil {
			continue
		}
		mainMig, err := ConvertGkeMig(createdNodeGroupResult.MainCreatedNodeGroup)
		if err != nil {
			return nil, err
		}

		createdMigs := []*GkeMig{mainMig}

		for _, extraNodeGroup := range createdNodeGroupResult.ExtraCreatedNodeGroups {
			additionalMig, err := ConvertGkeMig(extraNodeGroup)
			if err != nil {
				return nil, err
			}
			createdMigs = append(createdMigs, additionalMig)
		}

		createdNodePools = append(createdNodePools, createdMigs)
	}

	return &ScaleUpStatus{
		Result:               originalStatus.Result,
		PodsTriggeredScaleUp: podsTriggeredScaleUp,
		PodsAwaitEvaluation:  podsAwaitEvaluation,
		ConsideredMigs:       consideredMigs,
		CreatedNodePools:     createdNodePools,
		ScaleUpMigs:          scaleUpInfos,
		NoScaleUpInfos:       noScaleUpInfos,
	}, nil
}
