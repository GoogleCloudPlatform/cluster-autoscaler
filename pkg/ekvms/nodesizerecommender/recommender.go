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

package nodesizerecommender

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

type InstanceFetcher interface {
	InstanceByRef(ref gce.GceRef) *gce.GceInstance
}

type MaxSizeRecommendation struct {
	size.VmSize
	// Time when the max size recommendation was created, or zero time in case the creation time was not available
	// (recommendations with missing creation time should be treated the same way as very old recommendations).
	CreationTime time.Time
}

type NodeSizeRecommender interface {
	gke.ClusterLocationsObserver

	// MaxSize returns max size recommendation for a node, or nil if there is no recommendation.
	MaxSize(*v1.Node) *MaxSizeRecommendation
}

type RecommenderFactory = func(internalopts.AutoscalingOptions, InstanceFetcher, config.StringFlagEvaluator) (NodeSizeRecommender, error)

func DefaultFactory(internalopts.AutoscalingOptions, InstanceFetcher, config.StringFlagEvaluator) (NodeSizeRecommender, error) {
	return NewNoOpNodeSizeRecommender(), nil
}
