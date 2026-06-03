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

package tracking

import (
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/cli"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// InitExperimentsManager initializes experiments Manager.
func InitExperimentsManager(o internalopts.AutoscalingOptions) experiments.Manager {
	v := cli.ComponentVersion()
	klog.Infof("Cluster Autoscaler Component Version: %s", v.String())
	return experiments.NewManager(v, NewExperimentsEvaluator(o))
}

// experimentsEvaluator is the experiments evaluator constructor. It can be overridden dynamically.
var experimentsEvaluator = newDefaultExperimentsEvaluator

func newDefaultExperimentsEvaluator(o internalopts.AutoscalingOptions) experiments.Evaluator {
	return experiments.NewNoopEvaluator()
}

// NewExperimentsEvaluator returns the experiments evaluator.
func NewExperimentsEvaluator(o internalopts.AutoscalingOptions) experiments.Evaluator {
	return experimentsEvaluator(o)
}

// SetExperimentsEvaluator sets the experiments evaluator constructor.
func SetExperimentsEvaluator(f func(internalopts.AutoscalingOptions) experiments.Evaluator) {
	experimentsEvaluator = f
}
