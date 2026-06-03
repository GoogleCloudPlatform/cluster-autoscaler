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
	"strconv"
	"strings"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

type workloadSeparationLimiter struct {
	experimentsManager experiments.Manager
	defaultLimit       int
	componentVersion   version.Version
}

func NewWorkloadSeparationLimiter(experimentsManager experiments.Manager, defaultLimit int, componentVersion version.Version) *workloadSeparationLimiter {
	if defaultLimit < 0 {
		defaultLimit = 0
	}

	return &workloadSeparationLimiter{
		experimentsManager: experimentsManager,
		defaultLimit:       defaultLimit,
		componentVersion:   componentVersion,
	}
}

// Limit returns the maximum number of non-default workload separations that can have lookahead buffer.
func (w *workloadSeparationLimiter) Limit() int {
	flag := w.experimentsManager.EvaluateStringFlagOrFailsafe(experiments.EkLookaheadMaxWorkloadSeparationsFlag, "0,999.999.999")
	if len(flag) == 0 {
		return w.defaultLimit
	}
	config := strings.Split(flag, ",")
	if len(config) != 2 {
		klog.Warningf("Experiment %q provided unexpected flag: %q, expected format <limit>,<min_version>", experiments.EkLookaheadMaxWorkloadSeparationsFlag, flag)
		return w.defaultLimit
	}
	limit, minVersionValue := config[0], config[1]
	minVersion, err := version.FromString(minVersionValue)
	if err != nil {
		klog.Errorf("Experiment %q provided invalid min version %q, using default workload separation limit", experiments.EkLookaheadMaxWorkloadSeparationsFlag, minVersionValue)
		return w.defaultLimit
	}
	if w.componentVersion.LessThan(minVersion) {
		return w.defaultLimit
	}
	l, err := strconv.Atoi(limit)
	if err != nil {
		klog.Warningf("Experiment %q provided unexpected limit: %q, expected integer", experiments.EkLookaheadMaxWorkloadSeparationsFlag, limit)
		return w.defaultLimit
	}
	// Clipping at 0 in case the experiment set negative value.
	l = max(0, l)
	klog.V(4).Infof("Using limit %d from experiment %q, raw flag: %q", l, experiments.EkLookaheadMaxWorkloadSeparationsFlag, flag)
	return l
}
