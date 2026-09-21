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

package providers

import (
	"reflect"
)

type launchPhase string

const (
	launchCoarseGrainedResize     launchPhase = "COARSE_GRAINED_RESIZE"
	launchEnabledNoResize         launchPhase = "NO_RESIZE"
	launchNotEnabled              launchPhase = "NOT_ENABLED"
	launchDisabled                launchPhase = "DISABLED"
	launchDisabledCgroupv1        launchPhase = "DISABLED_CGROUPV1"
	launchDisabledBalloonPodError launchPhase = "DISABLED_BALLOON_POD_ERROR"
)

type launchSource string

const (
	launchExperiment   launchSource = "GIRAFFE"
	launchClusterProto launchSource = "CLUSTER_PROTO"
	launchUndefined    launchSource = ""
)

type LaunchStatus struct {
	phase  launchPhase
	source launchSource
}

type autoprovisioningProvider interface {
	refresh()
	isEnabledInAutopilot() bool
	managedNodesEnabled() bool
	resizingEnabled() bool
	registerNodesCountProvider(nodesCountProvider)
	nodesCount() int
}

func getNodesCount(countProvider nodesCountProvider, machineFamily string) int {
	if countProvider == nil || reflect.ValueOf(countProvider).IsNil() {
		return 0
	}
	return countProvider.NodesCount(machineFamily)
}
