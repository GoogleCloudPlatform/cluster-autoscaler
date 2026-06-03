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

package asyncnodegroups

import (
	"reflect"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

// AsyncNapNodeGroupStateChecker required to expose information about node-group state in OSS without exposing GkeMig struct.
type AsyncNapNodeGroupStateChecker struct{}

func NewAsyncNapNodeGroupStateChecker() *AsyncNapNodeGroupStateChecker {
	return &AsyncNapNodeGroupStateChecker{}
}

func (*AsyncNapNodeGroupStateChecker) IsUpcoming(nodeGroup cloudprovider.NodeGroup) bool {
	return IsUpcoming(nodeGroup)
}

func (*AsyncNapNodeGroupStateChecker) CleanUp() {
}

func IsUpcoming(nodeGroup cloudprovider.NodeGroup) bool {
	if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
		return false
	}
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		// assuming only gke migs can be upcoming
		return false
	}
	return mig.IsUpcoming()
}
