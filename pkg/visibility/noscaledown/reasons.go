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

package noscaledown

import vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"

// Reasons contains all possible reasons that can cause the scale-down not to be performed.
type Reasons struct {
	TopLevel         *vistypes.Message
	UnremovableNodes []*vistypes.NodeExplanation
}

// IsEmpty returns whether all possible reasons are empty.
func (r *Reasons) IsEmpty() bool {
	return r.TopLevel == nil && len(r.UnremovableNodes) == 0
}
