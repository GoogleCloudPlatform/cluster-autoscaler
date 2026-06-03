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

package status

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
)

// Mutator is a function that applies a partial change to the status.
// It is designed to be safe to run on the "master" copy.
type Mutator func(status crd.CRDStatus)

// UpdateMessage is sent over the channel for the aggregator to update the CRD data.
type UpdateMessage struct {
	// Id is the identifier of the CRD to update.
	Id CRDId
	// Mutate contains the logic to apply the specific update (e.g., updating CRD ResourceInfo).
	Mutate Mutator
}

// CRDId identifies a CRD by its label and name.
type CRDId struct {
	// CRDLabel is the label of the CRD.
	CRDLabel string
	// CRDName is the name of the CRD.
	CRDName string
}
