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

import "strconv"

type PendingPodKind string

const (
	ProvisioningInProgress PendingPodKind = "provisioning_in_progress"
	UnableToProvision      PendingPodKind = "unable_to_provision"
	Unprocessed            PendingPodKind = "unprocessed"
	NoActionTaken          PendingPodKind = "no_action_taken"
)

type PendingPodsMetric struct {
	Kind      PendingPodKind
	SystemPod bool
	Count     int
}

type PendingPodsPerCccMetric struct {
	PendingPodsMetric
	CccName string
}

func (m PendingPodsMetric) Labels() []string {
	return []string{string(m.Kind), strconv.FormatBool(m.SystemPod)}
}
