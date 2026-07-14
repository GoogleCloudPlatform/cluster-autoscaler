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

package zonetypes

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// MissingAIZonesError - no AI zones were found in the region, either there are no
//
//	AI zones in the region, or the project is not allowlisted
const MissingAIZonesError errors.AutoscalerErrorType = "missingAiZonesError"

type ErrNoAIZones struct {
	Prefix string
	Msg    string
}

func (e *ErrNoAIZones) Error() string {
	return e.Prefix + ": " + e.Msg
}

func (e *ErrNoAIZones) Type() errors.AutoscalerErrorType {
	return MissingAIZonesError
}

func (e *ErrNoAIZones) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

func NewErrNoAIZones() *ErrNoAIZones {
	return &ErrNoAIZones{Msg: "zoneTypes: no AI zones were found in the cluster region"}
}
