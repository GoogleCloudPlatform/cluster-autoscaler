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

package api

import (
	"fmt"
)

// MinEstimatedUptime represents a Spot uptime filter bucket for GCE Flex Advisor.
// Its zero value (MinUptimeUnset) indicates no uptime filter is configured.
type MinEstimatedUptime string

const (
	// MinUptimeUnset represents an unset minimum estimated Spot uptime.
	MinUptimeUnset MinEstimatedUptime = ""
	// MinUptime1Min represents the 1-minute (60s) Spot uptime bucket.
	MinUptime1Min MinEstimatedUptime = "60s"
	// MinUptime10Min represents the 10-minute (600s) Spot uptime bucket.
	MinUptime10Min MinEstimatedUptime = "600s"
	// MinUptime1Hour represents the 1-hour (3600s) Spot uptime bucket.
	MinUptime1Hour MinEstimatedUptime = "3600s"
)

// MinEstimatedUptimeFromMinutes converts a ComputeClass rule's minimum
// estimated uptime in minutes into a MinEstimatedUptime domain value.
func MinEstimatedUptimeFromMinutes(minutes *int) (MinEstimatedUptime, error) {
	if minutes == nil {
		return MinUptimeUnset, nil
	}
	switch *minutes {
	case 1:
		return MinUptime1Min, nil
	case 10:
		return MinUptime10Min, nil
	case 60:
		return MinUptime1Hour, nil
	default:
		return MinUptimeUnset, fmt.Errorf("unsupported minEstimatedUptimeMinutes %d (allowed: 1, 10, 60)", *minutes)
	}
}

// IsSet reports whether the value is not MinUptimeUnset. IsSet does not check
// that the value is a valid Spot uptime bucket.
func (u MinEstimatedUptime) IsSet() bool {
	return u != MinUptimeUnset
}
