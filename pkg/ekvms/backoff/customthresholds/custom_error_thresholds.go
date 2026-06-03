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

package customthresholds

import (
	"encoding/json"
	"fmt"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

type Status string

const (
	Enabled  Status = "STATUS_ENABLED"
	Disabled Status = "STATUS_DISABLED"
)

type ErrorThreshold struct {
	Name      string `json:"name"`
	Threshold int    `json:"threshold"`
}

// CustomThresholdsPerErrorType refers to the EkClusterBackoff::CustomThresholdsPerErrorType experiment flag parsing
type CustomThresholdsPerErrorType struct {
	Status                  Status           `json:"status,omitempty"`
	MinCaVersion            string           `json:"minCaVersion,omitempty"`
	ErrorThresholdsDisabled bool             `json:"errorThresholdsDisabled"`
	Errors                  []ErrorThreshold `json:"errors"`
	ForceScaleUpDisabled    bool             `json:"forceScaleUpDisabled"`
	UpsizeTriesThreshold    int              `json:"upsizeTriesThreshold"`
}

// CustomErrorThresholds provides the custom thresholds and corresponding feature flags.
type CustomErrorThresholds struct {
	errorThresholdsFeatureDisabled bool
	thresholds                     map[string]int
	forceScaleUpFeatureDisabled    bool
	upsizeTriesThreshold           int
}

// NewCustomErrorThresholds creates a new CustomErrorThresholds instance from the experiment config.
func NewCustomErrorThresholds(config CustomThresholdsPerErrorType, caVersion version.Version) *CustomErrorThresholds {
	thresholds := make(map[string]int)
	experimentVersion, err := version.FromString(config.MinCaVersion)
	if err != nil {
		klog.Errorf("Experiment %q provided invalid min version %q, not using custom error thresholds", experiments.ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag, config.MinCaVersion)
		return &CustomErrorThresholds{thresholds: thresholds, errorThresholdsFeatureDisabled: true, forceScaleUpFeatureDisabled: true}
	}
	if !isCustomThresholdsExperimentEnabled(experimentVersion, config, caVersion) {
		// Fallback to total node error count (without error type classification) if experiment is disabled or misconfigured.
		return &CustomErrorThresholds{thresholds: thresholds, errorThresholdsFeatureDisabled: true, forceScaleUpFeatureDisabled: true}
	}
	if config.Errors == nil {
		return &CustomErrorThresholds{thresholds: thresholds, errorThresholdsFeatureDisabled: config.ErrorThresholdsDisabled}
	}
	for _, errorThreshold := range config.Errors {
		thresholds[errorThreshold.Name] = errorThreshold.Threshold
	}
	return &CustomErrorThresholds{thresholds: thresholds, errorThresholdsFeatureDisabled: config.ErrorThresholdsDisabled, upsizeTriesThreshold: config.UpsizeTriesThreshold, forceScaleUpFeatureDisabled: config.ForceScaleUpDisabled}
}

// getThreshold returns the threshold for a given error type.
func (c *CustomErrorThresholds) getThreshold(errorType string) (int, bool) {
	if c == nil {
		return 0, false
	}
	threshold, found := c.thresholds[errorType]
	return threshold, found
}

// isCustomThresholdsExperimentEnabled returns true if the custom thresholds experiment is enabled.
func isCustomThresholdsExperimentEnabled(experimentVersion version.Version, experimentConfig CustomThresholdsPerErrorType, caVersion version.Version) bool {
	if experimentConfig.Status != Enabled {
		klog.Infof("Experiment %q status is %q", experiments.ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag, experimentConfig.Status)
		return false
	}
	if caVersion.LessThan(experimentVersion) {
		klog.Infof("Experiment %q is disabled: minCaVersion %q is greater than caVersion", experiments.ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag, experimentVersion)
		return false
	}
	return true
}

// parseCustomThresholdsPerErrorType reads YAML configuration from a string and returns the populated CustomThresholdsPerErrorType instance
func parseCustomThresholdsPerErrorType(s string) (CustomThresholdsPerErrorType, error) {
	ct := CustomThresholdsPerErrorType{}

	// when experiment is enabled but filtered out by some criteria empty string is passed
	// it can cause parsing errors
	if len(s) == 0 {
		s = "{}"
	}

	err := json.Unmarshal([]byte(s), &ct)
	if err != nil {
		return ct, fmt.Errorf("Parsing CustomThresholdsPerErrorType from a JSON string failed: %v", err)
	}

	if ct.Status != Enabled {
		ct.Status = Disabled
	}

	return ct, nil
}
