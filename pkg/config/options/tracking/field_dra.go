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
	"fmt"
	"strconv"
	"strings"

	"k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const (
	draApiMinEmulatedClusterVersionMinor = 34                         // DRA API is enabled by default since K8s 1.34
	draBoolMitigationExperiment          = experiments.DraEnabledFlag // The only expected value for this experiment is "false"
)

var dynamicResourceAllocationEnabledField = trackedField{
	name: "DynamicResourceAllocationEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.DynamicResourceAllocationEnabled == optsB.DynamicResourceAllocationEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.DynamicResourceAllocationEnabled)
	},
	setValueFromClusterProto: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, cluster gkeclient.Cluster, optsToModify *internalopts.AutoscalingOptions) error {
		apiAvailable, err := draApiAvailable(cluster)
		if err != nil {
			return err
		}
		if !apiAvailable {
			// When DRA integration is enabled, Cluster Autoscaler has a hard dependency on v1 DRA API. If the API is not available, CA will be stuck at boot
			// waiting for it to become available - so we need to disable the integration in this case.
			klog.V(5).Infof("DRA API is not enabled because emulatedClusterVersion=%q is less than the minimum required version \"1.%d\"; Disabling DRA integration", cluster.EmulatedClusterVersion, draApiMinEmulatedClusterVersionMinor)
			optsToModify.DynamicResourceAllocationEnabled = false
			return nil
		}

		// The DRA experiments are not defined by default, and are only supposed to be defined in case mitigation
		// is needed and the feature needs to be disabled. So by default the field value matches the CLI flag,
		// but any of the experiments override it if defined. Full details: go/gke-autoscaling-dra-rollout.
		flagValue := optsFromFlags.DynamicResourceAllocationEnabled
		// Defaults to flagValue if the version-based experiment isn't defined; if it is defined, the value depends on the current CA version (CA version lower -> we need to disable)
		noVersionBasedMitigation := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.DraMinCAVersionFlag, flagValue)
		// Defaults to flagValue if the bool experiment isn't defined; if it is defined, the only expected value is "false"
		noBoolMitigation := experimentsManager.EvaluateBoolFlagOrFailsafe(draBoolMitigationExperiment, flagValue)

		if flagValue {
			if !noVersionBasedMitigation {
				klog.V(5).Infof("DRA integration is disabled via the %s mitigation experiment", experiments.DraMinCAVersionFlag)
			}
			if !noBoolMitigation {
				klog.V(5).Infof("DRA integration is disabled via the %s mitigation experiment", draBoolMitigationExperiment)
			}
		}

		optsToModify.DynamicResourceAllocationEnabled = flagValue && noVersionBasedMitigation && noBoolMitigation
		return nil
	},
}

// draApiAvailable returns whether the v1 DRA API is available based on the provided Cluster proto.
func draApiAvailable(cluster gkeclient.Cluster) (bool, error) {
	emulatedVer := cluster.EmulatedClusterVersion

	// The v1 DRA API is available by default since K8s 1.34. DRA support in Cluster Autoscaler is also only available since 1.34,
	// so it seems like the API should always be available if DRA support is enabled in CA. However, that's not true if
	// Cluster.EmulatedClusterVersion is configured.
	if emulatedVer == "" {
		// No EmulatedClusterVersion configured, so DRA API should be available.
		return true, nil
	}

	// During the 1st phase of a "Safer upgrade" the cluster is first upgraded to the next minor, but the API server runs in an emulated mode,
	// pretending to still be the previous minor. The emulated version is recorded in the Cluster proto. If that emulated version is configured,
	// the DRA API will only be available if it's configured to at least "1.34". More details: go/gke-component-emulated-version.
	_, emulatedVerMinor, err := parseEmulatedClusterVersion(emulatedVer)
	if err != nil {
		return false, fmt.Errorf("couldn't parse emulatedClusterVersion %q, err: %v", emulatedVer, err)
	}
	apiAvailable := emulatedVerMinor >= draApiMinEmulatedClusterVersionMinor
	return apiAvailable, nil
}

func parseEmulatedClusterVersion(ver string) (int, int, error) {
	// emulatedClusterVersion should just have the major and minor numbers, e.g. "1.33"
	parts := strings.Split(ver, ".")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("emulatedClusterVersion should be in the format \"<major>.<minor>\", got %q instead", ver)
	}
	majorStr := parts[0]
	major, err := strconv.Atoi(majorStr)
	if err != nil {
		return 0, 0, fmt.Errorf("error when trying to conver major number %q to int", majorStr)
	}
	minorStr := parts[1]
	minor, err := strconv.Atoi(minorStr)
	if err != nil {
		return 0, 0, fmt.Errorf("error when trying to conver minor number %q to int", minorStr)
	}
	return major, minor, nil
}
