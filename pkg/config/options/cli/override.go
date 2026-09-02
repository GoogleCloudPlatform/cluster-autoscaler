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

package cli

import (
	"flag"
	"os"
	"strings"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

const overridePrefix = "override_"

type flagOverrideResult struct {
	args              []string
	activeCount       int
	unrecognizedCount int
}

// ProcessFlagOverrides identifies any flags prefixed with --override_,
// removes their standard counterparts from os.Args according to the rules, and appends the new flags.
func ProcessFlagOverrides() {
	definedFlags := make(map[string]bool)
	pflag.CommandLine.VisitAll(func(f *pflag.Flag) {
		definedFlags[f.Name] = true
	})
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		definedFlags[f.Name] = true
	})
	result := processFlagOverrides(os.Args, definedFlags)
	os.Args = result.args
	metrics.UpdateComponentFlagOverrides(result.activeCount, result.unrecognizedCount)
}

func parseFlagOverrides(args []string) map[string]bool {
	flagOverrides := make(map[string]bool)

	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		flagName := strings.TrimLeft(arg, "-")

		if strings.HasPrefix(flagName, overridePrefix) {
			flagName = strings.TrimPrefix(flagName, overridePrefix)
			if idx := strings.Index(flagName, "="); idx != -1 {
				flagName = flagName[:idx]
				flagOverrides[flagName] = true
			}
		}
	}
	return flagOverrides
}

func processFlagOverrides(args []string, definedFlags map[string]bool) flagOverrideResult {
	flagOverrides := parseFlagOverrides(args)
	var newArgs []string
	skipNext := false
	var activeCount, unrecognizedCount int

	for i := 0; i < len(args); i++ {
		if skipNext {
			skipNext = false
			continue
		}

		arg := args[i]

		if !strings.HasPrefix(arg, "-") {
			newArgs = append(newArgs, arg)
			continue
		}

		flagName := strings.TrimLeft(arg, "-")

		var value string
		hasValue := false
		if idx := strings.Index(flagName, "="); idx != -1 {
			value = flagName[idx+1:]
			flagName = flagName[:idx]
			hasValue = true
		}

		// Handle override_
		if strings.HasPrefix(flagName, overridePrefix) {
			if !hasValue {
				klog.Warningf("[flag_override] Skipping flag override without value: --%s", flagName)
				continue
			}

			baseFlagName := strings.TrimPrefix(flagName, overridePrefix)

			if definedFlags[baseFlagName] {
				activeCount++
				klog.Infof("[flag_override] Setting flag: --%s=%q", baseFlagName, value)
				newArg := "--" + baseFlagName + "=" + value
				newArgs = append(newArgs, newArg)
			} else {
				unrecognizedCount++
				klog.Warningf("[flag_override] Skipping unrecognized flag override: --%s=%q", baseFlagName, value)
			}
			continue
		}

		// Handle base flags
		valueFromNext := false
		if !hasValue && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			value = args[i+1]
			hasValue = true
			valueFromNext = true
		}

		if flagOverrides[flagName] {
			klog.Infof("[flag_override] Dropping flag: --%s=%q", flagName, value)
			if valueFromNext {
				skipNext = true
			}
			continue
		}

		newArgs = append(newArgs, arg)
	}

	if activeCount > 0 {
		klog.Infof("[flag_override] Applied flag overrides: %d", activeCount)
	}

	return flagOverrideResult{
		args:              newArgs,
		activeCount:       activeCount,
		unrecognizedCount: unrecognizedCount,
	}
}
