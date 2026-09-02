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
	redundantCount    int
}

// ProcessFlagOverrides identifies any flags prefixed with --override_,
// removes their standard counterparts from os.Args according to the rules, and appends the new flags.
func ProcessFlagOverrides() {
	definedFlags := make(map[string]string)
	pflag.CommandLine.VisitAll(func(f *pflag.Flag) {
		definedFlags[f.Name] = f.DefValue
	})
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		definedFlags[f.Name] = f.DefValue
	})
	result := resolveFlags(os.Args, definedFlags)
	os.Args = result.args
	metrics.UpdateComponentFlagOverrides(result.activeCount, result.unrecognizedCount, result.redundantCount)
}

func resolveFlags(args []string, definedFlags map[string]string) flagOverrideResult {
	overrideVals, cliVals := collectFlagValues(args)
	redundantFlags := findRedundantFlags(overrideVals, cliVals, definedFlags)

	var newArgs []string
	skipNext := false
	var activeCount, unrecognizedCount, redundantCount int

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

		valueFromNext := false
		if !hasValue && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			value = args[i+1]
			hasValue = true
			valueFromNext = true
		}

		// Handle override_
		if strings.HasPrefix(flagName, overridePrefix) {
			if !hasValue {
				klog.Warningf("[flag_override] Skipping flag override without value: --%s", flagName)
				continue
			}
			if valueFromNext {
				skipNext = true
			}

			baseFlagName := strings.TrimPrefix(flagName, overridePrefix)
			_, isDefined := definedFlags[baseFlagName]
			if isDefined {
				if redundantFlags[baseFlagName] {
					redundantCount++
					if len(cliVals[baseFlagName]) > 0 {
						klog.Infof("[flag_override] Skipping redundant flag override (matches CLI): --%s=%q", baseFlagName, value)
					} else {
						klog.Infof("[flag_override] Skipping redundant flag override (matches default): --%s=%q", baseFlagName, value)
					}
				} else {
					activeCount++
					klog.Infof("[flag_override] Setting flag: --%s=%q", baseFlagName, value)
					newArg := "--" + baseFlagName + "=" + value
					newArgs = append(newArgs, newArg)
				}
			} else {
				unrecognizedCount++
				klog.Warningf("[flag_override] Skipping unrecognized flag override: --%s=%q", baseFlagName, value)
			}
			continue
		}

		if _, ok := overrideVals[flagName]; ok && !redundantFlags[flagName] {
			klog.Infof("[flag_override] Dropping flag: --%s=%q", flagName, value)
			if valueFromNext {
				skipNext = true
			}
			continue
		}

		newArgs = append(newArgs, arg)
		if valueFromNext {
			newArgs = append(newArgs, args[i+1])
			skipNext = true
		}
	}

	if activeCount > 0 || redundantCount > 0 || unrecognizedCount > 0 {
		klog.Infof("[flag_override] Applied flag overrides: %d (redundant: %d, unrecognized: %d)", activeCount, redundantCount, unrecognizedCount)
	}

	return flagOverrideResult{
		args:              newArgs,
		activeCount:       activeCount,
		unrecognizedCount: unrecognizedCount,
		redundantCount:    redundantCount,
	}
}

func collectFlagValues(args []string) (map[string][]string, map[string][]string) {
	overrideVals := make(map[string][]string)
	cliVals := make(map[string][]string)
	skipNext := false
	for i := 0; i < len(args); i++ {
		if skipNext {
			skipNext = false
			continue
		}

		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
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

		valueFromNext := false
		if !hasValue && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			value = args[i+1]
			hasValue = true
			valueFromNext = true
		}

		if strings.HasPrefix(flagName, overridePrefix) {
			if hasValue {
				baseName := strings.TrimPrefix(flagName, overridePrefix)
				overrideVals[baseName] = append(overrideVals[baseName], value)
				if valueFromNext {
					skipNext = true
				}
			}
			continue
		}

		cliVals[flagName] = append(cliVals[flagName], value)
		if valueFromNext {
			skipNext = true
		}
	}
	return overrideVals, cliVals
}

func findRedundantFlags(overrideVals, cliVals map[string][]string, definedFlags map[string]string) map[string]bool {
	redundantFlags := make(map[string]bool)
	for baseName, oVals := range overrideVals {
		defVal, isDefined := definedFlags[baseName]
		if !isDefined {
			continue
		}

		rVals := cliVals[baseName]
		if len(rVals) == 0 {
			rVals = []string{defVal}
		}

		if len(oVals) == len(rVals) {
			same := true
			for i := range oVals {
				if oVals[i] != rVals[i] {
					same = false
					break
				}
			}
			if same {
				redundantFlags[baseName] = true
			}
		}
	}
	return redundantFlags
}
