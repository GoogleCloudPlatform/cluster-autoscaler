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
	"fmt"
	"os"
	"slices"
	"strings"
	"unicode"

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

var flagOverrideStats *metrics.FlagOverrideStats

// GetFlagOverrideStats returns the stats collected during ProcessFlagOverrides,
// or an error if ProcessFlagOverrides has not been executed yet.
func GetFlagOverrideStats() (metrics.FlagOverrideStats, error) {
	if flagOverrideStats == nil {
		return metrics.FlagOverrideStats{}, fmt.Errorf("ProcessFlagOverrides was not called")
	}
	return *flagOverrideStats, nil
}

// ProcessFlagOverrides identifies any flags prefixed with --override_,
// removes their standard counterparts from os.Args according to the rules, and appends the new flags.
// All non-boolean CLI flags in os.Args[1:] must use the --flag=value (or -flag=value) format.
func ProcessFlagOverrides() {
	definedFlags := make(map[string]string)
	pflag.CommandLine.VisitAll(func(f *pflag.Flag) {
		definedFlags[f.Name] = f.DefValue
	})
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		definedFlags[f.Name] = f.DefValue
	})
	result, err := mergeFlagOverrides(os.Args, definedFlags)
	if err != nil {
		klog.Fatalf("[flag_override] Failed to process flag overrides: %v", err)
	}
	os.Args = result.args
	flagOverrideStats = &metrics.FlagOverrideStats{
		ActiveCount:       result.activeCount,
		UnrecognizedCount: result.unrecognizedCount,
		RedundantCount:    result.redundantCount,
	}
}

// mergeFlagOverrides merges CLI flags with --override_<flag>=<value> arguments.
// It removes base CLI flags that have a non-redundant override, replaces active
// --override_<flag>=<value> arguments with --<flag>=<value>, drops redundant or
// unrecognized overrides, and returns the resulting argument list along with
// counts of active, unrecognized, and redundant overrides for metrics.
func mergeFlagOverrides(args []string, definedFlags map[string]string) (flagOverrideResult, error) {
	if len(args) == 0 {
		return flagOverrideResult{}, nil
	}
	if err := validateFlagsFormat(args); err != nil {
		return flagOverrideResult{}, err
	}

	overrideVals, cliVals := collectFlagValues(args)
	if len(overrideVals) == 0 {
		return flagOverrideResult{args: args}, nil
	}
	redundantFlags := findRedundantFlags(overrideVals, cliVals, definedFlags)

	newArgs := []string{args[0]}
	var activeCount, unrecognizedCount, redundantCount int

	for _, arg := range args[1:] {
		flagName, value, _ := parseFlag(arg)

		// Handle override_
		if baseFlagName, ok := strings.CutPrefix(flagName, overridePrefix); ok {
			_, isDefined := definedFlags[baseFlagName]
			if !isDefined {
				unrecognizedCount++
				klog.Warningf("[flag_override] Skipping unrecognized flag override: --%s=%q", baseFlagName, value)
				continue
			}
			if redundantFlags[baseFlagName] {
				redundantCount++
				if len(cliVals[baseFlagName]) > 0 {
					klog.Infof("[flag_override] Skipping redundant flag override (matches CLI): --%s=%q", baseFlagName, value)
				} else {
					klog.Infof("[flag_override] Skipping redundant flag override (matches default): --%s=%q", baseFlagName, value)
				}
				continue
			}
			activeCount++
			klog.Infof("[flag_override] Setting flag override: --%s=%q", baseFlagName, value)
			newArg := "--" + baseFlagName + "=" + value
			newArgs = append(newArgs, newArg)
			continue
		}

		_, isDefined := definedFlags[flagName]
		if _, hasOverride := overrideVals[flagName]; hasOverride && isDefined && !redundantFlags[flagName] {
			klog.Infof("[flag_override] Dropping overridden CLI flag: --%s=%q", flagName, value)
			continue
		}

		newArgs = append(newArgs, arg)
	}

	if activeCount > 0 || redundantCount > 0 || unrecognizedCount > 0 {
		klog.Infof("[flag_override] Processed flag overrides (active: %d, redundant: %d, unrecognized: %d)", activeCount, redundantCount, unrecognizedCount)
	}

	return flagOverrideResult{
		args:              newArgs,
		activeCount:       activeCount,
		unrecognizedCount: unrecognizedCount,
		redundantCount:    redundantCount,
	}, nil
}

// validateFlagsFormat enforces that all non-boolean CLI flags and --override_*
// arguments use the --flag=value format (as generated in GKE manifests).
// Requiring '=' avoids ambiguous lookahead parsing for space-separated arguments
// (such as negative numbers like "--retry-count -3" or bare boolean flags).
// Validating all arguments upfront—even when no overrides are present—ensures
// that any space-separated flag accidentally added to a manifest fails fast in
// tests rather than silently breaking when an override is later applied.
func validateFlagsFormat(args []string) error {
	for _, arg := range args[1:] {
		if _, _, ok := parseFlag(arg); !ok {
			return fmt.Errorf("invalid CLI argument format (expected --flag=value or --bool-flag): %q", arg)
		}
	}
	return nil
}

func parseFlag(arg string) (string, string, bool) {
	if !strings.HasPrefix(arg, "-") || strings.HasPrefix(arg, "---") {
		return "", "", false
	}
	trimmed := strings.TrimLeft(arg, "-")
	if len(trimmed) == 0 || !unicode.IsLetter(rune(trimmed[0])) {
		return "", "", false
	}
	idx := strings.Index(trimmed, "=")
	if idx == -1 {
		if strings.HasPrefix(trimmed, overridePrefix) {
			return "", "", false
		}
		return trimmed, "true", true
	}
	if trimmed[:idx] == overridePrefix {
		return "", "", false
	}
	return trimmed[:idx], trimmed[idx+1:], true
}

func collectFlagValues(args []string) (map[string][]string, map[string][]string) {
	overrideVals := make(map[string][]string)
	cliVals := make(map[string][]string)
	for _, arg := range args[1:] {
		flagName, value, _ := parseFlag(arg)
		if baseName, ok := strings.CutPrefix(flagName, overridePrefix); ok {
			overrideVals[baseName] = append(overrideVals[baseName], value)
			continue
		}
		cliVals[flagName] = append(cliVals[flagName], value)
	}
	return overrideVals, cliVals
}

// findRedundantFlags identifies overrides whose values are identical to the
// effective values that would be used without the override (either the explicit
// CLI values if the flag was passed on the command line, or the flag's default
// value otherwise). This allows skipping no-op overrides and reporting them
// separately in metrics.
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

		if slices.Equal(oVals, rVals) {
			redundantFlags[baseName] = true
		}
	}
	return redundantFlags
}
