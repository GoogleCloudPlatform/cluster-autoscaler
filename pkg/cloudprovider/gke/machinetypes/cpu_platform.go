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

package machinetypes

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

// CpuPlatform is a representation of a GCE CPU platform.
type CpuPlatform string

// IsAny returns true if the platform represents any platform.
func (p CpuPlatform) IsAny() bool {
	return p == AnyPlatform
}

// IsUnknown returns true if the platform represents an unkown platform.
func (p CpuPlatform) IsUnknown() bool {
	return p == UnknownPlatform
}

// CpuPlatformRequirements represents inclusive boundary requirements for a CPU platform.
type CpuPlatformRequirements struct {
	// inclusive
	lowerBound CpuPlatform
	// inclusive
	upperBound CpuPlatform
}

// NewCpuPlatformRequirements returns a newly create CpuPlatformRequirements
func NewCpuPlatformRequirements(lower, upper CpuPlatform) CpuPlatformRequirements {
	return CpuPlatformRequirements{lower, upper}
}

// validate checks whether the given platform is between the bounds of the requirements. Always
// returns false for unknownPlatform and true for AnyPlatform.
// TODO(b/474309633): get rid of this method altogether when migrating to MachineConfigProvider.
func (r CpuPlatformRequirements) validate(platform CpuPlatform) bool {
	return cpuPlatforms.validateRequirements(r, platform)
}

// ToCpuPlatform returns a representation of a CPU platform by its name. Names with underscores in place
// of spaces are still accepted.
func ToCpuPlatform(platform string) (CpuPlatform, error) {
	nameWithSpaces := CpuPlatform(strings.ReplaceAll(platform, "_", " "))
	p, found := cpuPlatforms.get(nameWithSpaces)
	if !found {
		return UnknownPlatform, fmt.Errorf("unknown CPU platform %q", platform)
	}
	return p.name, nil
}

// PlatformsLowerOrEqualTo returns all platforms from the given platform's vendor that are lower or equal to the given platform.
func PlatformsLowerOrEqualTo(platform CpuPlatform) []CpuPlatform {
	var result []CpuPlatform
	if platform.IsAny() {
		klog.Warningf("skipping finding platforms lower or equal to %s", platform)
		result = append(result, platform)
	}
	platformInfo, found := cpuPlatforms.get(platform)
	if !found {
		klog.Warningf("comparing unknown CPU platform %s", platform)
		return append(result, platform)
	}
	for _, p := range cpuPlatforms.platformByName {
		if platformInfo.vendor == p.vendor && platformInfo.order >= p.order {
			result = append(result, p.name)
		}
	}
	return result
}

// CanonicalCpuPlatformName returns the name of a given platform that can be passed to GCE (without underscores). If underscores
// is true, the returned name has underscores instead of spaces (e.g. to be used in k8s labels). An empty string is returned
// for anyPlatform (as the correct value to pass to GCE for AnyPlatform), and for unknownPlatform and unexpected CpuPlatform values
// (as a fallback to anyPlatform behavior for unexpected values).
// TODO(b/476063718): Refactor this to (string, found) instead of defaulting to "" implicitly.
func CanonicalCpuPlatformName(platform CpuPlatform, underscores bool) string {
	p, found := cpuPlatforms.get(platform)
	if !found {
		return ""
	}
	name := string(p.name)
	if underscores {
		return strings.ReplaceAll(name, " ", "_")
	}
	return name
}

// CpuPlatformDebugName returns the name of a given platform that can be used for debugging purposes (e.g. logging).
// Special strings are returned for unknownPlatform and anyPlatform, an empty string is returned for unexpected
// CpuPlatform values.
func CpuPlatformDebugName(p CpuPlatform) string {
	if p.IsAny() || p.IsUnknown() {
		return string(p)
	}
	platform, found := cpuPlatforms.get(p)
	if !found {
		return ""
	}
	return string(platform.name)
}

// PlatformIsAtLeast returns true if the toKnow platform has greater or equal order than base platform.
func PlatformIsAtLeast(toKnow, base CpuPlatform) bool {
	return cpuPlatforms.isAtLeast(toKnow, base)
}

// compare is the idiomatic function used for sorting a platform slice.
// it implies that the compared platforms are present in the considered platform order.
func compareOrder(p1, p2 cpuPlatformInfo) int {
	return p1.order - p2.order
}

// BoundsOrFail returns the lower and upper bound CPU platforms from a given slice.
// It also validates if all platforms are within the same platform order, and rejects an empty list.
func BoundsOrFail(platforms []cpuPlatformInfo) (CpuPlatform, CpuPlatform, []error) {
	if errs := validateSameVendor(platforms); errs != nil {
		return UnknownPlatform, UnknownPlatform, errs
	}

	return slices.MinFunc(platforms, compareOrder).name, slices.MaxFunc(platforms, compareOrder).name, nil
}

// validateSameVendor checks if a given slice of CPU platforms belong to the same platform order. Rejects an empty list.
func validateSameVendor(platforms []cpuPlatformInfo) []error {
	if len(platforms) == 0 {
		return []error{fmt.Errorf("empty CPU platform list")}
	}

	var errs []error

	for _, p := range platforms {
		if p.vendor != platforms[0].vendor || p.vendor == "" {
			errs = append(errs, fmt.Errorf("platforms %s, %s do not belong to the same vendor (%s, %s)",
				CpuPlatformDebugName(platforms[0].name), CpuPlatformDebugName(p.name), platforms[0].vendor, p.vendor))
		}
	}
	if len(errs) > 0 {
		return errs
	}

	return nil
}

type cpuPlatformInfo struct {
	name    CpuPlatform
	aliases []CpuPlatform
	vendor  string
	order   int
}

func newCpuPlatformInfo(name CpuPlatform, vendor string, aliases []CpuPlatform) cpuPlatformInfo {
	return cpuPlatformInfo{
		name:    name,
		aliases: aliases,
		vendor:  vendor,
		// order will be set when the platform is registered in source
	}
}

// IsAtLeast returns true if and only if both platforms belong to the same vendor, and p is higher or equal to
// otherPlatform.
func (p cpuPlatformInfo) isAtLeast(otherPlatform cpuPlatformInfo) bool {
	if p.vendor == "" || p.vendor != otherPlatform.vendor {
		// Platforms belong to a different vendor.
		return false
	}
	return p.order >= otherPlatform.order
}

type cpuPlatformsSource struct {
	platformByName map[CpuPlatform]cpuPlatformInfo
	aliases        map[CpuPlatform]CpuPlatform
	vendorCounter  map[string]int
	mu             sync.RWMutex
}

func newCpuPlatformsSource() *cpuPlatformsSource {
	return &cpuPlatformsSource{
		platformByName: map[CpuPlatform]cpuPlatformInfo{},
		aliases:        map[CpuPlatform]CpuPlatform{},
		vendorCounter:  map[string]int{},
	}
}

func (s *cpuPlatformsSource) register(p cpuPlatformInfo) *cpuPlatformsSource {
	s.mu.Lock()
	defer s.mu.Unlock()

	next, _ := s.vendorCounter[p.vendor]
	s.vendorCounter[p.vendor] = next + 1
	p.order = next
	s.platformByName[p.name] = p
	for _, alias := range p.aliases {
		s.aliases[alias] = p.name
	}
	return s
}

// registerDynamic safely adds a CPU platform to the global registry, preserving the order specified by the CRD.
func (s *cpuPlatformsSource) registerDynamic(p cpuPlatformInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.platformByName[p.name] = p
	for _, alias := range p.aliases {
		s.aliases[alias] = p.name
	}
}

func (s *cpuPlatformsSource) get(name CpuPlatform) (cpuPlatformInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	canonical, found := s.aliases[name]
	if !found {
		canonical = CpuPlatform(name)
	}
	p, found := s.platformByName[canonical]
	return p, found
}

func (s *cpuPlatformsSource) isAtLeast(toKnow, base CpuPlatform) bool {
	basePlatform, found := s.get(base)
	if !found {
		return false
	}
	toKnowPlatform, found := s.get(toKnow)
	if !found {
		return false
	}
	return toKnowPlatform.isAtLeast(basePlatform)
}

func (s *cpuPlatformsSource) validateRequirements(reqs CpuPlatformRequirements, p CpuPlatform) bool {
	if p.IsUnknown() {
		return false
	}
	if p.IsAny() {
		return true
	}
	platform, pFound := s.get(p)
	lower, lFound := s.get(reqs.lowerBound)
	upper, uFound := s.get(reqs.upperBound)
	if !pFound || !lFound || !uFound {
		return false
	}
	return platform.isAtLeast(lower) && upper.isAtLeast(platform)
}
