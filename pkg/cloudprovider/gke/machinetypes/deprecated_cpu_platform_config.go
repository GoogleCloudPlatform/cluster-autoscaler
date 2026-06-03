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

const (
	intel  = "Intel"
	amd    = "AMD"
	arm    = "ARM"
	google = "Google"

	AnyPlatform     CpuPlatform = "__ANY_PLATFORM"
	UnknownPlatform CpuPlatform = "__UNKNOWN_PLATFORM"

	IntelSandyBridge    = "Intel Sandy Bridge"
	IntelIvyBridge      = "Intel Ivy Bridge"
	IntelHaswell        = "Intel Haswell"
	IntelBroadwell      = "Intel Broadwell"
	IntelSkylake        = "Intel Skylake"
	IntelCascadeLake    = "Intel Cascade Lake"
	IntelIceLake        = "Intel Ice Lake"
	IntelSapphireRapids = "Intel Sapphire Rapids"
	IntelEmeraldRapids  = "Intel Emerald Rapids"
	IntelGraniteRapids  = "Intel Granite Rapids"
	AmdRome             = "AMD Rome"
	AmdMilan            = "AMD Milan"
	AmdGenoa            = "AMD Genoa"
	AmdTurin            = "AMD Turin"
	AmpereAltra         = "Ampere Altra"
	GoogleAxion         = "Google Axion"
	GoogleAxionTamar    = "Google Axion (Tamar)"
	NvidiaGrace         = "Nvidia Grace"
)

var (
	// anyPlatform is an object representing a platform that matches everything.
	anyPlatform = cpuPlatformInfo{name: AnyPlatform}
	// unknownPlatform is an object representing an unknown platform.
	unknownPlatform = cpuPlatformInfo{name: UnknownPlatform}

	intelSandyBridgePlatform    = newCpuPlatformInfo(IntelSandyBridge, intel, []CpuPlatform{"sandybridge", "intel-sandybridge"})
	intelIvyBridgePlatform      = newCpuPlatformInfo(IntelIvyBridge, intel, []CpuPlatform{"ivybridge", "intel-ivybridge"})
	intelHaswellPlatform        = newCpuPlatformInfo(IntelHaswell, intel, []CpuPlatform{"haswell", "intel-haswell"})
	intelBroadwellPlatform      = newCpuPlatformInfo(IntelBroadwell, intel, []CpuPlatform{"broadwell", "intel-broadwell"})
	intelSkylakePlatform        = newCpuPlatformInfo(IntelSkylake, intel, []CpuPlatform{"skylake", "intel-skylake"})
	intelCascadeLakePlatform    = newCpuPlatformInfo(IntelCascadeLake, intel, []CpuPlatform{"cascadelake", "intel-cascadelake", "Intel Cascadelake"})
	intelIceLakePlatform        = newCpuPlatformInfo(IntelIceLake, intel, []CpuPlatform{"icelake", "intel-icelake", "Intel Icelake"})
	intelSapphireRapidsPlatform = newCpuPlatformInfo(IntelSapphireRapids, intel, []CpuPlatform{"sapphirerapids", "intel-sapphirerapids"})
	intelEmeraldRapidsPlatform  = newCpuPlatformInfo(IntelEmeraldRapids, intel, []CpuPlatform{"emeraldrapids", "intel-emeraldrapids", "Intel Emeraldrapids"})
	// TODO(b/476062834): Review the aliases and figure out if 'granate' is a typo (and if someone actually uses it in prod).
	intelGraniteRapidsPlatform = newCpuPlatformInfo(IntelGraniteRapids, intel, []CpuPlatform{"intel-granaterapids", "Intel Granite Rapids"})
	amdRomePlatform            = newCpuPlatformInfo(AmdRome, amd, []CpuPlatform{"rome", "amd-rome"})
	amdMilanPlatform           = newCpuPlatformInfo(AmdMilan, amd, []CpuPlatform{"milan", "Amd Milan", "amd-milan"})
	amdGenoaPlatform           = newCpuPlatformInfo(AmdGenoa, amd, []CpuPlatform{"Amd Genoa", "amd-genoa"})
	amdTurinPlatform           = newCpuPlatformInfo(AmdTurin, amd, []CpuPlatform{"Amd Turin", "amd-turin"})
	ampereAltraPlatform        = newCpuPlatformInfo(AmpereAltra, arm, []CpuPlatform{"altra", "ampere-altra", "ampere altra"})
	googleAxionPlatform        = newCpuPlatformInfo(GoogleAxion, google, []CpuPlatform{"google-axion", "axion"})
	googleAxionTamarPlatform   = newCpuPlatformInfo(GoogleAxionTamar, google, []CpuPlatform{"google-axion-tamar"})
	nvidiaGracePlatform        = newCpuPlatformInfo(NvidiaGrace, arm, []CpuPlatform{"NVIDIA Grace", "grace", "nvidia-grace", "stellarisa-grace", "StellarisA Grace"})

	cpuPlatforms = newCpuPlatformsSource().
			register(intelSandyBridgePlatform).
			register(intelIvyBridgePlatform).
			register(intelHaswellPlatform).
			register(intelBroadwellPlatform).
			register(intelSkylakePlatform).
			register(intelCascadeLakePlatform).
			register(intelIceLakePlatform).
			register(intelSapphireRapidsPlatform).
			register(intelEmeraldRapidsPlatform).
			register(intelGraniteRapidsPlatform).
			register(amdRomePlatform).
			register(amdMilanPlatform).
			register(amdGenoaPlatform).
			register(amdTurinPlatform).
			register(ampereAltraPlatform).
			register(googleAxionPlatform).
			register(googleAxionTamarPlatform).
			register(nvidiaGracePlatform)

	noPlatformSupported = CpuPlatformRequirements{lowerBound: UnknownPlatform, upperBound: UnknownPlatform}
)
