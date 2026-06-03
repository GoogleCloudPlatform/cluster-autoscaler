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

// SelectionType specifies how Cluster Autoscaler decided which machine families should be used.
type SelectionType string

const (
	// SelectionTypeNone means that the machine family was not successfully selected.
	SelectionTypeNone SelectionType = ""
	// SelectionTypeSpecified means that the machine family was specified by user.
	SelectionTypeSpecified SelectionType = "Specified"
	// SelectionTypeImplied means that the machine family was not explicitly specified, but
	// is implied by other settings specified by user.
	SelectionTypeImplied SelectionType = "Implied"
	// SelectionTypeDefault means that user didn't put any constraints on machine family and
	// the default value was used.
	SelectionTypeDefault SelectionType = "Default"
)
