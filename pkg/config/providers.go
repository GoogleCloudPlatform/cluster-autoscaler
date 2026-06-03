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

package config

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

type Provider[T any] interface {
	// Provide provides a value of type T.
	Provide() T
}

func NewExperimentStringSetProvider(experimentEvaluator StringFlagEvaluator, experimentFlag string, fallback Provider[sets.Set[string]]) ExperimentProvider[sets.Set[string]] {
	convert := func(s string) (sets.Set[string], bool) {
		values := splitOrEmpty(s)
		return sets.New(values...), len(values) > 0
	}
	return ExperimentProvider[sets.Set[string]]{
		ExperimentEvaluator: experimentEvaluator,
		ExperimentFlag:      experimentFlag,
		Convert:             convert,
		Fallback:            fallback,
	}
}

func NewSimpleStringSetProvider(values []string) SimpleProvider[sets.Set[string]] {
	return SimpleProvider[sets.Set[string]]{Value: sets.New[string](values...)}
}

func NewCommaSeparatedStringSetProvider(commaSeparated string) SimpleProvider[sets.Set[string]] {
	return NewSimpleStringSetProvider(splitOrEmpty(commaSeparated))
}

type SimpleProvider[T any] struct {
	Value T
}

// Provide returns values.
func (p SimpleProvider[T]) Provide() T {
	return p.Value
}

type StringFlagEvaluator interface {
	EvaluateStringFlagOrFailsafe(flag, fallback string) string
}

type ExperimentProvider[T any] struct {
	ExperimentFlag      string
	ExperimentEvaluator StringFlagEvaluator
	Convert             func(string) (T, bool)
	Fallback            Provider[T]
}

// Provide returns the value of ExperimentFlag obtained from StringFlagEvaluator, and converted to T using Covert() method.
// If there is no value or the conversion fails, it returns the value from fallback provider.
func (p ExperimentProvider[T]) Provide() T {
	flagValue := p.ExperimentEvaluator.EvaluateStringFlagOrFailsafe(p.ExperimentFlag, "")
	converted, hasValue := p.Convert(flagValue)
	if !hasValue {
		return p.Fallback.Provide()
	}
	return converted
}

// splitOrEmpty converts a comma-separated string to a slice.
func splitOrEmpty(commaSeparated string) []string {
	// strings.Split returns a 1-element slice for empty string
	if len(commaSeparated) == 0 {
		return []string{}
	}
	return strings.Split(commaSeparated, ",")
}
