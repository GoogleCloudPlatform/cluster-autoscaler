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

package flexadvisor

import "sync/atomic"

// atomicValue is a type-safe generic wrapper around sync/atomic.Value for value and enum types (e.g. PollingMode).
//
// The Go standard library's sync/atomic package does not provide a generic atomic.Value[T]:
//   - atomic.Value operates on untyped any: it lacks compile-time type safety (mismatched concrete types panic at
//     runtime), requires type assertions on every Load/Swap, and returns nil before the first Store (causing direct
//     v.Load().(T) assertions on zero-value structs to panic instead of returning T's zero value).
//   - atomic.Pointer[T] is the only generic type in sync/atomic, which requires pointer indirection/allocations for
//     small value enums and exposes nil states instead of value semantics.
//   - atomic.Int32 / atomic.Int64 are concrete primitive types, requiring manual integer casts at every call site for
//     custom defined types like PollingMode (and cannot hold non-integer types such as string-based enums).
type atomicValue[T comparable] struct {
	v atomic.Value
}

func (a *atomicValue[T]) Load() T {
	if val, ok := a.v.Load().(T); ok {
		return val
	}
	var zero T
	return zero
}

func (a *atomicValue[T]) Store(val T) {
	a.v.Store(val)
}

func (a *atomicValue[T]) Swap(newVal T) T {
	if old, ok := a.v.Swap(newVal).(T); ok {
		return old
	}
	var zero T
	return zero
}
