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

package ops

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperationType_HasAny(t *testing.T) {
	tests := []struct {
		name string
		op   OperationType
		mask OperationType
		want bool
	}{
		{
			name: "single_flag_matches_itself",
			op:   SuspendOp,
			mask: SuspendOp,
			want: true,
		},
		{
			name: "single_flag_does_not_match_different_flag",
			op:   SuspendOp,
			mask: ConsumeOp,
			want: false,
		},
		{
			name: "combined_flags_match_one_of_them",
			op:   SuspendOp | ConsumeOp,
			mask: SuspendOp,
			want: true,
		},
		{
			name: "combined_flags_do_not_match_disjoint_flag",
			op:   SuspendOp | ConsumeOp,
			mask: AssignBufferOp,
			want: false,
		},
		{
			name: "noop_matches_nothing",
			op:   NoOp,
			mask: SuspendOp,
			want: false,
		},
		{
			name: "matches_overlapping_mask",
			op:   SuspendOp,
			mask: SuspendOp | ConsumeOp,
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.op.HasAny(tc.mask))
		})
	}
}

func TestOperationType_Contains(t *testing.T) {
	tests := []struct {
		name string
		op   OperationType
		mask OperationType
		want bool
	}{
		{
			name: "single_flag_contains_itself",
			op:   SuspendOp,
			mask: SuspendOp,
			want: true,
		},
		{
			name: "combined_flags_contain_subset",
			op:   SuspendOp | ConsumeOp,
			mask: SuspendOp,
			want: true,
		},
		{
			name: "combined_flags_contain_exact_match",
			op:   SuspendOp | ConsumeOp,
			mask: SuspendOp | ConsumeOp,
			want: true,
		},
		{
			name: "single_flag_does_not_contain_superset",
			op:   SuspendOp,
			mask: SuspendOp | ConsumeOp,
			want: false,
		},
		{
			name: "single_flag_does_not_contain_disjoint_flag",
			op:   SuspendOp,
			mask: ConsumeOp,
			want: false,
		},
		{
			name: "noop_contains_noop",
			op:   NoOp,
			mask: NoOp,
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.op.Contains(tc.mask))
		})
	}
}

func TestOperationType_With(t *testing.T) {
	tests := []struct {
		name string
		op   OperationType
		mask OperationType
		want OperationType
	}{
		{
			name: "add_flag_to_noop",
			op:   NoOp,
			mask: SuspendOp,
			want: SuspendOp,
		},
		{
			name: "add_flag_to_existing_different_flag",
			op:   SuspendOp,
			mask: ConsumeOp,
			want: SuspendOp | ConsumeOp,
		},
		{
			name: "add_existing_flag_is_noop",
			op:   SuspendOp | ConsumeOp,
			mask: SuspendOp,
			want: SuspendOp | ConsumeOp,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.op.With(tc.mask))
		})
	}
}

func TestOperationType_Without(t *testing.T) {
	tests := []struct {
		name string
		op   OperationType
		mask OperationType
		want OperationType
	}{
		{
			name: "remove_flag_from_combined",
			op:   SuspendOp | ConsumeOp,
			mask: SuspendOp,
			want: ConsumeOp,
		},
		{
			name: "remove_flag_from_itself",
			op:   SuspendOp,
			mask: SuspendOp,
			want: NoOp,
		},
		{
			name: "remove_non_existent_flag",
			op:   SuspendOp,
			mask: ConsumeOp,
			want: SuspendOp,
		},
		{
			name: "remove_from_noop",
			op:   NoOp,
			mask: SuspendOp,
			want: NoOp,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.op.Without(tc.mask))
		})
	}
}

func TestOperationType_String(t *testing.T) {
	tests := []struct {
		name string
		op   OperationType
		want string
	}{
		{
			name: "suspend_op",
			op:   SuspendOp,
			want: "SUSPEND",
		},
		{
			name: "consume_op",
			op:   ConsumeOp,
			want: "CONSUME",
		},
		{
			name: "assign_buffer_op",
			op:   AssignBufferOp,
			want: "ASSIGN_BUFFER",
		},
		{
			name: "no_op",
			op:   NoOp,
			want: "NO_OP",
		},
		{
			name: "unknown_op",
			op:   OperationType(3),
			want: "UNKNOWN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, fmt.Sprintf("%s", tc.op))
		})
	}
}
