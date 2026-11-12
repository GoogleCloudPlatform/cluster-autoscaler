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

package fake

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	ekerrors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
)

func TestClientGetCurrentResizableVmState(t *testing.T) {
	mcp := machinetypes.NewMachineConfigProvider(nil)
	inst := gce.GceRef{Project: "p", Zone: "us-central1-a", Name: "node-1"}
	size2vCPU := ekvmsize.VmSize{MilliCpus: 2000, KBytes: 8 * ekvmsize.GiBToKiB}
	size4vCPU := ekvmsize.VmSize{MilliCpus: 4000, KBytes: 16 * ekvmsize.GiBToKiB}
	size8vCPU := ekvmsize.VmSize{MilliCpus: 8000, KBytes: 32 * ekvmsize.GiBToKiB}

	tests := []struct {
		name           string
		withResolver   bool
		machineType    string
		nilProvider    bool
		defaultSize    *ekvmsize.VmSize
		setDefaultSize *ekvmsize.VmSize
		initialState   *ekvmtypes.ResizableVmState
		initialSize    *ekvmsize.VmSize
		deleteInitial  bool
		wantState      ekvmtypes.ResizableVmState
		wantErr        bool
		wantUncached   bool
	}{
		{
			name:         "resolves_ek_machine_type_max_size",
			withResolver: true,
			machineType:  "ek-standard-8",
			wantState: ekvmtypes.ResizableVmState{
				Size:   size8vCPU,
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:         "resolves_e4_machine_type_max_size",
			withResolver: true,
			machineType:  "e4-standard-8",
			wantState: ekvmtypes.ResizableVmState{
				Size:   size8vCPU,
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:         "resolves_e4a_machine_type_max_size",
			withResolver: true,
			machineType:  "e4a-standard-8",
			wantState: ekvmtypes.ResizableVmState{
				Size:   size8vCPU,
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:         "uses_default_size_over_machine_type",
			withResolver: true,
			machineType:  "e4-standard-8",
			defaultSize:  &size2vCPU,
			wantState: ekvmtypes.ResizableVmState{
				Size:   size2vCPU,
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:           "set_default_size_helper_sets_fallback_size",
			withResolver:   true,
			machineType:    "e4-standard-8",
			setDefaultSize: &size2vCPU,
			wantState: ekvmtypes.ResizableVmState{
				Size:   size2vCPU,
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:        "per_instance_state_overrides_default_size",
			defaultSize: &size2vCPU,
			initialState: &ekvmtypes.ResizableVmState{
				Size:   size4vCPU,
				Status: ekvmtypes.ResizeStatusUnknownCA,
			},
			wantState: ekvmtypes.ResizableVmState{
				Size:   size4vCPU,
				Status: ekvmtypes.ResizeStatusUnknownCA,
			},
		},
		{
			name:        "set_vm_size_helper_sets_size_at_intent",
			defaultSize: &size2vCPU,
			initialSize: &size4vCPU,
			wantState: ekvmtypes.ResizableVmState{
				Size:   size4vCPU,
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:        "deleted_instance_state_falls_back_to_default_size",
			defaultSize: &size2vCPU,
			initialState: &ekvmtypes.ResizableVmState{
				Size:   size4vCPU,
				Status: ekvmtypes.ResizeStatusUnknownCA,
			},
			deleteInitial: true,
			wantState: ekvmtypes.ResizableVmState{
				Size:   size2vCPU,
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:         "unknown_machine_type_returns_error_without_caching",
			withResolver: true,
			machineType:  "unknown-standard-8",
			wantErr:      true,
			wantUncached: true,
		},
		{
			name:         "missing_resolver_returns_zero_state_without_caching",
			wantState:    ekvmtypes.ResizableVmState{},
			wantUncached: true,
		},
		{
			name:         "empty_machine_type_returns_zero_state_without_caching",
			withResolver: true,
			machineType:  "",
			wantState:    ekvmtypes.ResizableVmState{},
			wantUncached: true,
		},
		{
			name:         "nil_provider_returns_zero_state_without_caching",
			withResolver: true,
			machineType:  "ek-standard-8",
			nilProvider:  true,
			wantState:    ekvmtypes.ResizableVmState{},
			wantUncached: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var opts []Option
			if tc.withResolver {
				opts = append(opts, WithMachineTypeResolver(func(ref gce.GceRef) string {
					if ref == inst {
						return tc.machineType
					}
					return ""
				}))
			}
			if tc.defaultSize != nil {
				opts = append(opts, WithDefaultSize(*tc.defaultSize))
			}
			client := NewClient(opts...)
			if tc.setDefaultSize != nil {
				client.SetDefaultSize(*tc.setDefaultSize)
			}
			if tc.initialState != nil {
				client.SetVMState(inst, *tc.initialState)
			}
			if tc.initialSize != nil {
				client.SetVMSize(inst, *tc.initialSize)
			}
			if tc.deleteInitial {
				client.DeleteVMState(inst)
			}

			var provider resizablevms.MaxResizableVmSizeProvider = mcp
			if tc.nilProvider {
				provider = nil
			}
			got, err := client.GetCurrentResizableVmState(provider, inst)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("GetCurrentResizableVmState(%v) = (%+v, nil), want error", inst, got)
				}
			} else {
				if err != nil {
					t.Fatalf("GetCurrentResizableVmState(%v) unexpected error: %v", inst, err)
				}
				if diff := cmp.Diff(tc.wantState, got); diff != "" {
					t.Errorf("GetCurrentResizableVmState(%v) diff (-want +got):\n%s", inst, diff)
				}
			}

			bulk, err := client.BulkFetchCurrentResizableVmStates(mcp, "p", "c")
			if err != nil {
				t.Fatalf("BulkFetchCurrentResizableVmStates() unexpected error: %v", err)
			}
			gotBulk, cached := bulk[inst]
			if tc.wantUncached {
				if cached {
					t.Errorf("BulkFetchCurrentResizableVmStates()[%v] = %+v, want instance not cached", inst, gotBulk)
				}
				return
			}
			if !cached {
				t.Fatalf("BulkFetchCurrentResizableVmStates()[%v] missing, want %+v", inst, tc.wantState)
			}
			if diff := cmp.Diff(tc.wantState, gotBulk); diff != "" {
				t.Errorf("BulkFetchCurrentResizableVmStates()[%v] diff (-want +got):\n%s", inst, diff)
			}
		})
	}
}

func TestClientResizeVm(t *testing.T) {
	inst := gce.GceRef{Project: "p", Zone: "us-central1-a", Name: "node-a"}
	tests := []struct {
		name           string
		statusOnResize ekvmtypes.ResizeStatus
		desiredCPU     int64
		desiredMemKB   int64
		wantState      ekvmtypes.ResizableVmState
	}{
		{
			name:         "records_call_and_uses_default_at_intent_status",
			desiredCPU:   6000,
			desiredMemKB: 24 * ekvmsize.GiBToKiB,
			wantState: ekvmtypes.ResizableVmState{
				Size:   ekvmsize.VmSize{MilliCpus: 6000, KBytes: 24 * ekvmsize.GiBToKiB},
				Status: ekvmtypes.ResizeStatusAtIntent,
			},
		},
		{
			name:           "applies_custom_status_on_resize",
			statusOnResize: ekvmtypes.ResizeStatusInProgress,
			desiredCPU:     8000,
			desiredMemKB:   32 * ekvmsize.GiBToKiB,
			wantState: ekvmtypes.ResizableVmState{
				Size:   ekvmsize.VmSize{MilliCpus: 8000, KBytes: 32 * ekvmsize.GiBToKiB},
				Status: ekvmtypes.ResizeStatusInProgress,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient()
			if tc.statusOnResize != "" {
				client.SetStatusOnResize(tc.statusOnResize)
			}

			if err := client.ResizeVm(t.Context(), inst, tc.desiredCPU, tc.desiredMemKB); err != nil {
				t.Fatalf("ResizeVm(%v, %d, %d) unexpected error: %v", inst, tc.desiredCPU, tc.desiredMemKB, err)
			}

			wantCalls := []ResizeCall{{
				Instance:             inst,
				DesiredCPUMillicores: tc.desiredCPU,
				DesiredMemoryKB:      tc.desiredMemKB,
			}}
			if diff := cmp.Diff(wantCalls, client.ResizeCalls()); diff != "" {
				t.Errorf("ResizeCalls() diff (-want +got):\n%s", diff)
			}

			gotState, ok := client.VMState(inst)
			if !ok {
				t.Fatalf("VMState(%v) = (_, false), want (_, true)", inst)
			}
			if diff := cmp.Diff(tc.wantState, gotState); diff != "" {
				t.Errorf("VMState(%v) diff (-want +got):\n%s", inst, diff)
			}

			client.ResetResizeCalls()
			if got := client.ResizeCalls(); len(got) != 0 {
				t.Errorf("ResizeCalls() after ResetResizeCalls() = %v, want empty", got)
			}
		})
	}
}

func TestClientErrorInjection(t *testing.T) {
	mcp := machinetypes.NewMachineConfigProvider(nil)
	instA := gce.GceRef{Project: "p", Zone: "us-central1-a", Name: "node-a"}
	instB := gce.GceRef{Project: "p", Zone: "us-central1-a", Name: "node-b"}
	initialState := ekvmtypes.ResizableVmState{
		Size:   ekvmsize.VmSize{MilliCpus: 2000, KBytes: 8 * ekvmsize.GiBToKiB},
		Status: ekvmtypes.ResizeStatusAtIntent,
	}
	resizedState := ekvmtypes.ResizableVmState{
		Size:   ekvmsize.VmSize{MilliCpus: 8000, KBytes: 32 * ekvmsize.GiBToKiB},
		Status: ekvmtypes.ResizeStatusAtIntent,
	}
	hostErr := ekerrors.NewNotEnoughResourceOnHostError("e4", errors.New("no host capacity"))
	globalResizeErr := errors.New("global resize failure")
	instGetErr := errors.New("instance get failure")
	globalGetErr := errors.New("global get failure")
	bulkErr := errors.New("bulk fetch failure")

	tests := []struct {
		name               string
		instResizeErr      error
		clearInstResizeErr bool
		globResizeErr      error
		instGetErr         error
		clearInstGetErr    bool
		globGetErr         error
		bulkErr            error
		queryInst          gce.GceRef
		wantResizeErr      error
		wantGetErr         error
		wantBulkErr        error
	}{
		{
			name:          "instance_resize_error_matches_target_instance",
			instResizeErr: hostErr,
			queryInst:     instA,
			wantResizeErr: hostErr,
		},
		{
			name:          "instance_resize_error_does_not_affect_sibling_instance",
			instResizeErr: hostErr,
			queryInst:     instB,
		},
		{
			name:          "global_resize_error_applies_to_all_instances",
			globResizeErr: globalResizeErr,
			queryInst:     instB,
			wantResizeErr: globalResizeErr,
		},
		{
			name:       "instance_get_error_matches_target_instance",
			instGetErr: instGetErr,
			queryInst:  instA,
			wantGetErr: instGetErr,
		},
		{
			name:        "global_get_and_bulk_errors",
			globGetErr:  globalGetErr,
			bulkErr:     bulkErr,
			queryInst:   instB,
			wantGetErr:  globalGetErr,
			wantBulkErr: bulkErr,
		},
		{
			name:          "instance_errors_override_global_errors",
			instResizeErr: hostErr,
			globResizeErr: globalResizeErr,
			instGetErr:    instGetErr,
			globGetErr:    globalGetErr,
			queryInst:     instA,
			wantResizeErr: hostErr,
			wantGetErr:    instGetErr,
		},
		{
			name:               "clearing_instance_errors_falls_back_to_global_errors",
			instResizeErr:      hostErr,
			clearInstResizeErr: true,
			globResizeErr:      globalResizeErr,
			instGetErr:         instGetErr,
			clearInstGetErr:    true,
			globGetErr:         globalGetErr,
			queryInst:          instA,
			wantResizeErr:      globalResizeErr,
			wantGetErr:         globalGetErr,
		},
		{
			name:               "clearing_instance_errors_without_global_error_succeeds",
			instResizeErr:      hostErr,
			clearInstResizeErr: true,
			instGetErr:         instGetErr,
			clearInstGetErr:    true,
			queryInst:          instA,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient()
			client.SetVMState(tc.queryInst, initialState)
			client.SetInstanceResizeError(instA, tc.instResizeErr)
			if tc.clearInstResizeErr {
				client.SetInstanceResizeError(instA, nil)
			}
			client.SetResizeError(tc.globResizeErr)
			client.SetInstanceGetError(instA, tc.instGetErr)
			if tc.clearInstGetErr {
				client.SetInstanceGetError(instA, nil)
			}
			client.SetGetError(tc.globGetErr)
			client.SetBulkFetchError(tc.bulkErr)

			resizeErr := client.ResizeVm(t.Context(), tc.queryInst, 8000, 32*ekvmsize.GiBToKiB)
			if !errors.Is(resizeErr, tc.wantResizeErr) {
				t.Errorf("ResizeVm(%v) error = %v, want %v", tc.queryInst, resizeErr, tc.wantResizeErr)
			}

			wantState := resizedState
			if tc.wantResizeErr != nil {
				wantState = initialState
			}
			gotState, ok := client.VMState(tc.queryInst)
			if !ok {
				t.Fatalf("VMState(%v) = (_, false), want (_, true)", tc.queryInst)
			}
			if diff := cmp.Diff(wantState, gotState); diff != "" {
				t.Errorf("VMState(%v) after ResizeVm diff (-want +got):\n%s", tc.queryInst, diff)
			}

			_, getErr := client.GetCurrentResizableVmState(mcp, tc.queryInst)
			if !errors.Is(getErr, tc.wantGetErr) {
				t.Errorf("GetCurrentResizableVmState(%v) error = %v, want %v", tc.queryInst, getErr, tc.wantGetErr)
			}

			_, gotBulkErr := client.BulkFetchCurrentResizableVmStates(mcp, "p", "c")
			if !errors.Is(gotBulkErr, tc.wantBulkErr) {
				t.Errorf("BulkFetchCurrentResizableVmStates() error = %v, want %v", gotBulkErr, tc.wantBulkErr)
			}

			bulkCalls, getCalls, resizeCalls := client.CallCounts()
			if bulkCalls != 1 || getCalls != 1 || resizeCalls != 1 {
				t.Errorf("CallCounts() = (%d, %d, %d), want (1, 1, 1)", bulkCalls, getCalls, resizeCalls)
			}
		})
	}
}
