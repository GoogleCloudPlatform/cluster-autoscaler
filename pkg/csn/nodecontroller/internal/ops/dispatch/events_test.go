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

package dispatch

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/utils/set"
)

type recordedEvent struct {
	buffer    *v1beta1.CapacityBuffer
	eventType string
	reason    string
	message   string
}

type fakeEventRecorder struct {
	kube_record.FakeRecorder
	events []recordedEvent
}

func (r *fakeEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	buf, _ := object.(*v1beta1.CapacityBuffer)
	r.events = append(r.events, recordedEvent{
		buffer:    buf,
		eventType: eventtype,
		reason:    reason,
		message:   message,
	})
}

func TestEventEmitter_EmitSuccess(t *testing.T) {
	buf1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "buf-1"},
	}
	buf2 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "buf-2"},
	}
	buf1OtherNS := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "other-ns", Name: "buf-1"},
	}

	tests := []struct {
		name             string
		opType           ops.OperationType
		nodeNames        set.Set[string]
		nodeNameToBuffer map[string]*v1beta1.CapacityBuffer
		expectedEvents   []recordedEvent
	}{
		{
			name:      "suspend_groups_nodes_by_buffer_and_skips_unassigned",
			opType:    ops.SuspendOp,
			nodeNames: set.New("node-1", "node-2", "node-3", "node-4", "node-unassigned"),
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
				"node-1": buf1,
				"node-2": buf1,
				"node-3": buf2,
				"node-4": buf1OtherNS,
			},
			expectedEvents: []recordedEvent{
				{
					buffer:    buf1,
					eventType: v1.EventTypeNormal,
					reason:    StandbyBufferNodesSuspended,
					message:   formatSuccessMessage(ops.SuspendOp, []string{"node-1", "node-2"}),
				},
				{
					buffer:    buf2,
					eventType: v1.EventTypeNormal,
					reason:    StandbyBufferNodesSuspended,
					message:   formatSuccessMessage(ops.SuspendOp, []string{"node-3"}),
				},
				{
					buffer:    buf1OtherNS,
					eventType: v1.EventTypeNormal,
					reason:    StandbyBufferNodesSuspended,
					message:   formatSuccessMessage(ops.SuspendOp, []string{"node-4"}),
				},
			},
		},
		{
			name:      "consume_multiple_nodes_in_single_buffer",
			opType:    ops.ConsumeOp,
			nodeNames: set.New("node-1", "node-2", "node-3"),
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
				"node-1": buf1,
				"node-2": buf1,
				"node-3": buf1,
			},
			expectedEvents: []recordedEvent{
				{
					buffer:    buf1,
					eventType: v1.EventTypeNormal,
					reason:    StandbyBufferNodesConsumed,
					message:   formatSuccessMessage(ops.ConsumeOp, []string{"node-1", "node-2", "node-3"}),
				},
			},
		},
		{
			name:      "unsupported_op_does_not_emit",
			opType:    ops.AssignBufferOp,
			nodeNames: set.New("node-1"),
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
				"node-1": buf1,
			},
			expectedEvents: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &fakeEventRecorder{}
			sm := &statetest.MockStateManager{
				NodeNameToBuffer: tc.nodeNameToBuffer,
			}
			em := experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					experiments.ColdStandbyNodesEmitNodeControllerEventsFlag: true,
				},
				nil,
			)
			emitter := NewEventEmitter(recorder, sm, em)

			emitter.emitSuccess(tc.opType, tc.nodeNames)

			assert.ElementsMatch(t, tc.expectedEvents, recorder.events)
		})
	}
}

func TestEventEmitter_EmitFailure(t *testing.T) {
	buf1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "buf-1"},
	}
	buf2 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "buf-2"},
	}
	buf1OtherNS := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "other-ns", Name: "buf-1"},
	}

	quotaErr := ops.NewErrorWithCode(gce.ErrorCodeQuotaExceeded, errors.New("quota exceeded"))
	stockoutErr := ops.NewErrorWithCode(gce.ErrorCodeResourcePoolExhausted, errors.New("stockout"))
	internalErr := errors.New("internal error")

	tests := []struct {
		name             string
		opType           ops.OperationType
		failedNodes      set.Set[string]
		errs             map[string]error
		nodeNameToBuffer map[string]*v1beta1.CapacityBuffer
		expectedEvents   []recordedEvent
	}{
		{
			name:        "suspend_groups_nodes_by_error_code_and_ignores_nil_error",
			opType:      ops.SuspendOp,
			failedNodes: set.New("node-1", "node-2", "node-3", "node-4"),
			errs: map[string]error{
				"node-1": quotaErr,
				"node-2": quotaErr,
				"node-3": internalErr,
				"node-4": nil,
			},
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
				"node-1": buf1,
				"node-2": buf1,
				"node-3": buf1,
				"node-4": buf1,
			},
			expectedEvents: []recordedEvent{
				{
					buffer:    buf1,
					eventType: v1.EventTypeWarning,
					reason:    StandbyBufferSuspendFailed,
					message: formatFailureMessage(ops.SuspendOp, []string{"node-1", "node-2", "node-3", "node-4"}, map[string]error{
						"node-1": quotaErr,
						"node-2": quotaErr,
						"node-3": internalErr,
					}),
				},
			},
		},
		{
			name:        "consume_groups_nodes_by_buffer_and_skips_unassigned",
			opType:      ops.ConsumeOp,
			failedNodes: set.New("node-1", "node-2", "node-3", "node-4", "node-unassigned"),
			errs: map[string]error{
				"node-1":          quotaErr,
				"node-2":          quotaErr,
				"node-3":          stockoutErr,
				"node-4":          internalErr,
				"node-unassigned": ops.NewErrorWithCode(gce.ErrorIPSpaceExhausted, errors.New("ip exhausted")),
				"other-node":      ops.NewErrorWithCode(gce.ErrorCodePermissions, errors.New("not in failedNodes")),
			},
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
				"node-1": buf1,
				"node-2": buf1,
				"node-3": buf2,
				"node-4": buf1OtherNS,
			},
			expectedEvents: []recordedEvent{
				{
					buffer:    buf1,
					eventType: v1.EventTypeWarning,
					reason:    StandbyBufferConsumeFailed,
					message: formatFailureMessage(ops.ConsumeOp, []string{"node-1", "node-2"}, map[string]error{
						"node-1": quotaErr,
						"node-2": quotaErr,
					}),
				},
				{
					buffer:    buf2,
					eventType: v1.EventTypeWarning,
					reason:    StandbyBufferConsumeFailed,
					message: formatFailureMessage(ops.ConsumeOp, []string{"node-3"}, map[string]error{
						"node-3": stockoutErr,
					}),
				},
				{
					buffer:    buf1OtherNS,
					eventType: v1.EventTypeWarning,
					reason:    StandbyBufferConsumeFailed,
					message: formatFailureMessage(ops.ConsumeOp, []string{"node-4"}, map[string]error{
						"node-4": internalErr,
					}),
				},
			},
		},
		{
			name:        "all_nil_errors_does_not_emit",
			opType:      ops.SuspendOp,
			failedNodes: set.New("node-1"),
			errs:        map[string]error{"node-1": nil},
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
				"node-1": buf1,
			},
			expectedEvents: nil,
		},
		{
			name:        "unsupported_op_does_not_emit",
			opType:      ops.AssignBufferOp,
			failedNodes: set.New("node-1"),
			errs: map[string]error{
				"node-1": errors.New("failed to assign buffer"),
			},
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
				"node-1": buf1,
			},
			expectedEvents: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &fakeEventRecorder{}
			sm := &statetest.MockStateManager{
				NodeNameToBuffer: tc.nodeNameToBuffer,
			}
			em := experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					experiments.ColdStandbyNodesEmitNodeControllerEventsFlag: true,
				},
				nil,
			)
			emitter := NewEventEmitter(recorder, sm, em)

			emitter.emitFailure(tc.opType, tc.failedNodes, tc.errs)

			assert.ElementsMatch(t, tc.expectedEvents, recorder.events)
		})
	}
}

func TestEventEmitter_DisabledGuards(t *testing.T) {
	buf1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "buf-1"},
	}

	tests := []struct {
		name               string
		nodeNames          set.Set[string]
		nodeNameToBuffer   map[string]*v1beta1.CapacityBuffer
		nilEmitter         bool
		nilRecorder        bool
		nilStateManager    bool
		nilExperimentsMgr  bool
		experimentDisabled bool
	}{
		{
			name:       "nil_emitter",
			nodeNames:  set.New("node-1"),
			nilEmitter: true,
		},
		{
			name:             "empty_nodes",
			nodeNames:        set.New[string](),
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{"node-1": buf1},
		},
		{
			name:             "nil_recorder",
			nodeNames:        set.New("node-1"),
			nodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{"node-1": buf1},
			nilRecorder:      true,
		},
		{
			name:            "nil_state_manager",
			nodeNames:       set.New("node-1"),
			nilStateManager: true,
		},
		{
			name:              "nil_experiments_manager",
			nodeNames:         set.New("node-1"),
			nodeNameToBuffer:  map[string]*v1beta1.CapacityBuffer{"node-1": buf1},
			nilExperimentsMgr: true,
		},
		{
			name:               "experiment_disabled",
			nodeNames:          set.New("node-1"),
			nodeNameToBuffer:   map[string]*v1beta1.CapacityBuffer{"node-1": buf1},
			experimentDisabled: true,
		},
		{
			name:             "unassigned_nodes",
			nodeNames:        set.New("node-1"),
			nodeNameToBuffer: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &fakeEventRecorder{}
			var emitter *EventEmitter
			if !tc.nilEmitter {
				var rec kube_record.EventRecorder = recorder
				if tc.nilRecorder {
					rec = nil
				}
				var sm StateManager = &statetest.MockStateManager{
					NodeNameToBuffer: tc.nodeNameToBuffer,
				}
				if tc.nilStateManager {
					sm = nil
				}
				var em experiments.Manager
				if !tc.nilExperimentsMgr {
					em = experiments.NewMockManagerWithOptions(
						version.Version{},
						map[string]bool{
							experiments.ColdStandbyNodesEmitNodeControllerEventsFlag: !tc.experimentDisabled,
						},
						nil,
					)
				}
				emitter = NewEventEmitter(rec, sm, em)
			}

			emitter.emitSuccess(ops.ConsumeOp, tc.nodeNames)
			emitter.emitFailure(ops.ConsumeOp, tc.nodeNames, map[string]error{"node-1": errors.New("err")})

			assert.Empty(t, recorder.events)
		})
	}
}

func TestFormatEventMessages(t *testing.T) {
	quotaErr := ops.NewErrorWithCode(gce.ErrorCodeQuotaExceeded, errors.New("quota exceeded"))
	internalErr := errors.New("internal error")

	tests := []struct {
		name        string
		opType      ops.OperationType
		nodes       []string
		errs        map[string]error
		expectedMsg string
	}{
		{
			name:        "suspend_success_single_node",
			opType:      ops.SuspendOp,
			nodes:       []string{"node-1"},
			expectedMsg: "Successfully suspended 1 standby node (node-1).",
		},
		{
			name:        "consume_success_up_to_5_nodes_does_not_truncate",
			opType:      ops.ConsumeOp,
			nodes:       []string{"node-1", "node-2", "node-3", "node-4", "node-5"},
			expectedMsg: "Successfully consumed 5 standby nodes (node-1, node-2, node-3, node-4, node-5).",
		},
		{
			name:        "consume_success_more_than_5_nodes_truncates",
			opType:      ops.ConsumeOp,
			nodes:       []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6", "node-7"},
			expectedMsg: "Successfully consumed 7 standby nodes (node-1, node-2, node-3, node-4, node-5, and 2 more).",
		},
		{
			name:        "unsupported_op_success_returns_empty",
			opType:      ops.AssignBufferOp,
			nodes:       []string{"node-1"},
			expectedMsg: "",
		},
		{
			name:   "suspend_failure_multiple_error_codes_and_truncation",
			opType: ops.SuspendOp,
			nodes:  []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6", "node-7"},
			errs: map[string]error{
				"node-1": quotaErr,
				"node-2": quotaErr,
				"node-3": quotaErr,
				"node-4": quotaErr,
				"node-5": quotaErr,
				"node-6": quotaErr,
				"node-7": internalErr,
			},
			expectedMsg: "Failed to suspend 7 standby nodes: GKE_INTERNAL_ERROR (node-7), QUOTA_EXCEEDED (node-1, node-2, node-3, node-4, node-5, and 1 more).",
		},
		{
			name:   "consume_failure_single_node",
			opType: ops.ConsumeOp,
			nodes:  []string{"node-1"},
			errs: map[string]error{
				"node-1": quotaErr,
			},
			expectedMsg: "Failed to consume 1 standby node: QUOTA_EXCEEDED (node-1).",
		},
		{
			name:   "unsupported_op_failure_returns_empty",
			opType: ops.AssignBufferOp,
			nodes:  []string{"node-1"},
			errs: map[string]error{
				"node-1": quotaErr,
			},
			expectedMsg: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var actual string
			if tc.errs == nil {
				actual = formatSuccessMessage(tc.opType, tc.nodes)
			} else {
				actual = formatFailureMessage(tc.opType, tc.nodes, tc.errs)
			}
			assert.Equal(t, tc.expectedMsg, actual)
		})
	}
}
