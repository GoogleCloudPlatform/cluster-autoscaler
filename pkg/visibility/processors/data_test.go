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

package processors

import (
	"testing"

	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"

	"github.com/stretchr/testify/assert"
)

func TestEventNormalFlow(t *testing.T) {
	data := NewSharedData()
	data.StartEvent("e1", map[string]bool{"ng1": true, "ng2": true}, nil)
	data.StartEvent("e2", map[string]bool{"ng1": true, "ng3": true}, nil)
	data.StartEvent("e3", map[string]bool{"ng1": true}, nil)

	assert.Empty(t, data.GetNextResults())
	assert.Equal(t, map[string]bool{"ng1": true, "ng2": true, "ng3": true}, data.GetUnfinishedNodeGroupIds())

	data.FinishNodeGroup("ng3")
	assert.Empty(t, data.GetNextResults())
	assert.Equal(t, map[string]bool{"ng1": true, "ng2": true}, data.GetUnfinishedNodeGroupIds())

	data.FinishNodeGroup("ng1")
	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e2"},
		{EventId: "e3"},
	}, data.GetNextResults())
	assert.Equal(t, map[string]bool{"ng2": true}, data.GetUnfinishedNodeGroupIds())

	data.FinishNodeGroup("ng2")
	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e1"},
	}, data.GetNextResults())
	assert.Empty(t, data.GetUnfinishedNodeGroupIds())

	assert.Empty(t, data.GetNextResults())

	assert.NotEmpty(t, data.currentEvents)
	data.PeriodicCleanup()
	assert.Empty(t, data.currentEvents)
}

func TestEventErrorFlow(t *testing.T) {
	data := NewSharedData()
	data.StartEvent("e1", map[string]bool{"ng1": true, "ng2": true}, nil)
	data.StartEvent("e2", map[string]bool{"ng1": true, "ng3": true}, nil)
	data.StartEvent("e3", map[string]bool{"ng1": true}, nil)
	data.StartEvent("e4", map[string]bool{"ng1": true, "ng4": true}, nil)
	data.StartEvent("e5", map[string]bool{"ng2": true}, nil)
	data.StartEvent("e6", map[string]bool{"ng5": true}, nil)

	msg1 := &vistypes.Message{Id: 0, Params: []string{"p1"}}
	msg2 := &vistypes.Message{Id: 0, Params: []string{"p2"}}
	msg3 := &vistypes.Message{Id: 0, Params: []string{"p3"}}
	msg4 := &vistypes.Message{Id: 0, Params: []string{"p4"}}

	assert.Empty(t, data.GetNextResults())
	assert.Equal(t, map[string]bool{"ng1": true, "ng2": true, "ng3": true, "ng4": true, "ng5": true}, data.GetUnfinishedNodeGroupIds())
	assert.Equal(t, 6, len(data.currentEvents))

	// Test failing node groups for single events only.
	data.FailNodeGroupForEvent("e4", "ng1", []*vistypes.Message{msg1, msg2})
	data.FailNodeGroupForEvent("e5", "ng2", []*vistypes.Message{msg3})
	data.FailNodeGroupForEvent("e6", "ng5", []*vistypes.Message{msg4})
	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e4", ErrorMsg: msg1.Proto()},
		{EventId: "e4", ErrorMsg: msg2.Proto()},
		{EventId: "e5", ErrorMsg: msg3.Proto()},
		{EventId: "e6", ErrorMsg: msg4.Proto()},
	}, data.GetNextResults())
	data.PeriodicCleanup()
	assert.Equal(t, map[string]bool{"ng1": true, "ng2": true, "ng3": true, "ng4": true}, data.GetUnfinishedNodeGroupIds())
	assert.Equal(t, 4, len(data.currentEvents))

	// Test failing node groups for all events.
	data.FailNodeGroup("ng4", []*vistypes.Message{msg2, msg3})
	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e4", ErrorMsg: msg2.Proto()},
		{EventId: "e4", ErrorMsg: msg3.Proto()},
	}, data.GetNextResults())
	data.PeriodicCleanup()
	assert.Equal(t, map[string]bool{"ng1": true, "ng2": true, "ng3": true}, data.GetUnfinishedNodeGroupIds())
	assert.Equal(t, 3, len(data.currentEvents))

	data.FailNodeGroup("ng1", []*vistypes.Message{msg3, msg4})
	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e1", ErrorMsg: msg3.Proto()},
		{EventId: "e1", ErrorMsg: msg4.Proto()},
		{EventId: "e2", ErrorMsg: msg3.Proto()},
		{EventId: "e2", ErrorMsg: msg4.Proto()},
		{EventId: "e3", ErrorMsg: msg3.Proto()},
		{EventId: "e3", ErrorMsg: msg4.Proto()},
	}, data.GetNextResults())
	data.PeriodicCleanup()
	assert.Equal(t, map[string]bool{"ng2": true, "ng3": true}, data.GetUnfinishedNodeGroupIds())
	assert.Equal(t, 2, len(data.currentEvents))

	data.FailNodeGroup("ng2", []*vistypes.Message{msg1})
	data.FailNodeGroup("ng3", []*vistypes.Message{msg2})
	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e1", ErrorMsg: msg1.Proto()},
		{EventId: "e2", ErrorMsg: msg2.Proto()},
	}, data.GetNextResults())
	data.PeriodicCleanup()
	assert.Empty(t, data.GetUnfinishedNodeGroupIds())
	assert.Empty(t, data.currentEvents)
}

func TestUnstableTargetSizes(t *testing.T) {
	data := NewSharedData()
	data.StartEvent("e1", map[string]bool{"ng1": true}, nil)
	data.StartEvent("e2", map[string]bool{"ng2": true}, nil)
	data.StartEvent("e3", map[string]bool{"ng1": true, "ng2": true}, nil)

	msg1 := &vistypes.Message{Id: 0, Params: []string{"p1"}}

	data.MarkNodeGroupTargetSizeUnstable("ng1")
	data.MarkNodeGroupTargetSizeUnstable("ng2")

	assert.Equal(t, map[string]bool{"ng1": true, "ng2": true}, data.GetUnfinishedNodeGroupIds())

	data.FinishNodeGroup("ng1")
	data.FailNodeGroup("ng2", []*vistypes.Message{msg1})

	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e2", ErrorMsg: msg1.Proto()},
		{EventId: "e3", ErrorMsg: msg1.Proto()},
	}, data.GetNextResults())
	data.PeriodicCleanup()
	assert.Equal(t, map[string]bool{"ng1": true}, data.GetUnfinishedNodeGroupIds())
	assert.Equal(t, 2, len(data.currentEvents))

	data.MarkNodeGroupTargetSizeStable("ng1")
	data.MarkNodeGroupTargetSizeStable("ng2")

	data.FinishNodeGroup("ng1")

	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "e1"},
	}, data.GetNextResults())
	data.PeriodicCleanup()
	assert.Empty(t, data.GetUnfinishedNodeGroupIds())
	assert.Empty(t, data.currentEvents)
}
