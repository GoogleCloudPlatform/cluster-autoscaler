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

package scaledownpriority

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
)

const testTaintKey = "cloud.google.com/gke-scale-down-priority"

func taintedNode(name string, taints ...apiv1.Taint) *apiv1.Node {
	return &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       apiv1.NodeSpec{Taints: taints},
	}
}

func priorityTaint(value string) apiv1.Taint {
	return apiv1.Taint{Key: testTaintKey, Value: value, Effect: apiv1.TaintEffectNoSchedule}
}

func priorityTaintWithEffect(value string, effect apiv1.TaintEffect) apiv1.Taint {
	return apiv1.Taint{Key: testTaintKey, Value: value, Effect: effect}
}

func TestTaintParserEnabled(t *testing.T) {
	testCases := []struct {
		name   string
		parser *TaintParser
		want   bool
	}{
		{name: "nil parser", parser: nil, want: false},
		{name: "empty taint key", parser: NewTaintParser("", nil), want: false},
		{name: "non-empty taint key", parser: NewTaintParser(testTaintKey, nil), want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.parser.Enabled())
		})
	}
}

func TestNodePriority(t *testing.T) {
	parser := NewTaintParser(testTaintKey, nil)
	testCases := []struct {
		name   string
		parser *TaintParser
		node   *apiv1.Node
		want   int
	}{
		{name: "nil node", parser: parser, node: nil, want: 0},
		{name: "no taint", parser: parser, node: taintedNode("n"), want: 0},
		{name: "empty value", parser: parser, node: taintedNode("n", priorityTaint("")), want: 0},
		{name: "true", parser: parser, node: taintedNode("n", priorityTaint("true")), want: 0},
		{name: "positive integer", parser: parser, node: taintedNode("n", priorityTaint("5")), want: 5},
		{name: "max int", parser: parser, node: taintedNode("n", priorityTaint(strconv.Itoa(math.MaxInt))), want: math.MaxInt},
		{name: "above max int", parser: parser, node: taintedNode("n", priorityTaint("99999999999999999999")), want: 0},
		{name: "decimal", parser: parser, node: taintedNode("n", priorityTaint("5.0")), want: 0},
		{name: "false", parser: parser, node: taintedNode("n", priorityTaint("false")), want: 0},
		{name: "zero", parser: parser, node: taintedNode("n", priorityTaint("0")), want: 0},
		{name: "negative", parser: parser, node: taintedNode("n", priorityTaint("-1")), want: 0},
		{name: "non-numeric", parser: parser, node: taintedNode("n", priorityTaint("abc")), want: 0},
		{name: "other key ignored", parser: parser, node: taintedNode("n", apiv1.Taint{Key: "other", Value: "9", Effect: apiv1.TaintEffectNoSchedule}), want: 0},
		{name: "PreferNoSchedule effect", parser: parser, node: taintedNode("n", priorityTaintWithEffect("3", apiv1.TaintEffectPreferNoSchedule)), want: 3},
		{name: "NoExecute effect", parser: parser, node: taintedNode("n", priorityTaintWithEffect("3", apiv1.TaintEffectNoExecute)), want: 3},
		{name: "same key under two effects uses max", parser: parser, node: taintedNode("n", priorityTaintWithEffect("2", apiv1.TaintEffectPreferNoSchedule), priorityTaint("8")), want: 8},
		{name: "valid value under one effect, invalid under another", parser: parser, node: taintedNode("n", priorityTaintWithEffect("high", apiv1.TaintEffectPreferNoSchedule), priorityTaint("4")), want: 4},
		{name: "disabled parser", parser: NewTaintParser("", nil), node: taintedNode("n", priorityTaint("4")), want: 0},
		{name: "nil parser", parser: nil, node: taintedNode("n", priorityTaint("4")), want: 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.parser.NodePriority(tc.node))
		})
	}
}

func TestNodePriorityInvalidEvents(t *testing.T) {
	testCases := []struct {
		name           string
		nodes          []*apiv1.Node // fed to NodePriority in order, as if across loops
		wantPriorities []int
		wantEvents     []string // one substring per expected event, in order
	}{
		{
			name: "same invalid value over several loops emits one event",
			nodes: []*apiv1.Node{
				taintedNode("bad", priorityTaint("high")),
				taintedNode("bad", priorityTaint("high")),
				taintedNode("bad", priorityTaint("high")),
			},
			wantPriorities: []int{0, 0, 0},
			wantEvents:     []string{`"high"`},
		},
		{
			name: "different invalid value on the same node emits a new event",
			nodes: []*apiv1.Node{
				taintedNode("bad", priorityTaint("high")),
				taintedNode("bad", priorityTaint("-2")),
			},
			wantPriorities: []int{0, 0},
			wantEvents:     []string{`"high"`, `"-2"`},
		},
		{
			name: "same invalid value on different nodes emits an event per node",
			nodes: []*apiv1.Node{
				taintedNode("bad-1", priorityTaint("high")),
				taintedNode("bad-2", priorityTaint("high")),
			},
			wantPriorities: []int{0, 0},
			wantEvents:     []string{`"high"`, `"high"`},
		},
		{
			name:           "valid value emits no event",
			nodes:          []*apiv1.Node{taintedNode("good", priorityTaint("3"))},
			wantPriorities: []int{3},
		},
		{
			name: "invalid then valid value applies the priority without another event",
			nodes: []*apiv1.Node{
				taintedNode("bad", priorityTaint("high")),
				taintedNode("bad", priorityTaint("7")),
			},
			wantPriorities: []int{0, 7},
			wantEvents:     []string{`"high"`},
		},
		{
			name: "invalid, then valid, then the same invalid value again emits a second event",
			nodes: []*apiv1.Node{
				taintedNode("bad", priorityTaint("high")),
				taintedNode("bad", priorityTaint("7")),
				taintedNode("bad", priorityTaint("high")),
			},
			wantPriorities: []int{0, 7, 0},
			wantEvents:     []string{`"high"`, `"high"`},
		},
		{
			name: "valid value under another effect suppresses the event",
			nodes: []*apiv1.Node{
				taintedNode("mixed", priorityTaintWithEffect("invalid", apiv1.TaintEffectPreferNoSchedule), priorityTaint("5")),
			},
			wantPriorities: []int{5},
		},
		{
			name: "two invalid values under different effects emit one event listing both",
			nodes: []*apiv1.Node{
				taintedNode("bad", priorityTaintWithEffect("high", apiv1.TaintEffectNoSchedule), priorityTaintWithEffect("low", apiv1.TaintEffectPreferNoSchedule)),
				taintedNode("bad", priorityTaintWithEffect("high", apiv1.TaintEffectNoSchedule), priorityTaintWithEffect("low", apiv1.TaintEffectPreferNoSchedule)),
			},
			wantPriorities: []int{0, 0},
			wantEvents:     []string{`"high" "low"`},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := record.NewFakeRecorder(10)
			parser := NewTaintParser(testTaintKey, recorder)

			var gotPriorities []int
			for _, node := range tc.nodes {
				gotPriorities = append(gotPriorities, parser.NodePriority(node))
			}

			assert.Equal(t, tc.wantPriorities, gotPriorities)
			gotEvents := drainEvents(recorder)
			if len(gotEvents) != len(tc.wantEvents) {
				t.Fatalf("NodePriority() emitted events %q, want %d events containing %q", gotEvents, len(tc.wantEvents), tc.wantEvents)
			}
			for i, want := range tc.wantEvents {
				assert.Contains(t, gotEvents[i], apiv1.EventTypeWarning)
				assert.Contains(t, gotEvents[i], InvalidPriorityEventReason)
				assert.Contains(t, gotEvents[i], want)
			}
		})
	}
}

func TestNodePriorityInvalidWithoutRecorder(t *testing.T) {
	parser := NewTaintParser(testTaintKey, nil)

	got := parser.NodePriority(taintedNode("bad", priorityTaint("high")))

	assert.Equal(t, 0, got, "NodePriority() of a node with an invalid value")
}

// drainEvents returns the events recorded so far, without waiting for more.
func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

func TestReportsExpire(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())
	recorder := record.NewFakeRecorder(10)
	parser := newTaintParser(testTaintKey, recorder, clk)
	gone := taintedNode("gone", priorityTaint("high"))
	kept := taintedNode("kept", priorityTaint("high"))

	parser.NodePriority(gone)
	parser.NodePriority(kept)
	assert.Len(t, parser.reported, 2)
	if got := drainEvents(recorder); len(got) != 2 {
		t.Fatalf("NodePriority() emitted events %q, want 2 events", got)
	}

	// "kept" is still seen within the TTL, "gone" is not.
	clk.SetTime(clk.Now().Add(reportTTL / 2))
	parser.NodePriority(kept)
	clk.SetTime(clk.Now().Add(reportTTL/2 + time.Second))
	parser.NodePriority(kept) // triggers the sweep
	assert.Len(t, parser.reported, 1)
	assert.Contains(t, parser.reported, "kept")
	assert.Empty(t, recorder.Events, "a node seen continuously is not reported again")

	// A node that comes back with the same invalid value after expiry is
	// reported again.
	parser.NodePriority(gone)
	assert.Len(t, recorder.Events, 1)
}
