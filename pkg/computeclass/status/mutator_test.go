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

package status

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrySendUpdate(t *testing.T) {
	msg := UpdateMessage{
		Id: CRDId{CRDName: "test-cc", CRDLabel: "test-label"},
	}

	t.Run("nil channel returns false", func(t *testing.T) {
		assert.False(t, TrySendUpdate(nil, msg))
	})

	t.Run("send succeeds on buffered channel with space", func(t *testing.T) {
		ch := make(chan UpdateMessage, 1)
		assert.True(t, TrySendUpdate(ch, msg))
		assert.Len(t, ch, 1)
	})

	t.Run("send drops and returns false when channel is full", func(t *testing.T) {
		ch := make(chan UpdateMessage, 1)
		ch <- msg // fill channel

		assert.False(t, TrySendUpdate(ch, msg))
		assert.Len(t, ch, 1)
	})
}

func TestTrySendRuleUpdate(t *testing.T) {
	msg := UpdateMessage{
		Id: CRDId{CRDName: "test-cc", CRDLabel: "test-label"},
	}

	t.Run("nil channel returns false", func(t *testing.T) {
		assert.False(t, TrySendRuleUpdate(nil, msg, "0"))
	})

	t.Run("send succeeds on buffered channel with space", func(t *testing.T) {
		ch := make(chan UpdateMessage, 1)
		assert.True(t, TrySendRuleUpdate(ch, msg, "0"))
		assert.Len(t, ch, 1)
	})

	t.Run("send drops and returns false when channel is full", func(t *testing.T) {
		ch := make(chan UpdateMessage, 1)
		ch <- msg // fill channel

		assert.False(t, TrySendRuleUpdate(ch, msg, "0"))
		assert.Len(t, ch, 1)
	})
}
