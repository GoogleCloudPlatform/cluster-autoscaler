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

package visibility

import (
	"fmt"

	uuid "github.com/satori/go.uuid"
)

// EventIDGenerator is used to assign ids to new visibility events.
type EventIDGenerator interface {
	GenerateID() string
}

// UuidEventIDGenerator assigns UUID4 ids.
type UuidEventIDGenerator struct {
}

// GenerateID returns the newly assigned id.
func (g *UuidEventIDGenerator) GenerateID() string {
	return uuid.NewV4().String()
}

// MockEventIDGenerator deterministically assigns sequential ids. Useful for testing.
type MockEventIDGenerator struct {
	Counter int
}

// GenerateID returns the newly assigned id.
func (g *MockEventIDGenerator) GenerateID() string {
	g.Counter += 1
	return fmt.Sprintf("event_%v", g.Counter-1)
}
