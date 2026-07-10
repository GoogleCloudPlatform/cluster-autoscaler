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

package nodesnowflake

import (
	"context"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Watcher monitors node-pools that have been marked as snowflake.
type Watcher interface {
	NoScaleUpNodePools() sets.Set[string]
	NoScaleDownNodePools() sets.Set[string]
	Run(ctx context.Context)
}

type noOpWatcher struct{}

func (*noOpWatcher) NoScaleUpNodePools() sets.Set[string] {
	return sets.New[string]()
}

func (*noOpWatcher) NoScaleDownNodePools() sets.Set[string] {
	return sets.New[string]()
}

func (*noOpWatcher) Run(ctx context.Context) {}

func NewNoOpWatcher() Watcher {
	return &noOpWatcher{}
}
