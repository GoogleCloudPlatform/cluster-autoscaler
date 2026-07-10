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

package node

import apiv1 "k8s.io/api/core/v1"

// Option is a functional option for mutating nodes
type Option func(*apiv1.Node)

// ApplyOptions applies the given options to the node
func ApplyOptions(n *apiv1.Node, opts ...Option) *apiv1.Node {
	for _, opt := range opts {
		opt(n)
	}
	return n
}

func WithAnnotation(key, value string) Option {
	return func(n *apiv1.Node) {
		if n.Annotations == nil {
			n.Annotations = make(map[string]string)
		}
		n.Annotations[key] = value
	}
}

func WithLabel(key, value string) Option {
	return func(n *apiv1.Node) {
		if n.Labels == nil {
			n.Labels = make(map[string]string)
		}
		n.Labels[key] = value
	}
}
