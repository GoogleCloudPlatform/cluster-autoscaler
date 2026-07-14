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

package state

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// NewNodeStateManagerFromInformer returns a new instance of
// a node state manager that is backed by an informer.
func NewNodeStateManagerFromInformer(
	informerFactory informers.SharedInformerFactory,
	opts ...Option,
) *NodeStateManager {
	register := func(handler NodeHandler) error {
		nodeInformer := informerFactory.Core().V1().Nodes()
		_, err := nodeInformer.Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					if node, ok := obj.(*v1.Node); ok {
						handler.OnAdd(node)
					}
				},
				UpdateFunc: func(_, newObj interface{}) {
					if node, ok := newObj.(*v1.Node); ok {
						handler.OnUpdate(node)
					}
				},
				DeleteFunc: func(obj interface{}) {
					if node, ok := obj.(*v1.Node); ok {
						handler.OnDelete(node)
					}
				},
			})
		return err
	}
	return NewNodeStateManager(register, opts...)
}
