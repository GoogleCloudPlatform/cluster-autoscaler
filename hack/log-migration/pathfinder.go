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

package main

// Pathfinder encapsulates the logic for traversing a pre-computed graph
// to discover all nodes that lie on paths between entrypoints and targets.
type Pathfinder struct {
	forwardEdges  map[string][]string
	backwardEdges map[string][]string
	sources       map[string]bool
	targets       map[string]bool
}

func NewPathfinder() *Pathfinder {
	return &Pathfinder{
		forwardEdges:  make(map[string][]string),
		backwardEdges: make(map[string][]string),
		sources:       make(map[string]bool),
		targets:       make(map[string]bool),
	}
}

func (pf *Pathfinder) AddEdge(fID, tID string) {
	if fID != "" && tID != "" {
		pf.forwardEdges[fID] = append(pf.forwardEdges[fID], tID)
		pf.backwardEdges[tID] = append(pf.backwardEdges[tID], fID)
	}
}

func (pf *Pathfinder) AddSource(fID string) {
	if fID != "" {
		pf.sources[fID] = true
	}
}

func (pf *Pathfinder) AddTarget(fID string) {
	if fID != "" {
		pf.targets[fID] = true
	}
}

func (pf *Pathfinder) OnPaths() map[string]bool {
	P := make(map[string]bool)
	backward := make(map[string]bool)

	var dfsB func(string)
	dfsB = func(fID string) {
		if backward[fID] {
			return
		}
		backward[fID] = true
		for _, prev := range pf.backwardEdges[fID] {
			dfsB(prev)
		}
	}

	for target := range pf.targets {
		dfsB(target)
	}

	var dfsF func(string)
	dfsF = func(fID string) {
		if P[fID] || !backward[fID] {
			return
		}
		P[fID] = true
		for _, next := range pf.forwardEdges[fID] {
			dfsF(next)
		}
	}
	for ep := range pf.sources {
		dfsF(ep)
	}

	// Remove external targets from the final mutation set
	for target := range pf.targets {
		delete(P, target)
	}

	return P
}
