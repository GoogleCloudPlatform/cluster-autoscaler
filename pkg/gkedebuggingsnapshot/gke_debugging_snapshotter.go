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

package gkedebuggingsnapshot

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/debuggingsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	kubernetes2 "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	"k8s.io/klog/v2"
)

// GkeDebuggingSnapshotter is the impl of GkeDebuggingSnapshotter
// It uses a reference to debuggingsnapshot.DebuggingSnapshotImpl and uses
// the states in the default snapshot
type GkeDebuggingSnapshotter struct {
	*debuggingsnapshot.DebuggingSnapshotterImpl
	updateInfoFetcher    kubernetes2.UpdateInfoFetcher
	gkeDebuggingSnapshot *GkeDebuggingSnapshot
}

// CacheTemplateNodeLastUsedByNAP caches the last used set of Template Nodes by NAP for scale up
func (g *GkeDebuggingSnapshotter) CacheTemplateNodeLastUsedByNAP(templates map[string]*framework.NodeInfo) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()
	if g.isSnapshotterDisabledNoLock() {
		return
	}

	g.gkeDebuggingSnapshot.CacheTemplateNodeLastUsedByNAP(templates)
}

// isSnapshotterDisabledNoLock checked if the snapshotter is not in disabled state
// This means it will be able to use captured data at some point
func (g *GkeDebuggingSnapshotter) isSnapshotterDisabledNoLock() bool {
	return *g.DebuggingSnapshotterImpl.State == debuggingsnapshot.SNAPSHOTTER_DISABLED
}

// IsSnapshotterDisabled checked if the snapshotter is not in disabled state
// This means it will be able to use captured data at some point
func (g *GkeDebuggingSnapshotter) IsSnapshotterDisabled() bool {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()
	return g.isSnapshotterDisabledNoLock()
}

// SetUpdateInfoFetcher is the impl to capture UpdateInfoFetcher
func (g *GkeDebuggingSnapshotter) SetUpdateInfoFetcher(fetcher kubernetes2.UpdateInfoFetcher) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()
	if g.isSnapshotterDisabledNoLock() {
		return
	}

	g.updateInfoFetcher = fetcher
}

// SetCapacityRequest is the setter func for CapacityRequest
func (g *GkeDebuggingSnapshotter) SetCapacityRequest(list []*v1.CapacityRequest) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()
	if !g.IsDataCollectionAllowedNoLock() {
		return
	}

	g.gkeDebuggingSnapshot.SetCapacityRequest(list)
	*g.State = debuggingsnapshot.DATA_COLLECTED
}

// GenerateUpdateInfo is used to set the UpdateInfo from the fetcher, since
// the fetcher is only captured at container start
func (g *GkeDebuggingSnapshotter) GenerateUpdateInfo() {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()
	if !g.IsDataCollectionAllowedNoLock() {
		return
	}

	if g.updateInfoFetcher == nil {
		// state is not changed, since no attempt could be made to collect data here
		klog.Error("UpdateInfo fetcher should not be nil. Unable to fetch UpdateInfos for debugging snapshot")
		return
	}

	updateInfos, err := g.updateInfoFetcher.GetUpdateInfos()
	if err != nil {
		klog.Errorf("Unable to fetch UpdateInfos for Debugging snapshot, %v", err)
		return
	}

	g.gkeDebuggingSnapshot.SetUpdateInfo(updateInfos)
	*g.State = debuggingsnapshot.DATA_COLLECTED
}

// NewGkeDebuggingSnapshotter returns a new instance of GkeDebuggingSnapshotter
func NewGkeDebuggingSnapshotter(isDebuggerEnabled bool) (*GkeDebuggingSnapshotter, error) {

	debuggingSnapshotter := debuggingsnapshot.NewDebuggingSnapshotter(isDebuggerEnabled)
	debuggingSnapshotterImpl, ok := debuggingSnapshotter.(*debuggingsnapshot.DebuggingSnapshotterImpl)
	if !ok {
		klog.Error("OSS snapshot is not of type DebuggingSnapshotterImpl, unable to start GkeDebuggingSnapshotter")
		return nil, fmt.Errorf("DebuggingSnapshotter not of Type DebuggingSnapshotterImpl")
	}

	// pointing the gke snapshot to oss snapshotter to use gke overridden common methods
	gkeDebuggingSnapshot := &GkeDebuggingSnapshot{
		DebuggingSnapshotImpl: debuggingSnapshotterImpl.DebuggingSnapshot.(*debuggingsnapshot.DebuggingSnapshotImpl),
	}
	debuggingSnapshotterImpl.DebuggingSnapshot = gkeDebuggingSnapshot

	return &GkeDebuggingSnapshotter{
		DebuggingSnapshotterImpl: debuggingSnapshotterImpl,
		gkeDebuggingSnapshot:     gkeDebuggingSnapshot,
	}, nil
}
