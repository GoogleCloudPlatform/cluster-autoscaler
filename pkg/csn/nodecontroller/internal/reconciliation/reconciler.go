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

package reconciliation

import (
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

const logPrefix = "CSN Reconciler:"

// NodeStateManager allows for retrieving node state.
type NodeStateManager interface {
	List(filters ...state.NodeFilter) []state.TrackedNode
	Get(nodeName string) (state.TrackedNode, bool)
}

// CloudProvider provides MIG information for nodes.
type CloudProvider interface {
	GkeMigForNode(node *v1.Node) (*gke.GkeMig, error)
	InstanceByRef(ref gce.GceRef) *gce.GceInstance
}

// WorkQueue allows for enqueueing operations.
type WorkQueue interface {
	EnqueueWithOpts(o ops.Operation, opts ...state.PendingOperationOpt) error
}

// Config contains numeric Reconciler parameters.
type Config struct {
	// MaxInvalidCount determines the maximum amount of times a node can have
	// an incorrect status before it's consumed. This is used to mitigate the
	// issue of the CloudProvider cache being stale.
	MaxInvalidCount int
}

// Reconciler is responsible for periodically enqueueing Consume operations if
// CSN nodes' observed state differs from their real-world GCE instance state.
// The implementation uses CloudProvider, which should store a cache of all
// GCE instances.
type Reconciler struct {
	stateManager  NodeStateManager
	cloudProvider CloudProvider
	workQueue     WorkQueue
	cfg           Config
	// node name -> invalid state count
	invalidCount map[string]int
	running      atomic.Bool
}

// NewReconciler returns a new instance of Reconciler.
func NewReconciler(
	sm NodeStateManager,
	cp CloudProvider,
	wq WorkQueue,
	cfg Config,
) *Reconciler {
	return &Reconciler{
		stateManager:  sm,
		cloudProvider: cp,
		workQueue:     wq,
		cfg:           cfg,
		invalidCount:  make(map[string]int),
	}
}

// Reconcile will enqueue consumption for any node
// if its state deviates from the one expected by the CSN Node Controller.
// In the future this could be expanded to force the actual state to
// become the expected state.
func (r *Reconciler) Reconcile() {
	if r.running.Swap(true) {
		return
	}
	defer r.running.Store(false)
	trackedNodes := r.stateManager.List(state.WithoutPendingOperationsFilter)
	r.refreshInvalidCounts(trackedNodes)
	if len(trackedNodes) == 0 {
		return
	}

	migToNodeNames := make(map[gce.GceRef]set.Set[string])
	for _, tn := range trackedNodes {
		mig, err := r.cloudProvider.GkeMigForNode(tn.Node)
		if err != nil {
			klog.Errorf("%s failed to get MIG for node %q: %v", logPrefix, tn.Node.Name, err)
			continue
		}
		if mig == nil {
			// nil mig means CA should not manage the node.
			continue
		}
		ref := mig.GceRef()
		if _, ok := migToNodeNames[ref]; !ok {
			migToNodeNames[ref] = set.New[string]()
		}
		migToNodeNames[ref].Insert(tn.Node.Name)
	}

	for mig, nodeNames := range migToNodeNames {
		r.reconcileMig(mig, nodeNames)
	}
}

func (r *Reconciler) reconcileMig(mig gce.GceRef, nodeNames set.Set[string]) {
	nodesToConsume := set.New[string]()
	for nodeName := range nodeNames {
		tn, ok := r.stateManager.Get(nodeName)
		if !ok {
			// no longer in the state manager - let's forget about it
			continue
		}

		// reconcile might mess with pending operations
		if tn.PendingOperations != ops.NoOp {
			continue
		}

		instanceRef, err := gce.GceRefFromProviderId(tn.Node.Spec.ProviderID)
		if err != nil {
			klog.Errorf("%s skipping reconciliation of node %q because "+
				"calculating instance ref failed: %v", logPrefix, tn.Node.Name, err)
		}

		instance := r.cloudProvider.InstanceByRef(instanceRef)
		if instance == nil || instance.GCEStatus == "" {
			klog.Warningf("%s status for instance %q not found in cloud provider "+
				"cache with ref %v", logPrefix, nodeName, instanceRef)
			continue
		}

		if r.isStatusAsExpected(tn, instance.GCEStatus) {
			delete(r.invalidCount, nodeName)
			continue
		}

		r.invalidCount[nodeName] += 1
		if r.invalidCount[nodeName] < r.cfg.MaxInvalidCount {
			continue
		}
		klog.Warningf("%s node %q state (curr: %v, desired: %v) is out of "+
			"sync with instance status %q, enqueueing Consume operation",
			logPrefix, nodeName, tn.State, tn.DesiredState,
			instance.GCEStatus,
		)
		nodesToConsume.Insert(tn.Node.Name)
		delete(r.invalidCount, nodeName)
	}
	if nodesToConsume.Len() == 0 {
		return
	}
	if err := r.workQueue.EnqueueWithOpts(ops.Operation{
		MIG:       mig,
		Type:      ops.ConsumeOp,
		NodeNames: nodesToConsume,
	}, state.ExclusiveOp); err != nil {
		klog.Errorf("%s failed to enqueue Consume operation for nodes %v: %v",
			logPrefix, nodesToConsume.UnsortedList(), err)
	}
}

func (r *Reconciler) isStatusAsExpected(tn state.TrackedNode, instanceStatus string) bool {
	// Instance can be removed in any state.
	if internal.IsStopped(instanceStatus) {
		return true
	}
	nodeState := tn.State
	if tn.DesiredState != "" {
		nodeState = tn.DesiredState
	}
	switch nodeState {
	case csn.NodeStateSuspended:
		return internal.IsSuspended(instanceStatus)
	default:
		return !internal.IsSuspended(instanceStatus)
	}
}

func (r *Reconciler) refreshInvalidCounts(nodes []state.TrackedNode) {
	newInvalidCounts := make(map[string]int)
	for _, tn := range nodes {
		if c := r.invalidCount[tn.Node.Name]; c > 0 {
			newInvalidCounts[tn.Node.Name] = c
		}
	}
	r.invalidCount = newInvalidCounts
}
