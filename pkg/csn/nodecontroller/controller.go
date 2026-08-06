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

package nodecontroller

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/bluegreen"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/cfg"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/k8s"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/dispatch"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/handler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/queue"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/retry"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/reconciliation"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

const logPrefix = "CSN Node Controller:"

// CSNNode contains information about a CSN node.
type CSNNode struct {
	Name                 string
	State                csn.NodeState
	DesiredState         csn.NodeState
	HasPendingOperations bool
	Buffer               *BufferInfo
	IsBackedOff          bool
}

// BufferInfo contains information about the buffer to which a
// CSNNode is assigned.
type BufferInfo struct {
	Namespace string
	Name      string
}

func (b *BufferInfo) Id() string {
	if b == nil {
		return ""
	}
	return b.Namespace + "/" + b.Name
}

// CSNFilter represents a filter used for listing CSN nodes.
type CSNFilter func(CSNNode) bool

var (
	WithoutPendingOperationsFilter CSNFilter = func(n CSNNode) bool {
		return !n.HasPendingOperations
	}
	WithoutBackedOffSuspendedFilter CSNFilter = func(n CSNNode) bool {
		return !(n.State == csn.NodeStateSuspended && n.IsBackedOff)
	}
)

// CloudProvider allows for the retrieval of additional node information.
// It also allows for performing mutating operations on GCE instances.
type CloudProvider interface {
	GkeMigForNode(node *v1.Node) (*gke.GkeMig, error)
	// ResumeInstances resumes instances
	ResumeInstances(migRef gce.GceRef, instances []gce.GceRef, nonBlockingErrorsHandler gceclient.NonBlockingErrorsHandler) error
	// SuspendInstances suspends instances
	SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error
	// InstanceByRef allows for retrieval of GCE instances to get their status
	InstanceByRef(ref gce.GceRef) *gce.GceInstance
}

type csnNodeController struct {
	nodeStateManager *state.NodeStateManager
	cloudProvider    CloudProvider
	k8sClient        *k8s.ClientAdapter

	workQueue  *queue.WorkQueue
	dispatcher *dispatch.Dispatcher
	reconciler *reconciliation.Reconciler
	cfg        cfg.Controller
}

// NewCSNNodeController returns a new instance of CSN node controller.
func NewCSNNodeController(
	informerFactory informers.SharedInformerFactory,
	clientSet clientset.Interface,
	cp CloudProvider,
	experimentsManager experiments.Manager,
	backoff handler.CSNCompositeBackoff,
) *csnNodeController {
	config := cfg.NewProvider(experimentsManager).GetConfig()
	var nodeEventHandlers []state.EventHandler
	nsm := state.NewNodeStateManagerFromInformer(informerFactory,
		state.WithEventHandler(func(event state.NodeEvent) {
			for _, f := range nodeEventHandlers {
				f(event)
			}
		}),
		state.WithStopTrackingDelay(config.StateManager.StopTrackingDelay.Duration),
		state.WithMetricsSyncInterval(config.StateManager.MetricsSyncInterval.Duration),
		state.WithBackoff(backoff),
		state.WithCloudProvider(cp),
		state.WithExperimentsManager(experimentsManager),
	)
	wq := queue.NewWorkQueue(config.WorkQueue.MaxSize, nsm)
	d := dispatch.NewDispatcher(config.Dispatcher.WorkerCount, retry.Config{
		MaxRetries:   config.Dispatcher.Retry.MaxRetries,
		InitialDelay: config.Dispatcher.Retry.InitialDelay.Duration,
		MaxDelay:     config.Dispatcher.Retry.MaxDelay.Duration,
	}, wq, func(op ops.OperationType, nodeNames set.Set[string]) {
		// best-effort clear operation.
		nsm.SetPendingOperation(op, false, nodeNames)
	})
	tracker := taints.NewTracker(wq)
	nodeEventHandlers = append(nodeEventHandlers, tracker.HandleNodeEvent)
	k8sAdapter := k8s.NewClientAdapter(clientSet)
	r := reconciliation.NewReconciler(nsm, cp, wq,
		reconciliation.Config{
			MaxInvalidCount: config.Reconciliation.MaxInvalidCount,
		},
	)
	c := &csnNodeController{
		cloudProvider:    cp,
		nodeStateManager: nsm,
		k8sClient:        k8sAdapter,
		workQueue:        wq,
		dispatcher:       d,
		reconciler:       r,
		cfg:              config,
	}

	d.RegisterHandler(ops.SuspendOp, handler.NewSuspendHandler(nsm, cp, k8sAdapter, wq.Enqueue, config.Suspend.PreSuspendDelay.Duration).Handle)
	d.RegisterHandler(ops.ConsumeOp, handler.NewConsumeHandler(nsm, cp, k8sAdapter, backoff).Handle)
	d.RegisterHandler(ops.AssignBufferOp, handler.NewAssignBufferHandler(nsm, k8sAdapter).Handle)
	d.RegisterHandler(ops.AssignSoftTaintOp, handler.NewAssignSoftTaintHandler(nsm, k8sAdapter, tracker).Handle)

	return c
}

// Run initializes all background processes of the node controller.
func (c *csnNodeController) Run(ctx context.Context) {
	if err := c.nodeStateManager.Run(ctx); err != nil {
		klog.Errorf("%s failed to run node state manager: %v", logPrefix, err)
	}
	go c.dispatcher.Run(ctx)
}

func (c *csnNodeController) Consume(nodes []string) set.Set[string] {
	var nodesToConsume []*v1.Node
	for _, nodeName := range nodes {
		tn, ok := c.nodeStateManager.Get(nodeName)
		if !ok {
			continue
		}
		if tn.State != csn.NodeStateSuspended && tn.State != csn.NodeStateChilling {
			continue
		}
		if tn.PendingOperations.HasAny(ops.SuspendOp | ops.ConsumeOp) {
			continue
		}
		nodesToConsume = append(nodesToConsume, tn.Node)
	}

	// Group by MIG
	migNodes := c.groupByMig(nodesToConsume)

	enqueuedNodes := set.New[string]()
	for ref, m := range migNodes {
		err := c.workQueue.Enqueue(ops.Operation{
			MIG:       ref,
			Type:      ops.ConsumeOp,
			NodeNames: m.nodes,
		})
		if err != nil {
			klog.Errorf("%s failed to enqueue consume op for MIG %v: %v", logPrefix, ref, err)
			continue
		}
		enqueuedNodes.Insert(m.nodes.UnsortedList()...)
	}
	return enqueuedNodes
}

// MarkAsSuspendable receives a list of nodeInfos that can be suspended, and
// the node controller will decide which one to be suspended based on
// multiple criteria and return names of nodes it decided to suspend.
func (c *csnNodeController) MarkAsSuspendable(nodes []*framework.NodeInfo) set.Set[string] {
	var nodesToSuspend []*v1.Node

	for _, node := range nodes {
		name := node.Node().Name
		tn, ok := c.nodeStateManager.Get(name)
		if !ok {
			continue
		}
		if tn.State != csn.NodeStateChilling {
			continue
		}
		if nodeLifetime, minLifetime := time.Now().Sub(node.Node().CreationTimestamp.Time), c.minLifetimeForSuspend(&tn); nodeLifetime <= minLifetime {
			continue
		}
		if tn.PendingOperations.HasAny(ops.SuspendOp | ops.ConsumeOp) {
			continue
		}
		nodesToSuspend = append(nodesToSuspend, node.Node().DeepCopy())
	}

	nodeNames := set.New[string]()

	// Group by MIG
	migNodes := c.groupByMig(nodesToSuspend)

	// Filter MIGs not ready for suspension.
	for ref, m := range migNodes {
		if !isMigSuspendable(m.mig) {
			delete(migNodes, ref)
		}
	}

	for ref, m := range migNodes {
		namesList := m.nodes.UnsortedList()
		err := c.workQueue.Enqueue(ops.Operation{
			MIG:       ref,
			Type:      ops.SuspendOp,
			NodeNames: m.nodes,
		})
		if err != nil {
			klog.Errorf("%s failed to enqueue suspend op for MIG %v: %v", logPrefix, ref, err)
		} else {
			nodeNames.Insert(namesList...)
		}
	}

	return nodeNames
}

// Reconcile triggers internal cleanup mechanisms that enqueue
// consumption operations for nodes with states that deviate from
// the actual suspension status.
func (c *csnNodeController) Reconcile() {
	c.reconciler.Reconcile()
}

func (c *csnNodeController) UpdateBackoffStatus() {
	c.nodeStateManager.UpdateBackoffStatus()
}

func (c *csnNodeController) toCSNNode(tn *state.TrackedNode) CSNNode {
	desiredState := tn.DesiredState
	if desiredState == "" {
		desiredState = tn.State
	}
	var buffer *BufferInfo
	if tn.Buffer != nil {
		buffer = &BufferInfo{
			Namespace: tn.Buffer.Namespace,
			Name:      tn.Buffer.Name,
		}
	}
	node := tn.Node
	return CSNNode{
		Name:                 node.Name,
		State:                tn.State,
		DesiredState:         desiredState,
		HasPendingOperations: tn.PendingOperations.HasAny(ops.SuspendOp | ops.ConsumeOp),
		Buffer:               buffer,
		IsBackedOff:          tn.IsBackedOff,
	}
}

func (c *csnNodeController) List(filters ...CSNFilter) ([]CSNNode, error) {
	stateFilters := make([]state.NodeFilter, 0, len(filters))
	for _, filter := range filters {
		if filter == nil {
			continue
		}
		stateFilters = append(stateFilters, func(tn *state.TrackedNode) bool {
			return filter(c.toCSNNode(tn))
		})
	}

	trackedNodes := c.nodeStateManager.List(stateFilters...)
	result := make([]CSNNode, 0, len(trackedNodes))
	for _, tn := range trackedNodes {
		result = append(result, c.toCSNNode(&tn))
	}
	return result, nil
}

func (c *csnNodeController) ProcessBufferAssignment(nodeNameToBuffer map[string]*v1beta1.CapacityBuffer) {
	if len(nodeNameToBuffer) == 0 {
		return
	}
	c.nodeStateManager.AssignBuffers(nodeNameToBuffer)

	nodeNames := make([]string, 0, len(nodeNameToBuffer))
	for nodeName, buffer := range nodeNameToBuffer {
		tn, ok := c.nodeStateManager.Get(nodeName)
		if !ok || tn.PendingOperations.Contains(ops.AssignBufferOp) {
			continue
		}
		// no point in enqueueing op if the node is already assigned
		if csn.IsAssignedToBuffer(tn.Node, (&BufferInfo{
			Namespace: buffer.Namespace,
			Name:      buffer.Name,
		}).Id()) {
			continue
		}
		nodeNames = append(nodeNames, nodeName)
	}

	if len(nodeNames) == 0 {
		return
	}

	if err := c.workQueue.Enqueue(ops.Operation{
		Type:      ops.AssignBufferOp,
		NodeNames: set.New(nodeNames...),
	}); err != nil {
		klog.Errorf("%s error while enqueueing buffer assignment operation: %v", logPrefix, err)
	}
}

type migWithNodes struct {
	mig   *gke.GkeMig
	nodes set.Set[string]
}

func (c *csnNodeController) groupByMig(nodes []*v1.Node) map[gce.GceRef]migWithNodes {
	migNodes := make(map[gce.GceRef]migWithNodes)
	for _, n := range nodes {
		mig, err := c.cloudProvider.GkeMigForNode(n)
		if err != nil {
			klog.Errorf("%s failed to get MIG for node %q: %v", logPrefix, n.Name, err)
			continue
		}
		if mig == nil {
			// nil mig means that the node should not be handled by CA
			continue
		}
		if _, ok := migNodes[mig.GceRef()]; !ok {
			migNodes[mig.GceRef()] = migWithNodes{
				mig:   mig,
				nodes: set.New[string](),
			}
		}
		migNodes[mig.GceRef()].nodes.Insert(n.Name)
	}
	return migNodes
}

func isMigSuspendable(mig *gke.GkeMig) bool {
	_, shouldBlockScaleDown := bluegreen.ShouldBlockScaling(mig.BlueGreenInfo())
	if shouldBlockScaleDown {
		// Conditions for preventing scale-down are the same for suspension.
		klog.Infof("%s MIG %q is currently undergoing a blue-green upgrade, skipping suspension", logPrefix, mig.Id())
		return false
	}
	return true
}

func (c *csnNodeController) minLifetimeForSuspend(tn *state.TrackedNode) time.Duration {
	minLifetime := c.cfg.Suspend.MinNodeLifetime.Duration
	if tn.Buffer == nil || tn.Buffer.Annotations == nil {
		return minLifetime
	}
	fromBuffer, ok := tn.Buffer.Annotations[csn.ColdCapacityInitTimeAnnotationKey]
	if !ok {
		return minLifetime
	}
	d, err := time.ParseDuration(fromBuffer)
	if err != nil {
		klog.Errorf("%s failed to read %q as capacity buffer init time: %v", logPrefix, fromBuffer, err)
		return minLifetime
	}
	return d
}
