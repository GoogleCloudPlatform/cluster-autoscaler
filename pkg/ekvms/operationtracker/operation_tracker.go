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

// Package operationtracker defines OperationTracker that tracks and executes resizable VM resize and fix operations.
package operationtracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodetracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	ekvms_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	unknownMachineFamily             = "unknown"
	defaultWorkerRestartDelay        = time.Second
	allPodsNamespace                 = ""
	patchTimeout                     = 6 * time.Second
	patchLoggingThreshold            = patchTimeout - 2*time.Second
	upsizeTimeout                    = 1 * time.Minute
	downsizeTimeout                  = 2 * time.Minute
	resizeUnknownStateNodeTimeout    = 5 * time.Minute
	maxFixUnknownStateNodeAttempts   = 5
	reconcileNodeStateRequeueBackoff = 30 * time.Second
	fixerLogPrefix                   = "Resizable VM fixer: "
	downsizeDelayAfterTaint          = 1 * time.Second
)

type ResizeDirection string

const (
	Upsize        ResizeDirection = "upsize"
	Downsize      ResizeDirection = "downsize"
	UnknownResize ResizeDirection = "unknown"
)

type resizeStatus string

const (
	resizeSuccess resizeStatus = "success"
	resizeFailure resizeStatus = "failure"
)

type fixType string

const (
	balloonPodFix  fixType = "balloon_pod"
	resizeTaintFix fixType = "resize_taint"
)

type fixStatus string

const (
	fixSuccess fixStatus = "success"
	fixFailure fixStatus = "failure"
)

type reconcileNodeStateStatus string

const (
	reconcileNodeStateSuccess    reconcileNodeStateStatus = "success"
	reconcileNodeStateFailure    reconcileNodeStateStatus = "failure"
	reconcileNodeStateInProgress reconcileNodeStateStatus = "in_progress"
)

// OperationTracker executes and tracks all resizable VM resize and fix operations.
type OperationTracker interface {
	// Run executes the main loop of OperationTracker. It blocks until stopCh is closed.
	Run(chan struct{})
	// Resize triggers the resize operation of a given node. This is not a blocking call.
	Resize(ResizeOperation)
	// Fix triggers the fix operation of a given node. This is not a blocking call.
	Fix(machineFamily, nodeName string)
	// IsNodeInProcess return if the node is in process of running an operation or not
	IsNodeInProcess(nodeName string) bool
	// IsNodeResizingOrPending return if the node is in process of running a resize operation or not
	IsNodeResizingOrPending(nodeName string) bool
}

type operation struct {
	// IMPORTANT: Exactly one of the fields below should be set.
	resize             *ResizeOperation
	fix                *fixOperation
	reconcileNodeState *reconcileNodeStateOperation
}

func (op operation) nodeName() string {
	switch {
	case op.resize != nil:
		return op.resize.NodeName
	case op.fix != nil:
		return op.fix.NodeName
	case op.reconcileNodeState != nil:
		return op.reconcileNodeState.nodeName
	default:
		return ""
	}
}

// ResizeOperation represents a single resize operation.
type ResizeOperation struct {
	// Node is GKE Node to be resized.
	NodeName string
	// StartingSize is the size of the VM when the operation starts.
	StartingSize size.VmSize
	// DesiredSize is the size we want to resize to.
	DesiredSize size.VmSize
}

func (o *ResizeOperation) direction() ResizeDirection {
	if o.DesiredSize.IsDownsizeFrom(o.StartingSize) {
		return Downsize
	}
	if o.DesiredSize.IsUpsizeFrom(o.StartingSize) {
		return Upsize
	}
	return UnknownResize
}

type fixOperation struct {
	// Node is GKE Node to be checked for fixes.
	NodeName string
	// MachineFamily is the machine family of the node.
	MachineFamily string
}

type fixOperationSource string

const (
	newNodeSource   fixOperationSource = "NEW_NODE"
	fixerLoopSource fixOperationSource = "FIXER_LOOP"
)

type reconcileNodeStateOperation struct {
	nodeName   string
	targetSize size.VmSize
	attempts   int
}

// cloudProvider is the subset of GkeCloudProvider used by OperationTracker.
type cloudProvider interface {
	GetCurrentResizableVmState(node *v1.Node) (ekvmtypes.ResizableVmState, error)
	BulkFetchCurrentResizableVmStates() (map[gce.GceRef]ekvmtypes.ResizableVmState, error)
	ResizeVm(context.Context, *v1.Node, size.VmSize) error
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// balloonPodResizer is a subset of balloonPodResizer used by OperationTracker.
type balloonPodResizer interface {
	init() error
	resizeBalloonPod(node *v1.Node, desiredSize size.Allocatable) error
	addTaint(node *v1.Node, timeAdded time.Time) (*v1.Node, error)
	removeTaint(node *v1.Node) (*v1.Node, error)
	hasTaint(node *v1.Node) bool
	getPodForNode(node *v1.Node) *v1.Pod
	listAllBalloonPods(node *v1.Node) []*v1.Pod
}

// resizeMetrics is a subset of metrics used by operationTracker.
type resizeMetrics interface {
	ObserveVmGceResizeRequestDuration(machineFamily, direction, status string, duration time.Duration)
	RegisterResizableVmFixerEvents(machineFamily, fixType, status, source string)
	RegisterResizableVmReconcileNodeStateEvents(machineFamily string, attemptsNum int, status string, retry bool)
	RegisterVmResizeOperation(machineFamily, direction, reason string, status metrics.OperationStatus)
}

// operationTracker executes and tracks all resizable VM resize and fix operations.
type operationTracker struct {
	clientSet         clientset.Interface
	provider          cloudProvider
	informerFactory   informers.SharedInformerFactory
	balloonPodResizer balloonPodResizer
	metrics           resizeMetrics

	sizeCalculator                   calculator.Calculator
	worker                           func()
	numOfWorkers                     int
	clock                            clock.PassiveClock
	workerRestartDelay               time.Duration
	opQueue                          *operationQueue
	nodeStateManager                 nodeStateManager
	nodesBeingProcessed              nodetracker.Interface
	vmStateCache                     *vmStateCache
	reconcileNodeStateRequeueBackoff time.Duration
	waitingOnAdd                     sync.WaitGroup // Currently used for testing only.

	fixerEnabled  bool
	fixerInterval time.Duration
}

// New builds and returns an instance of OperationTracker.
func New(clientSet clientset.Interface, informerFactory informers.SharedInformerFactory, provider cloudProvider, nodeStateManager nodeStateManager, metrics resizeMetrics, sizeCalculator calculator.Calculator, workers int, fixerEnabled bool, fixerInterval time.Duration) OperationTracker {
	return newOperationTracker(clientSet, informerFactory, provider, nodeStateManager, metrics, sizeCalculator, workers, fixerEnabled, fixerInterval, clock.RealClock{})
}

// newOperationTracker builds and returns a new operationTracker instance.
func newOperationTracker(clientSet clientset.Interface, informerFactory informers.SharedInformerFactory, provider cloudProvider, nodeStateManager nodeStateManager, metrics resizeMetrics, sizeCalculator calculator.Calculator, workers int, fixerEnabled bool, fixerInterval time.Duration, clock clock.PassiveClock) *operationTracker {
	opTracker := &operationTracker{
		clientSet:       clientSet,
		provider:        provider,
		informerFactory: informerFactory,
		metrics:         metrics,
		sizeCalculator:  sizeCalculator,
		clock:           clock,
		balloonPodResizer: &defaultBalloonPodResizer{
			bPController: newBalloonPodController(clientSet, informerFactory),
			clientSet:    clientSet,
		},
		opQueue:                          newOperationQueue("OperationTracker"),
		nodeStateManager:                 nodeStateManager,
		nodesBeingProcessed:              nodetracker.New(clock),
		vmStateCache:                     newVmStateCache(provider),
		reconcileNodeStateRequeueBackoff: reconcileNodeStateRequeueBackoff,
		waitingOnAdd:                     sync.WaitGroup{},
		fixerEnabled:                     fixerEnabled,
		fixerInterval:                    fixerInterval,
	}
	// Configure workers.
	opTracker.worker = opTracker.opWorker
	opTracker.numOfWorkers = workers
	opTracker.workerRestartDelay = defaultWorkerRestartDelay
	return opTracker
}

// Run executes the main loop of operationTracker.
func (o *operationTracker) Run(stopCh chan struct{}) {
	defer o.opQueue.ShutDown()
	defer klog.Info("Operation tracker is shutting down.")

	klog.V(4).Infof("Initializing operation tracker")
	o.vmStateCache.refresh()

	if err := o.balloonPodResizer.init(); err != nil {
		klog.Fatalf("Can't initialize balloon pod controller: %v", err)
	}

	nodeInformer := o.informerFactory.Core().V1().Nodes()
	if _, err := nodeInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    o.onAddNode,
			UpdateFunc: o.onUpdateNode,
			DeleteFunc: o.onDeleteNode,
		},
	); err != nil {
		klog.Fatalf("Can't create node informer: %v", err)
	}

	o.startWorkers(stopCh)

	if o.fixerEnabled {
		go wait.Until(o.runFixerOnce, o.fixerInterval, stopCh)
	}

	klog.V(4).Infof("Initialized operation tracker")

	<-stopCh
}

func (o *operationTracker) onUpdateNode(_, newObj interface{}) {
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		klog.Warningf("Informer has incorrect object: %v", newObj)
		return
	}
	o.addOrUpdateNode(newNode)
}

func (o *operationTracker) onAddNode(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.Warningf("Informer has incorrect object: %v", obj)
		return
	}
	o.addOrUpdateNode(node)
}

func (o *operationTracker) addOrUpdateNode(node *v1.Node) {
	// Instance label is added *after* node creation by node controller,
	// and it is expected that instance label can be absent for short time
	// for new nodes.
	if _, exists := node.Labels[v1.LabelInstanceTypeStable]; !exists {
		klog.V(5).Infof("Node %q does not yet have instance type label set, skipping", node.Name)
		return
	}
	isResizable, err := ekvms_utils.IsResizableNode(node, o.provider.MachineConfigProvider())
	if err != nil {
		klog.Errorf("Unable to get machine type for node %q: %v", node.Name, err)
		return
	}
	if !isResizable {
		return
	}

	if o.tryToUpdate(node) {
		return
	}
	if o.nodesBeingProcessed.IsTracked(node.Name) {
		return
	}
	if node.Status.Allocatable == nil {
		return
	}
	if !isNodeReady(node) {
		return
	}

	maxVmSize, err := ekvms_utils.GetMaxResizableVmSize(o.provider.MachineConfigProvider(), node)
	if err != nil {
		klog.Errorf("Cannot add or update node %q, skipping: %v", node.Name, err)
		return
	}

	// Node handling is done in a separate goroutine to not block the informer on potential GCE calls.
	o.waitingOnAdd.Add(1)
	o.nodesBeingProcessed.AddNode(node.Name, o.clock.Now().Add(time.Hour))
	go func() {
		defer o.nodesBeingProcessed.DeleteNode(node.Name)
		defer o.waitingOnAdd.Done()
		o.handleNewNode(node, maxVmSize)
	}()
}

// tryToUpdate updates a node only if it is already tracked, and returns true if the update actually happened.
func (o *operationTracker) tryToUpdate(node *v1.Node) bool {
	// We don't want to further process resizable nodes under deletion.
	// For instance, we were getting false metric spikes in UAS recommendation ages
	// due to gceCache.instanceToMig cache already being invalidated.
	if taints.HasTaint(node, taints.ToBeDeletedTaint) {
		o.nodeStateManager.deleteNode(node.Name)
		return true
	}

	resizableNode, exists := o.nodeStateManager.getNode(node.Name)
	if !exists {
		return false
	}
	resizableNode.Node = node
	o.nodeStateManager.setNode(node.Name, resizableNode)
	return true
}

func (o *operationTracker) handleNewNode(node *v1.Node, maxVmSize size.VmSize) {
	klog.V(1).Infof("Adding resizable node %q.", node.Name)

	family, err := ekvms_utils.GetMachineFamilyName(node)
	if err != nil {
		klog.Errorf("Cannot get machine family for node %q, skipping: %v", node.Name, err)
		return
	}

	currentAllocatable, err := o.calculateCurrentAllocatable(node)
	if err != nil {
		klog.Errorf("Couldn't calculate current allocatable for node %q: %v", node.Name, err)
		return
	}
	// Store current VM state if not in cache.
	if vmState, err := o.vmStateCache.getState(node); err != nil {
		currentVmSize, err := o.sizeCalculator.ToVmSize(node, currentAllocatable)
		if err != nil {
			klog.Errorf("Failed to convert allocatable to current VM size for node %q: %v", node.Name, err)
			return
		}
		// We refresh the cache before we start informers in operation tracker. Since the node is not found in cache, it is a new node and there is no resize occurred and the VM status is at intent.
		if err := o.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{Size: currentVmSize, Status: ekvmtypes.ResizeStatusAtIntent}); err != nil {
			klog.Errorf("Cannot set current VM state for node %q: %v", node.Name, err)
		}
	} else if vmState.Status != ekvmtypes.ResizeStatusAtIntent {
		targetVmSize, err := o.sizeCalculator.ToVmSize(node, currentAllocatable)
		if err != nil {
			klog.Errorf("Failed to convert allocatable to target VM size for node %q: %v", node.Name, err)
			return
		}
		if err := o.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{
			Size:   targetVmSize,
			Status: vmState.Status,
		}); err != nil {
			klog.Errorf("Cannot set current VM state (should never happen here): %v", err)
		}
		o.registerNodeToBeReconciled(node.Name, targetVmSize)
	}

	// Fix operation will adjust the balloon pod.
	if err := o.fixNode(node, family, newNodeSource); err != nil {
		// Log error and allow node to be added to resizable nodes - no return.
		// Fixer will try to adjust the balloon pod again at some point.
		klog.Errorf("Adjusting balloon pod failed for node %q: %v", node.Name, err)
	}

	klog.V(4).Infof("Adding resizable node %q with allocatable %v and max VM size %v.", node.Name, currentAllocatable, maxVmSize)
	o.nodeStateManager.setNode(node.Name, ResizableNode{
		DesiredSize:     currentAllocatable,
		PhysicalMaxSize: o.sizeCalculator.ToAllocatable(node, maxVmSize),
		Node:            node,
		MachineFamily:   family,
	})
}

// calculateCurrentAllocatable calculates the current allocatable of VM and updates current VM size.
func (o *operationTracker) calculateCurrentAllocatable(node *v1.Node) (size.Allocatable, error) {
	currentAllocatable, hasBalloonPod := o.getCurrentAllocatableFromBalloonPod(node)
	if hasBalloonPod {
		klog.V(4).Infof("Resizable node %q has a balloon pod.", node.Name)
		return currentAllocatable, nil
	}

	klog.V(4).Infof("Resizable node %q has no balloon pod.", node.Name)
	currentVmState, err := o.vmStateCache.getStateOrRefresh(node)
	if err != nil {
		return size.Allocatable{}, fmt.Errorf("OperationTracker couldn't retrieve VM size for node %q: %w", node.Name, err)
	}

	currentAllocatable = o.sizeCalculator.ToAllocatable(node, currentVmState.Size)
	return currentAllocatable, nil
}

func (o *operationTracker) getCurrentAllocatableFromBalloonPod(node *v1.Node) (size.Allocatable, bool) {
	balloonPod := o.balloonPodResizer.getPodForNode(node)
	if balloonPod != nil {
		if len(balloonPod.Spec.Containers) > 0 && balloonPod.Spec.Containers[0].Resources.Requests != nil {
			bpSize := ekvms_utils.PodRequestsAsSize(balloonPod)
			return size.Allocatable{
				MilliCpus: node.Status.Allocatable.Cpu().MilliValue() - bpSize.MilliCpus,
				KBytes:    node.Status.Allocatable.Memory().Value()/size.KiB - bpSize.KBytes,
			}, true
		}
		klog.Warningf("Resizable node %q has malformed balloon pod %v.", node.Name, balloonPod)
	}
	return size.Allocatable{}, false
}

func (o *operationTracker) onDeleteNode(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.Warningf("Informer has incorrect object: %v", obj)
		return
	}
	isResizable, err := ekvms_utils.IsResizableNode(node, o.provider.MachineConfigProvider())
	if err != nil {
		klog.V(1).Infof("OperationTracker: node %q skipped: %v", node.Name, err)
		return
	}
	if !isResizable {
		return
	}
	o.nodeStateManager.deleteNode(node.Name)
	err = o.vmStateCache.invalidate(node)
	if err != nil {
		klog.Warningf("Invalidating current vm size cache for node %q failed: %v", node.Name, err)
	}
}

func (o *operationTracker) startWorkers(stopCh chan struct{}) {
	for i := 0; i < o.numOfWorkers; i++ {
		go wait.Until(o.worker, o.workerRestartDelay, stopCh)
	}
}

// Resize triggers the resize operation of a given node. This is not a blocking call.
func (o *operationTracker) Resize(ro ResizeOperation) {
	o.opQueue.Enqueue(operation{resize: &ro})
}

func (o *operationTracker) registerNodeToBeReconciled(nodeName string, targetSize size.VmSize) {
	o.nodeStateManager.setNodeAsUnhealthy(nodeName, UnknownResizeStatus)
	o.opQueue.Enqueue(operation{
		reconcileNodeState: &reconcileNodeStateOperation{
			nodeName:   nodeName,
			targetSize: targetSize,
		},
	})
}

func (o *operationTracker) runFixerOnce() {
	for nodeName, resizableNode := range o.nodeStateManager.snapshot(true) {
		o.Fix(resizableNode.MachineFamily, nodeName)
	}
}

// Fix triggers the fix operation of a given node. This is not a blocking call.
func (o *operationTracker) Fix(machineFamily, nodeName string) {
	o.opQueue.Enqueue(operation{fix: &fixOperation{MachineFamily: machineFamily, NodeName: nodeName}})
}

func (o *operationTracker) opWorker() {
	for o.processNextOperation() {
	}
	klog.Info("Operation tracker worker is shutting down.")
}

func (o *operationTracker) processNextOperation() bool {
	operation, quit := o.opQueue.Get()
	if quit {
		return false
	}
	defer o.opQueue.Done(operation)

	switch {
	case operation.resize != nil:
		o.handleResizeOperation(*operation.resize)
	case operation.fix != nil:
		err := o.handleFixOperation(*operation.fix)
		if err != nil {
			klog.Errorf("Handling fix operation failed: %v", err)
		}
	case operation.reconcileNodeState != nil:
		o.handleReconcileNodeStateOperation(*operation.reconcileNodeState)
	default:
		klog.Errorf("Operation not supported: %+v", operation)
	}
	return true
}

func (o *operationTracker) handleFixOperation(op fixOperation) error {
	resizableNode, exists := o.nodeStateManager.getNode(op.NodeName)
	if !exists {
		klog.Warningf("%sFixing node %q: node was not found", fixerLogPrefix, op.NodeName)
		return nil
	}
	return o.fixNode(resizableNode.Node.DeepCopy(), op.MachineFamily, fixerLoopSource)
}

func (o *operationTracker) fixNode(node *v1.Node, machineFamily string, source fixOperationSource) error {
	nodeName := node.Name
	balloonPods := o.balloonPodResizer.listAllBalloonPods(node)
	currentVmState, sizeError := o.vmStateCache.getState(node)
	if sizeError != nil {
		return fmt.Errorf("retrieving current size of resizable node %q failed: %w", nodeName, sizeError)
	}
	desiredAllocatable := o.sizeCalculator.ToAllocatable(node, currentVmState.Size)
	var err error
	bpIsCorrect, bpStatus := balloonPodIsCorrect(node, desiredAllocatable, balloonPods)
	if !bpIsCorrect {
		klog.V(4).Infof("%sFixing balloon pod for node %q. Extra info: {%s}", fixerLogPrefix, nodeName, getBalloonPodsLog(bpStatus, balloonPods, node, desiredAllocatable))
		node, err = o.balloonPodResizer.addTaint(node, time.Now())
		if err != nil {
			o.metrics.RegisterResizableVmFixerEvents(machineFamily, string(balloonPodFix), string(fixFailure), string(source))
			return fmt.Errorf("applying resize taints for node %q failed: %w", nodeName, err)
		}

		// resizeBalloonPod also deletes all existing balloon pods.
		err = o.balloonPodResizer.resizeBalloonPod(node, desiredAllocatable)
		if err != nil {
			o.metrics.RegisterResizableVmFixerEvents(machineFamily, string(balloonPodFix), string(fixFailure), string(source))
			return fmt.Errorf("fixing balloon pods for node %q failed: %w", nodeName, err)
		}
		o.metrics.RegisterResizableVmFixerEvents(machineFamily, string(balloonPodFix), string(fixSuccess), string(source))
	}
	if o.balloonPodResizer.hasTaint(node) {
		klog.V(4).Infof("%sFixing resize taint for node %q.", fixerLogPrefix, nodeName)
		if _, err = o.balloonPodResizer.removeTaint(node); err != nil {
			o.metrics.RegisterResizableVmFixerEvents(machineFamily, string(resizeTaintFix), string(fixFailure), string(source))
			return fmt.Errorf("removing balloon pods resize taint for node %q failed: %w", nodeName, err)
		}
		o.metrics.RegisterResizableVmFixerEvents(machineFamily, string(resizeTaintFix), string(fixSuccess), string(source))
	}
	return nil
}

// handleReconcileNodeStateOperation fixes a resizable VM that has unknown resize state.
func (o *operationTracker) handleReconcileNodeStateOperation(op reconcileNodeStateOperation) {
	resizableNode, exists := o.nodeStateManager.getNode(op.nodeName)
	if !exists {
		klog.Warningf("%sFixing node with Unknown State %q: node was not found", fixerLogPrefix, op.nodeName)
		return
	}

	status := o.reconcileNodeState(op, resizableNode.Node)
	shouldRetry := true
	op.attempts++
	if status == reconcileNodeStateSuccess {
		o.nodeStateManager.setNodeAsHealthy(op.nodeName)
		shouldRetry = false
	}
	if op.attempts >= maxFixUnknownStateNodeAttempts {
		klog.Errorf("Reached maximum number of attempts (%d) for fixing resizable node with unknown state %q", maxFixUnknownStateNodeAttempts, op.nodeName)
		o.nodeStateManager.setNodeAsUnhealthy(op.nodeName, FailedResizeStatus)
		shouldRetry = false
	}
	o.metrics.RegisterResizableVmReconcileNodeStateEvents(resizableNode.MachineFamily, op.attempts, string(status), shouldRetry)
	if shouldRetry {
		o.opQueue.EnqueueAfter(operation{reconcileNodeState: &op}, o.reconcileNodeStateRequeueBackoff)
	}
}

// reconcileNodeState fixes node with unknown resize state by making resize calls again.
func (o *operationTracker) reconcileNodeState(op reconcileNodeStateOperation, node *v1.Node) reconcileNodeStateStatus {
	targetSize := op.targetSize

	resizableVmState, err := o.provider.GetCurrentResizableVmState(node)
	if err != nil {
		klog.Errorf("Failed to fetch resizable VM state for node %q: %v", node.Name, err)
		return reconcileNodeStateFailure
	}
	if err := o.vmStateCache.updateState(node, resizableVmState); err != nil {
		// This should never fail.
		klog.Errorf("Failed to update VM state in cache for node %q: %v", node.Name, err)
	}
	if resizableVmState.Status == ekvmtypes.ResizeStatusAtIntent {
		return reconcileNodeStateSuccess
	}
	if resizableVmState.Status == ekvmtypes.ResizeStatusInProgress {
		klog.Infof("Resizable node %q is still in progress (has ongoing operation)", node.Name)
		return reconcileNodeStateInProgress
	}

	resizeCtx, resizeCtxCancel := context.WithTimeout(context.Background(), resizeUnknownStateNodeTimeout)
	defer resizeCtxCancel()
	klog.Infof("Attempting to fix node with unknown status %q, target size: %v, attempt number %d (max attempts is %d)", node.Name, targetSize, op.attempts+1, maxFixUnknownStateNodeAttempts)
	err = o.provider.ResizeVm(resizeCtx, node, targetSize)
	if err != nil {
		klog.Errorf("Failed to fix node with unknown status %q (by resizing it to target size %v): %v", node.Name, targetSize, err)
		return reconcileNodeStateFailure
	}
	return reconcileNodeStateSuccess
}

func (o *operationTracker) handleResizeOperation(op ResizeOperation) {
	switch {
	case op.StartingSize == op.DesiredSize:
		// Nothing to do.
		return
	case op.DesiredSize.IsUpsizeFrom(op.StartingSize):
		if err := o.upsize(op); err != nil {
			o.handleResizeError(err, op)
			o.opQueue.ClearResizeOperations(op.NodeName)
			if resizableNode, exists := o.nodeStateManager.getNode(op.NodeName); exists {
				o.Fix(resizableNode.MachineFamily, op.NodeName)
			}
			return
		}
	case op.DesiredSize.IsDownsizeFrom(op.StartingSize):
		if err := o.downsize(op); err != nil {
			o.handleResizeError(err, op)
			o.opQueue.ClearResizeOperations(op.NodeName)
			if resizableNode, exists := o.nodeStateManager.getNode(op.NodeName); exists {
				o.Fix(resizableNode.MachineFamily, op.NodeName)
			}
			return
		}
	default:
		o.handleResizeError(fmt.Errorf("combining resizes and downsizes of different resource dimensions is not supported, this should never happen"), op)
		o.opQueue.ClearResizeOperations(op.NodeName)
		return
	}
	o.registerResizeSuccess(op)
}

func (o *operationTracker) upsize(operation ResizeOperation) error {
	resizableNode, exists := o.nodeStateManager.getNode(operation.NodeName)
	if !exists {
		klog.Warningf("Upsize operation for node %q: node was not found", operation.NodeName)
		return nil
	}
	node := resizableNode.Node.DeepCopy()
	machineFamily := resizableNode.MachineFamily

	vmResize := make(chan error)
	bpResize := make(chan error)
	taintedNode, err := o.balloonPodResizer.addTaint(node, time.Now())
	if err != nil {
		return ek_errors.NewBalloonPodResizeTaintError(
			machineFamily, fmt.Errorf("adding taint failed for node %q: %w", node.Name, err),
			ek_errors.StartingState)
	}
	go func() {
		upsizeCtx, upsizeCtxCancel := context.WithTimeout(context.Background(), upsizeTimeout)
		defer upsizeCtxCancel()
		vmResize <- o.resizeVm(upsizeCtx, node, operation.DesiredSize, Upsize)
	}()
	go func() {
		bpResize <- o.balloonPodResizer.resizeBalloonPod(node, o.sizeCalculator.ToAllocatable(node, operation.DesiredSize))
	}()
	vmResizeErr := <-vmResize
	bpResizeErr := <-bpResize

	// Prioritize handling VM resize error even if balloon pod resize also failed.
	if vmResizeErr != nil {
		return vmResizeErr
	}

	// If we are here, it means upsize was a success.
	if err := o.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{Size: operation.DesiredSize, Status: ekvmtypes.ResizeStatusAtIntent}); err != nil {
		// This should never fail.
		return ek_errors.NewGenericError(machineFamily, err, ek_errors.DesiredState)
	}
	if bpResizeErr != nil {
		return ek_errors.NewBalloonPodResizeError(machineFamily, bpResizeErr, ek_errors.DesiredState)
	}
	if _, err := o.balloonPodResizer.removeTaint(taintedNode); err != nil {
		return ek_errors.NewBalloonPodResizeTaintError(
			machineFamily, fmt.Errorf("removing taint failed for node %q: %w", taintedNode.Name, err),
			ek_errors.DesiredState)
	}
	return nil
}

func (o *operationTracker) downsize(operation ResizeOperation) error {
	resizableNode, exists := o.nodeStateManager.getNode(operation.NodeName)
	if !exists {
		klog.Warningf("Downsize operation for node %q: node was not found", operation.NodeName)
		return nil
	}
	node := resizableNode.Node.DeepCopy()
	machineFamily := resizableNode.MachineFamily

	taintedNode, err := o.balloonPodResizer.addTaint(node, time.Now())
	if err != nil {
		return ek_errors.NewBalloonPodResizeTaintError(
			machineFamily, fmt.Errorf("adding taint failed for node %q: %w", node.Name, err),
			ek_errors.StartingState)
	}
	// We want to give some time to scheduler to become aware of the new taint and prevent scheduling any new pods on this node.
	// This is not very elegant way of reducing the risk of race condition with scheduler (b/430544128).
	time.Sleep(downsizeDelayAfterTaint)
	requestedResources, err := calculateRequestedResources(o.clientSet, taintedNode)
	if err != nil {
		return ek_errors.NewGenericError(
			machineFamily, fmt.Errorf("calculating requested resources failed for node %q: %w", taintedNode.Name, err),
			ek_errors.StartingState)
	}
	desiredSizeAllocatable := o.sizeCalculator.ToAllocatable(taintedNode, operation.DesiredSize)
	if requestedResources.IsUpsizeFrom(desiredSizeAllocatable) {
		return ek_errors.NewExceededPodRequestsWarning(
			machineFamily, fmt.Errorf("requested resources (%v) exceed new requested node size (%v) for node: %q", requestedResources, desiredSizeAllocatable, taintedNode.Name),
			ek_errors.StartingState)
	}
	if err := o.balloonPodResizer.resizeBalloonPod(taintedNode, desiredSizeAllocatable); err != nil {
		return ek_errors.NewBalloonPodResizeError(machineFamily, err, ek_errors.StartingState)
	}

	untaintedNode, err := o.balloonPodResizer.removeTaint(taintedNode)
	if err != nil {
		return ek_errors.NewBalloonPodResizeTaintError(machineFamily, fmt.Errorf("removing taint failed for node %q: %w", taintedNode.Name, err), ek_errors.StartingState)
	}
	downsizeCtx, downsizeCtxCancel := context.WithTimeout(context.Background(), downsizeTimeout)
	defer downsizeCtxCancel()
	if err := o.resizeVm(downsizeCtx, untaintedNode, operation.DesiredSize, Downsize); err != nil {
		return err
	}
	// If we are here, it means downsize was a success.
	if err := o.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{Size: operation.DesiredSize, Status: ekvmtypes.ResizeStatusAtIntent}); err != nil {
		return ek_errors.NewGenericError(machineFamily, err, ek_errors.DesiredState)
	}
	return nil
}

func (o *operationTracker) getMachineFamily(nodeName string) string {
	resizableNode, exists := o.nodeStateManager.getNode(nodeName)
	if exists {
		return resizableNode.MachineFamily
	}
	return unknownMachineFamily
}

func (o *operationTracker) registerResizeSuccess(operation ResizeOperation) {
	machineFamily := o.getMachineFamily(operation.NodeName)
	klog.V(4).Infof("[%s resize] Resized (%s) node %q to %+v", machineFamily, string(operation.direction()), operation.NodeName, operation.DesiredSize)
	o.metrics.RegisterVmResizeOperation(machineFamily, string(operation.direction()), "", metrics.OperationSucceeded)
}

func (o *operationTracker) handleResizeError(resizeError error, operation ResizeOperation) {
	resizableNode, exists := o.nodeStateManager.getNode(operation.NodeName)
	machineFamily := unknownMachineFamily
	if exists {
		machineFamily = resizableNode.MachineFamily
	}

	wrappedResizeError, isResizeError := ek_errors.ToResizeError(resizeError)
	if !isResizeError {
		klog.Errorf("[%s resize] Resize failed for node %q but error isn't of type ResizeError. This is illegal state. Ensure all errors produced by upsize/downsize flows are wrapped in ResizeError.", machineFamily, operation.NodeName)
		wrappedResizeError = ek_errors.NewUntypedError(machineFamily, ek_errors.Empty, resizeError)
	}

	if wrappedResizeError.ErrType == ek_errors.ExceededPodRequestWarning {
		klog.Warningf("[%s resize] Warning during resize (%s) of node %q: %v", machineFamily, string(operation.direction()), operation.NodeName, wrappedResizeError.OriginalError)
	} else {
		klog.Errorf("[%s resize] Error during resize (%s) of node %q: %v", machineFamily, string(operation.direction()), operation.NodeName, resizeError)
	}

	if !exists {
		klog.Warningf("[%s resize] handleResizeError for node %q: node was not found", machineFamily, operation.NodeName)
		return
	}
	node := resizableNode.Node.DeepCopy()

	o.nodeStateManager.backoff(node, *wrappedResizeError)
	o.metrics.RegisterVmResizeOperation(machineFamily, string(operation.direction()), string(wrappedResizeError.ErrType), metrics.OperationFailed)

	/*
		A successful resize flow looks like this:
			1. Update desired node size in nodeStateManager
			2. Resize VM
			3. Update VM size in vmSizeCache

		There are 3 types of resize errors:
			1. VM resize failed and we don't know VM's current size.
				- Need to assume the minimum size and store it in vmSizeCache and nodeStateManager to avoid OOMs and throttling.

			2. VM resize failed and we are guaranteed it's in the starting state.
				- Need to revert nodeStateManager only since vmSizeCache was never modified.

			3. VM resize succeeded but something else failed during the resize.
				- Resize technically succeeded so we need to update vmSizeCache to reflect the new size.
	*/

	if wrappedResizeError.VmState == ek_errors.UnknownState {
		safeSize := size.MinSize(operation.StartingSize, operation.DesiredSize)
		if err := o.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{Size: safeSize, Status: ekvmtypes.ResizeStatusUnknownCA}); err != nil {
			// This should never fail.
			klog.Errorf("[%s resize] While handling resize error %v for node: %q, encountered another error: %v", machineFamily, resizeError, operation.NodeName, err)
		}
		o.nodeStateManager.setNodeSize(node, o.sizeCalculator.ToAllocatable(node, safeSize))
		o.registerNodeToBeReconciled(node.Name, safeSize)
		return
	}

	if wrappedResizeError.VmState == ek_errors.StartingState {
		o.nodeStateManager.setNodeSize(node, o.sizeCalculator.ToAllocatable(node, operation.StartingSize))
		return
	}

	if wrappedResizeError.VmState == ek_errors.DesiredState {
		o.nodeStateManager.setNodeSize(node, o.sizeCalculator.ToAllocatable(node, operation.DesiredSize))
		if err := o.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{Size: operation.DesiredSize, Status: ekvmtypes.ResizeStatusAtIntent}); err != nil {
			// This should never fail.
			klog.Errorf("[%s resize] While handling resize error %v for node: %q, encountered another error: %v", machineFamily, resizeError, operation.NodeName, err)
		}
		return
	}
}

// resizeVm is a wrapper around provider.ResizeVm() that measures call duration and reports it as a metric.
func (o *operationTracker) resizeVm(ctx context.Context, node *v1.Node, size size.VmSize, direction ResizeDirection) error {
	start := o.clock.Now()
	err := o.provider.ResizeVm(ctx, node, size)
	duration := time.Since(start)

	resizeStatus := resizeSuccess
	if err != nil {
		resizeStatus = resizeFailure
	}
	family := o.getMachineFamily(node.Name)
	o.metrics.ObserveVmGceResizeRequestDuration(family, string(direction), string(resizeStatus), duration)
	return err
}

func (o *operationTracker) IsNodeInProcess(nodeName string) bool {
	return o.opQueue.IsNodeInProcess(nodeName)
}

func (o *operationTracker) IsNodeResizingOrPending(nodeName string) bool {
	return o.opQueue.IsNodeResizingOrPending(nodeName)
}
