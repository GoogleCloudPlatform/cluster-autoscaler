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

package handler

import (
	"context"
	"fmt"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

const (
	consumeHandlerLogPrefix = "CSN Consume Handler:"
)

// CSNCompositeBackoff handles backoff logic specifically for CSN resumption errors.
type CSNCompositeBackoff interface {
	base_backoff.Backoff
	ReportResumptionError(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorCode, errorMessage, instanceStatus string)
}

type ConsumeHandler struct {
	stateManager  StateManager
	cloudProvider CloudProvider
	k8sClient     K8sClient
	// nodeLister is the readiness source of the watched nodes. The state manager
	// cannot play that role: patching a node to consumed takes it out of CSN, and
	// from then on the state manager stops refreshing its copy of it.
	nodeLister NodeLister
	backoff    CSNCompositeBackoff
	// experimentsManager says whether waiting for the resumed nodes to become
	// ready is turned on. It is read on every operation, so the wait can be turned
	// off without restarting the autoscaler.
	experimentsManager experiments.Manager
	// resume drives the GCE side of the operation: it tells which instances still
	// need to be resumed, resumes them and polls until they are up.
	resume instanceTransitioner
}

func NewConsumeHandler(sm StateManager, cp CloudProvider, c K8sClient, nl NodeLister, b CSNCompositeBackoff, em experiments.Manager) *ConsumeHandler {
	return &ConsumeHandler{
		stateManager:       sm,
		cloudProvider:      cp,
		k8sClient:          c,
		nodeLister:         nl,
		backoff:            b,
		experimentsManager: em,
		resume:             newResumeTransitioner(sm, cp),
	}
}

// Handle resumes the instances of the operation and marks their nodes as
// consumed. A node that had to be resumed is only reported as a success once
// Kubernetes sees it as ready again; nodes whose instances were already up are
// reported as soon as they are patched. Waiting for readiness is behind an
// experiment flag: with it off, every node is reported as soon as it is patched.
//
// Nodes progress independently, so the slowest node of a batch only delays this
// call, never its peers.
func (h *ConsumeHandler) Handle(ctx context.Context, op ops.Operation) (ops.Result, error) {
	result := ops.NewResult()
	if op.Type != ops.ConsumeOp {
		return result, fmt.Errorf("got operation type %s, expected %s", op.Type, ops.ConsumeOp)
	}

	categorized := h.resume.categorizeInstances(op.MIG, op.NodeNames, &result)

	// Patching runs on its own goroutine so that the poll loop below can keep an
	// eye on the remaining instances while the Kubernetes API works through the
	// nodes that are already up.
	queuePatch, waitForPatches := h.startPatchWorker(ctx, len(op.NodeNames))
	// Readiness is watched alongside it, so that a node is timed from the moment
	// its instance came back up rather than from whenever the patches ahead of it
	// are done.
	watchReadiness, waitForReadiness := h.startReadinessWatcher(ctx, len(op.NodeNames))

	// Instances that are already up only need the Kubernetes patch: their nodes
	// didn't get suspended, so there is no readiness to wait for.
	for _, ref := range categorized.completed {
		queuePatch(ref.Name)
	}

	if toPoll := h.resume.start(op.MIG, categorized, &result); len(toPoll) > 0 {
		// Each node is queued for patching as soon as its instance is up, instead of
		// being held hostage to the slowest instance in the batch.
		for ref, nonBlockingErr := range h.resume.pollUntilDone(ctx, op.MIG, toPoll, &result) {
			if nonBlockingErr != nil {
				// GCE keeps retrying the instance, so back off globally and keep waiting.
				h.reportNonBlockingError(ref.Name, nonBlockingErr)
				continue
			}
			queuePatch(ref.Name)
			// This instance went down and came back up, so it needs to report as ready
			// before the node can be counted on.
			watchReadiness(ref.Name)
		}
	}

	// Only the nodes whose patch went out are being consumed, so they are the only
	// ones whose readiness matters.
	consumed := waitForPatches(&result)
	waitForReadiness(&result, consumed)
	return result, nil
}

// startPatchWorker starts a goroutine that patches nodes to the consumed state
// one by one, and returns a function to queue a node for patching and a function
// to wait for the queued nodes to be patched.
//
// queuePatch never blocks: the queue has room for every node of the operation,
// which keeps the poll loop cheap as its contract requires. It must not be
// called once waitForPatches has been called.
//
// waitForPatches must be called exactly once, and only after every node has been
// queued. It blocks until the worker is done, merges the outcome of each patch
// into result, which must not be read concurrently with it, and returns the
// nodes whose patch actually went out.
func (h *ConsumeHandler) startPatchWorker(ctx context.Context, maxNodes int) (queuePatch func(nodeName string), waitForPatches func(result *ops.Result) (patchedNodes set.Set[string])) {
	nodeNames := make(chan string, maxNodes)
	done := make(chan struct{})
	// Only the worker touches these until it exits, so no synchronization is
	// needed, and keeping them apart from the caller's result lets the caller keep
	// recording its own errors while the worker runs.
	patchResult := ops.NewResult()
	patchedNodes := set.New[string]()
	go func() {
		defer close(done)
		for nodeName := range nodeNames {
			patched, err := h.patchNodeToConsumed(ctx, nodeName)
			if err != nil {
				patchResult.Errs[nodeName] = err
				continue
			}
			patchResult.Success.Insert(nodeName)
			if patched {
				patchedNodes.Insert(nodeName)
			}
		}
	}()

	queuePatch = func(nodeName string) {
		nodeNames <- nodeName
	}
	waitForPatches = func(result *ops.Result) set.Set[string] {
		close(nodeNames)
		<-done
		result.AddResult(patchResult)
		return patchedNodes
	}
	return queuePatch, waitForPatches
}

// startReadinessWatcher returns a function to start watching a node for
// readiness and a function to collect the outcome. Resuming an instance is only
// half of the job: the node is of no use until its is ready, as unready nodes
// are not available for scheduling pods.
//
// watchReadiness hands a node to the watcher, which polls it from then on. It
// must not be called once waitForReadiness has been.
//
// waitForReadiness must be called exactly once, and only after the last watch
// has been started. It drops the nodes outside consumed, which need their retry
// now rather than after the timeout, waits for the rest, and fails the nodes
// that never came back by way of result, which must not be read concurrently
// with it.
//
// A node that already failed for another reason keeps that error.
//
// With the experiment off, both functions do nothing and no watcher is started,
// which leaves the handler reporting a node as consumed as soon as it is
// patched.
func (h *ConsumeHandler) startReadinessWatcher(ctx context.Context, maxNodes int) (watchReadiness func(nodeName string), waitForReadiness func(result *ops.Result, consumed set.Set[string])) {
	if !h.waitForNodeReadinessEnabled() {
		return func(string) {}, func(*ops.Result, set.Set[string]) {}
	}
	watcher := newReadinessWatcher(h.nodeLister, maxNodes, consumeHandlerLogPrefix)
	// noMoreNodes tells the watcher that the caller is done handing over nodes,
	// and stopped tells the caller that the watcher has exited.
	noMoreNodes, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		watcher.run(ctx, noMoreNodes)
	}()

	watchReadiness = watcher.watch
	waitForReadiness = func(result *ops.Result, consumed set.Set[string]) {
		watcher.keepOnly(consumed)
		close(noMoreNodes)
		<-stopped

		for nodeName, readinessErr := range watcher.notReady {
			if !consumed.Has(nodeName) {
				// It was dropped from the watcher, so it never got its full timeout.
				continue
			}
			if earlierErr, failed := result.Errs[nodeName]; failed {
				// Something else went wrong first, and that is the reason worth
				// reporting. The readiness failure goes no further than this line, so it
				// is spelled out here next to the one that wins.
				klog.V(4).Infof("%s resumed node %q did not become ready, but it already failed for another reason: reporting %q, dropping %q", consumeHandlerLogPrefix, nodeName, earlierErr, readinessErr)
				continue
			}
			// A node that never became ready has not been consumed, however well its
			// patch went, so the success the patch recorded has to go.
			result.Success.Delete(nodeName)
			result.Errs[nodeName] = readinessErr
		}
	}
	return watchReadiness, waitForReadiness
}

// waitForNodeReadinessEnabled reports whether the resumed nodes are to be waited
// on before being reported as consumed.
func (h *ConsumeHandler) waitForNodeReadinessEnabled() bool {
	return h.experimentsManager != nil && h.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesWaitForNodeReadiness)
}

// patchNodeToConsumed marks the node as consumed and reports whether the patch
// went out, which it does not for a node that is no longer tracked. It runs on
// the patch worker goroutine, so it must not touch state that the caller of
// startPatchWorker is still using.
func (h *ConsumeHandler) patchNodeToConsumed(ctx context.Context, nodeName string) (patched bool, err error) {
	tn, ok := h.stateManager.Get(nodeName)
	if !ok || tn.Node == nil {
		// The node stopped being tracked while the instance was resuming.
		return false, nil
	}
	if err := h.k8sClient.ApplyNodePatch(ctx, tn.Node, csn.NodeStateConsumed); err != nil {
		return false, fmt.Errorf("failed to patch node %q to be consumed: %w", nodeName, err)
	}
	return true, nil
}

// reportNonBlockingError reports to the global backoff a non-blocking error
// reported by the poll for a resuming instance.
func (h *ConsumeHandler) reportNonBlockingError(nodeName string, e *gceclient.NonBlockingInstanceError) {
	if h.backoff == nil || !h.stateManager.IsBackoffEnabled() {
		return
	}
	tn, ok := h.stateManager.Get(nodeName)
	if !ok || tn.Node == nil {
		return
	}
	nodeGroup, _ := h.cloudProvider.GkeMigForNode(tn.Node)
	if nodeGroup == nil {
		return
	}
	nodeInfo := framework.NewNodeInfo(tn.Node, nil)
	h.backoff.ReportResumptionError(nodeGroup, nodeInfo, e.Code, e.Message, e.InstanceStatus)
	klog.Errorf("%s resuming node %q encountered an error (will continue polling for resumption but will trigger global backoffs): code %q, message: %q, instance status: %q", consumeHandlerLogPrefix, tn.Node.Name, e.Code, e.Message, e.InstanceStatus)
}
