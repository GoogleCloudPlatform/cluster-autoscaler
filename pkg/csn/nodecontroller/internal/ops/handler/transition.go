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
	"iter"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

// Limit for GCE is 1000 instances per call.
// Source: https://docs.cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers/resumeInstances
// Date of access: 2026.03.27
const maxBatchSize = 1000

// Statuses reported by GCE for a managed instance. Both
// ManagedInstance.InstanceStatus and ManagedInstance.TargetStatus draw from the
// same set of values.
const (
	instanceStatusRunning   = "RUNNING"
	instanceStatusSuspended = "SUSPENDED"
)

type instanceTransitioner interface {
	// categorizeInstances groups the instances backing the given nodes by the work
	// they still need. Nodes whose instance cannot be categorized are recorded in
	// res right away, either as a success (the node is no longer tracked) or as an
	// error.
	categorizeInstances(migRef gce.GceRef, nodeNames set.Set[string], res *ops.Result) categorizedInstances
	// start asks GCE to transition the instances that have not started yet and
	// returns the instances worth polling: the ones whose call succeeded, plus the
	// ones GCE was already transitioning. Instances whose call failed are recorded
	// in res.
	start(migRef gce.GceRef, categorized categorizedInstances, res *ops.Result) []gce.GceRef
	// pollUntilDone polls the given instances until GCE stops working on them, and
	// yields:
	//   - (ref, nil) as soon as an instance completes the transition, at most once
	//     per instance;
	//   - (ref, nonBlocking) for a recoverable failure GCE reports for an instance
	//     it keeps retrying, possibly several times for the same instance.
	//
	// Instances that never complete, e.g. because the poll timed out on them or
	// because GCE left them in an intermediate state, are not yielded by pollUntilDone;
	// they are not recorded in res either. It is the responsibility of the caller
	// to record errors for them, e.g. as part of the errors it accumulates when
	// retrying the operation.
	pollUntilDone(ctx context.Context, migRef gce.GceRef, instances []gce.GceRef, res *ops.Result) iter.Seq2[gce.GceRef, *gceclient.NonBlockingInstanceError]
}

// instanceTransitionerImpl drives the GCE side of a node state change: it sorts the
// nodes by the work their instances still need, asks GCE to start the change,
// and polls until GCE stops working on them.
//
// Resuming and suspending are the same choreography with different statuses and
// calls, so each handler configures one of these and is left with only its own
// Kubernetes-side specifics.
type instanceTransitionerImpl struct {
	stateManager  StateManager
	cloudProvider CloudProvider
	// name names the transition in error messages, e.g. "resume".
	name string
	// inProgressAction is the CurrentAction GCE reports while transitioning.
	inProgressAction gceclient.InstanceAction
	// initialStatus is the TargetStatus of a not-yet-transitioned instance.
	initialStatus string
	// finalStatus is the TargetStatus of an instance GCE has been asked to
	// transition, whether or not it got there already.
	finalStatus string
	// startCall asks GCE to transition a batch of at most maxBatchSize instances.
	startCall func(migRef gce.GceRef, instances []gce.GceRef) error
	// callType labels startCall in the opGceBatchSize metric.
	callType  string
	logPrefix string
}

// newResumeTransitioner returns a transitioner that brings suspended instances
// back up.
func newResumeTransitioner(sm StateManager, cp CloudProvider) instanceTransitionerImpl {
	return instanceTransitionerImpl{
		stateManager:     sm,
		cloudProvider:    cp,
		name:             "resume",
		inProgressAction: gceclient.ActionResuming,
		initialStatus:    instanceStatusSuspended,
		finalStatus:      instanceStatusRunning,
		startCall:        cp.ResumeInstances,
		callType:         resumeCall,
		logPrefix:        consumeHandlerLogPrefix,
	}
}

// newSuspendTransitioner returns a transitioner that suspends running instances.
func newSuspendTransitioner(sm StateManager, cp CloudProvider) instanceTransitionerImpl {
	return instanceTransitionerImpl{
		stateManager:     sm,
		cloudProvider:    cp,
		name:             "suspend",
		inProgressAction: gceclient.ActionSuspending,
		initialStatus:    instanceStatusRunning,
		finalStatus:      instanceStatusSuspended,
		startCall: func(migRef gce.GceRef, instances []gce.GceRef) error {
			return cp.SuspendInstances(migRef, instances, false /* forceSuspend */)
		},
		callType:  suspendCall,
		logPrefix: suspendHandlerLogPrefix,
	}
}

// categorizedInstances splits the instances of an operation by the work they
// still need, based on the state GCE reports for them.
//
// The buckets hold GCE instance refs while the handlers are keyed by node name;
// the two coincide because GKE names a node after the instance backing it.
type categorizedInstances struct {
	// toStart needs a start call followed by polling.
	toStart []gce.GceRef
	// inProgress is already being transitioned by GCE, so it only needs polling.
	inProgress []gce.GceRef
	// completed already reached the final status, so it needs nothing.
	completed []gce.GceRef
}

// categorizeInstances groups the instances backing the given nodes by the work
// they still need. Nodes whose instance cannot be categorized are recorded in
// res right away, either as a success (the node is no longer tracked) or as an
// error.
func (t instanceTransitionerImpl) categorizeInstances(migRef gce.GceRef, nodeNames set.Set[string], res *ops.Result) categorizedInstances {
	var categorized categorizedInstances
	instances, err := t.cloudProvider.FetchManagedInstances(migRef, "")
	if err != nil {
		fetchErr := fmt.Errorf("failed to fetch instances for MIG %v: %w", migRef, err)
		res.AddErrForNodeSet(fetchErr, nodeNames)
		return categorized
	}

	instancesByName := make(map[string]*gceclient.ManagedInstance, len(instances))
	for _, inst := range instances {
		if inst != nil {
			instancesByName[inst.Name] = inst
		}
	}

	for nodeName := range nodeNames {
		tn, ok := t.stateManager.Get(nodeName)
		if !ok || tn.Node == nil {
			// No longer tracked, so there is nothing left to do for it.
			res.Success.Insert(nodeName)
			continue
		}
		ref, err := gce.GceRefFromProviderId(tn.Node.Spec.ProviderID)
		if err != nil {
			res.Errs[nodeName] = fmt.Errorf("invalid provider ID for node %q: %w", nodeName, err)
			continue
		}
		inst, ok := instancesByName[ref.Name]
		if !ok || inst == nil || inst.TargetStatus == "" {
			res.Errs[nodeName] = fmt.Errorf("could not find instance status for node %q", nodeName)
			continue
		}

		// TargetStatus is the status GCE is converging the instance towards, so it
		// tells whether the transition is still needed, under way, or futile.
		switch inst.TargetStatus {
		case t.initialStatus:
			categorized.toStart = append(categorized.toStart, ref)
		case t.finalStatus:
			if inst.InstanceStatus == t.finalStatus && inst.CurrentAction != string(t.inProgressAction) {
				categorized.completed = append(categorized.completed, ref)
			} else if inst.CurrentAction == string(t.inProgressAction) {
				categorized.inProgress = append(categorized.inProgress, ref)
			} else {
				res.Errs[nodeName] = fmt.Errorf("unsupported instance state (target_status: %q, instance_status: %q, current_action: %q) for node %q", inst.TargetStatus, inst.InstanceStatus, inst.CurrentAction, nodeName)
			}
		default:
			// e.g. TERMINATED, STOPPED, DELETED or ABANDONED: the instance is on its
			// way out, so it will never reach the final status.
			res.Errs[nodeName] = fmt.Errorf("incompatible instance target status %q for node %q", inst.TargetStatus, nodeName)
		}
	}
	return categorized
}

// start asks GCE to transition the instances that have not started yet and
// returns the instances worth polling: the ones whose call succeeded, plus the
// ones GCE was already transitioning. Instances whose call failed are recorded
// in res.
func (t instanceTransitionerImpl) start(migRef gce.GceRef, categorized categorizedInstances, res *ops.Result) []gce.GceRef {
	started := t.startInBatches(migRef, categorized.toStart, res)
	toPoll := make([]gce.GceRef, 0, len(started)+len(categorized.inProgress))
	toPoll = append(toPoll, started...)
	toPoll = append(toPoll, categorized.inProgress...)
	return toPoll
}

// startInBatches issues the start calls, in batches of at most maxBatchSize
// instances, and returns the refs of the instances whose call succeeded.
func (t instanceTransitionerImpl) startInBatches(migRef gce.GceRef, instances []gce.GceRef, res *ops.Result) []gce.GceRef {
	var started []gce.GceRef
	for i := 0; i < len(instances); i += maxBatchSize {
		batch := instances[i:min(i+maxBatchSize, len(instances))]
		status := gceSuccess
		if err := t.startCall(migRef, batch); err != nil {
			status = gceFailure
			batchErr := fmt.Errorf("failed to %s instances: %w, instances in batch: %v", t.name, err, batch)
			res.AddErrForRefSlice(batchErr, batch)
		} else {
			started = append(started, batch...)
		}
		opGceBatchSize.WithLabelValues(t.callType, status).Observe(float64(len(batch)))
	}
	return started
}

// pollUntilDone polls the given instances until GCE stops working on them, and
// yields:
//   - (ref, nil) as soon as an instance completes the transition, at most once
//     per instance;
//   - (ref, nonBlocking) for a recoverable failure GCE reports for an instance
//     it keeps retrying, possibly several times for the same instance.
//
// Instances that never complete, e.g. because the poll timed out on them or
// because ctx was cancelled, are recorded in res rather than yielded.
func (t instanceTransitionerImpl) pollUntilDone(ctx context.Context, migRef gce.GceRef, instances []gce.GceRef, res *ops.Result) iter.Seq2[gce.GceRef, *gceclient.NonBlockingInstanceError] {
	return func(yield func(gce.GceRef, *gceclient.NonBlockingInstanceError) bool) {
		// Remembers why each instance is struggling, so that an instance that never
		// completes can be reported with the code of the last non-blocking error seen for it.
		errCodes := make(map[string]string)
		for ref, update := range t.cloudProvider.PollUntilActionStops(ctx, t.inProgressAction, migRef, instances) {
			switch u := update.(type) {
			case gceclient.PollCompleted:
				if !yield(ref, nil) {
					return
				}
			case gceclient.PollRetrying:
				// GCE keeps retrying, so this is not the end of the road for the
				// instance: remember the error and let the caller react.
				errCodes[ref.Name] = ops.ErrorCodeFromGCEError(u.Err.Code, u.Err.Message, u.Err.InstanceStatus)
				if !yield(ref, u.Err) {
					return
				}
			case gceclient.PollAborted:
				res.Errs[ref.Name] = ops.NewErrorWithCode(errCodes[ref.Name], fmt.Errorf("failed to wait for instance to %s: %w", t.name, u.Err))
			default:
				klog.Warningf("%s unexpected poll update of type %T for instance %v, ignoring it", t.logPrefix, update, ref)
			}
		}
	}
}
