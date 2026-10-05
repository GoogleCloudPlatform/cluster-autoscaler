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

// Package capacitybuffers implements the reconciliation loop, in-flight caching, and candidate election
// for GKE ComputeClass capacity buffers and pod templates.
package capacitybuffers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	ccapiv1ac "github.com/googlecloudplatform/compute-class-api/client/applyconfiguration/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/rest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	logPrefix = "[CapacityBufferReconciler]"
)

// Reconciler reconciles ComputeClass objects to maintain their required capacity buffers.
type Reconciler struct {
	Client client.Client
	caches sync.Map
}

func (r *Reconciler) getOrCreateCache(cccName string) *cccInFlightCache {
	if val, exists := r.caches.Load(cccName); exists {
		return val.(*cccInFlightCache)
	}
	c := newCCCCache()
	actual, _ := r.caches.LoadOrStore(cccName, c)
	return actual.(*cccInFlightCache)
}

func (r *Reconciler) deleteCache(cccName string) {
	r.caches.Delete(cccName)
}

// SetupWithManager registers the Reconciler and dependent watches with the controller manager.
func SetupWithManager(mgr ctrl.Manager) error {
	registerCapacityBuffersCollector(mgr.GetClient())

	c, err := newJSONClient(mgr.GetConfig(), client.Options{
		HTTPClient: mgr.GetHTTPClient(),
		Scheme:     mgr.GetScheme(),
		Mapper:     mgr.GetRESTMapper(),
		Cache:      &client.CacheOptions{Reader: mgr.GetCache()},
	})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&ccapiv1.ComputeClass{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&cbv1beta1.CapacityBuffer{},
			handler.EnqueueRequestsFromMapFunc(enqueueRequestsForOwner),
		).
		Watches(
			&corev1.PodTemplate{},
			handler.EnqueueRequestsFromMapFunc(enqueueRequestsForOwner),
		).
		Complete(&Reconciler{Client: c})
}

// newJSONClient returns a client that encodes request bodies as JSON. CA's rest config
// takes its ContentType from --kube-api-content-type (protobuf by default), and objects
// without protobuf support, such as CapacityBuffer, can't be encoded with it on Create.
func newJSONClient(cfg *rest.Config, opts client.Options) (client.Client, error) {
	cfg = rest.CopyConfig(cfg)
	cfg.ContentType = runtime.ContentTypeJSON
	return client.New(cfg, opts)
}

func enqueueRequestsForOwner(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj.GetNamespace() != NamespaceGkeManagedCCC {
		return nil
	}
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == kindComputeClass {
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{Name: ref.Name}},
			}
		}
	}
	return nil
}

// Reconcile drives the desired capacity buffers and pod templates from the ComputeClass specification.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	startTime := time.Now()
	res, err := r.reconcile(ctx, req)
	status := "success"
	if err != nil {
		status = "error"
	}
	recordReconcile(status, time.Since(startTime))
	return res, err
}

func (r *Reconciler) reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var cc ccapiv1.ComputeClass
	err := r.Client.Get(ctx, req.NamespacedName, &cc)
	if apierrors.IsNotFound(err) {
		r.deleteCache(req.Name)
		return reconcile.Result{}, nil
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	// if the CCC is in the process of being deleted, ignore it
	if cc.DeletionTimestamp != nil {
		r.deleteCache(req.Name)
		klog.Infof("%s ComputeClass %s is marked for deletion; ignoring", logPrefix, cc.Name)
		return reconcile.Result{}, nil
	}

	// Default missing or empty provisioning strategies and reject duplicate strategies.
	seenStrategies := make(map[string]struct{}, len(cc.Spec.Buffers))
	for i := range cc.Spec.Buffers {
		if cc.Spec.Buffers[i].ProvisioningStrategy == "" {
			cc.Spec.Buffers[i].ProvisioningStrategy = DefaultProvisioningStrategy()
		}
		strategy := cc.Spec.Buffers[i].ProvisioningStrategy
		if _, exists := seenStrategies[strategy]; exists {
			msg := fmt.Sprintf("Each provisioning strategy can only be specified once across capacity buffers (duplicate: %q)", strategy)
			klog.Warningf("%s %s in ComputeClass %s", logPrefix, msg, cc.Name)
			return reconcile.Result{}, r.updateStatus(
				ctx,
				&cc,
				metav1.ConditionFalse,
				"DuplicateProvisioningStrategy",
				msg,
				cc.Status.ObservedBuffers,
			)
		}
		seenStrategies[strategy] = struct{}{}
	}

	// 1. Apply required PodTemplates
	expectedPTNames, err := r.applyRequiredTemplates(ctx, &cc)
	if err != nil {
		klog.Errorf("%s applyRequiredTemplates failed for %s: %v", logPrefix, cc.Name, err)
		r.recordReconcileFailure(ctx, &cc, err)
		return reconcile.Result{}, err
	}

	// 2. Reconcile CapacityBuffers
	activeCB, deleteFailed, bufErr := r.reconcileBuffers(ctx, &cc)
	if bufErr != nil {
		klog.Errorf("%s reconcileBuffers failed for %s: %v", logPrefix, cc.Name, bufErr)
		if activeCB == nil {
			r.recordReconcileFailure(ctx, &cc, bufErr)
			return reconcile.Result{}, bufErr
		}
	}

	// 3. Prune unneeded PodTemplates (only when all buffer deletions succeeded)
	if !deleteFailed {
		if err := r.pruneUnneededTemplates(ctx, &cc, expectedPTNames); err != nil {
			klog.Warningf("%s pruneUnneededTemplates failed for %s: %v", logPrefix, cc.Name, err)
		}
	}

	// 4. Update status
	if statusErr := r.reconcileStatus(ctx, &cc, activeCB, bufErr); statusErr != nil {
		klog.Errorf("%s reconcileStatus failed for %s: %v", logPrefix, cc.Name, statusErr)
		return reconcile.Result{}, errors.Join(bufErr, statusErr)
	}
	if bufErr != nil {
		return reconcile.Result{}, bufErr
	}

	// An in-flight create stands in for its buffer until it's listed or expires. If the buffer
	// is deleted before any List sees it, its events can arrive while the record still blocks
	// a replacement, or not at all, so run one more pass after the record expires.
	// Count records that expired during this pass too: they may have blocked a create in it.
	if len(r.getOrCreateCache(cc.Name).bufferCreateRequests) > 0 {
		return reconcile.Result{RequeueAfter: defaultInFlightTTL}, nil
	}
	return reconcile.Result{}, nil
}

func (r *Reconciler) recordReconcileFailure(ctx context.Context, cc *ccapiv1.ComputeClass, err error) {
	if !r.shouldRecordReconcileFailure(ctx, cc) {
		return
	}
	if updateErr := r.updateStatus(ctx, cc, metav1.ConditionFalse, "ReconcileFailed", sanitizeReconcileError(err), cc.Status.ObservedBuffers); updateErr != nil {
		klog.Warningf("%s Failed to update status on reconcile failure for %s: %v", logPrefix, cc.Name, updateErr)
	}
}

func (r *Reconciler) shouldRecordReconcileFailure(ctx context.Context, cc *ccapiv1.ComputeClass) bool {
	if len(cc.Spec.Buffers) > 0 ||
		len(cc.Status.ObservedBuffers) > 0 ||
		meta.FindStatusCondition(cc.Status.Conditions, conditionTypeBuffersConfigured) != nil {
		return true
	}
	var cbList cbv1beta1.CapacityBufferList
	if err := r.Client.List(ctx, &cbList, client.InNamespace(NamespaceGkeManagedCCC), client.MatchingLabels{gkelabels.ComputeClassLabel: cc.Name}); err == nil {
		for i := range cbList.Items {
			if cbList.Items[i].GetDeletionTimestamp() == nil && hasOwnerReferenceUID(&cbList.Items[i], cc.UID) {
				return true
			}
		}
	}
	var ptList corev1.PodTemplateList
	if err := r.Client.List(ctx, &ptList, client.InNamespace(NamespaceGkeManagedCCC), client.MatchingLabels{gkelabels.ComputeClassLabel: cc.Name}); err == nil {
		for i := range ptList.Items {
			if ptList.Items[i].GetDeletionTimestamp() == nil && hasOwnerReferenceUID(&ptList.Items[i], cc.UID) {
				return true
			}
		}
	}
	return false
}

func sanitizeReconcileError(err error) string {
	errs := flattenErrors(err)
	switch {
	case slices.ContainsFunc(errs, func(e error) bool { return apierrors.IsInvalid(e) || apierrors.IsBadRequest(e) }):
		return "Failed to reconcile capacity buffers: invalid resource configuration"
	case slices.ContainsFunc(errs, func(e error) bool { return apierrors.IsForbidden(e) || apierrors.IsUnauthorized(e) }):
		return "Failed to reconcile capacity buffers: insufficient permissions or quota"
	case slices.ContainsFunc(errs, func(e error) bool {
		return apierrors.IsTimeout(e) || apierrors.IsServerTimeout(e) || apierrors.IsTooManyRequests(e) || apierrors.IsServiceUnavailable(e)
	}):
		return "Failed to reconcile capacity buffers: API server temporarily unavailable"
	case slices.ContainsFunc(errs, apierrors.IsConflict):
		return "Failed to reconcile capacity buffers: resource update conflict"
	default:
		return "Failed to reconcile capacity buffers: internal error"
	}
}

func flattenErrors(err error) []error {
	if err == nil {
		return nil
	}
	if u, ok := err.(interface{ Unwrap() []error }); ok {
		var out []error
		for _, child := range u.Unwrap() {
			out = append(out, flattenErrors(child)...)
		}
		return out
	}
	return []error{err}
}

// --------------------
// PT related functions
// --------------------

// applyRequiredTemplates creates and updates PodTemplates required by the ComputeClass specification.
// Returns the names of the required pod templates. Strategies that share a pod template apply it once.
func (r *Reconciler) applyRequiredTemplates(
	ctx context.Context,
	cc *ccapiv1.ComputeClass,
) (sets.Set[string], error) {
	expectedPTNames := sets.New[string]()
	var errs []error
	for _, buf := range cc.Spec.Buffers {
		strategy := buf.ProvisioningStrategy
		ptName, err := podTemplateName(cc.Name, strategy)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if expectedPTNames.Has(ptName) {
			continue
		}
		expectedPTNames.Insert(ptName)
		if err := r.applyPodTemplateSSA(ctx, cc, strategy); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return expectedPTNames, nil
}

// pruneUnneededTemplates deletes any PodTemplates owned by this ComputeClass that are no longer required.
func (r *Reconciler) pruneUnneededTemplates(
	ctx context.Context,
	cc *ccapiv1.ComputeClass,
	expectedPTNames sets.Set[string],
) error {
	var ptList corev1.PodTemplateList
	if err := r.Client.List(ctx, &ptList, client.InNamespace(NamespaceGkeManagedCCC), client.MatchingLabels{gkelabels.ComputeClassLabel: cc.Name}); err != nil {
		return err
	}

	for i := range ptList.Items {
		pt := &ptList.Items[i]
		if pt.GetDeletionTimestamp() != nil || !hasOwnerReferenceUID(pt, cc.UID) {
			continue
		}
		if !expectedPTNames.Has(pt.Name) {
			if err := r.Client.Delete(ctx, pt); err != nil && !apierrors.IsNotFound(err) {
				klog.Warningf("%s Failed to clean up unneeded PodTemplate %s: %v", logPrefix, pt.Name, err)
			}
		}
	}

	return nil
}

func (r *Reconciler) applyPodTemplateSSA(ctx context.Context, cc *ccapiv1.ComputeClass, strategy string) error {
	applyConfig, err := buildPodTemplateSSAConfig(cc, strategy)
	if err != nil {
		return err
	}
	patchBytes, err := json.Marshal(applyConfig)
	if err != nil {
		return fmt.Errorf("marshaling PodTemplate SSA config: %w", err)
	}
	ptObj := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *applyConfig.Name,
			Namespace: NamespaceGkeManagedCCC,
		},
	}
	patch := client.RawPatch(types.ApplyPatchType, patchBytes)
	return r.Client.Patch(ctx, ptObj, patch, client.FieldOwner(fieldOwnerCCC), client.ForceOwnership)
}

// --------------------
// CB related functions
// --------------------

// reconcileBuffers creates, applies, and cleans up CapacityBuffers using BufferMatchPair.
func (r *Reconciler) reconcileBuffers(
	ctx context.Context,
	cc *ccapiv1.ComputeClass,
) ([]*cbv1beta1.CapacityBuffer, bool, error) {
	cccCache := r.getOrCreateCache(cc.Name)
	cccCache.PruneExpired()

	// list all buffers owned by the cc that are not in the process of being deleted
	var cbList cbv1beta1.CapacityBufferList
	if err := r.Client.List(ctx, &cbList, client.InNamespace(NamespaceGkeManagedCCC), client.MatchingLabels{gkelabels.ComputeClassLabel: cc.Name}); err != nil {
		return nil, false, err
	}

	actualBuffers := make([]*cbv1beta1.CapacityBuffer, 0, len(cbList.Items))
	for i := range cbList.Items {
		cb := &cbList.Items[i]
		// A listed buffer ends its in-flight record even if it's skipped below, so it can be replaced.
		cccCache.DropBufferCreate(cb.Name)
		if cb.GetDeletionTimestamp() != nil || !hasOwnerReferenceUID(cb, cc.UID) || cccCache.HasRecentBufferDelete(cb.GetUID()) {
			continue
		}
		actualBuffers = append(actualBuffers, cb)
	}

	// construct the candidate list from actual buffers and those whose creation was recently registered
	pendingBuffers := cccCache.GetPendingBuffers()
	candidates := buildCandidateRepresentations(actualBuffers, pendingBuffers)

	// match buffers and pending buffers to spec
	matchedBufferSpecs := matchCapacityBuffers(cc.Spec.Buffers, candidates)

	nameToActualBuffer := make(map[string]*cbv1beta1.CapacityBuffer, len(actualBuffers))
	for _, cb := range actualBuffers {
		nameToActualBuffer[cb.Name] = cb
	}

	var errs []error
	var deleteFailed bool
	activeCB := make([]*cbv1beta1.CapacityBuffer, 0, len(matchedBufferSpecs))
	for _, pair := range matchedBufferSpecs {
		cb, pairDeleteFailed, err := r.reconcileBufferPair(ctx, cc, cccCache, nameToActualBuffer, pair)
		if pairDeleteFailed {
			deleteFailed = true
		}
		if err != nil {
			errs = append(errs, err)
		}
		if cb != nil {
			activeCB = append(activeCB, cb)
		}
	}

	return activeCB, deleteFailed, errors.Join(errs...)
}

func (r *Reconciler) reconcileBufferPair(
	ctx context.Context,
	cc *ccapiv1.ComputeClass,
	cccCache *cccInFlightCache,
	nameToActualBuffer map[string]*cbv1beta1.CapacityBuffer,
	pair bufferMatchPair,
) (*cbv1beta1.CapacityBuffer, bool, error) {
	if pair.Desired == nil {
		// Case 1: desired is nil, actual is not nil -> delete/evict
		if err := r.handleBufferDelete(ctx, cccCache, nameToActualBuffer, pair.Actual); err != nil {
			if pair.Actual != nil {
				if liveCB := nameToActualBuffer[pair.Actual.Name]; liveCB != nil {
					failedCB := liveCB.DeepCopy()
					setBufferReconcileFailedCondition(failedCB, err)
					return failedCB, true, err
				}
			}
			return nil, true, err
		}
		return nil, false, nil
	}

	targetPTName, err := podTemplateName(cc.Name, pair.Desired.ProvisioningStrategy)
	if err != nil {
		return nil, false, err
	}

	if pair.Actual == nil {
		// Case 2: desired is not nil, actual is nil -> Create new buffer
		cb, err := r.handleBufferCreate(ctx, cc, cccCache, *pair.Desired, targetPTName)
		return cb, false, err
	}

	// Case 3: desired is not nil, actual is not nil -> Verify conformance or Update in place
	cb, err := r.handleBufferUpdate(ctx, cc, nameToActualBuffer, pair, targetPTName)
	return cb, false, err
}

func setBufferReconcileFailedCondition(cb *cbv1beta1.CapacityBuffer, err error) {
	meta.SetStatusCondition(&cb.Status.Conditions, metav1.Condition{
		Type:    capacitybuffer.ReadyForProvisioningCondition,
		Status:  metav1.ConditionFalse,
		Reason:  "ReconcileFailed",
		Message: sanitizeReconcileError(err),
	})
}

// buildCandidateRepresentations merges observed cluster state with in-flight creates into a unified representation pool.
func buildCandidateRepresentations(validCBs []*cbv1beta1.CapacityBuffer, inFlightReps []internalCapacityBuffer) []internalCapacityBuffer {
	candidates := make([]internalCapacityBuffer, 0, len(validCBs)+len(inFlightReps))
	for _, cb := range validCBs {
		candidates = append(candidates, buildInternalCapacityBuffer(cb))
	}
	candidates = append(candidates, inFlightReps...)
	return candidates
}

func (r *Reconciler) handleBufferCreate(
	ctx context.Context,
	cc *ccapiv1.ComputeClass,
	cccCache *cccInFlightCache,
	targetBufferSpec ccapiv1.ComputeClassBuffer,
	targetPTName string,
) (*cbv1beta1.CapacityBuffer, error) {
	newCB := buildCapacityBufferForCreate(cc, targetBufferSpec, targetPTName)
	if err := r.Client.Create(ctx, newCB); err != nil {
		return nil, err
	}
	cccCache.RecordBufferCreate(newCB)
	return newCB, nil
}

func (r *Reconciler) handleBufferDelete(
	ctx context.Context,
	cccCache *cccInFlightCache,
	nameToLiveBuffer map[string]*cbv1beta1.CapacityBuffer,
	actual *internalCapacityBuffer,
) error {
	if actual == nil {
		return nil
	}
	buf := nameToLiveBuffer[actual.Name]
	if buf == nil {
		buf = capacityBufferStub(actual.Name, actual.UID)
	}
	if err := deleteCapacityBuffer(ctx, r.Client, cccCache, buf); err != nil {
		klog.Errorf("%s Failed to delete CapacityBuffer %s: %v", logPrefix, buf.Name, err)
		return err
	}
	return nil
}

func (r *Reconciler) handleBufferUpdate(
	ctx context.Context,
	cc *ccapiv1.ComputeClass,
	liveByName map[string]*cbv1beta1.CapacityBuffer,
	pair bufferMatchPair,
	targetPTName string,
) (*cbv1beta1.CapacityBuffer, error) {
	if pair.Desired == nil || pair.Actual == nil {
		return nil, fmt.Errorf("invalid buffer match pair: desired or actual is nil")
	}

	expectedCB := buildCapacityBuffer(cc, *pair.Desired, targetPTName, pair.Actual.Name, pair.Actual.UID)

	if pair.Actual.PendingCreation {
		return expectedCB, nil
	}

	cb := liveByName[pair.Actual.Name]
	if cb != nil && cbFullyConforms(cb, cc, *pair.Desired, targetPTName) {
		return cb.DeepCopy(), nil
	}

	if err := r.patchCapacityBuffer(ctx, cc, cb, *pair.Desired, targetPTName); err != nil {
		if cb != nil {
			expectedCB.Status = *cb.Status.DeepCopy()
		}
		setBufferReconcileFailedCondition(expectedCB, err)
		return expectedCB, err
	}
	expectedCB.Status = *cb.Status.DeepCopy()

	return expectedCB, nil
}

// deleteCapacityBuffer issues client.Delete against a CapacityBuffer,
// and records its UID in cccCache if deleted or already gone.
func deleteCapacityBuffer(
	ctx context.Context,
	c client.Client,
	cccCache *cccInFlightCache,
	cb *cbv1beta1.CapacityBuffer,
) error {
	if err := c.Delete(ctx, cb); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	cccCache.RecordBufferDelete(cb.UID, cb.Name)
	return nil
}

// patchCapacityBuffer sets the controller's labels, ProvisioningStrategy,
// PodTemplateRef and Limits on cb with one merge patch. client.MergeFrom
// nulls Limits keys that are no longer desired; an apply can't remove keys
// that the Create also owns.
func (r *Reconciler) patchCapacityBuffer(ctx context.Context, cc *ccapiv1.ComputeClass, cb *cbv1beta1.CapacityBuffer, buf ccapiv1.ComputeClassBuffer, targetPTName string) error {
	if cb == nil {
		return fmt.Errorf("no live CapacityBuffer to patch")
	}
	patched := cb.DeepCopy()
	if patched.Labels == nil {
		patched.Labels = make(map[string]string)
	}
	for k, v := range commonBufferLabels(cc.Name) {
		patched.Labels[k] = v
	}
	strategy := buf.ProvisioningStrategy
	limits := toCBResourceList(buf.Limits)
	patched.Spec.ProvisioningStrategy = &strategy
	patched.Spec.PodTemplateRef = &cbv1beta1.LocalObjectRef{Name: targetPTName}
	patched.Spec.Limits = &limits
	return r.Client.Patch(ctx, patched, client.MergeFrom(cb), client.FieldOwner(fieldOwnerCCC))
}

// ------------------------
// status related functions
// ------------------------

// reconcileStatus inspects active capacity buffers and updates the ComputeClass status.
func (r *Reconciler) reconcileStatus(ctx context.Context, cc *ccapiv1.ComputeClass, activeCB []*cbv1beta1.CapacityBuffer, bufErr error) error {
	if len(cc.Spec.Buffers) == 0 && bufErr == nil {
		return r.updateStatus(ctx, cc, metav1.ConditionTrue, "", "", nil)
	}
	oldObservedMap := make(map[string]ccapiv1.ObservedBufferStatus, len(cc.Status.ObservedBuffers))
	for _, obs := range cc.Status.ObservedBuffers {
		if obs.Name != "" {
			oldObservedMap[obs.Name] = obs
		}
	}
	observedSlice := make([]ccapiv1.ObservedBufferStatus, 0, len(activeCB))
	var messages, failureReasons []string
	var reconcileFailedCount int
	for _, cb := range activeCB {
		if cb == nil {
			continue
		}
		obs := buildObservedBuffer(cb, oldObservedMap[cb.Name])
		observedSlice = append(observedSlice, obs)
		if cond := meta.FindStatusCondition(obs.Conditions, capacitybuffer.ReadyForProvisioningCondition); cond != nil && cond.Status == metav1.ConditionFalse {
			if cond.Reason == "ReconcileFailed" {
				reconcileFailedCount++
			}
			if cond.Reason != "Pending" && cond.Reason != "" {
				failureReasons = append(failureReasons, cond.Reason)
			}
			messages = append(messages, fmt.Sprintf("Buffer %q: %s", obs.Name, cond.Message))
		} else if provCond := meta.FindStatusCondition(obs.Conditions, capacitybuffer.ProvisioningCondition); provCond != nil && provCond.Status == metav1.ConditionFalse {
			if provCond.Reason != "Pending" && provCond.Reason != "" {
				failureReasons = append(failureReasons, provCond.Reason)
			}
			messages = append(messages, fmt.Sprintf("Buffer %q: %s", obs.Name, provCond.Message))
		}
	}

	if bufErr != nil && reconcileFailedCount < len(flattenErrors(bufErr)) {
		failureReasons = append(failureReasons, "ReconcileFailed")
		messages = append(messages, sanitizeReconcileError(bufErr))
	}

	if len(messages) == 0 {
		return r.updateStatus(ctx, cc, metav1.ConditionTrue, "Ready", "All capacity buffers configured and ready for provisioning", observedSlice)
	}

	reason := "Pending"
	if len(failureReasons) == 1 {
		reason = failureReasons[0]
	} else if len(failureReasons) > 1 {
		reason = "BufferProvisioningFailed"
	}
	return r.updateStatus(ctx, cc, metav1.ConditionFalse, reason, strings.Join(messages, "; "), observedSlice)
}

func buildObservedBuffer(cb *cbv1beta1.CapacityBuffer, oldObserved ccapiv1.ObservedBufferStatus) ccapiv1.ObservedBufferStatus {
	if cb == nil {
		return oldObserved
	}
	strategy := getCapacityBufferStrategy(cb)
	// 1. Custom strategy: surface CustomProvisioningStrategy = True (and ReadyForProvisioning = False if reconcile failed)
	if !isSupportedProvisioningStrategy(strategy) {
		var conditions []metav1.Condition
		if oldCustom := meta.FindStatusCondition(oldObserved.Conditions, "CustomProvisioningStrategy"); oldCustom != nil {
			conditions = append(conditions, *oldCustom)
		}
		meta.SetStatusCondition(&conditions, metav1.Condition{
			Type:    "CustomProvisioningStrategy",
			Status:  metav1.ConditionTrue,
			Reason:  "CustomProvisioningStrategy",
			Message: fmt.Sprintf("Buffer uses custom provisioning strategy %q", strategy),
		})
		if cond := meta.FindStatusCondition(cb.Status.Conditions, capacitybuffer.ReadyForProvisioningCondition); cond != nil && cond.Status == metav1.ConditionFalse {
			if oldReady := meta.FindStatusCondition(oldObserved.Conditions, capacitybuffer.ReadyForProvisioningCondition); oldReady != nil {
				conditions = append(conditions, *oldReady)
			}
			meta.SetStatusCondition(&conditions, *cond)
		}
		return ccapiv1.ObservedBufferStatus{
			Name:       cb.Name,
			Namespace:  NamespaceGkeManagedCCC,
			Conditions: conditions,
		}
	}
	// 2. Built-in strategy with existing ReadyForProvisioning condition (and optional Provisioning condition)
	if cond := meta.FindStatusCondition(cb.Status.Conditions, capacitybuffer.ReadyForProvisioningCondition); cond != nil {
		var conditions []metav1.Condition
		if cond.Reason == "ReconcileFailed" {
			if oldReady := meta.FindStatusCondition(oldObserved.Conditions, capacitybuffer.ReadyForProvisioningCondition); oldReady != nil {
				conditions = append(conditions, *oldReady)
			}
			meta.SetStatusCondition(&conditions, *cond)
		} else {
			conditions = append(conditions, *cond)
		}
		if provCond := meta.FindStatusCondition(cb.Status.Conditions, capacitybuffer.ProvisioningCondition); provCond != nil {
			conditions = append(conditions, *provCond)
		}
		return ccapiv1.ObservedBufferStatus{
			Name:       cb.Name,
			Namespace:  NamespaceGkeManagedCCC,
			Conditions: conditions,
		}
	}
	// 3. Built-in strategy initializing (missing ReadyForProvisioning condition)
	var conditions []metav1.Condition
	if oldReady := meta.FindStatusCondition(oldObserved.Conditions, capacitybuffer.ReadyForProvisioningCondition); oldReady != nil {
		conditions = append(conditions, *oldReady)
	}
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type:    capacitybuffer.ReadyForProvisioningCondition,
		Status:  metav1.ConditionFalse,
		Reason:  "Pending",
		Message: "CapacityBuffer is initializing",
	})
	if provCond := meta.FindStatusCondition(cb.Status.Conditions, capacitybuffer.ProvisioningCondition); provCond != nil {
		conditions = append(conditions, *provCond)
	}
	return ccapiv1.ObservedBufferStatus{
		Name:       cb.Name,
		Namespace:  NamespaceGkeManagedCCC,
		Conditions: conditions,
	}
}

// isObservedBuffersEqual checks if two ObservedBufferStatus slices contain equivalent statuses,
// using Name matching to ignore slice ordering differences.
func isObservedBuffersEqual(oldSlice, newSlice []ccapiv1.ObservedBufferStatus) bool {
	if len(oldSlice) != len(newSlice) {
		return false
	}
	oldMap := make(map[string]ccapiv1.ObservedBufferStatus, len(oldSlice))
	for _, obs := range oldSlice {
		oldMap[obs.Name] = obs
	}
	for _, newObs := range newSlice {
		oldObs, exists := oldMap[newObs.Name]
		if !exists {
			return false
		}
		if !equality.Semantic.DeepEqual(oldObs, newObs) {
			return false
		}
	}
	return true
}

func (r *Reconciler) updateStatus(
	ctx context.Context,
	cc *ccapiv1.ComputeClass,
	status metav1.ConditionStatus,
	reason, message string,
	observedSlice []ccapiv1.ObservedBufferStatus,
) error {
	var newConds []metav1.Condition
	clearCondition := len(cc.Spec.Buffers) == 0 && status == metav1.ConditionTrue
	if clearCondition {
		newConds = removeBufferConditions(cc.Status.Conditions)
	} else {
		newConds = computeUpdatedConditions(cc.Status.Conditions, status, reason, message, cc.Generation)
	}

	if equality.Semantic.DeepEqual(cc.Status.Conditions, newConds) && isObservedBuffersEqual(cc.Status.ObservedBuffers, observedSlice) {
		return nil
	}

	statusApply := ccapiv1ac.ComputeClassStatus()
	for _, obs := range observedSlice {
		obsApply := ccapiv1ac.ObservedBufferStatus().
			WithName(obs.Name).
			WithNamespace(obs.Namespace)
		for _, cond := range obs.Conditions {
			condApply := metav1ac.Condition().
				WithType(cond.Type).
				WithStatus(cond.Status).
				WithReason(cond.Reason).
				WithMessage(cond.Message).
				WithLastTransitionTime(cond.LastTransitionTime).
				WithObservedGeneration(cond.ObservedGeneration)
			obsApply.WithConditions(condApply)
		}
		statusApply.WithObservedBuffers(obsApply)
	}
	// ComputeClassStatus.Conditions is an associative map list keyed by type (+listType=map, +listMapKey=type).
	// We only include BuffersConfigured in the SSA payload so fieldOwnerCCC manages solely that condition;
	// omitting it when clearCondition is true causes SSA to remove our owned BuffersConfigured entry.
	if !clearCondition {
		for _, cond := range newConds {
			if cond.Type == conditionTypeBuffersConfigured {
				condApply := metav1ac.Condition().
					WithType(cond.Type).
					WithStatus(cond.Status).
					WithReason(cond.Reason).
					WithMessage(cond.Message).
					WithLastTransitionTime(cond.LastTransitionTime).
					WithObservedGeneration(cond.ObservedGeneration)
				statusApply.WithConditions(condApply)
			}
		}
	}

	ccApply := ccapiv1ac.ComputeClass(cc.Name).WithStatus(statusApply)
	patchBytes, err := json.Marshal(ccApply)
	if err != nil {
		return fmt.Errorf("marshaling ComputeClass status SSA config: %w", err)
	}
	ccObj := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: cc.Name,
		},
	}
	patch := client.RawPatch(types.ApplyPatchType, patchBytes)
	return r.Client.Status().Patch(ctx, ccObj, patch, client.FieldOwner(fieldOwnerCCC), client.ForceOwnership)
}
