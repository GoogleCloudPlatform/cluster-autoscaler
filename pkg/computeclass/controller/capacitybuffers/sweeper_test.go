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

package capacitybuffers

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	ccapiv1ac "github.com/googlecloudplatform/compute-class-api/client/applyconfiguration/cloud.google.com/v1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestEnqueueInitialSweep(t *testing.T) {
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()

	if err := enqueueInitialSweep(t.Context(), q); err != nil {
		t.Fatalf("enqueueInitialSweep() error = %v, want nil", err)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("q.Len() = %d, want 1", got)
	}
	got, _ := q.Get()
	want := reconcile.Request{NamespacedName: types.NamespacedName{Name: NamespaceGkeManagedCCC}}
	if got != want {
		t.Errorf("q.Get() = %+v, want %+v", got, want)
	}
}

// newFakeClient returns a fake client that serves objs, with status subresources for ComputeClasses
// and CapacityBuffers, and runs funcs on its calls.
func newFakeClient(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, addToScheme := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, ccapiv1.AddToScheme, cbv1beta1.AddToScheme} {
		if err := addToScheme(scheme); err != nil {
			t.Fatalf("Failed to build scheme: %v", err)
		}
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ccapiv1.ComputeClass{}, &cbv1beta1.CapacityBuffer{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
}

func applyConfigPatch(t *testing.T, cfg *ccapiv1ac.ComputeClassApplyConfiguration) client.Patch {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Failed to marshal ComputeClass apply configuration: %v", err)
	}
	return client.RawPatch(types.ApplyPatchType, data)
}

func seedBufferStatus(t *testing.T, c client.Client, ccName string) {
	t.Helper()
	ctx := t.Context()
	now := metav1.NewTime(time.Now())
	ccObj := &ccapiv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: ccName}}

	bufApply := ccapiv1ac.ComputeClass(ccName).WithStatus(
		ccapiv1ac.ComputeClassStatus().
			WithConditions(
				metav1ac.Condition().
					WithType(conditionTypeBuffersConfigured).
					WithStatus(metav1.ConditionTrue).
					WithReason("Ready").
					WithMessage("All capacity buffers configured and ready for provisioning").
					WithLastTransitionTime(now).
					WithObservedGeneration(1),
			).
			WithObservedBuffers(
				ccapiv1ac.ObservedBufferStatus().
					WithName("cb-owned").
					WithNamespace(NamespaceGkeManagedCCC).
					WithConditions(
						metav1ac.Condition().
							WithType(capacitybuffer.ReadyForProvisioningCondition).
							WithStatus(metav1.ConditionTrue).
							WithReason("Ready").
							WithMessage("ready").
							WithLastTransitionTime(now),
					),
			),
	)
	if err := c.Status().Patch(ctx, ccObj, applyConfigPatch(t, bufApply), client.FieldOwner(fieldOwnerCCC), client.ForceOwnership); err != nil {
		t.Fatalf("Failed to apply buffer status to %s: %v", ccName, err)
	}
}

func TestSweeper_DeletesAllObjectsClearsBufferStatusAndZeroesMetrics(t *testing.T) {
	activeStrat := activeCapacityStrategy
	standbyStrat := standbyCapacityStrategy

	cbOwned := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-owned",
			Namespace: NamespaceGkeManagedCCC,
			Labels:    commonBufferLabels("cc-with-buffers"),
		},
		Spec: cbv1beta1.CapacityBufferSpec{ProvisioningStrategy: &activeStrat},
	}
	cbOrphan := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-orphan",
			Namespace: NamespaceGkeManagedCCC,
			Labels:    commonBufferLabels("deleted-cc"),
		},
		Spec: cbv1beta1.CapacityBufferSpec{ProvisioningStrategy: &standbyStrat},
	}
	ptOwned := ownedPT("cc-with-buffers", "uid-1", "pt-basic-cc-with-buffers")
	ptUnlabeled := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "capacitybuffer-cb-orphan", Namespace: NamespaceGkeManagedCCC},
	}
	ptOtherNS := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "user-pt", Namespace: "default"},
	}
	ccWithBuffers := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc-with-buffers", UID: "uid-1", Generation: 1},
	}
	ccClean := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc-clean", UID: "uid-2", Generation: 1},
		Status: ccapiv1.ComputeClassStatus{
			Conditions: []metav1.Condition{{Type: "Healthy", Status: metav1.ConditionTrue, Reason: "Healthy"}},
		},
	}

	var statusPatchedCCs []string
	c := newFakeClient(t, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, cl client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			statusPatchedCCs = append(statusPatchedCCs, obj.GetName())
			return cl.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	}, cbOwned, cbOrphan, &ptOwned, ptUnlabeled, ptOtherNS, ccWithBuffers, ccClean)

	seedBufferStatus(t, c, ccWithBuffers.Name)
	statusPatchedCCs = nil

	sweeper := &Sweeper{Client: c}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: NamespaceGkeManagedCCC}}
	res, err := sweeper.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Sweeper.Reconcile() error = %v, want nil", err)
	}
	if want := (reconcile.Result{RequeueAfter: sweepInterval}); res != want {
		t.Errorf("Sweeper.Reconcile() result = %+v, want %+v", res, want)
	}

	var cbList cbv1beta1.CapacityBufferList
	if err := c.List(t.Context(), &cbList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil || len(cbList.Items) != 0 {
		t.Errorf("CapacityBuffers in %s after the sweep = %d (err=%v), want 0", NamespaceGkeManagedCCC, len(cbList.Items), err)
	}
	var ptList corev1.PodTemplateList
	if err := c.List(t.Context(), &ptList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil || len(ptList.Items) != 0 {
		t.Errorf("PodTemplates in %s after the sweep = %d (err=%v), want 0", NamespaceGkeManagedCCC, len(ptList.Items), err)
	}
	var keptPT corev1.PodTemplate
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "user-pt"}, &keptPT); err != nil {
		t.Errorf("Get(default/user-pt) after the sweep error = %v, want the default-namespace PodTemplate intact", err)
	}

	if want := []string{"cc-with-buffers"}; !slices.Equal(statusPatchedCCs, want) {
		t.Errorf("status-patched ComputeClasses = %v, want %v", statusPatchedCCs, want)
	}
	var updatedCC ccapiv1.ComputeClass
	if err := c.Get(t.Context(), types.NamespacedName{Name: "cc-with-buffers"}, &updatedCC); err != nil {
		t.Fatalf("Failed to get cc-with-buffers: %v", err)
	}
	if cond := meta.FindStatusCondition(updatedCC.Status.Conditions, conditionTypeBuffersConfigured); cond != nil {
		t.Errorf("cc-with-buffers BuffersConfigured condition = %+v, want none", cond)
	}
	if len(updatedCC.Status.ObservedBuffers) != 0 {
		t.Errorf("cc-with-buffers ObservedBuffers = %+v, want none", updatedCC.Status.ObservedBuffers)
	}
	var keptCleanCC ccapiv1.ComputeClass
	if err := c.Get(t.Context(), types.NamespacedName{Name: "cc-clean"}, &keptCleanCC); err != nil {
		t.Fatalf("Failed to get cc-clean: %v", err)
	}
	if healthy := meta.FindStatusCondition(keptCleanCC.Status.Conditions, "Healthy"); healthy == nil || healthy.Status != metav1.ConditionTrue {
		t.Errorf("cc-clean Healthy condition = %+v, want it untouched (True)", healthy)
	}

	// Verify capacityBuffersCollector reports 0 across all 12 metric buckets after the sweep.
	if err := checkCollectorValues(&capacityBuffersCollector{client: c}, nil); err != nil {
		t.Errorf("capacityBuffersCollector after the sweep: %v", err)
	}
}

// checkCollectorValues returns an error unless collector reports one sample for every provisioning
// strategy and status, with the value in want, or 0 for the pairs that want leaves out.
func checkCollectorValues(collector *capacityBuffersCollector, want map[bufferMetricKey]float64) error {
	wantAll := make(map[bufferMetricKey]float64, len(knownBufferMetricStrategies)*len(knownBufferMetricStatuses))
	for _, strategy := range knownBufferMetricStrategies {
		for _, status := range knownBufferMetricStatuses {
			wantAll[bufferMetricKey{strategy: strategy, status: status}] = 0
		}
	}
	for key, value := range want {
		if _, ok := wantAll[key]; !ok {
			return fmt.Errorf("want has unknown sample %+v", key)
		}
		wantAll[key] = value
	}

	ch := make(chan prometheus.Metric, len(wantAll))
	collector.Collect(ch)
	close(ch)
	got := make(map[bufferMetricKey]float64, len(wantAll))
	var samples int
	for m := range ch {
		samples++
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			return fmt.Errorf("reading metric: %w", err)
		}
		var key bufferMetricKey
		for _, label := range pb.GetLabel() {
			switch label.GetName() {
			case "provisioning_strategy":
				key.strategy = label.GetValue()
			case "status":
				key.status = label.GetValue()
			}
		}
		got[key] = pb.GetGauge().GetValue()
	}
	if samples != len(wantAll) || !maps.Equal(got, wantAll) {
		return fmt.Errorf("collected %d samples %v, want %d samples %v", samples, got, len(wantAll), wantAll)
	}
	return nil
}

func TestSweeper_CapacityBufferDeleteFailureGatesTemplatesAndStatus(t *testing.T) {
	cb1 := &cbv1beta1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{Name: "cb-1", Namespace: NamespaceGkeManagedCCC}}
	cb2 := &cbv1beta1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{Name: "cb-2", Namespace: NamespaceGkeManagedCCC}}
	pt1 := &corev1.PodTemplate{ObjectMeta: metav1.ObjectMeta{Name: "pt-1", Namespace: NamespaceGkeManagedCCC}}
	cc := &ccapiv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "test-cc"}}

	failCB1 := true
	c := newFakeClient(t, interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if failCB1 && obj.GetName() == "cb-1" {
				return apierrors.NewServiceUnavailable("unavailable")
			}
			return cl.Delete(ctx, obj, opts...)
		},
	}, cb1, cb2, pt1, cc)
	seedBufferStatus(t, c, cc.Name)

	sweeper := &Sweeper{Client: c}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: NamespaceGkeManagedCCC}}

	// Pass 1: cb-1 fails to delete, cb-2 is still deleted, and PodTemplates and ComputeClass status are untouched.
	res, err := sweeper.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatal("Sweeper.Reconcile() error = nil, want the CapacityBuffer delete error")
	}
	if want := `deleting CapacityBuffer "cb-1"`; !strings.Contains(err.Error(), want) {
		t.Errorf("Sweeper.Reconcile() error = %v, want it to contain %q", err, want)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("Sweeper.Reconcile() result = %+v, want empty on error", res)
	}
	var cbList cbv1beta1.CapacityBufferList
	if err := c.List(t.Context(), &cbList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil || len(cbList.Items) != 1 || cbList.Items[0].Name != "cb-1" {
		t.Errorf("CapacityBuffers = %+v (err=%v), want only cb-1", cbList.Items, err)
	}
	var ptList corev1.PodTemplateList
	if err := c.List(t.Context(), &ptList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil || len(ptList.Items) != 1 {
		t.Errorf("PodTemplates = %+v (err=%v), want pt-1 untouched while the cb-1 delete fails", ptList.Items, err)
	}
	var gotCC ccapiv1.ComputeClass
	if err := c.Get(t.Context(), types.NamespacedName{Name: cc.Name}, &gotCC); err != nil || !hasBufferStatus(&gotCC) {
		t.Errorf("ComputeClass status = %+v (err=%v), want the buffer status kept while the cb-1 delete fails", gotCC.Status, err)
	}

	// Pass 2: cb-1 delete succeeds, so pt-1 is deleted and ComputeClass status is cleared.
	failCB1 = false
	res, err = sweeper.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Retry: Sweeper.Reconcile() error = %v, want nil", err)
	}
	if want := (reconcile.Result{RequeueAfter: sweepInterval}); res != want {
		t.Errorf("Retry: Sweeper.Reconcile() result = %+v, want %+v", res, want)
	}
	if err := c.List(t.Context(), &ptList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil || len(ptList.Items) != 0 {
		t.Errorf("Retry: PodTemplates = %d (err=%v), want 0", len(ptList.Items), err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: cc.Name}, &gotCC); err != nil || hasBufferStatus(&gotCC) {
		t.Errorf("Retry: ComputeClass status = %+v (err=%v), want the buffer status cleared", gotCC.Status, err)
	}
}

func TestSweeper_NotFoundIsIgnored(t *testing.T) {
	cb := &cbv1beta1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{Name: "cb-gone", Namespace: NamespaceGkeManagedCCC}}
	pt := &corev1.PodTemplate{ObjectMeta: metav1.ObjectMeta{Name: "pt-gone", Namespace: NamespaceGkeManagedCCC}}
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "cc-gone"},
		Status: ccapiv1.ComputeClassStatus{
			Conditions: []metav1.Condition{{Type: conditionTypeBuffersConfigured, Status: metav1.ConditionTrue, Reason: "Ready"}},
		},
	}

	c := newFakeClient(t, interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "objects"}, obj.GetName())
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "computeclasses"}, obj.GetName())
		},
	}, cb, pt, cc)

	sweeper := &Sweeper{Client: c}
	res, err := sweeper.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: NamespaceGkeManagedCCC}})
	if err != nil {
		t.Fatalf("Sweeper.Reconcile() error = %v, want nil (NotFound errors are ignored)", err)
	}
	if want := (reconcile.Result{RequeueAfter: sweepInterval}); res != want {
		t.Errorf("Sweeper.Reconcile() result = %+v, want %+v", res, want)
	}
}

func TestSweeper_PodTemplateAndStatusFailuresRetry(t *testing.T) {
	pt1 := &corev1.PodTemplate{ObjectMeta: metav1.ObjectMeta{Name: "pt-1", Namespace: NamespaceGkeManagedCCC}}
	pt2 := &corev1.PodTemplate{ObjectMeta: metav1.ObjectMeta{Name: "pt-2", Namespace: NamespaceGkeManagedCCC}}
	cc1 := &ccapiv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "cc-1"}}
	cc2 := &ccapiv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "cc-2"}}

	failFirst := true
	c := newFakeClient(t, interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if failFirst && obj.GetName() == "pt-1" {
				return apierrors.NewServiceUnavailable("unavailable")
			}
			return cl.Delete(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if failFirst && obj.GetName() == "cc-1" {
				return apierrors.NewServiceUnavailable("unavailable")
			}
			return cl.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	}, pt1, pt2, cc1, cc2)

	failFirst = false
	seedBufferStatus(t, c, cc1.Name)
	seedBufferStatus(t, c, cc2.Name)
	failFirst = true

	sweeper := &Sweeper{Client: c}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: NamespaceGkeManagedCCC}}

	// Pass 1: pt-1 and cc-1 fail, but pt-2 is still deleted and cc-2 status is still cleared in the same pass.
	res, err := sweeper.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatal("Sweeper.Reconcile() error = nil, want the PodTemplate delete and status patch errors")
	}
	for _, want := range []string{`deleting PodTemplate "pt-1"`, `clearing buffer status of ComputeClass "cc-1"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Sweeper.Reconcile() error = %v, want it to contain %q", err, want)
		}
	}
	if res != (reconcile.Result{}) {
		t.Errorf("Sweeper.Reconcile() result = %+v, want empty on error", res)
	}
	var ptList corev1.PodTemplateList
	if err := c.List(t.Context(), &ptList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil || len(ptList.Items) != 1 || ptList.Items[0].Name != "pt-1" {
		t.Errorf("PodTemplates = %+v (err=%v), want only pt-1", ptList.Items, err)
	}
	var gotCC1, gotCC2 ccapiv1.ComputeClass
	if err := c.Get(t.Context(), types.NamespacedName{Name: "cc-1"}, &gotCC1); err != nil || !hasBufferStatus(&gotCC1) {
		t.Errorf("cc-1 status = %+v (err=%v), want the buffer status kept after the failed patch", gotCC1.Status, err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "cc-2"}, &gotCC2); err != nil || hasBufferStatus(&gotCC2) {
		t.Errorf("cc-2 status = %+v (err=%v), want the buffer status cleared in Pass 1", gotCC2.Status, err)
	}

	// Pass 2: retry deletes pt-1 and clears cc-1 status.
	failFirst = false
	res, err = sweeper.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Retry: Sweeper.Reconcile() error = %v, want nil", err)
	}
	if want := (reconcile.Result{RequeueAfter: sweepInterval}); res != want {
		t.Errorf("Retry: Sweeper.Reconcile() result = %+v, want %+v", res, want)
	}
	if err := c.List(t.Context(), &ptList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil || len(ptList.Items) != 0 {
		t.Errorf("Retry: PodTemplates = %d (err=%v), want 0", len(ptList.Items), err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "cc-1"}, &gotCC1); err != nil || hasBufferStatus(&gotCC1) {
		t.Errorf("Retry: cc-1 status = %+v (err=%v), want the buffer status cleared", gotCC1.Status, err)
	}
}
