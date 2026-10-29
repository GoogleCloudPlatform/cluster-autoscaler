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
	"errors"
	"fmt"
	"slices"
	"testing"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	csn "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestCleanupModeRestartScenario follows a ComputeClass with an active and a standby buffer through
// CA restarts: normal mode (Reconciler), then cleanup mode (Sweeper only, no Reconciler),
// then fully disabled (neither controller), then normal mode again. Each restart computes the
// options from the experiments through OptionsTracker, as CA startup does.
func TestCleanupModeRestartScenario(t *testing.T) {
	ctx := t.Context()
	const ccName = "test-cc"
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: ccName, UID: "cc-uid", Generation: 1},
		Spec: ccapiv1.ComputeClassSpec{
			Buffers: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: activeCapacityStrategy, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}},
				{ProvisioningStrategy: standbyCapacityStrategy, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}},
			},
		},
	}
	// A class without buffers, which cleanup mode must leave alone.
	plain := &ccapiv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "plain-cc", UID: "plain-cc-uid", Generation: 1}}
	c, writes := newScenarioClient(t, cc, plain)
	collector := &capacityBuffersCollector{client: c}
	// Once markBuffersProvisioning has run, the collector counts both buffers as provisioning.
	provisioningSamples := map[bufferMetricKey]float64{
		{strategy: activeCapacityStrategy, status: bufferStatusProvisioning}:  1,
		{strategy: standbyCapacityStrategy, status: bufferStatusProvisioning}: 1,
	}

	enabledFlags := map[string]bool{}
	cleanupFlags := map[string]bool{
		experiments.ComputeClassCapacityBuffersFlag:        false,
		experiments.ComputeClassCapacityBuffersCleanupFlag: true,
	}
	disabledFlags := map[string]bool{
		experiments.ComputeClassCapacityBuffersFlag:        false,
		experiments.ComputeClassCapacityBuffersCleanupFlag: false,
	}

	// 1. CA starts with the feature on: Reconciler creates the derived objects and the class gets Ready.
	r, err := startNormalReconciler(c, enabledFlags)
	if err != nil {
		t.Fatalf("Normal mode: %v", err)
	}
	if _, err := reconcileScenario(ctx, r, ccName); err != nil {
		t.Fatalf("Normal mode: Reconcile() error = %v, want nil", err)
	}
	if _, err := checkDerivedObjects(ctx, c, cc); err != nil {
		t.Errorf("Normal mode, first pass: %v", err)
	}
	markBuffersProvisioning(t, c, ccName)
	if _, err := reconcileScenario(ctx, r, ccName); err != nil {
		t.Fatalf("Normal mode: Reconcile() error = %v, want nil", err)
	}
	enabledCBs, err := checkDerivedObjects(ctx, c, cc)
	if err != nil {
		t.Errorf("Normal mode: %v", err)
	}
	if err := checkBuffersReady(ctx, c, ccName, enabledCBs); err != nil {
		t.Errorf("Normal mode: %v", err)
	}
	if err := checkCollectorValues(collector, provisioningSamples); err != nil {
		t.Errorf("Normal mode: capacityBuffersCollector: %v", err)
	}

	// 2. Disablement flow step 1: enable Cleanup and disable Enabled. CA restarts and runs only
	// Sweeper (Reconciler is not registered).
	if err := checkRestartRequired(enabledFlags, cleanupFlags); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	sweeper, err := startCleanupSweeper(c, cleanupFlags)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	writes.reset()
	res, err := reconcileSweeper(ctx, sweeper)
	if err != nil {
		t.Fatalf("Cleanup: Sweeper.Reconcile() error = %v, want nil", err)
	}
	if want := (reconcile.Result{RequeueAfter: sweepInterval}); res != want {
		t.Errorf("Cleanup: Sweeper.Reconcile() result = %+v, want %+v", res, want)
	}
	// Only test-cc should receive a status patch; plain-cc (no buffer status) is untouched.
	if writes.statusPatches != 1 {
		t.Errorf("Cleanup: status patches = %d, want 1 (for %s only)", writes.statusPatches, ccName)
	}
	if err := checkTornDown(ctx, c, ccName, &cc.Spec); err != nil {
		t.Errorf("Cleanup: %v", err)
	}
	if err := checkCollectorValues(collector, nil); err != nil {
		t.Errorf("Cleanup: capacityBuffersCollector: %v", err)
	}

	// Later periodic sweeps find nothing to do and issue no writes.
	writes.reset()
	if _, err := reconcileSweeper(ctx, sweeper); err != nil {
		t.Fatalf("Cleanup, repeated sweep: Sweeper.Reconcile() error = %v, want nil", err)
	}
	if *writes != (scenarioWrites{}) {
		t.Errorf("Cleanup, repeated sweep: writes = %+v, want none", *writes)
	}

	// A spec.buffers edit doesn't bring the objects back (Reconciler is not running), and the next
	// sweep issues no writes.
	edited, err := getScenarioClass(ctx, c, ccName)
	if err != nil {
		t.Fatalf("Cleanup, spec edit: %v", err)
	}
	edited.Generation = 2
	edited.Spec.Buffers[0].Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6")}
	if err := c.Update(ctx, edited); err != nil {
		t.Fatalf("Failed to edit spec.buffers: %v", err)
	}
	writes.reset()
	if _, err := reconcileSweeper(ctx, sweeper); err != nil {
		t.Fatalf("Cleanup, spec edit: Sweeper.Reconcile() error = %v, want nil", err)
	}
	if *writes != (scenarioWrites{}) {
		t.Errorf("Cleanup, spec edit: writes = %+v, want none", *writes)
	}
	if err := checkTornDown(ctx, c, ccName, &edited.Spec); err != nil {
		t.Errorf("Cleanup, spec edit: %v", err)
	}

	// A stray CapacityBuffer in gke-managed-ccc is deleted on the next sweep.
	late := buildCapacityBufferForCreate(edited, edited.Spec.Buffers[0], "pt-basic-"+ccName)
	if err := c.Create(ctx, late); err != nil {
		t.Fatalf("Failed to create the late CapacityBuffer: %v", err)
	}
	writes.reset()
	if _, err := reconcileSweeper(ctx, sweeper); err != nil {
		t.Fatalf("Cleanup, late buffer: Sweeper.Reconcile() error = %v, want nil", err)
	}
	if want := (scenarioWrites{deletes: 1}); *writes != want {
		t.Errorf("Cleanup, late buffer: writes = %+v, want %+v", *writes, want)
	}
	if err := checkTornDown(ctx, c, ccName, &edited.Spec); err != nil {
		t.Errorf("Cleanup, late buffer: %v", err)
	}
	if err := checkCollectorValues(collector, nil); err != nil {
		t.Errorf("Cleanup, late buffer: capacityBuffersCollector: %v", err)
	}

	// 3. Disablement flow step 2: once cleanup has succeeded, disable Cleanup flag. CA restarts
	// with neither Reconciler nor Sweeper registered.
	if err := checkRestartRequired(cleanupFlags, disabledFlags); err != nil {
		t.Fatalf("Disabled: %v", err)
	}
	disabledOpts := scenarioOptionsTracker(disabledFlags).Options()
	if disabledOpts.ComputeClassCapacityBuffersEnabled || disabledOpts.ComputeClassCapacityBuffersCleanup {
		t.Fatalf("Disabled: options Enabled=%v Cleanup=%v, want both false",
			disabledOpts.ComputeClassCapacityBuffersEnabled, disabledOpts.ComputeClassCapacityBuffersCleanup)
	}

	// 4. Re-enabling the feature restarts CA in normal mode and recreates everything from the
	// current spec.buffers.
	if err := checkRestartRequired(disabledFlags, enabledFlags); err != nil {
		t.Fatalf("Re-enabled: %v", err)
	}
	r, err = startNormalReconciler(c, enabledFlags)
	if err != nil {
		t.Fatalf("Re-enabled: %v", err)
	}
	if _, err := reconcileScenario(ctx, r, ccName); err != nil {
		t.Fatalf("Re-enabled: Reconcile() error = %v, want nil", err)
	}
	if _, err := checkDerivedObjects(ctx, c, edited); err != nil {
		t.Errorf("Re-enabled, first pass: %v", err)
	}
	markBuffersProvisioning(t, c, ccName)
	if _, err := reconcileScenario(ctx, r, ccName); err != nil {
		t.Fatalf("Re-enabled: Reconcile() error = %v, want nil", err)
	}
	recreatedCBs, err := checkDerivedObjects(ctx, c, edited)
	if err != nil {
		t.Errorf("Re-enabled: %v", err)
	}
	if err := checkBuffersReady(ctx, c, ccName, recreatedCBs); err != nil {
		t.Errorf("Re-enabled: %v", err)
	}
	if err := checkCollectorValues(collector, provisioningSamples); err != nil {
		t.Errorf("Re-enabled: capacityBuffersCollector: %v", err)
	}
	for _, name := range recreatedCBs {
		if slices.Contains(enabledCBs, name) {
			t.Errorf("Re-enabled: CapacityBuffer name %s is an old name, want a new CapacityBuffer", name)
		}
	}
}

// scenarioWrites counts the writes sent to the fake API server.
type scenarioWrites struct {
	creates, patches, deletes, statusPatches int
}

func (w *scenarioWrites) reset() {
	*w = scenarioWrites{}
}

// newScenarioClient returns a fake client that serves objs with status subresources and SSA, and
// counts its writes.
func newScenarioClient(t *testing.T, objs ...client.Object) (client.Client, *scenarioWrites) {
	t.Helper()
	writes := &scenarioWrites{}
	var uids int
	c := newFakeClient(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			// The fake client doesn't set UIDs, and the reconciler remembers deleted buffers by UID.
			if obj.GetUID() == "" {
				uids++
				obj.SetUID(types.UID(fmt.Sprintf("uid-%d", uids)))
			}
			writes.creates++
			return c.Create(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			writes.patches++
			return c.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			writes.deletes++
			return c.Delete(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			writes.statusPatches++
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	}, objs...)
	return c, writes
}

// scenarioExperiments returns experiments for a CA version that meets the prod MinCAVersion.
func scenarioExperiments(boolFlags map[string]bool) experiments.Manager {
	return experiments.NewMockManagerWithOptions(
		version.Version{37, 237, 0, 1},
		boolFlags,
		map[string]string{experiments.ComputeClassCapacityBuffersMinCAVersionFlag: "37.237.0"},
	)
}

// scenarioOptionsTracker returns the OptionsTracker of a CA that starts with the given experiment values.
func scenarioOptionsTracker(boolFlags map[string]bool) *optstracking.OptionsTracker {
	return optstracking.TestOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{ClusterVersion: "1.37.1-gke.3078000"}, scenarioExperiments(boolFlags))
}

// startNormalReconciler checks the options computed from the experiments as CA startup does, and
// returns a new Reconciler when normal mode is active (Enabled=true, Cleanup=false).
func startNormalReconciler(c client.Client, boolFlags map[string]bool) (*Reconciler, error) {
	opts := scenarioOptionsTracker(boolFlags).Options()
	if !opts.ComputeClassCapacityBuffersEnabled || opts.ComputeClassCapacityBuffersCleanup {
		return nil, fmt.Errorf("options for experiments %v: Enabled=%v Cleanup=%v, want Enabled=true Cleanup=false (normal Reconciler)",
			boolFlags, opts.ComputeClassCapacityBuffersEnabled, opts.ComputeClassCapacityBuffersCleanup)
	}
	return &Reconciler{Client: c}, nil
}

// startCleanupSweeper checks the options computed from the experiments as CA startup does, and
// returns a new Sweeper when cleanup mode is active (Cleanup=true).
func startCleanupSweeper(c client.Client, boolFlags map[string]bool) (*Sweeper, error) {
	opts := scenarioOptionsTracker(boolFlags).Options()
	if !opts.ComputeClassCapacityBuffersCleanup {
		return nil, fmt.Errorf("options for experiments %v: Cleanup=false, want Cleanup=true (Sweeper)", boolFlags)
	}
	return &Sweeper{Client: c}, nil
}

func reconcileSweeper(ctx context.Context, s *Sweeper) (reconcile.Result, error) {
	return s.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: NamespaceGkeManagedCCC},
	})
}

// checkRestartRequired returns an error unless OptionsTracker asks for a CA restart when the
// experiments change.
func checkRestartRequired(before, after map[string]bool) error {
	tracker := scenarioOptionsTracker(before)
	optstracking.ChangeTestExperimentsManager(tracker, scenarioExperiments(after))
	if err := tracker.RecomputeOptions(); err != nil {
		return fmt.Errorf("RecomputeOptions() error = %v, want nil", err)
	}
	if !tracker.OptionChangesRequireRestart() {
		return fmt.Errorf("OptionChangesRequireRestart() = false when the experiments change from %v to %v, want true", before, after)
	}
	return nil
}

func reconcileScenario(ctx context.Context, r *Reconciler, ccName string) (reconcile.Result, error) {
	return r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: ccName}})
}

func getScenarioClass(ctx context.Context, c client.Client, ccName string) (*ccapiv1.ComputeClass, error) {
	var cc ccapiv1.ComputeClass
	if err := c.Get(ctx, types.NamespacedName{Name: ccName}, &cc); err != nil {
		return nil, fmt.Errorf("getting ComputeClass %s: %w", ccName, err)
	}
	return &cc, nil
}

func listScenarioObjects(ctx context.Context, c client.Client, ccName string) ([]corev1.PodTemplate, []cbv1beta1.CapacityBuffer, error) {
	inClass := []client.ListOption{client.InNamespace(NamespaceGkeManagedCCC), client.MatchingLabels{gkelabels.ComputeClassLabel: ccName}}
	var pts corev1.PodTemplateList
	if err := c.List(ctx, &pts, inClass...); err != nil {
		return nil, nil, fmt.Errorf("listing PodTemplates: %w", err)
	}
	var cbs cbv1beta1.CapacityBufferList
	if err := c.List(ctx, &cbs, inClass...); err != nil {
		return nil, nil, fmt.Errorf("listing CapacityBuffers: %w", err)
	}
	return pts.Items, cbs.Items, nil
}

// checkDerivedObjects checks the PodTemplates and CapacityBuffers derived from cc and returns the
// CapacityBuffer names.
func checkDerivedObjects(ctx context.Context, c client.Client, cc *ccapiv1.ComputeClass) ([]string, error) {
	pts, cbs, err := listScenarioObjects(ctx, c, cc.Name)
	if err != nil {
		return nil, err
	}

	var errs []error
	gotPTs := make(map[string]corev1.PodTemplate, len(pts))
	for _, pt := range pts {
		gotPTs[pt.Name] = pt
	}
	if len(gotPTs) != 2 {
		errs = append(errs, fmt.Errorf("PodTemplates = %d, want 2", len(gotPTs)))
	}
	for _, strategy := range []string{activeCapacityStrategy, standbyCapacityStrategy} {
		name, _ := podTemplateName(cc.Name, strategy)
		pt, ok := gotPTs[name]
		if !ok {
			errs = append(errs, fmt.Errorf("PodTemplate %s is missing", name))
			continue
		}
		if err := checkOwnedByClass(&pt, cc); err != nil {
			errs = append(errs, err)
		}
		spec := pt.Template.Spec
		if spec.NodeSelector[gkelabels.ComputeClassLabel] != cc.Name {
			errs = append(errs, fmt.Errorf("PodTemplate %s: nodeSelector = %v, want %s=%s", name, spec.NodeSelector, gkelabels.ComputeClassLabel, cc.Name))
		}
		if len(spec.Containers) != 1 || !spec.Containers[0].Resources.Requests.Cpu().Equal(resource.MustParse("2")) ||
			!spec.Containers[0].Resources.Requests.Memory().Equal(resource.MustParse("2Gi")) {
			errs = append(errs, fmt.Errorf("PodTemplate %s: containers = %+v, want one container requesting 2 CPU and 2Gi", name, spec.Containers))
		}
		if got, want := excludesStandbyNodes(spec), strategy != standbyCapacityStrategy; got != want {
			errs = append(errs, fmt.Errorf("PodTemplate %s: standby node anti-affinity = %v, want %v", name, got, want))
		}
	}

	if len(cbs) != len(cc.Spec.Buffers) {
		errs = append(errs, fmt.Errorf("CapacityBuffers = %d, want %d", len(cbs), len(cc.Spec.Buffers)))
	}
	var names []string
	for _, buf := range cc.Spec.Buffers {
		i := slices.IndexFunc(cbs, func(cb cbv1beta1.CapacityBuffer) bool {
			return getCapacityBufferStrategy(&cb) == buf.ProvisioningStrategy
		})
		if i < 0 {
			errs = append(errs, fmt.Errorf("CapacityBuffer with strategy %s is missing", buf.ProvisioningStrategy))
			continue
		}
		cb := cbs[i]
		names = append(names, cb.Name)
		if err := checkOwnedByClass(&cb, cc); err != nil {
			errs = append(errs, err)
		}
		wantPT, _ := podTemplateName(cc.Name, buf.ProvisioningStrategy)
		if cb.Spec.PodTemplateRef == nil || cb.Spec.PodTemplateRef.Name != wantPT {
			errs = append(errs, fmt.Errorf("CapacityBuffer %s: podTemplateRef = %+v, want %s", cb.Name, cb.Spec.PodTemplateRef, wantPT))
		}
		if got := toCoreResourceList(cb.Spec.Limits); !equality.Semantic.DeepEqual(got, buf.Limits) {
			errs = append(errs, fmt.Errorf("CapacityBuffer %s: limits = %v, want %v", cb.Name, got, buf.Limits))
		}
	}
	return names, errors.Join(errs...)
}

func checkOwnedByClass(obj metav1.Object, cc *ccapiv1.ComputeClass) error {
	var errs []error
	if !equality.Semantic.DeepEqual(obj.GetLabels(), commonBufferLabels(cc.Name)) {
		errs = append(errs, fmt.Errorf("%s: labels = %v, want %v", obj.GetName(), obj.GetLabels(), commonBufferLabels(cc.Name)))
	}
	if ref := metav1.GetControllerOf(obj); ref == nil || ref.Kind != kindComputeClass || ref.Name != cc.Name || ref.UID != cc.UID {
		errs = append(errs, fmt.Errorf("%s: controller = %+v, want ComputeClass %s (%s)", obj.GetName(), ref, cc.Name, cc.UID))
	}
	return errors.Join(errs...)
}

func excludesStandbyNodes(spec corev1.PodSpec) bool {
	if spec.Affinity == nil || spec.Affinity.NodeAffinity == nil || spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	for _, term := range spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key == csn.SoftWorkloadSeparationKey && expr.Operator == corev1.NodeSelectorOpNotIn && slices.Equal(expr.Values, []string{"true"}) {
				return true
			}
		}
	}
	return false
}

// markBuffersProvisioning sets the conditions that CA's CapacityBuffer controller and pod list
// processor write once a buffer is accepted and its fake pods are injected.
func markBuffersProvisioning(t *testing.T, c client.Client, ccName string) {
	t.Helper()
	_, cbs, err := listScenarioObjects(t.Context(), c, ccName)
	if err != nil {
		t.Fatalf("Failed to list the derived objects of %s: %v", ccName, err)
	}
	for i := range cbs {
		cb := &cbs[i]
		meta.SetStatusCondition(&cb.Status.Conditions, metav1.Condition{
			Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue, Reason: "AttributesSetSuccessfully", Message: "ready",
		})
		meta.SetStatusCondition(&cb.Status.Conditions, metav1.Condition{
			Type: capacitybuffer.ProvisioningCondition, Status: metav1.ConditionTrue, Reason: "FakePodsInjected",
		})
		if err := c.Status().Update(t.Context(), cb); err != nil {
			t.Fatalf("Failed to update CapacityBuffer %s status: %v", cb.Name, err)
		}
	}
}

// checkBuffersReady returns an error unless the class is Ready and observes exactly wantBuffers,
// each with ReadyForProvisioning and Provisioning True.
func checkBuffersReady(ctx context.Context, c client.Client, ccName string, wantBuffers []string) error {
	cc, err := getScenarioClass(ctx, c, ccName)
	if err != nil {
		return err
	}
	var errs []error
	cond := meta.FindStatusCondition(cc.Status.Conditions, conditionTypeBuffersConfigured)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Ready" || cond.ObservedGeneration != cc.Generation {
		errs = append(errs, fmt.Errorf("BuffersConfigured = %+v, want True (Ready) for generation %d", cond, cc.Generation))
	}
	var got []string
	for _, obs := range cc.Status.ObservedBuffers {
		got = append(got, obs.Name)
		for _, condType := range []string{capacitybuffer.ReadyForProvisioningCondition, capacitybuffer.ProvisioningCondition} {
			if obsCond := meta.FindStatusCondition(obs.Conditions, condType); obsCond == nil || obsCond.Status != metav1.ConditionTrue {
				errs = append(errs, fmt.Errorf("observed buffer %s: %s = %+v, want True", obs.Name, condType, obsCond))
			}
		}
	}
	slices.Sort(got)
	want := slices.Sorted(slices.Values(wantBuffers))
	if !slices.Equal(got, want) {
		errs = append(errs, fmt.Errorf("observed buffers = %v, want %v", got, want))
	}
	return errors.Join(errs...)
}

// checkTornDown returns an error unless no derived objects or buffer status are left and spec is wantSpec.
func checkTornDown(ctx context.Context, c client.Client, ccName string, wantSpec *ccapiv1.ComputeClassSpec) error {
	cc, err := getScenarioClass(ctx, c, ccName)
	if err != nil {
		return err
	}
	var errs []error
	if !equality.Semantic.DeepEqual(&cc.Spec, wantSpec) {
		errs = append(errs, fmt.Errorf("spec = %+v, want %+v", cc.Spec, *wantSpec))
	}
	if cond := meta.FindStatusCondition(cc.Status.Conditions, conditionTypeBuffersConfigured); cond != nil {
		errs = append(errs, fmt.Errorf("BuffersConfigured = %+v, want none", cond))
	}
	if len(cc.Status.ObservedBuffers) != 0 {
		errs = append(errs, fmt.Errorf("observed buffers = %+v, want none", cc.Status.ObservedBuffers))
	}
	pts, cbs, err := listScenarioObjects(ctx, c, ccName)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	if len(pts) != 0 || len(cbs) != 0 {
		errs = append(errs, fmt.Errorf("PodTemplates = %d and CapacityBuffers = %d, want none", len(pts), len(cbs)))
	}
	return errors.Join(errs...)
}
