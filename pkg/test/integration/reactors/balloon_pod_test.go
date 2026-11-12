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

package reactors

import (
	"encoding/json"
	"errors"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func newTestBalloonPod(nodeName, cpu, mem string) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    "kube-system",
			GenerateName: "gke-system-balloon-pod-",
		},
		Spec: apiv1.PodSpec{
			NodeName: nodeName,
			Containers: []apiv1.Container{{
				Name: "balloon",
				Resources: apiv1.ResourceRequirements{
					Requests: apiv1.ResourceList{
						apiv1.ResourceCPU:    resource.MustParse(cpu),
						apiv1.ResourceMemory: resource.MustParse(mem),
					},
				},
			}},
		},
	}
}

func TestBalloonPodReactorCreate(t *testing.T) {
	injectedErr := errors.New("webhook rejected")

	tests := []struct {
		name          string
		pod           *apiv1.Pod
		createOpts    metav1.CreateOptions
		dryRunErr     error
		createErr     error
		wantErr       error
		wantStoredLen int
		wantPhase     apiv1.PodPhase
		wantDryRuns   int64
		wantCreates   int64
	}{
		{
			name:          "dry_run_balloon_pod_not_persisted",
			pod:           newTestBalloonPod("", "2", "4Gi"),
			createOpts:    metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}},
			wantStoredLen: 0,
			wantDryRuns:   1,
		},
		{
			name:        "dry_run_injected_error_returned",
			pod:         newTestBalloonPod("", "2", "4Gi"),
			createOpts:  metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}},
			dryRunErr:   injectedErr,
			wantErr:     injectedErr,
			wantDryRuns: 1,
		},
		{
			name:          "bound_balloon_pod_starts_running_with_allocated_resources",
			pod:           newTestBalloonPod("ek-node-1", "2", "4Gi"),
			wantStoredLen: 1,
			wantPhase:     apiv1.PodRunning,
			wantCreates:   1,
		},
		{
			name:        "bound_create_injected_error_returned",
			pod:         newTestBalloonPod("ek-node-1", "2", "4Gi"),
			createErr:   injectedErr,
			wantErr:     injectedErr,
			wantCreates: 1,
		},
		{
			name:          "unbound_non_dry_run_balloon_pod_persisted_without_starting",
			pod:           newTestBalloonPod("", "2", "4Gi"),
			wantStoredLen: 1,
			wantCreates:   1,
		},
		{
			name: "regular_workload_pod_passthrough",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "user-pod"},
			},
			wantStoredLen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			reactor := InstallBalloonPodReactor(client)
			reactor.SetDryRunCreateError(tc.dryRunErr)
			reactor.SetCreateError(tc.createErr)

			ns := tc.pod.Namespace
			created, err := client.CoreV1().Pods(ns).Create(t.Context(), tc.pod, tc.createOpts)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Pods(%q).Create() error = %v, want %v", ns, err, tc.wantErr)
			}
			if err == nil {
				if created.Status.Phase != tc.wantPhase {
					t.Errorf("created.Status.Phase = %q, want %q", created.Status.Phase, tc.wantPhase)
				}
				if tc.wantPhase != "" {
					if created.Name == "" || created.UID == "" {
						t.Errorf("created Name/UID = (%q, %q), want non-empty", created.Name, created.UID)
					}
					if len(created.Status.ContainerStatuses) != 1 {
						t.Fatalf("len(ContainerStatuses) = %d, want 1", len(created.Status.ContainerStatuses))
					}
					gotCPU := created.Status.ContainerStatuses[0].AllocatedResources[apiv1.ResourceCPU]
					wantCPU := resource.MustParse("2")
					if !gotCPU.Equal(wantCPU) {
						t.Errorf("AllocatedResources[cpu] = %v, want %v", gotCPU, wantCPU)
					}
					if created.Status.Resources == nil {
						t.Fatalf("created.Status.Resources = nil, want non-nil")
					}
					gotPodMem := created.Status.Resources.Requests[apiv1.ResourceMemory]
					wantPodMem := resource.MustParse("4Gi")
					if !gotPodMem.Equal(wantPodMem) {
						t.Errorf("Status.Resources.Requests[memory] = %v, want %v", gotPodMem, wantPodMem)
					}
				} else if len(created.Status.ContainerStatuses) != 0 {
					t.Errorf("len(ContainerStatuses) = %d, want 0", len(created.Status.ContainerStatuses))
				}
			}

			list, listErr := client.CoreV1().Pods(ns).List(t.Context(), metav1.ListOptions{})
			if listErr != nil {
				t.Fatalf("Pods(%q).List() unexpected error: %v", ns, listErr)
			}
			if len(list.Items) != tc.wantStoredLen {
				t.Errorf("len(Pods(%q).Items) = %d, want %d", ns, len(list.Items), tc.wantStoredLen)
			}

			dryRuns, creates, _ := reactor.Counts()
			if dryRuns != tc.wantDryRuns || creates != tc.wantCreates {
				t.Errorf("Counts() = (%d, %d, _), want (%d, %d, _)", dryRuns, creates, tc.wantDryRuns, tc.wantCreates)
			}
		})
	}
}

func TestBalloonPodReactorResizePatch(t *testing.T) {
	patchAPIError := errors.New("resize patch failed")
	infeasibleCond := &apiv1.PodCondition{
		Type:   apiv1.PodResizePending,
		Status: apiv1.ConditionTrue,
		Reason: apiv1.PodReasonInfeasible,
	}

	tests := []struct {
		name               string
		pod                *apiv1.Pod
		freezePodLevelMem  bool
		patchCPU           string
		patchMem           string
		patchErr           error
		rejectCond         *apiv1.PodCondition
		wantErr            error
		wantAllocatedCPU   string
		wantAllocatedMem   string
		wantPodLevelStatus string
		wantCondReason     string
		wantResizePatches  int64
	}{
		{
			name:               "actuates_balloon_pod_ippr_resources",
			patchCPU:           "6",
			patchMem:           "12Gi",
			wantAllocatedCPU:   "6",
			wantAllocatedMem:   "12Gi",
			wantPodLevelStatus: "12Gi",
			wantResizePatches:  1,
		},
		{
			name:               "freezes_pod_level_memory_when_enabled",
			freezePodLevelMem:  true,
			patchCPU:           "1",
			patchMem:           "1Gi",
			wantAllocatedCPU:   "1",
			wantAllocatedMem:   "1Gi",
			wantPodLevelStatus: "4Gi",
			wantResizePatches:  1,
		},
		{
			name:               "records_rejection_condition_without_updating_allocated",
			patchCPU:           "6",
			patchMem:           "12Gi",
			rejectCond:         infeasibleCond,
			wantAllocatedCPU:   "2",
			wantAllocatedMem:   "4Gi",
			wantPodLevelStatus: "4Gi",
			wantCondReason:     apiv1.PodReasonInfeasible,
			wantResizePatches:  1,
		},
		{
			name:              "returns_injected_resize_patch_error",
			patchCPU:          "6",
			patchMem:          "12Gi",
			patchErr:          patchAPIError,
			wantErr:           patchAPIError,
			wantResizePatches: 1,
		},
		{
			name: "resizes_non_balloon_workload_pod_ignoring_balloon_faults",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "workload-pod",
				},
				Spec: apiv1.PodSpec{
					NodeName: "ek-node-1",
					Containers: []apiv1.Container{{
						Name: "app",
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse("2"),
								apiv1.ResourceMemory: resource.MustParse("4Gi"),
							},
						},
					}},
				},
				Status: apiv1.PodStatus{
					Resources: &apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:    resource.MustParse("2"),
							apiv1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
			},
			freezePodLevelMem:  true,
			patchCPU:           "6",
			patchMem:           "12Gi",
			patchErr:           patchAPIError,
			rejectCond:         infeasibleCond,
			wantAllocatedCPU:   "6",
			wantAllocatedMem:   "12Gi",
			wantPodLevelStatus: "12Gi",
			wantResizePatches:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			reactor := InstallBalloonPodReactor(client)

			pod := tc.pod
			if pod == nil {
				pod = newTestBalloonPod("ek-node-1", "2", "4Gi")
			}
			ns := pod.Namespace
			containerName := pod.Spec.Containers[0].Name
			created, err := client.CoreV1().Pods(ns).Create(t.Context(), pod.DeepCopy(), metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Create() unexpected error: %v", err)
			}

			reactor.SetResizePatchError(tc.patchErr)
			reactor.SetResizeRejectionCondition(tc.rejectCond)
			reactor.SetFreezePodLevelMemoryOnResize(tc.freezePodLevelMem)

			rawPatch, err := json.Marshal(&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{{
						Name: containerName,
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse(tc.patchCPU),
								apiv1.ResourceMemory: resource.MustParse(tc.patchMem),
							},
						},
					}},
				},
			})
			if err != nil {
				t.Fatalf("json.Marshal() unexpected error: %v", err)
			}

			patched, err := client.CoreV1().Pods(ns).Patch(
				t.Context(), created.Name, types.StrategicMergePatchType, rawPatch, metav1.PatchOptions{}, "resize",
			)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Patch(resize) error = %v, want %v", err, tc.wantErr)
			}
			_, _, patches := reactor.Counts()
			if patches != tc.wantResizePatches {
				t.Errorf("Counts() resizePatches = %d, want %d", patches, tc.wantResizePatches)
			}
			if err != nil {
				return
			}

			gotCPU := patched.Status.ContainerStatuses[0].AllocatedResources[apiv1.ResourceCPU]
			wantCPU := resource.MustParse(tc.wantAllocatedCPU)
			if !gotCPU.Equal(wantCPU) {
				t.Errorf("AllocatedResources[cpu] = %v, want %v", gotCPU, wantCPU)
			}
			gotMem := patched.Status.ContainerStatuses[0].AllocatedResources[apiv1.ResourceMemory]
			wantMem := resource.MustParse(tc.wantAllocatedMem)
			if !gotMem.Equal(wantMem) {
				t.Errorf("AllocatedResources[memory] = %v, want %v", gotMem, wantMem)
			}
			if patched.Status.Resources == nil {
				t.Fatalf("patched.Status.Resources = nil, want non-nil")
			}
			gotPodMem := patched.Status.Resources.Requests[apiv1.ResourceMemory]
			wantPodMem := resource.MustParse(tc.wantPodLevelStatus)
			if !gotPodMem.Equal(wantPodMem) {
				t.Errorf("Status.Resources.Requests[memory] = %v, want %v", gotPodMem, wantPodMem)
			}

			var gotReason string
			for _, cond := range patched.Status.Conditions {
				if cond.Type == apiv1.PodResizePending {
					gotReason = cond.Reason
				}
			}
			if gotReason != tc.wantCondReason {
				t.Errorf("PodResizePending reason = %q, want %q", gotReason, tc.wantCondReason)
			}
		})
	}
}

func TestBalloonPodReactorResizeClearsConditionsOnRecovery(t *testing.T) {
	tests := []struct {
		name        string
		initialCond *apiv1.PodCondition
	}{
		{
			name: "clears_pod_resize_pending_preserving_non_resize_conditions",
			initialCond: &apiv1.PodCondition{
				Type:   apiv1.PodResizePending,
				Status: apiv1.ConditionTrue,
				Reason: apiv1.PodReasonInfeasible,
			},
		},
		{
			name: "clears_pod_resize_in_progress_preserving_non_resize_conditions",
			initialCond: &apiv1.PodCondition{
				Type:   apiv1.PodResizeInProgress,
				Status: apiv1.ConditionTrue,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			reactor := InstallBalloonPodReactor(client)

			pod := newTestBalloonPod("ek-node-1", "2", "4Gi")
			pod.Status.Conditions = []apiv1.PodCondition{{
				Type:   apiv1.PodReady,
				Status: apiv1.ConditionTrue,
			}}
			created, err := client.CoreV1().Pods("kube-system").Create(t.Context(), pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Create() unexpected error: %v", err)
			}

			rawPatch, err := json.Marshal(&apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{{
						Name: "balloon",
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse("6"),
								apiv1.ResourceMemory: resource.MustParse("12Gi"),
							},
						},
					}},
				},
			})
			if err != nil {
				t.Fatalf("json.Marshal() unexpected error: %v", err)
			}

			reactor.SetResizeRejectionCondition(tc.initialCond)
			rejected, err := client.CoreV1().Pods("kube-system").Patch(
				t.Context(), created.Name, types.StrategicMergePatchType, rawPatch, metav1.PatchOptions{}, "resize",
			)
			if err != nil {
				t.Fatalf("initial Patch(%q, resize) unexpected error: %v", created.Name, err)
			}
			var sawInitial, sawReady bool
			for _, cond := range rejected.Status.Conditions {
				if cond.Type == tc.initialCond.Type {
					sawInitial = true
				}
				if cond.Type == apiv1.PodReady && cond.Status == apiv1.ConditionTrue {
					sawReady = true
				}
			}
			if !sawInitial || !sawReady {
				t.Fatalf("Patch(%q, resize) with rejection returned Conditions = %+v, want both %q and PodReady=True", created.Name, rejected.Status.Conditions, tc.initialCond.Type)
			}

			reactor.SetResizeRejectionCondition(nil)
			recovered, err := client.CoreV1().Pods("kube-system").Patch(
				t.Context(), created.Name, types.StrategicMergePatchType, rawPatch, metav1.PatchOptions{}, "resize",
			)
			if err != nil {
				t.Fatalf("recovery Patch(%q, resize) unexpected error: %v", created.Name, err)
			}
			sawReady = false
			for _, cond := range recovered.Status.Conditions {
				if cond.Type == apiv1.PodResizePending || cond.Type == apiv1.PodResizeInProgress {
					t.Errorf("Patch(%q, resize) after recovery returned condition %+v, want no %s or %s condition", created.Name, cond, apiv1.PodResizePending, apiv1.PodResizeInProgress)
				}
				if cond.Type == apiv1.PodReady && cond.Status == apiv1.ConditionTrue {
					sawReady = true
				}
			}
			if !sawReady {
				t.Errorf("Patch(%q, resize) after recovery returned Conditions = %+v, want PodReady=True retained", created.Name, recovered.Status.Conditions)
			}

			gotCPU := recovered.Status.ContainerStatuses[0].AllocatedResources[apiv1.ResourceCPU]
			wantCPU := resource.MustParse("6")
			if !gotCPU.Equal(wantCPU) {
				t.Errorf("Patch(%q, resize) after recovery AllocatedResources[cpu] = %v, want %v", created.Name, gotCPU, wantCPU)
			}
			gotMem := recovered.Status.ContainerStatuses[0].AllocatedResources[apiv1.ResourceMemory]
			wantMem := resource.MustParse("12Gi")
			if !gotMem.Equal(wantMem) {
				t.Errorf("Patch(%q, resize) after recovery AllocatedResources[memory] = %v, want %v", created.Name, gotMem, wantMem)
			}
		})
	}
}
