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

package processors

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
)

var (
	exampleInitTime = time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC)
	exampleTimeInc  = time.Minute
)

func TestProvisioningRequestPodListProcessorInjectProvisioningRequestPods(t *testing.T) {
	timeNow := time.Date(2022, 12, 15, 10, 42, 0, 0, time.UTC)
	nowFunc := func() time.Time { return timeNow }
	pendingPRs := []*provreqwrapper.ProvisioningRequest{
		provreqstate.ProvisioningRequestInStateForTests("default", "PendingState0", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "PendingState1", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "PendingState2", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
	}
	otherPRs := []*provreqwrapper.ProvisioningRequest{
		provreqstate.ProvisioningRequestInStateForTests("default", "AcceptedState", "", "", provreqstate.AcceptedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "ProvisionedState", "", "", provreqstate.ProvisionedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "BookingExpiredState", "", "", provreqstate.BookingExpiredState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "CapacityRevokedState", "", "", provreqstate.CapacityRevokedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "FailedState", "", "", provreqstate.FailedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "UninitializedState", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "PendingWithResizeRequestAnMIGNames", "test-name-resize-request", "test-name-mig", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
	}
	tests := []struct {
		name                  string
		unschedulablePods     []*apiv1.Pod
		upcomingNGProvReqs    []*provreqwrapper.ProvisioningRequest
		initializedNGProvReqs []*provreqwrapper.ProvisioningRequest
		provisioningRequests  []*provreqwrapper.ProvisioningRequest
		// want fields
		wantPods        []*apiv1.Pod
		wantPodsFromPRs []*provreqwrapper.ProvisioningRequest
	}{
		{
			name:                 "simple test",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                 "couple of ProvReqs",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{pendingPRs[1], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[1], pendingPRs[0]},
		},
		{
			name:                 "some other pods",
			unschedulablePods:    []*apiv1.Pod{mockedPod(42, ""), mockedPod(55, "")},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{pendingPRs[1], pendingPRs[0]},
			wantPods:             []*apiv1.Pod{mockedPod(42, ""), mockedPod(55, "")},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[1], pendingPRs[0]},
		},
		{
			name:                 "more pods and ProvReqs",
			unschedulablePods:    []*apiv1.Pod{mockedPod(42, ""), mockedPod(64, ""), mockedPod(55, "")},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{pendingPRs[2], pendingPRs[1], pendingPRs[0]},
			wantPods:             []*apiv1.Pod{mockedPod(42, ""), mockedPod(64, ""), mockedPod(55, "")},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[2], pendingPRs[1], pendingPRs[0]},
		},
		{
			name:                 "some pods consuming ProvReq",
			unschedulablePods:    []*apiv1.Pod{mockedPod(42, "test1"), mockedPod(64, ""), mockedPod(55, "test2")},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{pendingPRs[1], pendingPRs[0]},
			wantPods:             []*apiv1.Pod{mockedPod(42, "test1"), mockedPod(64, ""), mockedPod(55, "test2")},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[1], pendingPRs[0]},
		},
		{
			name:                 "accepted ProvReq is ignored",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{otherPRs[0], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                 "provisioned ProvReq is ignored",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{otherPRs[1], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                 "booking expired ProvReq is ignored",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{otherPRs[2], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                 "capacity revoked ProvReq is ignored",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{otherPRs[3], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                 "failed ProvReq is ignored",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{otherPRs[4], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                 "uninitialized ProvReq is ignored",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{otherPRs[5], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                 "pending ProvReq with RR and Mig set is ignored",
			unschedulablePods:    []*apiv1.Pod{},
			provisioningRequests: []*provreqwrapper.ProvisioningRequest{otherPRs[6], pendingPRs[0]},
			wantPodsFromPRs:      []*provreqwrapper.ProvisioningRequest{pendingPRs[0]},
		},
		{
			name:                  "upcoming ProvReq with RR and Mig set is ignored",
			provisioningRequests:  []*provreqwrapper.ProvisioningRequest{pendingPRs[0], pendingPRs[1], pendingPRs[2]},
			upcomingNGProvReqs:    []*provreqwrapper.ProvisioningRequest{pendingPRs[0], pendingPRs[1]},
			initializedNGProvReqs: []*provreqwrapper.ProvisioningRequest{pendingPRs[1]},
			wantPodsFromPRs:       []*provreqwrapper.ProvisioningRequest{pendingPRs[1], pendingPRs[2]},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.provisioningRequests...)
			queuedProvisioningCache := provreqcache.NewQueuedProvisioningCache(fakeClient)
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			experimentsManager := experiments.NewMockManager()
			getExpectedPods := func(prs ...*provreqwrapper.ProvisioningRequest) []*apiv1.Pod {
				t.Helper()
				result := []*apiv1.Pod{}
				for _, pr := range prs {
					pods, err := pr_pods.PodsForProvisioningRequest(provider, experimentsManager, pr)
					if err != nil {
						t.Fatalf("Got error while getting expected pods: %v", err)
					}
					result = append(result, pods...)
				}
				return result
			}

			p := NewProvisioningRequestPodListProcessor(fakeClient, queuedProvisioningCache, nil, 0, experimentsManager)
			p.cache = newEventCache(10)
			p.now = nowFunc

			context := &ca_context.AutoscalingContext{
				CloudProvider: provider,
			}
			for _, pr := range tt.upcomingNGProvReqs {
				queuedProvisioningCache.RegisterUpcomingProvReq(pr_pods.GetProvReqID(pr))
			}
			for _, pr := range tt.initializedNGProvReqs {
				queuedProvisioningCache.UnregisterUpcomingProvReq(pr_pods.GetProvReqID(pr))
			}

			queuedProvisioningCache.Refresh()
			got, err := p.InjectProvisioningRequestPods(context, tt.unschedulablePods)
			if err != nil {
				t.Errorf("ProvisioningRequestPodListProcessor.Process() error = %v", err)
				return
			}
			if tt.wantPods == nil {
				tt.wantPods = []*apiv1.Pod{}
			}
			if tt.wantPodsFromPRs != nil {
				tt.wantPods = append(tt.wantPods, getExpectedPods(tt.wantPodsFromPRs...)...)
			}
			sort.Slice(tt.wantPods, lessPod(tt.wantPods))
			sort.Slice(got, lessPod(got))
			if diff := cmp.Diff(tt.wantPods, got); diff != "" {
				t.Errorf("Wrong ProvisioningRequestPodListProcessor.Process() in %q diff (-want +got):\n%s", tt.name, diff)
			}
		})
	}
}

func lessPod(s []*apiv1.Pod) func(x, y int) bool {
	return func(x, y int) bool {
		return s[x].Name < s[y].Name
	}
}

func TestProvisioningRequestPodListProcessorIgnorePodsConsumingProvisioningRequest(t *testing.T) {
	defaultNow := exampleInitTime.Add(time.Duration(5) * time.Minute)
	defaultPRs := []*provreqwrapper.ProvisioningRequest{
		provreqstate.ProvisioningRequestInStateForTests("default", "UninitializedState", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "PendingState", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "AcceptedState", "", "", provreqstate.AcceptedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "ProvisionedState", "", "", provreqstate.ProvisionedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "BookingExpiredState", "", "", provreqstate.BookingExpiredState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "CapacityRevokedState", "", "", provreqstate.CapacityRevokedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "FailedState", "", "", provreqstate.FailedState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "InvalidState", "", "", provreqstate.InvalidState, exampleInitTime, exampleTimeInc),
		provreqstate.ProvisioningRequestInStateForTests("default", "PendingWithResizeRequestAnMIGNames", "test-name-resize-request", "test-name-mig", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
	}
	missingEvent := func(prName string) string {
		return fmt.Sprintf("Warning %s Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that does not exist. Consider creating pods after creation of ProvisioningRequest.", missingPRReason, prName)
	}
	inStateEvent := func(prName, state string) string {
		return fmt.Sprintf("Normal %s Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that is in %s state. Consider creating pods after observing Provisioned condition of ProvisioningRequest.", ignoredReason, prName, state)
	}
	inStateEventToController := func(prName, state, namespace, podName string) string {
		return fmt.Sprintf("Normal %s Pod: %s/%s: Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that is in %s state. Consider creating pods after observing Provisioned condition of ProvisioningRequest.", ignoredReason, namespace, podName, prName, state)
	}
	provisionedEvent := func(prName string) string {
		return fmt.Sprintf("Warning %s Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that is in Provisioned state. This situation persisted for some time, perhaps pod spec inconsistent with ProvisioningRequest spec or pod arrived too late and will never schedule.", ignoredReason, prName)
	}
	bookingExpiredEvent := func(prName string) string {
		return fmt.Sprintf("Warning %s Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that is in BookingExpired state. The pod most likely arrived too late and will never schedule as the VM was already scaled-down.", ignoredReason, prName)
	}
	capacityRevokedEvent := func(prName string) string {
		return fmt.Sprintf("Warning %s Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that is in CapacityRevoked state. Pod arrived too late and will never schedule.", ignoredReason, prName)
	}
	failedEvent := func(prName string) string {
		return fmt.Sprintf("Warning %s Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that is in Failed state, with a following reason and message: [Failed] \"Provisioning Request has failed.\".", failedPRReason, prName)
	}
	invalidEvent := func(prName string) string {
		return fmt.Sprintf("Warning %s Unschedulable pod ignored in scale-up loop, because it's consuming ProvisioningRequest default/%s that is in Invalid state.", internalErrorReason, prName)
	}
	type eventEntry struct {
		kind       string
		namespace  string
		name       string
		lastUpdate time.Time
	}
	tests := []struct {
		name                       string
		eventEntries               []eventEntry
		unschedulablePods          []*apiv1.Pod
		nowTime                    time.Time
		maxPodEventsPerLoop        int
		maxControllerEventsPerLoop int
		// want fields
		wantEvents []string
		want       []*apiv1.Pod
	}{
		{
			name:              "simple test",
			unschedulablePods: []*apiv1.Pod{},

			want: []*apiv1.Pod{},
		},
		{
			name:              "couple of ProvReqs",
			unschedulablePods: []*apiv1.Pod{},

			want: []*apiv1.Pod{},
		},
		{
			name:              "some other pods",
			unschedulablePods: []*apiv1.Pod{mockedPod(42, ""), mockedPod(55, "")},

			want: []*apiv1.Pod{mockedPod(42, ""), mockedPod(55, "")},
		},
		{
			name:              "more pods and ProvReqs",
			unschedulablePods: []*apiv1.Pod{mockedPod(42, ""), mockedPod(64, ""), mockedPod(55, "")},

			want: []*apiv1.Pod{mockedPod(42, ""), mockedPod(64, ""), mockedPod(55, "")},
		},
		{
			name:                       "some pods consuming ProvReq",
			unschedulablePods:          []*apiv1.Pod{mockedPod(42, "UninitializedState"), mockedPod(64, ""), mockedPod(55, "PendingState")},
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{inStateEvent("UninitializedState", "Uninitialized"), inStateEvent("PendingState", "Pending")},
			want:       []*apiv1.Pod{mockedPod(64, "")},
		},
		{
			name:                       "all pods consuming ProvReq",
			unschedulablePods:          []*apiv1.Pod{mockedPodWithOptions(42, "UninitializedState", withDaemonSetOwnerRef()), mockedPodWithOptions(64, "PendingState", withDaemonSetOwnerRef()), mockedPodWithOptions(55, "AcceptedState", withDaemonSetOwnerRef())},
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{inStateEvent("UninitializedState", "Uninitialized"), inStateEvent("PendingState", "Pending"), inStateEvent("AcceptedState", "Accepted"), inStateEventToController("UninitializedState", "Uninitialized", "default", "testname-42"), inStateEventToController("PendingState", "Pending", "default", "testname-64"), inStateEventToController("AcceptedState", "Accepted", "default", "testname-55")},
			want:       []*apiv1.Pod{},
		},
		{
			name:                       "missing, provisioned, failed and invalid PRs",
			unschedulablePods:          []*apiv1.Pod{mockedPod(1, "MissingPR"), mockedPod(5, "ProvisionedState"), mockedPod(6, "FailedState"), mockedPod(7, "InvalidState")},
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{missingEvent("MissingPR"), failedEvent("FailedState"), invalidEvent("InvalidState")},
			want:       []*apiv1.Pod{},
		},
		{
			name:                       "provisioned logged after 30 minutes",
			unschedulablePods:          []*apiv1.Pod{mockedPod(1, "ProvisionedState")},
			nowTime:                    defaultNow.Add(provisionedUnschedulableTimeout),
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{provisionedEvent("ProvisionedState")},
			want:       []*apiv1.Pod{},
		},
		{
			name:                       "booking expired logged",
			unschedulablePods:          []*apiv1.Pod{mockedPod(1, "BookingExpiredState")},
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{bookingExpiredEvent("BookingExpiredState")},
			want:       []*apiv1.Pod{},
		},
		{
			name:                       "capacity revoked logged",
			unschedulablePods:          []*apiv1.Pod{mockedPod(1, "CapacityRevokedState")},
			nowTime:                    defaultNow.Add(provisionedUnschedulableTimeout),
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{capacityRevokedEvent("CapacityRevokedState")},
			want:       []*apiv1.Pod{},
		},
		{
			name: "some events are skipped due to recent events",
			eventEntries: []eventEntry{
				{"Pod", "default", "testname-42", defaultNow.Add(time.Duration(-3) * time.Minute)},
				{"Pod", "default", "testname-55", defaultNow.Add(time.Duration(-4) * time.Minute)},
				{"Pod", "default", "testname-64", defaultNow.Add(time.Duration(-6) * time.Minute)},
				{"DaemonSet", "default", "testname-42", defaultNow.Add(time.Duration(-3) * time.Minute)},
				{"DaemonSet", "default", "testname-64", defaultNow.Add(time.Duration(-4) * time.Minute)},
				{"DaemonSet", "default", "testname-55", defaultNow.Add(time.Duration(-6) * time.Minute)},
			},
			unschedulablePods:          []*apiv1.Pod{mockedPod(42, "UninitializedState"), mockedPod(64, "PendingState"), mockedPodWithOptions(55, "AcceptedState", withDaemonSetOwnerRef())},
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{inStateEvent("PendingState", "Pending"), inStateEventToController("AcceptedState", "Accepted", "default", "testname-55")},
			want:       []*apiv1.Pod{},
		},
		{
			name:                       "under pod limit, under controller limit, no controller - send event to pod",
			unschedulablePods:          []*apiv1.Pod{mockedPod(42, "UninitializedState")},
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{inStateEvent("UninitializedState", "Uninitialized")},
			want:       []*apiv1.Pod{},
		},
		{
			name:                       "above pod limit, under controller limit - send event to controller",
			unschedulablePods:          []*apiv1.Pod{mockedPodWithOptions(42, "UninitializedState", withDaemonSetOwnerRef())},
			maxPodEventsPerLoop:        0,
			maxControllerEventsPerLoop: 50,

			wantEvents: []string{inStateEventToController("UninitializedState", "Uninitialized", "default", "testname-42")},
			want:       []*apiv1.Pod{},
		},
		{
			name:                       "under pod limit, above controller limit - send event to pod",
			unschedulablePods:          []*apiv1.Pod{mockedPodWithOptions(42, "UninitializedState", withDaemonSetOwnerRef())},
			maxPodEventsPerLoop:        50,
			maxControllerEventsPerLoop: 0,

			wantEvents: []string{inStateEvent("UninitializedState", "Uninitialized")},
			want:       []*apiv1.Pod{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, defaultPRs...)
			nowFunc := func() time.Time {
				if !tt.nowTime.IsZero() {
					return tt.nowTime
				}
				return defaultNow
			}
			p := &ProvisioningRequestPodListProcessor{
				prClient:                   fakeClient,
				now:                        nowFunc,
				maxPodEventsPerLoop:        tt.maxPodEventsPerLoop,
				maxControllerEventsPerLoop: tt.maxControllerEventsPerLoop,
				cache:                      newEventCache(tt.maxPodEventsPerLoop + tt.maxControllerEventsPerLoop),
			}

			for _, entry := range tt.eventEntries {
				p.cache.setLastRecorded(entry.kind, entry.namespace, entry.name, entry.lastUpdate)
			}
			fakeRecorder := kube_record.NewFakeRecorder(len(tt.wantEvents))
			ca_ctx := &ca_context.AutoscalingContext{
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
					ClientSet: fake.NewSimpleClientset(),
					Recorder:  fakeRecorder,
				},
			}
			got := p.IgnorePodsConsumingProvisioningRequest(ca_ctx, tt.unschedulablePods)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ProvisioningRequestPodListProcessor.Process() = %v\n want %v", got, tt.want)
			}
			assertExpectedEvents(t, fakeRecorder.Events, tt.wantEvents)
		})
	}
}

func withDaemonSetOwnerRef() podOption {
	return func(pod *apiv1.Pod) {
		pod.OwnerReferences = test.GenerateOwnerReferences(fmt.Sprintf("%s-ds", pod.Name), "DaemonSet", "apps/v1", "")
	}
}

type podOption func(*apiv1.Pod)

func mockedPodWithOptions(id int, consumingProvReq string, opts ...podOption) *apiv1.Pod {
	pod := mockedPod(id, consumingProvReq)
	for _, opt := range opts {
		opt(pod)
	}
	return pod
}

func mockedPod(id int, consumingProvReq string) *apiv1.Pod {
	annotations := map[string]string{}
	if consumingProvReq != "" {
		annotations[pods.DeprecatedProvisioningRequestPodAnnotationKey] = consumingProvReq
	}
	return &apiv1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        fmt.Sprintf("testname-%d", id),
			Annotations: annotations,
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Image: fmt.Sprintf("test-image-%d", id),
				},
			},
		},
	}
}

func assertExpectedEvents(t *testing.T, events chan string, wantEvents []string) {
	if wantEvents == nil {
		wantEvents = make([]string, 0)
	}

	close(events)
	goEvents := make([]string, 0, len(events))
	for event := range events {
		goEvents = append(goEvents, event)
	}

	sort.Strings(goEvents)
	sort.Strings(wantEvents)
	if !reflect.DeepEqual(goEvents, wantEvents) {
		t.Errorf("Events that were recorded = %v\n want %v", goEvents, wantEvents)
	}
}
