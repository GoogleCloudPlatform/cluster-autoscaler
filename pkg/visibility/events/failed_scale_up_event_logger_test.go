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

package events

import (
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

func TestEmitEventsFromFailures(t *testing.T) {
	eventLogger := failedScaleUpEventLoggerImpl{}
	tcs := map[string]struct {
		failures       []*vistypes.Message
		pods           []*vistypes.Pod
		expectedEvents []string
		ngs            []cloudprovider.NodeGroup
	}{
		"Other error should be mapped to internal error event": {
			failures:       []*vistypes.Message{vistypes.NewScaleUpErrorOtherMsg("")},
			ngs:            []cloudprovider.NodeGroup{makeMig("us-central1")},
			pods:           []*vistypes.Pod{makePod("pod1", "ns1")},
			expectedEvents: []string{"Warning FailedScaleUp Node scale up in zones us-central1 associated with this pod failed: Internal error. Pod is at risk of not being scheduled."},
		},
		"Same failure in multiple zones should be mapped to one event": {
			failures:       []*vistypes.Message{vistypes.NewScaleUpErrorQuotaExceededMsg("ng1"), vistypes.NewScaleUpErrorQuotaExceededMsg("ng2")},
			ngs:            []cloudprovider.NodeGroup{makeMig("us-central1-c"), makeMig("us-central1-a")},
			pods:           []*vistypes.Pod{makePod("pod1", "ns1")},
			expectedEvents: []string{"Warning FailedScaleUp Node scale up in zones us-central1-c, us-central1-a associated with this pod failed: GCE quota exceeded. Pod is at risk of not being scheduled."},
		},
		"Different failures should be mapped to different events": {
			failures: []*vistypes.Message{vistypes.NewScaleUpErrorQuotaExceededMsg("ng1"), vistypes.NewScaleUpErrorOutOfResourcesMsg("ng2")},
			ngs:      []cloudprovider.NodeGroup{makeMig("us-central1-c"), makeMig("us-central1-a")},
			pods:     []*vistypes.Pod{makePod("pod1", "ns1")},
			expectedEvents: []string{
				"Warning FailedScaleUp Node scale up in zones us-central1-c, us-central1-a associated with this pod failed: GCE quota exceeded, GCE out of resources. Pod is at risk of not being scheduled.",
			},
		},
		"For each pod there should be an event emitted": {
			failures: []*vistypes.Message{vistypes.NewScaleUpErrorQuotaExceededMsg("ng1"), vistypes.NewScaleUpErrorQuotaExceededMsg("ng1")},
			ngs:      []cloudprovider.NodeGroup{makeMig("us-central1-c"), makeMig("us-central1-a")},
			pods:     []*vistypes.Pod{makePod("pod1", "ns1"), makePod("pod2", "ns1")},
			expectedEvents: []string{
				"Warning FailedScaleUp Node scale up in zones us-central1-c, us-central1-a associated with this pod failed: GCE quota exceeded. Pod is at risk of not being scheduled.",
				"Warning FailedScaleUp Node scale up in zones us-central1-c, us-central1-a associated with this pod failed: GCE quota exceeded. Pod is at risk of not being scheduled.",
			},
		},
		"Different internal errors should be mapped to one event": {
			failures:       []*vistypes.Message{vistypes.NewScaleUpErrorOtherMsg("ng1"), vistypes.NewScaleUpErrorOtherMsg("ng2")},
			ngs:            []cloudprovider.NodeGroup{makeMig("us-central1-c"), makeMig("us-central1-a")},
			pods:           []*vistypes.Pod{makePod("pod1", "ns1")},
			expectedEvents: []string{"Warning FailedScaleUp Node scale up in zones us-central1-c, us-central1-a associated with this pod failed: Internal error. Pod is at risk of not being scheduled."},
		},
	}
	for description, tc := range tcs {
		t.Run(description, func(t *testing.T) {

			fakeRecorder := kube_record.NewFakeRecorder(5)
			fakeRecorder.IncludeObject = false
			ctx := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					Recorder: fakeRecorder,
				},
			}
			eventLogger.EmitEventsFromFailure(ctx, tc.pods, tc.ngs, tc.failures)
			i := 0
			close(fakeRecorder.Events)
			for event := range fakeRecorder.Events {
				if i < len(tc.expectedEvents) && event != tc.expectedEvents[i] {
					t.Errorf("Incorrect event: expected: %v, got: %v.", tc.expectedEvents[i], event)
				}
				i++
			}
			if i != len(tc.expectedEvents) {
				t.Errorf("Incorrect number of received events: expected: %d, got: %d.", len(tc.expectedEvents), i)
			}
		})
	}
}

func makePod(name, namespace string) *vistypes.Pod {
	return &vistypes.Pod{
		Name:      name,
		Namespace: namespace,
	}
}

func makeMig(zone string) *gke.GkeMig {
	return gke.NewTestGkeMigBuilder().
		SetGceRefZone(zone).
		SetMinSize(1).
		SetMaxSize(1).
		SetExist(true).
		Build()
}
