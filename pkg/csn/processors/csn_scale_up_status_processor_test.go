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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

const (
	// smallNodeGroup is small enough to be suspended, largeNodeGroup is not.
	smallNodeGroupID = "small-node-group"
	largeNodeGroupID = "large-node-group"

	// defaultBufferName is the buffer most cases use. Node templates carry its buffer assignment
	// label so that only the memory limit term can reject them.
	defaultBufferName = "buffer"

	// nodeGroupLabel marks each node group template with its own id.
	nodeGroupLabel = "example.com/node-group"

	memoryLimitMessageFragment = "nodes with less memory"
)

// testEnv holds the fixtures shared by the processor test cases.
type testEnv struct {
	provider     *testprovider.TestCloudProvider
	registry     *fakepods.Registry
	buffer       *v1beta1.CapacityBuffer
	otherBuffer  *v1beta1.CapacityBuffer
	fakeRecorder *kube_record.FakeRecorder
	ctx          *ca_context.AutoscalingContext
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	provider := testprovider.NewTestCloudProviderBuilder().WithMachineTemplates(map[string]*framework.NodeInfo{
		smallNodeGroupID: framework.NewTestNodeInfo(nodeWithMemoryScalingLevel(t, smallNodeGroupID, 64)),
		largeNodeGroupID: framework.NewTestNodeInfo(nodeWithMemoryScalingLevel(t, largeNodeGroupID, 256)),
	}).Build()
	provider.AddNodeGroup(smallNodeGroupID, 0, 10, 0)
	provider.AddNodeGroup(largeNodeGroupID, 0, 10, 0)

	fakeRecorder := kube_record.NewFakeRecorder(20)
	return &testEnv{
		provider:     provider,
		registry:     fakepods.NewRegistry(nil),
		buffer:       capacityBuffer(defaultBufferName, "buffer-uid"),
		otherBuffer:  capacityBuffer("other-buffer", "other-buffer-uid"),
		fakeRecorder: fakeRecorder,
		ctx: &ca_context.AutoscalingContext{
			AutoscalingKubeClients: ca_context.AutoscalingKubeClients{Recorder: fakeRecorder},
		},
	}
}

// events drains the fake recorder.
func (e *testEnv) events() []string {
	var events []string
	for {
		select {
		case event := <-e.fakeRecorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// csnPodForBuffer builds a standby buffer fake pod registered against buffer.
func (e *testEnv) csnPodForBuffer(name string, memoryBytes int64, buffer *v1beta1.CapacityBuffer, options ...func(*apiv1.Pod)) *apiv1.Pod {
	pod := test.BuildTestPod(name, 100, memoryBytes, options...)
	csn.MakePodCSN(pod, "default/"+buffer.Name)
	e.registry.SetCapacityBuffer(pod.UID, buffer)
	return pod
}

func TestCSNScaleUpStatusProcessor(t *testing.T) {
	// A request above 209 GB cannot fit on any suspendable node.
	overLimitMemory := int64(300) * units.GB
	smallMemory := int64(1) * units.GB

	testCases := []struct {
		name string
		// buildStatus receives the prepared environment and returns the status to process.
		buildStatus func(e *testEnv) *status.ScaleUpStatus
		boolFlags   map[string]bool
		stringFlags map[string]string
		wantEvents  int
		// wantMessageFragment is checked against the single expected event.
		wantMessageFragment string
	}{
		{
			name: "request above the memory limit",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := e.csnPodForBuffer("over-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
			wantEvents:          1,
			wantMessageFragment: memoryLimitMessageFragment,
		},
		{
			name: "small request blocked only by the injected memory affinity",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				// The pod also demands a large machine, so only the large node group could host
				// it, and only the injected memory term rejects that group.
				pod := e.csnPodForBuffer("needs-large", smallMemory, e.buffer, withNodeSelector(map[string]string{nodeGroupLabel: largeNodeGroupID}))
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					ConsideredNodeGroups:    e.provider.NodeGroups(t.Context()),
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod, RejectedNodeGroups: rejectedFor(pod, largeNodeGroupID, "NodeAffinity")}},
				}
			},
			wantEvents:          1,
			wantMessageFragment: memoryLimitMessageFragment,
		},
		{
			name: "rejected for insufficient cpu only",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := e.csnPodForBuffer("needs-cpu", smallMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					ConsideredNodeGroups:    e.provider.NodeGroups(t.Context()),
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod, RejectedNodeGroups: rejectedFor(pod, smallNodeGroupID, "NodeResourcesFit")}},
				}
			},
		},
		{
			name: "over-limit node group the pod's own selector also rejects",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				// The large node group is over the limit, but it lacks the label the pod asks
				// for, so lifting the limit would not have helped. Not our story to tell.
				pod := e.csnPodForBuffer("needs-label", smallMemory, e.buffer, withNodeSelector(map[string]string{"example.com/unavailable": "true"}))
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					ConsideredNodeGroups:    e.provider.NodeGroups(t.Context()),
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod, RejectedNodeGroups: rejectedFor(pod, largeNodeGroupID, "NodeAffinity")}},
				}
			},
		},
		{
			name: "regular workload pod",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := test.BuildTestPod("regular", 100, overLimitMemory)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
		},
		{
			name: "standby buffer pod missing from the registry",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := test.BuildTestPod("unregistered", 100, overLimitMemory)
				csn.MakePodCSN(pod, "default/buffer")
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
		},
		{
			name: "several replicas of one buffer produce one event",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				var infos []status.NoScaleUpInfo
				for i := 0; i < 5; i++ {
					pod := e.csnPodForBuffer(fmt.Sprintf("replica-%d", i), overLimitMemory, e.buffer)
					infos = append(infos, status.NoScaleUpInfo{Pod: pod})
				}
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: infos,
				}
			},
			wantEvents:          1,
			wantMessageFragment: memoryLimitMessageFragment,
		},
		{
			name: "unattributable and blocked replicas of one buffer produce one event",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				unattributablePod := e.csnPodForBuffer("unattributable", smallMemory, e.buffer, withNodeSelector(map[string]string{"example.com/unavailable": "true"}))
				blockedPod := e.csnPodForBuffer("blocked", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:               status.ScaleUpNoOptionsAvailable,
					ConsideredNodeGroups: e.provider.NodeGroups(t.Context()),
					PodsRemainUnschedulable: []status.NoScaleUpInfo{
						{Pod: unattributablePod, RejectedNodeGroups: rejectedFor(unattributablePod, smallNodeGroupID, "NodeAffinity")},
						{Pod: blockedPod},
					},
				}
			},
			wantEvents:          1,
			wantMessageFragment: memoryLimitMessageFragment,
		},
		{
			name: "two buffers produce two events",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				first := e.csnPodForBuffer("first", overLimitMemory, e.buffer)
				second := e.csnPodForBuffer("second", overLimitMemory, e.otherBuffer)
				return &status.ScaleUpStatus{
					Result: status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{
						{Pod: first},
						{Pod: second},
					},
				}
			},
			wantEvents: 2,
		},
		{
			name: "successful scale-up of an unrelated workload does not silence the event",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := e.csnPodForBuffer("over-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpSuccessful,
					PodsTriggeredScaleUp:    []*apiv1.Pod{test.BuildTestPod("unrelated", 100, smallMemory)},
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
			wantEvents:          1,
			wantMessageFragment: memoryLimitMessageFragment,
		},
		{
			name: "pod over limit with nil considered node groups",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := e.csnPodForBuffer("over-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpError,
					ConsideredNodeGroups:    nil,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod, RejectedNodeGroups: rejectedFor(pod, largeNodeGroupID, "NodeAffinity")}},
				}
			},
			wantEvents:          1,
			wantMessageFragment: memoryLimitMessageFragment,
		},
		{
			name: "direct launch flag off",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := e.csnPodForBuffer("over-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
			boolFlags: map[string]bool{experiments.ColdStandbyNodesScaleUpStatusProcessorFlag: false},
		},
		{
			name: "limit raised above the request",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				pod := e.csnPodForBuffer("over-default-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
			stringFlags: map[string]string{experiments.ColdStandbyNodesMinUnsupportedMemoryGBFlag: "400"},
		},
		{
			name: "skip buffers without provisioning strategy",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				e.buffer.Spec.ProvisioningStrategy = nil
				e.buffer.Status.ProvisioningStrategy = nil
				pod := e.csnPodForBuffer("over-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
		},
		{
			name: "skip buffers with a different provisioning strategy in spec and status",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				e.buffer.Spec.ProvisioningStrategy = new("some-other-strategy")
				e.buffer.Status.ProvisioningStrategy = new("some-other-strategy")
				pod := e.csnPodForBuffer("over-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
		},
		{
			name: "skip buffers with a different provisioning strategy in status",
			buildStatus: func(e *testEnv) *status.ScaleUpStatus {
				e.buffer.Status.ProvisioningStrategy = new("some-other-strategy")
				pod := e.csnPodForBuffer("over-limit", overLimitMemory, e.buffer)
				return &status.ScaleUpStatus{
					Result:                  status.ScaleUpNoOptionsAvailable,
					PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}},
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, tc.stringFlags)
			processor := NewCSNScaleUpStatusProcessor(env.registry, experimentsManager)

			processor.Process(t.Context(), env.ctx, tc.buildStatus(env))

			events := env.events()
			assert.Len(t, events, tc.wantEvents)
			for _, event := range events {
				assert.Contains(t, event, csnScaleUpFailedReason)
			}
			if tc.wantMessageFragment != "" {
				assert.Len(t, events, 1)
				assert.Contains(t, events[0], tc.wantMessageFragment)
			}
		})
	}
}

// nodeWithMemoryScalingLevel builds a node group template that a standby buffer pod for
// defaultBufferName would accept, apart from its memory size.
func nodeWithMemoryScalingLevel(t *testing.T, name string, memoryGB int64) *apiv1.Node {
	t.Helper()
	node := test.BuildTestNode(name, 8000, memoryGB*units.GB)
	node, err := csn.SetNodeAs(node, csn.NodeStateChilling)
	if err != nil {
		t.Fatalf("csn.SetNodeAs(%q, %v) returned error: %v", name, csn.NodeStateChilling, err)
	}
	bufferId := "default/" + defaultBufferName
	node, err = assignNodeToBufferForProcessors(node, bufferId)
	if err != nil {
		t.Fatalf("assignNodeToBufferForProcessors(%q, %q) returned error: %v", name, bufferId, err)
	}

	node.Labels[labels.MemoryScalingLevelLabel] = strconv.FormatInt(memoryGB, 10)
	// Lets a test pod demand this particular shape without mentioning its memory size, which
	// would make the pod's own nodeSelector a second reason for the node to be rejected.
	node.Labels[nodeGroupLabel] = name
	return node
}

func rejectedFor(pod *apiv1.Pod, nodeGroupID, predicateName string) map[string]status.Reasons {
	reason := "node(s) didn't match Pod's node affinity/selector"
	if !strings.EqualFold(predicateName, "NodeAffinity") {
		reason = "insufficient " + predicateName
	}
	return map[string]status.Reasons{
		nodeGroupID: clustersnapshot.NewFailingPredicateError(pod, predicateName, []string{reason}, "", ""),
	}
}

func capacityBuffer(name, uid string) *v1beta1.CapacityBuffer {
	return &v1beta1.CapacityBuffer{
		Status: v1beta1.CapacityBufferStatus{
			ProvisioningStrategy: new(capacitybuffers.ColdProvisioningStrategy),
		},
		Spec: v1beta1.CapacityBufferSpec{
			ProvisioningStrategy: new(capacitybuffers.ColdProvisioningStrategy),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(uid),
		},
	}
}

// Ensure the processor satisfies the interface the chain expects.
var _ status.ScaleUpStatusProcessor = &CSNScaleUpStatusProcessor{}
