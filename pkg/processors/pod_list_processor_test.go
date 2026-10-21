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
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/drainability"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type mockScaleToZeroProcessor struct {
	called bool
}

func (m *mockScaleToZeroProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	return unschedulablePods, nil
}

func (m *mockScaleToZeroProcessor) Drainable(drainContext *drainability.DrainContext, pod *v1.Pod, node *framework.NodeInfo) drainability.Status {
	return drainability.NewUndefinedStatus()
}

func (m *mockScaleToZeroProcessor) Name() string {
	return "mockScaleToZeroProcessor"
}

type mockDefaultPodListProcessor struct {
	called                  bool
	scaleToZeroCalledBefore bool
	scaleToZero             *mockScaleToZeroProcessor
}

func (m *mockDefaultPodListProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	if m.scaleToZero != nil {
		m.scaleToZeroCalledBefore = m.scaleToZero.called
	}
	return unschedulablePods, nil
}

func (m *mockDefaultPodListProcessor) CleanUp() {}

func TestScaleToZeroLateRun(t *testing.T) {
	testCases := []struct {
		name                   string
		scaleToZeroLateRunFlag bool
		expectScaleToZeroEarly bool
	}{
		{
			name:                   "ScaleToZero late run disabled",
			scaleToZeroLateRunFlag: false,
			expectScaleToZeroEarly: true,
		},
		{
			name:                   "ScaleToZero late run enabled",
			scaleToZeroLateRunFlag: true,
			expectScaleToZeroEarly: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			boolFlags := map[string]bool{
				experiments.ScaleToZeroLateRunFlag: tc.scaleToZeroLateRunFlag,
			}
			em := experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{})

			scaleToZero := &mockScaleToZeroProcessor{}
			defaultPodListProcessor := &mockDefaultPodListProcessor{scaleToZero: scaleToZero}

			processor := &GkeInternalPodListProcessor{
				scaleToZeroProcessor:      scaleToZero,
				defaultPodListerProcessor: defaultPodListProcessor,
				experimentsManager:        em,
				podStatusAggregator:       metrics_processors.NewPodStatusAggregator(),
			}

			processor.Process(context.TODO(), &ca_context.AutoscalingContext{}, []*v1.Pod{})

			if !scaleToZero.called {
				t.Errorf("Expected scaleToZeroProcessor to be called")
			}
			if !defaultPodListProcessor.called {
				t.Errorf("Expected defaultPodListProcessor to be called")
			}

			if defaultPodListProcessor.scaleToZeroCalledBefore != tc.expectScaleToZeroEarly {
				t.Errorf("Expected scaleToZeroProcessor called before defaultPodListProcessor: %v, got: %v", tc.expectScaleToZeroEarly, defaultPodListProcessor.scaleToZeroCalledBefore)
			}
		})
	}
}

type mockDefragProcessor struct {
	called          bool
	cleanedUp       bool
	pickedCandidate bool
	podsToReturn    []*v1.Pod
	err             error
}

func (m *mockDefragProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	if !m.pickedCandidate {
		return unschedulablePods, m.err
	}
	if m.podsToReturn != nil {
		return m.podsToReturn, m.err
	}
	return unschedulablePods, m.err
}

func (m *mockDefragProcessor) DefragPickedCandidate() bool {
	return m.pickedCandidate
}

func (m *mockDefragProcessor) CleanUp() {
	m.cleanedUp = true
}

type mockCSNBufferConsumptionProcessor struct {
	called       bool
	cleanedUp    bool
	receivedPods []*v1.Pod
	podsToReturn []*v1.Pod
	err          error
}

func (m *mockCSNBufferConsumptionProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	m.receivedPods = unschedulablePods
	if m.podsToReturn != nil {
		return m.podsToReturn, m.err
	}
	return unschedulablePods, m.err
}

func (m *mockCSNBufferConsumptionProcessor) CleanUp() {
	m.cleanedUp = true
}

func TestNewGkeInternalPodListProcessorNil(t *testing.T) {
	processor := NewGkeInternalPodListProcessor(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if processor.defragProcessor != nil {
		t.Errorf("Expected defragProcessor to be strictly nil")
	}
	if processor.csnBufferConsumptionProcessor != nil {
		t.Errorf("Expected csnBufferConsumptionProcessor to be strictly nil")
	}
}

func TestDefragAndCSNBufferConsumptionInPodListProcessor(t *testing.T) {
	p1 := test.BuildTestPod("p1", 100, 1)
	p2 := test.BuildTestPod("p2", 200, 2)

	initialPods := []*v1.Pod{p1}

	testCases := []struct {
		name                         string
		defragPickedCandidate        bool
		defragErr                    error
		defragReturnPods             []*v1.Pod
		csnErr                       error
		expectCSNReceivedPods        []*v1.Pod
		expectErr                    bool
		expectUnschedulablePodsCount int
	}{
		{
			name:                         "Defrag candidate not picked, CSN receives unschedulable pods",
			defragPickedCandidate:        false,
			defragReturnPods:             []*v1.Pod{p1, p2},
			expectCSNReceivedPods:        []*v1.Pod{p1},
			expectUnschedulablePodsCount: 1,
		},
		{
			name:                         "Defrag picked candidate, CSN receives nil pods",
			defragPickedCandidate:        true,
			defragReturnPods:             []*v1.Pod{p1, p2},
			expectCSNReceivedPods:        nil,
			expectUnschedulablePodsCount: 2,
		},
		{
			name:                         "Defrag fails with error, defragPickedCandidate treated as false",
			defragPickedCandidate:        true, // Set on mock to ensure DefragPickedCandidate() isn't queried on error
			defragErr:                    errors.New("defrag failed"),
			defragReturnPods:             []*v1.Pod{p1, p2},
			expectCSNReceivedPods:        []*v1.Pod{p1},
			expectUnschedulablePodsCount: 1,
		},
		{
			name:                  "CSN processor fails with error",
			defragPickedCandidate: false,
			defragReturnPods:      []*v1.Pod{p1},
			csnErr:                errors.New("csn failed"),
			expectCSNReceivedPods: []*v1.Pod{p1},
			expectErr:             true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defragMock := &mockDefragProcessor{
				pickedCandidate: tc.defragPickedCandidate,
				err:             tc.defragErr,
				podsToReturn:    tc.defragReturnPods,
			}
			csnMock := &mockCSNBufferConsumptionProcessor{
				err: tc.csnErr,
			}
			defaultPodListProcessor := &mockDefaultPodListProcessor{}

			processor := &GkeInternalPodListProcessor{
				defaultPodListerProcessor:     defaultPodListProcessor,
				defragProcessor:               defragMock,
				csnBufferConsumptionProcessor: csnMock,
				podStatusAggregator:           metrics_processors.NewPodStatusAggregator(),
				experimentsManager:            experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, map[string]string{}),
			}

			res, err := processor.Process(context.TODO(), &ca_context.AutoscalingContext{}, initialPods)
			if (err != nil) != tc.expectErr {
				t.Fatalf("Expected error: %v, got: %v", tc.expectErr, err)
			}
			if tc.expectErr {
				return
			}

			if !defragMock.called {
				t.Errorf("Expected defragProcessor to be called")
			}
			if !csnMock.called {
				t.Errorf("Expected csnBufferConsumptionProcessor to be called")
			}
			if diff := cmp.Diff(tc.expectCSNReceivedPods, csnMock.receivedPods); diff != "" {
				t.Errorf("csnBufferConsumptionProcessor received unexpected pods diff (-want +got):\n%s", diff)
			}
			if len(res) != tc.expectUnschedulablePodsCount {
				t.Errorf("Expected %d unschedulable pods returned, got %d", tc.expectUnschedulablePodsCount, len(res))
			}
		})
	}
}

func TestGkeInternalPodListProcessorCleanUp(t *testing.T) {
	defragMock := &mockDefragProcessor{}
	csnMock := &mockCSNBufferConsumptionProcessor{}
	defaultPodListProcessor := &mockDefaultPodListProcessor{}

	processor := &GkeInternalPodListProcessor{
		defaultPodListerProcessor:     defaultPodListProcessor,
		defragProcessor:               defragMock,
		csnBufferConsumptionProcessor: csnMock,
	}

	processor.CleanUp()

	if !defragMock.cleanedUp {
		t.Errorf("Expected defragProcessor.CleanUp to be called")
	}
	if !csnMock.cleanedUp {
		t.Errorf("Expected csnBufferConsumptionProcessor.CleanUp to be called")
	}
}

type observedReaction struct {
	systemPod    bool
	reactionType metrics.ReactionType
}

type mockReactionTimeMetrics struct {
	reactions []observedReaction
}

func (m *mockReactionTimeMetrics) ObserveFirstReactionTime(_ time.Duration, systemPod bool, _ bool, _ bool, reactionType metrics.ReactionType, _ string) {
	m.reactions = append(m.reactions, observedReaction{
		systemPod:    systemPod,
		reactionType: reactionType,
	})
}

// newTestPodStateObserver returns a PodStateObserver that already tracks the
// given pending pods, and the fake that records the reactions it reports.
// It must be called inside a synctest bubble.
func newTestPodStateObserver(t *testing.T, pendingPods ...*v1.Pod) (*podstate.PodStateObserver, *mockReactionTimeMetrics) {
	t.Helper()
	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	reactionMetrics := &mockReactionTimeMetrics{}
	observer, err := podstate.NewPodStateObserver(
		informerFactory,
		reactionMetrics,
		systempods.NewClassifier([]string{metav1.NamespaceSystem}),
		npc_lister.NewMockCrdLister(nil),
		false, // enablePendingPodsMetric
		false, // enablePendingPodsPerCccMetric
		-10,   // expendablePodsPriorityCutoff
		nil,   // allowedSchedulers
	)
	if err != nil {
		t.Fatalf("NewPodStateObserver() unexpected error: %v", err)
	}

	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)

	for _, pod := range pendingPods {
		if _, err := kubeClient.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Create(%s/%s) unexpected error: %v", pod.Namespace, pod.Name, err)
		}
	}
	synctest.Wait()
	return observer, reactionMetrics
}

func TestScaleToZeroObservesPodState(t *testing.T) {
	systemPod := buildTestPod(metav1.NamespaceSystem, "kube-dns", func(p *v1.Pod) {
		p.UID = types.UID("kube-dns-uid")
	})
	userPod := buildTestPod(namespaceDefault, "user-pod", func(p *v1.Pod) {
		p.UID = types.UID("user-pod-uid")
	})

	testCases := []struct {
		name                   string
		scaleToZeroLateRunFlag bool
		unschedulablePods      []*v1.Pod
		wantRemainingPods      []*v1.Pod
		wantReactions          []observedReaction
	}{
		{
			name:                   "system pod filtered on 0-node cluster observes NoActionNeeded (early run)",
			scaleToZeroLateRunFlag: false,
			unschedulablePods:      []*v1.Pod{systemPod},
			wantRemainingPods:      []*v1.Pod{},
			wantReactions: []observedReaction{
				{
					systemPod:    true,
					reactionType: metrics.NoActionNeeded,
				},
			},
		},
		{
			name:                   "system pod filtered on 0-node cluster observes NoActionNeeded (late run)",
			scaleToZeroLateRunFlag: true,
			unschedulablePods:      []*v1.Pod{systemPod},
			wantRemainingPods:      []*v1.Pod{},
			wantReactions: []observedReaction{
				{
					systemPod:    true,
					reactionType: metrics.NoActionNeeded,
				},
			},
		},
		{
			name:                   "user pod pending on 0-node cluster prevents filtering and does not observe NoActionNeeded",
			scaleToZeroLateRunFlag: false,
			unschedulablePods:      []*v1.Pod{systemPod, userPod},
			wantRemainingPods:      []*v1.Pod{systemPod, userPod},
			wantReactions:          nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			synctest.Test(t, func(t *testing.T) {
				observer, reactionMetrics := newTestPodStateObserver(t, tc.unschedulablePods...)

				classifier := systempods.NewClassifier([]string{metav1.NamespaceSystem})
				scaleToZero := NewScaleToZeroPodListProcessor(filter.NewMetricsFilter(), 5*time.Minute, classifier)
				em := experiments.NewMockManagerWithOptions(
					version.Version{},
					map[string]bool{experiments.ScaleToZeroLateRunFlag: tc.scaleToZeroLateRunFlag},
					map[string]string{},
				)

				processor := &GkeInternalPodListProcessor{
					scaleToZeroProcessor:      scaleToZero,
					defaultPodListerProcessor: &mockDefaultPodListProcessor{},
					podStateObserver:          observer,
					podStatusAggregator:       metrics_processors.NewPodStatusAggregator(),
					experimentsManager:        em,
				}

				autoscalingCtx := &ca_context.AutoscalingContext{
					ClusterSnapshot: snapshot,
				}

				gotPods, err := processor.Process(context.Background(), autoscalingCtx, tc.unschedulablePods)
				if err != nil {
					t.Fatalf("processor.Process() unexpected error: %v", err)
				}

				if diff := cmp.Diff(tc.wantRemainingPods, gotPods, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("processor.Process() remaining pods diff (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(tc.wantReactions, reactionMetrics.reactions, cmp.AllowUnexported(observedReaction{})); diff != "" {
					t.Errorf("observed reactions diff (-want +got):\n%s", diff)
				}
			})
		})
	}
}

// mockFilterOutSchedulable mimics FilterOutSchedulable: it places the pods that
// fit in the cluster snapshot, drops both them and the pods it skipped from the
// list, and sets GracefulDegradationLoopCount to the given value.
type mockFilterOutSchedulable struct {
	fits          map[*v1.Pod]string // pod -> node it fits on
	skipped       []*v1.Pod
	degradedLoops int
}

func (m *mockFilterOutSchedulable) Process(_ context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	for pod, nodeName := range m.fits {
		if err := autoscalingCtx.ClusterSnapshot.ForceAddPod(pod, nodeName); err != nil {
			return nil, err
		}
	}
	autoscalingCtx.GracefulDegradationLoopCount = m.degradedLoops
	return slices.DeleteFunc(slices.Clone(unschedulablePods), func(pod *v1.Pod) bool {
		_, fits := m.fits[pod]
		return fits || slices.Contains(m.skipped, pod)
	}), nil
}

func (m *mockFilterOutSchedulable) CleanUp() {}

func TestGracefulDegradationObservesPodState(t *testing.T) {
	newPod := func(name string) *v1.Pod {
		return buildTestPod(namespaceDefault, name, func(p *v1.Pod) {
			p.UID = types.UID(name + "-uid")
		})
	}
	fitsOnNode, fitsOnUpcomingNode, skipped := newPod("fits-on-node"), newPod("fits-on-upcoming-node"), newPod("skipped")
	noActionNeeded := observedReaction{systemPod: false, reactionType: metrics.NoActionNeeded}

	// loop is what FilterOutSchedulable does in one CA loop, and the reactions
	// observed in that loop.
	type loop struct {
		fits          map[*v1.Pod]string
		skipped       []*v1.Pod
		degradedLoops int
		wantReactions []observedReaction
	}
	testCases := []struct {
		name  string
		loops []loop
	}{
		{
			name: "not degraded: pods that fit observe NoActionNeeded",
			loops: []loop{{
				fits:          map[*v1.Pod]string{fitsOnNode: "node", fitsOnUpcomingNode: "upcoming-node"},
				wantReactions: []observedReaction{noActionNeeded, noActionNeeded},
			}},
		},
		{
			name: "degraded: pods that fit observe NoActionNeeded, skipped pod is not observed",
			loops: []loop{{
				fits:          map[*v1.Pod]string{fitsOnNode: "node", fitsOnUpcomingNode: "upcoming-node"},
				skipped:       []*v1.Pod{skipped},
				degradedLoops: 1,
				wantReactions: []observedReaction{noActionNeeded, noActionNeeded},
			}},
		},
		{
			name: "degradation ends: pod skipped while degraded observes NoActionNeeded once it fits",
			loops: []loop{
				{skipped: []*v1.Pod{skipped}, degradedLoops: 1},
				{fits: map[*v1.Pod]string{skipped: "node"}, wantReactions: []observedReaction{noActionNeeded}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			upcomingNode := test.BuildTestNode("upcoming-node", 1000, 1000)
			upcomingNode.Annotations = map[string]string{annotations.NodeUpcomingAnnotation: "true"}
			for _, node := range []*v1.Node{test.BuildTestNode("node", 1000, 1000), upcomingNode} {
				if err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node)); err != nil {
					t.Fatalf("AddNodeInfo(%s) unexpected error: %v", node.Name, err)
				}
			}
			synctest.Test(t, func(t *testing.T) {
				unschedulablePods := []*v1.Pod{fitsOnNode, fitsOnUpcomingNode, skipped}
				observer, reactionMetrics := newTestPodStateObserver(t, unschedulablePods...)
				for i, l := range tc.loops {
					reactionMetrics.reactions = nil
					processor := &GkeInternalPodListProcessor{
						defaultPodListerProcessor: &mockFilterOutSchedulable{fits: l.fits, skipped: l.skipped, degradedLoops: l.degradedLoops},
						podStateObserver:          observer,
						podStatusAggregator:       metrics_processors.NewPodStatusAggregator(),
						experimentsManager:        experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, map[string]string{}),
					}
					autoscalingCtx := &ca_context.AutoscalingContext{
						ClusterSnapshot: snapshot,
					}

					if _, err := processor.Process(context.Background(), autoscalingCtx, unschedulablePods); err != nil {
						t.Fatalf("loop %d: processor.Process() unexpected error: %v", i+1, err)
					}

					if diff := cmp.Diff(l.wantReactions, reactionMetrics.reactions, cmp.AllowUnexported(observedReaction{})); diff != "" {
						t.Errorf("loop %d: observed reactions diff (-want +got):\n%s", i+1, diff)
					}
				}
			})
		})
	}
}
