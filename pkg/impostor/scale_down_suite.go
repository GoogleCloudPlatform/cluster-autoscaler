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

package impostor

import (
	"context"
	"fmt"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	cacontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/actuation"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/deletiontracker"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/planner"
	ca_processors "k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// ScaleDownSuite encapsulates functionality used for testing scale down.
type ScaleDownSuite struct {
	Cluster       *Cluster
	KubeClient    *MockKubeClient
	Provider      *MockCloudProvider
	statsExporter *statsExporter
}

// SetUpDefaultSuite sets up default suite for testing scale down.
func SetUpDefaultSuite(p *SdTestParameters, scaleDownUnneededTime time.Duration, scaleDownUtilThreshold float64, maxGracefulTerminationSec int, testName string) *ScaleDownSuite {
	nodes := &sync.Map{}
	pods := &sync.Map{}
	ctx, cancel := context.WithCancel(context.Background())
	fakeRecorder := kube_record.NewFakeRecorder(100)
	go clearRecorder(ctx, fakeRecorder)
	defer cancel()
	var pdbs []*policyv1.PodDisruptionBudget
	sE := NewStatsExporter(p, testName, scaleDownUnneededTime, scaleDownUtilThreshold)
	kubeClient, mockListers := NewMockKubeClient(pods, pdbs)
	kubeClient.AddPodCallback(mockListers.pdbLister)
	kubeClient.AddPodCallback(sE)
	sE.client = kubeClient
	cloudProvider := NewMockCloudProvider(kubeClient, scaleDownUtilThreshold, scaleDownUnneededTime, maxPodPerNodeCount, false, false)
	rec, _ := utils.NewStatusMapRecorder(kubeClient, "kube-system", fakeRecorder, false, "test-configmap")
	autoscalingKubeClients := cacontext.AutoscalingKubeClients{
		ClientSet:      kubeClient,
		Recorder:       fakeRecorder,
		ListerRegistry: mockListers.newKubernetesRegistry(),
		LogRecorder:    rec,
	}
	opts := CreateAutoscalingOptions(maxGracefulTerminationSec, defaultScaleDownSimulationTimeout, 100*time.Second, p.maxScaleDownParallelism, p.maxDrainParallelism)
	caContext := CreateAutoscalingContext(opts, cloudProvider, autoscalingKubeClients)
	processors := CreateAutoscalingProcessors(opts, caContext, cloudProvider, caContext.ClusterSnapshot)
	clusterState := clusterstate.NewNotifiedClusterStateRegistry(cloudProvider, rec, nil, processors.NodeGroupConfigProcessor, nil, clusterstate.WithAsyncNodeGroupStateChecker(processors.AsyncNodeGroupStateChecker))
	ndt := deletiontracker.NewNodeDeletionTracker(1 * time.Minute)
	deleteOptions := options.NodeDeleteOptions{}
	actuator := actuation.NewActuator(caContext, clusterState, ndt, deleteOptions, rules.Default(deleteOptions), processors.NodeGroupConfigProcessor)
	quotasTrackerFactory := newQuotasTrackerFactory(caContext, processors)
	sdPlanner := planner.New(caContext, processors, deleteOptions, rules.Default(deleteOptions), quotasTrackerFactory)
	autoscaler := NewParameterizedTestAutoscaler(caContext, cloudProvider, processors, sdPlanner, actuator)
	cluster := NewParameterizedCluster(kubeClient, cloudProvider, autoscaler, pods, nodes, scaleDownUtilThreshold, scaleDownUnneededTime)
	klog.Infof("Scale down suite with following parameters created:\nScale "+
		"down unneeded time: %v\nScale down utilization threshold: %v\nMax scale"+
		" down parallelism: %d\nMax drain parallelism: %d",
		scaleDownUnneededTime, scaleDownUtilThreshold, p.maxScaleDownParallelism, p.maxDrainParallelism)
	return &ScaleDownSuite{
		Cluster:       cluster,
		KubeClient:    kubeClient,
		Provider:      cloudProvider,
		statsExporter: sE,
	}
}

// AddAndFillUpNodePool adds and fills new node pool to the cluster based on provided config.
func (s *ScaleDownSuite) AddAndFillUpNodePool(name, machineType string, nodePoolSize, numberOfPodsPerNode int,
	nodeMemTargetUtilization, nodeCpuTargetUtilization int64, podController string,
	podTerminationTime time.Duration, labels map[string]string) []*apiv1.Node {
	ng := s.Cluster.AddOrScaleUpNodeGroupWithCustomLabels(name, machineType, nodePoolSize, false, labels)
	s.Cluster.FillUpNodesPartially(ng, nodeMemTargetUtilization, nodeCpuTargetUtilization, numberOfPodsPerNode, podTerminationTime, podController, 10, "default", labels)
	return ng
}

// AddPDB adds provided pod disruption budget.
func (s *ScaleDownSuite) AddPDB(pdb *policyv1.PodDisruptionBudget) error {
	return s.KubeClient.listers.pdbLister.Add(pdb)
}

// ScaleDownUntilConditionMet performs scale down until either condition is met or timeout occurs.
func (s *ScaleDownSuite) ScaleDownUntilConditionMet(ctx context.Context, expectedNodeCount int, retryFor, waitBetweenRetries time.Duration) error {
	start := time.Now()
	retryUntil := time.Now().Add(retryFor)
	ctx, cancel := context.WithCancel(ctx)
	go s.statsExporter.trackNodesCount(ctx, nodeCountRecorderInterval)
	defer cancel()

	for ; time.Now().Before(retryUntil) && expectedNodeCount < getNodeCount(s.KubeClient); time.Sleep(waitBetweenRetries) {
		err := s.Cluster.autoscaler.ScaleDown()
		if err != nil {
			return err
		}
	}
	nodeCount := getNodeCount(s.KubeClient)
	err := s.statsExporter.export(nodeCount, time.Since(start))
	if err != nil {
		return err
	}
	if expectedNodeCount < nodeCount {
		return fmt.Errorf("timeout occurred before cluster reached expected node count %d, cluster node count: %d", expectedNodeCount, nodeCount)
	}
	return nil
}

func getNodeCount(client *MockKubeClient) int {
	nodes, _ := client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	return len(nodes.Items)
}

func clearRecorder(ctx context.Context, recorder *kube_record.FakeRecorder) {
	for {
		select {
		case <-recorder.Events:
		case <-ctx.Done():
			break
		}
	}
}

func newQuotasTrackerFactory(autoscalingCtx *cacontext.AutoscalingContext, p *ca_processors.AutoscalingProcessors) *resourcequotas.TrackerFactory {
	cloudQuotasProvider := resourcequotas.NewCloudQuotasProvider(autoscalingCtx.CloudProvider)
	quotasProvider := resourcequotas.NewCombinedQuotasProvider([]resourcequotas.Provider{cloudQuotasProvider})
	return resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
		CustomResourcesProcessor: p.CustomResourcesProcessor,
		QuotaProvider:            quotasProvider,
	})
}
