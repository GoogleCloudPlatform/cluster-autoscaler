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

package dualca

import (
	"context"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/require"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	stdComputeClass    = "std-compute"
	solverComputeClass = "solver-compute"
	stdPoolName        = "std-pool"
	solverPoolName     = "solver-pool"

	// Pods request more than half of the allocatable CPU of an n1-standard-2
	// node, so each pod needs its own node.
	podCPU = int64(1000)
	podMem = int64(100 * 1024 * 1024)
)

var (
	standardCANames = []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName}
	solverCANames   = []string{options.SolverAutoscalerName}
)

// newOwnedComputeClass returns a ComputeClass owned by the given autoscaler,
// which scales up the given node pool.
func newOwnedComputeClass(name, autoscalerName, nodePool string) *v1.ComputeClass {
	return ccc.NewComputeClassBuilder(name).
		WithNodePoolsRules(nodePool).
		WithAnnotation(options.AutoscalerNameAnnotation, autoscalerName).
		Build()
}

// newComputeClassPool returns a node pool of the given ComputeClass, with size
// nodes in each of the given zones.
func newComputeClassPool(name, computeClass string, size int64, zones ...string) *gke_api_beta.NodePool {
	return integration.EmptyNodePool(name).
		WithCCCLabel(computeClass).
		WithSize(size).
		WithMin(0).
		WithLocations(zones...).
		Build()
}

// newDualCATestConfig returns the config of a CA instance owning
// autoscalerNames, in a cluster with a standard and a solver ComputeClass, each
// scaling up its own node pool.
func newDualCATestConfig(autoscalerNames []string, stdPool, solverPool *gke_api_beta.NodePool) *integration.TestConfig {
	return integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoscalerNames(autoscalerNames...),
			// TODO(b/555150194): Mirror the production manifests, where only
			// the instance handling unannotated
			// ComputeClasses runs ProvisioningRequests, once CA respects
			// --enable-provisioning-requests.
		).
		WithNodePools(stdPool, solverPool).
		WithCccCrds(
			newOwnedComputeClass(stdComputeClass, options.StandardAutoscalerName, stdPool.Name),
			newOwnedComputeClass(solverComputeClass, options.SolverAutoscalerName, solverPool.Name),
		)
}

// pendingPod returns an unschedulable pod of the given ComputeClass, or without
// a ComputeClass if it is empty.
func pendingPod(name, computeClass string) *apiv1.Pod {
	opts := []func(*apiv1.Pod){tu.MarkUnschedulable()}
	if computeClass != "" {
		opts = append(opts, pod.WithCCC(computeClass))
	}
	return tu.BuildTestPod(name, podCPU, podMem, opts...)
}

// mustGetPoolNode returns the only node of the given node pool.
func mustGetPoolNode(t *testing.T, infra *integration.TestInfrastructure, poolName string) *apiv1.Node {
	t.Helper()
	var poolNodes []*apiv1.Node
	nodes := infra.Fakes.K8s.Nodes().Items
	for i := range nodes {
		if nodes[i].Labels[gkelabels.GkeNodePoolLabel] == poolName {
			poolNodes = append(poolNodes, &nodes[i])
		}
	}
	require.Len(t, poolNodes, 1, "expected exactly one node in node pool %s", poolName)
	return poolNodes[0]
}

// handOverComputeClass hands the given ComputeClass over to the given
// autoscaler.
func handOverComputeClass(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, name, autoscalerName string) {
	t.Helper()
	cc, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	cc.Annotations[options.AutoscalerNameAnnotation] = autoscalerName
	// The API server bumps the resource version on update, the fake client does
	// not. Informers ignore updates that don't change it.
	cc.ResourceVersion = "2"
	_, err = infra.Fakes.CccClient.CloudV1().ComputeClasses().Update(ctx, cc, metav1.UpdateOptions{})
	require.NoError(t, err)
}

// mustGetComputeClassStatus returns the status of the given ComputeClass.
func mustGetComputeClassStatus(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, name string) v1.ComputeClassStatus {
	t.Helper()
	cc, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	return cc.Status
}

// noScaleUpPodNames returns the names of the pods reported in NoScaleUp
// visibility events.
func noScaleUpPodNames(infra *integration.TestInfrastructure) []string {
	names := []string{}
	for _, event := range infra.Fakes.EventLogger.NoScaleUpEvents() {
		for _, group := range event.GetUnhandledPodGroups() {
			names = append(names, group.GetPodGroup().GetSamplePod().GetName())
		}
	}
	return names
}
