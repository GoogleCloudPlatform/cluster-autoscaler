// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package bench

import (
	"crypto/md5"
	"fmt"
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func BenchmarkMegaclusterScaleUpWithAntiAffinity(b *testing.B) {
	// The benchmark creates 100 nodepools with 201 nodes in each (67 nodes per zone), so 20100 existing nodes total.
	// Change benchmarkTotalNodes and/or benchmarkNumPools to adjust the initial cluster size.
	benchmarkTotalNodes := 20100 // should be divisible by benchmarkNumPools and by 3 zones
	benchmarkNumPools := 100
	megaclusterScaleUpWithAntiAffinity(benchmarkTotalNodes, benchmarkNumPools).run(b)
}

// TestMegaclusterScaleUpWithAntiAffinity runs the megacluster benchmark scenario in CI with small number of nodes to ensure the scenario works.
func TestMegaclusterScaleUpWithAntiAffinity(t *testing.T) {
	testTotalNodes := 300
	testNumPools := 1
	megaclusterScaleUpWithAntiAffinity(testTotalNodes, testNumPools).run(t)
}

// There are 10 running pods on each existing node with labels l0 ... l-9.
// 30% of the existing pods have anti-affinity:
//   - l7 pods have AA for l8
//   - l8 pods have AA for themselves (l8)
//   - l9 pods have AA for themselves (l9)
//
// All pods and nodes in the test have a realistic set of k8s labels (this affects interpodaffinity plugin performance).
//
// The test creates 200 pending pods with the same label setup.
// Those 200 pods require 21 to 24 nodes (it's not deterministic because the number of nodes depends on the order in which we simulate pod scheduling)
//
// The test is similar to the autoscaled part of the Pine manual stress test setup, but without CCC and with a larger number of labels http://google3/experimental/users/jonatany/megacluster-ccc/README.md;rcl=939761744
func megaclusterScaleUpWithAntiAffinity(benchmarkTotalNodes, benchmarkNumPools int) scenario {
	nodesPerPool := benchmarkTotalNodes / benchmarkNumPools
	minNodesPerZone := nodesPerPool / 3
	return scenario{
		given: func() *integration.TestConfig {
			return megaclusterBenchmarkConfig(benchmarkNumPools, minNodesPerZone).WithOverrides(
				integration.WithMaxNodesPerScaleUp(500),
				integration.WithMaxBinpackingTime(300*time.Second),
				// Feature gate for performance optimization of (anti)affinity with topologyKey=hostname
				integration.WithInterPodAffinityHostnameFastPath(true),
				integration.WithBalanceSimilarNodeGroups(),
			)
		},
		when: func(infra *integration.TestInfrastructure) {
			podsPerNode := 10
			labelKey := "logical-type"
			numApps := benchmarkTotalNodes / 10 // (e.g. 2000 apps per 20000 nodes)
			gceInstancesByMig := make(map[string][]gce.GceInstance)

			// Pre-populate nodes and pods directly in the fakes.
			// Bypassing REST controllers via Tracker Add before informers start
			// eliminates watch event channel congestion and panics.
			podNumber := 0
			for i := 0; i < benchmarkNumPools; i++ {
				poolName := fmt.Sprintf("default-pool-%d", i)

				zones := []string{"us-central1-a", "us-central1-b", "us-central1-c"}
				for n := 0; n < nodesPerPool; n++ {
					zone := zones[n%len(zones)]
					migKey := fmt.Sprintf("%s/%s", zone, poolName)
					migRef := gce.GceRef{Project: "test-project", Zone: zone, Name: poolName}

					nodeName := fmt.Sprintf("node-%d-%d", i, n)
					providerID := fmt.Sprintf("gce://test-project/%s/%s", zone, nodeName)

					gceInstancesByMig[migKey] = append(gceInstancesByMig[migKey], gce.GceInstance{
						Instance: cloudprovider.Instance{
							Id: providerID,
							Status: &cloudprovider.InstanceStatus{
								State: cloudprovider.InstanceRunning,
							},
						},
						Igm:                  migRef,
						InstanceTemplateName: fmt.Sprintf("%s-%s-tmpl", poolName, zone),
					})

					node := BuildTestNode(nodeName, "e2-standard-32", zone, poolName)
					// Add never returns an error in the current fake implementation.
					_ = infra.Fakes.KubeClient.Tracker().Add(node)

					// Add 10 pods to each node
					// Each pod requests 3180m CPU to saturate the node's 32000m CPU capacity,
					// leaving only 200m CPU free.
					for p := 0; p < podsPerNode; p++ {
						podNumber++
						labelVal := fmt.Sprintf("l%d", p)
						appName := fmt.Sprintf("app-%d", podNumber%numApps)
						pod := buildBenchmarkPod(podNumber/podsPerNode, appName, nodeName, labelKey, labelVal, 3180)

						// Pod l7 has Required anti-affinity targeting l8.
						// The last 2 pods (l8 and l9) carry required hostname anti-affinity for their own label value.
						if p == 7 || p >= 8 {
							targetLabelVal := labelVal
							if p == 7 {
								targetLabelVal = "l8"
							}
							pod.Spec.Affinity = &apiv1.Affinity{
								PodAntiAffinity: &apiv1.PodAntiAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchLabels: map[string]string{labelKey: targetLabelVal},
											},
											TopologyKey: "kubernetes.io/hostname",
										},
									},
								},
							}
						}

						_ = infra.Fakes.KubeClient.Tracker().Add(pod)
					}
				}
			}

			// Register manually created GceInstances bulk-wise inside GCE Service fake
			infra.Fakes.GceService.WithInstances(gceInstancesByMig)

			// Create 200 pending pods requesting 600m CPU each.
			// 10 label values:
			// - l7 has required anti-affinity for l8.
			// - l8 and l9 have required anti-affinity for itself.
			for i := 0; i < 200; i++ {
				podNumber++
				appName := fmt.Sprintf("app-%d", podNumber%numApps)
				logicalTypeVal := fmt.Sprintf("l%d", i%10)

				pod := buildBenchmarkPod(podNumber, appName, "", labelKey, logicalTypeVal, 600)

				// Apply required anti-affinity rules
				if i%10 == 7 || i%10 >= 8 {
					targetLabelVal := logicalTypeVal
					if i%10 == 7 {
						targetLabelVal = "l8"
					}
					pod.Spec.Affinity = &apiv1.Affinity{
						PodAntiAffinity: &apiv1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{labelKey: targetLabelVal},
									},
									TopologyKey: "kubernetes.io/hostname",
								},
							},
						},
					}
				}

				_ = infra.Fakes.KubeClient.Tracker().Add(pod)
			}
		},
		then: func(tb testing.TB, infra *integration.TestInfrastructure) {
			zones := []string{"us-central1-a", "us-central1-b", "us-central1-c"}
			totalTargetSize := 0
			for _, zone := range zones {
				migs, err := infra.Fakes.GceService.FetchAllMigs(zone)
				if err != nil {
					tb.Fatalf("Failed to fetch MIGs in zone %s: %v", zone, err)
				}
				for _, mig := range migs {
					totalTargetSize += int(mig.TargetSize)
				}
			}
			// We expect all pools to successfully scale up, adding 21 to 24 nodes total due to anti-affinity and randomized pool split packing
			baseTarget := int(benchmarkNumPools * minNodesPerZone * 3)
			if totalTargetSize < baseTarget+21 || totalTargetSize > baseTarget+24 {
				tb.Fatalf("total target size = %d, want between %d and %d", totalTargetSize, baseTarget+21, baseTarget+24)
			}
			tb.Logf("Benchmark completed successfully. computed target size: %d nodes", totalTargetSize)
		},
	}
}

func megaclusterBenchmarkConfig(numPools, minNodesPerZone int) *integration.TestConfig {
	mcp := machinetypes.NewMachineConfigProvider(nil)
	e2, err := mcp.ToMachineType("e2-standard-32")
	if err != nil {
		panic(err)
	}
	// Cluster-wide limits
	maxCores := e2.CPU * int64(60000)
	maxMemory := e2.Memory * int64(60000)

	config := integration.NewTestConfig().
		WithClusterWideLimits(60000, maxCores, maxMemory).
		WithOverrides(integration.WithPredicateParallelism(16))

	for i := 0; i < numPools; i++ {
		poolName := fmt.Sprintf("default-pool-%d", i)
		config = config.WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolName(poolName),
			integration.WithNodePoolMachineType("e2-standard-32"),
			integration.WithNodePoolSize(int64(minNodesPerZone)),
		))
	}

	return config
}

func BuildTestNode(name, machineType, zone, nodePool string) *apiv1.Node {
	node := tu.BuildTestNode(name, 32000, 131072*1024*1024)
	node.Spec.ProviderID = fmt.Sprintf("gce://test-project/%s/%s", zone, name)
	node.Labels = map[string]string{
		"addon.gke.io/node-local-dns-ds-ready":         "true",
		"beta.kubernetes.io/arch":                      "amd64",
		"beta.kubernetes.io/instance-type":             machineType,
		"beta.kubernetes.io/os":                        "linux",
		"cloud.google.com/gke-boot-disk":               "pd-standard",
		"cloud.google.com/gke-container-runtime":       "containerd",
		"cloud.google.com/gke-cpu-scaling-level":       "32",
		"cloud.google.com/gke-logging-variant":         "DEFAULT",
		"cloud.google.com/gke-max-pods-per-node":       "110",
		"cloud.google.com/gke-memory-gb-scaling-level": "131",
		"cloud.google.com/gke-nodepool":                nodePool,
		"cloud.google.com/gke-os-distribution":         "cos",
		"cloud.google.com/gke-provisioning":            "standard",
		"cloud.google.com/gke-stack-type":              "IPV4",
		"cloud.google.com/machine-family":              "e2",
		"cloud.google.com/private-node":                "true",
		"disk-type.gke.io/pd-balanced":                 "true",
		"disk-type.gke.io/pd-extreme":                  "true",
		"disk-type.gke.io/pd-ssd":                      "true",
		"disk-type.gke.io/pd-standard":                 "true",
		"failure-domain.beta.kubernetes.io/region":     "us-central1",
		"failure-domain.beta.kubernetes.io/zone":       zone,
		"kubernetes.io/arch":                           "amd64",
		"kubernetes.io/hostname":                       name,
		"kubernetes.io/os":                             "linux",
		"node.kubernetes.io/instance-type":             machineType,
		"topology.gke.io/zone":                         zone,
		"topology.kubernetes.io/region":                "us-central1",
		"topology.kubernetes.io/zone":                  zone,
	}
	node.Annotations = map[string]string{
		"node.gke.io/last-applied-node-labels": "fake-node-label",
	}
	tu.SetNodeReadyState(node, true, time.Now())
	return node
}

func buildBenchmarkPod(podIndex int, appName, nodeName, labelKey, labelVal string, cpu int64) *apiv1.Pod {
	podName := fmt.Sprintf("%s-%d", appName, podIndex)
	revisionHash := fmt.Sprintf("%x", md5.Sum([]byte(appName)))

	var pod *apiv1.Pod
	if nodeName != "" {
		pod = tu.BuildTestPod(podName, cpu, 200)
		pod.Spec.NodeName = nodeName
	} else {
		pod = tu.BuildTestPod(podName, cpu, 1000, tu.MarkUnschedulable())
	}

	pod.Labels = map[string]string{
		"affinity-group":                     "anti-affinity",
		"app":                                appName,
		labelKey:                             labelVal,
		"apps.kubernetes.io/pod-index":       fmt.Sprintf("%d", podIndex),
		"controller-revision-hash":           revisionHash,
		"statefulset.kubernetes.io/pod-name": podName,
		"sts-group":                          "sts-pod",
	}
	return pod
}
