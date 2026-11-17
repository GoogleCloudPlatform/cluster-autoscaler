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

package podsharding

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
)

type shardFeatureParts map[string]string

type podFeatureParts []struct {
	uid        apitypes.UID
	nodeLables shardFeatureParts
}
type expectedShards map[apitypes.UID][]ShardSignature // uid -> list of shards

func TestCompositePodSharderBasic(t *testing.T) {
	tests := []struct {
		name           string
		podsFeatures   podFeatureParts
		expectedShards expectedShards
	}{
		{
			name:           "no pods",
			podsFeatures:   nil,
			expectedShards: nil,
		},
		{
			name: "only default shard",
			podsFeatures: podFeatureParts{
				{"p1", shardFeatureParts{}},
				{"p2", shardFeatureParts{}},
				{"p3", shardFeatureParts{}},
			},
			expectedShards: expectedShards{
				"p1": []ShardSignature{"_"},
				"p2": []ShardSignature{"_"},
				"p3": []ShardSignature{"_"},
			},
		},
		{
			name: "only non default shard",
			podsFeatures: podFeatureParts{
				{"p1", shardFeatureParts{"f1": "v1", "f2": "v2"}},
				{"p2", shardFeatureParts{"f1": "v1", "f2": "v2"}},
				{"p3", shardFeatureParts{"f1": "v1", "f2": "v2"}},
			},
			expectedShards: expectedShards{
				"p1": []ShardSignature{"Labels(f1=v1,f2=v2)"},
				"p2": []ShardSignature{"Labels(f1=v1,f2=v2)"},
				"p3": []ShardSignature{"Labels(f1=v1,f2=v2)"},
			},
		},
		{
			name: "mixed default and non default",
			podsFeatures: podFeatureParts{
				{"p1", shardFeatureParts{}},
				{"p2", shardFeatureParts{"f1": "v1", "f2": "v2"}},
				{"p3", shardFeatureParts{"f1": "v1", "f2": "v2"}},
			},
			expectedShards: expectedShards{
				"p1": []ShardSignature{"_"},
				"p2": []ShardSignature{"Labels(f1=v1,f2=v2)"},
				"p3": []ShardSignature{"Labels(f1=v1,f2=v2)"},
			},
		},
		{
			name: "different values for one feature",
			podsFeatures: podFeatureParts{
				{"p1", shardFeatureParts{"f1": "v1", "f2": "v2"}},
				{"p2", shardFeatureParts{"f1": "v1", "f2": "v2"}},
				{"p3", shardFeatureParts{"f1": "v1", "f2": "v3"}},
				{"p4", shardFeatureParts{"f1": "v1"}},
			},
			expectedShards: expectedShards{
				"p1": []ShardSignature{"Labels(f1=v1,f2=v2)"},
				"p2": []ShardSignature{"Labels(f1=v1,f2=v2)"},
				"p3": []ShardSignature{"Labels(f1=v1,f2=v3)"},
				"p4": []ShardSignature{"Labels(f1=v1)"},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var computeFunctions []FeatureShardComputeFunction
			for _, feature := range getAllFeaturesSorted(tc.podsFeatures) {
				featureCopy := feature
				computeFunctions = append(computeFunctions,
					FeatureShardComputeFunction{
						Feature: featureCopy,
						Function: func(pod *v1.Pod, _ *podrequirements.Requirements, nodeGroupDescriptor *NodeGroupDescriptor) {
							for _, podFeatures := range tc.podsFeatures {
								if podFeatures.uid == pod.UID {
									for k, v := range podFeatures.nodeLables {
										nodeGroupDescriptor.Labels[k] = v
									}
								}
							}
						},
					})
			}

			podSharder := NewCompositePodSharder(computeFunctions, false, nil)

			var pods []*v1.Pod
			for _, podFeaturesEntry := range tc.podsFeatures {
				pods = append(pods, podForUID(podFeaturesEntry.uid))
			}
			podShards := podSharder.ComputePodShards(pods)

			allShardSignatures := allExpectedShardSignatures(tc.expectedShards)
			resultShardSignatures := getShardSignatures(podShards)

			assert.ElementsMatch(t, allShardSignatures, resultShardSignatures, "List of shards %v does not match expected %v one", resultShardSignatures, allShardSignatures)
			for _, podShard := range podShards {
				expectedUidsForShard := expectedUidsForShard(tc.expectedShards, podShard.Signature())
				assert.ElementsMatch(t, expectedUidsForShard, podShard.PodUidsSlice(), "List of pods does not match expected one for shard %s", podShard.Signature())
			}
		})
	}
}

func TestCompositePodSharderNodeDescriptor(t *testing.T) {
	buildComputeFunction := func(labelKey string) FeatureShardComputeFunction {
		return FeatureShardComputeFunction{
			Feature: labelKey,
			Function: func(pod *v1.Pod, _ *podrequirements.Requirements, nodeGroupDescriptor *NodeGroupDescriptor) {
				for podLabelKey, podLabelValue := range pod.Labels {
					if podLabelKey == labelKey {
						nodeGroupDescriptor.Labels[podLabelKey] = podLabelValue

					}
				}
			},
		}
	}

	computeFunctions := []FeatureShardComputeFunction{
		buildComputeFunction("a"),
		buildComputeFunction("b"),
	}

	sharder := NewCompositePodSharder(computeFunctions, false, nil)

	p1 := podForUID("p1")
	p1.Labels = make(map[string]string)
	p1.Labels["a"] = "a1"
	p1.Labels["b"] = "b1"

	p2 := podForUID("p2")
	p2.Labels = make(map[string]string)
	p2.Labels["a"] = "a1"
	p2.Labels["b"] = "b1"

	p3 := podForUID("p3")
	p3.Labels = make(map[string]string)
	p3.Labels["a"] = "a1"
	p3.Labels["b"] = "b2"

	// p1 and p2 are in single shard (a1/b1)
	// p3 is in separate shard (a1/b2)

	shards := sharder.ComputePodShards([]*v1.Pod{p1, p2, p3})
	assert.ElementsMatch(t, []ShardSignature{"Labels(a=a1,b=b1)", "Labels(a=a1,b=b2)"}, getShardSignatures(shards))

	shard1 := getShardForSignature("Labels(a=a1,b=b1)", shards)
	shard2 := getShardForSignature("Labels(a=a1,b=b2)", shards)

	assertNodeGroupDescriptorEqual(t, NodeGroupDescriptor{
		Labels: map[string]string{
			"a": "a1",
			"b": "b1",
		},
	}, shard1.NodeGroupDescriptor)

	assertNodeGroupDescriptorEqual(t, NodeGroupDescriptor{
		Labels: map[string]string{
			"a": "a1",
			"b": "b2",
		},
	}, shard2.NodeGroupDescriptor)
}

func getShardSignatures(podShards []*PodShard) []ShardSignature {
	var resultShardSignatures []ShardSignature
	for _, podShard := range podShards {
		resultShardSignatures = append(resultShardSignatures, podShard.Signature())
	}
	return resultShardSignatures
}

func getShardForSignature(signature ShardSignature, podShards []*PodShard) *PodShard {
	for _, podShard := range podShards {
		if podShard.Signature() == signature {
			return podShard
		}
	}
	return nil
}
func allExpectedShardSignatures(expectedShards expectedShards) []ShardSignature {
	allShardSignaturesSet := make(map[ShardSignature]bool)
	for _, shardSignatures := range expectedShards {
		for _, shardSignature := range shardSignatures {
			allShardSignaturesSet[shardSignature] = true
		}
	}
	var allShardSignatures []ShardSignature
	for shardSignature := range allShardSignaturesSet {
		allShardSignatures = append(allShardSignatures, shardSignature)
	}
	return allShardSignatures
}

func expectedUidsForShard(expectedShards expectedShards, shardSignature ShardSignature) []apitypes.UID {
	var uidsForShards []apitypes.UID
	for uid, shardSignatures := range expectedShards {
		if containsShard(shardSignatures, shardSignature) {
			uidsForShards = append(uidsForShards, uid)
		}
	}
	return uidsForShards
}

func containsShard(slice []ShardSignature, element ShardSignature) bool {
	for _, sliceElement := range slice {
		if element == sliceElement {
			return true
		}
	}
	return false
}

func getAllFeaturesSorted(features podFeatureParts) []string {
	allFeaturesSet := make(map[string]bool)
	for _, podFeaturesEntry := range features {
		for feature := range podFeaturesEntry.nodeLables {
			allFeaturesSet[feature] = true
		}
	}
	var allFeatures []string
	for feature := range allFeaturesSet {
		allFeatures = append(allFeatures, feature)
	}
	sort.Strings(allFeatures)
	return allFeatures
}

func TestCompositePodSharderDefaultPayPerPodTagging(t *testing.T) {
	computeFunctions := []FeatureShardComputeFunction{
		{
			Feature: "ccc",
			Function: func(pod *v1.Pod, req *podrequirements.Requirements, ngd *NodeGroupDescriptor) {
				if name, found := req.LabelReq.GetSingleValue(gkelabels.ComputeClassLabel); found {
					ngd.SystemLabels[gkelabels.ComputeClassLabel] = name
				}
			},
		},
		{
			Feature: "gpu",
			Function: func(pod *v1.Pod, req *podrequirements.Requirements, ngd *NodeGroupDescriptor) {
				if q := req.PodRequests[gpu.ResourceNvidiaGPU]; q.Value() > 0 {
					ngd.RequiresGPU = true
				}
			},
		},
		{
			Feature: "tpu",
			Function: func(pod *v1.Pod, req *podrequirements.Requirements, ngd *NodeGroupDescriptor) {
				if q := req.PodRequests[tpu.ResourceGoogleTPU]; q.Value() > 0 {
					ngd.SystemLabels[gkelabels.TPULabel] = tpu.DefaultTPU
				}
			},
		},
	}

	plainPod := func(uid apitypes.UID) *v1.Pod {
		return podForUID(uid)
	}
	machineFamilyPod := func(uid apitypes.UID, family string) *v1.Pod {
		p := podForUID(uid)
		p.Spec.NodeSelector = map[string]string{gkelabels.MachineFamilyLabel: family}
		return p
	}
	cccPod := func(uid apitypes.UID, cccName string) *v1.Pod {
		p := podForUID(uid)
		p.Spec.NodeSelector = map[string]string{gkelabels.ComputeClassLabel: cccName}
		return p
	}
	multiValueCccPod := func(uid apitypes.UID, op v1.NodeSelectorOperator, values ...string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{UID: uid},
			Spec: v1.PodSpec{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      gkelabels.ComputeClassLabel,
											Operator: op,
											Values:   values,
										},
									},
								},
							},
						},
					},
				},
			},
		}
	}
	gpuLimitOnlyPod := func(uid apitypes.UID) *v1.Pod {
		p := podForUID(uid)
		p.Spec.Containers = []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					Limits: v1.ResourceList{gpu.ResourceNvidiaGPU: resource.MustParse("1")},
				},
			},
		}
		return p
	}
	gpuRequestPod := func(uid apitypes.UID) *v1.Pod {
		p := podForUID(uid)
		p.Spec.Containers = []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{gpu.ResourceNvidiaGPU: resource.MustParse("1")},
				},
			},
		}
		return p
	}
	tpuRequestPod := func(uid apitypes.UID) *v1.Pod {
		p := podForUID(uid)
		p.Spec.Containers = []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{tpu.ResourceGoogleTPU: resource.MustParse("1")},
				},
			},
		}
		return p
	}

	payPerPodEnabledManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
		experiments.FlexAdvisorProcessingEnabledFlag:         true,
		experiments.PayPerPodFleetEfficiencyEnabledFlag:      true,
		experiments.PayPerPodFleetEfficiencyMinCAVersionFlag: true,
	}, nil)

	testCases := []struct {
		name                    string
		payPerPodTaggingEnabled bool
		experimentsManager      experiments.Manager
		pods                    []*v1.Pod
		wantTags                map[ShardSignature]bool
	}{
		{
			name:                    "nil experiments manager on autopilot - leaves HasPayPerPodWorkloads false",
			payPerPodTaggingEnabled: true,
			experimentsManager:      nil,
			pods:                    []*v1.Pod{plainPod("p1")},
			wantTags:                map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "standard cluster with PayPerPodFleetEfficiency enabled - leaves HasPayPerPodWorkloads false",
			payPerPodTaggingEnabled: false,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{plainPod("p1")},
			wantTags:                map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with FlexAdvisorProcessing disabled - leaves HasPayPerPodWorkloads false",
			payPerPodTaggingEnabled: true,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:         false,
				experiments.PayPerPodFleetEfficiencyEnabledFlag:      true,
				experiments.PayPerPodFleetEfficiencyMinCAVersionFlag: true,
			}, nil),
			pods:     []*v1.Pod{plainPod("p1")},
			wantTags: map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with PayPerPodFleetEfficiency disabled - leaves HasPayPerPodWorkloads false",
			payPerPodTaggingEnabled: true,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.PayPerPodFleetEfficiencyEnabledFlag:      false,
				experiments.PayPerPodFleetEfficiencyMinCAVersionFlag: true,
			}, nil),
			pods:     []*v1.Pod{plainPod("p1")},
			wantTags: map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with PayPerPodFleetEfficiency min CA version unmet - leaves HasPayPerPodWorkloads false",
			payPerPodTaggingEnabled: true,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.PayPerPodFleetEfficiencyEnabledFlag:      true,
				experiments.PayPerPodFleetEfficiencyMinCAVersionFlag: false,
			}, nil),
			pods:     []*v1.Pod{plainPod("p1")},
			wantTags: map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with empty pod list - returns empty shards",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    nil,
			wantTags:                map[ShardSignature]bool{},
		},
		{
			name:                    "autopilot with plain pods - tags shard true without altering signature",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{plainPod("p1"), plainPod("p2")},
			wantTags:                map[ShardSignature]bool{"_": true},
		},
		{
			name:                    "autopilot with machine-family pods only - tags shard false",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{machineFamilyPod("n2-1", "n2"), machineFamilyPod("n2-2", "n2")},
			wantTags:                map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with machine-family pod before plain pod in same shard - tags shard true",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{machineFamilyPod("n2-1", "n2"), plainPod("p1")},
			wantTags:                map[ShardSignature]bool{"_": true},
		},
		{
			name:                    "autopilot with plain pod before machine-family pod in same shard - preserves true tag",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{plainPod("p1"), machineFamilyPod("n2-1", "n2")},
			wantTags:                map[ShardSignature]bool{"_": true},
		},
		{
			name:                    "autopilot with explicit single compute-class label - tags shard false",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{cccPod("p1", "custom-ccc")},
			wantTags:                map[ShardSignature]bool{"SystemLabels(cloud.google.com/compute-class=custom-ccc)": false},
		},
		{
			name:                    "autopilot with multi-value compute-class In affinity - stays false in plain shard",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{multiValueCccPod("p1", v1.NodeSelectorOpIn, "ccc-a", "ccc-b")},
			wantTags:                map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with compute-class Exists affinity - stays false in plain shard",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{multiValueCccPod("p1", v1.NodeSelectorOpExists)},
			wantTags:                map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with GPU request - tags shard false",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{gpuRequestPod("p1")},
			wantTags:                map[ShardSignature]bool{"RequiresGPU(true)": false},
		},
		{
			name:                    "autopilot with GPU limit only - falls through to billing check and tags false",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{gpuLimitOnlyPod("p1")},
			wantTags:                map[ShardSignature]bool{"_": false},
		},
		{
			name:                    "autopilot with TPU request - tags shard false",
			payPerPodTaggingEnabled: true,
			experimentsManager:      payPerPodEnabledManager,
			pods:                    []*v1.Pod{tpuRequestPod("p1")},
			wantTags:                map[ShardSignature]bool{ShardSignature("SystemLabels(cloud.google.com/gke-tpu-accelerator=" + tpu.DefaultTPU + ")"): false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sharder := NewCompositePodSharder(computeFunctions, tc.payPerPodTaggingEnabled, tc.experimentsManager)
			shards := sharder.ComputePodShards(tc.pods)
			require.Len(t, shards, len(tc.wantTags))
			for _, shard := range shards {
				want, ok := tc.wantTags[shard.Signature()]
				require.True(t, ok, "unexpected shard signature %q", shard.Signature())
				assert.Equal(t, want, shard.HasPayPerPodWorkloads, "shard %q", shard.Signature())
			}
		})
	}
}
