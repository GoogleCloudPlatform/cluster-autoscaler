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

package pods_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/utils/ptr"
)

var (
	testTS = time.Date(2023, 12, 3, 0, 0, 0, 0, time.UTC)
)

func addValueIfNotEmpty(m map[string]string, k string, v string) map[string]string {
	if v != "" {
		m[k] = v
	}
	return m
}

type testPodConfig struct {
	name                   string
	generateName           string
	containerName          string
	containerImage         string
	provReqName            string
	capacitySearchStrategy string
	creationTimestamp      time.Time
	nodeSelectors          map[string]string
}

func TestPodsForProvisioningRequest(t *testing.T) {
	testPod := func(conf testPodConfig) *v1.Pod {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              conf.name,
				GenerateName:      conf.generateName,
				Namespace:         "test-namespace",
				CreationTimestamp: metav1.NewTime(conf.creationTimestamp),
				UID:               types.UID(fmt.Sprintf("test-namespace/%s", conf.name)),
				Annotations: addValueIfNotEmpty(map[string]string{
					prv1.ProvisioningRequestPodAnnotationKey: conf.provReqName,
					prv1.ProvisioningClassPodAnnotationKey:   queuedwrapper.QueuedProvisioningClassName,
				}, pods.ProvisioningCapacitySearchStrategyAnnotationKey, conf.capacitySearchStrategy),
				OwnerReferences: []metav1.OwnerReference{
					{
						Controller: proto.Bool(true),
						Name:       conf.provReqName,
						Kind:       "ProvisioningRequest",
					},
				},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  conf.containerName,
						Image: conf.containerImage,
					},
				},
				EnableServiceLinks: ptr.To(true),
				Tolerations: []v1.Toleration{
					{
						Key:      pods.QueuedTaintKey,
						Operator: v1.TolerationOpEqual,
						Value:    pods.QueuedTaintValue,
						Effect:   v1.TaintEffectNoSchedule,
					},
				},
			},
		}
		if conf.nodeSelectors != nil {
			pod.Spec.NodeSelector = conf.nodeSelectors
		}
		return pod
	}
	tests := []struct {
		desc    string
		pr      *prv1.ProvisioningRequest
		pt      []*v1.PodTemplate
		want    []*v1.Pod
		wantErr bool
	}{
		{
			desc: "simple ProvReq",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-pr-name",
					Namespace:         "test-namespace",
					CreationTimestamp: metav1.NewTime(testTS),
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							Count: 1,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt-name",
							},
						},
					},
				},
			},
			pt: []*v1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
					},
				},
			}},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:              "test-pr-name-0-0",
					generateName:      "test-pr-name-",
					containerName:     "test-container",
					containerImage:    "test-image",
					provReqName:       "test-pr-name",
					creationTimestamp: testTS,
				}),
			},
		},
		{
			desc: "ProvReq already having taint",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pr-name",
					Namespace: "test-namespace",
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							Count: 1,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt-name",
							},
						},
					},
				},
			},
			pt: []*v1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
						Tolerations: []v1.Toleration{
							{
								Key:      pods.QueuedTaintKey,
								Operator: v1.TolerationOpEqual,
								Value:    pods.QueuedTaintValue,
								Effect:   v1.TaintEffectNoSchedule,
							},
						},
					},
				},
			}},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:           "test-pr-name-0-0",
					generateName:   "test-pr-name-",
					containerName:  "test-container",
					containerImage: "test-image",
					provReqName:    "test-pr-name",
				}),
			},
		},
		{
			desc: "ProvReq already having nodeSelector",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pr-name",
					Namespace: "test-namespace",
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							Count: 1,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt-name",
							},
						},
					},
				},
			},
			pt: []*v1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
						NodeSelector: map[string]string{
							gkelabels.ProvisioningRequestLabelKey: "test-value",
						},
					},
				},
			}},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:           "test-pr-name-0-0",
					generateName:   "test-pr-name-",
					containerName:  "test-container",
					containerImage: "test-image",
					provReqName:    "test-pr-name",
				}),
			},
		},
		{
			desc: "ProvReq with multiple pod sets",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pr-name",
					Namespace: "test-namespace",
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							PodTemplateRef: prv1.Reference{
								Name: "test-pt1-name",
							},
							Count: 2,
						},
						{
							PodTemplateRef: prv1.Reference{
								Name: "test-pt2-name",
							},
							Count: 3,
						},
					},
				},
			},
			pt: []*v1.PodTemplate{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pt1-name",
						Namespace: "test-namespace",
					},
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "test-container",
									Image: "test-image",
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pt2-name",
						Namespace: "test-namespace",
					},
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "test-container-2",
									Image: "test-image-2",
								},
							},
						},
					},
				},
			},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:           "test-pr-name-0-0",
					generateName:   "test-pr-name-",
					containerName:  "test-container",
					containerImage: "test-image",
					provReqName:    "test-pr-name",
				}),
				testPod(testPodConfig{
					name:           "test-pr-name-0-1",
					generateName:   "test-pr-name-",
					containerName:  "test-container",
					containerImage: "test-image",
					provReqName:    "test-pr-name",
				}),
				testPod(testPodConfig{
					name:           "test-pr-name-1-0",
					generateName:   "test-pr-name-",
					containerName:  "test-container-2",
					containerImage: "test-image-2",
					provReqName:    "test-pr-name",
				}),
				testPod(testPodConfig{
					name:           "test-pr-name-1-1",
					generateName:   "test-pr-name-",
					containerName:  "test-container-2",
					containerImage: "test-image-2",
					provReqName:    "test-pr-name",
				}),
				testPod(testPodConfig{
					name:           "test-pr-name-1-2",
					generateName:   "test-pr-name-",
					containerName:  "test-container-2",
					containerImage: "test-image-2",
					provReqName:    "test-pr-name",
				}),
			},
		},
		{
			desc: "provisioning_capacity_search_strategy_obtainability",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pr-name",
					Namespace: "test-namespace",
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							Count: 1,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt-name",
							},
						},
					},
					Parameters: map[string]prv1.Parameter{
						"capacitySearchStrategy": prv1.Parameter("OBTAINABILITY"),
					},
				},
			},
			pt: []*v1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
					},
				},
			}},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:                   "test-pr-name-0-0",
					generateName:           "test-pr-name-",
					containerName:          "test-container",
					containerImage:         "test-image",
					provReqName:            "test-pr-name",
					capacitySearchStrategy: "OBTAINABILITY",
				}),
			},
		},
		{
			desc: "provisioning_capacity_search_strategy_other",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pr-name",
					Namespace: "test-namespace",
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							Count: 1,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt-name",
							},
						},
					},
					Parameters: map[string]prv1.Parameter{
						"capacitySearchStrategy": prv1.Parameter("OTHER"),
					},
				},
			},
			pt: []*v1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
					},
				},
			}},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:           "test-pr-name-0-0",
					generateName:   "test-pr-name-",
					containerName:  "test-container",
					containerImage: "test-image",
					provReqName:    "test-pr-name",
				}),
			},
		},

		{
			desc: "ProvReq with Bulk provisioning pods and empty MRD - sets MRD node selector with default MRD value",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-pr-name",
					Namespace:         "test-namespace",
					CreationTimestamp: metav1.NewTime(testTS),
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							Count: 1,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt-name",
							},
						},
					},
				},
			},
			pt: []*v1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
						NodeSelector: map[string]string{
							gkelabels.GPULabel:       gkelabels.NvidiaGB200,
							gkelabels.PolicyLabel:    "policy-1",
							gkelabels.FlexStartLabel: "true",
						},
					},
				},
			}},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:              "test-pr-name-0-0",
					generateName:      "test-pr-name-",
					containerName:     "test-container",
					containerImage:    "test-image",
					provReqName:       "test-pr-name",
					creationTimestamp: testTS,
					nodeSelectors: map[string]string{
						gkelabels.GPULabel:               gkelabels.NvidiaGB200,
						gkelabels.PolicyLabel:            "policy-1",
						gkelabels.FlexStartLabel:         "true",
						gkelabels.MaxRunDurationLabelKey: "604800",
					},
				}),
			},
		},
		{
			desc: "ProvReq with Bulk provisioning pods and MRD 3600 - sets MRD node selector to 3600",
			pr: &prv1.ProvisioningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-pr-name",
					Namespace:         "test-namespace",
					CreationTimestamp: metav1.NewTime(testTS),
				},
				Spec: prv1.ProvisioningRequestSpec{
					ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
					PodSets: []prv1.PodSet{
						{
							Count: 2,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt0-name",
							},
						},
						{
							Count: 2,
							PodTemplateRef: prv1.Reference{
								Name: "test-pt1-name",
							},
						},
					},
					Parameters: map[string]prv1.Parameter{
						queuedwrapper.MaxRunDurationSecondsKey: "3600",
					},
				},
			},
			pt: []*v1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt0-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
						NodeSelector: map[string]string{
							gkelabels.GPULabel:       gkelabels.NvidiaGB200,
							gkelabels.PolicyLabel:    "policy-1",
							gkelabels.FlexStartLabel: "true",
						},
					},
				},
			}, {
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pt1-name",
					Namespace: "test-namespace",
				},
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "test-container",
								Image: "test-image",
							},
						},
						NodeSelector: map[string]string{
							gkelabels.GPULabel:       gkelabels.NvidiaGB200,
							gkelabels.PolicyLabel:    "policy-1",
							gkelabels.FlexStartLabel: "true",
						},
					},
				},
			}},
			want: []*v1.Pod{
				testPod(testPodConfig{
					name:              "test-pr-name-0-0",
					generateName:      "test-pr-name-",
					containerName:     "test-container",
					containerImage:    "test-image",
					provReqName:       "test-pr-name",
					creationTimestamp: testTS,
					nodeSelectors: map[string]string{
						gkelabels.GPULabel:               gkelabels.NvidiaGB200,
						gkelabels.PolicyLabel:            "policy-1",
						gkelabels.FlexStartLabel:         "true",
						gkelabels.MaxRunDurationLabelKey: "3600",
					},
				}),
				testPod(testPodConfig{
					name:              "test-pr-name-0-1",
					generateName:      "test-pr-name-",
					containerName:     "test-container",
					containerImage:    "test-image",
					provReqName:       "test-pr-name",
					creationTimestamp: testTS,
					nodeSelectors: map[string]string{
						gkelabels.GPULabel:               gkelabels.NvidiaGB200,
						gkelabels.PolicyLabel:            "policy-1",
						gkelabels.FlexStartLabel:         "true",
						gkelabels.MaxRunDurationLabelKey: "3600",
					},
				}),
				testPod(testPodConfig{
					name:              "test-pr-name-1-0",
					generateName:      "test-pr-name-",
					containerName:     "test-container",
					containerImage:    "test-image",
					provReqName:       "test-pr-name",
					creationTimestamp: testTS,
					nodeSelectors: map[string]string{
						gkelabels.GPULabel:               gkelabels.NvidiaGB200,
						gkelabels.PolicyLabel:            "policy-1",
						gkelabels.FlexStartLabel:         "true",
						gkelabels.MaxRunDurationLabelKey: "3600",
					},
				}),
				testPod(testPodConfig{
					name:              "test-pr-name-1-1",
					generateName:      "test-pr-name-",
					containerName:     "test-container",
					containerImage:    "test-image",
					provReqName:       "test-pr-name",
					creationTimestamp: testTS,
					nodeSelectors: map[string]string{
						gkelabels.GPULabel:               gkelabels.NvidiaGB200,
						gkelabels.PolicyLabel:            "policy-1",
						gkelabels.FlexStartLabel:         "true",
						gkelabels.MaxRunDurationLabelKey: "3600",
					},
				}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.ProvisioningRequestBulkMigsFlag: true,
			}, nil)
			got, err := pods.PodsForProvisioningRequest(provider, experimentsManager, provreqwrapper.NewProvisioningRequest(tt.pr, tt.pt))
			if (err != nil) != tt.wantErr {
				t.Errorf("PodsForProvisioningRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want, protocmp.Transform(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("unexpected response from PodsForProvisioningRequest(), diff (-want +got): %v", diff)
			}
		})
	}
}

func TestDoNotScheduleOnDWS(t *testing.T) {
	tests := []struct {
		name string
		n    *framework.NodeInfo
		want bool
	}{
		{
			name: "node without Queued taint",
			n: framework.NewTestNodeInfo(&v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{},
				},
			}),
			want: true,
		},
		{
			name: "node with Queued taint",
			n: framework.NewTestNodeInfo(&v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:   pods.QueuedTaintKey,
							Value: "true",
						},
					},
				},
			}),
			want: false,
		},
		{
			name: "node with other taint",
			n: framework.NewTestNodeInfo(&v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:   "some-other-taint",
							Value: "true",
						},
					},
				},
			}),
			want: true,
		},
		{
			name: "node with other taints, but with queued",
			n: framework.NewTestNodeInfo(&v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:   "some-other-taint1",
							Value: "true",
						},
						{
							Key:   "some-other-taint2",
							Value: "true",
						},
						{
							Key:   "some-other-taint3",
							Value: "true",
						},
						{
							Key:   pods.QueuedTaintKey,
							Value: "true",
						},
						{
							Key:   "some-other-taint4",
							Value: "true",
						},
					},
				},
			}),
			want: false,
		},
		{
			name: "node without taint array",
			n: framework.NewTestNodeInfo(&v1.Node{
				Spec: v1.NodeSpec{},
			}),
			want: true,
		},
		{
			name: "nil node",
			n:    framework.NewTestNodeInfo(nil),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pods.DoNotScheduleOnDWS(tt.n); got != tt.want {
				t.Errorf("DoNotScheduleOnDWS() = %v, want %v", got, tt.want)
			}
		})
	}
}

type bulkPodConfig struct {
	name        string
	policy      string
	accelerator string
	flex        bool
}
type usesBulkTestConfig struct {
	name               string
	pod                *v1.Pod
	wantUsesBulk       bool
	experimentDisabled bool
}

func TestUsesBulkProvisioning(t *testing.T) {
	testPod := func(config bulkPodConfig) *v1.Pod {
		pod := test.BuildTestPod(config.name, 1, 1)
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = map[string]string{}
		}
		// GB200 is known accelerator with slices support
		pod.Spec.NodeSelector[gkelabels.GPULabel] = config.accelerator
		pod.Spec.NodeSelector[gkelabels.PolicyLabel] = config.policy
		pod.Spec.NodeSelector[gkelabels.FlexStartLabel] = fmt.Sprintf("%v", config.flex)
		return pod
	}
	tests := []usesBulkTestConfig{
		{
			name: "experiment disabled - false",
			pod: testPod(bulkPodConfig{
				name:        "bulk-pod",
				policy:      "policy-1",
				accelerator: gkelabels.NvidiaGB200,
				flex:        true,
			}),
			experimentDisabled: true,
			wantUsesBulk:       false,
		},
		{
			name:         "no node selectors - false",
			pod:          test.BuildTestPod("non-bulk-pod", 1, 1),
			wantUsesBulk: false,
		},
	}
	for _, usesFlex := range []bool{true, false} {
		for _, policyName := range []string{"", "policy-1"} {
			for _, accelerator := range []string{gkelabels.NvidiaB200, gkelabels.NvidiaGB200} {
				wantUsesBulk := usesFlex == true && policyName != "" && accelerator == gkelabels.NvidiaGB200
				podConfig := bulkPodConfig{
					name:        "pod",
					policy:      policyName,
					accelerator: accelerator,
					flex:        usesFlex,
				}
				tests = append(tests, usesBulkTestConfig{
					name:         fmt.Sprintf("flex=%v, policyName=%v, accelerator=%v - returns %v", podConfig.flex, podConfig.policy, podConfig.accelerator, wantUsesBulk),
					pod:          testPod(podConfig),
					wantUsesBulk: wantUsesBulk,
				})
			}
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.ProvisioningRequestBulkMigsFlag: !tc.experimentDisabled,
			}, nil)
			if got := pods.UsesBulkProvisioning(provider, experimentsManager, tc.pod); got != tc.wantUsesBulk {
				t.Errorf("UsesBulkProvisioning fail: wanted %v, got %v", tc.wantUsesBulk, got)
			}
		})
	}

}
