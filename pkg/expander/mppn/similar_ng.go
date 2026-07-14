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

package mppn

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func groupOptions(options []expander.Option) [][]expander.Option {
	similarOptions := make(map[string][]expander.Option)
	for _, option := range options {
		sig := optionSignature(option)
		similarOptions[sig] = append(similarOptions[sig], option)
	}
	result := make([][]expander.Option, 0, len(similarOptions))
	for _, opts := range similarOptions {
		if len(opts) > 1 {
			samePodsOptions := make(map[string][]expander.Option)
			for _, opt := range opts {
				podsSig := podsSignature(opt.Pods)
				samePodsOptions[podsSig] = append(samePodsOptions[podsSig], opt)
			}
			for _, opt := range samePodsOptions {
				result = append(result, opt)
			}
		} else {
			result = append(result, opts)
		}
	}
	return result
}

func optionSignature(option expander.Option) string {
	sig := strings.Builder{}
	sig.WriteString(fmt.Sprintf("%d-", option.NodeCount))
	sig.WriteString(fmt.Sprintf("pods_count:_%d", len(option.Pods)))
	gkeMig := option.NodeGroup.(*gke.GkeMig)
	sig.WriteString(migSignature(gkeMig))
	return sig.String()
}

func podsSignature(pods []*corev1.Pod) string {
	podUIDs := make([]string, 0, len(pods))
	for _, pod := range pods {
		podUIDs = append(podUIDs, string(pod.UID))
	}
	sort.Slice(podUIDs, func(i, j int) bool {
		return podUIDs[i] < podUIDs[j]
	})
	return strings.Join(podUIDs, ",")
}

func migSignature(mig *gke.GkeMig) string {
	sig := strings.Builder{}
	sig.WriteString(mig.MachineType())
	if mig.Spec().Accelerators != nil {
		sig.WriteString(fmt.Sprintf("%v", mig.Spec().Accelerators))
	}
	if mig.Spec().LocalSSDConfig != nil {
		sig.WriteString(fmt.Sprintf("%v", *mig.Spec().LocalSSDConfig))
	}
	sig.WriteString(mig.Spec().TpuTopology)
	sig.WriteString(mig.Spec().TpuType)
	sig.WriteString(fmt.Sprintf("%v", mig.Spec().TpuMultiHost))
	sig.WriteString(fmt.Sprintf("%d", mig.DiskSize()))
	return sig.String()
}
