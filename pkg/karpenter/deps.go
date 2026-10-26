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

// Package karpenter contains the GKE integration for the Karpenter provisioning engine.
package karpenter

import (
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/kubernetes/scheme"

	// Blank imports to retain Karpenter dependencies in go.mod and vendor/
	// before the full integration is wired up.
	_ "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	_ "sigs.k8s.io/karpenter/pkg/apis/v1"
	_ "sigs.k8s.io/karpenter/pkg/cloudprovider"
	_ "sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	_ "sigs.k8s.io/karpenter/pkg/controllers/state"
	_ "sigs.k8s.io/karpenter/pkg/operator/options"
)

func init() {
	// Karpenter's pkg/apis/autoscaling/v1beta1 package (imported transitively by
	// pkg/controllers/state) unconditionally registers its own copy of CapacityBuffer
	// and CapacityBufferList into the global client-go scheme during init().
	// This conflicts with k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer,
	// which registers the canonical CapacityBuffer types into the same scheme and
	// panics on double registration of different Go types for the same GroupVersionKind.
	// Remove Karpenter's registrations and re-register the canonical Cluster Autoscaler types.
	knownTypes := scheme.Scheme.AllKnownTypes()
	delete(knownTypes, cbv1beta1.SchemeGroupVersion.WithKind("CapacityBuffer"))
	delete(knownTypes, cbv1beta1.SchemeGroupVersion.WithKind("CapacityBufferList"))
	utilruntime.Must(cbv1beta1.AddToScheme(scheme.Scheme))
}
