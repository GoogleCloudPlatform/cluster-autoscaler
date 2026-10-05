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

package capacitybuffers

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	quota "k8s.io/apiserver/pkg/quota/v1"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	csn "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	procbuffers "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
)

// internalCapacityBuffer captures the flat, unified specification and identity of either a live CapacityBuffer
// from client.List or a transient in-flight creation record from cccInFlightCache.
type internalCapacityBuffer struct {
	Name                 string
	UID                  types.UID
	ProvisioningStrategy string
	Limits               corev1.ResourceList
	PendingCreation      bool
	CreationTimestamp    time.Time
}

const (
	// NamespaceGkeManagedCCC is the system namespace reserved for GKE managed ComputeClass resources.
	NamespaceGkeManagedCCC = "gke-managed-ccc"
	// fieldOwnerCCC identifies this controller as the field manager in its Patch calls.
	fieldOwnerCCC = "ccc-capacity-buffers"
	// kindComputeClass is the API Kind name for ComputeClass resources.
	kindComputeClass = "ComputeClass"
	labelManagedBy   = "autoscaling.gke.io/managed-by"

	// managedByController is the canonical label value for resources owned by this controller.
	managedByController = "ccc-cb-controller"

	// conditionTypeBuffersConfigured is the condition type for ComputeClass status condition.
	conditionTypeBuffersConfigured = "BuffersConfigured"

	// Provisioning strategies supported by capacity buffers.
	activeCapacityStrategy  = capacitybuffer.ActiveProvisioningStrategy
	standbyCapacityStrategy = procbuffers.ColdProvisioningStrategy
)

var (
	defaultCPUQuantity = resource.MustParse("2")
	defaultMemQuantity = resource.MustParse("2Gi")
)

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:3])
}

// podTemplateName returns the deterministic PodTemplate name for a given ComputeClass and provisioning strategy.
// All non-standby strategies (both active-capacity and custom strategies) share the "pt-basic-" PodTemplate.
// Sharing this template is safe and race-free under Server-Side Apply because the generated PodTemplate
// labels and PodSpec only distinguish between standbyCapacityStrategy and non-standby strategies, and do not
// include the strategy name itself.
// While workloads match a ComputeClass via a 63-character label, ComputeClass is a cluster-scoped CR whose
// metadata.name allows up to 253 characters (RFC 1123 DNS subdomain), so names exceeding 253 characters
// truncate ccName and append a deterministic hash of ccName.
func podTemplateName(ccName, strategy string) (string, error) {
	if ccName == "" {
		return "", fmt.Errorf("compute class name cannot be empty")
	}
	prefix := "pt-basic-"
	if strategy == standbyCapacityStrategy {
		prefix = "pt-standby-"
	}
	name := prefix + ccName
	// TODO(b/567423309): investigate a method for generating names that results in no potential collisions
	if len(name) > 253 {
		hash := shortHash(ccName)
		maxBaseLen := 253 - len(prefix) - len(hash) - 1
		name = fmt.Sprintf("%s%s-%s", prefix, ccName[:maxBaseLen], hash)
	}
	return name, nil
}

func capacityBufferPrefix(ccName, strategy string) string {
	limit := 30
	truncated := ccName
	if len(truncated) > limit {
		truncated = truncated[:limit]
	}
	return fmt.Sprintf("ccc-buffer-%s-%s-", truncated, shortHash(ccName+"-"+strategy))
}

// buildComputeClassOwnerRefSSAConfig builds the apply configuration OwnerReference linking children to their ComputeClass parent.
func buildComputeClassOwnerRefSSAConfig(cc *ccapiv1.ComputeClass) *metav1ac.OwnerReferenceApplyConfiguration {
	blockOwnerDeletion := true
	controller := true
	return metav1ac.OwnerReference().
		WithAPIVersion(ccapiv1.SchemeGroupVersion.String()).
		WithKind(kindComputeClass).
		WithName(cc.Name).
		WithUID(cc.UID).
		WithController(controller).
		WithBlockOwnerDeletion(blockOwnerDeletion)
}

// buildPodTemplateSSAConfig builds the Server-Side Apply configuration (*corev1ac.PodTemplateApplyConfiguration) for client.Apply.
func buildPodTemplateSSAConfig(cc *ccapiv1.ComputeClass, strategy string) (*corev1ac.PodTemplateApplyConfiguration, error) {
	ptName, err := podTemplateName(cc.Name, strategy)
	if err != nil {
		return nil, fmt.Errorf("building pod template SSA config: %w", err)
	}

	limits := corev1.ResourceList{
		corev1.ResourceCPU:    defaultCPUQuantity.DeepCopy(),
		corev1.ResourceMemory: defaultMemQuantity.DeepCopy(),
	}

	containerConfig := corev1ac.Container().
		WithName("buffer").
		WithImage("gke.gcr.io/pause:3.6").
		WithResources(corev1ac.ResourceRequirements().
			WithRequests(limits).
			WithLimits(limits))

	tolerationConfig := corev1ac.Toleration().
		WithOperator(corev1.TolerationOpExists)

	podSpecConfig := corev1ac.PodSpec().
		WithContainers(containerConfig).
		WithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: cc.Name}).
		WithTolerations(tolerationConfig)

	if strategy != standbyCapacityStrategy {
		affinityConfig := corev1ac.Affinity().
			WithNodeAffinity(corev1ac.NodeAffinity().
				WithRequiredDuringSchedulingIgnoredDuringExecution(corev1ac.NodeSelector().
					WithNodeSelectorTerms(corev1ac.NodeSelectorTerm().
						WithMatchExpressions(corev1ac.NodeSelectorRequirement().
							WithKey(csn.SoftWorkloadSeparationKey).
							WithOperator(corev1.NodeSelectorOpNotIn).
							WithValues("true")))))
		podSpecConfig.WithAffinity(affinityConfig)
	}

	labels := commonBufferLabels(cc.Name)
	ptConfig := corev1ac.PodTemplate(ptName, NamespaceGkeManagedCCC).
		WithLabels(labels).
		WithOwnerReferences(buildComputeClassOwnerRefSSAConfig(cc)).
		WithTemplate(corev1ac.PodTemplateSpec().
			WithLabels(labels).
			WithSpec(podSpecConfig))

	return ptConfig, nil
}

func toCBResourceList(limits corev1.ResourceList) cbv1beta1.ResourceList {
	cbLimits := make(cbv1beta1.ResourceList, len(limits))
	for k, v := range limits {
		cbLimits[cbv1beta1.ResourceName(k)] = v.DeepCopy()
	}
	return cbLimits
}

func toCoreResourceList(limits *cbv1beta1.ResourceList) corev1.ResourceList {
	if limits == nil {
		return nil
	}
	res := make(corev1.ResourceList, len(*limits))
	for k, v := range *limits {
		res[corev1.ResourceName(k)] = v.DeepCopy()
	}
	return res
}

func commonBufferLabels(ccName string) map[string]string {
	return map[string]string{
		gkelabels.ComputeClassLabel: ccName,
		labelManagedBy:              managedByController,
	}
}

// buildCapacityBuffer constructs a typed *cbv1beta1.CapacityBuffer.
// If name is non-empty, Name and UID are populated (e.g. for existing/in-flight buffers).
// If name is empty, GenerateName is populated with the strategy prefix (for creations).
func buildCapacityBuffer(
	cc *ccapiv1.ComputeClass,
	buf ccapiv1.ComputeClassBuffer,
	podTemplateName string,
	name string,
	uid types.UID,
) *cbv1beta1.CapacityBuffer {
	strategy := buf.ProvisioningStrategy
	limits := toCBResourceList(buf.Limits)
	blockOwnerDeletion := true
	controller := true

	cb := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: NamespaceGkeManagedCCC,
			Labels:    commonBufferLabels(cc.Name),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         ccapiv1.SchemeGroupVersion.String(),
					Kind:               kindComputeClass,
					Name:               cc.Name,
					UID:                cc.UID,
					Controller:         &controller,
					BlockOwnerDeletion: &blockOwnerDeletion,
				},
			},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &strategy,
			PodTemplateRef:       &cbv1beta1.LocalObjectRef{Name: podTemplateName},
			Limits:               &limits,
		},
	}

	if name != "" {
		cb.Name = name
		cb.UID = uid
	} else {
		cb.GenerateName = capacityBufferPrefix(cc.Name, strategy)
	}
	return cb
}

// buildCapacityBufferForCreate constructs a typed *cbv1beta1.CapacityBuffer for creation with GenerateName.
func buildCapacityBufferForCreate(cc *ccapiv1.ComputeClass, buf ccapiv1.ComputeClassBuffer, podTemplateName string) *cbv1beta1.CapacityBuffer {
	return buildCapacityBuffer(cc, buf, podTemplateName, "", "")
}

// capacityBufferStub returns a minimal CapacityBuffer with metadata identifying the target resource for client operations.
func capacityBufferStub(name string, uid types.UID) *cbv1beta1.CapacityBuffer {
	return &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: NamespaceGkeManagedCCC,
			UID:       uid,
		},
	}
}

func buildInternalCapacityBuffer(cb *cbv1beta1.CapacityBuffer) internalCapacityBuffer {
	return internalCapacityBuffer{
		Name:                 cb.Name,
		UID:                  cb.UID,
		ProvisioningStrategy: getCapacityBufferStrategy(cb),
		Limits:               toCoreResourceList(getCapacityBufferLimits(cb)),
		PendingCreation:      false,
		CreationTimestamp:    cb.CreationTimestamp.Time,
	}
}

// TODO(b/555784871): Move this to a CapacityBuffer package.
// DefaultProvisioningStrategy returns the default capacity buffer provisioning strategy ("buffer.x-k8s.io/active-capacity").
func DefaultProvisioningStrategy() string {
	return activeCapacityStrategy
}

// TODO(b/555784871): Move this to a CapacityBuffer package.
// getCapacityBufferStrategy extracts the provisioning strategy, prioritizing Status over Spec.
// If the field is omitted (nil) or empty (""), it defaults to DefaultProvisioningStrategy().
// If the buffer object itself is nil, it returns "".
func getCapacityBufferStrategy(cb *cbv1beta1.CapacityBuffer) string {
	if cb == nil {
		return ""
	}
	if cb.Status.ProvisioningStrategy != nil && *cb.Status.ProvisioningStrategy != "" {
		return *cb.Status.ProvisioningStrategy
	}
	if cb.Spec.ProvisioningStrategy != nil && *cb.Spec.ProvisioningStrategy != "" {
		return *cb.Spec.ProvisioningStrategy
	}
	return DefaultProvisioningStrategy()
}

// TODO(b/555784871): Move this to a CapacityBuffer package.
// isSupportedProvisioningStrategy checks whether a provisioning strategy is built-in and supported by CA.
func isSupportedProvisioningStrategy(strategy string) bool {
	return strategy == activeCapacityStrategy || strategy == standbyCapacityStrategy
}

// getCapacityBufferLimits returns the Limits pointer from Spec.
func getCapacityBufferLimits(cb *cbv1beta1.CapacityBuffer) *cbv1beta1.ResourceList {
	if cb == nil {
		return nil
	}
	return cb.Spec.Limits
}

// hasOwnerReferenceUID checks whether an object carries an OwnerReference whose UID matches the provided parent UID.
func hasOwnerReferenceUID(obj metav1.Object, ownerUID types.UID) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == ownerUID {
			return true
		}
	}
	return false
}

// computeUpdatedConditions purely computes a new Condition slice for the BuffersConfigured condition type without mutating existing items.
func computeUpdatedConditions(existingSlice []metav1.Condition, status metav1.ConditionStatus, reason, message string, generation int64) []metav1.Condition {
	// 1. Zero-Allocation Steady-State Fast-Path: check if identical condition already exists upfront
	for i, existing := range existingSlice {
		if existing.Type == conditionTypeBuffersConfigured {
			if existing.Status == status && existing.Reason == reason && existing.Message == message && existing.ObservedGeneration == generation {
				return existingSlice // Return immediately with 0 allocations!
			}
			// Condition exists but changed: allocate and update exactly once
			result := make([]metav1.Condition, len(existingSlice))
			copy(result, existingSlice)
			newCond := metav1.Condition{
				Type:               conditionTypeBuffersConfigured,
				Status:             status,
				Reason:             reason,
				Message:            message,
				ObservedGeneration: generation,
				LastTransitionTime: metav1.Now(),
			}
			if existing.Status == status {
				newCond.LastTransitionTime = existing.LastTransitionTime
			}
			result[i] = newCond
			return result
		}
	}

	// 2. Condition not found: allocate and append new condition
	newCond := metav1.Condition{
		Type:               conditionTypeBuffersConfigured,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
	}
	result := make([]metav1.Condition, len(existingSlice), len(existingSlice)+1)
	copy(result, existingSlice)
	return append(result, newCond)
}

// removeBufferConditions purely returns a Condition slice with "BuffersConfigured" filtered out without mutating existing items.
func removeBufferConditions(existingSlice []metav1.Condition) []metav1.Condition {
	found := false
	for _, existing := range existingSlice {
		if existing.Type == conditionTypeBuffersConfigured {
			found = true
			break
		}
	}
	if !found {
		return existingSlice // Zero-allocation fast-path if already empty/cleared!
	}

	result := make([]metav1.Condition, 0, len(existingSlice)-1)
	for _, existing := range existingSlice {
		if existing.Type != conditionTypeBuffersConfigured {
			result = append(result, existing)
		}
	}
	return result
}

// cbFullyConforms checks if an existing or in-flight CapacityBuffer already matches desired specification across all critical fields.
func cbFullyConforms(cb *cbv1beta1.CapacityBuffer, cc *ccapiv1.ComputeClass, buf ccapiv1.ComputeClassBuffer, targetPTName string) bool {
	if cb == nil {
		return false
	}
	// Compare Spec, not Status: the CapacityBuffer controller sets
	// Status.PodTemplateRef to its own derived PodTemplate, which never
	// matches targetPTName.
	if cb.Spec.PodTemplateRef == nil || cb.Spec.PodTemplateRef.Name != targetPTName {
		return false
	}
	if !quota.Equals(toCoreResourceList(getCapacityBufferLimits(cb)), buf.Limits) {
		return false
	}
	if !hasOwnerReferenceUID(cb, cc.UID) {
		return false
	}
	if getCapacityBufferStrategy(cb) != buf.ProvisioningStrategy {
		return false
	}
	if cb.Labels == nil {
		return false
	}
	for k, v := range commonBufferLabels(cc.Name) {
		if cb.Labels[k] != v {
			return false
		}
	}
	return true
}
