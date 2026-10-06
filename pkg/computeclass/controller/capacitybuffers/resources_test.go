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
	"strings"
	"testing"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestResourceNamingAndBounds(t *testing.T) {
	// Empty bounds check
	if _, err := podTemplateName("", activeCapacityStrategy); err == nil {
		t.Fatalf("Expected error for empty ccName")
	}

	// 253 max char bound check: should gracefully truncate and keep <= 253 chars
	longName := strings.Repeat("a", 250)
	truncatedPTName, err := podTemplateName(longName, standbyCapacityStrategy)
	if err != nil {
		t.Fatalf("Unexpected error for long ccName: %v", err)
	}
	if len(truncatedPTName) > 253 {
		t.Fatalf("Expected podTemplateName <= 253 chars, got %d (%s)", len(truncatedPTName), truncatedPTName)
	}
	truncatedActivePT, err := podTemplateName(longName, activeCapacityStrategy)
	if err != nil {
		t.Fatalf("Unexpected error for long active ccName: %v", err)
	}
	truncatedCustomPT, err := podTemplateName(longName, "custom.io/strategy")
	if err != nil {
		t.Fatalf("Unexpected error for long custom ccName: %v", err)
	}
	if truncatedActivePT != truncatedCustomPT {
		t.Fatalf("Expected active and custom strategies to share the same truncated pt-basic name, got %s vs %s", truncatedActivePT, truncatedCustomPT)
	}

	validBasicName, err := podTemplateName("hello-world", activeCapacityStrategy)
	if err != nil || validBasicName != "pt-basic-hello-world" {
		t.Fatalf("Invalid basic pod template formulation: %v, %v", err, validBasicName)
	}

	validStandbyName, err := podTemplateName("hello-world", standbyCapacityStrategy)
	if err != nil || validStandbyName != "pt-standby-hello-world" {
		t.Fatalf("Invalid standby pod template formulation: %v, %v", err, validStandbyName)
	}

	h1 := shortHash("hello-active")
	h2 := shortHash("hello-active")
	if h1 != h2 {
		t.Fatalf("shortHash was non-deterministic")
	}
	if len(h1) != 6 {
		t.Fatalf("shortHash unexpectedly violated 6 byte hex encoding length")
	}
}

func TestConditionManagement(t *testing.T) {
	// Zero-allocation removal
	existing := []metav1.Condition{
		{Type: "AnotherCondition", Status: metav1.ConditionTrue},
	}
	result := removeBufferConditions(existing)
	// Same capacity/pointer inherently shows slice was unmodified
	if &result[0] != &existing[0] {
		t.Fatalf("removeBufferConditions allocated fresh memory when it wasn't required")
	}

	// Successful removal
	existingWithBuf := []metav1.Condition{
		{Type: "AnotherCondition", Status: metav1.ConditionTrue},
		{Type: "BuffersConfigured", Status: metav1.ConditionFalse},
	}
	resultCleaned := removeBufferConditions(existingWithBuf)
	if len(resultCleaned) != 1 || resultCleaned[0].Type != "AnotherCondition" {
		t.Fatalf("removeBufferConditions failed to strip the correct entity")
	}

	// Zero-allocation computation
	tNow := metav1.Now()
	existingIdentical := []metav1.Condition{
		{Type: "BuffersConfigured", Status: metav1.ConditionTrue, Reason: "Provisioned", Message: "Done", ObservedGeneration: 1, LastTransitionTime: tNow},
	}
	resultIdentical := computeUpdatedConditions(existingIdentical, metav1.ConditionTrue, "Provisioned", "Done", 1)
	if &resultIdentical[0] != &existingIdentical[0] {
		t.Fatalf("computeUpdatedConditions allocated memory when identically mapped")
	}

	// Mutation allocation
	resultMutated := computeUpdatedConditions(existingIdentical, metav1.ConditionFalse, "FailedMsg", "Err", 1)
	if len(resultMutated) != 1 || resultMutated[0].Status != metav1.ConditionFalse || resultMutated[0].Reason != "FailedMsg" || resultMutated[0].ObservedGeneration != 1 {
		t.Fatalf("computeUpdatedConditions failed to actually mutate the conditions upon delta")
	}

	// Generation change allocation
	resultGenChange := computeUpdatedConditions(existingIdentical, metav1.ConditionTrue, "Provisioned", "Done", 2)
	if len(resultGenChange) != 1 || resultGenChange[0].ObservedGeneration != 2 {
		t.Fatalf("computeUpdatedConditions failed to update ObservedGeneration on generation change")
	}
}

func TestStrategyExtraction(t *testing.T) {
	if DefaultProvisioningStrategy() != activeCapacityStrategy {
		t.Fatalf("Expected DefaultProvisioningStrategy to return %s, got %s", activeCapacityStrategy, DefaultProvisioningStrategy())
	}

	// Strategy extraction precedence
	activeStr := activeCapacityStrategy
	cb := &cbv1beta1.CapacityBuffer{
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStr,
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				gkelabels.ComputeClassLabel: "test-cc",
			},
		},
	}
	if getCapacityBufferStrategy(cb) != activeCapacityStrategy {
		t.Fatalf("Expected extraction to prioritize Spec strictly")
	}

	cb.Spec.ProvisioningStrategy = nil
	if getCapacityBufferStrategy(cb) != activeCapacityStrategy {
		t.Fatalf("Expected extraction with nil Spec to default to %v", activeCapacityStrategy)
	}

	emptyStr := ""
	cb.Spec.ProvisioningStrategy = &emptyStr
	if getCapacityBufferStrategy(cb) != activeCapacityStrategy {
		t.Fatalf("Expected empty string strategy to fallback to %v", activeCapacityStrategy)
	}

	if getCapacityBufferStrategy(nil) != "" {
		t.Fatalf("Expected nil object to instantly return blanks")
	}

	// Test limits getter and toCoreResourceList converter
	if getCapacityBufferLimits(nil) != nil || getCapacityBufferLimits(cb) != nil {
		t.Fatalf("Expected nil limits for nil/empty spec limits")
	}
	if toCoreResourceList(nil) != nil || toCoreResourceList(getCapacityBufferLimits(cb)) != nil {
		t.Fatalf("Expected nil core limits for nil/empty spec limits")
	}
	cb.Spec.Limits = &cbv1beta1.ResourceList{
		cbv1beta1.ResourceName("cpu"): resource.MustParse("2"),
	}
	if getCapacityBufferLimits(cb) == nil {
		t.Fatalf("Expected non-nil limits")
	}
	coreLimits := toCoreResourceList(getCapacityBufferLimits(cb))
	cpuLimit := coreLimits[corev1.ResourceCPU]
	if cpuLimit.String() != "2" {
		t.Fatalf("Expected 2 CPU limits, got %v", cpuLimit)
	}
}

func TestIsSupportedProvisioningStrategy(t *testing.T) {
	testCases := []struct {
		description string
		strategy    string
		expected    bool
	}{
		{
			description: "active capacity strategy is supported",
			strategy:    activeCapacityStrategy,
			expected:    true,
		},
		{
			description: "standby capacity strategy is supported",
			strategy:    standbyCapacityStrategy,
			expected:    true,
		},
		{
			description: "custom strategy is not supported",
			strategy:    "custom-strategy",
			expected:    false,
		},
		{
			description: "empty strategy is not supported",
			strategy:    "",
			expected:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			if got := isSupportedProvisioningStrategy(tc.strategy); got != tc.expected {
				t.Errorf("isSupportedProvisioningStrategy(%q) = %v, expected %v", tc.strategy, got, tc.expected)
			}
		})
	}
}

func TestPodTemplateAffinitySeparation(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cc",
			UID:  "test-uid",
		},
	}

	// Under Option A, active templates carry NodeAffinity NotIn ["true"] and wildcard toleration {operator: "Exists"}
	activePT, err := buildPodTemplateSSAConfig(cc, activeCapacityStrategy)
	if err != nil {
		t.Fatalf("Building active pod template failed: %v", err)
	}

	if activePT.Template.Spec.Affinity == nil || activePT.Template.Spec.Affinity.NodeAffinity == nil {
		t.Fatalf("Active PodTemplate expected NodeAffinity restriction under Option A")
	}
	if len(activePT.Template.Spec.Tolerations) != 1 || *activePT.Template.Spec.Tolerations[0].Operator != corev1.TolerationOpExists {
		t.Fatalf("Active PodTemplate expected wildcard toleration under Option A")
	}

	standbyPT, err := buildPodTemplateSSAConfig(cc, standbyCapacityStrategy)
	if err != nil {
		t.Fatalf("Building Standby pod template failed: %v", err)
	}

	if standbyPT.Template.Spec.Affinity != nil && standbyPT.Template.Spec.Affinity.NodeAffinity != nil {
		t.Fatalf("Standby PodTemplate should NOT have NodeAffinity restriction (to avoid contradiction with MakePodCSN nodeSelector)")
	}
	if len(standbyPT.Template.Spec.Tolerations) != 1 || *standbyPT.Template.Spec.Tolerations[0].Operator != corev1.TolerationOpExists {
		t.Fatalf("Standby PodTemplate expected wildcard toleration under Option A")
	}
}

func TestCBFullyConforms(t *testing.T) {
	ptName := "test-pt"
	strategy := activeCapacityStrategy

	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-valid-cc",
			UID:  "cc-uid",
		},
	}
	buf := ccapiv1.ComputeClassBuffer{
		ProvisioningStrategy: strategy,
		Limits: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"),
		},
	}

	cb := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Labels: commonBufferLabels("my-valid-cc"),
			OwnerReferences: []metav1.OwnerReference{
				{Kind: kindComputeClass, UID: "cc-uid"},
			},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &strategy,
			PodTemplateRef:       &cbv1beta1.LocalObjectRef{Name: ptName},
			Limits: &cbv1beta1.ResourceList{
				cbv1beta1.ResourceName("cpu"): resource.MustParse("2"),
			},
		},
	}

	if cbFullyConforms(nil, cc, buf, ptName) {
		t.Fatalf("Expected cbFullyConforms with nil cb to return false")
	}

	if !cbFullyConforms(cb, cc, buf, ptName) {
		t.Fatalf("Structurally identical layout flagged as non-conforming")
	}

	// The CapacityBuffer controller points Status.PodTemplateRef at its own
	// derived PodTemplate, so conformance must be decided by Spec alone.
	cb.Status.PodTemplateRef = &cbv1beta1.LocalObjectRef{Name: "capacitybuffer-cb-pod-template"}
	if !cbFullyConforms(cb, cc, buf, ptName) {
		t.Fatalf("Derived Status.PodTemplateRef flagged as non-conforming")
	}
	cb.Spec.PodTemplateRef = &cbv1beta1.LocalObjectRef{Name: "other-pt"}
	cb.Status.PodTemplateRef = &cbv1beta1.LocalObjectRef{Name: ptName}
	if cbFullyConforms(cb, cc, buf, ptName) {
		t.Fatalf("Spec.PodTemplateRef drift hidden by Status.PodTemplateRef")
	}
	cb.Spec.PodTemplateRef = nil
	if cbFullyConforms(cb, cc, buf, ptName) {
		t.Fatalf("Missing Spec.PodTemplateRef flagged as conforming")
	}
	cb.Spec.PodTemplateRef = &cbv1beta1.LocalObjectRef{Name: ptName}
	cb.Status.PodTemplateRef = nil

	// Simulates label modification drift to verify structural bounds constraints
	cb.Labels[gkelabels.ComputeClassLabel] = "drifting"
	if cbFullyConforms(cb, cc, buf, ptName) {
		t.Fatalf("Drifted capacity buffer mapped structurally without errors isolating constraints")
	}
}

func TestBuildCapacityBuffer(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cc",
			UID:  "test-uid",
		},
	}
	buf := ccapiv1.ComputeClassBuffer{
		ProvisioningStrategy: activeCapacityStrategy,
		Limits: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"),
		},
	}

	// Test typed builder for creation
	createdCB := buildCapacityBufferForCreate(cc, buf, "test-pt")
	if !strings.HasPrefix(createdCB.GenerateName, "ccc-buffer-") {
		t.Fatalf("Expected GenerateName prefix ccc-buffer-, got: %v", createdCB.GenerateName)
	}
	if createdCB.Namespace != NamespaceGkeManagedCCC {
		t.Fatalf("Expected namespace %s, got: %s", NamespaceGkeManagedCCC, createdCB.Namespace)
	}
	if createdCB.Labels[labelManagedBy] != managedByController || createdCB.Labels[gkelabels.ComputeClassLabel] != "test-cc" {
		t.Fatalf("Unexpected labels on created CB: %v", createdCB.Labels)
	}
	if len(createdCB.OwnerReferences) != 1 || createdCB.OwnerReferences[0].Name != "test-cc" || createdCB.OwnerReferences[0].Kind != kindComputeClass {
		t.Fatalf("Unexpected owner references on created CB: %v", createdCB.OwnerReferences)
	}

	// Test typed builder with explicit name/UID
	namedCB := buildCapacityBuffer(cc, buf, "test-pt", "named-cb", "uid-123")
	if namedCB.Name != "named-cb" || namedCB.UID != "uid-123" || namedCB.GenerateName != "" {
		t.Fatalf("Unexpected Name/UID on named CB: name=%s, uid=%s, generateName=%s", namedCB.Name, namedCB.UID, namedCB.GenerateName)
	}
	if namedCB.Namespace != NamespaceGkeManagedCCC {
		t.Fatalf("Expected namespace %s, got: %s", NamespaceGkeManagedCCC, namedCB.Namespace)
	}

	// Test stub builder
	stubCB := capacityBufferStub("stub-cb", "uid-456")
	if stubCB.Name != "stub-cb" || stubCB.UID != "uid-456" || stubCB.Namespace != NamespaceGkeManagedCCC {
		t.Fatalf("Unexpected stub CB metadata: name=%s, uid=%s, ns=%s", stubCB.Name, stubCB.UID, stubCB.Namespace)
	}
}
