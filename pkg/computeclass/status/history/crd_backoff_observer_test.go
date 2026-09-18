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

package history

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

func buildTestNodeGroup(id string, labels map[string]string) cloudprovider.NodeGroup {
	return gke.NewTestGkeMigBuilder().
		SetNodePoolName(id).
		SetSpec(&gkeclient.NodePoolSpec{
			Labels: labels,
		}).Build()
}

func TestCrdBackoffObserver_FullBackoff(t *testing.T) {
	now := time.Now()
	until := now.Add(5 * time.Minute)
	defaultErrorInfo := cloudprovider.InstanceErrorInfo{ErrorCode: "RESOURCE_POOL_EXHAUSTED"}
	crdName := "test-crd"
	testLabel := "test-label"

	testCases := []struct {
		name               string
		errorInfo          *cloudprovider.InstanceErrorInfo
		initialConditions  map[string][]metav1.Condition
		expectedConditions []metav1.Condition
	}{
		{
			name: "OnNpcBackoff adds full cooldown",
			expectedConditions: []metav1.Condition{
				{
					Type:    ConditionTypeNodeProvisioningInCooldown,
					Status:  metav1.ConditionTrue,
					Reason:  "OutOfResources",
					Message: fmt.Sprintf("NodeProvisioning associated with this priority failed due to the OutOfResources error. Backing off the priority until %v.", until.Format("2006-01-02 15:04:05 MST")),
				},
			},
		},
		{
			name: "OnNpcBackoff prolongs existing cooldown",
			initialConditions: map[string][]metav1.Condition{
				"0": {
					{
						Type:               ConditionTypeNodeProvisioningInCooldown,
						Status:             metav1.ConditionTrue,
						Reason:             "OutOfResources",
						Message:            fmt.Sprintf("NodeProvisioning associated with this priority failed due to the OutOfResources error. Backing off the priority until %v.", now.Format("2006-01-02 15:04:05 MST")),
						LastTransitionTime: metav1.Time{},
					},
				},
			},
			expectedConditions: []metav1.Condition{
				{
					Type:    ConditionTypeNodeProvisioningInCooldown,
					Status:  metav1.ConditionTrue,
					Reason:  "OutOfResources",
					Message: fmt.Sprintf("NodeProvisioning associated with this priority failed due to the OutOfResources error. Backing off the priority until %v.", until.Format("2006-01-02 15:04:05 MST")),
				},
			},
		},
		{
			name:      "OnNpcBackoff translates internal error",
			errorInfo: &cloudprovider.InstanceErrorInfo{ErrorCode: "UNKNOWN_ERROR"},
			expectedConditions: []metav1.Condition{
				{
					Type:    ConditionTypeNodeProvisioningInCooldown,
					Status:  metav1.ConditionTrue,
					Reason:  "InternalError",
					Message: fmt.Sprintf("NodeProvisioning associated with this priority failed due to the InternalError error. Backing off the priority until %v.", until.Format("2006-01-02 15:04:05 MST")),
				},
			},
		},
		{
			name:      "OnNpcBackoff translates quota error",
			errorInfo: &cloudprovider.InstanceErrorInfo{ErrorCode: "QUOTA_EXCEEDED"},
			expectedConditions: []metav1.Condition{
				{
					Type:    ConditionTypeNodeProvisioningInCooldown,
					Status:  metav1.ConditionTrue,
					Reason:  "QuotaExceeded",
					Message: fmt.Sprintf("NodeProvisioning associated with this priority failed due to the QuotaExceeded error. Backing off the priority until %v.", until.Format("2006-01-02 15:04:05 MST")),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updates := make(chan npc_status.UpdateMessage, 10)

			testCrd := crd.NewTestCrd(crd.WithLabel(testLabel),
				crd.WithName(crdName),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"pool-1"})),
				}))

			mockL := npc_lister.NewMockCrdListerWithLabel([]crd.CRD{testCrd}, testLabel)
			mockP := &gke.GkeCloudProviderMock{}
			mockP.On("IsAutopilotEnabled").Return(false).Maybe()

			informer := NewCrdBackoffObserver(updates, mockL, mockP, false)
			informer.now = func() time.Time { return now }

			errInfo := defaultErrorInfo
			if tc.errorInfo != nil {
				errInfo = *tc.errorInfo
			}

			informer.OnNpcBackoff(testCrd, 0, errInfo, until)

			select {
			case msg := <-updates:
				assert.Equal(t, crdName, msg.Id.CRDName)

				testStatus := crd.NewMockCRDStatus(tc.initialConditions)
				msg.Mutate(testStatus)

				assert.True(t, testStatus.UpdateRuleConditionsCalled, "Expected UpdateRuleConditions to be called")
				assert.Equal(t, "0", testStatus.RuleIdx)

				for i := range testStatus.Conditions {
					testStatus.Conditions[i].LastTransitionTime = metav1.Time{}
				}

				assert.Equal(t, tc.expectedConditions, testStatus.Conditions)
			case <-time.After(1 * time.Second):
				t.Fatalf("Expected update message but channel was empty")
			}
		})
	}
}

func TestCrdBackoffObserver_RemoveExpiredBackoffs(t *testing.T) {
	now := time.Now()
	expired := now.Add(-5 * time.Minute)
	notExpired := now.Add(5 * time.Minute)
	defaultErrorInfo := cloudprovider.InstanceErrorInfo{ErrorCode: "RESOURCE_POOL_EXHAUSTED"}
	crdName := "test-crd"
	testLabel := "test-label"

	testCases := []struct {
		name               string
		until              time.Time
		isFullCooldown     bool
		initialConditions  map[string][]metav1.Condition
		expectedConditions []metav1.Condition
	}{
		{
			name:           "RemoveExpiredBackoffs clears expired partial cooldown",
			until:          expired,
			isFullCooldown: false,
			initialConditions: map[string][]metav1.Condition{
				"0": {
					{Type: ConditionTypeNodeProvisioningInPartialCooldown, Status: metav1.ConditionTrue},
					{Type: "OtherCondition", Status: metav1.ConditionTrue},
				},
			},
			expectedConditions: []metav1.Condition{
				{Type: "OtherCondition", Status: metav1.ConditionTrue},
			},
		},
		{
			name:           "RemoveExpiredBackoffs keeps active partial cooldown",
			until:          notExpired,
			isFullCooldown: false,
			initialConditions: map[string][]metav1.Condition{
				"0": {
					{Type: ConditionTypeNodeProvisioningInPartialCooldown, Status: metav1.ConditionTrue},
					{Type: "OtherCondition", Status: metav1.ConditionTrue},
				},
			},
			expectedConditions: nil, // If it doesn't expire, it shouldn't mutate conditions
		},
		{
			name:           "RemoveExpiredBackoffs clears expired full cooldown",
			until:          expired,
			isFullCooldown: true,
			initialConditions: map[string][]metav1.Condition{
				"0": {
					{Type: ConditionTypeNodeProvisioningInCooldown, Status: metav1.ConditionTrue},
					{Type: ConditionTypeNodeProvisioningInPartialCooldown, Status: metav1.ConditionTrue},
				},
			},
			expectedConditions: []metav1.Condition{
				{Type: ConditionTypeNodeProvisioningInPartialCooldown, Status: metav1.ConditionTrue},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updates := make(chan npc_status.UpdateMessage, 10)

			testCrd := crd.NewTestCrd(crd.WithLabel(testLabel),
				crd.WithName(crdName),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"pool-1"})),
				}))

			mockL := npc_lister.NewMockCrdListerWithLabel([]crd.CRD{testCrd}, testLabel)
			mockP := &gke.GkeCloudProviderMock{}
			mockP.On("IsAutopilotEnabled").Return(false).Maybe()

			informer := NewCrdBackoffObserver(updates, mockL, mockP, false)
			informer.now = func() time.Time { return now }

			ng := buildTestNodeGroup("pool-1", map[string]string{testLabel: crdName})

			if tc.isFullCooldown {
				informer.OnNpcBackoff(testCrd, 0, defaultErrorInfo, tc.until)
			} else {
				informer.OnBackoff(ng, defaultErrorInfo, tc.until)
			}
			<-updates // Drain the setup message

			informer.RemoveExpiredBackoffs(now)

			if tc.expectedConditions == nil {
				select {
				case <-updates:
					t.Fatalf("Expected no update message because backoff didn't expire")
				case <-time.After(100 * time.Millisecond):
				}
			} else {
				select {
				case msg := <-updates:
					assert.Equal(t, crdName, msg.Id.CRDName)

					testStatus := crd.NewMockCRDStatus(tc.initialConditions)
					msg.Mutate(testStatus)

					assert.True(t, testStatus.UpdateRuleConditionsCalled, "Expected UpdateRuleConditions to be called")
					assert.Equal(t, "0", testStatus.RuleIdx)

					for i := range testStatus.Conditions {
						testStatus.Conditions[i].LastTransitionTime = metav1.Time{}
					}

					assert.Equal(t, tc.expectedConditions, testStatus.Conditions)
				case <-time.After(1 * time.Second):
					t.Fatalf("Expected update message but channel was empty")
				}
			}
		})
	}
}

func TestCrdBackoffObserver_ErrorDetails(t *testing.T) {
	now := time.Now()
	until := now.Add(5 * time.Minute)
	crdName := "test-crd"
	testLabel := "test-label"
	quotaError := "Quota 'CPUS' exceeded. Limit: 24.0 in region us-central1."
	quotaMessage := fmt.Sprintf("NodeProvisioning associated with this priority failed due to the QuotaExceeded error. Backing off the priority until %v.", until.Format("2006-01-02 15:04:05 MST"))
	unknownError := "A completely unknown error occurred."
	unknownMessage := fmt.Sprintf("NodeProvisioning associated with this priority failed due to the InternalError error. Backing off the priority until %v.", until.Format("2006-01-02 15:04:05 MST"))

	testCases := []struct {
		name                string
		includeErrorDetails bool
		errorInfo           cloudprovider.InstanceErrorInfo
		wantMessage         string
		wantReason          string
	}{
		{
			name:                "details omitted when experiment is disabled",
			includeErrorDetails: false,
			errorInfo:           cloudprovider.InstanceErrorInfo{ErrorCode: "QUOTA_EXCEEDED", ErrorMessage: quotaError},
			wantMessage:         quotaMessage,
			wantReason:          "QuotaExceeded",
		},

		{
			name:                "details appended when experiment is enabled",
			includeErrorDetails: true,
			errorInfo:           cloudprovider.InstanceErrorInfo{ErrorCode: "QUOTA_EXCEEDED", ErrorMessage: quotaError},
			wantMessage:         quotaMessage + " Cloud provider error: " + quotaError,
			wantReason:          "QuotaExceeded",
		},
		{
			name:                "no suffix when the cloud provider reported no message",
			includeErrorDetails: true,
			errorInfo:           cloudprovider.InstanceErrorInfo{ErrorCode: "QUOTA_EXCEEDED"},
			wantMessage:         quotaMessage,
			wantReason:          "QuotaExceeded",
		},
		{
			name:                "whitespace only message is treated as empty",
			includeErrorDetails: true,
			errorInfo:           cloudprovider.InstanceErrorInfo{ErrorCode: "QUOTA_EXCEEDED", ErrorMessage: "   \n"},
			wantMessage:         quotaMessage,
			wantReason:          "QuotaExceeded",
		},
		{
			name:                "long message is truncated",
			includeErrorDetails: true,
			errorInfo:           cloudprovider.InstanceErrorInfo{ErrorCode: "QUOTA_EXCEEDED", ErrorMessage: strings.Repeat("x", maxErrorDetailLength+50)},
			wantMessage:         quotaMessage + " Cloud provider error: " + strings.Repeat("x", maxErrorDetailLength) + "...",
			wantReason:          "QuotaExceeded",
		},
		{
			name:                "details omitted for unknown error when feature is disabled",
			includeErrorDetails: false,
			errorInfo:           cloudprovider.InstanceErrorInfo{ErrorCode: "UNKNOWN_ERROR", ErrorMessage: unknownError},
			wantMessage:         unknownMessage,
			wantReason:          "InternalError",
		},
		{
			name:                "details appended for unknown error when feature is enabled",
			includeErrorDetails: true,
			errorInfo:           cloudprovider.InstanceErrorInfo{ErrorCode: "UNKNOWN_ERROR", ErrorMessage: unknownError},
			wantMessage:         unknownMessage + " Cloud provider error: " + unknownError,
			wantReason:          "InternalError",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updates := make(chan npc_status.UpdateMessage, 10)

			testCrd := crd.NewTestCrd(crd.WithLabel(testLabel),
				crd.WithName(crdName),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"pool-1"})),
				}))

			mockL := npc_lister.NewMockCrdListerWithLabel([]crd.CRD{testCrd}, testLabel)
			mockP := &gke.GkeCloudProviderMock{}
			mockP.On("IsAutopilotEnabled").Return(false).Maybe()

			informer := NewCrdBackoffObserver(updates, mockL, mockP, tc.includeErrorDetails)
			informer.now = func() time.Time { return now }

			informer.OnNpcBackoff(testCrd, 0, tc.errorInfo, until)

			select {
			case msg := <-updates:
				testStatus := crd.NewMockCRDStatus(nil)
				msg.Mutate(testStatus)

				assert.Len(t, testStatus.Conditions, 1)
				assert.Equal(t, tc.wantMessage, testStatus.Conditions[0].Message)
				// The reason must stay a short token regardless of the error text.
				assert.Equal(t, tc.wantReason, testStatus.Conditions[0].Reason)
			case <-time.After(1 * time.Second):
				t.Fatalf("Expected update message but channel was empty")
			}
		})
	}
}

func TestTruncateErrorDetail(t *testing.T) {
	testCases := []struct {
		name   string
		detail string
		want   string
	}{
		{
			name:   "short message is returned unchanged",
			detail: "boom",
			want:   "boom",
		},
		{
			name:   "message at the limit is returned unchanged",
			detail: strings.Repeat("x", maxErrorDetailLength),
			want:   strings.Repeat("x", maxErrorDetailLength),
		},
		{
			name:   "longer message is truncated",
			detail: strings.Repeat("x", maxErrorDetailLength+1),
			want:   strings.Repeat("x", maxErrorDetailLength) + "...",
		},
		{
			// "ą" is two bytes, so a naive byte slice at the limit would split it and
			// produce invalid UTF-8, which the apiserver rejects.
			name:   "multi byte character is not split",
			detail: strings.Repeat("x", maxErrorDetailLength-1) + "ąą",
			want:   strings.Repeat("x", maxErrorDetailLength-1) + "...",
		},
		{
			name:   "invalid utf-8 is replaced",
			detail: "boom\xff boom",
			want:   "boom\uFFFD boom",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateErrorDetail(tc.detail)
			assert.Equal(t, tc.want, got)
			assert.True(t, utf8.ValidString(got), "truncated message must remain valid UTF-8")
		})
	}
}
