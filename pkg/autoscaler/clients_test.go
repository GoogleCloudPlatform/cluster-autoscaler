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

package autoscaler

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
)

func TestOverrideInformer(t *testing.T) {
	testCases := []struct {
		name                  string
		resource              string
		verb                  string
		options               internalopts.AutoscalingOptions
		expectedLabelSelector string
		expectedFieldSelector string
	}{
		{
			name:     "pod list calls filter by label and field selectors",
			resource: "pods",
			verb:     "list",
			options: internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					PodWatchLabelSelector: "ca-ignore!=true",
					PodWatchFieldSelector: "spec.schedulerName!=ignored1,spec.schedulerName!=ignored2",
				},
			},
			expectedLabelSelector: "ca-ignore!=true",
			expectedFieldSelector: "spec.schedulerName!=ignored1,spec.schedulerName!=ignored2",
		},
		{
			name:     "pod watch calls filter by label and field selectors",
			resource: "pods",
			verb:     "watch",
			options: internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					PodWatchLabelSelector: "ca-ignore!=true",
					PodWatchFieldSelector: "spec.schedulerName!=ignored1,spec.schedulerName!=ignored2",
				},
			},
			expectedLabelSelector: "ca-ignore!=true",
			expectedFieldSelector: "spec.schedulerName!=ignored1,spec.schedulerName!=ignored2",
		},
		{
			name:     "node list calls filter by label and field selectors",
			resource: "nodes",
			verb:     "list",
			options: internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					NodeWatchLabelSelector: "type!=unsupported",
					NodeWatchFieldSelector: "spec.unschedulable=false",
				},
			},
			expectedLabelSelector: "type!=unsupported",
			expectedFieldSelector: "spec.unschedulable=false",
		},
		{
			name:     "node watch calls filter by label and field selectors",
			resource: "nodes",
			verb:     "watch",
			options: internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					NodeWatchLabelSelector: "type!=unsupported",
					NodeWatchFieldSelector: "spec.unschedulable=false",
				},
			},
			expectedLabelSelector: "type!=unsupported",
			expectedFieldSelector: "spec.unschedulable=false",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset()
			if tc.verb == "list" {
				fakeClient.PrependWatchReactor(tc.resource, func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
					return true, nil, errors.New("forcing watchlist fallback to list")
				})
			}

			factory := NewSharedInformerFactory(tc.options, fakeClient, 0)
			factory.StartWithContext(t.Context())

			assert.Eventually(t, func() bool {
				for _, action := range fakeClient.Actions() {
					if action.GetResource().Resource == tc.resource && action.GetVerb() == tc.verb {
						var labelSelector, fieldSelector string

						if tc.verb == "list" {
							if listAction, ok := action.(k8stesting.ListAction); ok {
								restrictions := listAction.GetListRestrictions()
								labelSelector = restrictions.Labels.String()
								fieldSelector = restrictions.Fields.String()
							}
						} else if tc.verb == "watch" {
							if watchAction, ok := action.(k8stesting.WatchAction); ok {
								restrictions := watchAction.GetWatchRestrictions()
								labelSelector = restrictions.Labels.String()
								fieldSelector = restrictions.Fields.String()
							}
						}

						t.Logf("found action %q on resource %q with label selector %q and field selector %q",
							action.GetVerb(), action.GetResource().Resource, labelSelector, fieldSelector)
						if labelSelector == tc.expectedLabelSelector && fieldSelector == tc.expectedFieldSelector {
							return true
						}
					}
				}
				return false
			}, 2*time.Second, 100*time.Millisecond, "did not find action with expected selectors")
		})
	}
}
