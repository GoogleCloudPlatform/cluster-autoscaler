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

package experiments

import (
	"context"
	"encoding/json"
	"testing"
	"testing/synctest"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func marshalExps(data map[string]string) string {
	rawJSON, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return string(rawJSON)
}

func TestConfigMapEvaluatorEvaluateStringFlagOrFailsafe(t *testing.T) {
	tests := map[string]struct {
		evaluator *ConfigMapEvaluator
		flag      string
		fallback  string
		want      string
	}{
		"FlagPresentInJSON": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"StringFlag": "custom-value",
						"AutopilotSliceOfHardware::ReservationSteerLocalSSD": "enabled",
					}),
				},
			}),
			flag:     "AutopilotSliceOfHardware::ReservationSteerLocalSSD",
			fallback: "fallback-value",
			want:     "enabled",
		},
		"FlagMissingFromJSON": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"OtherFlag": "other-value",
					}),
				},
			}),
			flag:     "MissingFlag",
			fallback: "fallback-value",
			want:     "fallback-value",
		},
		"EmptyJSON": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{}),
				},
			}),
			flag:     "StringFlag",
			fallback: "fallback-value",
			want:     "fallback-value",
		},
		"InvalidJSON": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": `invalid-json`,
				},
			}),
			flag:     "StringFlag",
			fallback: "fallback-value",
			want:     "fallback-value",
		},
		"MissingExperimentsJSONKey": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{},
			}),
			flag:     "StringFlag",
			fallback: "fallback-value",
			want:     "fallback-value",
		},
		"NilConfigMap": {
			evaluator: NewConfigMapEvaluator(nil),
			flag:      "StringFlag",
			fallback:  "fallback-value",
			want:      "fallback-value",
		},
		"NilEvaluator": {
			evaluator: nil,
			flag:      "StringFlag",
			fallback:  "fallback-value",
			want:      "fallback-value",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := tc.evaluator.EvaluateStringFlagOrFailsafe(tc.flag, tc.fallback)
			if got != tc.want {
				t.Errorf("EvaluateStringFlagOrFailsafe(%q, %q) = %q, want %q", tc.flag, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestConfigMapEvaluatorEvaluateBoolFlagOrFailsafe(t *testing.T) {
	tests := map[string]struct {
		evaluator *ConfigMapEvaluator
		flag      string
		failsafe  bool
		want      bool
	}{
		"BoolStringTrue": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"AutopilotSliceOfHardware::ReservationSteerLocalSSD": "true",
					}),
				},
			}),
			flag:     "AutopilotSliceOfHardware::ReservationSteerLocalSSD",
			failsafe: false,
			want:     true,
		},
		"BoolStringFalse": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"BoolFlag": "false",
					}),
				},
			}),
			flag:     "BoolFlag",
			failsafe: true,
			want:     false,
		},
		"FlagInvalidParsable": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"BoolFlag": "invalid-bool",
					}),
				},
			}),
			flag:     "BoolFlag",
			failsafe: true,
			want:     true,
		},
		"InvalidJSON": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": `invalid-json`,
				},
			}),
			flag:     "BoolFlag",
			failsafe: true,
			want:     true,
		},
		"MissingExperimentsJSONKey": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{},
			}),
			flag:     "BoolFlag",
			failsafe: true,
			want:     true,
		},
		"FlagMissing": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{}),
				},
			}),
			flag:     "MissingFlag",
			failsafe: true,
			want:     true,
		},
		"NilConfigMap": {
			evaluator: NewConfigMapEvaluator(nil),
			flag:      "BoolFlag",
			failsafe:  false,
			want:      false,
		},
		"NilEvaluator": {
			evaluator: nil,
			flag:      "BoolFlag",
			failsafe:  false,
			want:      false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := tc.evaluator.EvaluateBoolFlagOrFailsafe(tc.flag, tc.failsafe)
			if got != tc.want {
				t.Errorf("EvaluateBoolFlagOrFailsafe(%q, %v) = %v, want %v", tc.flag, tc.failsafe, got, tc.want)
			}
		})
	}
}

func TestConfigMapEvaluatorDirectLaunchBoolFlag(t *testing.T) {
	tests := map[string]struct {
		evaluator *ConfigMapEvaluator
		flag      string
		want      bool
	}{
		"FlagExplicitlyFalseString": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"LaunchFlag": "false",
					}),
				},
			}),
			flag: "LaunchFlag",
			want: false,
		},
		"FlagExplicitlyTrue": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"LaunchFlag": "true",
					}),
				},
			}),
			flag: "LaunchFlag",
			want: true,
		},
		"InvalidJSONDefaultsToTrue": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": `invalid-json`,
				},
			}),
			flag: "LaunchFlag",
			want: true,
		},
		"MissingExperimentsJSONKeyDefaultsToTrue": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{},
			}),
			flag: "LaunchFlag",
			want: true,
		},
		"FlagMissingDefaultsToTrue": {
			evaluator: NewConfigMapEvaluator(&v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{}),
				},
			}),
			flag: "MissingFlag",
			want: true,
		},
		"NilConfigMapDefaultsToTrue": {
			evaluator: NewConfigMapEvaluator(nil),
			flag:      "LaunchFlag",
			want:      true,
		},
		"NilEvaluatorDefaultsToTrue": {
			evaluator: nil,
			flag:      "LaunchFlag",
			want:      true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := tc.evaluator.DirectLaunchBoolFlag(tc.flag)
			if got != tc.want {
				t.Errorf("DirectLaunchBoolFlag(%q) = %v, want %v", tc.flag, got, tc.want)
			}
		})
	}
}

func TestConfigMapEvaluatorFromClient(t *testing.T) {
	tests := map[string]struct {
		configMap     *v1.ConfigMap
		namespace     string
		configMapName string
		flagToTest    string
		wantValue     string
	}{
		"ConfigMapExists": {
			configMap: &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cm",
					Namespace: "kube-system",
				},
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"TestFlag": "found-value",
						"AutopilotSliceOfHardware::ReservationSteerLocalSSD": "true",
					}),
				},
			},
			namespace:     "kube-system",
			configMapName: "test-cm",
			flagToTest:    "AutopilotSliceOfHardware::ReservationSteerLocalSSD",
			wantValue:     "true",
		},
		"ConfigMapNotFound": {
			configMap:     nil,
			namespace:     "kube-system",
			configMapName: "non-existent-cm",
			flagToTest:    "TestFlag",
			wantValue:     "fallback-value",
		},
		"EmptyConfigMapName": {
			configMap:     nil,
			namespace:     "kube-system",
			configMapName: "",
			flagToTest:    "TestFlag",
			wantValue:     "fallback-value",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var fakeClient *fake.Clientset
			if tc.configMap != nil {
				fakeClient = fake.NewSimpleClientset(tc.configMap)
			} else {
				fakeClient = fake.NewSimpleClientset()
			}

			evaluator := NewConfigMapEvaluatorFromClient(fakeClient, tc.namespace, tc.configMapName)
			got := evaluator.EvaluateStringFlagOrFailsafe(tc.flagToTest, "fallback-value")
			if got != tc.wantValue {
				t.Errorf("EvaluateStringFlagOrFailsafe(%q) = %q, want %q", tc.flagToTest, got, tc.wantValue)
			}
		})
	}
}

func TestConfigMapEvaluatorUpdateAndSubscribe(t *testing.T) {
	tests := map[string]struct {
		initialConfigMap *v1.ConfigMap
		updatedConfigMap *v1.ConfigMap
		flag             string
		wantInitial      string
		wantUpdated      string
	}{
		"UpdateChangesFlagValueAndNotifiesSubscriber": {
			initialConfigMap: &v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"FeatureFlag": "initial-value",
					}),
				},
			},
			updatedConfigMap: &v1.ConfigMap{
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"FeatureFlag": "updated-value",
					}),
				},
			},
			flag:        "FeatureFlag",
			wantInitial: "initial-value",
			wantUpdated: "updated-value",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				evaluator := NewConfigMapEvaluator(tc.initialConfigMap)
				if got := evaluator.EvaluateStringFlagOrFailsafe(tc.flag, "fallback"); got != tc.wantInitial {
					t.Errorf("initial EvaluateStringFlagOrFailsafe(%q) = %q, want %q", tc.flag, got, tc.wantInitial)
				}

				var notified bool
				evaluator.SubscribeToUpdate(func() {
					notified = true
				})

				evaluator.UpdateConfigMap(tc.updatedConfigMap)
				synctest.Wait()

				if got := evaluator.EvaluateStringFlagOrFailsafe(tc.flag, "fallback"); got != tc.wantUpdated {
					t.Errorf("updated EvaluateStringFlagOrFailsafe(%q) = %q, want %q", tc.flag, got, tc.wantUpdated)
				}
				if !notified {
					t.Errorf("subscriber was not notified")
				}
			})
		})
	}
}

func TestConfigMapEvaluatorRealtimeInformer(t *testing.T) {
	tests := map[string]struct {
		initialConfigMap *v1.ConfigMap
		updatedConfigMap *v1.ConfigMap
		flag             string
		wantInitial      string
		wantUpdated      string
	}{
		"RealtimeInformerReceivesUpdates": {
			initialConfigMap: &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-experiments-cm",
					Namespace: "kube-system",
				},
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"TestRealtimeFlag": "initial-value",
					}),
				},
			},
			updatedConfigMap: &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-experiments-cm",
					Namespace: "kube-system",
				},
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"TestRealtimeFlag": "updated-realtime-value",
					}),
				},
			},
			flag:        "TestRealtimeFlag",
			wantInitial: "initial-value",
			wantUpdated: "updated-realtime-value",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				fakeClient := fake.NewSimpleClientset(tc.initialConfigMap)
				evaluator := NewConfigMapEvaluatorFromClient(fakeClient, tc.initialConfigMap.Namespace, tc.initialConfigMap.Name)
				go evaluator.Start(ctx)

				synctest.Wait()

				if got := evaluator.EvaluateStringFlagOrFailsafe(tc.flag, "fallback"); got != tc.wantInitial {
					t.Errorf("initial EvaluateStringFlagOrFailsafe(%q) = %q, want %q", tc.flag, got, tc.wantInitial)
				}

				var notified bool
				evaluator.SubscribeToUpdate(func() {
					notified = true
				})

				_, err := fakeClient.CoreV1().ConfigMaps(tc.updatedConfigMap.Namespace).Update(ctx, tc.updatedConfigMap, metav1.UpdateOptions{})
				if err != nil {
					t.Fatalf("failed to update configmap in fake client: %v", err)
				}

				synctest.Wait()

				if got := evaluator.EvaluateStringFlagOrFailsafe(tc.flag, "fallback"); got != tc.wantUpdated {
					t.Errorf("updated EvaluateStringFlagOrFailsafe(%q) = %q, want %q", tc.flag, got, tc.wantUpdated)
				}
				if !notified {
					t.Errorf("subscriber was not notified")
				}
			})
		})
	}
}

func TestNoopEvaluator(t *testing.T) {
	evaluator := NewNoopEvaluator()

	tests := map[string]struct {
		evalFunc func() bool
	}{
		"EvaluateStringReturnsFallback": {
			evalFunc: func() bool {
				return evaluator.EvaluateStringFlagOrFailsafe("AnyFlag", "fallback") == "fallback"
			},
		},
		"EvaluateBoolReturnsFailsafe": {
			evalFunc: func() bool {
				return evaluator.EvaluateBoolFlagOrFailsafe("AnyFlag", true) == true &&
					evaluator.EvaluateBoolFlagOrFailsafe("AnyFlag", false) == false
			},
		},
		"DirectLaunchReturnsTrue": {
			evalFunc: func() bool {
				return evaluator.DirectLaunchBoolFlag("AnyFlag") == true
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if !tc.evalFunc() {
				t.Errorf("unexpected evaluation result for noopEvaluator")
			}
		})
	}
}

func TestConfigMapEvaluatorStart(t *testing.T) {
	tests := map[string]struct {
		configMap *v1.ConfigMap
	}{
		"StartBlocksUntilContextDone": {
			configMap: &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cm",
					Namespace: "kube-system",
				},
				Data: map[string]string{
					"experiments.json": marshalExps(map[string]string{
						"TestFlag": "value",
					}),
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				fakeClient := fake.NewSimpleClientset(tc.configMap)
				evaluator := NewConfigMapEvaluatorFromClient(fakeClient, tc.configMap.Namespace, tc.configMap.Name)

				done := make(chan error, 1)
				go func() {
					done <- evaluator.Start(ctx)
				}()

				synctest.Wait()
				cancel()
				synctest.Wait()

				select {
				case err := <-done:
					if err != nil {
						t.Errorf("Start() returned unexpected error: %v", err)
					}
				default:
					t.Errorf("Start() did not return after context cancellation")
				}
			})
		})
	}
}
