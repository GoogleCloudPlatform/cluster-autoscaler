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
	"fmt"
	"strconv"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/informers/internalinterfaces"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Evaluator implements interface for evaluating experiment flags.
// There should only be one created and used for the lifetime of GCW.
type Evaluator interface {
	EvaluateStringFlagOrFailsafe(flag, fallback string) string
	EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool
	DirectLaunchBoolFlag(flag string) bool
	SubscribeToUpdate(s Subscriber)
	UpdateReleaseChannel(releaseChannel string)
}

type Subscriber func()

// noopEvaluator is used when dedicated experiment evaluation is disabled.
// Always returns failsafe/fallback values for checks.
type noopEvaluator struct{}

func (n *noopEvaluator) EvaluateStringFlagOrFailsafe(_, fallback string) string {
	return fallback
}

func (n *noopEvaluator) EvaluateBoolFlagOrFailsafe(_ string, failsafe bool) bool {
	return failsafe
}

func (n *noopEvaluator) DirectLaunchBoolFlag(_ string) bool {
	return true
}

func (n *noopEvaluator) SubscribeToUpdate(subscriber Subscriber) {}

func (n *noopEvaluator) UpdateReleaseChannel(_ string) {}

// NewNoopEvaluator returns an instance of noopEvaluator.
func NewNoopEvaluator() *noopEvaluator {
	return &noopEvaluator{}
}

// ConfigMapEvaluator implements Evaluator by reading experiment flags from a ConfigMap
type ConfigMapEvaluator struct {
	client        kube_client.Interface
	namespace     string
	configMapName string

	lock        sync.RWMutex
	data        map[string]string
	subLock     sync.Mutex
	subscribers []Subscriber
}

// We are not able to use direct configmap data as usually cluster autoscaler
// experiment keys use special characters which is not possible to store
// there as example keys contain :: separators
//
// This key is used to store a JSON blob within a configmap to allow special
// characters and remove configmap serialization limitations apart of 1MB natural
// data limit
const experimentsJSONKey = "experiments.json"

// experimentsFromConfigMap extracts key value pairs from configmap storing experiments
func experimentsFromConfigMap(cm *v1.ConfigMap) map[string]string {
	data := make(map[string]string)
	if cm == nil || cm.Data == nil {
		return data
	}

	rawJSON, ok := cm.Data[experimentsJSONKey]
	if !ok || rawJSON == "" {
		return data
	}

	var rawMap map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &rawMap); err != nil {
		klog.Warningf("Failed to unmarshal %s from experiments ConfigMap: %v", experimentsJSONKey, err)
		return data
	}

	for k, v := range rawMap {
		switch val := v.(type) {
		case string:
			data[k] = val
		case bool:
			data[k] = strconv.FormatBool(val)
		default:
			data[k] = fmt.Sprintf("%v", val)
		}
	}
	return data
}

// NewConfigMapEvaluator creates a new ConfigMapEvaluator from a ConfigMap object.
func NewConfigMapEvaluator(cm *v1.ConfigMap) *ConfigMapEvaluator {
	return &ConfigMapEvaluator{
		data: experimentsFromConfigMap(cm),
	}
}

// NewConfigMapEvaluatorFromClient creates a ConfigMapEvaluator for the specified ConfigMap
// and tries to populate it with initial data fetched from the configmap
func NewConfigMapEvaluatorFromClient(client kube_client.Interface, namespace, configMapName string) *ConfigMapEvaluator {
	evaluator := &ConfigMapEvaluator{
		client:        client,
		namespace:     namespace,
		configMapName: configMapName,
		data:          make(map[string]string),
	}
	if client == nil || configMapName == "" {
		return evaluator
	}

	cm, err := client.CoreV1().ConfigMaps(namespace).Get(context.Background(), configMapName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to fetch experiments ConfigMap %s/%s: %v", namespace, configMapName, err)
	} else {
		evaluator.UpdateConfigMap(cm)
	}

	return evaluator
}

// Start listens to ConfigMap updates using an informer and blocks until ctx is expired.
func (c *ConfigMapEvaluator) Start(ctx context.Context) error {
	if c.client == nil || c.configMapName == "" {
		klog.Errorf("Unable to pull updates from experiments ConfigMap: source is unavailable")
		<-ctx.Done()
		return nil
	}

	informer := corev1informers.NewTypedConfigMapInformerWithOptions(
		c.client,
		c.namespace,
		internalinterfaces.InformerOptions{
			TweakListOptions: func(opts *metav1.ListOptions) {
				opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", c.configMapName).String()
			},
		},
	)

	informer.AddTypedEventHandler(cache.TypedResourceEventHandlerFuncs[*v1.ConfigMap]{
		AddFunc: func(cm *v1.ConfigMap) {
			c.UpdateConfigMap(cm)
		},
		UpdateFunc: func(oldCm, newCm *v1.ConfigMap) {
			c.UpdateConfigMap(newCm)
		},
		DeleteFunc: func(obj cache.DeletedObject[*v1.ConfigMap]) {
			c.UpdateConfigMap(nil)
		},
	})

	informer.Run(ctx.Done())
	return nil
}

// EvaluateStringFlagOrFailsafe evaluates string flag value from the ConfigMap data.
func (c *ConfigMapEvaluator) EvaluateStringFlagOrFailsafe(flag, fallback string) string {
	if c == nil {
		return fallback
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	if val, ok := c.data[flag]; ok {
		return val
	}
	return fallback
}

// EvaluateBoolFlagOrFailsafe evaluates bool flag value from the ConfigMap data.
func (c *ConfigMapEvaluator) EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool {
	if c == nil {
		return failsafe
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	if val, ok := c.data[flag]; ok {
		parsed, err := strconv.ParseBool(val)
		if err == nil {
			return parsed
		}
		klog.Warningf("Failed to parse bool value %q for flag %q: %v, using failsafe: %v", val, flag, err, failsafe)
	}
	return failsafe
}

// DirectLaunchBoolFlag evaluates direct launch bool flag value.
// If the flag is explicitly set to false, it returns false; otherwise it defaults to true.
func (c *ConfigMapEvaluator) DirectLaunchBoolFlag(flag string) bool {
	if c == nil {
		return true
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	if val, ok := c.data[flag]; ok {
		parsed, err := strconv.ParseBool(val)
		if err == nil && !parsed {
			return false
		}
	}
	return true
}

// SubscribeToUpdate registers a subscriber callback to be notified when the ConfigMap is updated.
func (c *ConfigMapEvaluator) SubscribeToUpdate(subscriber Subscriber) {
	if c == nil {
		return
	}
	c.subLock.Lock()
	defer c.subLock.Unlock()
	c.subscribers = append(c.subscribers, subscriber)
}

// UpdateReleaseChannel no-op method to satisfy Evaluator interface.
func (c *ConfigMapEvaluator) UpdateReleaseChannel(_ string) {}

// UpdateConfigMap updates the evaluator's data from a new ConfigMap and notifies subscribers.
func (c *ConfigMapEvaluator) UpdateConfigMap(cm *v1.ConfigMap) {
	if c == nil {
		return
	}

	data := experimentsFromConfigMap(cm)
	c.lock.Lock()
	c.data = data
	c.lock.Unlock()

	c.notifySubscribers()
}

func (c *ConfigMapEvaluator) notifySubscribers() {
	c.subLock.Lock()
	subs := make([]Subscriber, len(c.subscribers))
	copy(subs, c.subscribers)
	c.subLock.Unlock()

	for _, sub := range subs {
		go sub()
	}
}
