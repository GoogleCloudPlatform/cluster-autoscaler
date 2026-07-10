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

package multitenancy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// ProviderConfig represents GCP specific configuration in a
// GKE MT cluster for a tenant. More info at go/gke-mt-resource-model-design.
type ProviderConfig struct {
	Name          string
	ProjectNumber int64
	ProjectID     string
	NetworkConfig *ProviderNetworkConfig
	AuthConfig    *AuthConfig
}

// ProviderNetworkConfig defines the network configuration.
type ProviderNetworkConfig struct {
	// Network is the name of the VPC.
	Network string
	// Subnetwork is the name of the subnetwork.
	Subnetwork string
	// The name of the secondary range.
	PodRange string
}

// AuthConfig defines the necessary information for a controller to acquire an authentication token.
// This structure is analogous to the token-url and token-body configuration in gce.conf.
type AuthConfig struct {
	// TokenURL is the full URL endpoint for generating an authentication token.
	// +kubebuilder:validation:MinLength=1
	TokenURL string `json:"tokenURL"`

	// TokenBody is the JSON body required for the token generation POST request.
	// +kubebuilder:validation:MinLength=1
	TokenBody string `json:"tokenBody"`
}

const (
	ProviderConfigLabel = "tenancy.gke.io/provider-config"
	VPCLabel            = "tenancy.gke.io/network"
	SubnetLabel         = "tenancy.gke.io/subnet"
)

// ProviderConfigEventHandler is an interface that can be implemented by
// consumers that need to act on ProviderConfig CRUD in a GKE MT cluster.
type ProviderConfigEventHandler interface {
	AddProviderConfig(providerConfig *ProviderConfig) error
	DeleteProviderConfig(providerConfig *ProviderConfig) error
}

type ProviderConfigObserver interface {
	RegisterEventHandlers(name string, addEventFunc func(*ProviderConfig) error, deleteEventFunc func(*ProviderConfig) error) error
}

const (
	resyncDuration = 5 * time.Minute
)

var (
	providerConfigsGVR = schema.GroupVersionResource{
		Group:    "cloud.gke.io",
		Version:  "v1",
		Resource: "providerconfigs",
	}
)

type providerConfigCRInformer struct {
	informer cache.SharedIndexInformer
}

func NewProviderConfigInformer(dynamicClientSet dynamic.Interface) *providerConfigCRInformer {
	providerConfigInformer := &providerConfigCRInformer{
		informer: dynamicinformer.NewDynamicSharedInformerFactory(dynamicClientSet, resyncDuration).ForResource(providerConfigsGVR).Informer(),
	}
	return providerConfigInformer
}

// RegisterEventHandlers is used to register add and delete event handling functions for ProviderConfig.
// For example, if a client if MT aware it can register functions here and recieve updates as MT cluster state changes.
func (p *providerConfigCRInformer) RegisterEventHandlers(name string, addEventFunc func(*ProviderConfig) error, deleteEventFunc func(*ProviderConfig) error) error {
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pc, err := toUnstructured(obj)
			if err != nil {
				klog.Error(err.Error())
				return
			}
			providerConfig, err := providerConfigFromUnstructured(pc)
			if err != nil {
				klog.Errorf("Error generating ProviderConfig, skipping Add event for handler %s, err: %v", name, err)
				return
			}
			err = addEventFunc(providerConfig)
			if err != nil {
				klog.Errorf("Error adding ProviderConfig for handler: %s, err: %v", name, err)
				return
			}
			klog.Infof("Added ProviderConfig %s event for %s", pc.GetName(), name)
		},
		DeleteFunc: func(obj any) {
			pc, err := toUnstructured(obj)
			if err != nil {
				klog.Error(err.Error())
				return
			}
			providerConfig, err := providerConfigFromUnstructured(pc)
			if err != nil {
				klog.Errorf("Error generating ProviderConfig, skipping Delete event for handler %s, err: %v", name, err)
				return
			}
			err = deleteEventFunc(providerConfig)
			if err != nil {
				klog.Errorf("Error deleting ProviderConfig for handler: %s, err: %v", name, err)
				return
			}
			klog.Infof("Deleted ProviderConfig %s event for %s", pc.GetName(), name)
		},
	}
	_, err := p.informer.AddEventHandlerWithResyncPeriod(handler, resyncDuration)
	return err
}

func toUnstructured(obj any) (*unstructured.Unstructured, error) {
	providerConfig, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unable to convert providerConfig object to unstructured")
	}
	return providerConfig, nil
}

// TODO(b/392637714): Use typed ProviderConfig defintion.
func providerConfigFromUnstructured(pc *unstructured.Unstructured) (*ProviderConfig, error) {
	projectID, err := stringFieldFromProviderConfig(pc, "spec", "projectID")
	if err != nil {
		return nil, err
	}
	projectNumber, err := intFieldFromProviderConfig(pc, "spec", "projectNumber")
	if err != nil {
		return nil, err
	}

	network, err := stringFieldFromProviderConfig(pc, "spec", "networkConfig", "network")
	if err != nil {
		return nil, err
	}
	subnetwork, err := stringFieldFromProviderConfig(pc, "spec", "networkConfig", "subnetInfo", "subnetwork")
	if err != nil {
		return nil, err
	}
	podRange, err := podRangeInfoFromProviderConfig(pc)
	if err != nil {
		return nil, err
	}
	networkConfig := &ProviderNetworkConfig{
		Network:    network,
		Subnetwork: subnetwork,
		PodRange:   podRange,
	}

	var authConfig *AuthConfig
	auth, found, err := unstructured.NestedMap(pc.UnstructuredContent(), "spec", "authSpec")
	if err == nil && found {
		klog.Infof("Found authSpec for ProviderConfig %v: %v", pc.GetName(), auth)
		tokenURL, found, errURL := unstructured.NestedString(auth, "tokenURL")
		if !found {
			return nil, fmt.Errorf("tokenURL not found for ProviderConfig %v", pc.GetName())
		}
		if errURL != nil {
			return nil, fmt.Errorf("invalid tokenURL for ProviderConfig %v: %v", pc.GetName(), errURL)
		}
		tokenBody, found, errBody := unstructured.NestedString(auth, "tokenBody")
		if !found {
			return nil, fmt.Errorf("tokenBody not found for ProviderConfig %v", pc.GetName())
		}
		if errBody != nil {
			return nil, fmt.Errorf("invalid tokenBody for ProviderConfig %v: %v", pc.GetName(), errBody)
		}
		if tokenURL != "" {
			klog.Infof("Successfully parsed TokenURL for ProviderConfig %v: URL=%v, Body=%v", pc.GetName(), tokenURL, tokenBody)
			authConfig = &AuthConfig{
				TokenURL:  tokenURL,
				TokenBody: tokenBody,
			}
		} else {
			klog.Infof("authSpec found but TokenURL is empty for ProviderConfig: %v", pc.GetName())
		}
	} else {
		klog.Infof("No authSpec found for ProviderConfig: %v (found=%v, err=%v)", pc.GetName(), found, err)
	}

	return &ProviderConfig{
		Name:          pc.GetName(),
		ProjectID:     projectID,
		ProjectNumber: projectNumber,
		NetworkConfig: networkConfig,
		AuthConfig:    authConfig,
	}, nil
}

func stringFieldFromProviderConfig(pc *unstructured.Unstructured, fields ...string) (string, error) {
	fieldValue, found, err := unstructured.NestedString(pc.UnstructuredContent(), fields...)
	if err != nil {
		return "", fmt.Errorf("field path %s is not a string: %w", strings.Join(fields, "."), err)
	}
	if !found {
		return "", fmt.Errorf("field path %s not found in ProviderConfig CR %s", strings.Join(fields, "."), pc.GetName())
	}
	return fieldValue, nil
}

func intFieldFromProviderConfig(pc *unstructured.Unstructured, fields ...string) (int64, error) {
	fieldValue, found, err := unstructured.NestedNumberAsFloat64(pc.UnstructuredContent(), fields...)
	if err != nil {
		return -1, fmt.Errorf("field path %s is not an int64: %w", strings.Join(fields, "."), err)
	}
	if !found {
		return -1, fmt.Errorf("field path %s not found in ProviderConfig CR %s", strings.Join(fields, "."), pc.GetName())
	}
	return int64(fieldValue), nil
}

func podRangeInfoFromProviderConfig(pc *unstructured.Unstructured) (string, error) {
	podRanges, found, err := unstructured.NestedSlice(pc.UnstructuredContent(), "spec", "networkConfig", "subnetInfo", "podRanges")
	if err != nil {
		return "", fmt.Errorf("field path %s is not a slice: %w", "spec.networkConfig.subnetInfo.podRanges", err)
	}
	if !found {
		return "", fmt.Errorf("field path %s not found in CR", "spec.networkConfig.subnetInfo.podRanges")
	}
	if len(podRanges) == 0 {
		return "", fmt.Errorf("field path %s is empty", "spec.networkConfig.subnetInfo.podRanges")
	}
	secondaryRange, ok := podRanges[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("field path %s is not a map[string]interface{}", "spec.networkConfig.subnetInfo.podRanges[0]")
	}
	secondaryRangeName, err := safeStringLookup(secondaryRange, "name")
	if err != nil {
		return "", fmt.Errorf("unable to get secondary range name from spec.networkConfig.subnetInfo.podRanges[0], err: %w", err)
	}
	return secondaryRangeName, nil
}

func safeStringLookup(m map[string]interface{}, key string) (string, error) {
	value, ok := m[key]
	if !ok {
		return "", fmt.Errorf("key %s not found in map", key)
	}
	stringValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("value for key %s is not a string", key)
	}
	return stringValue, nil
}

// Run starts the tenant CR informer.
func (t *providerConfigCRInformer) Run(ctx context.Context) {
	klog.Infof("Starting ProviderConfig CR informer.")
	go t.informer.Run(ctx.Done())
}
