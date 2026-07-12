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

package gceclient

import (
	"net/http"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
)

const (
	defaultOperationWaitTimeout  = 25 * time.Minute
	defaultOperationPollInterval = 100 * time.Millisecond
)

type GceConfig struct {
	ProjectId              string
	ProjectNumber          int64
	MultitenancyEnabled    bool
	HttpClient             *http.Client
	Cache                  MigInfoProvider
	ClusterName            string
	UserAgent              string
	ExperimentsManager     experiments.Manager
	Endpoint               string
	ProviderConfigObserver multitenancy.ProviderConfigObserver
	Region                 string
}

// CreateClient initializes http.Client and the GCE client.
func CreateClient(cfg GceConfig) (AutoscalingInternalGceClient, error) {
	httpClient := cfg.HttpClient
	if cfg.Region != "" && httpClient != nil {
		httpClientWithQuotaOverride := *httpClient
		httpClientWithQuotaOverride.Transport = &globalGceApiHeaderRoundTripper{
			delegate: httpClientWithQuotaOverride.Transport,
			region:   cfg.Region,
		}
		httpClient = &httpClientWithQuotaOverride
	}

	if cfg.MultitenancyEnabled {
		multitenantGCEClient, err := NewMultitenancyGCEClient(httpClient, cfg.Cache, cfg.ProjectId, cfg.ProjectNumber, cfg.ClusterName, cfg.UserAgent, defaultOperationWaitTimeout, defaultOperationPollInterval, cfg.ExperimentsManager)
		if err != nil {
			return nil, err
		}
		err = cfg.ProviderConfigObserver.RegisterEventHandlers("MTGCEClient", multitenantGCEClient.AddProviderConfig, multitenantGCEClient.DeleteProviderConfig)
		if err != nil {
			return nil, err
		}
		return multitenantGCEClient, nil
	}

	var client *autoscalingInternalGceClient
	var err error
	if cfg.Endpoint != "" {
		client, err = NewCustomAutoscalingInternalGceClient(httpClient, cfg.Cache, cfg.ProjectId, cfg.ClusterName, cfg.Endpoint, cfg.UserAgent, defaultOperationWaitTimeout, defaultOperationPollInterval, cfg.ExperimentsManager)
	} else {
		client, err = NewAutoscalingInternalGceClient(httpClient, cfg.Cache, cfg.ProjectId, cfg.ClusterName, cfg.UserAgent, defaultOperationWaitTimeout, defaultOperationPollInterval, cfg.ExperimentsManager)
	}

	if err != nil {
		return nil, err
	}
	return client, nil
}
