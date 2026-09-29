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

package cfg

import (
	"encoding/json"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// Provider is responsible for providing the CSN Node Controller configuration.
type Provider struct {
	experimentsManager experiments.Manager
}

// NewProvider creates a new Provider using the provided experiments.Manager.
func NewProvider(experimentsManager experiments.Manager) *Provider {
	return &Provider{
		experimentsManager: experimentsManager,
	}
}

// GetConfig retrieves and parses the Controller from the experiments manager flag.
func (c *Provider) GetConfig() Controller {
	val := c.experimentsManager.EvaluateStringFlagOrFailsafe(experiments.ColdStandbyNodesControllerConfigV1Flag, "")
	if val == "" {
		defaultCfg := getDefaultConfig()
		klog.V(4).Infof("CSN Node Controller: config not found in experiments manager, default config applied: %v", defaultCfg)
		return defaultCfg
	}

	// Initialize with defaults first so that any omitted fields in the experiment
	// config JSON will retain their fallback values after unmarshalling.
	cfg := getDefaultConfig()
	if err := json.Unmarshal([]byte(val), &cfg); err != nil {
		defaultCfg := getDefaultConfig()
		klog.Errorf("CSN Node Controller: default config %v will be applied because unmarshalling experiment config failed: %v", defaultCfg, err)
		return defaultCfg
	}
	klog.V(4).Infof("CSN Node Controller: found experiment config: %v", val)
	return cfg
}

// getDefaultConfig provides a sensible config that will be used
// when the config cannot be found via experiments.
func getDefaultConfig() Controller {
	return Controller{
		WorkQueue: WorkQueue{
			MaxSize: 1000,
		},
		Dispatcher: Dispatcher{
			WorkerCount: 100,
			Retry: Retry{
				MaxRetries: 6,
				MaxRetriesByOp: map[string]int{
					"CONSUME": 6,
					"SUSPEND": 6,
				},
				InitialDelay: metav1.Duration{Duration: 5 * time.Second},
				MaxDelay:     metav1.Duration{Duration: 5 * time.Minute},
			},
		},
		Suspend: Suspend{
			MinNodeLifetime: metav1.Duration{Duration: 5 * time.Minute},
			PreSuspendDelay: metav1.Duration{Duration: 1 * time.Second},
		},
		Reconciliation: Reconciliation{
			Interval:        metav1.Duration{Duration: 15 * time.Second},
			MaxInvalidCount: 3,
		},
		StateManager: StateManager{
			StopTrackingDelay:   metav1.Duration{Duration: 10 * time.Minute},
			MetricsSyncInterval: metav1.Duration{Duration: 5 * time.Second},
		},
	}
}
