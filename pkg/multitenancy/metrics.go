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
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	multitenancySubsystem = "multitenancy"

	// tenant UID label represent the unique identifier for each tenant
	// for supervisors this is the cluster_hash. Required for all metrics
	// in this file.
	tenantUIDLabel = "tenant_uid"
)

var (
	// A custom prometheus registry to export multitenancy metrics on
	// a separate endpoint so that prom-to-sd can process tenant_uid
	// labels correctly to solve for cardinality of tenant uid.
	// Separate registry keeps the other metrics isolated from the MT related
	// configurations of prom-to-sd
	Registry = prometheus.NewRegistry()

	tenantScaleToZeroCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: multitenancySubsystem,
			Name:      "scale_to_zero_int",
			Help:      "Whether a tenant has been scaled to zero or not",
		}, []string{tenantUIDLabel},
	)
)

func init() {
	Registry.MustRegister(tenantScaleToZeroCounter)
}

func MetricsRegistryHandler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}

func ObserveTenantScaleToZero(tenantUID string, scaledToZero bool) {
	if scaledToZero {
		tenantScaleToZeroCounter.WithLabelValues(tenantUID).Set(1)
	} else {
		tenantScaleToZeroCounter.WithLabelValues(tenantUID).Set(0)
	}
}
