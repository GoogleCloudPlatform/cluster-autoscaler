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

package nodeinfosprovider

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

const (
	logFrequency = time.Hour
)

type metricsCloudProvider interface {
	GetAllZones() ([]string, error)
	GetGkeMigs() []*gke.GkeMig
}

type distributionConfig struct {
	machineType    string
	locationPolicy string
}

type distribution map[string]int64

func (config distributionConfig) labels(zone string) map[string]string {
	return map[string]string{
		"zone":            zone,
		"machine_type":    config.machineType,
		"location_policy": config.locationPolicy,
	}
}

func (config distributionConfig) logLine(zone string) string {
	return fmt.Sprintf("%s_%s_%s_cores", config.machineType, zone, config.locationPolicy)
}

type coreDistributionMetrics struct {
	observer         coreDistributionObserver
	coreDistribution map[distributionConfig]distribution

	lastLogTime time.Time
}

func newCoreDistributionMetrics() *coreDistributionMetrics {
	return &coreDistributionMetrics{
		observer:    &metricsCoreDistributionObserver{},
		lastLogTime: time.Now().Add(-logFrequency),
	}
}

func (m *coreDistributionMetrics) UpdateMetrics(ctx *context.AutoscalingContext, nodeInfos map[string]*framework.NodeInfo) {
	m.updateCoreDistribution(ctx, nodeInfos)
	m.logCoreDistribution()
}

func (m *coreDistributionMetrics) updateCoreDistribution(ctx *context.AutoscalingContext, nodeInfos map[string]*framework.NodeInfo) {
	provider, ok := ctx.CloudProvider.(metricsCloudProvider)
	if !ok {
		klog.Errorf("skipping update, could not cast the CloudProvider to the metricsCloudProvider")
		return
	}

	allZones, err := provider.GetAllZones()
	if err != nil {
		klog.Errorf("skipping update, cannot get target size of mig: %v", err)
		return
	}

	newCoreDistribution := make(map[distributionConfig]distribution)
	for _, mig := range provider.GetGkeMigs() {
		targetSize, err := mig.TargetSize()
		if err != nil {
			klog.Errorf("skipping update, cannot get target size of mig: %v", err)
			return
		}
		nodeInfo, found := nodeInfos[mig.Id()]
		if !found || nodeInfo.Node() == nil || nodeInfo.Node().Status.Capacity.Cpu() == nil {
			klog.Errorf("skipping update, cannot retrieve core capacity for mig: %s", mig.Id())
			return
		}

		config := distributionConfig{
			machineType:    mig.MachineType(),
			locationPolicy: string(mig.LocationPolicy()),
		}
		if _, found := newCoreDistribution[config]; !found {
			newCoreDistribution[config] = make(distribution)
			for _, zone := range allZones {
				newCoreDistribution[config][zone] = 0
			}
		}
		newCoreDistribution[config][mig.GceRef().Zone] += int64(targetSize) * nodeInfo.Node().Status.Capacity.Cpu().Value()
	}

	for config, dist := range m.coreDistribution {
		if _, found := newCoreDistribution[config]; !found {
			for zone := range dist {
				m.observer.UnregisterNodeZonalDistribution(config.labels(zone))
			}
		}
	}
	for config, dist := range newCoreDistribution {
		for zone, count := range dist {
			m.observer.UpdateNodeZonalDistribution(config.labels(zone), count)
		}
	}

	m.coreDistribution = newCoreDistribution
}

func (m *coreDistributionMetrics) logCoreDistribution() {
	if time.Since(m.lastLogTime) > logFrequency {
		for config, dist := range m.coreDistribution {
			var logLines []string
			for zone, count := range dist {
				logLines = append(logLines, fmt.Sprintf("%s=%v", config.logLine(zone), count))
			}
			klog.V(2).Info(strings.Join(logLines, " "))
		}
		m.lastLogTime = time.Now()
	}
}

type coreDistributionObserver interface {
	UpdateNodeZonalDistribution(map[string]string, int64)
	UnregisterNodeZonalDistribution(map[string]string)
}

type metricsCoreDistributionObserver struct{}

func (o *metricsCoreDistributionObserver) UpdateNodeZonalDistribution(labels map[string]string, count int64) {
	metrics.Metrics.UpdateCoreZonalDistribution(labels, count)
}

func (o *metricsCoreDistributionObserver) UnregisterNodeZonalDistribution(labels map[string]string) {
	metrics.Metrics.UnregisterCoreZonalDistribution(labels)
}
