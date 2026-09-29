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

package reconciler

import (
	"context"
	"fmt"
	"path"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	gke_util "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
)

// KwokGceReconciler reconciles GCE instances with K8s Nodes.
type KwokGceReconciler struct {
	kubeClient  client.Client
	gceClient   gce.AutoscalingGceClient
	gceService  *gce_api.Service
	nodeMapper  *kwokNodeMapper
	projectID   string
	clusterName string
	location    string
	interval    time.Duration
}

// NewKwokGceReconciler creates a new KwokGceReconciler instance.
func NewKwokGceReconciler(
	kubeClient client.Client,
	gceClient gce.AutoscalingGceClient,
	gceService *gce_api.Service,
	projectID string,
	clusterName string,
	location string,
	interval time.Duration,
) *KwokGceReconciler {
	return &KwokGceReconciler{
		kubeClient:  kubeClient,
		gceClient:   gceClient,
		gceService:  gceService,
		nodeMapper:  newKwokNodeMapper(gceClient, projectID),
		projectID:   projectID,
		clusterName: clusterName,
		location:    location,
		interval:    interval,
	}
}

// Start starts the periodic reconciliation loop. It blocks until the context is cancelled,
// satisfying the manager.Runnable interface.
func (r *KwokGceReconciler) Start(ctx context.Context) error {
	klog.Infof("Starting KWOK GCE Reconciler for cluster %s in project %s (location: %s, interval: %v)", r.clusterName, r.projectID, r.location, r.interval)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			klog.Infof("Stopping KWOK GCE Reconciler")
			return nil
		case <-ticker.C:
			r.Reconcile(ctx)
		}
	}
}

// Reconcile runs a single reconciliation loop.
func (r *KwokGceReconciler) Reconcile(ctx context.Context) {
	if r.kubeClient == nil {
		klog.Errorf("[KwokReconciler] Kube client is nil, skipping reconciliation")
		return
	}

	zones, err := gke_util.GetZonesFromLocation(ctx, r.location, r.gceClient)
	if err != nil {
		klog.Errorf("[KwokReconciler] Failed to fetch zones for location %s: %v", r.location, err)
		return
	}

	gceInstances := make(map[string]*gce_api.Instance)
	filter := fmt.Sprintf("labels.goog-k8s-cluster-name=%s", r.clusterName)
	for _, zone := range zones {
		// TODO: b/543357957 - For large scalability tests we should have some caching here
		instances, err := r.fetchRawInstances(ctx, zone, filter)
		if err != nil {
			klog.Errorf("[KwokReconciler] Failed to list GCE instances in zone %s: %v", zone, err)
			continue
		}
		for _, inst := range instances {
			gceInstances[inst.Name] = inst
		}
	}

	var nodeList apiv1.NodeList
	if err := r.kubeClient.List(ctx, &nodeList); err != nil {
		klog.Errorf("[KwokReconciler] Failed to list K8s nodes: %v", err)
		return
	}
	k8sNodes := make(map[string]*apiv1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		k8sNodes[node.Name] = node
	}

	for name, inst := range gceInstances {
		// Skip deleted or deleting instances
		if inst.Status == "STOPPING" || inst.Status == "TERMINATED" {
			continue
		}

		if _, exists := k8sNodes[name]; !exists {
			zone := path.Base(inst.Zone)
			klog.Infof("[KwokReconciler] Creating K8s node for GCE instance %s in zone %s", name, zone)
			node, err := r.nodeMapper.mapInstanceToNode(ctx, inst, zone)
			if err != nil {
				klog.Errorf("[KwokReconciler] Failed to construct Node object for %s: %v", name, err)
				continue
			}
			if err := r.kubeClient.Create(ctx, node); err != nil {
				klog.Errorf("[KwokReconciler] Failed to create K8s node %s: %v", name, err)
			}
		}
	}
}

func (r *KwokGceReconciler) fetchRawInstances(ctx context.Context, zone, filter string) ([]*gce_api.Instance, error) {
	if r.gceService == nil {
		return nil, fmt.Errorf("gceService is nil")
	}
	var instances []*gce_api.Instance
	err := r.gceService.Instances.List(r.projectID, zone).Filter(filter).Pages(ctx, func(page *gce_api.InstanceList) error {
		instances = append(instances, page.Items...)
		return nil
	})
	return instances, err
}
