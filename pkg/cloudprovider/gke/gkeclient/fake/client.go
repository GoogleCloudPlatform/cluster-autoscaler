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

package fake

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	gcev1 "google.golang.org/api/compute/v1"
	gkeapibeta "google.golang.org/api/container/v1beta1"
	fakek8s "k8s.io/autoscaler/cluster-autoscaler/utils/fake"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
)

type Client struct {
	sync.Mutex

	// --- Internal State ---
	cluster    *gkeapibeta.Cluster
	operations map[string]*gkeapibeta.Operation
	gceClient  *fakegce.GceClient
	project    string
	location   string
	opCounter  int64

	k8s *fakek8s.Kubernetes
}

// NewClient creates a new fake client.
func NewClient(gceClient *fakegce.GceClient, k8s *fakek8s.Kubernetes) *Client {
	return &Client{
		operations: make(map[string]*gkeapibeta.Operation),
		gceClient:  gceClient,
		k8s:        k8s,
	}
}

// --- State Builders ---

// WithCluster sets the initial cluster state.
// TODO(b/463315524): extract WithCluster() as a separate, top-level CreateCluster() step.
// It already performs non-trivial initialization logic, i.e. node pool simulation and
// device classes initialization.
func (f *Client) WithCluster(c *gkeapibeta.Cluster) (*Client, error) {
	f.cluster = c
	if c == nil {
		return f, nil
	}

	for _, np := range c.NodePools {
		if len(np.InstanceGroupUrls) == 0 {
			np.InstanceGroupUrls = f.instanceGroupURLs(np)
		}
		for _, zone := range np.Locations {
			if err := f.initializeNodePoolInZone(np, zone); err != nil {
				return nil, err
			}
		}
	}
	return f, nil
}

// WithProject sets the project ID.
func (f *Client) WithProject(project string) *Client {
	f.project = project
	return f
}

// WithLocation sets the primary location (region or zone).
func (f *Client) WithLocation(location string) *Client {
	f.location = location
	return f
}

// --- Interface Implementation ---

func (f *Client) GetCluster(clusterPath string) (*gkeapibeta.Cluster, error) {
	f.Lock()
	defer f.Unlock()

	if f.cluster == nil {
		return nil, fmt.Errorf("cluster not found")
	}
	if f.project == "" || f.location == "" {
		return nil, fmt.Errorf("fake Client not initialized with WithProject and WithLocation")
	}

	expectedPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", f.project, f.location, f.cluster.Name)
	if clusterPath != expectedPath {
		return nil, fmt.Errorf("cluster path mismatch: expected %s, got %s", expectedPath, clusterPath)
	}

	clusterCopy := *f.cluster
	return &clusterCopy, nil
}

// CreateNodePool simulates node pool creation.
//
// Quiet assumption: It directly manipulates the fake GCE client to create the
// underlying Instance Template and MIG, skipping the actual GKE control plane delays.
//
// Zero-delay assumption: This fake client generally assumes zero-delay for operations
// unless explicitly configured otherwise (though GetOperation simulates a 1-retry delay).
func (f *Client) CreateNodePool(clusterPath string, req *gkeapibeta.CreateNodePoolRequest) (*gkeapibeta.Operation, error) {
	f.Lock()
	defer f.Unlock()

	if f.cluster == nil {
		return nil, fmt.Errorf("cluster not found")
	}

	npName := req.NodePool.Name
	var firstZone string
	if len(req.NodePool.Locations) > 0 {
		firstZone = req.NodePool.Locations[0]
	}

	for _, zone := range req.NodePool.Locations {
		templateName := fmt.Sprintf("%s-%s-tmpl", npName, zone)
		newTemplate, err := buildInstanceTemplateFromRequest(f.project, templateName, req.NodePool, zone)
		if err != nil {
			return nil, fmt.Errorf("template creation failed for zone %s: %v", zone, err)
		}

		if err := f.gceClient.InsertInstanceTemplate(f.project, newTemplate); err != nil {
			return nil, fmt.Errorf("insert template operation failed for zone %s: %v", zone, err)
		}

		if err := f.gceClient.InsertInstanceGroupManager(f.project, zone, f.newMig(req.NodePool, zone)); err != nil {
			return nil, fmt.Errorf("InsertInstanceGroupManager failed for zone %s: %w", zone, err)
		}
	}

	f.cluster.NodePools = append(f.cluster.NodePools, f.newNodePool(req.NodePool, clusterPath))

	opName := f.generateOpName("createNodePool")
	operation := f.newOperation(opName, "RUNNING", "CREATE_NODE_POOL", firstZone, fmt.Sprintf("%s/nodePools/%s", clusterPath, npName))
	f.operations[opName] = operation
	return operation, nil
}

// DeleteNodePool simulates node pool deletion.
// Quiet assumption: Since the fake GCE client currently doesn't support deleting MIGs,
// it hacks around this by resizing the MIG to 0 so the simulated instances disappear.
func (f *Client) DeleteNodePool(nodePoolPath string) (*gkeapibeta.Operation, error) {
	f.Lock()
	defer f.Unlock()

	var targetPool *gkeapibeta.NodePool
	var keptPools []*gkeapibeta.NodePool
	for _, np := range f.cluster.NodePools {
		if np.SelfLink == nodePoolPath {
			targetPool = np
		} else {
			keptPools = append(keptPools, np)
		}
	}

	if targetPool == nil {
		return nil, fmt.Errorf("node pool %s not found", nodePoolPath)
	}

	if len(targetPool.InstanceGroupUrls) > 0 {
		igmUrl := targetPool.InstanceGroupUrls[0]
		urlParts := strings.Split(igmUrl, "/")
		migName := urlParts[len(urlParts)-1]
		zone := urlParts[len(urlParts)-3]

		err := f.gceClient.DeleteInstanceGroupManager(zone, migName)
		if err != nil {
			return nil, err
		}
		templateName := fmt.Sprintf("%s-tmpl", targetPool.Name)
		err = f.gceClient.DeleteInstanceTemplate(templateName)
		if err != nil {
			return nil, err
		}
	}

	f.cluster.NodePools = keptPools

	opName := f.generateOpName("deleteNodePool")
	zone := targetPool.Locations[0]
	operation := f.newOperation(opName, "RUNNING", "DELETE_NODE_POOL", zone, nodePoolPath)
	f.operations[opName] = operation
	return operation, nil
}

// GetOperation retrieves an operation by its path.
// Quiet assumption: To simulate real-world delays and trigger the Autoscaler's
// operation polling retry logic, this method returns a copy of the operation in its current
// state but immediately marks the stored operation as DONE if it was RUNNING.
// Thus, a RUNNING operation requires exactly one retry before it reports as DONE.
func (f *Client) GetOperation(operationPath string) (*gkeapibeta.Operation, error) {
	f.Lock()
	defer f.Unlock()

	opName := path.Base(operationPath)

	op, found := f.operations[opName]
	if !found {
		return nil, fmt.Errorf("operation %s not found", operationPath)
	}

	// Copy the operation to simulate asynchronous state transitions
	// and allow returning the RUNNING state once before marking it DONE.
	opCopy := *op

	if op.Status == "RUNNING" {
		op.Status = "DONE"
	}
	return &opCopy, nil
}

func (f *Client) UpdateNodePoolLabels(nodePoolPath string, req *gkeapibeta.UpdateNodePoolRequest) (*gkeapibeta.Operation, error) {
	return nil, fmt.Errorf("not implemented")
}

// --- Helper functions ---

func (f *Client) newOperation(opName, opStatus, opType, zone, path string) *gkeapibeta.Operation {
	opPath := fmt.Sprintf("projects/%s/locations/%s/operations/%s", f.project, f.location, opName)
	return &gkeapibeta.Operation{Name: opPath, Status: opStatus, Zone: zone, OperationType: opType, TargetLink: path}
}

func (f *Client) generateOpName(opType string) string {
	seq := atomic.AddInt64(&f.opCounter, 1)
	return fmt.Sprintf("fake-%s-op-%d", opType, seq)
}

func (f *Client) newMig(np *gkeapibeta.NodePool, zone string) *gcev1.InstanceGroupManager {
	return &gcev1.InstanceGroupManager{
		Name:             np.Name,
		Zone:             zone,
		InstanceTemplate: fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/instanceTemplates/%s-%s-tmpl", f.project, np.Name, zone),
		TargetSize:       np.InitialNodeCount,
		BaseInstanceName: np.Name,
	}
}

func (f *Client) instanceGroupURLs(np *gkeapibeta.NodePool) []string {
	var urls []string
	for _, zone := range np.Locations {
		url := fmt.Sprintf(
			"https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instanceGroupManagers/%s",
			f.project, zone, np.Name,
		)
		urls = append(urls, url)
	}
	return urls
}

func (f *Client) newNodePool(np *gkeapibeta.NodePool, clusterPath string) *gkeapibeta.NodePool {
	nodePoolPath := fmt.Sprintf("%s/nodePools/%s", clusterPath, np.Name)
	return &gkeapibeta.NodePool{
		Name:              np.Name,
		SelfLink:          nodePoolPath,
		Status:            "RUNNING",
		Locations:         np.Locations,
		InitialNodeCount:  np.InitialNodeCount,
		InstanceGroupUrls: f.instanceGroupURLs(np),
		Config:            np.Config,
		Autoscaling:       np.Autoscaling,
		PlacementPolicy:   np.PlacementPolicy,
	}
}

func (f *Client) initializeNodePoolInZone(np *gkeapibeta.NodePool, zone string) error {
	templateName := fmt.Sprintf("%s-%s-tmpl", np.Name, zone)
	newTemplate, err := buildInstanceTemplateFromRequest(f.project, templateName, np, zone)
	if err != nil {
		return fmt.Errorf("failed to build template for node pool %s in zone %s: %v", np.Name, zone, err)
	}

	if err := ignoreAlreadyExists(f.gceClient.InsertInstanceTemplate(f.project, newTemplate)); err != nil {
		return fmt.Errorf("failed to insert template for node pool %s in zone %s: %v", np.Name, zone, err)
	}

	if err := ignoreAlreadyExists(f.gceClient.InsertInstanceGroupManager(f.project, zone, f.newMig(np, zone))); err != nil {
		return fmt.Errorf("failed to insert MIG for node pool %s in zone %s: %v", np.Name, zone, err)
	}

	return nil
}

func ignoreAlreadyExists(err error) error {
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}
