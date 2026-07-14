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

package placement

import (
	"context"
	"errors"
	"sync"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const (
	// resourcePolicyPullerInterval is the interval for pulling resource policies.
	resourcePolicyPullerInterval = time.Minute
)

type ResourcePolicyPuller interface {
	// GetResourcePolicy returns the cached resource policy
	GetResourcePolicy(name string) *gceclient.GceResourcePolicy
	// Run starts the resource policies puller
	Run(ctx context.Context)
}

type resourcePolicyPullerProvider interface {
	// GetResourcePolicies returns the resource policies in the provided project.
	GetResourcePolicies(projectID string) ([]*gceclient.GceResourcePolicy, error)
}

// ResourcePolicyPuller pulls GCE resource policies.
type rpPuller struct {
	sync.Mutex
	experimentsManager experiments.Manager
	provider           resourcePolicyPullerProvider
	projectID          string
	resourcePolicies   map[string]*gceclient.GceResourcePolicy
}

// NewResourcePolicyPuller builds a new ResourcePolicyPuller.
func NewResourcePolicyPuller(experimentsManager experiments.Manager, provider resourcePolicyPullerProvider, projectID string) *rpPuller {
	return &rpPuller{
		experimentsManager: experimentsManager,
		provider:           provider,
		projectID:          projectID,
		resourcePolicies:   make(map[string]*gceclient.GceResourcePolicy),
	}
}

// GetResourcePolicy returns the cached resource policy
func (p *rpPuller) GetResourcePolicy(name string) *gceclient.GceResourcePolicy {
	p.Lock()
	defer p.Unlock()
	return p.resourcePolicies[name]
}

// Run starts the resource policies puller
func (p *rpPuller) Run(ctx context.Context) {
	klog.V(0).Info("Enabling Resource Policies Puller")

	p.Loop()

	ticker := time.NewTicker(resourcePolicyPullerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.Loop()
		}
	}
}

// Loop runs a single loop of resource policies pulling.
func (p *rpPuller) Loop() {
	pullerEnabled := p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ResourcePolicyPullerFlag, false)
	if !pullerEnabled {
		klog.Info("Skipping resourcePolicyPuller loop: disabled by experiment")
		p.Lock()
		defer p.Unlock()
		clear(p.resourcePolicies)
		return
	}

	startTime := time.Now()
	klog.Info("Starting resourcePolicyPuller loop")

	rps, err := p.provider.GetResourcePolicies(p.projectID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			klog.Errorf("Internal timeout occurred while getting resource policies for project %s: %v", p.projectID, err)
		} else {
			klog.Errorf("Error when getting resource policies in project %s: %v", p.projectID, err)
		}
		return
	}

	p.Lock()
	defer p.Unlock()

	p.resourcePolicies = resourcePoliciesByName(rps)
	klog.V(4).Infof("ResourcePolicyPuller cached %d/%d resource policies in project %q in %v", len(p.resourcePolicies), len(rps), p.projectID, time.Since(startTime))
}

func resourcePoliciesByName(rps []*gceclient.GceResourcePolicy) map[string]*gceclient.GceResourcePolicy {
	rpsByName := make(map[string]*gceclient.GceResourcePolicy)
	for _, rp := range rps {
		if rp != nil && rp.Status == "READY" {
			rpsByName[rp.Name] = rp
		}
	}
	return rpsByName
}
