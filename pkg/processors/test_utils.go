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

package processors

import (
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

type migTemplate struct {
	project        string
	zone           string
	name           string
	nodePoolName   string
	locationPolicy gke.LocationPolicyEnum
	sizes          migSizes
	blueGreenInfo  *gke.MigBlueGreenInfo
	tpuMultiHost   bool
	tpuType        string
}

type migTemplateOpt func(*migTemplate)

func newMigTemplate(project, zone, name, nodePoolName string, sizes migSizes, opts ...migTemplateOpt) migTemplate {
	tmpl := migTemplate{
		project, zone, name, nodePoolName, gke.LocationPolicyUnspecified, sizes, nil, false, "",
	}
	for _, opt := range opts {
		opt(&tmpl)
	}
	return tmpl
}

func withLocationPolicy(lp gke.LocationPolicyEnum) migTemplateOpt { // nolint:unused
	return func(t *migTemplate) {
		t.locationPolicy = lp
	}
}

func withBlueGreenInfo(bgi *gke.MigBlueGreenInfo) migTemplateOpt {
	return func(t *migTemplate) {
		t.blueGreenInfo = bgi
	}
}

func withTpuMultiHost() migTemplateOpt {
	return func(t *migTemplate) {
		t.tpuMultiHost = true
		t.tpuType = "tpuV5"
	}
}

type migSizes struct {
	currentSize  int
	totalMaxSize int
	totalMinSize int
	minSize      int
	maxSize      int
}

func (n migTemplate) id() string {
	if n.blueGreenInfo == nil {
		return n.nodePoolName
	}
	return fmt.Sprintf("%s:%s", n.nodePoolName, n.blueGreenInfo.Color)
}

func (n migTemplate) fillTemplate(gkeMock *gke.GkeManagerMock) *gke.GkeMig {
	val := gke.NewTestGkeMigBuilder().
		SetGceRef(gce.GceRef{Project: n.project, Zone: n.zone, Name: n.name}).
		SetGkeManager(gkeMock).
		SetMinSize(n.sizes.minSize).
		SetMaxSize(n.sizes.maxSize).
		SetTotalMaxSize(n.sizes.totalMaxSize).
		SetTotalMinSize(n.sizes.totalMinSize).
		SetLocationPolicy(n.locationPolicy).
		SetExist(true).
		SetNodePoolName(n.nodePoolName).
		SetSpec(&gkeclient.NodePoolSpec{
			TpuMultiHost: n.tpuMultiHost,
			TpuType:      n.tpuType,
		}).
		SetBlueGreenInfo(n.blueGreenInfo).
		Build()
	gkeMock.On("GetMigSize", val).Return(int64(n.sizes.currentSize), nil).Times(2)
	gkeMock.On("CapacityCheckWaitTimeSeconds", val).Return(10*time.Minute, nil)
	gkeMock.On("IsResizeRequestErrorHandlingEnabled").Return(false)
	return val
}

func fillMigTemplates(gkeMock *gke.GkeManagerMock, templates []migTemplate, getMigsTargetSizeError error) ([]cloudprovider.NodeGroup, []*gke.GkeMig) {
	// Build a slice of MigRef within a nodepool
	gceRefsMap := map[string][]gce.GceRef{}
	for _, t := range templates {
		gceRef := gce.GceRef{Project: t.project, Zone: t.zone, Name: t.name}
		gceRefs := gceRefsMap[t.id()]
		gceRefsMap[t.id()] = append(gceRefs, gceRef)
	}

	// Build a slice of Migs within a nodepool
	nodeGroups := make([]cloudprovider.NodeGroup, 0, len(templates))
	migs := make([]*gke.GkeMig, 0, len(templates))
	for _, m := range templates {
		mig := m.fillTemplate(gkeMock)
		nodeGroups = append(nodeGroups, mig)
		migs = append(migs, mig)
	}

	migsByNodePool := make(map[string][]*gke.GkeMig)
	for _, m := range migs {
		migsByNodePool[m.NodePoolName()] = append(migsByNodePool[m.NodePoolName()], m)
	}
	for name, ms := range migsByNodePool {
		gke.AddMigsToNodePool(name, ms...)
	}

	// Mock total size calls for each color
	for _, gceRefs := range gceRefsMap {
		currentNodePoolSize := computeNodePoolSize(gceRefs, templates)
		gkeMock.On("GetMigsTargetSize", gceRefs).Return(currentNodePoolSize, getMigsTargetSizeError)
	}
	return nodeGroups, migs
}

func computeNodePoolSize(gceRefs []gce.GceRef, templates []migTemplate) int64 {
	var currentNodePoolSize int64
	for _, t := range templates {
		tg := gce.GceRef{Project: t.project, Zone: t.zone, Name: t.name}
		for _, g := range gceRefs {
			if tg.String() == g.String() {
				currentNodePoolSize += int64(t.sizes.currentSize)
			}
		}
	}
	return currentNodePoolSize
}

type scaleUpTemplate struct {
	currentSize int
	newSize     int
	maxSize     int
}

func (t scaleUpTemplate) fillTemplate(nodeGroup cloudprovider.NodeGroup) nodegroupset.ScaleUpInfo {
	return nodegroupset.ScaleUpInfo{
		Group:       nodeGroup,
		CurrentSize: t.currentSize,
		NewSize:     t.newSize,
		MaxSize:     t.maxSize,
	}
}

func fillScaleUpTemplates(templates []scaleUpTemplate, nodeGroups []cloudprovider.NodeGroup) []nodegroupset.ScaleUpInfo {
	result := make([]nodegroupset.ScaleUpInfo, 0, len(templates))
	for i, t := range templates {
		result = append(result, t.fillTemplate(nodeGroups[i]))
	}
	return result
}

// AddNodePoolLabelToNode adds nodepool label to node.
func AddNodePoolLabelToNode(node *apiv1.Node, nodePoolname string) {
	node.Labels[nodePoolLabel] = nodePoolname
}
