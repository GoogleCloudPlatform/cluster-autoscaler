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
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/mock"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestTotalMaxSizeProcessorBalanceScaleUpBetweenGroups(t *testing.T) {
	type args struct {
		newNodes               int
		migTemplates           []migTemplate
		getMigsTargetSizeError error
	}
	tests := []struct {
		name          string
		args          args
		wantTemplates []scaleUpTemplate
		wantErr       bool
	}{
		{
			name: "simple test",
			args: args{
				newNodes: 5,
				migTemplates: []migTemplate{
					newMigTemplate("test-proj", "us-test1-a", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 10}),
					newMigTemplate("test-proj", "us-test1-b", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 10}),
					newMigTemplate("test-proj", "us-test1-c", "testName", "testNodePoolName", migSizes{totalMaxSize: 10}),
				},
			},
			wantTemplates: []scaleUpTemplate{
				{currentSize: 2, newSize: 3, maxSize: 8},
				{currentSize: 2, newSize: 3, maxSize: 8},
				{newSize: 3, maxSize: 6},
			},
		},
		{
			name: "scale-up capped to the total max size",
			args: args{
				newNodes: 20,
				migTemplates: []migTemplate{
					newMigTemplate("test-proj", "us-test1-a", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 12}),
					newMigTemplate("test-proj", "us-test1-b", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 12}),
					newMigTemplate("test-proj", "us-test1-c", "testName", "testNodePoolName", migSizes{totalMaxSize: 12}),
				},
			},
			wantTemplates: []scaleUpTemplate{
				{currentSize: 2, newSize: 4, maxSize: 10},
				{currentSize: 2, newSize: 4, maxSize: 10},
				{newSize: 4, maxSize: 8},
			},
		},
		{
			name: "scale-up capped for the two zones",
			args: args{
				newNodes: 10,
				migTemplates: []migTemplate{
					newMigTemplate("test-proj", "us-test1-a", "testName", "testNodePoolName", migSizes{currentSize: 1, totalMaxSize: 5}),
					newMigTemplate("test-proj", "us-test1-c", "testName", "testNodePoolName", migSizes{totalMaxSize: 5}),
				},
			},
			wantTemplates: []scaleUpTemplate{
				{currentSize: 1, newSize: 2, maxSize: 5},
				{newSize: 3, maxSize: 4},
			},
		},
		{
			name: "scale-up not limited by total size",
			args: args{
				newNodes: 5,
				migTemplates: []migTemplate{
					newMigTemplate("test-proj", "us-test1-a", "testName", "testNodePoolName", migSizes{currentSize: 2, maxSize: 5}),
					newMigTemplate("test-proj", "us-test1-b", "testName", "testNodePoolName", migSizes{currentSize: 2, maxSize: 5}),
					newMigTemplate("test-proj", "us-test1-c", "testName", "testNodePoolName", migSizes{maxSize: 5}),
				},
			},
			wantTemplates: []scaleUpTemplate{
				{currentSize: 2, newSize: 3, maxSize: 5},
				{currentSize: 2, newSize: 3, maxSize: 5},
				{newSize: 3, maxSize: 5},
			},
		},
		{
			name: "scale-up cannot scale-up as total max size was alerady reached",
			args: args{
				newNodes: 5,
				migTemplates: []migTemplate{
					newMigTemplate("test-proj", "us-test1-a", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 5}),
					newMigTemplate("test-proj", "us-test1-b", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 5}),
					newMigTemplate("test-proj", "us-test1-c", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 5}),
				},
			},
			wantTemplates: []scaleUpTemplate{},
		},
		{
			name: "node pool size cannot be accessed",
			args: args{
				newNodes: 5,
				migTemplates: []migTemplate{
					newMigTemplate("test-proj", "us-test1-a", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 10}),
					newMigTemplate("test-proj", "us-test1-b", "testName", "testNodePoolName", migSizes{currentSize: 2, totalMaxSize: 10}),
					newMigTemplate("test-proj", "us-test1-c", "testName", "testNodePoolName", migSizes{totalMaxSize: 10}),
				},
				getMigsTargetSizeError: errors.New("test error1"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gkeManager := &gke.GkeManagerMock{}
			s := NewTotalMaxSizeProcessor(&nodegroupset.BalancingNodeGroupSetProcessor{
				Comparator: IsGkeNodeInfoSimilar,
			})
			groups, _ := fillMigTemplates(gkeManager, tt.args.migTemplates, tt.args.getMigsTargetSizeError)

			got, err := s.BalanceScaleUpBetweenGroups(nil, groups, tt.args.newNodes)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NodeGroupSetProcessor.BalanceScaleUpBetweenGroups() error = %v, wantErr %v", err, tt.wantErr)
			} else if tt.wantErr {
				return
			}

			sort.Slice(got, func(i, j int) bool {
				return got[i].Group.Id() < got[j].Group.Id()
			})
			wantVal := fillScaleUpTemplates(tt.wantTemplates, groups)
			if !reflect.DeepEqual(got, wantVal) {
				t.Errorf("NodeGroupSetProcessor.BalanceScaleUpBetweenGroups() got = %+v, want %+v", got, wantVal)
			}
		})
	}
}

func TestTotalMaxSizeProcessorBlueGreeScaleUp(t *testing.T) {
	zones := []string{"us-test1-a", "us-test1-b", "us-test1-c"}
	gkeManager := &gke.GkeManagerMock{}
	blueMigs, greenMigs, _ := createMockMigs(gkeManager, zones, 5, 30)

	s := NewTotalMaxSizeProcessor(&nodegroupset.BalancingNodeGroupSetProcessor{
		Comparator: IsGkeNodeInfoSimilar,
	})
	gotBlue, err := s.BalanceScaleUpBetweenGroups(nil, blueMigs, 9)
	if err != nil {
		t.Fatalf("NodeGroupSetProcessor.BalanceScaleUpBetweenGroups() error = %v", err)
	}
	wantBlue := createExpectedScaleUp(blueMigs, 5, 8, 20)
	if !reflect.DeepEqual(gotBlue, wantBlue) {
		t.Errorf("NodeGroupSetProcessor.BalanceScaleUpBetweenGroups() gotBlue = %+v, wantBlue %+v", gotBlue, wantBlue)
	}

	gotGreen, err := s.BalanceScaleUpBetweenGroups(nil, greenMigs, 9)
	if err != nil {
		t.Fatalf("NodeGroupSetProcessor.BalanceScaleUpBetweenGroups() error = %v", err)
	}
	wantGreen := createExpectedScaleUp(greenMigs, 5, 8, 20)
	if !reflect.DeepEqual(gotGreen, wantGreen) {
		t.Errorf("NodeGroupSetProcessor.BalanceScaleUpBetweenGroups() gotGreen = %+v, wantGreen %+v", gotGreen, wantGreen)
	}
	mock.AssertExpectationsForObjects(t, gkeManager)
}

func createMockMigs(gkeMock *gke.GkeManagerMock, zones []string, migSize, totalMaxSize int) ([]cloudprovider.NodeGroup, []cloudprovider.NodeGroup, []*gke.GkeMig) {
	var migGroups []*gke.GkeMig
	var blueMigs, greenMigs []cloudprovider.NodeGroup
	var blueGceRefs, greenGceRefs []gce.GceRef
	for _, zone := range zones {
		builder := gke.NewTestGkeMigBuilder().
			SetGceRefProject("test-project").
			SetGceRefZone(zone).
			SetGkeManager(gkeMock).
			SetTotalMaxSize(totalMaxSize).
			SetExist(true).
			SetNodePoolName("test-pool").
			SetSpec(&gkeclient.NodePoolSpec{})

		builder.SetBlueGreenInfo(&gke.MigBlueGreenInfo{
			Color: gke.BlueMig,
			Phase: gkeclient.PhaseUnspecified,
		}).SetGceRefName("blue-mig-" + zone)
		blueMig := builder.Build()
		gkeMock.On("GetMigSize", blueMig).Return(int64(migSize), nil).Times(2)
		migGroups = append(migGroups, blueMig)
		blueMigs = append(blueMigs, blueMig)
		blueGceRefs = append(blueGceRefs, blueMig.GceRef())

		builder.SetBlueGreenInfo(&gke.MigBlueGreenInfo{
			Color: gke.GreenMig,
			Phase: gkeclient.PhaseUnspecified,
		}).SetGceRefName("green-mig-" + zone)
		greenMig := builder.Build()
		gkeMock.On("GetMigSize", greenMig).Return(int64(migSize), nil).Times(2)
		migGroups = append(migGroups, greenMig)
		greenMigs = append(greenMigs, greenMig)
		greenGceRefs = append(greenGceRefs, greenMig.GceRef())
	}

	migsByNodePool := make(map[string][]*gke.GkeMig)
	for _, m := range migGroups {
		migsByNodePool[m.NodePoolName()] = append(migsByNodePool[m.NodePoolName()], m)
	}
	for name, ms := range migsByNodePool {
		gke.AddMigsToNodePool(name, ms...)
	}

	gkeMock.On("GetMigsTargetSize", greenGceRefs).Return(int64(migSize*len(greenGceRefs)), nil)
	gkeMock.On("GetMigsTargetSize", blueGceRefs).Return(int64(migSize*len(blueGceRefs)), nil)
	return blueMigs, greenMigs, migGroups
}

func createExpectedScaleUp(groups []cloudprovider.NodeGroup, migSize, newSize, maxSize int) []nodegroupset.ScaleUpInfo {
	var scaleUps []nodegroupset.ScaleUpInfo
	for _, g := range groups {
		scaleUps = append(scaleUps, nodegroupset.ScaleUpInfo{
			Group:       g,
			CurrentSize: migSize,
			NewSize:     newSize,
			MaxSize:     maxSize,
		})
	}
	return scaleUps
}
