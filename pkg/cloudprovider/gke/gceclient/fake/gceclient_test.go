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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	gceinternal "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

func TestCreateInstancesWithRecommendation(t *testing.T) {
	mig := newDefaultMig(2)
	client := newDefaultGceClient(t).
		WithTemplates(mig.template).
		WithMigs(mig.mig)

	names, err := client.CreateInstancesWithRecommendation(testMigRef(), testTemplate, mig.size, nil, "1/rec1//spec1")
	assert.NoError(t, err)
	assert.Len(t, names, 2)

	recs := client.GetRecordedRecommendations(testMigRef())
	assert.Equal(t, []string{"1/rec1//spec1"}, recs)
}

func TestZoneScopedCallsAreCountedAndFail(t *testing.T) {
	const zone, otherZone = "us-central1-a", "us-central1-b"
	ctx := context.Background()
	ref := gceinternal.GceRef{Project: "test-project", Zone: zone, Name: "test-mig"}
	zoneErr := errors.New("zone is down")

	tests := map[string]func(c *GceClient) error{
		"FetchAllMigs":                     func(c *GceClient) error { _, err := c.FetchAllMigs(zone); return err },
		"FetchMig":                         func(c *GceClient) error { _, err := c.FetchMig(ctx, ref); return err },
		"FetchAllInstances":                func(c *GceClient) error { _, err := c.FetchAllInstances(ctx, ref.Project, zone, ""); return err },
		"FetchMigTargetSize":               func(c *GceClient) error { _, err := c.FetchMigTargetSize(ctx, ref); return err },
		"FetchMigBasename":                 func(c *GceClient) error { _, err := c.FetchMigBasename(ctx, ref); return err },
		"FetchMigInstances":                func(c *GceClient) error { _, err := c.FetchMigInstances(ctx, ref); return err },
		"FetchMigTemplateName":             func(c *GceClient) error { _, err := c.FetchMigTemplateName(ctx, ref); return err },
		"FetchMigTemplate":                 func(c *GceClient) error { _, err := c.FetchMigTemplate(ctx, ref, "tmpl", false); return err },
		"FetchMigsWithName":                func(c *GceClient) error { _, err := c.FetchMigsWithName(ctx, zone, nil); return err },
		"FetchListManagedInstancesResults": func(c *GceClient) error { _, err := c.FetchListManagedInstancesResults(ctx, ref); return err },
		"FetchManagedInstances":            func(c *GceClient) error { _, err := c.FetchManagedInstances(ref, ""); return err },
		"FetchMachineType":                 func(c *GceClient) error { _, err := c.FetchMachineType(ctx, zone, "n1-standard-4"); return err },
		"FetchMachineTypes":                func(c *GceClient) error { _, err := c.FetchMachineTypes(zone); return err },
		"FetchAvailableDiskTypes":          func(c *GceClient) error { _, err := c.FetchAvailableDiskTypes(zone); return err },
		"FetchAcceleratorTypes":            func(c *GceClient) error { _, err := c.FetchAcceleratorTypes(zone); return err },
		"ResizeMig":                        func(c *GceClient) error { return c.ResizeMig(ctx, ref, 1) },
		"DeleteInstances":                  func(c *GceClient) error { return c.DeleteInstances(ctx, ref, nil) },
		"CreateInstances":                  func(c *GceClient) error { _, err := c.CreateInstances(ctx, ref, "tmpl", 1, nil); return err },
		"CreateInstancesWithRecommendation": func(c *GceClient) error {
			_, err := c.CreateInstancesWithRecommendation(ref, "tmpl", 1, nil, "rec")
			return err
		},
		"ResizeAtomically": func(c *GceClient) error { return c.ResizeAtomically(ref, "tmpl", nil) },
		"ResumeInstances":  func(c *GceClient) error { return c.ResumeInstances(ref, nil) },
		"SuspendInstances": func(c *GceClient) error { return c.SuspendInstances(ref, nil, false) },
		"WaitForOperation": func(c *GceClient) error { return c.WaitForOperation(ctx, "op", "insert", ref.Project, zone) },
		"InsertInstanceGroupManager": func(c *GceClient) error {
			return c.InsertInstanceGroupManager(ref.Project, zone, &gcev1.InstanceGroupManager{Name: ref.Name})
		},
		"DeleteInstanceGroupManager": func(c *GceClient) error { return c.DeleteInstanceGroupManager(zone, ref.Name) },
	}

	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			client := NewGceClient(t, nil)
			client.SetZoneError(zone, zoneErr)

			assert.ErrorIs(t, call(client), zoneErr)
			assert.Equal(t, 1, client.TotalCallsForZone(zone))
			assert.Equal(t, 0, client.TotalCallsForZone(otherZone))
		})
	}
}
