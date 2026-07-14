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

package bulkmig

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

type bulkMigClientFake struct {
	bulkMigs map[string]Status
}

func NewBulkMigClientFake(bulkMigs []Status) GceMigClient {
	res := map[string]Status{}
	for _, bmig := range bulkMigs {
		res[bmig.Ref.Name] = bmig
	}
	return &bulkMigClientFake{
		bulkMigs: res,
	}
}

func (f *bulkMigClientFake) BulkMigStatus(migRef gce.GceRef) (Status, error) {
	bmig, found := f.bulkMigs[migRef.Name]
	if !found {
		return Status{}, fmt.Errorf("Couldn't retrieve BulkMigStatus, mig %s not found.", migRef)
	}
	return bmig, nil
}

func (f *bulkMigClientFake) SetZeroTargetSize(migRef gce.GceRef) error {
	bmig, found := f.bulkMigs[migRef.Name]
	if !found {
		return fmt.Errorf("Couldn't retrieve BulkMigStatus, mig %s not found.", migRef)
	}
	bmig.TargetSize = 0
	bmig.InProgress = false
	f.bulkMigs[migRef.Name] = bmig
	return nil
}
