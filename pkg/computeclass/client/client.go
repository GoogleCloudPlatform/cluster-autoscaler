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

package client

import (
	ccc_clientset "github.com/googlecloudplatform/compute-class-api/client/clientset/versioned"
	"k8s.io/client-go/rest"
)

// Client groups clients for node provisioning CRDs
type Client interface {
	CccClient() ccc_clientset.Interface
}

type client struct {
	cccClient ccc_clientset.Interface
}

// CccClient returns the CCC CRD client
func (c *client) CccClient() ccc_clientset.Interface {
	return c.cccClient
}

// NewClient returns bundled clients for node provisioning CRDs
func NewClient(kubeConfig *rest.Config) (Client, error) {
	var cccClient *ccc_clientset.Clientset
	var err error

	cccClient, err = ccc_clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	return &client{
		cccClient: cccClient,
	}, err
}

// NewClientFromClientsets returns bundled clients for node provisioning CRDs from given clientsets
func NewClientFromClientsets(cccClient ccc_clientset.Interface) Client {
	return &client{
		cccClient: cccClient,
	}
}
