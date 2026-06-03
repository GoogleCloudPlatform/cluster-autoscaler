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

package gce

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	gce_api "google.golang.org/api/compute/v1"
	"gopkg.in/gcfg.v1"

	provider_gce "k8s.io/cloud-provider-gcp/providers/gce"
	"k8s.io/klog/v2"
)

// GceConfigProvider reads GCE provider config from the environment.
func GceConfigProvider(cloudConfig string) (*provider_gce.ConfigFile, error) {
	var configReader io.ReadCloser
	if cloudConfig != "" {
		var err error
		configReader, err = os.Open(cloudConfig)
		if err != nil {
			return nil, fmt.Errorf("couldn't open cloud provider configuration %s: %#v", cloudConfig, err)
		}
		defer configReader.Close()
	}
	if configReader != nil {
		var cfg provider_gce.ConfigFile
		if err := gcfg.FatalOnly(gcfg.ReadInto(&cfg, configReader)); err != nil {
			klog.Errorf("Couldn't read config: %v", err)
			return nil, err
		}
		return &cfg, nil
	}
	return nil, nil
}

// GetTokenSource reads GCE token source from the environment.
func GetTokenSource(cfg *provider_gce.ConfigFile, ctx context.Context) (oauth2.TokenSource, error) {
	var err error
	tokenSource := google.ComputeTokenSource("")
	if len(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")) > 0 {
		tokenSource, err = google.DefaultTokenSource(ctx, gce_api.ComputeScope)
		if err != nil {
			return nil, err
		}
	}
	if cfg != nil {
		if cfg.Global.TokenURL == "" {
			klog.Warning("Empty tokenUrl in cloud config")
		} else {
			tokenSource = provider_gce.NewAltTokenSource(cfg.Global.TokenURL, cfg.Global.TokenBody)
			klog.V(1).Infof("Using TokenSource from config %#v", tokenSource)
		}
	} else {
		klog.V(1).Infof("Using default TokenSource %#v", tokenSource)
	}
	return tokenSource, nil
}

// GetProjectAndLocation lookups GCE project and locationm, if the function fails
// to discover project and/or location from the provider, it uses default values from
// the config file.
func GetProjectAndLocation(regional bool, cfg *provider_gce.ConfigFile, ctx context.Context) (string, string, error) {
	var projectId, location string
	if cfg != nil {
		projectId = cfg.Global.ProjectID
		location = cfg.Global.LocalZone
	}

	if len(projectId) == 0 || len(location) == 0 {
		// XXX: On GKE discoveredProjectId is hosted master project and
		// not the project we want to use, however, zone seems to not
		// be specified in config. For now we can just assume that hosted
		// master project is in the same zone as cluster and only use
		// discoveredZone.
		discoveredProjectId, discoveredLocation, err := discoverGceProviderProjectAndLocation(regional, ctx)
		if err != nil {
			return "", "", err
		}
		if len(projectId) == 0 {
			projectId = discoveredProjectId
		}
		if len(location) == 0 {
			location = discoveredLocation
		}
	}
	return projectId, location, nil
}

// Code borrowed from gce cloud provider. Reuse the original as soon as it becomes public.
func discoverGceProviderProjectAndLocation(regional bool, ctx context.Context) (string, string, error) {
	result, err := metadata.GetWithContext(ctx, "instance/zone")
	if err != nil {
		return "", "", err
	}
	parts := strings.Split(result, "/")
	if len(parts) != 4 {
		return "", "", fmt.Errorf("unexpected response: %s", result)
	}
	location := parts[3]
	if regional {
		location, err = provider_gce.GetGCERegion(location)
		if err != nil {
			return "", "", err
		}
	}
	projectID, err := metadata.ProjectIDWithContext(ctx)
	if err != nil {
		return "", "", err
	}
	return projectID, location, nil
}
