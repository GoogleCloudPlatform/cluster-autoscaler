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

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	gce_api "google.golang.org/api/compute/v1"
	"gopkg.in/gcfg.v1"

	provider_gce "k8s.io/cloud-provider-gcp/providers/gce"
	gkeutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
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

type metadataClient interface {
	ProjectIDWithContext(ctx context.Context) (string, error)
	ZoneWithContext(ctx context.Context) (string, error)
}

// GetProjectAndLocation returns the GCP project and the cluster location, a zone
// for zonal clusters or a region for regional ones.
func GetProjectAndLocation(ctx context.Context, md metadataClient, regional bool, cfg *provider_gce.ConfigFile, explicitLocation string) (string, string, error) {
	projectId, err := getProject(ctx, md, cfg)
	if err != nil {
		return "", "", err
	}
	location, err := getLocation(ctx, md, regional, cfg, explicitLocation)
	if err != nil {
		return "", "", err
	}
	return projectId, location, nil
}

// On the GKE control plane the metadata server returns the hosted master project
// rather than the cluster project, so the GKE-managed CA sets project-id in the
// config file to the cluster project.
func getProject(ctx context.Context, md metadataClient, cfg *provider_gce.ConfigFile) (string, error) {
	if cfg != nil && cfg.Global.ProjectID != "" {
		return cfg.Global.ProjectID, nil
	}
	return md.ProjectIDWithContext(ctx)
}

// getLocation returns the cluster location from the first of these that is set:
//   - the explicit cluster location (the --location flag)
//   - local-zone from the cloud config file
//   - the zone reported by the metadata server
//
// For regional clusters, the region is returned. Correctness of the last two
// options in isolation relies on the CA running in a KCP zone. That is true for the
// regular GKE CA, but a self-hosted CA may run in a non-KCP zone of a multi-zonal
// cluster. For self-hosted we assume that the explicit location is always provided.
func getLocation(ctx context.Context, md metadataClient, regional bool, cfg *provider_gce.ConfigFile, explicitLocation string) (string, error) {
	if explicitLocation != "" {
		return explicitLocation, nil
	}

	// TODO: Drop the local-zone and metadata server fallbacks. The regular GKE CA always
	// passes --location and a self-hosted CA is only supported with it.
	var zone string
	if cfg != nil {
		zone = cfg.Global.LocalZone
	}
	if zone == "" {
		var err error
		if zone, err = md.ZoneWithContext(ctx); err != nil {
			return "", err
		}
	}

	if !regional {
		return zone, nil
	}
	return gkeutil.GetRegionFromLocation(zone)
}
