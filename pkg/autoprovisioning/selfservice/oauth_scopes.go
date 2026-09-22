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

package selfservice

import (
	"sort"
	"strings"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const oauthScopesPrefix = "oauth-scopes.cloud.google.com/"

func newOAuthScopes() feature {
	return &oauthScopes{}
}

type oauthScopes struct {
	internalFeatureDefaultImplementation
}

// prefixOAuthScopes maps each scope in oauthScopes to a flat metadata key.
// This allows CA's native superset matching logic to match node pools that
// have all required OAuth scopes.
func prefixOAuthScopes(oauthScopes []string) Metadata {
	if len(oauthScopes) == 0 {
		return nil
	}
	m := make(Metadata, len(oauthScopes))
	for _, scope := range oauthScopes {
		if scope != "" {
			m[oauthScopesPrefix+scope] = "true"
		}
	}
	if len(m) > 0 {
		return m
	}
	return nil
}

func (o *oauthScopes) FromNodepool(np *container.NodePool) Metadata {
	if np != nil && np.Config != nil {
		return prefixOAuthScopes(np.Config.OauthScopes)
	}
	return nil
}

func (o *oauthScopes) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (o *oauthScopes) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig != nil {
		return prefixOAuthScopes(spec.NodePoolConfig.OAuthScopes)
	}
	return nil
}

func (o *oauthScopes) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (o *oauthScopes) ToNodePoolLabels(_ map[string]string, _ Metadata) {}

func (o *oauthScopes) ToNodepool(np *container.NodePool, m Metadata) {
	if np == nil {
		return
	}
	var scopes []string
	for k, v := range m {
		if strings.HasPrefix(k, oauthScopesPrefix) && v == "true" {
			scope := strings.TrimPrefix(k, oauthScopesPrefix)
			if scope != "" {
				scopes = append(scopes, scope)
			}
		}
	}
	if len(scopes) == 0 {
		return
	}
	sort.Strings(scopes)
	if np.Config == nil {
		np.Config = &container.NodeConfig{}
	}
	np.Config.OauthScopes = scopes
}
