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

package multitenancy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
)

const (
	// Max QPS to allow through to the token URL.
	tokenURLQPS = .05 // back off to once every 20 seconds when failing
	// Maximum burst of requests to token URL before limiting.
	tokenURLBurst = 3
)

type tenantTokenSource struct {
	authConfig           *AuthConfig
	clusterProjectNumber int64
	tenantProjectNumber  int64
	tokenURL             string
	httpClient           *http.Client
	throttle             flowcontrol.RateLimiter
}

type tokenResponse struct {
	AccessToken      string `json:"access_token"`
	AccessTokenCamel string `json:"accessToken"`
	ExpiresIn        int    `json:"expires_in"`
	ExpiresInCamel   int    `json:"expiresIn"`
	ExpireTime       string `json:"expireTime"`
	TokenType        string `json:"token_type"`
	TokenTypeCamel   string `json:"tokenType"`
}

// NewTenantTokenSource creates a new oauth2.TokenSource for a tenant.
func NewTenantTokenSource(authConfig *AuthConfig, clusterProjectNumber, tenantProjectNumber int64) oauth2.TokenSource {
	client := oauth2.NewClient(context.Background(), google.ComputeTokenSource(""))
	ts := &tenantTokenSource{
		authConfig:           authConfig,
		clusterProjectNumber: clusterProjectNumber,
		tenantProjectNumber:  tenantProjectNumber,
		httpClient:           client,
		throttle:             flowcontrol.NewTokenBucketRateLimiter(tokenURLQPS, tokenURLBurst),
	}
	// Pre-calculate the per-tenant-project URL if possible.
	if authConfig != nil && authConfig.TokenURL != "" {
		if transformedURL, err := ts.generateTenantProjectTokenURL(); err == nil {
			ts.tokenURL = transformedURL
		} else {
			klog.Warningf("Failed to generate per-tenant-project URL, falling back to original: %v", err)
			ts.tokenURL = authConfig.TokenURL
		}
	}
	return oauth2.ReuseTokenSource(nil, ts)
}

func (ts *tenantTokenSource) Token() (*oauth2.Token, error) {
	ts.throttle.Accept()

	if ts.authConfig == nil || ts.tokenURL == "" {
		return nil, fmt.Errorf("invalid AuthConfig for tenant token source")
	}

	tokenURL := ts.tokenURL
	u, err := url.Parse(tokenURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token URL %q: %v", tokenURL, err)
	}
	host := u.Hostname()
	if host != "googleapis.com" && !strings.HasSuffix(host, ".googleapis.com") {
		return nil, fmt.Errorf("token URL %q is not a trusted Google API domain", tokenURL)
	}

	klog.Infof("Fetching tenant token from URL: %s, Body: %s", tokenURL, ts.authConfig.TokenBody)

	req, err := http.NewRequest("POST", tokenURL, bytes.NewBufferString(ts.authConfig.TokenBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tenant token: %v", err)
	}
	defer resp.Body.Close()

	klog.Infof("Token request to %s returned status: %s", tokenURL, resp.Status)

	if err := googleapi.CheckResponse(resp); err != nil {
		return nil, fmt.Errorf("token request failed: %v", err)
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %v", err)
	}

	accessToken := tr.AccessToken
	if accessToken == "" {
		accessToken = tr.AccessTokenCamel
	}

	if accessToken == "" {
		return nil, fmt.Errorf("token response missing access_token from %s", tokenURL)
	}

	tokenType := tr.TokenType
	if tokenType == "" {
		tokenType = tr.TokenTypeCamel
	}

	token := &oauth2.Token{
		AccessToken: accessToken,
		TokenType:   tokenType,
	}

	expiresIn := tr.ExpiresIn
	if expiresIn == 0 {
		expiresIn = tr.ExpiresInCamel
	}

	if expiresIn > 0 {
		token.Expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
	} else if tr.ExpireTime != "" {
		if expiry, err := time.Parse(time.RFC3339, tr.ExpireTime); err == nil {
			token.Expiry = expiry
		}
	}

	klog.V(4).Infof("Successfully fetched new token for tenant from %s", tokenURL)
	return token, nil
}

func (ts *tenantTokenSource) generateTenantProjectTokenURL() (string, error) {
	u, err := url.Parse(ts.authConfig.TokenURL)
	if err != nil {
		return ts.authConfig.TokenURL, nil
	}

	// Expected path format: /v1/projects/{project}/locations/{location}/tenants/{tenant}:generateTenantToken
	// Splitting "/" results in ["", "v1", "projects", "{project}", "locations", "{location}", "tenants", "{tenant}:generateTenantToken"]
	parts := strings.Split(u.Path, "/")
	if len(parts) != 8 ||
		parts[1] != "v1" ||
		parts[2] != "projects" ||
		parts[4] != "locations" ||
		parts[6] != "tenants" ||
		!strings.HasSuffix(parts[7], ":generateTenantToken") {
		// If it doesn't match the per-tenant format, return the original URL as-is.
		return ts.authConfig.TokenURL, nil
	}

	baseURL := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	location := parts[5]

	var body struct {
		ClusterID string `json:"clusterId"`
	}
	if err := json.Unmarshal([]byte(ts.authConfig.TokenBody), &body); err != nil {
		return "", fmt.Errorf("failed to parse token body for per-tenant-project URL: %v", err)
	}

	if body.ClusterID == "" {
		// Fallback to original URL if body doesn't have required cluster info
		return ts.authConfig.TokenURL, nil
	}

	return fmt.Sprintf("%s/v1/projects/%d/locations/%s/clusters/%s/tenantProjects/%d:generateTenantProjectToken",
		baseURL, ts.clusterProjectNumber, location, body.ClusterID, ts.tenantProjectNumber), nil
}
