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

package gceclient

import (
	"net/http"
	"regexp"
)

const quotaRegionHeader = "X-Compute-API-Quota-Region-Override"

var (
	zonalResourceRegex    = regexp.MustCompile(`/projects/[^/]+/zones/[^/]+/[^/]+`)
	regionalResourceRegex = regexp.MustCompile(`/projects/[^/]+/regions/[^/]+/[^/]+`)
)

func isGlobalGceApiRequest(path string) bool {
	if zonalResourceRegex.MatchString(path) || regionalResourceRegex.MatchString(path) {
		return false
	}
	return true
}

type globalGceApiHeaderRoundTripper struct {
	delegate http.RoundTripper
	region   string
}

func (rt *globalGceApiHeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.region != "" && isGlobalGceApiRequest(req.URL.Path) {
		if req.Header.Get(quotaRegionHeader) == "" {
			req = req.Clone(req.Context())
			req.Header.Set(quotaRegionHeader, rt.region)
		}
	}
	delegate := rt.delegate
	if delegate == nil {
		delegate = http.DefaultTransport
	}
	return delegate.RoundTrip(req)
}
