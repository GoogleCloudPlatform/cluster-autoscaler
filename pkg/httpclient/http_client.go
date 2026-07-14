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

package http_client

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/oauth2"
)

const (
	defaultHttpTimeout = 3 * time.Minute
)

func CreateHttpClient(tokenSource oauth2.TokenSource) *http.Client {
	httpClient := oauth2.NewClient(context.Background(), tokenSource)
	httpClient.Timeout = defaultHttpTimeout
	return httpClient
}

func CreateHttpClientWithTimeout(tokenSource oauth2.TokenSource, timeout time.Duration) *http.Client {
	httpClient := oauth2.NewClient(context.Background(), tokenSource)
	httpClient.Timeout = timeout
	return httpClient
}
