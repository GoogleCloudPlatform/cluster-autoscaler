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

package resourcemanager

import (
	"context"
	"fmt"
	"strings"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	rm "cloud.google.com/go/resourcemanager/apiv3"
	rmpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/api/option"
)

var (
	// TagKeysPrefix is the common prefix of the resource manager tag key IDs.
	TagKeysPrefix = "tagKeys/"
	// TagValuesPrefix is the common prefix of the resource manager tag value IDs.
	TagValuesPrefix = "tagValues/"
	// TagsRequiredIamPermissions is a set of required IAM permissions on
	// the tag value to attach it to a GKE node.
	TagsRequiredIamPermissions = []string{
		"resourcemanager.tagHolds.create",
		"resourcemanager.tagHolds.delete",
		"resourcemanager.tagHolds.list",
		"resourcemanager.tagValueBindings.create",
		"resourcemanager.tagValueBindings.delete",
		"resourcemanager.tagValueBindings.list",
	}
)

// TagClient is used for communicating with Resource Manager Tag API.
type TagClient interface {
	GetTagKey(ctx context.Context, name string) (*rmpb.TagKey, error)
	GetTagValue(ctx context.Context, name string) (*rmpb.TagValue, error)
	ValidateIamPermissions(ctx context.Context, tagValue string) (*iampb.TestIamPermissionsResponse, error)
	Close() error
}

type tagClientImpl struct {
	tagKeysClient   *rm.TagKeysClient
	tagValuesClient *rm.TagValuesClient
}

// NewTagClient creates a new client for interfacing with Resource Manager API.
// The returned client must call Close() when it is no longer required.
func NewTagClient(ctx context.Context, opts ...option.ClientOption) (TagClient, error) {
	tagKeysClient, err := rm.NewTagKeysClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create TagKeysClient: %w", err)
	}

	tagValuesClient, err := rm.NewTagValuesClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create TagValuesClient: %w", err)
	}

	return &tagClientImpl{
		tagKeysClient:   tagKeysClient,
		tagValuesClient: tagValuesClient,
	}, nil
}

// GetTagKey gets a TagKey by tag key ID, short name, or namespaced name.
func (c *tagClientImpl) GetTagKey(ctx context.Context, name string) (*rmpb.TagKey, error) {
	if strings.HasPrefix(name, TagKeysPrefix) {
		req := &rmpb.GetTagKeyRequest{Name: name}
		return c.tagKeysClient.GetTagKey(ctx, req)
	}
	req := &rmpb.GetNamespacedTagKeyRequest{Name: name}
	return c.tagKeysClient.GetNamespacedTagKey(ctx, req)
}

// GetTagValue gets a TagValue by tag value ID, or namespaced name.
func (c *tagClientImpl) GetTagValue(ctx context.Context, name string) (*rmpb.TagValue, error) {
	if strings.HasPrefix(name, TagValuesPrefix) {
		req := &rmpb.GetTagValueRequest{Name: name}
		return c.tagValuesClient.GetTagValue(ctx, req)
	}
	req := &rmpb.GetNamespacedTagValueRequest{Name: name}
	return c.tagValuesClient.GetNamespacedTagValue(ctx, req)
}

// ValidateIamPermissions validates iam permissions for a tag value string.
// Must be in the format of `tagValues/{tag_value_id}`.
func (c *tagClientImpl) ValidateIamPermissions(ctx context.Context, tagValue string) (*iampb.TestIamPermissionsResponse, error) {
	if !strings.HasPrefix(tagValue, TagValuesPrefix) {
		return nil, fmt.Errorf("want tag value format 'tagValues/{tag_value_id}', got '%s'", tagValue)
	}
	req := &iampb.TestIamPermissionsRequest{
		Resource:    tagValue,
		Permissions: TagsRequiredIamPermissions,
	}
	return c.tagValuesClient.TestIamPermissions(ctx, req)
}

// Close underlying gRPC connections to the tag client services.
func (c *tagClientImpl) Close() error {
	// Both tagKeysClient and tagValuesClient were created with the same options,
	// so they share the same underlying gRPC connection pool. Calling Close() on
	// the second client would be redundant and return a "connection is closing" error.
	return c.tagKeysClient.Close()
}
