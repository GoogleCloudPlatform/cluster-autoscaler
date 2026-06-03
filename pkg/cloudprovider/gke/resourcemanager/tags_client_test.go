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
	"errors"
	"net"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	rmpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"
)

var (
	projectId = "test-project"
)

func TestGetTagKey(t *testing.T) {
	ctx := context.Background()
	expectedTagKey := &rmpb.TagKey{
		Name:           "tagKeys/123",
		Parent:         "organizations/456",
		ShortName:      "key1",
		NamespacedName: projectId + "/key1",
		Purpose:        rmpb.Purpose_GCE_FIREWALL,
		PurposeData: map[string]string{
			"network": "default",
		},
	}

	tests := []struct {
		name       string
		queryKey   string
		setupMocks func(mockKeys *mockTagKeysServer)
		wantTagKey *rmpb.TagKey
		wantErr    bool
	}{
		{
			name:     "query by tag key ID",
			queryKey: "tagKeys/123",
			setupMocks: func(mockKeys *mockTagKeysServer) {
				mockKeys.On("GetTagKey", mock.Anything, &rmpb.GetTagKeyRequest{Name: "tagKeys/123"}).Return(expectedTagKey, nil).Once()
			},
			wantTagKey: expectedTagKey,
		},
		{
			name:     "query by tag key shortname",
			queryKey: "key1",
			setupMocks: func(mockKeys *mockTagKeysServer) {
				mockKeys.On(
					"GetNamespacedTagKey",
					mock.Anything,
					&rmpb.GetNamespacedTagKeyRequest{Name: "key1"},
				).Return(expectedTagKey, nil).Once()
			},
			wantTagKey: expectedTagKey,
		},
		{
			name:     "query by tag key namespaced name",
			queryKey: projectId + "/key1",
			setupMocks: func(mockKeys *mockTagKeysServer) {
				mockKeys.On(
					"GetNamespacedTagKey",
					mock.Anything,
					&rmpb.GetNamespacedTagKeyRequest{Name: projectId + "/key1"},
				).Return(expectedTagKey, nil).Once()
			},
			wantTagKey: expectedTagKey,
		},
		{
			name:     "missing tag key",
			queryKey: "tagKeys/456",
			setupMocks: func(mockKeys *mockTagKeysServer) {
				mockKeys.On(
					"GetTagKey",
					mock.Anything,
					&rmpb.GetTagKeyRequest{Name: "tagKeys/456"},
				).Return(nil, errors.New("not found")).Once()
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockKeys := &mockTagKeysServer{}
			mockValues := &mockTagValuesServer{}
			client, cleanup := setupTestTagClient(ctx, t, mockKeys, mockValues)
			defer cleanup()

			tc.setupMocks(mockKeys)

			res, err := client.GetTagKey(ctx, tc.queryKey)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Use cmp.Diff with protocmp.Transform() to ignore internal proto fields
				if diff := cmp.Diff(tc.wantTagKey, res, protocmp.Transform()); diff != "" {
					t.Errorf("GetTagKey() diff (-want +got):\n%s", diff)
				}
			}
			mockKeys.AssertExpectations(t)
		})
	}
}

func TestGetTagValue(t *testing.T) {
	ctx := context.Background()
	expectedTagValue := &rmpb.TagValue{
		Name:           "tagValues/123",
		Parent:         "tagKeys/456",
		ShortName:      "val1",
		NamespacedName: projectId + "/key1/val1",
	}

	tests := []struct {
		name         string
		queryValue   string
		setupMocks   func(mockValues *mockTagValuesServer)
		wantTagValue *rmpb.TagValue
		wantErr      bool
	}{
		{
			name:       "query by tag value ID",
			queryValue: "tagValues/123",
			setupMocks: func(mockValues *mockTagValuesServer) {
				mockValues.On(
					"GetTagValue",
					mock.Anything,
					&rmpb.GetTagValueRequest{Name: "tagValues/123"},
				).Return(expectedTagValue, nil).Once()
			},
			wantTagValue: expectedTagValue,
		},
		{
			name:       "query by tag value shortname",
			queryValue: "val1",
			setupMocks: func(mockValues *mockTagValuesServer) {
				mockValues.On(
					"GetNamespacedTagValue",
					mock.Anything,
					&rmpb.GetNamespacedTagValueRequest{Name: "val1"},
				).Return(expectedTagValue, nil).Once()
			},
			wantTagValue: expectedTagValue,
		},
		{
			name:       "query by namespaced name",
			queryValue: projectId + "/key1/val1",
			setupMocks: func(mockValues *mockTagValuesServer) {
				mockValues.On(
					"GetNamespacedTagValue",
					mock.Anything,
					&rmpb.GetNamespacedTagValueRequest{Name: projectId + "/key1/val1"},
				).Return(expectedTagValue, nil).Once()
			},
			wantTagValue: expectedTagValue,
		},
		{
			name:       "missing tag value",
			queryValue: "tagValues/456",
			setupMocks: func(mockValues *mockTagValuesServer) {
				mockValues.On(
					"GetTagValue",
					mock.Anything,
					&rmpb.GetTagValueRequest{Name: "tagValues/456"},
				).Return(nil, errors.New("not found")).Once()
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockKeys := &mockTagKeysServer{}
			mockValues := &mockTagValuesServer{}
			client, cleanup := setupTestTagClient(ctx, t, mockKeys, mockValues)
			defer cleanup()

			tc.setupMocks(mockValues)

			res, err := client.GetTagValue(ctx, tc.queryValue)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if diff := cmp.Diff(tc.wantTagValue, res, protocmp.Transform()); diff != "" {
					t.Errorf("GetTagValue() diff (-want +got):\n%s", diff)
				}
			}
			mockValues.AssertExpectations(t)
		})
	}
}

func TestValidateIamPermissions(t *testing.T) {
	ctx := context.Background()
	expectedPerms := &iampb.TestIamPermissionsResponse{Permissions: TagsRequiredIamPermissions}

	tests := []struct {
		name       string
		tagValueId string
		setupMocks func(mockValues *mockTagValuesServer)
		wantErr    bool
	}{
		{
			name:       "query by tag value ID",
			tagValueId: "tagValues/123",
			setupMocks: func(mockValues *mockTagValuesServer) {
				mockValues.On(
					"TestIamPermissions",
					mock.Anything,
					&iampb.TestIamPermissionsRequest{
						Resource:    "tagValues/123",
						Permissions: TagsRequiredIamPermissions,
					},
				).Return(expectedPerms, nil).Once()
			},
		},
		{
			name:       "missing tag value",
			tagValueId: "tagValues/456",
			setupMocks: func(mockValues *mockTagValuesServer) {
				mockValues.On(
					"TestIamPermissions",
					mock.Anything,
					&iampb.TestIamPermissionsRequest{
						Resource:    "tagValues/456",
						Permissions: TagsRequiredIamPermissions,
					},
				).Return(nil, errors.New("not found")).Once()
			},
			wantErr: true,
		},
		{
			name:       "invalid query",
			tagValueId: "invalid/123",
			setupMocks: func(mockValues *mockTagValuesServer) {},
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockKeys := &mockTagKeysServer{}
			mockValues := &mockTagValuesServer{}
			client, cleanup := setupTestTagClient(ctx, t, mockKeys, mockValues)
			defer cleanup()

			tc.setupMocks(mockValues)

			resp, err := client.ValidateIamPermissions(ctx, tc.tagValueId)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if diff := cmp.Diff(expectedPerms, resp, protocmp.Transform()); diff != "" {
					t.Errorf("ValidateIamPermissions() diff (-want +got):\n%s", diff)
				}
			}
			mockKeys.AssertExpectations(t)
			mockValues.AssertExpectations(t)
		})
	}
}

// mockTagKeysServer is a mock for the TagKeys gRPC server.
type mockTagKeysServer struct {
	rmpb.UnimplementedTagKeysServer
	mock.Mock
}

func (s *mockTagKeysServer) GetTagKey(ctx context.Context, req *rmpb.GetTagKeyRequest) (*rmpb.TagKey, error) {
	args := s.Called(ctx, req)
	if key, ok := args.Get(0).(*rmpb.TagKey); ok {
		return key, args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *mockTagKeysServer) GetNamespacedTagKey(ctx context.Context, req *rmpb.GetNamespacedTagKeyRequest) (*rmpb.TagKey, error) {
	args := s.Called(ctx, req)
	if key, ok := args.Get(0).(*rmpb.TagKey); ok {
		return key, args.Error(1)
	}
	return nil, args.Error(1)
}

// mockTagValuesServer is a mock for the TagValues gRPC server.
type mockTagValuesServer struct {
	rmpb.UnimplementedTagValuesServer
	mock.Mock
}

func (s *mockTagValuesServer) GetTagValue(ctx context.Context, req *rmpb.GetTagValueRequest) (*rmpb.TagValue, error) {
	args := s.Called(ctx, req)
	if val, ok := args.Get(0).(*rmpb.TagValue); ok {
		return val, args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *mockTagValuesServer) GetNamespacedTagValue(ctx context.Context, req *rmpb.GetNamespacedTagValueRequest) (*rmpb.TagValue, error) {
	args := s.Called(ctx, req)
	if val, ok := args.Get(0).(*rmpb.TagValue); ok {
		return val, args.Error(1)
	}
	return nil, args.Error(1)
}

func (s *mockTagValuesServer) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	args := s.Called(ctx, req)
	if resp, ok := args.Get(0).(*iampb.TestIamPermissionsResponse); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func setupTestTagClient(ctx context.Context, t *testing.T, keyServer rmpb.TagKeysServer, valueServer rmpb.TagValuesServer) (TagClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()

	rmpb.RegisterTagKeysServer(srv, keyServer)
	rmpb.RegisterTagValuesServer(srv, valueServer)

	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Server exited with error: %v", err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to create gRPC client: %v", err)
	}

	client, err := NewTagClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create TagClient: %v", err)
	}

	cleanup := func() {
		srv.Stop()
		err := client.Close()
		if err != nil {
			t.Errorf("Failed to close client connections: %v", err)
		}
	}

	return client, cleanup
}
