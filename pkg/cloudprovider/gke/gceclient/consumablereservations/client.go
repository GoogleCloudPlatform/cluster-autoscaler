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

package consumablereservations

import (
	"context"

	gce_api "google.golang.org/api/compute/v1"
)

// ErrorType represents the type of ConsumableReservationError.
type ErrorType string

const (
	// ClientError represents a client-side error (e.g., HTTP 4xx).
	ClientError ErrorType = "client"
	// InternalError represents a processing error (e.g., decoding failures).
	InternalError ErrorType = "internal"
)

// Error represents an error encountered by the consumable reservations client.
type Error interface {
	error
	Unwrap() error
	Type() ErrorType
}

type clientError struct {
	err     error
	errType ErrorType
}

func (e *clientError) Error() string {
	return e.err.Error()
}

func (e *clientError) Unwrap() error {
	return e.err
}

func (e *clientError) Type() ErrorType {
	return e.errType
}

func NewError(e error, t ErrorType) Error {
	return &clientError{
		err:     e,
		errType: t,
	}
}

// Client is used to fetch the consumable reservations
type Client interface {
	// FetchConsumableReservations fetches the consumable reservations in the provided project and zone.
	FetchConsumableReservations(ctx context.Context, projectID, zone string) ([]*gce_api.Reservation, Error)
}

type noOpClient struct{}

func (*noOpClient) FetchConsumableReservations(_ context.Context, _, _ string) ([]*gce_api.Reservation, Error) {
	return nil, nil
}

// NewNoOpClient returns new no-op client
func NewNoOpClient() *noOpClient {
	return &noOpClient{}
}
