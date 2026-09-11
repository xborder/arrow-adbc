// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package flightsql

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPollToCompletionUsesOneOperationDeadline(t *testing.T) {
	connection := &connectionImpl{}
	connection.pollCapabilities.enabled.Store(true)
	continuation := &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("next")}
	polls := 0
	poll := func(ctx context.Context, retry *flight.FlightDescriptor, _ ...grpc.CallOption) (*flight.PollInfo, error) {
		polls++
		if polls == 1 {
			require.Nil(t, retry)
			time.Sleep(60 * time.Millisecond)
			return &flight.PollInfo{Info: &flight.FlightInfo{}, FlightDescriptor: continuation}, nil
		}
		require.Same(t, continuation, retry)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	gets := 0
	get := func(context.Context, ...grpc.CallOption) (*flight.FlightInfo, error) {
		gets++
		return &flight.FlightInfo{}, nil
	}

	started := time.Now()
	_, err := connection.pollToCompletion(context.Background(), pollFamilyStatementSQL, 100*time.Millisecond, poll, get, nil)
	elapsed := time.Since(started)
	require.Error(t, err)
	require.Equal(t, 2, polls)
	require.Zero(t, gets)
	// A per-poll deadline reset would take roughly 160ms. Leave enough slack
	// for loaded CI while still distinguishing the one-operation deadline.
	require.Less(t, elapsed, 145*time.Millisecond)
}

func TestValidateIncrementalFlightInfoRequiresAppendOnlyEndpoints(t *testing.T) {
	first := &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: []byte("first")}}
	second := &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: []byte("second")}}
	previous := &flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{first}}

	require.NoError(t, validateIncrementalFlightInfo(previous,
		&flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{proto.Clone(first).(*flight.FlightEndpoint), second}}))
	err := validateIncrementalFlightInfo(previous,
		&flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{second}})
	require.Equal(t, codes.Internal, status.Code(err))
	require.ErrorContains(t, err, "mutated previously published endpoint 0")
}
