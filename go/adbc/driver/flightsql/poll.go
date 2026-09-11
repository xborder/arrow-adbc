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
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	flightproto "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type pollCommandFamily string

const (
	pollFamilyStatementSQL       pollCommandFamily = "statement-sql"
	pollFamilyStatementSubstrait pollCommandFamily = "statement-substrait"
	pollFamilyPrepared           pollCommandFamily = "prepared-statement"
	pollFamilyCatalogs           pollCommandFamily = "metadata-catalogs"
	pollFamilyDBSchemas          pollCommandFamily = "metadata-db-schemas"
	pollFamilyTables             pollCommandFamily = "metadata-tables"
	pollFamilyTableTypes         pollCommandFamily = "metadata-table-types"
	pollFamilySQLInfo            pollCommandFamily = "metadata-sql-info"
)

type pollCapabilityCache struct {
	enabled atomic.Bool
	mu      sync.RWMutex
	// unsupported contains only families for which the initial poll returned
	// UNIMPLEMENTED. Other failures must never affect future routing.
	unsupported map[pollCommandFamily]struct{}
}

func (p *pollCapabilityCache) shouldPoll(family pollCommandFamily) bool {
	if !p.enabled.Load() {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, unsupported := p.unsupported[family]
	return !unsupported
}

func (p *pollCapabilityCache) markUnsupported(family pollCommandFamily) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unsupported == nil {
		p.unsupported = make(map[pollCommandFamily]struct{})
	}
	p.unsupported[family] = struct{}{}
}

type pollCall func(context.Context, *flight.FlightDescriptor, ...grpc.CallOption) (*flight.PollInfo, error)
type getFlightInfoCall func(context.Context, ...grpc.CallOption) (*flight.FlightInfo, error)

// pollToCompletion synchronously drains PollFlightInfo. PollInfo.info is
// cumulative, so only the final FlightInfo is returned to the existing result
// path. The original descriptor is submitted only by the first call; every
// later call receives exactly the preceding continuation descriptor.
func (c *connectionImpl) pollToCompletion(
	ctx context.Context,
	family pollCommandFamily,
	queryTimeout time.Duration,
	poll pollCall,
	get getFlightInfoCall,
	getAfterInitialPoll getFlightInfoCall,
	opts ...grpc.CallOption,
) (*flight.FlightInfo, error) {
	if !c.pollCapabilities.shouldPoll(family) {
		return get(ctx, opts...)
	}

	pollCtx := ctx
	cancel := func() {}
	if queryTimeout > 0 {
		pollCtx, cancel = context.WithTimeout(ctx, queryTimeout)
	}
	defer cancel()

	var (
		retryDescriptor *flight.FlightDescriptor
		lastInfo        *flight.FlightInfo
	)
	for initial := true; ; initial = false {
		pollInfo, err := poll(pollCtx, retryDescriptor, opts...)
		if err != nil {
			if initial && status.Code(err) == codes.Unimplemented {
				c.pollCapabilities.markUnsupported(family)
				if getAfterInitialPoll != nil {
					return getAfterInitialPoll(pollCtx, opts...)
				}
				return get(pollCtx, opts...)
			}
			if lastInfo != nil && pollCtx.Err() != nil {
				c.cancelFlightInfoBestEffort(ctx, lastInfo)
			}
			return nil, err
		}
		if pollInfo == nil || pollInfo.GetInfo() == nil {
			return nil, status.Error(codes.Internal, "server returned PollInfo without cumulative FlightInfo")
		}

		lastInfo = pollInfo.GetInfo()
		retryDescriptor = pollInfo.GetFlightDescriptor()
		if retryDescriptor == nil {
			return lastInfo, nil
		}
	}
}

func descriptorForCommand(command proto.Message) (*flight.FlightDescriptor, error) {
	var packed anypb.Any
	if err := packed.MarshalFrom(command); err != nil {
		return nil, err
	}
	data, err := proto.Marshal(&packed)
	if err != nil {
		return nil, err
	}
	return &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: data}, nil
}

// pollCommand adapts metadata commands, for which Arrow Go currently exposes
// GetFlightInfo helpers but no typed PollFlightInfo helpers.
func (c *connectionImpl) pollCommand(
	ctx context.Context,
	family pollCommandFamily,
	command proto.Message,
	get getFlightInfoCall,
	opts ...grpc.CallOption,
) (*flight.FlightInfo, error) {
	original, err := descriptorForCommand(command)
	if err != nil {
		return nil, err
	}
	poll := func(ctx context.Context, retry *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error) {
		if retry == nil {
			retry = original
		}
		return c.cl.Client.PollFlightInfo(ctx, retry, opts...)
	}
	return c.pollToCompletion(ctx, family, c.timeouts.queryTimeout, poll, get, nil, opts...)
}

// cancelFlightInfoBestEffort does not delay returning cancellation to the
// caller. The detached attempt is bounded and uses the request metadata from
// the operation that was cancelled.
func (c *connectionImpl) cancelFlightInfoBestEffort(ctx context.Context, info *flight.FlightInfo) {
	client := c.cl
	if client == nil {
		return
	}
	info = proto.Clone(info).(*flight.FlightInfo)
	requestMD, _ := metadata.FromOutgoingContext(ctx)
	requestMD = requestMD.Copy()
	go func() {
		cancelCtx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), requestMD), time.Second)
		defer cancel()
		_, _ = client.CancelFlightInfo(cancelCtx, &flightproto.CancelFlightInfoRequest{Info: info})
	}()
}
