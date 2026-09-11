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

package flightsql_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type conformanceCounters struct {
	PollFlightInfo          int `json:"poll_flight_info"`
	GetFlightInfo           int `json:"get_flight_info"`
	ParameterBinding        int `json:"parameter_binding"`
	OriginalDescriptors     int `json:"original_descriptors"`
	ContinuationDescriptors int `json:"continuation_descriptors"`
	Cancellation            int `json:"cancellation"`
	ActiveCallTerminations  int `json:"active_call_terminations"`
	DoGet                   int `json:"do_get"`
}

type conformanceSnapshot struct {
	Counters          conformanceCounters            `json:"counters"`
	CountersByFamily  map[string]conformanceCounters `json:"counters_by_family"`
	ContinuationOrder []string                       `json:"continuation_order"`
	Events            []conformanceEvent             `json:"events"`
}

type conformanceEvent struct {
	Method string `json:"method"`
}

type conformanceHarness struct {
	t       *testing.T
	uri     string
	control string
}

func newConformanceHarness(t *testing.T) *conformanceHarness {
	uri := os.Getenv("ADBC_POLL_CONFORMANCE_URI")
	control := os.Getenv("ADBC_POLL_CONFORMANCE_CONTROL")
	if uri == "" || control == "" {
		t.Skip("set ADBC_POLL_CONFORMANCE_URI and ADBC_POLL_CONFORMANCE_CONTROL to run shared-server tests")
	}
	return &conformanceHarness{t: t, uri: uri, control: control}
}

func (h *conformanceHarness) open(options map[string]string) (adbc.Database, adbc.Connection) {
	h.t.Helper()
	db, cnxn := h.connect(options)
	h.reset()
	return db, cnxn
}

func (h *conformanceHarness) connect(options map[string]string) (adbc.Database, adbc.Connection) {
	h.t.Helper()
	opts := map[string]string{adbc.OptionKeyURI: h.uri}
	for key, value := range options {
		opts[key] = value
	}
	db, err := driver.NewDriver(memory.DefaultAllocator).NewDatabase(opts)
	require.NoError(h.t, err)
	cnxn, err := db.Open(context.Background())
	require.NoError(h.t, err)
	return db, cnxn
}

func (h *conformanceHarness) reset() {
	h.t.Helper()
	request, err := http.NewRequest(http.MethodPost, h.control+"/reset", nil)
	require.NoError(h.t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(h.t, err)
	defer response.Body.Close()
	require.Equal(h.t, http.StatusNoContent, response.StatusCode)
}

func (h *conformanceHarness) configure(family, scenario string) {
	h.t.Helper()
	body := fmt.Sprintf(`{"family":%q,"scenario":%q}`, family, scenario)
	request, err := http.NewRequest(http.MethodPost, h.control+"/control", strings.NewReader(body))
	require.NoError(h.t, err)
	request.Header.Set("content-type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(h.t, err)
	defer response.Body.Close()
	require.Equal(h.t, http.StatusNoContent, response.StatusCode)
}

func (h *conformanceHarness) snapshot() conformanceSnapshot {
	h.t.Helper()
	response, err := http.Get(h.control + "/state")
	require.NoError(h.t, err)
	defer response.Body.Close()
	require.Equal(h.t, http.StatusOK, response.StatusCode)
	var snapshot conformanceSnapshot
	require.NoError(h.t, json.NewDecoder(response.Body).Decode(&snapshot))
	h.t.Logf("shared server state: %+v", snapshot)
	return snapshot
}

func closeConformance(t *testing.T, db adbc.Database, cnxn adbc.Connection) {
	t.Helper()
	require.NoError(t, cnxn.Close())
	require.NoError(t, db.Close())
}

func executeConformanceQuery(t *testing.T, cnxn adbc.Connection, ctx context.Context, query string) ([]string, []int64, error) {
	t.Helper()
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return nil, nil, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		return nil, nil, err
	}
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer rdr.Release()
	var scenarios []string
	var values []int64
	for rdr.Next() {
		record := rdr.RecordBatch()
		names := record.Column(0).(*array.String)
		numbers := record.Column(1).(*array.Int64)
		for i := 0; i < int(record.NumRows()); i++ {
			scenarios = append(scenarios, names.Value(i))
			values = append(values, numbers.Value(i))
		}
	}
	return scenarios, values, rdr.Err()
}

func readConformancePartition(t *testing.T, cnxn adbc.Connection, partition []byte) ([]string, []int64) {
	t.Helper()
	rdr, err := cnxn.ReadPartition(context.Background(), partition)
	require.NoError(t, err)
	defer rdr.Release()
	var scenarios []string
	var values []int64
	for rdr.Next() {
		record := rdr.RecordBatch()
		names := record.Column(0).(*array.String)
		numbers := record.Column(1).(*array.Int64)
		for i := 0; i < int(record.NumRows()); i++ {
			scenarios = append(scenarios, names.Value(i))
			values = append(values, numbers.Value(i))
		}
	}
	require.NoError(t, rdr.Err())
	return scenarios, values
}

func pollAndGetEvents(snapshot conformanceSnapshot) []string {
	var events []string
	for _, event := range snapshot.Events {
		if event.Method == "PollFlightInfo" || event.Method == "DoGet" {
			events = append(events, event.Method)
		}
	}
	return events
}

func TestPollInfoSharedConformance(t *testing.T) {
	h := newConformanceHarness(t)

	t.Run("DatabaseOpenTransactionProbe", func(t *testing.T) {
		h.reset()
		db, cnxn := h.connect(nil)
		defer closeConformance(t, db, cnxn)
		state := h.snapshot()
		require.Equal(t, 3, state.CountersByFamily["metadata"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["metadata"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["metadata"].OriginalDescriptors)
	})

	t.Run("T1Immediate", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		scenarios, values, err := executeConformanceQuery(t, cnxn, context.Background(), "immediate")
		require.NoError(t, err)
		require.Equal(t, []string{"immediate", "immediate"}, scenarios)
		require.Equal(t, []int64{1, 2}, values)
		state := h.snapshot()
		require.Equal(t, 1, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["direct"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["direct"].OriginalDescriptors)
	})

	t.Run("T2IncrementalPartitionsConsumeBeforeNextPoll", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		defer stmt.Close()
		require.NoError(t, stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))
		require.NoError(t, stmt.SetSqlQuery("multi-step"))
		for expected := int64(1); expected <= 3; expected++ {
			_, partitions, _, err := stmt.ExecutePartitions(context.Background())
			require.NoError(t, err)
			require.Equal(t, uint64(1), partitions.NumPartitions)
			scenarios, values := readConformancePartition(t, cnxn, partitions.PartitionIDs[0])
			require.Equal(t, []string{"multi-step"}, scenarios)
			require.Equal(t, []int64{expected}, values)
		}
		_, partitions, _, err := stmt.ExecutePartitions(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(0), partitions.NumPartitions)
		state := h.snapshot()
		require.Equal(t, 3, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["direct"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["direct"].OriginalDescriptors)
		require.Equal(t, 2, state.CountersByFamily["direct"].ContinuationDescriptors)
		require.Len(t, state.ContinuationOrder, 2)
		require.NotEqual(t, state.ContinuationOrder[0], state.ContinuationOrder[1])
		require.Equal(t,
			[]string{"PollFlightInfo", "DoGet", "PollFlightInfo", "DoGet", "PollFlightInfo", "DoGet"},
			pollAndGetEvents(state))
	})

	t.Run("T2ExecuteQueryRemainsFinalOnly", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		_, values, err := executeConformanceQuery(t, cnxn, context.Background(), "multi-step")
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3}, values)
		state := h.snapshot()
		require.Equal(t,
			[]string{"PollFlightInfo", "PollFlightInfo", "PollFlightInfo", "DoGet", "DoGet", "DoGet"},
			pollAndGetEvents(state))
	})

	t.Run("T3Prepared", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		defer stmt.Close()
		require.NoError(t, stmt.SetSqlQuery("prepared-multi-step"))
		require.NoError(t, stmt.Prepare(context.Background()))
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		builder.Field(0).(*array.Int64Builder).Append(41)
		record := builder.NewRecordBatch()
		builder.Release()
		defer record.Release()
		require.NoError(t, stmt.Bind(context.Background(), record))
		rdr, _, err := stmt.ExecuteQuery(context.Background())
		require.NoError(t, err)
		defer rdr.Release()
		var values []int64
		for rdr.Next() {
			column := rdr.RecordBatch().Column(1).(*array.Int64)
			for i := 0; i < column.Len(); i++ {
				values = append(values, column.Value(i))
			}
		}
		require.NoError(t, rdr.Err())
		require.Equal(t, []int64{41, 42}, values)
		state := h.snapshot()
		require.Equal(t, 1, state.CountersByFamily["prepared"].ParameterBinding)
		require.Equal(t, 3, state.CountersByFamily["prepared"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["prepared"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["prepared"].OriginalDescriptors)
	})

	t.Run("T3PreparedFallbackBindsExactlyOnce", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		h.configure("prepared", "unimplemented")
		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		defer stmt.Close()
		require.NoError(t, stmt.SetSqlQuery("prepared-multi-step"))
		require.NoError(t, stmt.Prepare(context.Background()))
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		builder.Field(0).(*array.Int64Builder).Append(7)
		record := builder.NewRecordBatch()
		builder.Release()
		defer record.Release()
		require.NoError(t, stmt.Bind(context.Background(), record))
		rdr, _, err := stmt.ExecuteQuery(context.Background())
		require.NoError(t, err)
		defer rdr.Release()
		var values []int64
		for rdr.Next() {
			column := rdr.RecordBatch().Column(1).(*array.Int64)
			for i := 0; i < column.Len(); i++ {
				values = append(values, column.Value(i))
			}
		}
		require.NoError(t, rdr.Err())
		require.Equal(t, []int64{7, 8}, values)
		state := h.snapshot()
		require.Equal(t, 1, state.CountersByFamily["prepared"].ParameterBinding)
		require.Equal(t, 1, state.CountersByFamily["prepared"].PollFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["prepared"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["prepared"].OriginalDescriptors)
	})

	t.Run("T4Metadata", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		rdr, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		defer rdr.Release()
		var catalogs []string
		for rdr.Next() {
			column := rdr.RecordBatch().Column(0).(*array.String)
			for i := 0; i < column.Len(); i++ {
				catalogs = append(catalogs, column.Value(i))
			}
		}
		require.NoError(t, rdr.Err())
		require.Equal(t, []string{"bdx_catalog"}, catalogs)
		state := h.snapshot()
		require.Equal(t, 3, state.CountersByFamily["metadata"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["metadata"].GetFlightInfo)
	})

	t.Run("T5FallbackCacheAndFamilyIsolation", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		h.configure("direct", "unimplemented")
		for range 2 {
			_, values, err := executeConformanceQuery(t, cnxn, context.Background(), "unimplemented")
			require.NoError(t, err)
			require.Equal(t, []int64{1, 2}, values)
		}
		rdr, err := cnxn.GetObjects(context.Background(), adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		for rdr.Next() {
		}
		require.NoError(t, rdr.Err())
		rdr.Release()
		state := h.snapshot()
		require.Equal(t, 1, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 2, state.CountersByFamily["direct"].GetFlightInfo)
		require.Equal(t, 3, state.CountersByFamily["metadata"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["metadata"].GetFlightInfo)
	})

	t.Run("T6ConnectionOptOut", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		opts := cnxn.(adbc.GetSetOptions)
		require.NoError(t, opts.SetOption(driver.OptionUsePollFlightInfo, adbc.OptionValueDisabled))
		h.reset()
		_, values, err := executeConformanceQuery(t, cnxn, context.Background(), "immediate")
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2}, values)
		state := h.snapshot()
		require.Equal(t, 0, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["direct"].GetFlightInfo)
	})

	t.Run("T7UnavailableDoesNotFallback", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		_, _, err := executeConformanceQuery(t, cnxn, context.Background(), "unavailable")
		require.Error(t, err)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusIO, adbcErr.Code)
		state := h.snapshot()
		require.Equal(t, 1, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["direct"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["direct"].OriginalDescriptors)
	})

	t.Run("T7LateErrorAfterPublishedPartition", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		defer stmt.Close()
		require.NoError(t, stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))
		require.NoError(t, stmt.SetSqlQuery("late-error"))
		_, partitions, _, err := stmt.ExecutePartitions(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(1), partitions.NumPartitions)
		_, values := readConformancePartition(t, cnxn, partitions.PartitionIDs[0])
		require.Equal(t, []int64{1}, values)
		_, _, _, err = stmt.ExecutePartitions(context.Background())
		require.Error(t, err)
		state := h.snapshot()
		require.Equal(t, 2, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["direct"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["direct"].DoGet)
	})

	t.Run("T8OperationWideTimeout", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		defer stmt.Close()
		require.NoError(t, stmt.SetSqlQuery("blocked-poll"))
		require.NoError(t, stmt.(adbc.GetSetOptions).SetOptionDouble(driver.OptionTimeoutQuery, 0.2))
		started := time.Now()
		_, _, err = stmt.ExecuteQuery(context.Background())
		elapsed := time.Since(started)
		require.Error(t, err)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusTimeout, adbcErr.Code)
		require.Less(t, elapsed, time.Second)
		state := h.snapshot()
		require.Equal(t, 1, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["direct"].GetFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["direct"].ActiveCallTerminations)
	})

	t.Run("T9IncrementalContextCancellationAndBestEffortCancel", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		defer stmt.Close()
		require.NoError(t, stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))
		require.NoError(t, stmt.SetSqlQuery("cancel-observable"))
		_, partitions, _, err := stmt.ExecutePartitions(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(1), partitions.NumPartitions)
		_, values := readConformancePartition(t, cnxn, partitions.PartitionIDs[0])
		require.Equal(t, []int64{1, 2}, values)

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(200*time.Millisecond, cancel)
		started := time.Now()
		_, _, _, err = stmt.ExecutePartitions(ctx)
		require.Error(t, err)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
		require.Less(t, time.Since(started), time.Second)

		deadline := time.Now().Add(2 * time.Second)
		var state conformanceSnapshot
		for time.Now().Before(deadline) {
			state = h.snapshot()
			if state.Counters.Cancellation > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.Equal(t, 1, state.CountersByFamily["direct"].OriginalDescriptors)
		require.Equal(t, 2, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 0, state.CountersByFamily["direct"].GetFlightInfo)
		require.Equal(t, 1, state.Counters.Cancellation)
		require.Equal(t, 1, state.CountersByFamily["direct"].ActiveCallTerminations)
	})

	t.Run("T9IncrementalStatementCloseAttemptsCancellation", func(t *testing.T) {
		db, cnxn := h.open(nil)
		defer closeConformance(t, db, cnxn)
		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		require.NoError(t, stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))
		require.NoError(t, stmt.SetSqlQuery("cancel-observable"))
		_, partitions, _, err := stmt.ExecutePartitions(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(1), partitions.NumPartitions)
		_, values := readConformancePartition(t, cnxn, partitions.PartitionIDs[0])
		require.Equal(t, []int64{1, 2}, values)
		require.NoError(t, stmt.Close())

		deadline := time.Now().Add(2 * time.Second)
		var state conformanceSnapshot
		for time.Now().Before(deadline) {
			state = h.snapshot()
			if state.Counters.Cancellation > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.Equal(t, 1, state.Counters.Cancellation)
		require.Equal(t, 1, state.CountersByFamily["direct"].PollFlightInfo)
		require.Equal(t, 1, state.CountersByFamily["direct"].DoGet)
	})
}
