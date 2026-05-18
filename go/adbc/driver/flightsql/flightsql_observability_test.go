// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
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
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestFlightSQLObservabilityAsGoLibrary(t *testing.T) {
	sqliteDB, err := sql.Open("sqlite", "file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared")
	require.NoError(t, err)
	defer validation.CheckedClose(t, sqliteDB)

	quirks := &FlightSQLQuirks{db: sqliteDB}
	drv := quirks.SetupDriver(t)
	defer quirks.TearDownDriver(t, drv)

	logDir := t.TempDir()
	traceDir := t.TempDir()

	// The host process environment should not override explicit pre-init options.
	t.Setenv("ADBC_DRIVER_FLIGHTSQL_LOG_LEVEL", "error")
	t.Setenv("OTEL_TRACES_EXPORTER", "invalid-exporter")

	opts := quirks.DatabaseOptions()
	opts[driver.OptionLoggingLevel] = "trace"
	opts[driver.OptionLoggingSink] = "file"
	opts[driver.OptionLoggingFileLocation] = logDir
	opts[driver.OptionLoggingFilePrefix] = "flightsql-library-log"
	opts[driver.OptionLoggingFileMaxSizeKb] = "1"
	opts[driver.OptionLoggingFileMaxFiles] = "3"
	opts["adbc.traces.exporter"] = string(adbc.TelemetryExporterAdbcFile)
	opts["adbc.traces.exporter.adbcfile.location"] = traceDir
	opts["adbc.traces.exporter.adbcfile.maxtracesizekb"] = "16"
	opts["adbc.traces.exporter.adbcfile.maxtracefiles"] = "3"

	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)

	ctx := context.Background()
	cnxn, err := db.Open(ctx)
	require.NoError(t, err)

	stmt, err := cnxn.NewStatement()
	require.NoError(t, err)
	require.NoError(t, stmt.SetSqlQuery("SELECT 1"))

	rdr, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	for rdr.Next() {
	}
	require.NoError(t, rdr.Err())
	rdr.Release()

	require.NoError(t, stmt.Close())
	require.NoError(t, cnxn.Close())
	require.NoError(t, db.Close())

	logs := readMatchingFiles(t, logDir, "flightsql-library-log*.jsonl")
	require.Contains(t, logs, `"level":"TRACE"`)
	require.Contains(t, logs, "FlightService")

	traces := readMatchingFiles(t, traceDir, "*.jsonl")
	require.Contains(t, traces, "FlightSQL.Database.Open")
	require.Contains(t, traces, "FlightSQL.Statement.ExecuteQuery")
	require.Contains(t, traces, "Sent.arrow.flight.protocol.FlightService")
}

func readMatchingFiles(t *testing.T, dir string, pattern string) string {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(dir, pattern))
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	var out strings.Builder
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		out.Write(data)
	}
	return out.String()
}
