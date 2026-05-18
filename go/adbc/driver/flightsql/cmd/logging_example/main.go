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

// Package main is a small host application showing how to embed the Go
// Flight SQL driver as a library and configure driver logging without relying
// on process environment variables.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	flightsql "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
	var (
		uri           = flag.String("uri", os.Getenv("FLIGHTSQL_URI"), "Flight SQL URI, for example grpc://localhost:32010")
		query         = flag.String("query", "SELECT 1", "SQL query to execute")
		username      = flag.String("username", os.Getenv("FLIGHTSQL_USERNAME"), "Flight SQL username")
		password      = flag.String("password", os.Getenv("FLIGHTSQL_PASSWORD"), "Flight SQL password")
		logLevel      = flag.String("log-level", "trace", "Flight SQL log level: off, error, warn, info, debug, or trace")
		logSink       = flag.String("log-sink", "file", "Flight SQL log sink: stderr or file")
		logDir        = flag.String("log-dir", filepath.Join(os.TempDir(), "adbc-flightsql-logs"), "Directory for rotating JSONL log files when -log-sink=file")
		traceExporter = flag.String("trace-exporter", "none", "OpenTelemetry trace exporter: none, console, otlp, or adbcfile")
	)
	flag.Parse()

	if strings.TrimSpace(*uri) == "" {
		fmt.Fprintln(os.Stderr, "missing -uri or FLIGHTSQL_URI")
		flag.Usage()
		os.Exit(2)
	}

	if err := run(context.Background(), *uri, *query, *username, *password, *logLevel, *logSink, *logDir, *traceExporter); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, uri, query, username, password, logLevel, logSink, logDir, traceExporter string) (err error) {
	options := map[string]string{
		adbc.OptionKeyURI:            uri,
		flightsql.OptionLoggingLevel: logLevel,
		flightsql.OptionLoggingSink:  logSink,
		"adbc.traces.exporter":       traceExporter,
	}

	if username != "" {
		options[adbc.OptionKeyUsername] = username
	}
	if password != "" {
		options[adbc.OptionKeyPassword] = password
	}

	if strings.EqualFold(logSink, "file") {
		options[flightsql.OptionLoggingFileLocation] = logDir
		options[flightsql.OptionLoggingFilePrefix] = "adbc-flight-sql-sample"
		options[flightsql.OptionLoggingFileMaxSizeKb] = "1024"
		options[flightsql.OptionLoggingFileMaxFiles] = "5"
	}

	driver := flightsql.NewDriver(memory.DefaultAllocator)
	database, err := driver.NewDatabase(options)
	if err != nil {
		return fmt.Errorf("create Flight SQL database: %w", err)
	}
	defer func() {
		err = errors.Join(err, database.Close())
	}()

	connection, err := database.Open(ctx)
	if err != nil {
		return fmt.Errorf("open Flight SQL connection: %w", err)
	}
	defer func() {
		err = errors.Join(err, connection.Close())
	}()

	statement, err := connection.NewStatement()
	if err != nil {
		return fmt.Errorf("create statement: %w", err)
	}
	defer func() {
		err = errors.Join(err, statement.Close())
	}()

	if err = statement.SetSqlQuery(query); err != nil {
		return fmt.Errorf("set query: %w", err)
	}

	reader, _, err := statement.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer reader.Release()

	fmt.Printf("schema: %s\n", reader.Schema())

	var rows int64
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	if err = reader.Err(); err != nil {
		return fmt.Errorf("read results: %w", err)
	}

	fmt.Printf("rows: %d\n", rows)
	if strings.EqualFold(logSink, "file") {
		fmt.Printf("Flight SQL JSONL logs written under: %s\n", logDir)
	}
	return nil
}
