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

package driverbase

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	driverNamespace    = "apache.arrow.adbc"
	otelTracesExporter = "OTEL_TRACES_EXPORTER"

	OptionKeyTracesExporter                       = "adbc.traces.exporter"
	OptionKeyTracesExporterAdbcFileLocation       = "adbc.traces.exporter.adbcfile.location"
	OptionKeyTracesExporterAdbcFileMaxTraceSizeKb = "adbc.traces.exporter.adbcfile.maxtracesizekb"
	OptionKeyTracesExporterAdbcFileMaxTraceFiles  = "adbc.traces.exporter.adbcfile.maxtracefiles"
)

type traceExporterType int

const (
	TraceExporterNone traceExporterType = iota
	TraceExporterOtlp
	TraceExporterConsole
	TraceExporterAdbcFile
)

var traceExporterNames = map[string]traceExporterType{
	string(adbc.TelemetryExporterNone):     TraceExporterNone,
	string(adbc.TelemetryExporterOtlp):     TraceExporterOtlp,
	string(adbc.TelemetryExporterConsole):  TraceExporterConsole,
	string(adbc.TelemetryExporterAdbcFile): TraceExporterAdbcFile,
}

func (te traceExporterType) String() string {
	return [...]string{
		string(adbc.TelemetryExporterNone),
		string(adbc.TelemetryExporterOtlp),
		string(adbc.TelemetryExporterConsole),
		string(adbc.TelemetryExporterAdbcFile),
	}[te]
}

type traceConfig struct {
	Exporter       string
	ExporterSet    bool
	AdbcFilePath   string
	AdbcFileSizeKb int64
	AdbcFileCount  int
}

const (
	DatabaseMessageOptionUnknown                   = "Unknown database option"
	DatabaseMessageOtelTracesExporterOptionUnknown = "Unknown trace exporter option"
	DatabaseMessageNoOtelTracesExporters           = "No trace exporters added"
)

var getExporterName = sync.OnceValue(func() string {
	return os.Getenv(otelTracesExporter)
})

// DatabaseImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type DatabaseImpl interface {
	adbc.Database
	adbc.GetSetOptions
	Base() *DatabaseImplBase
}

// Database is the interface satisfied by the result of the NewDatabase constructor,
// given an input is provided satisfying the DatabaseImpl interface.
type Database interface {
	adbc.Database
	adbc.GetSetOptions
	adbc.DatabaseLogging
	adbc.OTelTracingInit
}

// DatabaseImplBase is a struct that provides default implementations of the
// DatabaseImpl interface. It is meant to be used as a composite struct for a
// driver's DatabaseImpl implementation.
type DatabaseImplBase struct {
	Alloc         memory.Allocator
	ErrorHelper   ErrorHelper
	DriverInfo    *DriverInfo
	Logger        *slog.Logger
	Tracer        trace.Tracer
	TraceProvider trace.TracerProvider

	loggerConfigured        bool
	loggerShutdownFunc      func() error
	tracerShutdownFunc      func(context.Context) error
	traceWriterShutdownFunc func() error
	traceParent             string
	traceConfig             traceConfig
}

// NewDatabaseImplBase instantiates DatabaseImplBase.
//
//   - driver is a DriverImplBase containing the common resources from the parent
//     driver, allowing the Arrow allocator and error handler to be reused.
func NewDatabaseImplBase(_ context.Context, driver *DriverImplBase) (DatabaseImplBase, error) {
	database := DatabaseImplBase{
		Alloc:       driver.Alloc,
		ErrorHelper: driver.ErrorHelper,
		DriverInfo:  driver.DriverInfo,
		Logger:      nilLogger(),
		Tracer:      otel.Tracer(driverNamespace + "." + driver.DriverInfo.GetName()),
	}
	return database, nil
}

func (base *DatabaseImplBase) Base() *DatabaseImplBase {
	return base
}

func (base *DatabaseImplBase) GetOption(key string) (string, error) {
	switch key {
	case OptionKeyTracesExporter:
		if base.traceConfig.ExporterSet {
			return base.traceConfig.Exporter, nil
		}
		return getExporterName(), nil
	case OptionKeyTracesExporterAdbcFileLocation:
		return base.traceConfig.AdbcFilePath, nil
	case OptionKeyTracesExporterAdbcFileMaxTraceSizeKb:
		if base.traceConfig.AdbcFileSizeKb == 0 {
			return "", nil
		}
		return strconv.FormatInt(base.traceConfig.AdbcFileSizeKb, 10), nil
	case OptionKeyTracesExporterAdbcFileMaxTraceFiles:
		if base.traceConfig.AdbcFileCount == 0 {
			return "", nil
		}
		return strconv.Itoa(base.traceConfig.AdbcFileCount), nil
	}
	return "", base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionKeyTracesExporterAdbcFileMaxTraceSizeKb:
		return base.traceConfig.AdbcFileSizeKb, nil
	case OptionKeyTracesExporterAdbcFileMaxTraceFiles:
		return int64(base.traceConfig.AdbcFileCount), nil
	}
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOption(key string, val string) error {
	switch key {
	case OptionKeyTracesExporter:
		value := strings.ToLower(strings.TrimSpace(val))
		if _, ok := tryParseTraceExporterType(value); !ok {
			return base.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "%s '%s'", DatabaseMessageOtelTracesExporterOptionUnknown, val)
		}
		base.traceConfig.Exporter = value
		base.traceConfig.ExporterSet = true
		return nil
	case OptionKeyTracesExporterAdbcFileLocation:
		base.traceConfig.AdbcFilePath = val
		return nil
	case OptionKeyTracesExporterAdbcFileMaxTraceSizeKb:
		parsed, err := strconv.ParseInt(val, 10, 64)
		if err != nil || parsed <= 0 {
			return base.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for database option '%s': '%s' is not a positive integer", key, val)
		}
		base.traceConfig.AdbcFileSizeKb = parsed
		return nil
	case OptionKeyTracesExporterAdbcFileMaxTraceFiles:
		parsed, err := strconv.Atoi(val)
		if err != nil || parsed <= 0 {
			return base.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for database option '%s': '%s' is not a positive integer", key, val)
		}
		base.traceConfig.AdbcFileCount = parsed
		return nil
	}
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOptionBytes(key string, val []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOptionDouble(key string, val float64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOptionInt(key string, val int64) error {
	switch key {
	case OptionKeyTracesExporterAdbcFileMaxTraceSizeKb:
		if val <= 0 {
			return base.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for database option '%s': '%d' is not a positive integer", key, val)
		}
		base.traceConfig.AdbcFileSizeKb = val
		return nil
	case OptionKeyTracesExporterAdbcFileMaxTraceFiles:
		if val <= 0 {
			return base.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for database option '%s': '%d' is not a positive integer", key, val)
		}
		base.traceConfig.AdbcFileCount = int(val)
		return nil
	}
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *database) Close() error {
	return base.Base().Close()
}

func (base *DatabaseImplBase) Close() (err error) {
	if base.Base().tracerShutdownFunc != nil {
		err = errors.Join(err, base.Base().tracerShutdownFunc(context.Background()))
		base.Base().tracerShutdownFunc = nil
	}
	if base.Base().traceWriterShutdownFunc != nil {
		err = errors.Join(err, base.Base().traceWriterShutdownFunc())
		base.Base().traceWriterShutdownFunc = nil
	}
	if base.Base().loggerShutdownFunc != nil {
		err = errors.Join(err, base.Base().loggerShutdownFunc())
		base.Base().loggerShutdownFunc = nil
	}
	return
}

func (base *DatabaseImplBase) Open(ctx context.Context) (adbc.Connection, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Open")
}

func (base *DatabaseImplBase) SetOptions(options map[string]string) error {
	for key, val := range options {
		if err := base.SetOption(key, val); err != nil {
			return err
		}
	}
	return nil
}

func IsTraceOption(key string) bool {
	switch key {
	case OptionKeyTracesExporter,
		OptionKeyTracesExporterAdbcFileLocation,
		OptionKeyTracesExporterAdbcFileMaxTraceSizeKb,
		OptionKeyTracesExporterAdbcFileMaxTraceFiles:
		return true
	default:
		return false
	}
}

func (base *DatabaseImplBase) SetTraceOptions(options map[string]string) error {
	for key, val := range options {
		if !IsTraceOption(key) {
			continue
		}
		if err := base.SetOption(key, val); err != nil {
			return err
		}
		delete(options, key)
	}
	return nil
}

func (d *DatabaseImplBase) GetInitialSpanAttributes() []attribute.KeyValue {
	return getInitialSpanAttributes(d.DriverInfo)
}

func (d *DatabaseImplBase) GetTraceParent() (traceParent string) {
	return d.traceParent
}

func (d *DatabaseImplBase) SetTraceParent(traceParent string) {
	d.traceParent = traceParent
}

func (d *DatabaseImplBase) StartSpan(
	ctx context.Context,
	spanName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	ctx, _ = maybeAddTraceParent(ctx, d, nil)
	return d.Tracer.Start(ctx, spanName, opts...)
}

// database is the implementation of adbc.Database.
type database struct {
	DatabaseImpl
}

// NewDatabase wraps a DatabaseImpl to create an adbc.Database.
func NewDatabase(impl DatabaseImpl) Database {
	return &database{
		DatabaseImpl: impl,
	}
}

func (db *database) SetLogger(logger *slog.Logger) {
	db.Base().SetLogger(logger)
}

func (db *database) IsLoggerConfigured() bool {
	return db.Base().IsLoggerConfigured()
}

func (base *DatabaseImplBase) SetLogger(logger *slog.Logger) {
	base.SetLoggerWithShutdown(logger, nil)
}

func (base *DatabaseImplBase) SetLoggerWithShutdown(logger *slog.Logger, shutdown func() error) {
	if base.loggerShutdownFunc != nil {
		_ = base.loggerShutdownFunc()
	}
	if logger != nil {
		base.Logger = logger
	} else {
		base.Logger = nilLogger()
	}
	base.loggerShutdownFunc = shutdown
	base.loggerConfigured = true
}

func (base *DatabaseImplBase) IsLoggerConfigured() bool {
	return base.loggerConfigured
}

func (base *database) InitTracing(ctx context.Context, driverName string, driverVersion string) error {
	return base.Base().InitTracing(ctx, driverName, driverVersion)
}

func (base *DatabaseImplBase) ConfigureTracing(ctx context.Context) error {
	return base.InitTracing(ctx, base.DriverInfo.GetName(), getDriverVersion(base.DriverInfo))
}

func (base *DatabaseImplBase) InitTracing(ctx context.Context, driverName string, driverVersion string) (err error) {
	fullyQualifiedDriverName := driverNamespace + "." + driverName

	if base.tracerShutdownFunc != nil {
		err = base.tracerShutdownFunc(ctx)
		base.tracerShutdownFunc = nil
		if err != nil {
			return
		}
	}
	if base.traceWriterShutdownFunc != nil {
		err = base.traceWriterShutdownFunc()
		base.traceWriterShutdownFunc = nil
		if err != nil {
			return
		}
	}
	base.TraceProvider = nil
	base.Tracer = otel.Tracer(fullyQualifiedDriverName)

	var exporterName string
	if base.traceConfig.ExporterSet {
		exporterName = base.traceConfig.Exporter
	} else {
		exporterName = getExporterName()
	}

	// Empty or explicitly disabled exporter.
	if exporterName == "" || exporterName == string(adbc.TelemetryExporterNone) {
		return
	}

	var (
		exporterType traceExporterType
		exporters    []sdktrace.SpanExporter
	)

	exporters, exporterType, err = getExporters(
		ctx,
		exporterName,
		base,
		driverName,
	)
	if err != nil {
		return
	}

	if exporterType == TraceExporterNone {
		return
	}

	if len(exporters) < 1 {
		// This should not normally happen after a successful call to getExporters,
		// but here for completeness
		err = base.ErrorHelper.Errorf(
			adbc.StatusInvalidState,
			"%s '%s'",
			DatabaseMessageNoOtelTracesExporters,
			exporterType.String(),
		)
		return
	}

	base.Tracer, base.TraceProvider, err = newTracer(exporters, base, fullyQualifiedDriverName, driverVersion)

	return
}

func getExporters(
	ctx context.Context,
	exporterName string,
	base *DatabaseImplBase,
	driverName string,
) (exporters []sdktrace.SpanExporter, exporterType traceExporterType, err error) {
	var exporter sdktrace.SpanExporter
	exporterType, ok := tryParseTraceExporterType(exporterName)
	if !ok {
		err = base.ErrorHelper.Errorf(
			adbc.StatusInvalidArgument,
			"%s '%s'",
			DatabaseMessageOtelTracesExporterOptionUnknown,
			exporterName,
		)
		return
	}
	switch exporterType {
	case TraceExporterNone:
		break
	case TraceExporterConsole:
		exporter, err = stdouttrace.New()
		if err != nil {
			return
		}
		exporters = append(exporters, exporter)
	case TraceExporterOtlp:
		exporters, err = newOtlpTraceExporters(ctx)
		if err != nil {
			return
		}
	case TraceExporterAdbcFile:
		exporter, err = newAdbcFileExporter(driverName, base)
		if err != nil {
			return
		}
		exporters = append(exporters, exporter)
	default:
		err = base.ErrorHelper.Errorf(
			adbc.StatusNotImplemented,
			"%s '%s'",
			DatabaseMessageOtelTracesExporterOptionUnknown,
			exporterType.String(),
		)
	}
	return
}

func newTracer(
	exporters []sdktrace.SpanExporter,
	base *DatabaseImplBase,
	fullyQualifiedDriverName string,
	driverVersion string,
) (tracer trace.Tracer, tracerProvider trace.TracerProvider, err error) {
	sdkTracerProvider, err := newTracerProvider(exporters...)
	if err != nil {
		return
	}
	base.Base().tracerShutdownFunc = sdkTracerProvider.Shutdown
	tracerProvider = sdkTracerProvider
	tracer = sdkTracerProvider.Tracer(
		fullyQualifiedDriverName,
		trace.WithInstrumentationVersion(driverVersion),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	return
}

func tryParseTraceExporterType(value string) (traceExporterType, bool) {
	if te, ok := traceExporterNames[value]; ok {
		return te, true
	}
	return TraceExporterNone, false
}

func getDriverVersion(driverInfo *DriverInfo) string {
	const unknownDriverVersion = "unknown"
	value, ok := driverInfo.GetInfoForInfoCode(adbc.InfoDriverVersion)
	if !ok {
		return unknownDriverVersion
	}
	if driverVersion, ok := value.(string); ok {
		return driverVersion
	}
	return unknownDriverVersion
}

func newOtlpTraceExporters(ctx context.Context) ([]sdktrace.SpanExporter, error) {
	// Configure these exporters using environment variables
	// see: https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/
	// see: https://opentelemetry.io/docs/specs/otel/protocol/exporter/
	// see: https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
	// see: https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp

	// Create the gRPC exporter
	grpcExporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: 5 * time.Second,
			MaxInterval:     30 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}
	// Create the http/protobuf exporter
	httpExporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			InitialInterval: 5 * time.Second,
			MaxInterval:     30 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}

	return []sdktrace.SpanExporter{grpcExporter, httpExporter}, nil
}

func newAdbcFileExporter(driverName string, base *DatabaseImplBase) (*stdouttrace.Exporter, error) {
	fullyQualifiedDriverName := strings.ToLower(driverNamespace + "." + driverName)
	fileWriter, err := NewRotatingFileWriter(
		WithTracingFolderPath(base.traceConfig.AdbcFilePath),
		WithLogNamePrefix(fullyQualifiedDriverName),
		WithFileSizeMaxKb(base.traceConfig.AdbcFileSizeKb),
		WithFileCountMax(base.traceConfig.AdbcFileCount),
	)
	if err != nil {
		return nil, err
	}
	base.traceWriterShutdownFunc = fileWriter.Close
	return stdouttrace.New(stdouttrace.WithWriter(fileWriter))
}

func newTracerProvider(exporters ...sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	// Ensure default SDK resource and the required service name are set.
	tracerResource, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(driverNamespace),
		),
	)
	if err != nil {
		if errors.Is(err, resource.ErrSchemaURLConflict) {
			// If unable to merge with the default resource (conflicting ShhemaURL),
			// use just our resource
			tracerResource = resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceName(driverNamespace),
			)
		} else {
			return nil, err
		}
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(tracerResource),
	}
	for _, exporter := range exporters {
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}
	return sdktrace.NewTracerProvider(
		opts...,
	), nil
}

var _ DatabaseImpl = (*DatabaseImplBase)(nil)
