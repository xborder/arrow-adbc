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

package flightsql

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	loggingOptionPrefix = "adbc.flight.sql.client_option.logging."

	loggingSinkStderr = "stderr"
	loggingSinkFile   = "file"

	logLevelTrace = slog.LevelDebug - 4

	defaultLoggingFilePrefix = "apache.adbc.flight.sql"

	envLogLevel         = "ADBC_DRIVER_FLIGHTSQL_LOG_LEVEL"
	envLogSink          = "ADBC_DRIVER_FLIGHTSQL_LOG_SINK"
	envLogFileLocation  = "ADBC_DRIVER_FLIGHTSQL_LOG_FILE_LOCATION"
	envLogFilePrefix    = "ADBC_DRIVER_FLIGHTSQL_LOG_FILE_PREFIX"
	envLogFileMaxSizeKb = "ADBC_DRIVER_FLIGHTSQL_LOG_FILE_MAX_SIZE_KB"
	envLogFileMaxFiles  = "ADBC_DRIVER_FLIGHTSQL_LOG_FILE_MAX_FILES"
)

func applyLoggingEnvFallback(opts map[string]string) {
	apply := func(key, env string) {
		if _, ok := opts[key]; ok {
			return
		}
		if value := os.Getenv(env); value != "" {
			opts[key] = value
		}
	}

	apply(OptionLoggingLevel, envLogLevel)
	apply(OptionLoggingSink, envLogSink)
	apply(OptionLoggingFileLocation, envLogFileLocation)
	apply(OptionLoggingFilePrefix, envLogFilePrefix)
	apply(OptionLoggingFileMaxSizeKb, envLogFileMaxSizeKb)
	apply(OptionLoggingFileMaxFiles, envLogFileMaxFiles)
}

func (d *databaseImpl) configureLogging(opts map[string]string) error {
	if !hasLoggingOption(opts) {
		return nil
	}
	if err := rejectUnknownLoggingOptions(opts); err != nil {
		return err
	}

	levelValue := loggingOption(opts, OptionLoggingLevel, "error")
	level, off, err := parseLoggingLevel(levelValue)
	if err != nil {
		return err
	}

	_, sinkConfigured := opts[OptionLoggingSink]
	sink := strings.ToLower(strings.TrimSpace(loggingOption(opts, OptionLoggingSink, loggingSinkStderr)))
	if !sinkConfigured && sink == loggingSinkStderr && hasFileLoggingOption(opts) {
		sink = loggingSinkFile
	}

	var (
		writer   io.Writer = os.Stderr
		shutdown func() error
	)

	if off {
		writer = io.Discard
	} else {
		switch sink {
		case loggingSinkStderr:
			writer = os.Stderr
		case loggingSinkFile:
			fileWriter, err := newLoggingFileWriter(opts)
			if err != nil {
				return err
			}
			writer = fileWriter
			shutdown = fileWriter.Close
		default:
			return d.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for database option '%s': '%s' must be '%s' or '%s'", OptionLoggingSink, sink, loggingSinkStderr, loggingSinkFile)
		}
	}

	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		AddSource:   false,
		Level:       level,
		ReplaceAttr: replaceTraceLevel,
	})
	d.SetLoggerWithShutdown(slog.New(handler), shutdown)
	deleteLoggingOptions(opts)
	return nil
}

func hasLoggingOption(opts map[string]string) bool {
	for key := range opts {
		if strings.HasPrefix(key, loggingOptionPrefix) {
			return true
		}
	}
	return false
}

func hasFileLoggingOption(opts map[string]string) bool {
	for _, key := range []string{OptionLoggingFileLocation, OptionLoggingFilePrefix, OptionLoggingFileMaxSizeKb, OptionLoggingFileMaxFiles} {
		if _, ok := opts[key]; ok {
			return true
		}
	}
	return false
}

func rejectUnknownLoggingOptions(opts map[string]string) error {
	for key := range opts {
		if strings.HasPrefix(key, loggingOptionPrefix) && !isLoggingOption(key) {
			return adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "[Flight SQL] Unknown database option '" + key + "'"}
		}
	}
	return nil
}

func isLoggingOption(key string) bool {
	switch key {
	case OptionLoggingLevel,
		OptionLoggingSink,
		OptionLoggingFileLocation,
		OptionLoggingFilePrefix,
		OptionLoggingFileMaxSizeKb,
		OptionLoggingFileMaxFiles:
		return true
	default:
		return false
	}
}

func deleteLoggingOptions(opts map[string]string) {
	for key := range opts {
		if isLoggingOption(key) {
			delete(opts, key)
		}
	}
}

func loggingOption(opts map[string]string, key string, fallback string) string {
	if value, ok := opts[key]; ok {
		return value
	}
	return fallback
}

func parseLoggingLevel(value string) (slog.Level, bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "trace":
		return logLevelTrace, false, nil
	case "debug":
		return slog.LevelDebug, false, nil
	case "info":
		return slog.LevelInfo, false, nil
	case "warn", "warning":
		return slog.LevelWarn, false, nil
	case "error":
		return slog.LevelError, false, nil
	case "off", "none":
		return slog.LevelError, true, nil
	default:
		return slog.LevelError, false, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "[Flight SQL] Invalid logging level '" + value + "'"}
	}
}

func replaceTraceLevel(_ []string, attr slog.Attr) slog.Attr {
	if attr.Key == slog.LevelKey {
		if level, ok := attr.Value.Any().(slog.Level); ok && level == logLevelTrace {
			attr.Value = slog.StringValue("TRACE")
		}
	}
	return attr
}

func newLoggingFileWriter(opts map[string]string) (io.WriteCloser, error) {
	location := strings.TrimSpace(loggingOption(opts, OptionLoggingFileLocation, ""))
	if location == "" {
		var err error
		location, err = defaultLoggingFolderPath()
		if err != nil {
			return nil, err
		}
	}
	prefix := strings.TrimSpace(loggingOption(opts, OptionLoggingFilePrefix, defaultLoggingFilePrefix))
	if prefix == "" {
		prefix = defaultLoggingFilePrefix
	}
	maxSizeKb, err := parseLoggingInt64Option(opts, OptionLoggingFileMaxSizeKb)
	if err != nil {
		return nil, err
	}
	maxFiles, err := parseLoggingIntOption(opts, OptionLoggingFileMaxFiles)
	if err != nil {
		return nil, err
	}
	return driverbase.NewRotatingFileWriter(
		driverbase.WithTracingFolderPath(location),
		driverbase.WithLogNamePrefix(prefix),
		driverbase.WithFileSizeMaxKb(maxSizeKb),
		driverbase.WithFileCountMax(maxFiles),
	)
}

func defaultLoggingFolderPath() (string, error) {
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(userConfigDir, ".adbc", "logs"), nil
}

func parseLoggingInt64Option(opts map[string]string, key string) (int64, error) {
	value, ok := opts[key]
	if !ok || strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "[Flight SQL] Invalid value for database option '" + key + "': '" + value + "' is not a positive integer"}
	}
	return parsed, nil
}

func parseLoggingIntOption(opts map[string]string, key string) (int, error) {
	value, ok := opts[key]
	if !ok || strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "[Flight SQL] Invalid value for database option '" + key + "': '" + value + "' is not a positive integer"}
	}
	return parsed, nil
}

func logRPC(ctx context.Context, logger *slog.Logger, level slog.Level, method string, attrs ...any) {
	logger.Log(ctx, level, method, attrs...)
}

func makeUnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryClientInterceptor {
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		// Ignore errors
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		err := invoker(ctx, method, req, reply, cc, opts...)
		if logger.Enabled(ctx, logLevelTrace) {
			logRPC(ctx, logger, logLevelTrace, method, "target", cc.Target(), "duration", time.Since(start), "err", err, "metadata", outgoing)
		} else {
			keys := maps.Keys(outgoing)
			slices.Sort(keys)
			if logger.Enabled(ctx, slog.LevelDebug) {
				logger.DebugContext(ctx, method, "target", cc.Target(), "duration", time.Since(start), "err", err, "metadata_keys", keys)
			} else {
				logger.InfoContext(ctx, method, "target", cc.Target(), "duration", time.Since(start), "err", err, "metadata_keys", keys)
			}
		}
		return err
	}
	return interceptor
}

func makeStreamLoggingInterceptor(logger *slog.Logger) grpc.StreamClientInterceptor {
	interceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		// Ignore errors
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			logger.InfoContext(ctx, method, "target", cc.Target(), "duration", time.Since(start), "err", err)
			return stream, err
		}

		return &loggedStream{ClientStream: stream, logger: logger, ctx: ctx, method: method, start: start, target: cc.Target(), outgoing: outgoing}, err
	}
	return interceptor
}

type loggedStream struct {
	grpc.ClientStream

	logger   *slog.Logger
	ctx      context.Context
	method   string
	start    time.Time
	target   string
	outgoing metadata.MD
}

func (stream *loggedStream) RecvMsg(m any) error {
	err := stream.ClientStream.RecvMsg(m)
	if err != nil {
		loggedErr := err
		if loggedErr == io.EOF {
			loggedErr = nil
		}

		if stream.logger.Enabled(stream.ctx, logLevelTrace) {
			logRPC(stream.ctx, stream.logger, logLevelTrace, stream.method, "target", stream.target, "duration", time.Since(stream.start), "err", loggedErr, "metadata", stream.outgoing)
		} else {
			keys := maps.Keys(stream.outgoing)
			slices.Sort(keys)
			if stream.logger.Enabled(stream.ctx, slog.LevelDebug) {
				stream.logger.DebugContext(stream.ctx, stream.method, "target", stream.target, "duration", time.Since(stream.start), "err", loggedErr, "metadata_keys", keys)
			} else {
				stream.logger.InfoContext(stream.ctx, stream.method, "target", stream.target, "duration", time.Since(stream.start), "err", loggedErr, "metadata_keys", keys)
			}
		}
	}
	return err
}
