<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Flight SQL Snowflake-style observability

The first Flight SQL observability PR will follow the Snowflake driver precedent: diagnostics are configured through database options and environment fallbacks, logs go to stderr or rotating JSONL files, and trace spans use OpenTelemetry exporters. A host-owned logging callback and process-global gRPC-Go internal log bridge were considered for embedded hosts, but are deferred because they require a broader API/ABI design and process-wide semantics; this PR will still include per-connection gRPC client OpenTelemetry spans when tracing is enabled.
