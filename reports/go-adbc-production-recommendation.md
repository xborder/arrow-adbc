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

# BDX-645 Go ADBC — production go/no-go and follow-ups

Companion to `go-adbc-report.md` (feasibility evidence) and `call-site-inventory.md`.

## Recommendation: GO, as a staged upstream contribution

The PollInfo POC is **feasible transparently** for the Go ADBC Flight SQL driver and is worth taking to production. No public ADBC method, signature or result type changes:

- `ExecuteQuery`, metadata, `GetInfo`, and the `Database.Open` probe drain `PollFlightInfo` to completion, then hand the final cumulative `FlightInfo` to the unchanged reader path.
- Incremental `ExecutePartitions` (`adbc.statement.exec.incremental`) stays the progressive API. It returns only newly appended endpoints per call, and the POC hardens it with append-only validation, context-aware backoff and best-effort `CancelFlightInfo`.

Go conditions:

1. Close the three hardening gaps marked **(blocking)** below before proposing upstream.
2. Expect upstream to debate **default-on**. Polling by default adds one extra `UNIMPLEMENTED` round trip per physical connection and command family against servers that do not implement polling (Arrow C++ and Arrow Go base servers). Default-on is still the recommendation, because servers built on the Java Flight SQL producer default answer `PollFlightInfo` with a completed `PollInfo` and pay nothing extra. Be ready to land default-off first if maintainers insist, with the same option name.

## Re-validation on current upstream (2026-11-25)

The branch was rebased from `4d50e2e30` (2026-09-10) onto `apache/arrow-adbc` main `bedbffc45`, 219 upstream commits later.

- Upstream `40323c6da` (`feat!(go): split Flight SQL into its own module`) moved the driver from `go/adbc/driver/flightsql` to `go/driver/flightsql`. `poll.go`, `poll_test.go` and `poll_conformance_test.go` moved with it, and the conformance test import was updated to `github.com/apache/arrow-adbc/go/driver/flightsql`.
- Upstream `58525bdb7` (`handle is_update in prepared stmts`) conflicted in `statement.ExecuteQuery`. Resolution: keep upstream's prepared `is_update` → `ExecuteUpdate` early return (a `DoPut` path, not applicable to polling) and route every other query through `executeQueryInfo`.

| Check | Command (from `go/driver/flightsql`) | Result |
| --- | --- | --- |
| Build + vet | `go build ./... && go vet .` | pass |
| Driver package | `go test . -count=1` | 302 pass (284 before rebase; the difference is new upstream tests) |
| Shared T1–T10 conformance | `ADBC_POLL_CONFORMANCE_URI=grpc+tcp://127.0.0.1:32437 ADBC_POLL_CONFORMANCE_CONTROL=http://127.0.0.1:32438 go test . -run '^TestPollInfoSharedConformance$' -count=1 -v` | 14/14 subtests pass |
| Race detector | same env, `go test . -race -count=1 -run 'Poll\|Incremental'` | 62 pass |
| Conformance fixture self-test | `go test ./...` in `tools/pollinfo-conformance-server` (`bdx-645-pollinfo-conformance-server`) | 13 pass |

Fixture: `dremio-employees/helder.gregorio` branch `bdx-645-pollinfo-conformance-server`, run as `conformance-server -flight-addr 127.0.0.1:32437 -control-addr 127.0.0.1:32438`. The shared suite was run again against the rebased fixture (`5c95beb`, unchanged API) on its assigned Go ADBC ports, `-flight-addr 127.0.0.1:31357 -control-addr 127.0.0.1:31358`. All 14 subtests passed. Server state is global, so each driver needs its own instance.

Request-order evidence after the rebase:

- `ExecuteQuery` stays final-only: `Poll, Poll, Poll, DoGet, DoGet, DoGet`.
- Incremental `ExecutePartitions` stays progressive: `Poll, DoGet, Poll, DoGet, Poll, DoGet`.

## Timeout routing changes

Current state:

- `getTimeout` in `timeouts.go` maps per-RPC timeouts by method suffix: `DoGet` → fetch, `GetFlightInfo` → query, `DoPut`/`DoAction` → update. `PollFlightInfo` matches none of these, so **no per-RPC timeout applies to it**.
- The POC therefore puts `queryTimeout` on one context **outside** the loop in `pollToCompletion`. Every poll, and the single initial-`UNIMPLEMENTED` fallback `GetFlightInfo`, share that one deadline. T8 proves the active poll is terminated when it expires.
- Behavior change to document: metadata calls (`GetObjects`, `GetTableTypes`, `GetTableSchema`, `GetInfo`) and the `Database.Open` probe used to give each `GetFlightInfo` call its own `queryTimeout`. They now get one `queryTimeout` for the whole poll sequence. The value is the same and the bound is stricter.

Production follow-ups:

1. **(blocking) Add `PollFlightInfo` → `queryTimeout` to `getTimeout`.** Inside `pollToCompletion` this is harmless, because the derived per-RPC deadline can never outlive the outer operation deadline. It also fixes a gap that predates the POC: incremental `ExecutePartitions` currently issues `PollFlightInfo` with no deadline at all, and this gives each call a bound.
2. Document `adbc.flight.sql.rpc.timeout_seconds.query` as one deadline for the whole operation on polled paths, and per call on incremental `ExecutePartitions`.
3. Optional: decide whether the timeout covers the server's long-poll wait. Today it does, so a server long-poll longer than `queryTimeout` surfaces `DEADLINE_EXCEEDED` rather than another continuation.

## `GetFlightInfo` call sites not yet covered

All call sites were re-inventoried on the rebased code; upstream added none. Remaining direct callers:

| Call site | Status | Recommendation |
| --- | --- | --- |
| `statement.ExecutePartitions`, incremental **disabled** → `prepared.Execute` / `query.execute` (`flightsql_statement.go`, non-incremental branch) | Deliberately unchanged in the POC | **Route through `executeQueryInfo` for production.** Contract item 1 says "every path that obtains a `FlightInfo` via `GetFlightInfo`". It is a mechanical change (final-only, same as `ExecuteQuery`), and leaving it out makes the opt-out and capability cache behave differently between `ExecuteQuery` and `ExecutePartitions`. |
| Fallback getters inside `pollToCompletion` (`connectionImpl.execute`, `executeSubstrait`, `prepared.Execute`, typed metadata getters) | By design | None. They run only when polling is disabled, cached as unsupported, or the initial poll returned `UNIMPLEMENTED`. |
| Prepared `is_update` → `ExecuteUpdate` (new upstream) | Not applicable (`DoPut`) | None. |
| `ExecuteSchema` → `GetSchema`, bulk ingest, transactions, session options | Not applicable (no `FlightInfo`) | None. |

## Hardening follow-ups

**Blocking before proposing upstream:**

1. **Timeout routing for `PollFlightInfo`.** See above.
2. **Bounded delay on unchanged poll responses (contract item 12, not implemented).** `pollToCompletion` loops as fast as the server answers. A server that returns immediately with a new continuation and no change in the cumulative info spins the client. Add a small capped backoff only when a successful response shows no observable change (same endpoints, same progress), reusing the incremental path's 100 ms → 5 s schedule.
3. **Non-incremental `ExecutePartitions` routing.** See the call-site table.

**Arrow Go (upstream prerequisites, lets ADBC drop workarounds):**

4. Add an exported generic Flight SQL command → `PollFlightInfo` helper, or typed `Poll*` metadata helpers. Today ADBC re-serializes standard `Command*` protobufs itself (`descriptorForCommand`).
5. Add a prepared-statement primitive that supports "initial poll returned `UNIMPLEMENTED`, now `GetFlightInfo` without binding again". The POC clears and restores the client-side binding (`executePreparedAfterUnsupportedPoll`), which works but depends on arrow-go internals.
6. Add Arrow Go tests for one deadline across continuations, and for `CancelFlightInfo` after a partial poll.

**Go ADBC:**

7. Final option naming plus docs for `adbc.flight.sql.rpc.use_poll_flight_info` (database and connection level) in `docs/source/driver/flight_sql.rst`.
8. Tracing and telemetry. The `ExecuteQuery` span still labels the stage `get_flight_info`. Add poll count, fallback/cached-bypass outcome and cancellation outcome as span attributes.
9. Lifecycle of the detached best-effort `CancelFlightInfo` goroutine. It is bounded at 1 s and does not delay the caller, but it is not tracked on connection close. Track it or document it.
10. Capability cache scope. The cache lives on `connectionImpl`, so every new physical connection probes again, including the `Database.Open` `GetSqlInfo` probe. That matches the contract, but consider sharing a negative cache at the `databaseImpl` level for connection-pooling clients.
11. Coverage. Add shared-fixture multi-step cases for Substrait and the remaining metadata families, and run an integration pass against a real polling server (Dremio) and a non-polling one (Arrow C++ example server).

## Documented limitations

- `ExecuteQuery` never exposes partial results. Progressive consumption is only available through incremental `ExecutePartitions`. This is by design and keeps the ADBC API split.
- After an `UNAVAILABLE` error on incremental `ExecutePartitions`, the driver keeps the continuation state, so the **application** can call again and resend the same continuation. That is not a hidden driver retry, but the behavior must be documented, because it is the only path that resends a descriptor after an error.
- Against servers without polling support, each new connection pays one extra `UNIMPLEMENTED` RPC per command family it uses (see default-on above).
- `go-adbc-report.md` and `evidence.jsonl` refer to `reports/raw/*.log` and `reports/experimental.patch`. Those were kept as uncommitted local evidence and are **not** on the branch. The re-validation table above is the reproducible replacement.
