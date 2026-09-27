# Integration improvements implementation plan

The approved audit is the design brief. Work sequentially on `codex/integration-improvements`; each numbered item is a separate functional commit. Preserve entity IDs and MQTT command topics, Gen 3 REST semantics, existing user files, and both MQTT build variants. Do not push or deploy.

1. **Responsive command processing.** Add a bounded, single-consumer work queue with replacement keys and STOP priority. Keep background refresh separate from commands and motion events. Cache shade metadata and eliminate GET-before-STOP. Bound REST concurrency explicitly. Test replacement, barriers, capacity, STOP priority, and enqueueing while work is blocked.
2. **Separate discovery and reconciliation.** Cache inventory; refresh shade state without rebuilding discovery every minute. Rebuild when inventory changes and on explicit registration events. Suppress unchanged state and protect moving/offline shades. Test deduplication and motion-safe snapshot policy.
3. **Motion recovery.** Add generation/deadline tracking, confirmed positions, and expiry reconciliation. Send commands before optimistic state changes; ignore obsolete cleanup. Test missing stop events and replacement generations.
4. **Availability.** Add retained bridge LWT and hub availability, combine with shade health, restore both rails, recover fixed-IP health. Test generated discovery and health transitions.
5. **Discovery lifecycle.** Handle only HA online birth, remove delete/recreate delays, retain discovery with QoS 1 and clear obsolete retained configs. Test registration deltas, deletion and birth filtering.
6. **Startup/SSE recovery.** Retry initial setup, validate stream responses, apply reconnect backoff, reconcile on reconnect, replace streams on IP changes. Test invalid HTTP responses and reconnect policy.
7. **Capability-aware covers.** Add MQTT tilt control and reporting, reverse primary position for top-down shades, and use product-appropriate rail labels. Test supported capability mappings and coordinate conversions.
8. **Settings and validation.** Persist effective per-shade velocities in an atomic local state file, configure add-on storage, reject invalid positions/velocity, publish effective values. Test round trips, invalid values, and clamping.
9. **Diagnostics.** Expose stream connectivity, last reconciliation, command latency/failures and reconnect count; label battery percentages as estimated. Test diagnostic values and discovery payloads.
10. **Build checkout fidelity.** Build add-on binaries from the checked-out repository rather than cloning main; update all build contexts and local build documentation. Verify workflow paths and container build where tooling permits.

For each item: add behavioral regressions, run them before the change, implement, run the complete Rust test suite and formatting, review the diff, then commit only that item's files. Final checks cover both TLS and no-TLS builds. Hardware behavior remains a deployment validation requirement.

## Progress

- Baseline: 10 tests pass after fetching locked dependencies; formatting passes.

### Completed commits

| Item | Commit | Outcome |
| --- | --- | --- |
| 1 | `8baf87c` | Independent bounded command/background queues; cached command metadata; two concurrent REST requests maximum. |
| 2 | `082efcf` | Minute state reconciliation, 15-minute metadata checks, changed-value publication. |
| 3 | `bfbdb29` | Motion generations, watchdogs, acknowledgement before optimistic updates. |
| 4 | `b7e65e0` | Retained bridge LWT, hub health, symmetrical shade availability. |
| 5 | `9d1e415` | Retained discovery/state, obsolete-topic cleanup, online-only HA birth handling. |
| 6 | `15e4144` | Startup retries, SSE HTTP validation, backoff and address-change notification. |
| 7 | `780b7e3` | Tilt, top-down coordinate normalization, capability-specific rail names. |
| 8 | `e671f26` | Atomic per-hub settings, persisted discovery manifest, effective velocity validation. |
| 9 | `0d5b19d` | Runtime diagnostic entities and explicitly estimated battery percentage. |
| 10 | `3a1675e` | Add-on builds use checkout; standalone containers have writable persistent storage. |

Independent review identified six correctness issues. Follow-up commits address all six:
- `d9fedc7`: serialized telemetry publication, stale-registration guard and per-shade command revisions; delayed-HTTP cross-shade regression verified failing with old logic and passing with fix.
- `acb405e`: motion deadline guards, transient REST failures affect hub rather than permanent shade radio availability, and consistent top-down settled state; each regression reproduced before repair.
- `6f72098`: STOP displaces lower-priority queued work when saturated; regression reproduced before repair.

### Validation and practical limits

- TLS and no-TLS suites: 29 tests pass in each configuration.
- Formatting and whitespace checks pass. Stable rustfmt warns that the existing `imports_granularity` option requires nightly.
- Clippy completes with three existing warnings (`build.rs` needless borrow, `ShadeData::name` returning the decoded name, and `Args::hub_ip` cloning a Copy value). Strict `-D warnings` fails on these existing warnings.
- Loopback HTTP fixtures consume complete requests, enforce socket deadlines, and check request paths/body.
- Shell syntax checks pass. Docker is unavailable locally, so image construction and cross-architecture validation remain CI checks.
- No live hub, broker or Home Assistant deployment was changed. Physical tilt/rail behavior and real outage recovery still need hardware validation.
- Ruling: keep commands serialized and prioritize STOP over pending work; an already-sent REST command cannot safely be preempted. Under saturation STOP may discard lower-priority queued work, with a warning; an all-STOP queue remains bounded.
- Ruling: use retained discovery and state, with explicit persisted cleanup, rather than registration sleeps. This adds retained broker state but removes subscription timing dependence.
- Ruling: keep the feature branch local. No push, merge or deployment was requested.
