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
