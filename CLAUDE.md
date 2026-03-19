# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                  # dev build
cargo build --release        # release build
cargo check                  # fast type/borrow check
cargo test                   # run all tests
cargo fmt                    # format code
```

Run a single command against a local hub:
```bash
cargo run -- list-shades
cargo run -- serve-mqtt
```

Hub connection is configured via environment variables (or a `.env` file):
- `PV_HUB_IP` — hub IP address (skips mDNS discovery)
- `PV_HUB_SERIAL` — disambiguates when multiple hubs are present

## Architecture

**pview** bridges Hunter Douglas PowerView Gen 3 hubs to Home Assistant via MQTT.

### Request flow

CLI args (`main.rs`) → `Args::hub()` resolves the hub (mDNS via `discovery.rs` or `PV_HUB_IP`) → returns a `Hub` (in `hub.rs`) → commands call `Hub` methods → `Hub` calls `http_helpers` → REST to the hub at `http://{addr}/home/...`.

### Key modules

- **`api_types.rs`** — All serde structs for the Gen 3 REST API. Important: `ShadeCapabilities` and `PowerType` use manual `Deserialize`/`Serialize` with `Unknown(i32)` fallbacks (not `serde_repr`) so unrecognised hub values don't cause parse errors.
- **`hub.rs`** — `Hub` struct; one public method per API operation. Shade positions are normalised `0.0–1.0` floats in the API; `ShadePosition::pos_to_percent`/`percent_to_pos` convert to/from 0–100.
- **`discovery.rs`** — mDNS discovery using service name `_PowerView-G3._tcp.local`. Hub resolves to a `ResolvedHub` carrying the IP.
- **`hass_helper.rs`** — Typed serde structs for Home Assistant MQTT discovery payloads (covers, sensors, buttons, scenes).
- **`commands/serve_mqtt.rs`** — The main long-running service. Uses `mosquitto-rs` with a topic router. Maintains an SSE connection to `home/shades/events` and drives position interpolation tasks (per-shade `AbortHandle` stored in `Pv2MqttState.motion_tasks`).

### Shade naming

Shades are identified by `"Room Shade"` (space-separated). `Hub::shade_by_name` tries every word-split position to find a unique room+shade match; bare names work only when unambiguous. Numeric IDs always work.

### Gen 3 API notes

- Base path: `http://{hub_ip}/home/...` (rooms, shades, scenes) and `/gateway`
- Stop: `PUT home/shades/stop?ids={id}` — shade ID is a **query parameter**, not in the body
- Set position: `PUT home/shades/{id}/positions` with body `{"positions": {"primary": 0.75}}`
- SSE stream: `GET home/shades/events` — events include `MotionStarted` (with `currentPositions`, `targetPositions` where `etaInSeconds` is nested) and `MotionStopped`

### Tokio runtime

Worker threads are intentionally limited to 2 (`#[tokio::main(worker_threads = 2)]`) to avoid overwhelming the hub with concurrent requests.

## References

- **Repository**: https://github.com/wez/pview
- **HA PowerView integration** (reference for Gen 3 API behaviour): https://github.com/home-assistant/core/tree/dev/homeassistant/components/hunterdouglas_powerview
- **aiopvapi** (Python library used by HA integration, useful for API details): https://github.com/sander76/aio-powerview-api
