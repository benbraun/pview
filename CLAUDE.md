# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                  # dev build
cargo build --release        # release build
cargo check                  # fast type/borrow check
cargo test                   # run all tests
cargo clippy --all-targets   # lint; keep it warning-free
cargo fmt                    # format code
```

There is no C code to build: MQTT is rumqttc without TLS (pview has never
connected to the broker over TLS). The `mqtt-tls` and `mqtt-no-tls` features are
empty and exist only so old build commands keep working.

The `serve_mqtt` tests run a real broker (rumqttd, a dev-dependency) in-process,
behind a TCP proxy they can cut, and a loopback fake hub.

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
- **`mqtt.rs`** — The MQTT layer over rumqttc: a `Client` that fails fast while disconnected (callers then re-send after the reconnect re-registration) and an `MqttRouter` whose handlers take `Topic`/`Payload`/`Params`/`State` extractors.
- **`commands/serve_mqtt.rs`** — The main long-running service. One task polls the rumqttc `EventLoop` (`mqtt_event_pump`); it must be running before anything is published, because rumqttc only sends what the poller pulls off its request queue. Polling after an error reconnects; on CONNACK the pump resubscribes (clean session) and asks the serve loop to re-register. Maintains an SSE connection to `home/shades/events` and drives position interpolation tasks (per-shade `AbortHandle` stored in `Pv2MqttState.motion_tasks`).

### Shade naming

Shades are identified by `"Room Shade"` (space-separated). `Hub::shade_by_name` tries every word-split position to find a unique room+shade match; bare names work only when unambiguous. Numeric IDs always work.

### Gen 3 API notes

- Base path: `http://{hub_ip}/home/...` (rooms, shades, scenes) and `/gateway`
- Stop: `PUT home/shades/stop?ids={id}` — shade ID is a **query parameter**, not in the body
- Set position: `PUT home/shades/{id}/positions` with body `{"positions": {"primary": 0.75}}`
- SSE stream: `GET home/shades/events` — events include `MotionStarted` (with `currentPositions`, `targetPositions` where `etaInSeconds` is nested) and `MotionStopped`

### Tokio runtime

Worker threads are intentionally limited to 2 (`#[tokio::main(worker_threads = 2)]`) to avoid overwhelming the hub with concurrent requests.

## Home Assistant app

`addon/` is a Home Assistant app (HA's new name for add-ons). The Dockerfile is
built from the repository root (`docker buildx build -f addon/Dockerfile .`) on
`ghcr.io/home-assistant/base`, which is multi-arch, so there is no `build.yaml`.
HA dropped armv7, and only amd64 and aarch64 are published.

Releases: `scripts/tag-release.sh` writes the tag into `addon/config.yaml` and
tags the commit; pushing the tag makes `build.yml` publish
`ghcr.io/benbraun/pview-addon-{arch}:<tag>` with `home-assistant/builder`'s
`build-image` action. The job refuses a tag that differs from the config
version, because Supervisor pulls exactly that tag. The images keep the
per-arch `{arch}` names because the existing GHCR packages are public; a new
multi-arch package name would start out private.

## References

- **Repository**: https://github.com/benbraun/pview (fork of https://github.com/wez/pview)
- **HA PowerView integration** (reference for Gen 3 API behaviour): https://github.com/home-assistant/core/tree/dev/homeassistant/components/hunterdouglas_powerview
- **aiopvapi** (Python library used by HA integration, useful for API details): https://github.com/sander76/aio-powerview-api
