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

The default `mqtt-tls` feature compiles the vendored libmosquitto 2.0.9, which
does not build against OpenSSL 4 (opaque `ASN1_STRING`). On a Mac with Homebrew
`openssl@4`, set `OPENSSL_DIR=$(brew --prefix openssl@3)`, or build with
`--no-default-features --features mqtt-no-tls`. Alpine 3.24 ships OpenSSL 3.5.

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
