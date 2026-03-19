# PowerView Gen 3 API Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all PowerView Gen 2 API types, HTTP client methods, and postback webhook with Gen 3 equivalents, including SSE-based shade event streaming.

**Architecture:** Clean in-place replacement across 12 files. `api_types.rs` is fully rewritten with v3 types; `hub.rs` maps to new v3 endpoints and adds an SSE stream method; `serve_mqtt.rs` removes the axum HTTP server and adds a tokio SSE listener task. No adapter layer; all v2 code removed.

**Tech Stack:** Rust, reqwest (stream feature), futures-util, async-stream, serde/serde_repr, tokio, mosquitto-rs

---

## File Structure

| File | Change |
|---|---|
| `Cargo.toml` | Remove axum/matchit/serde_urlencoded/base64/data-encoding; add reqwest stream feature, futures-util, async-stream |
| `src/api_types.rs` | Complete rewrite — v3 types, manual `ShadeCapabilities` Deserialize, `ShadeEventKind` with `#[serde(other)]` |
| `src/hub.rs` | New v3 endpoints, SSE stream method, remove v2 methods |
| `src/discovery.rs` | `user_data: Option<UserData>` → `gateway_data: Option<GatewayConfig>` |
| `src/commands/hub_info.rs` | Call `get_gateway_data()`, display structured fields |
| `src/commands/list_hubs.rs` | Use `gateway_data` instead of `user_data` |
| `src/commands/list_shades.rs` | `pt_name`, positions always present, `room_id` non-optional, one-arg `list_shades` |
| `src/commands/inspect_shade.rs` | Formatted display: signal strength in dBm, named position percentages, power_type |
| `src/commands/list_scenes.rs` | Show room names column, no scene members, fix `--room` filter |
| `src/commands/move_shade.rs` | Remove `Heart`/`Calibrate`; `--percent` always sets primary axis |
| `src/commands/activate_scene.rs` | No code changes — hub method changes internally |
| `src/commands/serve_mqtt.rs` | Remove axum server; add SSE listener task; update entity registrations and MQTT handlers |

---

### Task 1: Update Cargo.toml

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Apply dependency changes**

Edit the `[dependencies]` section of `Cargo.toml`. The result should be:

```toml
[dependencies]
anyhow = "1.0.86"
arc-swap = "1.7.1"
async-stream = "0.3"
bitflags = { version = "2.5.0", features = ["serde"] }
chrono = "0.4.38"
chrono-tz = "0.9.0"
clap = { version = "4.5.4", features = ["derive"] }
color-backtrace = "0.6.1"
dotenvy = "0.15.7"
env_logger = "0.10.2"
futures-util = "0.3"
iana-time-zone = "0.1.60"
log = "0.4.21"
reqwest = { version = "0.12.4", default-features=false, features = ["json", "stream"] }
serde = { version = "1.0.202", features = ["derive"] }
serde_json = "1.0.117"
serde_repr = "0.1.19"
tabout = "0.3.0"
thiserror = "1.0.61"
tokio = { version = "1.37.0", features = ["rt", "macros", "rt-multi-thread"] }
```

Removed: `axum`, `base64`, `data-encoding`, `matchit`, `serde_urlencoded`
Changed: `reqwest` gains `"stream"` feature
Added: `async-stream`, `futures-util`

Keep `[dependencies.wez-mdns]` and `[dependencies.mosquitto-rs]` blocks unchanged.

- [ ] **Step 2: Verify Cargo.toml parses**

```bash
cargo metadata --format-version 1 > /dev/null && echo "OK"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: update deps for powerview v3 (add stream/futures-util/async-stream, remove axum)"
```

---

### Task 2: Rewrite src/api_types.rs

**Files:**
- Modify: `src/api_types.rs`

- [ ] **Step 1: Replace the entire file with v3 types**

Write `src/api_types.rs`:

```rust
use serde::{Deserialize, Deserializer, Serialize};
use serde_repr::*;

// PowerView Gen 3 API types
// https://github.com/sander76/aio-powerview-api

// ── Rooms ─────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RoomData {
    pub id: i32,
    pub pt_name: String,
    pub name: String,   // raw base64, stored as-is
    pub color: String,
    pub icon: String,
    #[serde(rename = "type")]
    pub room_type: i32,
}

// ── Scenes ────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Scene {
    pub id: i32,
    pub pt_name: String,
    pub name: String,   // raw base64, stored as-is
    pub color: String,
    pub icon: String,
    pub network_number: i32,
    pub room_ids: Vec<i32>,
}

// ── Shades ────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ShadeData {
    pub id: i32,
    #[serde(rename = "type")]
    pub shade_type: i32,
    pub pt_name: String,
    pub name: String,   // raw base64, stored as-is
    pub capabilities: ShadeCapabilities,
    pub power_type: PowerType,
    pub battery_status: Option<BatteryStatus>, // nullable in v3; None = unavailable
    pub room_id: i32,                          // always present in v3
    pub firmware: ShadeFirmware,               // always present in v3
    pub positions: ShadePosition,              // always present in v3
    pub signal_strength: Option<f64>,          // RSSI in dBm, e.g. -55.0
    pub ble_name: String,
    pub shade_group_ids: Vec<i32>,
    pub serial_number: String,
}

impl ShadeData {
    pub fn name(&self) -> &str {
        &self.pt_name
    }

    /// Maps v3 BatteryStatus to a 0–100 percentage. Returns None when battery_status is None.
    pub fn battery_percent(&self) -> Option<u8> {
        match self.battery_status? {
            BatteryStatus::NoPower => Some(0),
            BatteryStatus::Low => Some(20),
            BatteryStatus::Medium => Some(50),
            BatteryStatus::High => Some(100),
        }
    }

    /// Maps RSSI dBm to 0–100%: -100 dBm → 0%, -50 dBm → 100%, clamped.
    pub fn signal_strength_percent(&self) -> Option<u8> {
        self.signal_strength
            .map(|dbm| ((dbm + 100.0) * 2.0).clamp(0.0, 100.0) as u8)
    }

    pub fn pos1_percent(&self) -> Option<u8> {
        self.positions.pos1_percent()
    }

    pub fn pos2_percent(&self) -> Option<u8> {
        self.positions.pos2_percent()
    }
}

// ── ShadePosition ─────────────────────────────────────────────────────────────

/// Named float fields (0.0–1.0). Fields with `skip_serializing_if` allow partial PUT updates —
/// the v3 hub preserves axes not included in the request body.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ShadePosition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub velocity: Option<f64>,
}

impl ShadePosition {
    pub fn pos_to_percent(v: f64) -> u8 {
        (v * 100.0).round() as u8
    }

    pub fn percent_to_pos(pct: u8) -> f64 {
        pct as f64 / 100.0
    }

    pub fn pos1_percent(&self) -> Option<u8> {
        self.primary.map(Self::pos_to_percent)
    }

    pub fn pos2_percent(&self) -> Option<u8> {
        self.secondary.map(Self::pos_to_percent)
    }

    pub fn describe_pos1(&self) -> String {
        self.primary
            .map(|v| format!("{}%", Self::pos_to_percent(v)))
            .unwrap_or_default()
    }

    pub fn describe_pos2(&self) -> String {
        self.secondary
            .map(|v| format!("{}%", Self::pos_to_percent(v)))
            .unwrap_or_default()
    }

    pub fn describe(&self) -> String {
        let mut parts = vec![];
        if let Some(p) = self.primary {
            parts.push(format!("primary: {}%", Self::pos_to_percent(p)));
        }
        if let Some(s) = self.secondary {
            parts.push(format!("secondary: {}%", Self::pos_to_percent(s)));
        }
        if let Some(t) = self.tilt {
            parts.push(format!("tilt: {}%", Self::pos_to_percent(t)));
        }
        if parts.is_empty() {
            "unknown".to_string()
        } else {
            parts.join("  ")
        }
    }
}

// ── ShadeFirmware ─────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ShadeFirmware {
    pub revision: i32,
    pub sub_revision: i32,
    pub build: i32,
}

// ── ShadeCapabilities ─────────────────────────────────────────────────────────

/// serde_repr does not support `#[serde(other)]`, so we implement manual Deserialize
/// with an `Unknown(i32)` fallback for forward compatibility.
#[derive(Serialize, Debug, Copy, Clone)]
pub enum ShadeCapabilities {
    BottomUp,             // 0
    BottomUpTilt90,       // 1
    BottomUpTilt180,      // 2
    VerticalTilt180,      // 3
    Vertical,             // 4
    TiltOnly180,          // 5
    TopDown,              // 6
    TopDownBottomUp,      // 7
    DualOverlapped,       // 8
    DualOverlappedTilt90, // 9
    DuoliteTilt180,       // 10
    Illuminated,          // 11
    Unknown(i32),         // forward compatibility; flags() returns empty
}

impl<'de> Deserialize<'de> for ShadeCapabilities {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = i32::deserialize(d)?;
        Ok(match v {
            0 => Self::BottomUp,
            1 => Self::BottomUpTilt90,
            2 => Self::BottomUpTilt180,
            3 => Self::VerticalTilt180,
            4 => Self::Vertical,
            5 => Self::TiltOnly180,
            6 => Self::TopDown,
            7 => Self::TopDownBottomUp,
            8 => Self::DualOverlapped,
            9 => Self::DualOverlappedTilt90,
            10 => Self::DuoliteTilt180,
            11 => Self::Illuminated,
            other => Self::Unknown(other),
        })
    }
}

impl ShadeCapabilities {
    pub fn flags(self) -> ShadeCapabilityFlags {
        match self {
            Self::BottomUp => ShadeCapabilityFlags::PRIMARY_RAIL,
            Self::BottomUpTilt90 => {
                ShadeCapabilityFlags::PRIMARY_RAIL | ShadeCapabilityFlags::TILT_ON_CLOSED
            }
            Self::BottomUpTilt180 => {
                ShadeCapabilityFlags::PRIMARY_RAIL
                    | ShadeCapabilityFlags::TILT_ANYWHERE
                    | ShadeCapabilityFlags::TILT_180
            }
            Self::VerticalTilt180 => {
                ShadeCapabilityFlags::PRIMARY_RAIL
                    | ShadeCapabilityFlags::TILT_ANYWHERE
                    | ShadeCapabilityFlags::TILT_180
            }
            Self::Vertical => ShadeCapabilityFlags::PRIMARY_RAIL,
            Self::TiltOnly180 => {
                ShadeCapabilityFlags::TILT_ANYWHERE | ShadeCapabilityFlags::TILT_180
            }
            Self::TopDown => {
                ShadeCapabilityFlags::PRIMARY_RAIL | ShadeCapabilityFlags::PRIMARY_RAIL_REVERSED
            }
            Self::TopDownBottomUp => {
                ShadeCapabilityFlags::PRIMARY_RAIL | ShadeCapabilityFlags::SECONDARY_RAIL
            }
            Self::DualOverlapped => {
                ShadeCapabilityFlags::PRIMARY_RAIL
                    | ShadeCapabilityFlags::SECONDARY_RAIL
                    | ShadeCapabilityFlags::SECONDARY_RAIL_OVERLAPPED
            }
            Self::DualOverlappedTilt90 => {
                ShadeCapabilityFlags::PRIMARY_RAIL
                    | ShadeCapabilityFlags::SECONDARY_RAIL
                    | ShadeCapabilityFlags::SECONDARY_RAIL_OVERLAPPED
                    | ShadeCapabilityFlags::TILT_ON_CLOSED
            }
            Self::DuoliteTilt180 => {
                ShadeCapabilityFlags::PRIMARY_RAIL
                    | ShadeCapabilityFlags::SECONDARY_RAIL
                    | ShadeCapabilityFlags::TILT_ANYWHERE
                    | ShadeCapabilityFlags::TILT_180
            }
            Self::Illuminated => {
                ShadeCapabilityFlags::PRIMARY_RAIL | ShadeCapabilityFlags::SECONDARY_RAIL
            }
            Self::Unknown(_) => ShadeCapabilityFlags::empty(),
        }
    }
}

bitflags::bitflags! {
    pub struct ShadeCapabilityFlags : u8 {
        const PRIMARY_RAIL = 1;
        const SECONDARY_RAIL = 2;
        const TILT_ON_CLOSED = 4;
        const TILT_ANYWHERE = 8;
        const TILT_180 = 16;
        const PRIMARY_RAIL_REVERSED = 32;
        const SECONDARY_RAIL_OVERLAPPED = 64;
    }
}

// ── PowerType / BatteryStatus ─────────────────────────────────────────────────

#[derive(Serialize_repr, Deserialize_repr, Debug, Copy, Clone, PartialEq, Eq)]
#[repr(i32)]
pub enum PowerType {
    Battery = 0,
    Hardwired = 1,
    Rechargeable = 2,
}

#[derive(Serialize_repr, Deserialize_repr, Debug, Copy, Clone, PartialEq, Eq)]
#[repr(i32)]
pub enum BatteryStatus {
    NoPower = 0,
    Low = 1,
    Medium = 2,
    High = 3,
}

// ── ShadeUpdateMotion ─────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone, Copy, clap::ValueEnum)]
#[serde(rename_all = "camelCase")]
pub enum ShadeUpdateMotion {
    Down,
    Jog,
    LeftTilt,
    RightTilt,
    Stop,
    Up,
}

// ── Gateway / Hub ─────────────────────────────────────────────────────────────

/// Internal wrapper: v3 GET /gateway nests the config under a "config" key.
#[derive(Deserialize)]
pub(crate) struct GatewayResponse {
    pub config: GatewayConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GatewayConfig {
    pub serial_number: String,
    pub brand: String,
    pub model: String,
    pub firmware: GatewayFirmwareVersions,
    pub network_status: GatewayNetworkStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GatewayFirmwareVersions {
    pub main_processor: GatewayFirmware,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GatewayFirmware {
    pub name: String,
    pub revision: i32,
    pub sub_revision: i32,
    pub build: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GatewayNetworkStatus {
    pub ip_address: String,
    pub primary_mac_address: String,
}

// ── SSE Events ────────────────────────────────────────────────────────────────

/// Wire format: JSON in SSE `data:` lines, delimited by blank lines.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ShadeEvent {
    pub evt: ShadeEventKind,
    pub id: i32,
    pub current_positions: Option<ShadePosition>,
    pub iso_date: Option<String>,
}

/// `#[serde(other)]` on `Unknown` provides forward compatibility:
/// any unrecognised evt string deserializes as Unknown instead of an error.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ShadeEventKind {
    ShadeOffline,
    ShadeOnline,
    MotionStarted,
    MotionStopped,
    BatteryAlert,
    #[serde(other)]
    Unknown,
}
```

- [ ] **Step 2: Run cargo check — expect errors only in other files**

```bash
cargo check 2>&1 | grep "^error\[" | grep "api_types" | head -5
```

Expected: no output (no errors inside `api_types.rs`). Errors in hub.rs and others are expected.

- [ ] **Step 3: Commit**

```bash
git add src/api_types.rs
git commit -m "feat: replace v2 api types with v3 types (ShadeData, GatewayConfig, ShadeEvent)"
```

---

### Task 3: Rewrite src/hub.rs

**Files:**
- Modify: `src/hub.rs`

- [ ] **Step 1: Replace hub.rs with v3 implementation**

Write `src/hub.rs`:

```rust
use crate::api_types::*;
use crate::discovery::resolve_hub;
use crate::http_helpers::{get_request_with_json_response, request_with_json_response};
use anyhow::Context;
use async_stream::try_stream;
use futures_util::{Stream, StreamExt};
use reqwest::Method;
use serde_json::json;
use std::net::IpAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Hub {
    addr: IpAddr,
}

impl Hub {
    fn url(&self, extra: &str) -> String {
        format!("http://{}/{extra}", self.addr)
    }

    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    pub async fn list_rooms(&self) -> anyhow::Result<Vec<RoomData>> {
        let mut rooms: Vec<RoomData> =
            get_request_with_json_response(self.url("home/rooms")).await?;
        rooms.sort_by(|a, b| a.pt_name.cmp(&b.pt_name));
        Ok(rooms)
    }

    pub async fn list_scenes(&self) -> anyhow::Result<Vec<Scene>> {
        let mut scenes: Vec<Scene> =
            get_request_with_json_response(self.url("home/scenes")).await?;
        scenes.sort_by(|a, b| a.pt_name.cmp(&b.pt_name));
        Ok(scenes)
    }

    /// Fetches all shades; filters by room_id client-side if provided
    /// (v3 GET /home/shades has no server-side room parameter).
    pub async fn list_shades(&self, room_id: Option<i32>) -> anyhow::Result<Vec<ShadeData>> {
        let mut shades: Vec<ShadeData> =
            get_request_with_json_response(self.url("home/shades")).await?;
        if let Some(id) = room_id {
            shades.retain(|s| s.room_id == id);
        }
        shades.sort_by(|a, b| a.pt_name.cmp(&b.pt_name));
        Ok(shades)
    }

    pub fn with_addr(addr: IpAddr) -> Self {
        Self { addr }
    }

    pub async fn discover(timeout: Duration) -> anyhow::Result<Self> {
        let addr = resolve_hub(timeout).await.context(
            "Failed to discover the PowerView Hub. \
             Ensure that pview is running on the same network as the Hub!",
        )?;
        Ok(Self::with_addr(addr))
    }

    pub async fn room_by_name(&self, name: &str) -> anyhow::Result<RoomData> {
        let rooms = self.list_rooms().await?;
        for room in rooms {
            if room.pt_name.eq_ignore_ascii_case(name) || room.id.to_string() == name {
                return Ok(room);
            }
        }
        anyhow::bail!("No room with name or id matching '{name}' was found");
    }

    pub async fn scene_by_name(&self, name: &str) -> anyhow::Result<Scene> {
        let scenes = self.list_scenes().await?;
        for s in scenes {
            if s.pt_name.eq_ignore_ascii_case(name) || s.id.to_string() == name {
                return Ok(s);
            }
        }
        anyhow::bail!("No scene with name or id matching '{name}' was found");
    }

    pub async fn shade_by_id(&self, shade_id: i32) -> anyhow::Result<ShadeData> {
        get_request_with_json_response(self.url(&format!("home/shades/{shade_id}"))).await
    }

    pub async fn shade_by_name(&self, name: &str) -> anyhow::Result<ShadeData> {
        let shades = self.list_shades(None).await?;
        for shade in shades {
            if shade.pt_name.eq_ignore_ascii_case(name) || shade.id.to_string() == name {
                return Ok(shade);
            }
        }
        anyhow::bail!("No shade with name or id matching '{name}' was found");
    }

    /// Sends a motion command to the hub.
    /// Up/Down/LeftTilt/RightTilt map to `PUT home/shades/{id}/positions`.
    /// Jog maps to `PUT home/shades/{id}/motion {"motion":"jog"}`.
    /// Stop maps to `PUT home/shades/stop {"ids":[id]}`.
    pub async fn move_shade(
        &self,
        shade_id: i32,
        motion: ShadeUpdateMotion,
    ) -> anyhow::Result<()> {
        match motion {
            ShadeUpdateMotion::Up => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition { primary: Some(1.0), ..Default::default() },
                )
                .await
            }
            ShadeUpdateMotion::Down => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition { primary: Some(0.0), ..Default::default() },
                )
                .await
            }
            ShadeUpdateMotion::LeftTilt => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition { tilt: Some(0.0), ..Default::default() },
                )
                .await
            }
            ShadeUpdateMotion::RightTilt => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition { tilt: Some(1.0), ..Default::default() },
                )
                .await
            }
            ShadeUpdateMotion::Jog => {
                let url = self.url(&format!("home/shades/{shade_id}/motion"));
                let _: serde_json::Value =
                    request_with_json_response(Method::PUT, url, &json!({"motion": "jog"}))
                        .await?;
                Ok(())
            }
            ShadeUpdateMotion::Stop => {
                let url = self.url("home/shades/stop");
                let _: serde_json::Value =
                    request_with_json_response(Method::PUT, url, &json!({"ids": [shade_id]}))
                        .await?;
                Ok(())
            }
        }
    }

    /// Sets shade position via `PUT home/shades/{id}/positions`.
    /// Only `Some` fields are serialized; the hub preserves axes not included in the body.
    pub async fn set_shade_position(
        &self,
        shade_id: i32,
        position: ShadePosition,
    ) -> anyhow::Result<()> {
        let url = self.url(&format!("home/shades/{shade_id}/positions"));
        let _: serde_json::Value =
            request_with_json_response(Method::PUT, url, &json!({ "positions": position }))
                .await?;
        Ok(())
    }

    /// Activates a scene. Returns the list of affected shade ids.
    /// v3 returns a bare JSON array, not a `{shadeIds:[...]}` wrapper.
    pub async fn activate_scene(&self, scene_id: i32) -> anyhow::Result<Vec<i32>> {
        let url = self.url(&format!("home/scenes/{scene_id}/activate"));
        let ids: Vec<i32> =
            request_with_json_response(Method::PUT, url, &json!({})).await?;
        Ok(ids)
    }

    pub async fn get_gateway_data(&self) -> anyhow::Result<GatewayConfig> {
        let resp: GatewayResponse =
            get_request_with_json_response(self.url("gateway")).await?;
        Ok(resp.config)
    }

    /// Opens the SSE shade events stream at `GET home/shades/events?sse=true`.
    /// Uses a dedicated reqwest client with no timeout (long-lived connection).
    /// SSE events delimited by blank lines; only `data:` lines are parsed.
    /// `Unknown` events are logged at debug level and not yielded.
    pub async fn shade_events_stream(
        &self,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<ShadeEvent>>> {
        let client = reqwest::Client::builder().timeout(None).build()?;
        let url = format!("http://{}/home/shades/events?sse=true", self.addr);
        let response = client.get(&url).send().await?;
        let byte_stream = response.bytes_stream();

        let stream = try_stream! {
            let mut buffer = String::new();
            tokio::pin!(byte_stream);
            while let Some(chunk) = byte_stream.next().await {
                let chunk = chunk?;
                buffer.push_str(&String::from_utf8_lossy(&chunk));
                loop {
                    // SSE events are separated by blank lines (\r\n\r\n or \n\n)
                    let delim = if let Some(pos) = buffer.find("\r\n\r\n") {
                        Some((pos, 4))
                    } else if let Some(pos) = buffer.find("\n\n") {
                        Some((pos, 2))
                    } else {
                        None
                    };
                    let Some((pos, delim_len)) = delim else { break };
                    let event_text = buffer[..pos].to_string();
                    buffer.drain(..pos + delim_len);

                    for line in event_text.lines() {
                        let line = line.trim_end_matches('\r');
                        if let Some(json_str) = line.strip_prefix("data:") {
                            let json_str = json_str.trim();
                            match serde_json::from_str::<ShadeEvent>(json_str) {
                                Ok(event) if event.evt != ShadeEventKind::Unknown => {
                                    yield event;
                                }
                                Ok(_) => {
                                    log::debug!("SSE: unknown event kind in: {json_str}");
                                }
                                Err(e) => {
                                    log::warn!("SSE: failed to parse event: {e:#} — {json_str}");
                                }
                            }
                        }
                    }
                }
            }
        };

        Ok(stream)
    }
}
```

- [ ] **Step 2: Run cargo check — expect errors only in other files**

```bash
cargo check 2>&1 | grep "^error\[" | grep "hub.rs" | head -5
```

Expected: no output (no errors inside `hub.rs`).

- [ ] **Step 3: Commit**

```bash
git add src/hub.rs
git commit -m "feat: update hub client to v3 endpoints with SSE stream"
```

---

### Task 4: Update src/discovery.rs

**Files:**
- Modify: `src/discovery.rs`

Changes: `UserData` → `GatewayConfig`; `user_data` field renamed to `gateway_data`; call `get_gateway_data()`.

- [ ] **Step 1: Replace discovery.rs**

Write `src/discovery.rs`:

```rust
use crate::api_types::GatewayConfig;
use crate::hub::Hub;
use anyhow::Context;
use std::net::IpAddr;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use wez_mdns::{QueryParameters, RecordKind};

pub const POWERVIEW_SERVICE: &str = "_powerview._tcp.local";

fn ip_from_response(response: wez_mdns::Response) -> anyhow::Result<IpAddr> {
    let mut ipv4 = None;
    let mut ipv6 = None;

    for record in &response.additional {
        match record.kind {
            RecordKind::A(v4) => {
                ipv4.replace(v4);
            }
            RecordKind::AAAA(v6) => {
                ipv6.replace(v6);
            }
            _ => {}
        }
    }

    if let Some(v4) = ipv4 {
        Ok(v4.into())
    } else if let Some(v6) = ipv6 {
        Ok(v6.into())
    } else {
        anyhow::bail!(
            "Response didn't include either a v4 or v6 address for the hub. {response:?}"
        );
    }
}

/// Discover a hub on the local network
pub async fn resolve_hub(timeout: Duration) -> anyhow::Result<IpAddr> {
    let params = QueryParameters {
        timeout_after: Some(timeout),
        ..QueryParameters::SERVICE_LOOKUP
    };

    let disco_rx = wez_mdns::resolve(POWERVIEW_SERVICE, params)
        .await
        .context("MDNS discovery")?;
    let mut responses = vec![];
    while let Ok(response) = disco_rx.recv().await {
        match ip_from_response(response) {
            Ok(addr) => return Ok(addr),
            Err(err) => {
                responses.push(format!("{err:#?}"));
            }
        }
    }

    anyhow::bail!(
        "Unable to discover PowerView Hub within {timeout:?}. {}",
        responses.join(", ")
    );
}

#[derive(Clone, Debug)]
pub struct ResolvedHub {
    pub hub: Hub,
    pub gateway_data: Option<GatewayConfig>,
}

impl ResolvedHub {
    async fn new(addr: IpAddr) -> Self {
        let hub = Hub::with_addr(addr);
        Self::with_hub(hub).await
    }

    pub async fn with_hub(hub: Hub) -> Self {
        let gateway_data = hub.get_gateway_data().await.ok();
        ResolvedHub { hub, gateway_data }
    }
}

impl std::ops::Deref for ResolvedHub {
    type Target = Hub;
    fn deref(&self) -> &Hub {
        &self.hub
    }
}

pub async fn resolve_hub_with_serial(
    timeout: Option<Duration>,
    serial: &str,
) -> anyhow::Result<Hub> {
    let mut rx = resolve_hubs(timeout).await?;
    while let Some(hub) = rx.recv().await {
        if let Some(gateway_data) = &hub.gateway_data {
            if gateway_data.serial_number == serial {
                return Ok(hub.hub);
            }
        }
    }
    anyhow::bail!("No hub found with serial {serial}");
}

pub async fn resolve_hubs(timeout: Option<Duration>) -> anyhow::Result<Receiver<ResolvedHub>> {
    let params = QueryParameters {
        timeout_after: timeout,
        ..QueryParameters::DISCOVERY
    };

    let disco_rx = wez_mdns::resolve(POWERVIEW_SERVICE, params)
        .await
        .context("MDNS discovery")?;
    let (tx, rx) = tokio::sync::mpsc::channel(8);

    tokio::spawn(async move {
        while let Ok(response) = disco_rx.recv().await {
            match ip_from_response(response) {
                Ok(addr) => {
                    let resolved = ResolvedHub::new(addr).await;
                    if let Err(err) = tx.send(resolved).await {
                        log::error!("resolve_hubs: tx.send error: {err:#?}");
                        break;
                    }
                }
                Err(err) => {
                    log::debug!("{err:#?}");
                }
            }
        }
    });

    Ok(rx)
}
```

- [ ] **Step 2: Run cargo check**

```bash
cargo check 2>&1 | grep "^error\[" | grep -E "discovery\.rs" | head -5
```

Expected: no output (no errors in `discovery.rs`).

- [ ] **Step 3: Commit**

```bash
git add src/discovery.rs
git commit -m "feat: discovery.rs uses GatewayConfig instead of UserData"
```

---

### Task 5: Update simple command files

**Files:**
- Modify: `src/commands/hub_info.rs`
- Modify: `src/commands/list_hubs.rs`

#### hub_info.rs

- [ ] **Step 1: Replace hub_info.rs**

```rust
/// Show diagnostic information for the hub
#[derive(clap::Parser, Debug)]
pub struct HubInfoCommand {}

impl HubInfoCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let hub = args.hub().await?;
        let config = hub.get_gateway_data().await?;
        let fw = &config.firmware.main_processor;
        println!("Serial:    {}", config.serial_number);
        println!("Brand:     {}", config.brand);
        println!("Model:     {}", config.model);
        println!(
            "Firmware:  {}.{}.{} ({})",
            fw.revision, fw.sub_revision, fw.build, fw.name
        );
        println!("IP:        {}", config.network_status.ip_address);
        println!("MAC:       {}", config.network_status.primary_mac_address);
        Ok(())
    }
}
```

#### list_hubs.rs

- [ ] **Step 2: Update list_hubs.rs to use gateway_data**

Replace `hub.user_data` references with `hub.gateway_data`:

```rust
use std::time::Duration;

/// Discover and list the hubs on your network
#[derive(clap::Parser, Debug)]
pub struct ListHubsCommand {
    /// How long to wait for discovery to complete, in seconds
    #[arg(long, default_value = "15")]
    timeout: u64,
}

impl ListHubsCommand {
    pub async fn run(&self, _args: &crate::Args) -> anyhow::Result<()> {
        let mut hubs =
            crate::discovery::resolve_hubs(Some(Duration::from_secs(self.timeout))).await?;

        while let Some(hub) = hubs.recv().await {
            if let Some(gateway_data) = &hub.gateway_data {
                println!(
                    "{addr} SN={serial} MAC={mac}",
                    addr = hub.hub.addr(),
                    serial = gateway_data.serial_number,
                    mac = gateway_data.network_status.primary_mac_address,
                );
            } else {
                println!("{} (Not responding)", hub.hub.addr());
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 3: Run cargo check — expect errors only in serve_mqtt.rs**

```bash
cargo check 2>&1 | grep "^error\[" | grep -v "serve_mqtt" | head -10
```

Expected: no output (only serve_mqtt.rs should still have errors).

- [ ] **Step 4: Commit**

```bash
git add src/commands/hub_info.rs src/commands/list_hubs.rs
git commit -m "feat: update hub_info and list_hubs for v3 GatewayConfig"
```

---

### Task 6: Update list_shades.rs, list_scenes.rs, and move_shade.rs

**Files:**
- Modify: `src/commands/list_shades.rs`
- Modify: `src/commands/list_scenes.rs`
- Modify: `src/commands/move_shade.rs`

#### list_shades.rs

Changes: `list_shades(opt_room_id)` (one arg); `shade.room_id` non-optional; `room.pt_name`; positions always present; secondary rail name uses pt_name.

- [ ] **Step 1: Replace list_shades.rs**

```rust
use crate::api_types::ShadeCapabilityFlags;
use std::collections::BTreeMap;
use tabout::{Alignment, Column};

/// List shades and their current positions
#[derive(clap::Parser, Debug)]
pub struct ListShadesCommand {
    /// Only return shades in the specified room
    #[clap(long)]
    room: Option<String>,
}

impl ListShadesCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let hub = args.hub().await?;

        let opt_room_id = match &self.room {
            Some(name) => Some(hub.room_by_name(name).await?.id),
            None => None,
        };

        let rooms = hub.list_rooms().await?;
        let shades = hub.list_shades(opt_room_id).await?;

        let mut shades_by_room: BTreeMap<i32, Vec<_>> = BTreeMap::new();
        for shade in shades {
            shades_by_room.entry(shade.room_id).or_default().push(shade);
        }

        let columns = &[
            Column { name: "ROOM".to_string(), alignment: Alignment::Left },
            Column { name: "SHADE".to_string(), alignment: Alignment::Left },
            Column { name: "POSITION".to_string(), alignment: Alignment::Right },
        ];
        let mut rows = vec![];
        for room_data in &rooms {
            if let Some(shades) = shades_by_room.get(&room_data.id) {
                for shade in shades {
                    rows.push(vec![
                        room_data.pt_name.clone(),
                        shade.pt_name.clone(),
                        shade.positions.describe_pos1(),
                    ]);
                    if shade
                        .capabilities
                        .flags()
                        .contains(ShadeCapabilityFlags::SECONDARY_RAIL)
                    {
                        rows.push(vec![
                            room_data.pt_name.clone(),
                            format!("{} Middle Rail", shade.pt_name),
                            shade.positions.describe_pos2(),
                        ]);
                    }
                }
            }
        }
        println!("{}", tabout::tabulate_output_as_string(columns, &rows)?);
        Ok(())
    }
}
```

#### list_scenes.rs

Changes: remove scene members and shades lookups; show SCENE + ROOMS columns; fix `--room` filter to use `room_ids.contains`.

- [ ] **Step 2: Replace list_scenes.rs**

```rust
use std::collections::HashMap;
use tabout::{Alignment, Column};

/// List scenes and their associated rooms
#[derive(clap::Parser, Debug)]
pub struct ListScenesCommand {
    /// Only return scenes in the specified room
    #[clap(long)]
    room: Option<String>,
}

impl ListScenesCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let hub = args.hub().await?;
        let mut scenes = hub.list_scenes().await?;

        if let Some(room) = &self.room {
            let room = hub.room_by_name(room).await?;
            scenes.retain(|scene| scene.room_ids.contains(&room.id));
        }

        let room_by_id: HashMap<i32, String> = hub
            .list_rooms()
            .await?
            .into_iter()
            .map(|r| (r.id, r.pt_name))
            .collect();

        let columns = &[
            Column { name: "SCENE".to_string(), alignment: Alignment::Left },
            Column { name: "ROOMS".to_string(), alignment: Alignment::Left },
        ];
        let mut rows = vec![];
        for scene in &scenes {
            let room_names: Vec<&str> = scene
                .room_ids
                .iter()
                .filter_map(|id| room_by_id.get(id).map(|s| s.as_str()))
                .collect();
            rows.push(vec![scene.pt_name.clone(), room_names.join(", ")]);
        }
        println!("{}", tabout::tabulate_output_as_string(columns, &rows)?);
        Ok(())
    }
}
```

#### move_shade.rs

Changes: `shade_by_name` returns `ShadeData` directly; `--percent` always sets primary axis via `set_shade_position`; remove `is_primary()` branch; `move_shade` returns `()`.

- [ ] **Step 3: Replace move_shade.rs**

```rust
use crate::api_types::{ShadePosition, ShadeUpdateMotion};

#[derive(clap::Args, Debug)]
#[group(required = true)]
struct TargetPosition {
    #[arg(long, conflicts_with = "percent")]
    motion: Option<ShadeUpdateMotion>,
    #[arg(long, group = "position")]
    percent: Option<u8>,
}

/// Move or set the position of a shade
#[derive(clap::Parser, Debug)]
pub struct MoveShadeCommand {
    /// The name or id of the shade to move.
    /// Names will be compared ignoring case.
    name: String,
    #[command(flatten)]
    target_position: TargetPosition,
}

impl MoveShadeCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let hub = args.hub().await?;
        let shade = hub.shade_by_name(&self.name).await?;

        if let Some(motion) = self.target_position.motion {
            hub.move_shade(shade.id, motion).await?;
            println!("Sent {:?} to {} (id={})", motion, shade.pt_name, shade.id);
        } else if let Some(percent) = self.target_position.percent {
            let pos = ShadePosition {
                primary: Some(ShadePosition::percent_to_pos(percent)),
                ..Default::default()
            };
            hub.set_shade_position(shade.id, pos).await?;
            println!("Set {} (id={}) primary to {percent}%", shade.pt_name, shade.id);
        } else {
            anyhow::bail!("One of --motion or --percent is required");
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Run cargo check — expect errors only in serve_mqtt.rs**

```bash
cargo check 2>&1 | grep "^error\[" | grep -v "serve_mqtt" | head -10
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add src/commands/list_shades.rs src/commands/list_scenes.rs src/commands/move_shade.rs
git commit -m "feat: update list_shades, list_scenes, move_shade for v3 API"
```

---

### Task 7: Update src/commands/inspect_shade.rs

**Files:**
- Modify: `src/commands/inspect_shade.rs`

The current file just `println!("{shade:#?}")`. The spec requires formatted output for signal strength (dBm) and positions (named percentages). Since `shade_by_name` now returns `ShadeData` directly (not `ResolvedShadeData`), this compiles cleanly after hub.rs changes but needs formatted display.

- [ ] **Step 1: Replace inspect_shade.rs with formatted output**

```rust
/// Show diagnostic information about a shade
#[derive(clap::Parser, Debug)]
pub struct InspectShadeCommand {
    /// The name or id of the shade to inspect.
    /// Names will be compared ignoring case.
    name: String,
}

impl InspectShadeCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let hub = args.hub().await?;
        let shade = hub.shade_by_name(&self.name).await?;

        println!("Name:          {}", shade.pt_name);
        println!("ID:            {}", shade.id);
        println!("Serial:        {}", shade.serial_number);
        println!("Room ID:       {}", shade.room_id);
        println!("Type:          {}", shade.shade_type);
        println!("Capabilities:  {:?}", shade.capabilities);
        println!("Power Type:    {:?}", shade.power_type);
        if let Some(bat) = shade.battery_status {
            println!("Battery:       {:?} ({}%)", bat, shade.battery_percent().unwrap_or(0));
        } else {
            println!("Battery:       unavailable");
        }
        if let Some(dbm) = shade.signal_strength {
            println!(
                "Signal:        {dbm:.0} dBm ({}%)",
                shade.signal_strength_percent().unwrap_or(0)
            );
        } else {
            println!("Signal:        unavailable");
        }
        let fw = &shade.firmware;
        println!("Firmware:      {}.{}.{}", fw.revision, fw.sub_revision, fw.build);
        println!("Positions:     {}", shade.positions.describe());
        Ok(())
    }
}
```

- [ ] **Step 2: Run cargo check — expect errors only in serve_mqtt.rs**

```bash
cargo check 2>&1 | grep "^error\[" | grep -v "serve_mqtt" | head -5
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add src/commands/inspect_shade.rs
git commit -m "feat: update inspect_shade for v3 formatted output"
```

---

### Task 8: Update serve_mqtt.rs — imports, structs, state (first pass)

**Files:**
- Modify: `src/commands/serve_mqtt.rs`

This is the largest file. We split its update into three tasks (7, 8, 9).

- [ ] **Step 1: Replace the imports section (lines 1–24)**

Replace everything from the top of the file through the imports with:

```rust
use crate::api_types::{
    GatewayConfig, PowerType, ShadeCapabilityFlags, ShadeData, ShadeEvent, ShadeEventKind,
    ShadePosition, ShadeUpdateMotion,
};
use crate::discovery::ResolvedHub;
use crate::hass_helper::*;
use crate::http_helpers::LockedError;
use crate::hub::Hub;
use crate::opt_env_var;
use crate::version_info::pview_version;
use anyhow::Context;
use arc_swap::ArcSwap;
use futures_util::StreamExt;
use mosquitto_rs::router::*;
use mosquitto_rs::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
```

- [ ] **Step 2: Replace the `ServeMqttCommand` struct (remove `bind_address` field)**

The new struct (no `bind_address`):

```rust
/// Launch the pv2mqtt bridge, adding your hub to Home Assistant
#[derive(clap::Parser, Debug)]
pub struct ServeMqttCommand {
    /// The mqtt broker hostname or address.
    /// You may also set this via the PV_MQTT_HOST environment variable.
    #[arg(long)]
    host: Option<String>,

    /// The mqtt broker port
    /// You may also set this via the PV_MQTT_PORT environment variable.
    /// If unspecified, uses 1883
    #[arg(long)]
    port: Option<u16>,

    /// The username to authenticate against the broker
    /// You may also set this via the PV_MQTT_USER environment variable.
    #[arg(long)]
    username: Option<String>,
    /// The password to authenticate against the broker
    /// You may also set this via the PV_MQTT_PASSWORD environment variable.
    #[arg(long)]
    password: Option<String>,

    #[arg(long, default_value = "homeassistant")]
    discovery_prefix: String,
}
```

- [ ] **Step 3: Replace the `ServerEvent` enum**

Remove `HomeAutomationData` variant, add `ShadeEvent`:

```rust
enum ServerEvent {
    MqttMessage {
        router: Arc<MqttRouter<Arc<Pv2MqttState>>>,
        msg: Message,
    },
    ShadeEvent(ShadeEvent),
    PeriodicStateUpdate,
    HubDiscovered(ResolvedHub),
}
```

- [ ] **Step 4: Replace `FullyResolvedHub` and `Pv2MqttState` structs (near bottom of file)**

Find and replace (around line 1526–1556):

```rust
struct FullyResolvedHub {
    hub: Hub,
    gateway_data: GatewayConfig,
}

struct Pv2MqttState {
    hub: ArcSwap<FullyResolvedHub>,
    client: Client,
    serial: String,
    discovery_prefix: String,
    first_run: AtomicBool,
    responding: AtomicBool,
}

impl Pv2MqttState {
    pub fn battery_availability_topic(&self, shade: &ShadeData) -> String {
        format!(
            "{MODEL}/sensor/{}/{}/battery/availability",
            self.serial, shade.id
        )
    }

    pub fn battery_state_topic(&self, shade: &ShadeData) -> String {
        format!("{MODEL}/sensor/{}-{}-battery/state", self.serial, shade.id)
    }

    pub fn power_type_state_topic(&self, shade: &ShadeData) -> String {
        format!("{MODEL}/sensor/{}/{}/psu/state", self.serial, shade.id)
    }
}
```

- [ ] **Step 5: Run cargo check to see progress**

```bash
cargo check 2>&1 | grep "^error\[" | head -15
```

Expected: many errors — functions still reference old types. That's expected at this stage.

---

### Task 9: Update serve_mqtt.rs — registration functions

**Files:**
- Modify: `src/commands/serve_mqtt.rs`

- [ ] **Step 1: Add `power_type_to_state` helper and replace `register_diagnostic_entity`**

Add this free function before `register_hub`:

```rust
fn power_type_to_state(power_type: PowerType) -> &'static str {
    match power_type {
        PowerType::Hardwired => HARD_WIRED_LABEL,
        PowerType::Battery => BATTERY_LABEL,
        PowerType::Rechargeable => RECHARGEABLE_LABEL,
    }
}
```

Replace the `register_diagnostic_entity` function (change `user_data: &UserData` → `gateway_data: &GatewayConfig`):

```rust
async fn register_diagnostic_entity(
    diagnostic: DiagnosticEntity,
    gateway_data: &GatewayConfig,
    state: &Arc<Pv2MqttState>,
    reg: &mut HassRegistration,
) -> anyhow::Result<()> {
    let serial = &gateway_data.serial_number;
    let unique_id = &diagnostic.unique_id;

    let config = SensorConfig {
        base: EntityConfig {
            name: Some(diagnostic.name),
            availability_topic: format!("{MODEL}/sensor/{unique_id}/availability"),
            device: Device {
                identifiers: vec![
                    format!("{MODEL}-{serial}"),
                    gateway_data.serial_number.to_string(),
                    gateway_data.network_status.primary_mac_address.to_string(),
                ],
                connections: vec![(
                    "mac".to_string(),
                    gateway_data.network_status.primary_mac_address.to_string(),
                )],
                name: format!("PowerView Hub {serial}"),
                manufacturer: WEZ.to_string(),
                model: MODEL.to_string(),
                sw_version: Some(pview_version().to_string()),
                suggested_area: None,
                via_device: None,
            },
            device_class: None,
            origin: Origin::default(),
            unique_id: unique_id.to_string(),
            entity_category: Some("diagnostic".to_string()),
            icon: None,
        },
        state_topic: format!("{MODEL}/sensor/{unique_id}/state"),
        unit_of_measurement: None,
    };

    reg.config(
        format!("{}/sensor/{unique_id}/config", state.discovery_prefix),
        serde_json::to_string(&config)?,
    );
    reg.update(config.base.availability_topic, "online");
    reg.update(format!("{MODEL}/sensor/{unique_id}/state"), diagnostic.value);
    Ok(())
}
```

- [ ] **Step 2: Replace `register_hub` (remove rfStatus, use GatewayConfig)**

```rust
async fn register_hub(
    gateway_data: &GatewayConfig,
    state: &Arc<Pv2MqttState>,
    reg: &mut HassRegistration,
) -> anyhow::Result<()> {
    let serial = &gateway_data.serial_number;
    register_diagnostic_entity(
        DiagnosticEntity {
            name: "IP Address".to_string(),
            unique_id: format!("{serial}-hub-ip"),
            value: gateway_data.network_status.ip_address.clone(),
        },
        gateway_data,
        state,
        reg,
    )
    .await?;

    register_diagnostic_entity(
        DiagnosticEntity {
            name: "Status".to_string(),
            unique_id: format!("{serial}-responding"),
            value: if state.responding.load(Ordering::SeqCst) { "OK" } else { "UNRESPONSIVE" }
                .to_string(),
        },
        gateway_data,
        state,
        reg,
    )
    .await?;

    Ok(())
}
```

- [ ] **Step 3: Replace `register_shades`**

Key changes: one-arg `list_shades(None)`; `room_id` non-optional; `pt_name` throughout; positions always present; firmware always present; remove calibrate/heart/refresh_battery/refresh_position buttons; change Power Source to `SensorConfig`.

```rust
async fn register_shades(
    state: &Arc<Pv2MqttState>,
    reg: &mut HassRegistration,
) -> anyhow::Result<()> {
    let hub = state.hub.load();
    let shades = hub.hub.list_shades(None).await?;
    let room_by_id: HashMap<i32, String> = hub
        .hub
        .list_rooms()
        .await?
        .into_iter()
        .map(|room| (room.id, room.pt_name))
        .collect();

    let serial = &state.serial;

    for shade in &shades {
        let position = &shade.positions;
        let mut shade_ids = vec![(shade.id.to_string(), None, position.pos1_percent())];

        if shade
            .capabilities
            .flags()
            .contains(ShadeCapabilityFlags::SECONDARY_RAIL)
        {
            shade_ids.push((
                format!("{}{SECONDARY_SUFFIX}", shade.id),
                Some("Middle Rail".to_string()),
                position.pos2_percent(),
            ));
        }

        let area = room_by_id.get(&shade.room_id).cloned();
        let device_id = format!("{serial}-{}", shade.id);
        let device = Device {
            suggested_area: area,
            identifiers: vec![device_id.clone()],
            via_device: Some(format!("{MODEL}-{serial}")),
            name: shade.pt_name.clone(),
            manufacturer: HUNTER_DOUGLAS.to_string(),
            model: MODEL.to_string(),
            connections: vec![],
            sw_version: Some(format!(
                "{}.{}.{}",
                shade.firmware.revision, shade.firmware.sub_revision, shade.firmware.build
            )),
        };

        for (shade_id, shade_name, pos) in shade_ids {
            let unique_id = format!("{serial}-{shade_id}");
            let config = CoverConfig {
                base: EntityConfig {
                    unique_id,
                    name: shade_name,
                    availability_topic: format!(
                        "{MODEL}/shade/{serial}/{shade_id}/availability"
                    ),
                    device_class: Some("shade".to_string()),
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: None,
                    icon: None,
                },
                command_topic: format!("{MODEL}/shade/{serial}/{shade_id}/command"),
                position_topic: format!("{MODEL}/shade/{serial}/{shade_id}/position"),
                set_position_topic: format!(
                    "{MODEL}/shade/{serial}/{shade_id}/set_position"
                ),
                state_topic: format!("{MODEL}/shade/{serial}/{shade_id}/state"),
            };

            reg.delete(format!(
                "{}/cover/{shade_id}/config",
                state.discovery_prefix
            ));
            reg.config(
                format!(
                    "{}/cover/{serial}-{shade_id}/config",
                    state.discovery_prefix
                ),
                serde_json::to_string(&config)?,
            );
            reg.update(config.base.availability_topic, "online");
            if let Some(pos) = pos {
                reg.update(
                    format!("{MODEL}/shade/{serial}/{shade_id}/position"),
                    format!("{pos}"),
                );
                let state_label = if pos == 0 { "closed" } else { "open" };
                reg.update(
                    format!("{MODEL}/shade/{serial}/{shade_id}/state"),
                    state_label,
                );
            }
        }

        {
            let jog = ButtonConfig {
                base: EntityConfig {
                    unique_id: format!("{device_id}-jog"),
                    name: Some("Jog".to_string()),
                    availability_topic: format!(
                        "{MODEL}/shade/{serial}/{}/jog/availability",
                        shade.id
                    ),
                    device_class: None,
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: Some("diagnostic".to_string()),
                    icon: None,
                },
                command_topic: format!("{MODEL}/shade/{serial}/{}/command", shade.id),
                payload_press: Some("JOG".to_string()),
            };
            reg.delete(format!(
                "{}/button/{device_id}-jog/config",
                state.discovery_prefix
            ));
            reg.config(
                format!("{}/button/{device_id}-jog/config", state.discovery_prefix),
                serde_json::to_string(&jog)?,
            );
            reg.update(jog.base.availability_topic, "online");
        }

        {
            let battery = SensorConfig {
                base: EntityConfig {
                    unique_id: format!("{device_id}-battery"),
                    name: Some("Battery".to_string()),
                    availability_topic: state.battery_availability_topic(shade),
                    device_class: Some("battery".to_string()),
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: Some("diagnostic".to_string()),
                    icon: None,
                },
                state_topic: state.battery_state_topic(shade),
                unit_of_measurement: Some("%".to_string()),
            };
            reg.delete(format!(
                "{}/sensor/{device_id}-battery/config",
                state.discovery_prefix
            ));
            reg.config(
                format!(
                    "{}/sensor/{device_id}-battery/config",
                    state.discovery_prefix
                ),
                serde_json::to_string(&battery)?,
            );
            if let Some(pct) = shade.battery_percent() {
                reg.update(battery.base.availability_topic, "online");
                reg.update(battery.state_topic, format!("{pct}"));
            } else {
                reg.update(battery.base.availability_topic, "offline");
            }
        }

        {
            let signal = SensorConfig {
                base: EntityConfig {
                    unique_id: format!("{device_id}-signal"),
                    name: Some("Signal Strength".to_string()),
                    availability_topic: format!(
                        "{MODEL}/sensor/{serial}/{}/signal/availability",
                        shade.id
                    ),
                    device_class: None,
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: Some("diagnostic".to_string()),
                    icon: Some("mdi:signal".to_string()),
                },
                state_topic: format!("{MODEL}/sensor/{device_id}-signal/state"),
                unit_of_measurement: Some("%".to_string()),
            };
            reg.delete(format!(
                "{}/sensor/{device_id}-signal/config",
                state.discovery_prefix
            ));
            reg.config(
                format!(
                    "{}/sensor/{device_id}-signal/config",
                    state.discovery_prefix
                ),
                serde_json::to_string(&signal)?,
            );
            if let Some(pct) = shade.signal_strength_percent() {
                reg.update(signal.base.availability_topic, "online");
                reg.update(signal.state_topic, format!("{pct}"));
            } else {
                reg.update(signal.base.availability_topic, "offline");
            }
        }

        {
            // Power Source is now a read-only sensor (v3 power_type is not writable)
            let power_source = SensorConfig {
                base: EntityConfig {
                    unique_id: format!("{device_id}-psu"),
                    name: Some("Power Source".to_string()),
                    availability_topic: format!(
                        "{MODEL}/sensor/{serial}/{}/psu/availability",
                        shade.id
                    ),
                    device_class: None,
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: Some("diagnostic".to_string()),
                    icon: Some("mdi:power-plug-outline".to_string()),
                },
                state_topic: state.power_type_state_topic(shade),
                unit_of_measurement: None,
            };
            // Delete legacy select entity if present
            reg.delete(format!(
                "{}/select/{device_id}-psu/config",
                state.discovery_prefix
            ));
            reg.delete(format!(
                "{}/sensor/{device_id}-psu/config",
                state.discovery_prefix
            ));
            reg.config(
                format!(
                    "{}/sensor/{device_id}-psu/config",
                    state.discovery_prefix
                ),
                serde_json::to_string(&power_source)?,
            );
            reg.update(power_source.base.availability_topic, "online");
            reg.update(
                power_source.state_topic,
                power_type_to_state(shade.power_type).to_string(),
            );
        }
    }

    Ok(())
}
```

- [ ] **Step 4: Replace `register_scenes` (use pt_name and room_ids)**

```rust
async fn register_scenes(
    state: &Arc<Pv2MqttState>,
    reg: &mut HassRegistration,
) -> anyhow::Result<()> {
    let hub = state.hub.load();
    let scenes = hub.hub.list_scenes().await?;
    let room_by_id: HashMap<i32, String> = hub
        .hub
        .list_rooms()
        .await?
        .into_iter()
        .map(|room| (room.id, room.pt_name))
        .collect();

    let serial = &state.serial;

    for scene in scenes {
        let scene_id = scene.id;
        let scene_name = scene.pt_name.clone();
        let suggested_area = scene
            .room_ids
            .first()
            .and_then(|id| room_by_id.get(id))
            .cloned();

        let unique_id = format!("{serial}-scene-{scene_id}");

        let config = SceneConfig {
            base: EntityConfig {
                device: Device {
                    suggested_area,
                    identifiers: vec![unique_id.clone()],
                    via_device: Some(format!("{MODEL}-{serial}")),
                    name: scene_name,
                    manufacturer: HUNTER_DOUGLAS.to_string(),
                    model: MODEL.to_string(),
                    connections: vec![],
                    sw_version: None,
                },
                availability_topic: format!("{MODEL}/scene/{serial}/{scene_id}/availability"),
                device_class: None,
                name: None,
                origin: Origin::default(),
                unique_id: unique_id.clone(),
                entity_category: None,
                icon: None,
            },
            command_topic: format!("{MODEL}/scene/{serial}/{scene_id}/set"),
            payload_on: "ON".to_string(),
        };

        reg.delete(format!(
            "{}/scene/{unique_id}/config",
            state.discovery_prefix
        ));
        reg.config(
            format!("{}/scene/{unique_id}/config", state.discovery_prefix),
            serde_json::to_string(&config)?,
        );
        reg.update(config.base.availability_topic, "online");
    }

    Ok(())
}
```

- [ ] **Step 5: Update `register_with_hass` to use gateway_data**

```rust
async fn register_with_hass(state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
    let mut reg = HassRegistration::new();

    register_hub(&state.hub.load().gateway_data, state, &mut reg)
        .await
        .context("register_hub")?;
    register_shades(state, &mut reg)
        .await
        .context("register_shades")?;
    register_scenes(state, &mut reg)
        .await
        .context("register_scenes")?;
    reg.apply_updates(state).await.context("apply_updates")?;
    Ok(())
}
```

- [ ] **Step 6: Run cargo check**

```bash
cargo check 2>&1 | grep "^error\[" | head -20
```

Expected: fewer errors. Remaining ones are in `ServeMqttCommand` impl methods.

---

### Task 10: Update serve_mqtt.rs — event handlers, MQTT handlers, and run()

**Files:**
- Modify: `src/commands/serve_mqtt.rs`

- [ ] **Step 1: Remove old methods that no longer exist in v3**

Delete these entire methods from the `impl ServeMqttCommand` block:
- `setup_http_server` (~lines 875–934)
- `handle_pv_event` (~lines 1136–1180)
- `update_homeautomation_hook` (~lines 1182–1194)

Delete these free functions:
- `battery_kind_to_state` (~line 821)
- `advise_hass_of_battery_kind` (~lines 829–846)

- [ ] **Step 2: Add `handle_shade_event` method to `impl ServeMqttCommand`**

```rust
async fn handle_shade_event(
    &self,
    state: &Arc<Pv2MqttState>,
    event: ShadeEvent,
) -> anyhow::Result<()> {
    log::debug!("SSE shade event: {event:#?}");
    let hub = state.hub.load();
    match event.evt {
        ShadeEventKind::MotionStopped => {
            if let Some(positions) = &event.current_positions {
                let shade_id_str = format!("{}", event.id);
                if let Some(pct) = positions.pos1_percent() {
                    advise_hass_of_position(state, &shade_id_str, pct).await?;
                    let shade_state = if pct == 0 { "closed" } else { "open" };
                    advise_hass_of_state_label(state, &shade_id_str, shade_state).await?;
                }
                if let Some(pct) = positions.pos2_percent() {
                    let sec_id = format!("{}{SECONDARY_SUFFIX}", event.id);
                    advise_hass_of_position(state, &sec_id, pct).await?;
                    let shade_state = if pct == 0 { "closed" } else { "open" };
                    advise_hass_of_state_label(state, &sec_id, shade_state).await?;
                }
            }
        }
        ShadeEventKind::MotionStarted => {
            advise_hass_of_state_label(state, &format!("{}", event.id), "moving").await?;
        }
        ShadeEventKind::ShadeOffline => {
            state
                .client
                .publish(
                    format!(
                        "{MODEL}/shade/{serial}/{}/availability",
                        event.id,
                        serial = state.serial
                    ),
                    "offline",
                    QoS::AtMostOnce,
                    false,
                )
                .await?;
        }
        ShadeEventKind::ShadeOnline | ShadeEventKind::BatteryAlert => {
            match hub.hub.shade_by_id(event.id).await {
                Ok(shade) => {
                    state
                        .client
                        .publish(
                            format!(
                                "{MODEL}/shade/{serial}/{}/availability",
                                shade.id,
                                serial = state.serial
                            ),
                            "online",
                            QoS::AtMostOnce,
                            false,
                        )
                        .await?;
                    advise_hass_of_updated_position(state, &shade).await?;
                    advise_hass_of_battery_level(state, &shade).await?;
                }
                Err(e) => {
                    log::warn!(
                        "SSE {evt:?}: failed to fetch shade {id}: {e:#}",
                        evt = event.evt,
                        id = event.id
                    );
                }
            }
        }
        ShadeEventKind::Unknown => {
            // Filtered in shade_events_stream, handled defensively here
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Replace `handle_discovery` to use GatewayConfig**

```rust
async fn handle_discovery(
    &self,
    state: &Arc<Pv2MqttState>,
    mut new_hub: ResolvedHub,
) -> anyhow::Result<()> {
    let hub = state.hub.load();
    match new_hub.gateway_data.take() {
        Some(gateway_data) => {
            if gateway_data.serial_number != state.serial {
                return Ok(());
            }
            let changed = !state.responding.load(Ordering::SeqCst)
                || gateway_data.network_status.ip_address
                    != hub.gateway_data.network_status.ip_address;
            if !changed {
                return Ok(());
            }
            log::info!("Hub ip or connectivity status changed");
            state.responding.store(true, Ordering::SeqCst);
            state.hub.store(Arc::new(FullyResolvedHub {
                hub: new_hub.hub.clone(),
                gateway_data,
            }));
            register_with_hass(state)
                .await
                .context("register_with_hass")?;
            Ok(())
        }
        None => {
            advise_hass_of_unresponsive(state)
                .await
                .context("advise_hass_of_unresponsive")?;
            Ok(())
        }
    }
}
```

- [ ] **Step 4: Replace `mqtt_shade_set_position` to use `set_shade_position`**

```rust
async fn mqtt_shade_set_position(
    params: Params<SerialAndShade>,
    Topic(topic): Topic,
    State(state): State<Arc<Pv2MqttState>>,
    Payload(position): Payload<u8>,
) -> anyhow::Result<()> {
    let Params(SerialAndShade {
        serial,
        shade_id: ShadeIdAddr {
            shade_id,
            is_secondary,
        },
    }) = params;

    if serial != state.serial {
        log::warn!(
            "ignoring {topic} which is intended for \
                    serial={serial}, while we are serial {actual_serial}",
            actual_serial = state.serial
        );
        return Ok(());
    }

    let hub = state.hub.load();
    let shade = hub.hub.shade_by_id(shade_id).await?;

    let pos = if is_secondary {
        ShadePosition { secondary: Some(ShadePosition::percent_to_pos(position)), ..Default::default() }
    } else {
        ShadePosition { primary: Some(ShadePosition::percent_to_pos(position)), ..Default::default() }
    };

    log::info!(
        "Set {shade_id} {} {} to {position}%",
        shade.pt_name,
        if is_secondary { "secondary" } else { "primary" }
    );
    hub.hub.set_shade_position(shade_id, pos).await?;
    Ok(())
}
```

- [ ] **Step 5: Replace `mqtt_shade_command` (remove CALIBRATE/HEART/UPDATE_BATTERY/REFRESH_POS/battery_kind handlers; remove `advise_hass_of_updated_position` calls)**

```rust
async fn mqtt_shade_command(
    params: Params<SerialAndShade>,
    Topic(topic): Topic,
    State(state): State<Arc<Pv2MqttState>>,
    Payload(command): Payload<String>,
) -> anyhow::Result<()> {
    let Params(SerialAndShade {
        serial,
        shade_id: ShadeIdAddr {
            shade_id,
            is_secondary: _,
        },
    }) = params;

    if serial != state.serial {
        log::warn!(
            "ignoring {topic} which is intended for \
                    serial={serial}, while we are serial {actual_serial}",
            actual_serial = state.serial
        );
        return Ok(());
    }

    let hub = state.hub.load();
    let shade = hub.hub.shade_by_id(shade_id).await?;

    log::info!("{command} {shade_id} {}", shade.pt_name);
    match command.as_ref() {
        "OPEN" => {
            hub.hub.move_shade(shade_id, ShadeUpdateMotion::Up).await?;
        }
        "CLOSE" => {
            hub.hub.move_shade(shade_id, ShadeUpdateMotion::Down).await?;
        }
        "STOP" => {
            hub.hub.move_shade(shade_id, ShadeUpdateMotion::Stop).await?;
        }
        "JOG" => {
            hub.hub.move_shade(shade_id, ShadeUpdateMotion::Jog).await?;
        }
        _ => {
            log::warn!("Command {command} has no handler");
        }
    }
    Ok(())
}
```

- [ ] **Step 6: Replace `serve()` (remove `HomeAutomationData` arm, add `ShadeEvent`)**

```rust
async fn serve(&self, mut rx: Receiver<ServerEvent>, state: Arc<Pv2MqttState>) {
    log::info!(
        "Version {}. Waiting for mqtt and pv messages",
        pview_version()
    );
    while let Some(msg) = rx.recv().await {
        match msg {
            ServerEvent::MqttMessage { msg, router } => {
                if let Err(err) = self.handle_mqtt_message(msg, &state, &router).await {
                    log::error!("handling mqtt message: {err:#}");
                }
            }
            ServerEvent::ShadeEvent(event) => {
                if let Err(err) = self.handle_shade_event(&state, event).await {
                    log::error!("handling shade event: {err:#}");
                }
            }
            ServerEvent::HubDiscovered(resolved_hub) => {
                if let Err(err) = self.handle_discovery(&state, resolved_hub).await {
                    log::error!("During handle_discovery: {err:#?}");
                }
            }
            ServerEvent::PeriodicStateUpdate => {
                if let Err(err) = register_with_hass(&state).await {
                    log::error!("During register_with_hass: {err:#?}");
                    let mut unresponsive = false;
                    for cause in err.chain() {
                        if let Some(http_err) = cause.downcast_ref::<reqwest::Error>() {
                            if http_err.is_connect() {
                                unresponsive = true;
                                break;
                            }
                        }
                        if cause.downcast_ref::<LockedError>().is_some() {
                            unresponsive = true;
                            break;
                        }
                    }
                    if unresponsive {
                        if let Err(err) = advise_hass_of_unresponsive(&state).await {
                            log::error!("While advising hass of unresponsive hub: {err:#}");
                        }
                    }
                }
            }
        }
    }
}
```

- [ ] **Step 7: Replace `run()` (remove http server setup, bind_address, homeautomation hook; add SSE task)**

```rust
pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
    let mqtt_host = match &self.host {
        Some(h) => h.to_string(),
        None => std::env::var("PV_MQTT_HOST").context(
            "specify the mqtt host either via the --host \
             option or the PV_MQTT_HOST environment variable",
        )?,
    };

    let mqtt_port: u16 = match self.port {
        Some(p) => p,
        None => opt_env_var("PV_MQTT_PORT")?.unwrap_or(1883),
    };

    let mqtt_username: Option<String> = match self.username.clone() {
        Some(u) => Some(u),
        None => opt_env_var("PV_MQTT_USER")?,
    };
    let mqtt_password: Option<String> = match self.password.clone() {
        Some(u) => Some(u),
        None => opt_env_var("PV_MQTT_PASSWORD")?,
    };

    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let hub = args.hub().await?;
    let mut resolved = ResolvedHub::with_hub(hub).await;
    let gateway_data = resolved.gateway_data.take().ok_or_else(|| {
        anyhow::anyhow!(
            "Unable to determine the serial number \
                of the hub. The hub is not responding correctly \
                and may need to be restarted"
        )
    })?;
    let serial = gateway_data.serial_number.clone();

    let client = Client::with_auto_id()?;
    let state = Arc::new(Pv2MqttState {
        hub: ArcSwap::new(Arc::new(FullyResolvedHub {
            hub: resolved.hub.clone(),
            gateway_data,
        })),
        client: client.clone(),
        serial: serial.clone(),
        discovery_prefix: self.discovery_prefix.clone(),
        first_run: AtomicBool::new(true),
        responding: AtomicBool::new(true),
    });

    client.set_username_and_password(mqtt_username.as_deref(), mqtt_password.as_deref())?;
    client
        .connect(
            &mqtt_host,
            mqtt_port.into(),
            Duration::from_secs(10),
            None,
        )
        .await
        .with_context(|| format!("connecting to mqtt broker {mqtt_host}:{mqtt_port}"))?;
    let subscriber = client.subscriber().expect("to own the subscriber");

    async fn rebuild_router(
        client: &Client,
        state: &Arc<Pv2MqttState>,
        discovery_prefix: &str,
    ) -> anyhow::Result<Arc<MqttRouter<Arc<Pv2MqttState>>>> {
        let mut router: MqttRouter<Arc<Pv2MqttState>> = MqttRouter::new(client.clone());
        router
            .route(format!("{discovery_prefix}/status"), mqtt_homeassitant_status)
            .await?;
        router
            .route(
                format!("{MODEL}/scene/:serial/:scene_id/set"),
                mqtt_scene_activate,
            )
            .await?;
        router
            .route(
                format!("{MODEL}/shade/:serial/:shade_id/set_position"),
                mqtt_shade_set_position,
            )
            .await?;
        router
            .route(
                format!("{MODEL}/shade/:serial/:shade_id/command"),
                mqtt_shade_command,
            )
            .await?;
        register_with_hass(state).await?;
        Ok(Arc::new(router))
    }

    let mut router = rebuild_router(&client, &state, &self.discovery_prefix).await?;
    let mut need_rebuild = false;

    // Periodic state update timer
    {
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                if let Err(err) = tx.send(ServerEvent::PeriodicStateUpdate).await {
                    log::error!("{err:#?}");
                    break;
                }
            }
        });
    }

    // Hub discovery task
    if !args.hub_ip_was_specified_by_user() {
        let tx = tx.clone();
        let serial_filter = args.hub_serial()?;
        let mut disco = crate::discovery::resolve_hubs(None).await?;
        tokio::spawn(async move {
            while let Some(resolved_hub) = disco.recv().await {
                log::trace!("disco resolved: {resolved_hub:?}");
                if let Some(gateway_data) = &resolved_hub.gateway_data {
                    if let Some(serial) = &serial_filter {
                        if *serial != gateway_data.serial_number {
                            continue;
                        }
                    }
                    if let Err(err) = tx.send(ServerEvent::HubDiscovered(resolved_hub)).await {
                        log::error!("discovery: send to main thread: {err:#}");
                        break;
                    }
                }
            }
            log::warn!("fell out of disco loop");
        });
    }

    // SSE shade event listener — reconnects on stream end or error
    {
        let tx = tx.clone();
        let hub = state.hub.load().hub.clone();
        tokio::spawn(async move {
            loop {
                log::info!("SSE: opening shade events stream");
                match hub.shade_events_stream().await {
                    Err(e) => {
                        log::error!("SSE: failed to open stream: {e:#}");
                    }
                    Ok(stream) => {
                        tokio::pin!(stream);
                        while let Some(result) = stream.next().await {
                            match result {
                                Ok(event) => {
                                    if let Err(e) =
                                        tx.send(ServerEvent::ShadeEvent(event)).await
                                    {
                                        log::error!("SSE: channel send error: {e:#}");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    log::error!("SSE: stream error: {e:#}");
                                    break;
                                }
                            }
                        }
                    }
                }
                log::info!("SSE: stream ended, reconnecting in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    // MQTT event loop
    {
        let state = state.clone();
        let discovery_prefix = self.discovery_prefix.to_string();
        tokio::spawn(async move {
            while let Ok(event) = subscriber.recv().await {
                match event {
                    Event::Message(msg) => {
                        if let Err(err) = tx
                            .send(ServerEvent::MqttMessage {
                                msg,
                                router: router.clone(),
                            })
                            .await
                        {
                            log::error!("{err:#?}");
                            break;
                        }
                    }
                    Event::Disconnected(reason) => {
                        log::warn!("MQTT disconnected: {reason}");
                        need_rebuild = true;
                    }
                    Event::Connected(status) => {
                        log::info!("MQTT (re)connected {status}");
                        if need_rebuild {
                            match rebuild_router(&client, &state, &discovery_prefix).await {
                                Err(err) => {
                                    log::error!("Rebuilding router: {err:#}");
                                    break;
                                }
                                Ok(r) => {
                                    router = r;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    self.serve(rx, state).await;
    Ok(())
}
```

- [ ] **Step 8: Run cargo check**

```bash
cargo check 2>&1 | grep "^error\[" | head -20
```

Expected: zero errors, or only minor/fixable issues.

- [ ] **Step 9: Run cargo build**

```bash
cargo build 2>&1 | tail -5
```

Expected: `Finished dev [unoptimized + debuginfo] target(s) in ...`

- [ ] **Step 10: Commit**

```bash
git add src/commands/serve_mqtt.rs
git commit -m "feat: replace axum postback server with SSE listener in serve_mqtt"
```

---

### Task 11: Final verification

**Files:**
- Check all source files

- [ ] **Step 1: Full release build**

```bash
cargo build --release 2>&1 | tail -5
```

Expected: `Finished release [optimized] target(s) in ...`

- [ ] **Step 2: Verify no remaining v2 API references**

```bash
grep -rn "Base64Name\|UserData\|ShadeBatteryKind\|HomeAutomation\|SceneMember\|ResolvedShadeData\|axum\|matchit\|serde_urlencoded\|data.encoding\|bind_address\|http_port\|setup_http_server" src/
```

Expected: no output.

- [ ] **Step 3: Verify no remaining v2 URL paths**

```bash
grep -rn "api/rooms\|api/shades\|api/scenes\|api/userdata\|api/homeautomation\|api/scenemembers" src/
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: complete PowerView Gen 3 API migration"
```
