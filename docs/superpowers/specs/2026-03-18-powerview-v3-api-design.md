# PowerView Gen 3 API Migration Design

**Date:** 2026-03-18
**Branch:** pview3
**Scope:** Clean v3-only replacement of all v2 API types, HTTP client, and event model.

## Background

The Hunter Douglas PowerView Gen 3 hub exposes a new REST API (`/home/...`, `/gateway`) that is incompatible with the Gen 2 API (`/api/...`). Key differences:

- Response bodies are direct JSON arrays instead of `{xData: [], xIds: []}` wrappers
- Shade positions use named float fields `primary/secondary/tilt` (0.0–1.0) instead of indexed u16 values
- Names are provided as plain text in `ptName` alongside base64 `name`
- Events are delivered via SSE stream instead of postback webhook
- No `scenemembers` endpoint; scenes carry `roomIds` (array) only
- `userdata` replaced by `gateway`; battery/signal fields simplified
- `GET /home/shades` has no `roomId`/`groupId` query params; all filtering is client-side
- Rooms and scenes have no `order` field; sort by `pt_name` alphabetically
- `shade.room_id` is always present (non-optional) in v3

**API reference:** Swagger at `http://<hub-ip>/` (port 3002 for local testing), plus [aio-powerview-api](https://github.com/sander76/aio-powerview-api).

## Approach

Clean in-place replacement (no parallel modules, no adapter layer). All files updated in a single pass. v2 code and types are fully removed.

---

## Section 1: Data Types (`src/api_types.rs`)

### Removed types

| Type | Reason |
|---|---|
| `Base64Name` | v3 provides `ptName` as plain text; base64 decoding no longer needed |
| `RoomResponse`, `ShadesResponse`, `ScenesResponse` | v3 returns direct arrays |
| `SceneMember`, `SceneMembersResponse` | No v3 scenemembers endpoint |
| `UserData`, `UserDataResponse`, `FirmwareInfo`, `Color`, `TimeConfiguration` | Replaced by `GatewayConfig` |
| `HomeAutomationPostBackData`, `HomeAutomationRecordType`, `HomeAutomationService` | Replaced by `ShadeEvent` (SSE) |
| `SmartPowerSupply`, `Motor` | Not present in v3 |

### New / changed types

**`ShadeData`**
```rust
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ShadeData {
    pub id: i32,
    #[serde(rename = "type")]
    pub shade_type: i32,           // raw int; v3 has 50+ opaque values ("for HD use only")
    pub pt_name: String,           // plain text name — use for all display and lookups
    pub name: String,              // raw base64 string as-is from JSON "name" key; stored but unused
    pub capabilities: ShadeCapabilities,
    pub power_type: PowerType,
    pub battery_status: Option<BatteryStatus>,  // nullable in v3 JSON; None = status unavailable
    pub room_id: i32,              // always present in v3 (in required[] list); no longer Option
    pub firmware: ShadeFirmware,   // always present (in v3 required[] list)
    pub positions: ShadePosition,  // always present (in v3 required[] list)
    pub signal_strength: Option<f64>,  // RSSI in dBm (e.g. -55.0); was 0–4 integer
    pub ble_name: String,
    pub shade_group_ids: Vec<i32>,     // was single group_id: i32
    pub serial_number: String,
}
```

Helpers:
- `name()` → returns `&pt_name`
- `battery_percent() -> Option<u8>` → `None` when `battery_status` is `None`; maps `High=3`→100, `Medium=2`→50, `Low=1`→20, `NoPower=0`→0
- `signal_strength_percent() -> Option<u8>` → maps RSSI dBm to 0–100 (clamp: -100 dBm → 0%, -50 dBm → 100%)
- `pos1_percent() -> Option<u8>` uses `positions.primary`
- `pos2_percent() -> Option<u8>` uses `positions.secondary`

Note: all v2 call sites that checked `shade.room_id.and_then(...)` change to `room_by_id.get(&shade.room_id)`.
Note: all v2 call sites that unwrapped `Option<ShadePosition>` or `shade.firmware.as_ref()` change to use the fields directly.

**`ShadePosition`** — completely new shape; always present on `ShadeData`:
```rust
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ShadePosition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<f64>,    // 0.0–1.0; None = shade offline or not applicable
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub velocity: Option<f64>,
}
```

`skip_serializing_if` allows `set_shade_position()` to send partial updates — the v3 hub preserves axes not included in the PUT body. This means the caller sets only the axis being changed without needing to pre-fetch the current full position.

Position helpers:
- `pos_to_percent(v: f64) -> u8` = `(v * 100.0).round() as u8`
- `percent_to_pos(pct: u8) -> f64` = `pct as f64 / 100.0`
- `pos1_percent() -> Option<u8>` = `primary.map(Self::pos_to_percent)`
- `pos2_percent() -> Option<u8>` = `secondary.map(Self::pos_to_percent)`
- `describe()` — updated to show named fields: `"primary: 50% secondary: 25%"`

**`RoomData`**
```rust
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RoomData {
    pub id: i32,
    pub pt_name: String,
    pub name: String,     // raw base64
    pub color: String,    // numeric id supplied as string
    pub icon: String,     // numeric id supplied as string
    #[serde(rename = "type")]
    pub room_type: i32,
}
```

No `order` field in v3. `list_rooms()` sorts by `pt_name` alphabetically.

**`Scene`**
```rust
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Scene {
    pub id: i32,
    pub pt_name: String,
    pub name: String,     // raw base64
    pub color: String,
    pub icon: String,
    pub network_number: i32,
    pub room_ids: Vec<i32>,   // was single room_id: i32
}
```

No `order` field in v3. `list_scenes()` sorts by `pt_name` alphabetically.

**`GatewayConfig`** — replaces `UserData`. The v3 `GET /gateway` response wraps fields under a `config` key. The gateway config includes `brand`, `model`, and `serialNumber` but does **not** include a human-readable hub name (`hub_name`). Use `serial_number` as the hub identifier in MQTT device names.

```rust
// Internal deserialization wrapper — not pub
#[derive(Deserialize)]
struct GatewayResponse {
    config: GatewayConfig,
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

// name is always present in v3 mainProcessor (in required[] list)
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
```

`get_gateway_data()` deserializes into `GatewayResponse`, returns `response.config`.

**`ShadeEvent`** — replaces `HomeAutomationPostBackData`. Wire format is JSON in SSE `data:` lines. The `evt` field uses kebab-case strings. The `battery-alert` event carries no battery level field; a full shade re-fetch is required.

Example SSE payload:
```
data: {"evt":"motion-stopped","id":55,"currentPositions":{"primary":0.5,"secondary":null,"tilt":null,"velocity":0.5},"isoDate":"2021-12-06T20:01:11.934Z","bleName":"SON:1234"}
```

SSE events are delimited by blank lines (`\n\n` or `\r\n\r\n`). Each event consists of one or more `field: value` lines. Only `data:` lines are relevant; strip any trailing `\r` before JSON parsing.

```rust
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ShadeEvent {
    pub evt: ShadeEventKind,
    pub id: i32,
    pub current_positions: Option<ShadePosition>,
    pub iso_date: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ShadeEventKind {
    ShadeOffline,
    ShadeOnline,
    MotionStarted,
    MotionStopped,
    BatteryAlert,
    #[serde(other)]
    Unknown,   // forward-compatible fallback; unknown events are logged at debug level and skipped
}
```

**`PowerType`** — replaces `ShadeBatteryKind`:
```rust
#[derive(Serialize_repr, Deserialize_repr, Debug, Copy, Clone, PartialEq, Eq)]
#[repr(i32)]
pub enum PowerType {
    Battery = 0,
    Hardwired = 1,
    Rechargeable = 2,
}
```

v2's `BatteryStatus::PluggedIn` is dropped. Use `power_type == PowerType::Hardwired` to detect AC-powered shades.

**`BatteryStatus`** — scale changes (v3: 3=high, 2=medium, 1=low, 0=none; nullable in JSON):
```rust
#[derive(Serialize_repr, Deserialize_repr, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum BatteryStatus {
    NoPower = 0,
    Low = 1,
    Medium = 2,
    High = 3,
}
```

`battery_percent()`: `None` (null) → `None`; `NoPower` → `Some(0)`; `Low` → `Some(20)`; `Medium` → `Some(50)`; `High` → `Some(100)`.

**`ShadeCapabilities`** — add types 10 and 11; add `Unknown(i32)` fallback. `serde_repr` does not support `#[serde(other)]`, so implement a manual `Deserialize`:

```rust
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
            0 => Self::BottomUp, 1 => Self::BottomUpTilt90,
            2 => Self::BottomUpTilt180, 3 => Self::VerticalTilt180,
            4 => Self::Vertical, 5 => Self::TiltOnly180,
            6 => Self::TopDown, 7 => Self::TopDownBottomUp,
            8 => Self::DualOverlapped, 9 => Self::DualOverlappedTilt90,
            10 => Self::DuoliteTilt180, 11 => Self::Illuminated,
            other => Self::Unknown(other),
        })
    }
}
```

`flags()` additions:
- `DuoliteTilt180` → `PRIMARY_RAIL | SECONDARY_RAIL | TILT_ANYWHERE | TILT_180`
- `Illuminated` → `PRIMARY_RAIL | SECONDARY_RAIL`
- `Unknown(_)` → `ShadeCapabilityFlags::empty()`

**`ShadeUpdateMotion`** — remove `Heart` and `Calibrate`:
```rust
#[derive(Serialize, Deserialize, Debug, Clone, Copy, clap::ValueEnum)]
#[serde(rename_all = "camelCase")]
pub enum ShadeUpdateMotion { Down, Jog, LeftTilt, RightTilt, Stop, Up }
```

**`ShadeFirmware`** — v3 drops the `index` field; always present:
```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ShadeFirmware {
    pub revision: i32,
    pub sub_revision: i32,
    pub build: i32,
}
```

---

## Section 2: Hub Client (`src/hub.rs`)

### URL mapping

| v2 | v3 | Response |
|---|---|---|
| `GET api/rooms` | `GET home/rooms` | `Vec<RoomData>` |
| `GET api/shades` | `GET home/shades` | `Vec<ShadeData>` (client-side filtered) |
| `GET api/shades/{id}` | `GET home/shades/{id}` | `ShadeData` |
| `GET api/scenes` | `GET home/scenes` | `Vec<Scene>` |
| `GET api/scenes?sceneId={id}` | `PUT home/scenes/{id}/activate` | `Vec<i32>` |
| `GET api/userdata` | `GET gateway` → unwrap `.config` | `GatewayConfig` |
| `PUT api/shades/{id}` (positions) | `PUT home/shades/{id}/positions` | ignored (check HTTP status) |
| `PUT api/shades/{id}` (motion=jog) | `PUT home/shades/{id}/motion` | ignored |
| *(new)* | `PUT home/shades/stop {"ids":[id]}` | ignored |
| `GET api/shades/{id}?refresh=true` | `GET home/shades/{id}` (no refresh param) | `ShadeData` |
| `GET api/shades/{id}?updateBatteryLevel=true` | removed | — |
| `PUT api/homeautomation` | removed | — |
| `GET api/scenemembers` | removed | — |

### list_shades() signature change

```rust
pub async fn list_shades(&self, room_id: Option<i32>) -> anyhow::Result<Vec<ShadeData>>
```

v3's `GET /home/shades` has no query parameters. Fetch all shades, then if `room_id` is `Some(id)`, retain only shades where `shade.room_id == id`. Sort by `pt_name`. The `group_id` parameter is removed.

### Changed and new methods

**`move_shade(shade_id, motion) -> anyhow::Result<()>`** — returns `()`:

| Motion | HTTP call |
|---|---|
| `Up` | `PUT home/shades/{id}/positions {"positions":{"primary":1.0}}` |
| `Down` | `PUT home/shades/{id}/positions {"positions":{"primary":0.0}}` |
| `Jog` | `PUT home/shades/{id}/motion {"motion":"jog"}` |
| `Stop` | `PUT home/shades/stop {"ids":[id]}` |
| `LeftTilt` | `PUT home/shades/{id}/positions {"positions":{"tilt":0.0}}` |
| `RightTilt` | `PUT home/shades/{id}/positions {"positions":{"tilt":1.0}}` |

After calling `move_shade()`, the MQTT bridge does **not** immediately re-fetch the shade. Position state is updated when the hub sends a `motion-stopped` SSE event with `current_positions`. For `Jog`, no position change is expected so no update is needed.

**`set_shade_position(shade_id: i32, position: ShadePosition) -> anyhow::Result<()>`** — replaces `change_shade_position()`. Sends `PUT home/shades/{id}/positions {"positions": {...}}`. Because `ShadePosition` uses `skip_serializing_if = "Option::is_none"`, the caller sets only the `primary` or `secondary` field it wants to change; the v3 hub preserves the other axes. No pre-fetch of current positions is needed. Used by the MQTT bridge when handling a cover set-position command.

**`get_gateway_data() -> anyhow::Result<GatewayConfig>`** — replaces `get_user_data()`.

**`list_rooms()` and `list_scenes()` sort:** by `pt_name` alphabetically (no `order` field in v3).

### Name-based lookup methods

`scene_by_name()`, `room_by_name()`, and `shade_by_name()` compare against `pt_name` (case-insensitive) and `id.to_string()`. `shade_by_name()` returns `ShadeData` directly; `ResolvedShadeData` is removed.

### New SSE method

```rust
pub async fn shade_events_stream(&self) -> anyhow::Result<impl Stream<Item = anyhow::Result<ShadeEvent>>>
```

Opens `GET home/shades/events?sse=true` using a **dedicated reqwest client with no timeout**:

```rust
reqwest::Client::builder().timeout(None).build()?
```

Do not reuse the shared client from `http_helpers`. Call `.bytes_stream()` on the response to get a `Stream<Item = Result<Bytes>>`. Buffer the chunks, split on blank lines (`\n\n` or `\r\n\r\n`) to get individual events, then within each event find lines starting with `data:`, strip the prefix and any trailing `\r`, and deserialize the trimmed JSON into `ShadeEvent`. Yield the event; caller skips `Unknown` variants.

### activate_scene() response

v3 `PUT home/scenes/{id}/activate` returns a bare JSON array `[22, 33, 44]` (not a `{shadeIds:[...]}` wrapper). Deserialize directly as `Vec<i32>`:

```rust
let ids: Vec<i32> = request_with_json_response(Method::PUT, url, &json!({})).await?;
```

### Removed methods

`list_scene_members()`, `change_battery_kind()`, `change_shade_position()`, `shade_update_battery_level()`, `shade_refresh_position()`, `enable_home_automation_hook()`, `suggest_bind_address()`

---

---

## Section 3: MQTT Bridge (`src/commands/serve_mqtt.rs`)

### Removed

- `bind_address` CLI argument
- Axum HTTP server and route handler
- `HomeAutomationData` server event variant
- `enable_home_automation_hook()` and `suggest_bind_address()` startup calls

### Struct updates

- `ResolvedHub.user_data: Option<UserData>` → `gateway_data: Option<GatewayConfig>` (also applies to `FullyResolvedHub` in `serve_mqtt.rs`)
- Remove `http_port: u16` field from `Pv2MqttState` (no longer used after axum server removal)

### SSE event source

A dedicated Tokio task opens the SSE stream via `hub.shade_events_stream()` and forwards events into the channel as `ServerEvent::ShadeEvent(ShadeEvent)`. On error/EOF, sleeps 5 s and reconnects. Unknown events are logged at debug level and skipped.

Startup sequence:
1. Discover hub
2. Load all shade/room/scene state
3. Register MQTT entities with Home Assistant
4. Start SSE listener task
5. Enter main event loop

### Event handling

| SSE `evt` | Action |
|---|---|
| `motion-stopped` | Publish MQTT position from `current_positions.primary` (and `.secondary` if present) |
| `motion-started` | Publish MQTT `moving` state |
| `shade-offline` | Publish MQTT `unavailable` |
| `shade-online` | Fetch shade via `GET home/shades/{id}`; publish availability + position |
| `battery-alert` | Fetch shade via `GET home/shades/{id}` (SSE carries no battery field); publish battery |
| `Unknown` | Skip |

### MQTT entity changes

**Removed entities (no v3 equivalent):**
- "Calibrate" button (`CALIBRATE` command) — remove registration and handler
- "Move to Favorite Position" button (`HEART` command) — remove registration and handler
- "Refresh Battery Status" button (`UPDATE_BATTERY` command) — remove registration and handler
- "Refresh Position" button (`REFRESH_POS` command) — remove registration and handler
- `rfStatus` diagnostic sensor — remove (field not in `GatewayConfig`)

**Changed entities:**
- "Power Source" — change from writable `select` to read-only `sensor` (diagnostic). v3 `power_type` is read-only. Register as `SensorConfig`, not `SelectConfig`. Publish `power_type_to_state(shade.power_type)` mapping: `PowerType::Battery` → `"Battery"`, `PowerType::Hardwired` → `"Hard Wired"`, `PowerType::Rechargeable` → `"Rechargeable Battery"`. Remove the MQTT command handler for this entity.

**Hub device name:** `GatewayConfig` has no `hub_name` field. Use `serial_number` as the hub identifier: device name becomes `"PowerView Hub {serial_number}"`.

**Hub device fields:** `brand` and `model` are available in `GatewayConfig`. Use `gateway_data.brand` for manufacturer, `gateway_data.model` for model. Replace `user_data.ip` with `gateway_data.network_status.ip_address`. Replace `user_data.mac_address` with `gateway_data.network_status.primary_mac_address`.

### Other call-site changes in serve_mqtt.rs

- `shade.room_id.and_then(...)` → `room_by_id.get(&shade.room_id).cloned()` (room_id non-optional)
- `shade.name()` / `room.name` / `scene.name.to_string()` → use `pt_name` throughout
- `shade.firmware.as_ref().map(...)` → `shade.firmware` directly (always present)
- `hub.hub.list_shades(None, None)` → `hub.hub.list_shades(None)`
- `scene.room_id` → `scene.room_ids.first().copied()` for `suggested_area` (use first room)
- `room_by_id` map value: use `room.pt_name` instead of `room.name`
- MQTT command handlers `"OPEN"` / `"CLOSE"` / `"STOP"` / `"JOG"`: call `move_shade().await?` and return. Remove the `advise_hass_of_updated_position` call that followed — position state is now updated by the SSE `motion-stopped` event handler instead. This means position feedback is slightly deferred but reflects the hub's confirmed final position.
- Remove handlers for `"CALIBRATE"`, `"HEART"`, `"UPDATE_BATTERY"`, `"REFRESH_POS"` commands entirely
- `handle_discovery` change-detection: replace `user_data.ip != hub.user_data.ip || user_data.hub_name != hub.user_data.hub_name` with `config.network_status.ip_address != hub.gateway_data.network_status.ip_address`. Drop the `hub_name` comparison — `GatewayConfig` does not need `PartialEq`.
- "Power Source" entity: rename topic helper from `battery_kind_state_topic()` to `power_type_state_topic()`, using `pv2mqtt/sensor/{serial}/{shade_id}/psu/state` (change `select/` to `sensor/` in the path to match the new `SensorConfig` registration). The availability topic changes similarly from `.../psu/availability` — keep the same path structure but under `sensor/`.

### Periodic polling

`PeriodicStateUpdate` timer retained unchanged.

### Dependency changes (`Cargo.toml`)

- **Remove:** `axum`, `matchit`, `serde_urlencoded`
- **Update:** `reqwest` gains `stream` feature: `features = ["json", "stream"]`
- **Add:** `futures-util = "0.3"` — provides `StreamExt` for working with the `Stream` returned by `shade_events_stream()` and for `bytes_stream()` processing

---

## Section 4: Remaining Commands

### `list_shades.rs`
- Display `shade.pt_name`
- `pos1_percent()` / `pos2_percent()` interface unchanged
- Room filter: resolve room by `pt_name`, pass `room_id` to `list_shades(Some(room_id))`; internal filter applies

### `inspect_shade.rs`
- Remove `battery_strength`, `smart_power_supply`, `motor` sections
- `battery_kind` → `power_type` (Battery / Hard Wired / Rechargeable)
- `signal_strength` shown as `{value:.0} dBm`
- Positions: `primary: X%  secondary: Y%  tilt: Z%`

### `list_scenes.rs`
- No scene members; output shows scene name + room names
- Resolve room names via `list_rooms()`, join `room_ids` to `pt_name`s
- `--room` filter: `scenes.retain(|scene| scene.room_ids.contains(&room.id))` (was `scene.room_id == room.id`)
- Remove `list_shades()` and `list_scene_members()` calls (no longer needed)
- Output:
  ```
  SCENE                            ROOMS
  Open Guest                       Bedroom 1, Zen Room
  ```

### `hub_info.rs`
- Calls `get_gateway_data()` → `GatewayConfig`
- Displays: serial number, firmware (`{revision}.{sub_revision}.{build}`), IP, MAC, brand, model

### `move_shade.rs`
- Remove `Heart` and `Calibrate` from `--motion` clap enum
- `shade_by_name()` now returns `ShadeData` directly (not `ResolvedShadeData`). For `--percent`, the primary/secondary distinction via `shade.is_primary()` is removed. The `--percent` option always sets the `primary` axis:
  ```rust
  let pos = ShadePosition { primary: Some(ShadePosition::percent_to_pos(percent)), ..Default::default() };
  hub.set_shade_position(shade.id, pos).await?;
  ```
  The secondary rail is no longer addressable by name from the CLI (no `secondaryName` in v3). Remove the `is_primary()` branch entirely.
- `move_shade()` and `set_shade_position()` return `()`, so print the shade's name/id rather than debug-printing the returned `ShadeData`

### `activate_scene.rs`
- No interface change; hub method changes internally

### `discovery.rs`
- mDNS service name `_powerview._tcp.local` unchanged
- `ResolvedHub.user_data: Option<UserData>` → `gateway_data: Option<GatewayConfig>`
- `resolve_hub_with_serial()` compares against `gateway_data.serial_number`
- `ResolvedHub::with_hub()` calls `get_gateway_data()`

---

## Files Changed

| File | Change |
|---|---|
| `Cargo.toml` | Remove axum/matchit/serde_urlencoded; add reqwest stream feature |
| `src/api_types.rs` | Complete rewrite |
| `src/hub.rs` | New endpoints, SSE stream, removed/renamed methods |
| `src/discovery.rs` | UserData → GatewayConfig |
| `src/commands/serve_mqtt.rs` | Remove axum server; add SSE client; entity updates |
| `src/commands/hub_info.rs` | GatewayConfig display |
| `src/commands/list_shades.rs` | pt_name, updated positions, client-side room filter |
| `src/commands/inspect_shade.rs` | Updated field display |
| `src/commands/list_scenes.rs` | Room-based output, no scene members |
| `src/commands/move_shade.rs` | Remove Heart/Calibrate |
| `src/commands/activate_scene.rs` | No interface change |
