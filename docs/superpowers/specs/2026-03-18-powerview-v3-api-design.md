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
- No `scenemembers` endpoint; scenes carry `roomIds` only
- `userdata` replaced by `gateway`; battery/signal fields simplified

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
| `UserData`, `UserDataResponse`, `FirmwareInfo`, `Color`, `TimeConfiguration` | Replaced by `GatewayData` |
| `HomeAutomationPostBackData`, `HomeAutomationRecordType`, `HomeAutomationService` | Replaced by `ShadeEvent` (SSE) |
| `SmartPowerSupply`, `Motor` | Not present in v3 |

### New / changed types

**`ShadeData`**
```rust
pub struct ShadeData {
    pub id: i32,
    pub shade_type: i32,           // raw int; v3 has many more types than can fit a stable enum
    pub pt_name: String,           // plain text name — use everywhere
    pub name: String,              // base64 encoded name — kept, unused
    pub capabilities: ShadeCapabilities,
    pub power_type: PowerType,
    pub battery_status: Option<BatteryStatus>,
    pub room_id: i32,
    pub firmware: ShadeFirmware,
    pub positions: ShadePosition,
    pub signal_strength: Option<f64>,  // RSSI in dBm (e.g. -55.0); was 0–4 integer
    pub ble_name: String,
    pub shade_group_ids: Vec<i32>,     // was single group_id: i32
    pub serial_number: String,
}
```

Helpers retained: `name()` returns `pt_name`, `battery_percent()`, `signal_strength_percent()` (maps dBm range to 0–100), `pos1_percent()`, `pos2_percent()`.

**`ShadePosition`** — completely new shape:
```rust
pub struct ShadePosition {
    pub primary: Option<f64>,    // 0.0–1.0; None = shade offline
    pub secondary: Option<f64>,
    pub tilt: Option<f64>,
    pub velocity: Option<f64>,
}
```
`pos_to_percent(v: f64) -> u8` = `(v * 100.0).round() as u8`
`percent_to_pos(pct: u8) -> f64` = `pct as f64 / 100.0`
`pos1_percent()` uses `primary`; `pos2_percent()` uses `secondary`.

**`RoomData`**
```rust
pub struct RoomData {
    pub id: i32,
    pub pt_name: String,
    pub name: String,
    pub color: String,   // numeric id as string
    pub icon: String,    // numeric id as string
    pub room_type: i32,
}
```

**`Scene`**
```rust
pub struct Scene {
    pub id: i32,
    pub pt_name: String,
    pub name: String,
    pub color: String,
    pub icon: String,
    pub network_number: i32,
    pub room_ids: Vec<i32>,   // was single room_id: i32
}
```

**`GatewayData`** — replaces `UserData`:
```rust
pub struct GatewayData {
    pub serial_number: String,
    pub firmware: GatewayFirmware,
    pub ip_address: String,
    pub mac_address: String,
}

pub struct GatewayFirmware {
    pub revision: i32,
    pub sub_revision: i32,
    pub build: i32,
    pub name: Option<String>,
}
```
Parsed from `GET /gateway` → `.config` object.

**`ShadeEvent`** — replaces `HomeAutomationPostBackData`:
```rust
pub struct ShadeEvent {
    pub evt: ShadeEventKind,
    pub id: i32,
    pub current_positions: Option<ShadePosition>,
    pub iso_date: Option<String>,
}

pub enum ShadeEventKind {
    ShadeOffline,
    ShadeOnline,
    MotionStarted,
    MotionStopped,
    BatteryAlert,
}
```

**`PowerType`** — replaces `ShadeBatteryKind`:
```rust
pub enum PowerType {
    Battery = 0,
    Hardwired = 1,
    Rechargeable = 2,
}
```

**`BatteryStatus`** — values change (v3 scale: 3=high, 2=medium, 1=low, 0=none; nullable):
```rust
pub enum BatteryStatus {
    None = 0,
    Low = 1,
    Medium = 2,
    High = 3,
}
```

**`ShadeCapabilities`** — add types 10 and 11:
```rust
DuoliteTilt180 = 10,    // Silhouette Halo Duolite
Illuminated = 11,        // Aura Illuminated shades
```

**`ShadeUpdateMotion`** — remove `Heart` and `Calibrate` (no v3 equivalent):
```rust
pub enum ShadeUpdateMotion { Down, Jog, LeftTilt, RightTilt, Stop, Up }
```

---

## Section 2: Hub Client (`src/hub.rs`)

### URL mapping

| v2 method | v3 endpoint |
|---|---|
| `GET api/rooms` | `GET home/rooms` → `Vec<RoomData>` |
| `GET api/shades[?roomId=]` | `GET home/shades[?roomId=]` → `Vec<ShadeData>` |
| `GET api/shades/{id}` | `GET home/shades/{id}` → `ShadeData` |
| `GET api/scenes` | `GET home/scenes` → `Vec<Scene>` |
| `PUT api/shades/{id}` (positions) | `PUT home/shades/{id}/positions` with `{"positions":{...}}` |
| `PUT api/shades/{id}` (motion=jog) | `PUT home/shades/{id}/motion` with `{"motion":"jog"}` |
| `GET api/scenes?sceneId={id}` (activate) | `PUT home/scenes/{id}/activate` → `Vec<i32>` |
| `GET api/userdata` | `GET gateway` → `GatewayData` |
| `GET api/shades/{id}?refresh=true` | `GET home/shades/{id}` (no explicit refresh param) |
| `GET api/shades/{id}?updateBatteryLevel=true` | removed (battery via SSE `battery-alert`) |
| `PUT api/homeautomation` | removed (SSE replaces postback) |
| `GET api/scenemembers` | removed |

### Motion routing in `move_shade()`

`move_shade(shade_id, motion)` dispatches internally:

| Motion | HTTP call |
|---|---|
| `Up` | `PUT home/shades/{id}/positions {"positions":{"primary":1.0}}` |
| `Down` | `PUT home/shades/{id}/positions {"positions":{"primary":0.0}}` |
| `Jog` | `PUT home/shades/{id}/motion {"motion":"jog"}` |
| `Stop` | `PUT home/shades/stop {"ids":[id]}` |
| `LeftTilt` | `PUT home/shades/{id}/positions {"positions":{"tilt":0.0}}` |
| `RightTilt` | `PUT home/shades/{id}/positions {"positions":{"tilt":1.0}}` |

### New method

```rust
pub async fn shade_events_stream(&self) -> anyhow::Result<impl Stream<Item = anyhow::Result<ShadeEvent>>>
```

Opens `GET home/shades/events?sse=true` as a streaming reqwest response. Reads raw bytes, splits on newlines, extracts `data:` lines, deserializes JSON into `ShadeEvent`. Called once at serve-mqtt startup; caller handles reconnection.

### Removed methods

`list_scene_members()`, `change_battery_kind()`, `shade_update_battery_level()`, `shade_refresh_position()`, `enable_home_automation_hook()`

### `shade_by_name()` change

Secondary name lookup removed. Lookup by `pt_name` and `id` only. `ResolvedShadeData::Secondary` variant removed; all lookups return `ShadeData` directly.

---

## Section 3: MQTT Bridge (`src/commands/serve_mqtt.rs`)

### Removed

- `bind_address` CLI argument
- Axum HTTP server and route handler
- `HomeAutomationData` server event variant
- `enable_home_automation_hook()` startup call

### SSE event source

A dedicated Tokio task opens the SSE stream via `hub.shade_events_stream()` and forwards parsed `ShadeEvent` values into the existing `Sender<ServerEvent>` channel as:

```rust
ServerEvent::ShadeEvent(ShadeEvent)
```

On stream error or EOF, the task sleeps briefly (5 s) and reconnects. The serve-mqtt startup sequence becomes:
1. Discover hub
2. Load all shade/room/scene state
3. Register MQTT entities with Home Assistant
4. Start SSE listener task
5. Enter main event loop

### Event handling

| SSE `evt` | Action |
|---|---|
| `motion-stopped` | Update cached shade position from `currentPositions`; publish MQTT position |
| `motion-started` | Publish MQTT `moving` state |
| `shade-offline` | Publish MQTT `unavailable` |
| `shade-online` | Fetch shade via `GET home/shades/{id}`; publish availability + position |
| `battery-alert` | Fetch shade; publish updated battery state |

### Secondary rail entities

Secondary MQTT cover entities (`{name}_middle`) are still published for shades whose `capabilities` flags include `SECONDARY_RAIL`. The secondary name is derived as `"{pt_name}_middle"` since v3 has no `secondaryName` field.

### Periodic polling

`PeriodicStateUpdate` timer retained unchanged. It re-fetches all shade positions on a schedule to recover from any missed SSE events during reconnect windows.

### Dependency changes (`Cargo.toml`)

- **Remove:** `axum`, `matchit`, `serde_urlencoded`
- **Update:** `reqwest` gains `stream` feature: `features = ["json", "stream"]`

---

## Section 4: Remaining Commands

### `list_shades.rs`
- Display `shade.pt_name` instead of `shade.name()`
- Position display uses `pos1_percent()` / `pos2_percent()` (unchanged interface, new backing)

### `inspect_shade.rs`
- Remove `battery_strength`, `smart_power_supply`, `motor` display sections
- `battery_kind` → `power_type`
- `signal_strength` shown as `{value} dBm`
- Positions shown as `primary: X% / secondary: Y% / tilt: Z%`

### `list_scenes.rs`
- Scene members no longer available; output changes to scene name + associated room names
- One extra `list_rooms()` call to resolve room names from `room_ids`
- Output format:
  ```
  SCENE                            ROOMS
  Open Guest                       Bedroom 1, Zen Room
  ```

### `hub_info.rs`
- Calls `get_gateway_data()` instead of `get_user_data()`
- Displays serial number, firmware version, IP, MAC

### `move_shade.rs`
- Removes `Heart` and `Calibrate` from `--motion` clap enum

### `activate_scene.rs`
- No interface change; internal HTTP call changes (GET→PUT, new URL)

### `discovery.rs`
- mDNS service name `_powerview._tcp.local` unchanged (v3 hub uses same name)
- `ResolvedHub.user_data: Option<UserData>` → `gateway_data: Option<GatewayData>`
- `resolve_hub_with_serial()` compares against `gateway_data.serial_number`

---

## Files Changed

| File | Change |
|---|---|
| `Cargo.toml` | Remove axum/matchit/serde_urlencoded; add reqwest stream feature |
| `src/api_types.rs` | Complete rewrite |
| `src/hub.rs` | New endpoints, new response parsing, SSE stream, removed methods |
| `src/discovery.rs` | UserData → GatewayData |
| `src/commands/serve_mqtt.rs` | Remove axum server; add SSE client task |
| `src/commands/hub_info.rs` | GatewayData display |
| `src/commands/list_shades.rs` | pt_name, updated position helpers |
| `src/commands/inspect_shade.rs` | Updated field display |
| `src/commands/list_scenes.rs` | Room-based output, no scene members |
| `src/commands/move_shade.rs` | Remove Heart/Calibrate |
| `src/commands/activate_scene.rs` | No interface change |
