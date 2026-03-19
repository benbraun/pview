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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ShadeData {
    pub id: i32,
    #[serde(rename = "type")]
    pub shade_type: i32,
    pub pt_name: String,
    pub name: String,   // raw base64, stored as-is
    pub capabilities: ShadeCapabilities,
    pub power_type: PowerType,
    #[serde(default)]
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

    /// Returns a human-readable model name for the shade type, matching the
    /// official Home Assistant PowerView integration's type mapping.
    pub fn type_name(&self) -> &'static str {
        match self.shade_type {
            1 => "Designer Roller",
            4 => "Roman",
            5 => "Bottom Up",
            6 => "Duette",
            7 => "Top Down",
            8 => "Duette, Top Down Bottom Up",
            9 => "Duette DuoLite, Top Down Bottom Up",
            10 => "Duette and Applause SkyLift",
            18 => "Pirouette",
            19 => "Provenance Woven Wood",
            23 => "Silhouette",
            26 => "Skyline Panel, Left Stack",
            27 => "Skyline Panel, Right Stack",
            28 => "Skyline Panel, Split Stack",
            31 => "Vignette",
            32 => "Vignette",
            33 => "Duette Architella, Top Down Bottom Up",
            38 => "Silhouette Duolite",
            40 => "Everwood Alternative Wood Blinds",
            42 => "M25T Roller Blind",
            43 => "Facette",
            44 => "Twist",
            47 => "Pleated, Top Down Bottom Up",
            49 => "AC Roller",
            51 => "Venetian, Tilt Anywhere",
            52 => "Banded Shades",
            53 => "Sonnette",
            54 => "Vertical Slats, Left Stack",
            55 => "Vertical Slats, Right Stack",
            56 => "Vertical Slats, Split Stack",
            57 => "Carole Roman Shades",
            62 => "Venetian, Tilt Anywhere",
            65 => "Vignette Duolite",
            66 => "Palm Beach Shutters",
            69 => "Curtain, Left Stack",
            70 => "Curtain, Right Stack",
            71 => "Curtain, Split Stack",
            72 => "Silhouette",
            79 => "Duolite Lift",
            84 => "Vignette",
            95 => "Aura Illuminated, Roller",
            _ => "PowerView Shade",
        }
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
    /// Only present in SSE `targetPositions`; not sent on PUT requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta_in_seconds: Option<f64>,
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
#[derive(Debug, Copy, Clone)]
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

impl serde::Serialize for ShadeCapabilities {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v: i32 = match self {
            Self::BottomUp => 0,
            Self::BottomUpTilt90 => 1,
            Self::BottomUpTilt180 => 2,
            Self::VerticalTilt180 => 3,
            Self::Vertical => 4,
            Self::TiltOnly180 => 5,
            Self::TopDown => 6,
            Self::TopDownBottomUp => 7,
            Self::DualOverlapped => 8,
            Self::DualOverlappedTilt90 => 9,
            Self::DuoliteTilt180 => 10,
            Self::Illuminated => 11,
            Self::Unknown(n) => *n,
        };
        v.serialize(s)
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

/// serde_repr does not support unknown integer fallback, so we use manual impls.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PowerType {
    Battery,
    Hardwired,
    Rechargeable,
    Unknown(i32),
}

impl<'de> serde::Deserialize<'de> for PowerType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match i32::deserialize(d)? {
            0 => Self::Battery,
            1 => Self::Hardwired,
            2 => Self::Rechargeable,
            other => Self::Unknown(other),
        })
    }
}

impl serde::Serialize for PowerType {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v: i32 = match self {
            Self::Battery => 0,
            Self::Hardwired => 1,
            Self::Rechargeable => 2,
            Self::Unknown(n) => *n,
        };
        v.serialize(s)
    }
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
    pub target_positions: Option<ShadePosition>,
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
