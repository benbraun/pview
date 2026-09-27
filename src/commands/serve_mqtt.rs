use crate::api_types::{
    GatewayConfig, PowerType, ShadeCapabilities, ShadeCapabilityFlags, ShadeData, ShadeEvent,
    ShadeEventKind, ShadePosition, ShadeUpdateMotion,
};
use crate::discovery::ResolvedHub;
use crate::hass_helper::*;
use crate::hub::Hub;
use crate::opt_env_var;
use crate::settings::{effective_velocity, StoredSettings};
use crate::version_info::pview_version;
use crate::work_queue::{WorkKind, WorkQueue};
use anyhow::Context;
use arc_swap::ArcSwap;
use futures_util::StreamExt;
use mosquitto_rs::router::*;
use mosquitto_rs::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

const SECONDARY_SUFFIX: &str = "_top";
const MODEL: &str = "pv2mqtt";
const WEZ: &str = "Wez Furlong";
const HUNTER_DOUGLAS: &str = "Hunter Douglas";
const BATTERY_LABEL: &str = "Battery";
const RECHARGEABLE_LABEL: &str = "Rechargeable Battery";
const HARD_WIRED_LABEL: &str = "Hard Wired";

// <https://www.home-assistant.io/integrations/cover.mqtt/>

/// Launch the pv2mqtt bridge, adding your hub to Home Assistant
#[derive(clap::Parser, Debug, Clone)]
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

    /// Persistent velocity settings and discovery manifest (or PV_STATE_FILE).
    #[arg(long)]
    state_file: Option<std::path::PathBuf>,
}

enum ServerEvent {
    MqttMessage {
        router: Arc<MqttRouter<Arc<Pv2MqttState>>>,
        msg: Message,
        received: std::time::Instant,
    },
    ShadeEvent(ShadeEvent),
    PeriodicStateUpdate,
    Register,
    Diagnostics,
    ReconcileMotion {
        shade_id: i32,
        generation: u64,
    },
    HubDiscovered(ResolvedHub),
}

#[derive(Debug)]
enum RegEntry {
    Msg { topic: String, payload: String },
}

impl RegEntry {
    pub fn msg<T: Into<String>, P: Into<String>>(topic: T, payload: P) -> Self {
        Self::Msg {
            topic: topic.into(),
            payload: payload.into(),
        }
    }
}

struct HassRegistration {
    deletes: Vec<RegEntry>,
    configs: Vec<RegEntry>,
    updates: Vec<RegEntry>,
}

impl HassRegistration {
    pub fn new() -> Self {
        Self {
            deletes: vec![],
            configs: vec![],
            updates: vec![],
        }
    }

    pub fn delete<T: Into<String>>(&mut self, topic: T) {
        self.deletes.push(RegEntry::msg(topic, ""));
    }

    pub fn config<T: Into<String>, P: Into<String>>(&mut self, topic: T, payload: P) {
        self.configs.push(RegEntry::msg(topic, payload));
    }

    pub fn update<T: Into<String>, P: Into<String>>(&mut self, topic: T, payload: P) {
        self.updates.push(RegEntry::msg(topic, payload));
    }

    pub async fn apply_updates(self, state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
        let current: HashSet<String> = self
            .configs
            .iter()
            .map(|entry| {
                let RegEntry::Msg { topic, .. } = entry;
                topic.clone()
            })
            .collect();
        let legacy = if state.first_run.load(Ordering::SeqCst) {
            self.deletes
                .into_iter()
                .map(|entry| {
                    let RegEntry::Msg { topic, .. } = entry;
                    topic
                })
                .collect()
        } else {
            vec![]
        };
        let removed = obsolete_configs(&state.known_configs.lock().unwrap(), &current, legacy);
        for topic in removed {
            state
                .client
                .load()
                .publish(&topic, b"", QoS::AtLeastOnce, true)
                .await?;
            state.published.lock().await.remove(&topic);
        }
        for entry in self.configs {
            let RegEntry::Msg { topic, payload } = entry;
            publish_changed(state, topic, discovery_payload(&payload, &state.serial)?).await?;
        }
        for entry in self.updates {
            let RegEntry::Msg { topic, payload } = entry;
            publish_changed(state, topic, payload).await?;
        }
        {
            let _guard = state.persistence.lock().await;
            let changed = *state.known_configs.lock().unwrap() != current;
            if changed {
                let velocities = state.velocities.lock().unwrap().clone();
                state.save_settings(velocities, current.clone()).await?;
                *state.known_configs.lock().unwrap() = current;
            }
        }
        state.first_run.store(false, Ordering::SeqCst);
        Ok(())
    }
}

fn obsolete_configs(
    previous: &HashSet<String>,
    current: &HashSet<String>,
    legacy: Vec<String>,
) -> HashSet<String> {
    previous
        .iter()
        .cloned()
        .chain(legacy)
        .filter(|topic| !current.contains(topic))
        .collect()
}

fn is_ha_birth(status: &str) -> bool {
    status == "online"
}

struct DiagnosticEntity {
    name: String,
    unique_id: String,
    value: String,
}

fn power_type_to_state(power_type: PowerType) -> &'static str {
    match power_type {
        PowerType::Hardwired => HARD_WIRED_LABEL,
        PowerType::Battery => BATTERY_LABEL,
        PowerType::Rechargeable => RECHARGEABLE_LABEL,
        PowerType::Unknown(_) => BATTERY_LABEL,
    }
}

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
                model: gateway_data.model.clone(),
                serial_number: None,
                sw_version: Some(pview_version().to_string()),
                suggested_area: None,
                via_device: None,
            },
            device_class: None,
            origin: Origin::default(),
            unique_id: unique_id.to_string(),
            entity_category: Some("diagnostic".to_string()),
            icon: None,
            enabled_by_default: None,
        },
        state_topic: format!("{MODEL}/sensor/{unique_id}/state"),
        unit_of_measurement: None,
    };

    reg.config(
        format!("{}/sensor/{unique_id}/config", state.discovery_prefix),
        serde_json::to_string(&config)?,
    );
    reg.update(config.base.availability_topic, "online");
    reg.update(
        format!("{MODEL}/sensor/{unique_id}/state"),
        diagnostic.value,
    );
    Ok(())
}

fn runtime_diagnostics(state: &Pv2MqttState) -> Vec<DiagnosticEntity> {
    [
        (
            "SSE Connection",
            "sse",
            if state.sse_connected.load(Ordering::SeqCst) {
                "connected"
            } else {
                "disconnected"
            }
            .to_string(),
        ),
        (
            "MQTT Reconnects",
            "mqtt-reconnects",
            state.mqtt_reconnects.load(Ordering::SeqCst).to_string(),
        ),
        (
            "Command Failures",
            "command-failures",
            state.command_failures.load(Ordering::SeqCst).to_string(),
        ),
        (
            "Last Command Latency (ms)",
            "command-latency",
            state.last_command_ms.load(Ordering::SeqCst).to_string(),
        ),
        (
            "Last Reconciliation",
            "last-reconciliation",
            state.last_reconciliation.lock().unwrap().clone(),
        ),
    ]
    .into_iter()
    .map(|(name, key, value)| DiagnosticEntity {
        name: name.into(),
        unique_id: format!("{}-{key}", state.serial),
        value,
    })
    .collect()
}

async fn publish_runtime_diagnostics(state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
    for diagnostic in runtime_diagnostics(state) {
        publish_changed(
            state,
            format!("{MODEL}/sensor/{}/state", diagnostic.unique_id),
            diagnostic.value,
        )
        .await?;
    }
    Ok(())
}

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
            value: if state.responding.load(Ordering::SeqCst) {
                "OK"
            } else {
                "UNRESPONSIVE"
            }
            .to_string(),
        },
        gateway_data,
        state,
        reg,
    )
    .await?;

    for diagnostic in runtime_diagnostics(state) {
        register_diagnostic_entity(diagnostic, gateway_data, state, reg).await?;
    }
    Ok(())
}

fn coordinate_percent(capabilities: ShadeCapabilities, secondary: bool, pct: u8) -> u8 {
    if !secondary
        && capabilities
            .flags()
            .contains(ShadeCapabilityFlags::PRIMARY_RAIL_REVERSED)
    {
        100 - pct.min(100)
    } else {
        pct
    }
}

fn rail_name(capabilities: ShadeCapabilities, secondary: bool) -> Option<&'static str> {
    if !capabilities
        .flags()
        .contains(ShadeCapabilityFlags::SECONDARY_RAIL)
    {
        return None;
    }
    Some(match (capabilities, secondary) {
        (ShadeCapabilities::TopDownBottomUp, false) => "Bottom",
        (ShadeCapabilities::TopDownBottomUp, true) => "Top",
        (_, false) => "Primary",
        (_, true) => "Secondary",
    })
}

fn capability_config(
    mut value: serde_json::Value,
    capabilities: ShadeCapabilities,
    secondary: bool,
    serial: &str,
    id: i32,
) -> serde_json::Value {
    let flags = capabilities.flags();
    if !secondary && matches!(capabilities, ShadeCapabilities::TiltOnly180) {
        for key in [
            "command_topic",
            "position_topic",
            "set_position_topic",
            "state_topic",
        ] {
            value.as_object_mut().unwrap().remove(key);
        }
    }
    if !secondary
        && flags
            .intersects(ShadeCapabilityFlags::TILT_ANYWHERE | ShadeCapabilityFlags::TILT_ON_CLOSED)
    {
        value["tilt_command_topic"] =
            serde_json::json!(format!("{MODEL}/shade/{serial}/{id}/tilt/set"));
        value["tilt_status_topic"] =
            serde_json::json!(format!("{MODEL}/shade/{serial}/{id}/tilt/state"));
    }
    value
}

fn reported_percent(state: &Pv2MqttState, key: &str, pct: u8) -> u8 {
    if let Ok(address) = key.parse::<ShadeIdAddr>() {
        if let Some(shade) = state.shades.lock().unwrap().get(&address.shade_id) {
            return coordinate_percent(shade.capabilities, address.is_secondary, pct);
        }
    }
    pct
}

fn settled_label(state: &Pv2MqttState, key: &str, pct: u8) -> &'static str {
    if reported_percent(state, key, pct) == 0 {
        "closed"
    } else {
        "open"
    }
}

async fn register_shades(
    state: &Arc<Pv2MqttState>,
    reg: &mut HassRegistration,
) -> anyhow::Result<()> {
    let hub = state.hub.load();
    let revision = state.revision.load(Ordering::SeqCst);
    let shades = hub.hub.list_shades(None).await?;
    cache_snapshot(state, &shades, revision);
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
        let has_secondary = shade
            .capabilities
            .flags()
            .contains(ShadeCapabilityFlags::SECONDARY_RAIL);

        let area = room_by_id.get(&shade.room_id).cloned();
        let device_id = format!("{serial}-{}", shade.id);
        let shade_display_name = match &area {
            Some(room) => format!("{room} {}", shade.pt_name),
            None => shade.pt_name.clone(),
        };
        // For TDBU shades the primary rail is the bottom and the secondary is the top.
        // Give them explicit names so HA shows "Bottom" and "Top" under one device.
        let primary_name = rail_name(shade.capabilities, false).map(str::to_owned);
        let mut shade_ids = vec![(shade.id.to_string(), primary_name, position.pos1_percent())];

        if has_secondary {
            shade_ids.push((
                format!("{}{SECONDARY_SUFFIX}", shade.id),
                rail_name(shade.capabilities, true).map(str::to_owned),
                position.pos2_percent(),
            ));
        }

        let device = Device {
            suggested_area: area,
            identifiers: vec![device_id.clone()],
            via_device: Some(format!("{MODEL}-{serial}")),
            name: shade_display_name,
            manufacturer: HUNTER_DOUGLAS.to_string(),
            model: shade.type_name().to_string(),
            serial_number: Some(shade.serial_number.clone()),
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
                    availability_topic: format!("{MODEL}/shade/{serial}/{shade_id}/availability"),
                    device_class: Some("shade".to_string()),
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: None,
                    icon: None,
                    enabled_by_default: None,
                },
                command_topic: format!("{MODEL}/shade/{serial}/{shade_id}/command"),
                position_topic: format!("{MODEL}/shade/{serial}/{shade_id}/position"),
                set_position_topic: format!("{MODEL}/shade/{serial}/{shade_id}/set_position"),
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
                serde_json::to_string(&capability_config(
                    serde_json::to_value(&config)?,
                    shade.capabilities,
                    shade_id.ends_with(SECONDARY_SUFFIX),
                    serial,
                    shade.id,
                ))?,
            );
            reg.update(
                config.base.availability_topic,
                if state.offline.lock().unwrap().contains(&shade.id) {
                    "offline"
                } else {
                    "online"
                },
            );
            if let Some(pos) = pos.filter(|_| {
                snapshot_is_current(
                    state.is_in_motion(shade.id),
                    revision,
                    state.revision.load(Ordering::SeqCst),
                )
            }) {
                reg.update(
                    format!("{MODEL}/shade/{serial}/{shade_id}/position"),
                    reported_percent(state, &shade_id, pos).to_string(),
                );
                let state_label = settled_label(state, &shade_id, pos);
                reg.update(
                    format!("{MODEL}/shade/{serial}/{shade_id}/state"),
                    state_label,
                );
            }
        }

        if let Some(tilt) = shade.positions.tilt {
            reg.update(
                format!("{MODEL}/shade/{serial}/{}/tilt/state", shade.id),
                ShadePosition::pos_to_percent(tilt).to_string(),
            );
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
                    enabled_by_default: None,
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
                    name: Some("Battery Estimate".to_string()),
                    availability_topic: state.battery_availability_topic(shade),
                    device_class: Some("battery".to_string()),
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: Some("diagnostic".to_string()),
                    icon: None,
                    enabled_by_default: None,
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
                    device_class: Some("signal_strength".to_string()),
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: Some("diagnostic".to_string()),
                    icon: Some("mdi:signal".to_string()),
                    enabled_by_default: Some(false),
                },
                state_topic: format!("{MODEL}/sensor/{device_id}-signal/state"),
                unit_of_measurement: Some("dBm".to_string()),
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
            if let Some(dbm) = shade.signal_strength {
                reg.update(signal.base.availability_topic, "online");
                reg.update(signal.state_topic, format!("{:.0}", dbm));
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
                    enabled_by_default: Some(false),
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
                format!("{}/sensor/{device_id}-psu/config", state.discovery_prefix),
                serde_json::to_string(&power_source)?,
            );
            reg.update(power_source.base.availability_topic, "online");
            reg.update(
                power_source.state_topic,
                power_type_to_state(shade.power_type).to_string(),
            );
        }

        {
            let velocity_pct = state
                .velocities
                .lock()
                .unwrap()
                .get(&shade.id)
                .copied()
                .unwrap_or(0.0)
                * 100.0;
            let velocity = NumberConfig {
                base: EntityConfig {
                    unique_id: format!("{device_id}-velocity"),
                    name: Some("Velocity".to_string()),
                    availability_topic: format!("{MODEL}/shade/{serial}/{}/availability", shade.id),
                    device_class: None,
                    origin: Origin::default(),
                    device: device.clone(),
                    entity_category: Some("config".to_string()),
                    icon: Some("mdi:speedometer".to_string()),
                    enabled_by_default: None,
                },
                state_topic: format!("{MODEL}/shade/{serial}/{}/velocity/state", shade.id),
                command_topic: format!("{MODEL}/shade/{serial}/{}/velocity/set", shade.id),
                min: 0.0,
                max: 100.0,
                step: 1.0,
                mode: "slider".to_string(),
            };
            reg.delete(format!(
                "{}/number/{device_id}-velocity/config",
                state.discovery_prefix
            ));
            reg.config(
                format!(
                    "{}/number/{device_id}-velocity/config",
                    state.discovery_prefix
                ),
                serde_json::to_string(&velocity)?,
            );
            reg.update(velocity.state_topic, format!("{velocity_pct:.0}"));
        }
    }

    Ok(())
}

async fn register_scenes(
    state: &Arc<Pv2MqttState>,
    reg: &mut HassRegistration,
) -> anyhow::Result<()> {
    let hub = state.hub.load();
    let scenes = hub.hub.list_scenes().await?;
    let serial = &state.serial;
    let hub_model = hub.gateway_data.model.clone();

    for scene in scenes {
        let scene_id = scene.id;
        let scene_name = scene.pt_name.clone();
        let unique_id = format!("{serial}-scene-{scene_id}");

        let config = SceneConfig {
            base: EntityConfig {
                device: Device {
                    identifiers: vec![format!("{MODEL}-{serial}")],
                    name: format!("PowerView Hub {serial}"),
                    manufacturer: HUNTER_DOUGLAS.to_string(),
                    model: hub_model.clone(),
                    serial_number: None,
                    suggested_area: None,
                    via_device: None,
                    connections: vec![],
                    sw_version: None,
                },
                availability_topic: format!("{MODEL}/scene/{serial}/{scene_id}/availability"),
                device_class: None,
                name: Some(scene_name),
                origin: Origin::default(),
                unique_id: unique_id.clone(),
                entity_category: None,
                icon: None,
                enabled_by_default: None,
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

async fn register_with_hass(state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
    publish_availability(
        state,
        format!("{MODEL}/bridge/{}/availability", state.serial),
        true,
    )
    .await?;
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
    set_hub_health(state, true).await?;
    Ok(())
}

fn snapshot_is_current(moving: bool, before: u64, now: u64) -> bool {
    !moving && before == now
}

fn inventory_signature(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(object) = value.as_object_mut() {
        for key in ["positions", "batteryStatus", "signalStrength"] {
            object.remove(key);
        }
    }
    value
}

fn cache_snapshot(state: &Pv2MqttState, shades: &[ShadeData], revision: u64) {
    let mut cache = state.shades.lock().unwrap();
    cache.retain(|id, _| shades.iter().any(|s| s.id == *id));
    for shade in shades {
        let mut shade = shade.clone();
        if !snapshot_is_current(
            state.is_in_motion(shade.id),
            revision,
            state.revision.load(Ordering::SeqCst),
        ) {
            if let Some(previous) = cache.get(&shade.id) {
                shade.positions = previous.positions.clone();
            }
        }
        cache.insert(shade.id, shade);
    }
}

async fn publish_changed(
    state: &Arc<Pv2MqttState>,
    topic: String,
    payload: String,
) -> anyhow::Result<()> {
    let mut published = state.published.lock().await;
    if published.get(&topic) == Some(&payload) {
        return Ok(());
    }
    state
        .client
        .load()
        .publish(
            &topic,
            payload.as_bytes(),
            if topic.ends_with("/availability") || topic.ends_with("/config") {
                QoS::AtLeastOnce
            } else {
                QoS::AtMostOnce
            },
            true,
        )
        .await?;
    published.insert(topic, payload);
    Ok(())
}

async fn refresh_shades(state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
    let revision = state.revision.load(Ordering::SeqCst);
    let shades = state.hub.load().hub.list_shades(None).await?;
    set_hub_health(state, true).await?;
    let changed = {
        let cache = state.shades.lock().unwrap();
        cache.len() != shades.len()
            || shades.iter().any(|shade| {
                cache
                    .get(&shade.id)
                    .map(|old| inventory_signature(serde_json::to_value(old).unwrap()))
                    != Some(inventory_signature(serde_json::to_value(shade).unwrap()))
            })
    };
    cache_snapshot(state, &shades, revision);
    if changed {
        return register_with_hass(state).await;
    }
    for shade in shades {
        if snapshot_is_current(
            state.is_in_motion(shade.id),
            revision,
            state.revision.load(Ordering::SeqCst),
        ) && !state.offline.lock().unwrap().contains(&shade.id)
        {
            advise_hass_of_updated_position(state, &shade).await?;
            for (key, pct) in [
                (rail_entity_id(shade.id, false), shade.pos1_percent()),
                (rail_entity_id(shade.id, true), shade.pos2_percent()),
            ] {
                if let Some(pct) = pct {
                    advise_hass_of_state_label(state, &key, settled_label(state, &key, pct))
                        .await?;
                }
            }
        }
        advise_hass_of_battery_level(state, &shade).await?;
        if let Some(dbm) = shade.signal_strength {
            publish_changed(
                state,
                format!("{MODEL}/sensor/{}-{}-signal/state", state.serial, shade.id),
                format!("{dbm:.0}"),
            )
            .await?;
        }
    }
    *state.last_reconciliation.lock().unwrap() = chrono::Utc::now().to_rfc3339();
    publish_runtime_diagnostics(state).await?;
    Ok(())
}

fn discovery_payload(payload: &str, serial: &str) -> anyhow::Result<String> {
    let mut value: serde_json::Value = serde_json::from_str(payload)?;
    let entity_topic = value
        .as_object_mut()
        .and_then(|o| o.remove("availability_topic"));
    let mut topics = vec![
        format!("{MODEL}/bridge/{serial}/availability"),
        format!("{MODEL}/hub/{serial}/availability"),
    ];
    if let Some(topic) = entity_topic.and_then(|v| v.as_str().map(str::to_owned)) {
        topics.push(topic);
    }
    if let Some(id) = value["device"]["identifiers"][0]
        .as_str()
        .and_then(|id| id.strip_prefix(&format!("{serial}-")))
        .filter(|id| id.parse::<i32>().is_ok())
    {
        let topic = format!("{MODEL}/shade/{serial}/{id}/availability");
        if !topics.contains(&topic) {
            topics.push(topic);
        }
    }
    if value["entity_category"] == "diagnostic"
        && value["device"]["identifiers"][0] == format!("{MODEL}-{serial}")
    {
        topics.retain(|topic| topic != &format!("{MODEL}/hub/{serial}/availability"));
    }
    value["availability_mode"] = serde_json::json!("all");
    value["availability"] = serde_json::json!(topics
        .into_iter()
        .map(|topic| serde_json::json!({"topic": topic}))
        .collect::<Vec<_>>());
    Ok(serde_json::to_string(&value)?)
}

async fn publish_availability(
    state: &Arc<Pv2MqttState>,
    topic: String,
    online: bool,
) -> anyhow::Result<()> {
    state
        .client
        .load()
        .publish(
            topic,
            if online { "online" } else { "offline" },
            QoS::AtLeastOnce,
            true,
        )
        .await?;
    Ok(())
}

async fn set_hub_health(state: &Arc<Pv2MqttState>, online: bool) -> anyhow::Result<()> {
    state.responding.store(online, Ordering::SeqCst);
    publish_availability(
        state,
        format!("{MODEL}/hub/{}/availability", state.serial),
        online,
    )
    .await?;
    publish_changed(
        state,
        format!("{MODEL}/sensor/{}-responding/state", state.serial),
        if online { "OK" } else { "UNRESPONSIVE" }.into(),
    )
    .await
}

async fn set_shade_health(
    state: &Arc<Pv2MqttState>,
    shade_id: i32,
    online: bool,
) -> anyhow::Result<()> {
    if online {
        state.offline.lock().unwrap().remove(&shade_id);
    } else {
        state.offline.lock().unwrap().insert(shade_id);
        state.cancel_motion(shade_id);
    }
    for key in [
        rail_entity_id(shade_id, false),
        rail_entity_id(shade_id, true),
    ] {
        publish_availability(
            state,
            format!("{MODEL}/shade/{}/{key}/availability", state.serial),
            online,
        )
        .await?;
    }
    Ok(())
}

async fn advise_hass_of_unresponsive(state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
    set_hub_health(state, false).await
}

async fn advise_hass_of_state_label(
    state: &Arc<Pv2MqttState>,
    shade_id: &str,
    shade_state: &str,
) -> anyhow::Result<()> {
    let shade_state = if reported_percent(state, shade_id, 0) == 100 {
        match shade_state {
            "opening" => "closing",
            "closing" => "opening",
            other => other,
        }
    } else {
        shade_state
    };
    publish_changed(
        state,
        format!("{MODEL}/shade/{}/{shade_id}/state", state.serial),
        shade_state.to_string(),
    )
    .await?;
    Ok(())
}

async fn advise_hass_of_position(
    state: &Arc<Pv2MqttState>,
    shade_id: &str,
    position: u8,
) -> anyhow::Result<()> {
    publish_changed(
        state,
        format!("{MODEL}/shade/{}/{shade_id}/position", state.serial),
        reported_percent(state, shade_id, position).to_string(),
    )
    .await?;
    state
        .last_published_pos
        .lock()
        .unwrap()
        .insert(shade_id.to_string(), position);

    Ok(())
}

/// Clamps a hub-supplied ETA to a sane range. The value comes from hub
/// JSON we don't control; a negative or NaN value would panic
/// `Duration::from_secs_f64`, and an absurdly large one would leave an
/// interpolation task running for hours.
fn sanitize_eta_secs(eta_secs: f64) -> f64 {
    if eta_secs.is_nan() {
        return 0.25;
    }
    eta_secs.clamp(0.25, 600.0)
}

/// Linear interpolation between two shade percentages; `t` is clamped
/// to 0..=1 so callers can pass elapsed/eta ratios directly.
fn interpolate_pct(start: u8, target: u8, t: f64) -> u8 {
    let t = t.clamp(0.0, 1.0);
    (start as f64 + (target as f64 - start as f64) * t).round() as u8
}

/// Spawns a task that publishes interpolated shade position on each tick
/// until `eta_secs` elapses. The returned `AbortHandle` cancels it early.
fn spawn_position_interpolation(
    state: Arc<Pv2MqttState>,
    shade_id: i32,
    start: ShadePosition,
    target: ShadePosition,
    eta_secs: f64,
    generation: u64,
) -> tokio::task::AbortHandle {
    let eta_secs = sanitize_eta_secs(eta_secs);
    let handle = tokio::spawn(async move {
        let start_time = tokio::time::Instant::now();
        let eta = Duration::from_secs_f64(eta_secs);
        let shade_id_str = format!("{shade_id}");
        let sec_id = format!("{shade_id}{SECONDARY_SUFFIX}");
        // Publish only when the integer percent changes, so the faster
        // tick doesn't multiply MQTT/HA traffic.
        let mut last_primary = None;
        let mut last_secondary = None;
        loop {
            tokio::time::sleep(INTERPOLATION_TICK).await;
            let elapsed = start_time.elapsed();
            let t = elapsed.as_secs_f64() / eta_secs;
            if let (Some(s), Some(tgt)) = (start.pos1_percent(), target.pos1_percent()) {
                let pct = interpolate_pct(s, tgt, t);
                if last_primary != Some(pct) {
                    let _ = advise_hass_of_position(&state, &shade_id_str, pct).await;
                    last_primary = Some(pct);
                }
            }
            if let (Some(s), Some(tgt)) = (start.pos2_percent(), target.pos2_percent()) {
                let pct = interpolate_pct(s, tgt, t);
                if last_secondary != Some(pct) {
                    let _ = advise_hass_of_position(&state, &sec_id, pct).await;
                    last_secondary = Some(pct);
                }
            }
            if elapsed >= eta {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = state
            .events
            .send(ServerEvent::ReconcileMotion {
                shade_id,
                generation,
            })
            .await;
    });
    handle.abort_handle()
}

fn begin_interpolation(
    state: &Arc<Pv2MqttState>,
    shade_id: i32,
    start: ShadePosition,
    target: ShadePosition,
    eta: f64,
    secs_per_pct: f64,
) -> MotionTask {
    let generation = state.next_motion.fetch_add(1, Ordering::SeqCst);
    let abort =
        spawn_position_interpolation(state.clone(), shade_id, start, target, eta, generation);
    MotionTask {
        abort,
        secs_per_pct,
        generation,
    }
}

async fn advise_hass_of_updated_position(
    state: &Arc<Pv2MqttState>,
    shade: &ShadeData,
) -> anyhow::Result<()> {
    if let Some(pct) = shade.pos1_percent() {
        advise_hass_of_position(&state, &format!("{}", shade.id), pct).await?;
    }
    if let Some(pct) = shade.pos2_percent() {
        advise_hass_of_position(&state, &format!("{}{SECONDARY_SUFFIX}", shade.id), pct).await?;
    }
    if let Some(tilt) = shade.positions.tilt {
        publish_changed(
            state,
            format!("{MODEL}/shade/{}/{}/tilt/state", state.serial, shade.id),
            ShadePosition::pos_to_percent(tilt).to_string(),
        )
        .await?;
    }
    Ok(())
}

async fn publish_confirmed_position(
    state: &Arc<Pv2MqttState>,
    shade: &ShadeData,
) -> anyhow::Result<()> {
    advise_hass_of_updated_position(state, shade).await?;
    for (key, pct) in [
        (rail_entity_id(shade.id, false), shade.pos1_percent()),
        (rail_entity_id(shade.id, true), shade.pos2_percent()),
    ] {
        if let Some(pct) = pct {
            advise_hass_of_state_label(state, &key, if pct == 0 { "closed" } else { "open" })
                .await?;
        }
    }
    Ok(())
}

async fn advise_hass_of_battery_level(
    state: &Arc<Pv2MqttState>,
    shade: &ShadeData,
) -> anyhow::Result<()> {
    let availability_topic = state.battery_availability_topic(shade);
    let state_topic = state.battery_state_topic(shade);

    if let Some(pct) = shade.battery_percent() {
        publish_changed(state, state_topic, pct.to_string()).await?;
        publish_changed(state, availability_topic, "online".into()).await?;
    } else {
        publish_changed(state, availability_topic, "offline".into()).await?;
    }

    Ok(())
}

impl ServeMqttCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let mut delay = RECONNECT_BASE_DELAY;
        loop {
            match self.run_session(args).await {
                Ok(()) => return Ok(()),
                Err(err) => log::error!("Bridge startup failed; retrying in {delay:?}: {err:#}"),
            }
            if !args.hub_ip_was_specified_by_user() {
                *args.hub_instance.lock().await = None;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RECONNECT_MAX_DELAY);
        }
    }

    async fn run_session(&self, args: &crate::Args) -> anyhow::Result<()> {
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

        let state_file = self
            .state_file
            .clone()
            .or_else(|| std::env::var_os("PV_STATE_FILE").map(Into::into))
            .unwrap_or_else(|| {
                let base = std::env::var_os("XDG_STATE_HOME")
                    .map(std::path::PathBuf::from)
                    .or_else(|| {
                        std::env::var_os("HOME")
                            .map(|home| std::path::PathBuf::from(home).join(".local/state"))
                    })
                    .unwrap_or_else(|| std::path::PathBuf::from("."));
                // Hex encoding keeps hub-provided serials from becoming filesystem paths.
                let key: String = serial.bytes().map(|b| format!("{b:02x}")).collect();
                base.join("pview").join(format!("{key}.json"))
            });
        let saved = StoredSettings::load(&state_file, &serial)?;
        let client = Client::with_auto_id()?;
        let state = Arc::new(Pv2MqttState {
            hub: ArcSwap::new(Arc::new(FullyResolvedHub {
                hub: resolved.hub.clone(),
                gateway_data,
            })),
            client: ArcSwap::new(Arc::new(client.clone())),
            serial: serial.clone(),
            discovery_prefix: self.discovery_prefix.clone(),
            first_run: AtomicBool::new(true),
            known_configs: std::sync::Mutex::new(saved.discovery_topics),
            responding: AtomicBool::new(true),
            motion_tasks: std::sync::Mutex::new(HashMap::new()),
            velocities: std::sync::Mutex::new(saved.velocities),
            last_published_pos: std::sync::Mutex::new(HashMap::new()),
            shades: std::sync::Mutex::new(HashMap::new()),
            offline: std::sync::Mutex::new(HashSet::new()),
            published: tokio::sync::Mutex::new(HashMap::new()),
            revision: AtomicU64::new(0),
            next_motion: AtomicU64::new(1),
            events: tx.clone(),
            sse_connected: AtomicBool::new(false),
            mqtt_reconnects: AtomicU64::new(0),
            command_failures: AtomicU64::new(0),
            last_command_ms: AtomicU64::new(0),
            last_reconciliation: std::sync::Mutex::new("Never".into()),
            state_file: Some(state_file),
            persistence: tokio::sync::Mutex::new(()),
            hub_changed: tokio::sync::watch::channel(resolved.hub.addr()).0,
        });

        client.set_last_will(
            format!("{MODEL}/bridge/{serial}/availability"),
            "offline",
            QoS::AtLeastOnce,
            true,
        )?;
        client.set_username_and_password(mqtt_username.as_deref(), mqtt_password.as_deref())?;
        client.set_reconnect_delay(RECONNECT_BASE_DELAY, RECONNECT_MAX_DELAY, true)?;
        tokio::time::timeout(
            MQTT_CONNECT_TIMEOUT,
            client.connect(&mqtt_host, mqtt_port.into(), Duration::from_secs(10), None),
        )
        .await
        .context("Initial MQTT connection timed out")?
        .with_context(|| format!("connecting to mqtt broker {mqtt_host}:{mqtt_port}"))?;
        let subscriber = client.subscriber().expect("to own the subscriber");

        let router = build_router(&client, &self.discovery_prefix).await?;
        publish_availability(
            &state,
            format!("{MODEL}/bridge/{serial}/availability"),
            true,
        )
        .await?;
        register_with_hass(&state).await?;

        // Periodic state update timer
        {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut ticks = 0;
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    ticks += 1;
                    let event = if ticks % 15 == 0 {
                        ServerEvent::Register
                    } else {
                        ServerEvent::PeriodicStateUpdate
                    };
                    if let Err(err) = tx.send(event).await {
                        log::error!("{err:#?}");
                        break;
                    }
                }
            });
        }

        // Hub discovery task: watches mDNS so we can follow the hub if its
        // IP changes. Supervised so that a dying mDNS channel restarts
        // discovery instead of silently disabling IP-change tracking.
        if !args.hub_ip_was_specified_by_user() {
            let tx = tx.clone();
            let serial_filter = args.hub_serial()?;
            tokio::spawn(async move {
                supervise_sessions(
                    "mdns discovery",
                    RECONNECT_BASE_DELAY,
                    RECONNECT_MAX_DELAY,
                    Duration::from_secs(60),
                    move || {
                        let tx = tx.clone();
                        let serial_filter = serial_filter.clone();
                        async move {
                            let mut disco = crate::discovery::resolve_hubs(None).await?;
                            while let Some(resolved_hub) = disco.recv().await {
                                log::trace!("disco resolved: {resolved_hub:?}");
                                if let Some(gateway_data) = &resolved_hub.gateway_data {
                                    if let Some(serial) = &serial_filter {
                                        if *serial != gateway_data.serial_number {
                                            continue;
                                        }
                                    }
                                    tx.send(ServerEvent::HubDiscovered(resolved_hub))
                                        .await
                                        .context("serve loop is gone")?;
                                }
                            }
                            anyhow::bail!("mdns discovery channel closed");
                        }
                    },
                )
                .await;
            });
        }

        // SSE shade event listener — reconnects on stream end or error
        {
            let tx = tx.clone();
            let state = state.clone();
            tokio::spawn(async move {
                supervise_sessions("SSE", RECONNECT_BASE_DELAY, RECONNECT_MAX_DELAY, Duration::from_secs(60), move || {
                    let state = state.clone();
                    let tx = tx.clone();
                    async move {
                        let result = async {
                        let mut changed = state.hub_changed.subscribe();
                        let hub = state.hub.load().hub.clone();
                        let stream = hub.shade_events_stream().await?;
                        tokio::pin!(stream);
                        state.sse_connected.store(true, Ordering::SeqCst);
                        let _ = tx.send(ServerEvent::Diagnostics).await;
                        tx.send(ServerEvent::PeriodicStateUpdate).await?;
                        let motions: Vec<_> = state.motion_tasks.lock().unwrap().iter().map(|(id, task)| (*id, task.generation)).collect();
                        for (shade_id, generation) in motions { tx.send(ServerEvent::ReconcileMotion { shade_id, generation }).await?; }
                        loop {
                            tokio::select! {
                                _ = changed.changed() => anyhow::bail!("hub address changed"),
                                event = stream.next() => match event {
                                    Some(event) => tx.send(ServerEvent::ShadeEvent(event?)).await?,
                                    None => anyhow::bail!("SSE stream ended"),
                                }
                            }
                        }
                        }.await;
                        state.sse_connected.store(false, Ordering::SeqCst);
                        let _ = tx.send(ServerEvent::Diagnostics).await;
                        result
                    }
                }).await;
            });
        }

        // Supervised MQTT event loop. Any way the session can die — the
        // client permanently disconnecting, the subscriber channel closing,
        // or a failed resubscribe after reconnect — tears down the session;
        // the supervisor then builds a fresh client and carries on.
        {
            let state = state.clone();
            let discovery_prefix = self.discovery_prefix.to_string();
            let initial = std::sync::Mutex::new(Some((client, subscriber, router)));
            let session_params = MqttSessionParams {
                mqtt_host,
                mqtt_port,
                mqtt_username,
                mqtt_password,
            };
            tokio::spawn(async move {
                supervise_sessions(
                    "mqtt",
                    RECONNECT_BASE_DELAY,
                    RECONNECT_MAX_DELAY,
                    Duration::from_secs(60),
                    move || {
                        let state = state.clone();
                        let tx = tx.clone();
                        let discovery_prefix = discovery_prefix.clone();
                        let params = session_params.clone();
                        let initial = initial.lock().unwrap().take();
                        async move {
                            let (client, subscriber, router) = match initial {
                                Some(session) => session,
                                None => {
                                    connect_mqtt_session(&params, &state, &tx, &discovery_prefix)
                                        .await?
                                }
                            };
                            mqtt_event_pump(client, subscriber, router, tx, discovery_prefix, state)
                                .await
                        }
                    },
                )
                .await;
            });
        }

        self.serve(rx, state).await;
        Ok(())
    }

    async fn handle_mqtt_message(
        &self,
        msg: Message,
        state: &Arc<Pv2MqttState>,
        router: &MqttRouter<Arc<Pv2MqttState>>,
    ) -> anyhow::Result<()> {
        log::debug!("msg: {msg:?}");
        Ok(router.dispatch(msg, Arc::clone(state)).await?)
    }

    async fn handle_shade_event(
        &self,
        state: &Arc<Pv2MqttState>,
        event: ShadeEvent,
    ) -> anyhow::Result<()> {
        log::debug!("SSE shade event: {event:#?}");
        state.revision.fetch_add(1, Ordering::SeqCst);
        let hub = state.hub.load();
        if let Some(positions) = &event.current_positions {
            if let Some(shade) = state.shades.lock().unwrap().get_mut(&event.id) {
                shade.positions = positions.clone();
            }
        }
        if let Some(tilt) = event.current_positions.as_ref().and_then(|p| p.tilt) {
            publish_changed(
                state,
                format!("{MODEL}/shade/{}/{}/tilt/state", state.serial, event.id),
                ShadePosition::pos_to_percent(tilt).to_string(),
            )
            .await?;
        }
        match event.evt {
            ShadeEventKind::MotionStopped => {
                // Cancel any in-progress interpolation for this shade
                state.cancel_motion(event.id);
                if let Some(positions) = &event.current_positions {
                    let shade_id_str = format!("{}", event.id);
                    if let Some(pct) = positions.pos1_percent() {
                        advise_hass_of_position(state, &shade_id_str, pct).await?;
                        let shade_state = settled_label(state, &shade_id_str, pct);
                        advise_hass_of_state_label(state, &shade_id_str, shade_state).await?;
                    }
                    if let Some(pct) = positions.pos2_percent() {
                        let sec_id = format!("{}{SECONDARY_SUFFIX}", event.id);
                        advise_hass_of_position(state, &sec_id, pct).await?;
                        let shade_state = settled_label(state, &sec_id, pct);
                        advise_hass_of_state_label(state, &sec_id, shade_state).await?;
                    }
                }
            }
            ShadeEventKind::MotionStarted => {
                let shade_id_str = rail_entity_id(event.id, false);
                // A TDBU event names both rails whichever one was driven, so
                // label each rail from its own travel. A rail that isn't
                // moving gets nothing and keeps the state it already has.
                match (&event.current_positions, &event.target_positions) {
                    (Some(cur), Some(tgt)) => {
                        if let Some(label) = motion_label(cur.pos1_percent(), tgt.pos1_percent()) {
                            advise_hass_of_state_label(state, &shade_id_str, label).await?;
                        }
                        if let Some(label) = motion_label(cur.pos2_percent(), tgt.pos2_percent()) {
                            let sec_id = rail_entity_id(event.id, true);
                            advise_hass_of_state_label(state, &sec_id, label).await?;
                        }
                    }
                    // No positions to compare: all we know is that it moved.
                    _ => advise_hass_of_state_label(state, &shade_id_str, "opening").await?,
                }
                // Cancel any previous interpolation task for this shade
                let _ = state.cancel_motion(event.id);
                // Spawn position interpolation if we have enough data
                {
                    let current = event.current_positions.unwrap_or_default();
                    let target = event.target_positions.unwrap_or_default();
                    let hub_eta = target.eta_in_seconds.unwrap_or(30.0);
                    {
                        // Derive the shade's travel rate so that a command
                        // superseding this move can estimate its own ETA.
                        let rail_distance = |c: Option<u8>, t: Option<u8>| match (c, t) {
                            (Some(c), Some(t)) => c.abs_diff(t),
                            _ => 0,
                        };
                        let distance = rail_distance(current.pos1_percent(), target.pos1_percent())
                            .max(rail_distance(current.pos2_percent(), target.pos2_percent()));
                        let secs_per_pct = if distance > 0 {
                            hub_eta / distance as f64
                        } else {
                            0.0
                        };
                        let task = begin_interpolation(
                            state,
                            event.id,
                            current,
                            target,
                            interpolation_eta(hub_eta),
                            secs_per_pct,
                        );
                        state.motion_tasks.lock().unwrap().insert(event.id, task);
                    }
                }
            }
            ShadeEventKind::ShadeOffline => {
                set_shade_health(state, event.id, false).await?;
            }
            ShadeEventKind::ShadeOnline | ShadeEventKind::BatteryAlert => {
                if event.evt == ShadeEventKind::ShadeOnline {
                    state.offline.lock().unwrap().remove(&event.id);
                }
                match hub.hub.shade_by_id(event.id).await {
                    Ok(shade) => {
                        if event.evt == ShadeEventKind::ShadeOnline {
                            set_shade_health(state, shade.id, true).await?;
                        }
                        state.shades.lock().unwrap().insert(shade.id, shade.clone());
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
                state.hub_changed.send_replace(new_hub.hub.addr());
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

    async fn serve(&self, mut rx: Receiver<ServerEvent>, state: Arc<Pv2MqttState>) {
        log::info!(
            "Version {}. Waiting for mqtt and pv messages",
            pview_version()
        );
        let commands = WorkQueue::new(64);
        let background = WorkQueue::new(32);
        while let Some(msg) = rx.recv().await {
            let (queue, kind) = match &msg {
                ServerEvent::MqttMessage { msg, .. }
                    if msg.topic != format!("{}/status", self.discovery_prefix) =>
                {
                    let key = msg
                        .topic
                        .rsplit_once('/')
                        .map(|(base, _)| base)
                        .unwrap_or(&msg.topic)
                        .to_string();
                    let kind = if msg.topic.ends_with("/command") && msg.payload == b"STOP" {
                        WorkKind::Stop(key.trim_end_matches(SECONDARY_SUFFIX).to_string())
                    } else if msg.topic.ends_with("/set_position")
                        || (msg.topic.ends_with("/command")
                            && (msg.payload == b"OPEN" || msg.payload == b"CLOSE"))
                    {
                        WorkKind::Replace(key)
                    } else {
                        WorkKind::Ordered
                    };
                    (&commands, kind)
                }
                ServerEvent::ShadeEvent(event)
                    if !matches!(
                        event.evt,
                        ShadeEventKind::ShadeOnline | ShadeEventKind::BatteryAlert
                    ) =>
                {
                    self.process_event(msg, &state).await;
                    continue;
                }
                ServerEvent::PeriodicStateUpdate => {
                    (&background, WorkKind::Replace("refresh".into()))
                }
                _ => (&background, WorkKind::Ordered),
            };
            let state = state.clone();
            let command = self.clone();
            if let Err(err) = queue.submit(kind, async move {
                command.process_event(msg, &state).await;
            }) {
                log::error!("{err:#}");
            }
        }
    }

    async fn process_event(&self, msg: ServerEvent, state: &Arc<Pv2MqttState>) {
        match msg {
            ServerEvent::MqttMessage {
                msg,
                router,
                received,
            } => {
                let command = msg.topic != format!("{}/status", self.discovery_prefix);
                let result = self.handle_mqtt_message(msg, state, &router).await;
                if command {
                    state.last_command_ms.store(
                        received.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        Ordering::SeqCst,
                    );
                    if result.is_err() {
                        state.command_failures.fetch_add(1, Ordering::SeqCst);
                    }
                    let _ = publish_runtime_diagnostics(state).await;
                }
                if let Err(err) = result {
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
            ServerEvent::ReconcileMotion {
                shade_id,
                generation,
            } => {
                if !state
                    .motion_tasks
                    .lock()
                    .unwrap()
                    .get(&shade_id)
                    .map(|t| t.generation == generation)
                    .unwrap_or(false)
                {
                    return;
                }
                let result = state.hub.load().hub.shade_by_id(shade_id).await;
                if !state.cancel_motion_generation(shade_id, generation) {
                    return;
                }
                match result {
                    Ok(shade) => {
                        state.shades.lock().unwrap().insert(shade_id, shade.clone());
                        if let Err(err) = publish_confirmed_position(state, &shade).await {
                            log::warn!("Motion reconciliation: {err:#}");
                        }
                    }
                    Err(err) => {
                        let _ = set_shade_health(state, shade_id, false).await;
                        log::warn!("Motion reconciliation failed for {shade_id}: {err:#}");
                    }
                }
            }
            ServerEvent::Diagnostics => {
                let _ = publish_runtime_diagnostics(state).await;
            }
            ServerEvent::Register => {
                state.published.lock().await.clear();
                if let Err(err) = register_with_hass(state).await {
                    log::error!("Registering with HA: {err:#}");
                    let _ = advise_hass_of_unresponsive(state).await;
                }
            }
            ServerEvent::PeriodicStateUpdate => {
                if let Err(err) = refresh_shades(state).await {
                    log::error!("During register_with_hass: {err:#?}");
                    if let Err(err) = advise_hass_of_unresponsive(state).await {
                        log::error!("Reporting hub availability: {err:#}");
                    }
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct SerialAndScene {
    serial: String,
    #[serde(deserialize_with = "parse_deser")]
    scene_id: i32,
}

async fn mqtt_scene_activate(
    Params(SerialAndScene { serial, scene_id }): Params<SerialAndScene>,
    Topic(topic): Topic,
    State(state): State<Arc<Pv2MqttState>>,
) -> anyhow::Result<()> {
    if serial != state.serial {
        log::warn!(
            "ignoring {topic} which is intended for \
                    serial={serial}, while we are serial {actual_serial}",
            actual_serial = state.serial
        );
        return Ok(());
    }

    state.hub.load().hub.activate_scene(scene_id).await?;
    Ok(())
}

struct ShadeIdAddr {
    shade_id: i32,
    is_secondary: bool,
}

impl FromStr for ShadeIdAddr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<ShadeIdAddr> {
        let (shade_id, is_secondary) = if let Some(id) = s.strip_suffix(SECONDARY_SUFFIX) {
            (id.parse::<i32>()?, true)
        } else {
            (s.parse::<i32>()?, false)
        };
        Ok(ShadeIdAddr {
            shade_id,
            is_secondary,
        })
    }
}

#[derive(Deserialize)]
struct SerialAndShade {
    serial: String,
    #[serde(deserialize_with = "parse_deser")]
    shade_id: ShadeIdAddr,
}
/// An interpolation in flight for one shade.
struct MotionTask {
    abort: tokio::task::AbortHandle,
    generation: u64,
    /// Seconds per percent of travel, derived from the hub's ETA for this
    /// move. Retains the shade's observed speed (which depends on its
    /// configured velocity) so a command that supersedes this move can
    /// estimate its own ETA. Zero when the rate could not be derived.
    secs_per_pct: f64,
}

/// The hub's ETA covers the whole move including deceleration and its own
/// reporting lag, so finish interpolating slightly early rather than parking
/// the graphic at the target while the shade is still travelling.
fn interpolation_eta(hub_eta_secs: f64) -> f64 {
    (hub_eta_secs - 1.5).max(0.5)
}

/// Estimates the ETA for a retargeted move from the travel rate observed on
/// the move it supersedes. `None` when there is nothing worth animating.
fn plan_retarget_interpolation(secs_per_pct: f64, current: u8, target: u8) -> Option<f64> {
    if !secs_per_pct.is_finite() || secs_per_pct <= 0.0 {
        return None;
    }
    let distance = current.abs_diff(target);
    if distance == 0 {
        return None;
    }
    Some(sanitize_eta_secs(interpolation_eta(
        secs_per_pct * distance as f64,
    )))
}

/// The MQTT entity id for one rail of a shade. TDBU shades expose the
/// secondary (top) rail as a separate cover under the same device.
fn rail_entity_id(shade_id: i32, is_secondary: bool) -> String {
    if is_secondary {
        format!("{shade_id}{SECONDARY_SUFFIX}")
    } else {
        format!("{shade_id}")
    }
}

/// A position payload addressing a single rail. The v3 hub preserves axes
/// omitted from the request body, so naming only the rail being moved
/// leaves the other one where it is.
fn rail_position(is_secondary: bool, pct: u8, velocity: Option<f64>) -> ShadePosition {
    let pos = Some(ShadePosition::percent_to_pos(pct));
    if is_secondary {
        ShadePosition {
            secondary: pos,
            velocity,
            ..Default::default()
        }
    } else {
        ShadePosition {
            primary: pos,
            velocity,
            ..Default::default()
        }
    }
}

/// Direction label for one rail of a `MotionStarted` event, from that
/// rail's own travel. `None` means "say nothing": either the rail has no
/// reported position, or it isn't moving on this event and must keep the
/// state it already has rather than be told the other rail's direction.
fn motion_label(current: Option<u8>, target: Option<u8>) -> Option<&'static str> {
    match (current, target) {
        (Some(cur), Some(tgt)) if tgt > cur => Some("opening"),
        (Some(cur), Some(tgt)) if tgt < cur => Some("closing"),
        _ => None,
    }
}

/// What to do about an incoming position command.
#[derive(Debug, PartialEq)]
enum PositionCommandPlan {
    /// The shade is already parked there; don't bother the hub.
    Skip,
    /// Send it, optionally publishing a motion state label first.
    Send(Option<&'static str>),
}

/// Decides how to react to a position command.
///
/// `current` is our best estimate of where the rail is now. The
/// skip-if-already-there optimisation only applies at rest: mid-motion the
/// shade is travelling *away* from `current`, so dropping the command would
/// strand it heading for the previous target.
fn plan_position_command(target: u8, current: Option<u8>, in_motion: bool) -> PositionCommandPlan {
    match current {
        Some(cur) if target > cur => PositionCommandPlan::Send(Some("opening")),
        Some(cur) if target < cur => PositionCommandPlan::Send(Some("closing")),
        Some(_) if !in_motion => PositionCommandPlan::Skip,
        // Moving, and commanded to roughly where it is right now: it will
        // stop here. Don't guess a direction; the imminent motion event
        // resolves the state.
        Some(_) => PositionCommandPlan::Send(None),
        None => PositionCommandPlan::Send(Some("opening")),
    }
}

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
    let shade = state
        .shades
        .lock()
        .unwrap()
        .get(&shade_id)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!("Unknown shade {shade_id}; waiting for inventory refresh")
        })?;

    anyhow::ensure!(position <= 100, "Position must be in 0..=100");
    anyhow::ensure!(
        shade.capabilities.flags().contains(if is_secondary {
            ShadeCapabilityFlags::SECONDARY_RAIL
        } else {
            ShadeCapabilityFlags::PRIMARY_RAIL
        }),
        "Shade does not support this rail"
    );
    let position = coordinate_percent(shade.capabilities, is_secondary, position);
    let velocity = state.velocities.lock().unwrap().get(&shade_id).copied();
    let pos = rail_position(is_secondary, position, velocity);

    log::info!(
        "Set {shade_id} {} {} to {position}%",
        shade.pt_name,
        if is_secondary { "secondary" } else { "primary" }
    );
    let shade_id_str = rail_entity_id(shade_id, is_secondary);
    let hub_pct = if is_secondary {
        shade.pos2_percent()
    } else {
        shade.pos1_percent()
    };
    let (in_motion, current) = current_estimate(&state, shade_id, &shade_id_str, hub_pct);
    if plan_position_command(position, current, in_motion) == PositionCommandPlan::Skip {
        return Ok(());
    }
    let revision = state.revision.load(Ordering::SeqCst);
    if let Err(err) = hub.hub.set_shade_position(shade_id, pos).await {
        let _ = state.events.try_send(ServerEvent::PeriodicStateUpdate);
        return Err(err);
    }
    if state.revision.load(Ordering::SeqCst) != revision {
        return Ok(());
    }
    match plan_position_command(position, current, in_motion) {
        PositionCommandPlan::Skip => {
            log::info!(
                "Shade {shade_id} already at {position}%, skipping duplicate set-position command"
            );
            return Ok(());
        }
        PositionCommandPlan::Send(label) => {
            // This command supersedes whatever target an interpolation was
            // driving toward: stop animating to the stale one and start
            // animating from here to the new target.
            let superseded = state.cancel_motion(shade_id);
            if let Some(label) = label {
                advise_hass_of_state_label(&state, &shade_id_str, label).await?;
            }
            retarget_interpolation(
                &state,
                shade_id,
                is_secondary,
                superseded,
                current,
                position,
            );
        }
    }
    Ok(())
}

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
    let shade = state
        .shades
        .lock()
        .unwrap()
        .get(&shade_id)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!("Unknown shade {shade_id}; waiting for inventory refresh")
        })?;

    log::info!(
        "{command} {shade_id} {} {}",
        shade.pt_name,
        if is_secondary { "secondary" } else { "primary" }
    );
    match command.as_ref() {
        // Both ends of the travel take the same path; only the target
        // differs. Sharing it keeps the addressed rail in one place.
        "OPEN" | "CLOSE" => {
            let target = if command == "OPEN" { 100 } else { 0 };
            return mqtt_shade_set_position(
                Params(SerialAndShade {
                    serial,
                    shade_id: ShadeIdAddr {
                        shade_id,
                        is_secondary,
                    },
                }),
                Topic(topic),
                State(state),
                Payload(target),
            )
            .await;
        }
        "STOP" => {
            // The shade halts wherever it is, so any interpolation toward
            // the old target is now wrong, and there is no new target to
            // animate toward. MotionStopped reports where it actually
            // ended up.
            let revision = state.revision.load(Ordering::SeqCst);
            hub.hub
                .move_shade(shade_id, ShadeUpdateMotion::Stop)
                .await?;
            if state.revision.load(Ordering::SeqCst) == revision {
                state.cancel_motion(shade_id);
                let task = begin_interpolation(
                    &state,
                    shade_id,
                    ShadePosition::default(),
                    ShadePosition::default(),
                    0.25,
                    0.0,
                );
                state.motion_tasks.lock().unwrap().insert(shade_id, task);
            }
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

async fn mqtt_shade_set_tilt(
    Params(SerialAndShade {
        serial,
        shade_id: ShadeIdAddr {
            shade_id,
            is_secondary,
        },
    }): Params<SerialAndShade>,
    State(state): State<Arc<Pv2MqttState>>,
    Payload(tilt): Payload<u8>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        serial == state.serial && !is_secondary,
        "Invalid tilt address"
    );
    anyhow::ensure!(tilt <= 100, "Tilt must be in 0..=100");
    let shade = state
        .shades
        .lock()
        .unwrap()
        .get(&shade_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Unknown shade {shade_id}"))?;
    let flags = shade.capabilities.flags();
    anyhow::ensure!(
        flags
            .intersects(ShadeCapabilityFlags::TILT_ANYWHERE | ShadeCapabilityFlags::TILT_ON_CLOSED),
        "Shade does not support tilt"
    );
    anyhow::ensure!(
        !flags.contains(ShadeCapabilityFlags::TILT_ON_CLOSED)
            || (shade.pos1_percent() == Some(0) && !state.is_in_motion(shade_id)),
        "Close the shade before adjusting tilt"
    );
    state
        .hub
        .load()
        .hub
        .set_shade_position(
            shade_id,
            ShadePosition {
                tilt: Some(ShadePosition::percent_to_pos(tilt)),
                ..Default::default()
            },
        )
        .await?;
    let _ = state.events.try_send(ServerEvent::PeriodicStateUpdate);
    Ok(())
}

async fn mqtt_shade_set_velocity(
    params: Params<SerialAndShade>,
    Topic(topic): Topic,
    State(state): State<Arc<Pv2MqttState>>,
    Payload(value): Payload<f64>,
) -> anyhow::Result<()> {
    let Params(SerialAndShade {
        serial,
        shade_id: ShadeIdAddr { shade_id, .. },
    }) = params;

    if serial != state.serial {
        log::warn!(
            "ignoring {topic} which is intended for \
                    serial={serial}, while we are serial {actual_serial}",
            actual_serial = state.serial
        );
        return Ok(());
    }

    anyhow::ensure!(
        state.shades.lock().unwrap().contains_key(&shade_id),
        "Unknown shade {shade_id}"
    );
    let velocity = effective_velocity(value)?;
    {
        let _guard = state.persistence.lock().await;
        let mut velocities = state.velocities.lock().unwrap().clone();
        match velocity {
            Some(value) => {
                velocities.insert(shade_id, value);
            }
            None => {
                velocities.remove(&shade_id);
            }
        }
        let configs = state.known_configs.lock().unwrap().clone();
        state.save_settings(velocities.clone(), configs).await?;
        *state.velocities.lock().unwrap() = velocities;
    }
    let value = velocity.unwrap_or(0.0) * 100.0;

    publish_changed(
        &state,
        format!("{MODEL}/shade/{serial}/{shade_id}/velocity/state"),
        format!("{value:.0}"),
    )
    .await?;

    log::info!("Set velocity for shade {shade_id} to {value:.0}%");
    Ok(())
}

async fn mqtt_homeassitant_status(
    Payload(status): Payload<String>,
    State(state): State<Arc<Pv2MqttState>>,
) -> anyhow::Result<()> {
    log::info!("Home Assistant status changed: {status}",);
    if !is_ha_birth(&status) {
        return Ok(());
    }
    state.published.lock().await.clear();
    register_with_hass(&state).await
}

struct FullyResolvedHub {
    hub: Hub,
    gateway_data: GatewayConfig,
}

struct Pv2MqttState {
    hub: ArcSwap<FullyResolvedHub>,
    /// Swapped out for a fresh client when an MQTT session dies
    /// and gets rebuilt by the session supervisor.
    client: ArcSwap<Client>,
    serial: String,
    discovery_prefix: String,
    state_file: Option<std::path::PathBuf>,
    persistence: tokio::sync::Mutex<()>,
    first_run: AtomicBool,
    known_configs: std::sync::Mutex<HashSet<String>>,
    responding: AtomicBool,
    motion_tasks: std::sync::Mutex<HashMap<i32, MotionTask>>,
    shades: std::sync::Mutex<HashMap<i32, ShadeData>>,
    offline: std::sync::Mutex<HashSet<i32>>,
    published: tokio::sync::Mutex<HashMap<String, String>>,
    revision: AtomicU64,
    next_motion: AtomicU64,
    events: tokio::sync::mpsc::Sender<ServerEvent>,
    sse_connected: AtomicBool,
    mqtt_reconnects: AtomicU64,
    command_failures: AtomicU64,
    last_command_ms: AtomicU64,
    last_reconciliation: std::sync::Mutex<String>,
    hub_changed: tokio::sync::watch::Sender<std::net::IpAddr>,
    /// Per-shade velocity (0.0–1.0). HA-driven; hub always reports 0 so we track it ourselves.
    velocities: std::sync::Mutex<HashMap<i32, f64>>,
    /// Last percent published per rail (keyed like the mqtt topic id, so
    /// `"5"` and `"5_top"` are tracked separately). While a shade is moving
    /// this is a better estimate of where it is than the hub's REST value,
    /// which reports the pre-motion position until the shade settles.
    last_published_pos: std::sync::Mutex<HashMap<String, u8>>,
}

impl Pv2MqttState {
    async fn save_settings(
        &self,
        velocities: HashMap<i32, f64>,
        discovery_topics: HashSet<String>,
    ) -> anyhow::Result<()> {
        if let Some(path) = self.state_file.clone() {
            let settings = StoredSettings {
                serial: self.serial.clone(),
                velocities,
                discovery_topics,
            };
            tokio::task::spawn_blocking(move || settings.save(&path)).await??;
        }
        Ok(())
    }

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

    /// True while an interpolation task is driving this shade, i.e. we
    /// believe it is physically moving.
    fn is_in_motion(&self, shade_id: i32) -> bool {
        self.motion_tasks.lock().unwrap().contains_key(&shade_id)
    }

    /// Stops interpolating this shade, returning the task that was in flight
    /// so the caller can reuse its travel rate. Callers do this when a new
    /// command supersedes the target the interpolation was driving toward.
    fn cancel_motion(&self, shade_id: i32) -> Option<MotionTask> {
        let task = self.motion_tasks.lock().unwrap().remove(&shade_id);
        if let Some(task) = &task {
            task.abort.abort();
        }
        task
    }

    fn cancel_motion_generation(&self, shade_id: i32, generation: u64) -> bool {
        let mut tasks = self.motion_tasks.lock().unwrap();
        if tasks.get(&shade_id).map(|t| t.generation) != Some(generation) {
            return false;
        }
        if let Some(task) = tasks.remove(&shade_id) {
            task.abort.abort();
        }
        true
    }

    fn last_published_pct(&self, key: &str) -> Option<u8> {
        self.last_published_pos.lock().unwrap().get(key).copied()
    }
}

/// Starts interpolating from where we believe the shade is now toward a
/// newly commanded target, reusing the travel rate of the move being
/// superseded.
///
/// The hub does not reliably send a fresh `MotionStarted` when a shade that
/// is already moving gets retargeted, so without this the reported position
/// would sit frozen at the point of the new command until the shade stopped.
/// If a `MotionStarted` does arrive it replaces this estimate with the hub's
/// authoritative figures, and `MotionStopped` snaps to the true position
/// either way.
fn retarget_interpolation(
    state: &Arc<Pv2MqttState>,
    shade_id: i32,
    is_secondary: bool,
    superseded: Option<MotionTask>,
    current: Option<u8>,
    target_pct: u8,
) {
    let rate = superseded.as_ref().map(|t| t.secs_per_pct).unwrap_or(0.0);
    let eta = current.and_then(|c| plan_retarget_interpolation(rate, c, target_pct));
    let (start, target, eta) = match (current, eta) {
        (Some(current), Some(eta)) => (
            rail_position(is_secondary, current, None),
            rail_position(is_secondary, target_pct, None),
            eta,
        ),
        _ => (ShadePosition::default(), ShadePosition::default(), 30.0),
    };
    let task = begin_interpolation(state, shade_id, start, target, eta, rate);
    state.motion_tasks.lock().unwrap().insert(shade_id, task);
}

/// Best estimate of where a rail is right now, and whether the shade is
/// moving. Mid-motion our interpolated estimate beats the hub's REST value,
/// which keeps reporting the position the shade started from.
///
/// Read this *before* calling `cancel_motion` — cancelling clears the
/// in-motion flag this depends on.
fn current_estimate(
    state: &Arc<Pv2MqttState>,
    shade_id: i32,
    key: &str,
    hub_pct: Option<u8>,
) -> (bool, Option<u8>) {
    let in_motion = state.is_in_motion(shade_id);
    let current = if in_motion {
        state.last_published_pct(key).or(hub_pct)
    } else {
        hub_pct
    };
    (in_motion, current)
}

const RECONNECT_BASE_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(60);
const MQTT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// How often a moving shade's interpolated position is recomputed.
/// 100ms captures nearly every integer percent step of typical shade
/// travel, which is as smooth as HA's integer position model allows.
const INTERPOLATION_TICK: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct MqttSessionParams {
    mqtt_host: String,
    mqtt_port: u16,
    mqtt_username: Option<String>,
    mqtt_password: Option<String>,
}

/// Subscribes to the topics we serve and returns the router for them.
/// Deliberately does not talk to the hub: a slow or unresponsive hub
/// must not be able to fail MQTT (re)connection.
async fn build_router(
    client: &Client,
    discovery_prefix: &str,
) -> anyhow::Result<Arc<MqttRouter<Arc<Pv2MqttState>>>> {
    let mut router: MqttRouter<Arc<Pv2MqttState>> = MqttRouter::new(client.clone());
    router
        .route(
            format!("{discovery_prefix}/status"),
            mqtt_homeassitant_status,
        )
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
    router
        .route(
            format!("{MODEL}/shade/:serial/:shade_id/velocity/set"),
            mqtt_shade_set_velocity,
        )
        .await?;
    router
        .route(
            format!("{MODEL}/shade/:serial/:shade_id/tilt/set"),
            mqtt_shade_set_tilt,
        )
        .await?;
    Ok(Arc::new(router))
}

/// Builds a fresh MQTT client, connects it, subscribes our routes, and
/// publishes the new client into `state` so that all publishers use it.
async fn connect_mqtt_session(
    params: &MqttSessionParams,
    state: &Arc<Pv2MqttState>,
    tx: &tokio::sync::mpsc::Sender<ServerEvent>,
    discovery_prefix: &str,
) -> anyhow::Result<(
    Client,
    async_channel::Receiver<Event>,
    Arc<MqttRouter<Arc<Pv2MqttState>>>,
)> {
    let client = Client::with_auto_id()?;
    client.set_last_will(
        format!("{MODEL}/bridge/{}/availability", state.serial),
        "offline",
        QoS::AtLeastOnce,
        true,
    )?;
    client.set_username_and_password(
        params.mqtt_username.as_deref(),
        params.mqtt_password.as_deref(),
    )?;
    client.set_reconnect_delay(RECONNECT_BASE_DELAY, RECONNECT_MAX_DELAY, true)?;
    tokio::time::timeout(
        MQTT_CONNECT_TIMEOUT,
        client.connect(
            &params.mqtt_host,
            params.mqtt_port.into(),
            Duration::from_secs(10),
            None,
        ),
    )
    .await
    .context("timed out connecting to mqtt broker")?
    .with_context(|| {
        format!(
            "connecting to mqtt broker {}:{}",
            params.mqtt_host, params.mqtt_port
        )
    })?;
    let subscriber = client
        .subscriber()
        .ok_or_else(|| anyhow::anyhow!("subscriber channel unavailable on new client"))?;
    let router = build_router(&client, discovery_prefix).await?;
    state.client.store(Arc::new(client.clone()));
    state.mqtt_reconnects.fetch_add(1, Ordering::SeqCst);
    // Ask the serve loop to re-register everything with hass; that path
    // tolerates (and reports) an unresponsive hub instead of failing us.
    tx.send(ServerEvent::Register).await.ok();
    Ok((client, subscriber, router))
}

/// Forwards MQTT events to the serve loop until the session dies.
/// Transient disconnects are handled by libmosquitto's auto-reconnect;
/// we resubscribe when the connection comes back. Returns an error when
/// the session is beyond repair and must be rebuilt from a new client.
async fn mqtt_event_pump(
    client: Client,
    subscriber: async_channel::Receiver<Event>,
    mut router: Arc<MqttRouter<Arc<Pv2MqttState>>>,
    tx: tokio::sync::mpsc::Sender<ServerEvent>,
    discovery_prefix: String,
    state: Arc<Pv2MqttState>,
) -> anyhow::Result<()> {
    let mut need_rebuild = false;
    loop {
        let event = subscriber
            .recv()
            .await
            .map_err(|_| anyhow::anyhow!("mqtt client closed the event channel"))?;
        match event {
            Event::Message(msg) => {
                tx.send(ServerEvent::MqttMessage {
                    msg,
                    router: router.clone(),
                    received: std::time::Instant::now(),
                })
                .await
                .context("serve loop is gone")?;
            }
            Event::Disconnected(reason) => {
                log::warn!("MQTT disconnected: {reason}; waiting for reconnect");
                need_rebuild = true;
            }
            Event::Connected(status) => {
                log::info!("MQTT (re)connected {status}");
                if need_rebuild {
                    state.mqtt_reconnects.fetch_add(1, Ordering::SeqCst);
                    router = build_router(&client, &discovery_prefix)
                        .await
                        .context("resubscribing after mqtt reconnect")?;
                    need_rebuild = false;
                    // Re-register with hass via the fault-tolerant serve loop path
                    tx.send(ServerEvent::Register).await.ok();
                }
            }
        }
    }
}

/// Runs `make_session` forever, restarting it whenever it ends.
/// Restarts are delayed with exponential backoff between `base_delay` and
/// `max_delay`; a session that survives at least `healthy_after` resets
/// the backoff to `base_delay`.
async fn supervise_sessions<F, Fut>(
    name: &str,
    base_delay: Duration,
    max_delay: Duration,
    healthy_after: Duration,
    mut make_session: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut delay = base_delay;
    loop {
        let started = tokio::time::Instant::now();
        match make_session().await {
            Ok(()) => log::warn!("{name}: session ended unexpectedly"),
            Err(err) => log::error!("{name}: session failed: {err:#}"),
        }
        if started.elapsed() >= healthy_after {
            delay = base_delay;
        }
        log::info!("{name}: restarting session in {delay:?}");
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(max_delay);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    fn test_state() -> (Arc<Pv2MqttState>, Receiver<ServerEvent>) {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let gateway_data = serde_json::from_value(serde_json::json!({
            "serialNumber": "test", "brand": "HD", "model": "G3",
            "firmware": {"mainProcessor": {"name": "test", "revision": 1, "subRevision": 0, "build": 0}},
            "networkStatus": {"ipAddress": "127.0.0.1", "primaryMacAddress": "00:00:00:00:00:00"}
        })).unwrap();
        (
            Arc::new(Pv2MqttState {
                hub: ArcSwap::new(Arc::new(FullyResolvedHub {
                    hub: Hub::with_addr("127.0.0.1".parse().unwrap()),
                    gateway_data,
                })),
                client: ArcSwap::new(Arc::new(Client::with_auto_id().unwrap())),
                serial: "test".into(),
                discovery_prefix: "homeassistant".into(),
                first_run: AtomicBool::new(true),
                known_configs: std::sync::Mutex::new(HashSet::new()),
                responding: AtomicBool::new(true),
                motion_tasks: std::sync::Mutex::new(HashMap::new()),
                velocities: std::sync::Mutex::new(HashMap::new()),
                last_published_pos: std::sync::Mutex::new(HashMap::new()),
                shades: std::sync::Mutex::new(HashMap::new()),
                offline: std::sync::Mutex::new(HashSet::new()),
                published: tokio::sync::Mutex::new(HashMap::new()),
                revision: AtomicU64::new(0),
                next_motion: AtomicU64::new(1),
                events: tx,
                sse_connected: AtomicBool::new(false),
                mqtt_reconnects: AtomicU64::new(0),
                command_failures: AtomicU64::new(0),
                last_command_ms: AtomicU64::new(0),
                last_reconciliation: std::sync::Mutex::new("Never".into()),
                state_file: None,
                persistence: tokio::sync::Mutex::new(()),
                hub_changed: tokio::sync::watch::channel("127.0.0.1".parse().unwrap()).0,
            }),
            rx,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn expired_motion_is_reconciled_and_old_generations_cannot_cancel_new_moves() {
        let (state, mut rx) = test_state();
        let task = begin_interpolation(
            &state,
            1,
            rail_position(false, 0, None),
            rail_position(false, 100, None),
            0.5,
            0.01,
        );
        let first = task.generation;
        state.motion_tasks.lock().unwrap().insert(1, task);
        tokio::time::advance(Duration::from_secs(5)).await;
        let event = rx.recv().await.unwrap();
        assert!(
            matches!(event, ServerEvent::ReconcileMotion { shade_id: 1, generation } if generation == first)
        );
        let replacement = begin_interpolation(
            &state,
            1,
            rail_position(false, 50, None),
            rail_position(false, 0, None),
            2.0,
            0.04,
        );
        state.motion_tasks.lock().unwrap().insert(1, replacement);
        assert!(!state.cancel_motion_generation(1, first));
        assert!(state.is_in_motion(1));
    }

    #[tokio::test]
    async fn runtime_diagnostics_report_failures_and_connection_state() {
        let (state, _rx) = test_state();
        state.command_failures.store(2, Ordering::SeqCst);
        state.last_command_ms.store(150, Ordering::SeqCst);
        state.mqtt_reconnects.store(3, Ordering::SeqCst);
        let values: HashMap<_, _> = runtime_diagnostics(&state)
            .into_iter()
            .map(|d| (d.unique_id, d.value))
            .collect();
        assert_eq!(values["test-sse"], "disconnected");
        assert_eq!(values["test-command-failures"], "2");
        assert_eq!(values["test-command-latency"], "150");
        assert_eq!(values["test-mqtt-reconnects"], "3");
    }

    #[test]
    fn cover_capabilities_support_tilt_only_reversal_and_distinct_rail_names() {
        use crate::api_types::ShadeCapabilities::*;
        let config = serde_json::json!({"command_topic":"command", "position_topic":"position", "set_position_topic":"set", "state_topic":"state"});
        let tilt = capability_config(config, TiltOnly180, false, "test", 7);
        assert!(tilt.get("position_topic").is_none());
        assert!(tilt.get("command_topic").is_none());
        assert_eq!(tilt["tilt_command_topic"], "pv2mqtt/shade/test/7/tilt/set");
        assert_eq!(rail_name(TopDownBottomUp, true), Some("Top"));
        assert_eq!(rail_name(DualOverlapped, true), Some("Secondary"));
        assert_eq!(coordinate_percent(TopDown, false, 25), 75);
        assert_eq!(coordinate_percent(TopDown, false, 75), 25);
        assert_eq!(coordinate_percent(TopDownBottomUp, true, 25), 25);
    }

    #[test]
    fn discovery_deletes_only_obsolete_topics_and_birth_ignores_offline() {
        let old = HashSet::from(["old".to_string(), "current".to_string()]);
        let new = HashSet::from(["current".to_string()]);
        let legacy = vec!["legacy".to_string(), "current".to_string()];
        assert_eq!(
            obsolete_configs(&old, &new, legacy),
            HashSet::from(["old".to_string(), "legacy".to_string()])
        );
        assert!(is_ha_birth("online"));
        assert!(!is_ha_birth("offline"));
        assert!(!is_ha_birth("unexpected"));
    }

    #[test]
    fn discovery_requires_bridge_hub_and_entity_availability() {
        let value = discovery_payload(r#"{"availability_topic":"pv2mqtt/shade/test/7/availability","device":{"identifiers":["test-7"]}}"#, "test").unwrap();
        let value: serde_json::Value = serde_json::from_str(&value).unwrap();
        assert!(value.get("availability_topic").is_none());
        assert_eq!(value["availability_mode"], "all");
        let topics = value["availability"].as_array().unwrap();
        assert_eq!(topics.len(), 3);
        assert!(topics
            .iter()
            .any(|a| a["topic"] == "pv2mqtt/bridge/test/availability"));
        assert!(topics
            .iter()
            .any(|a| a["topic"] == "pv2mqtt/hub/test/availability"));
    }

    #[test]
    fn snapshot_policy_preserves_live_motion_and_newer_events() {
        assert!(snapshot_is_current(false, 10, 10));
        assert!(!snapshot_is_current(true, 10, 10));
        assert!(!snapshot_is_current(false, 10, 11));
    }

    #[test]
    fn inventory_signature_ignores_telemetry_but_tracks_configuration() {
        let mut value = serde_json::json!({"id": 1, "ptName": "Shade", "positions": {"primary": 0.0}, "batteryStatus": 3, "signalStrength": -50});
        let before = inventory_signature(value.clone());
        value["positions"]["primary"] = serde_json::json!(1.0);
        value["batteryStatus"] = serde_json::json!(1);
        assert_eq!(before, inventory_signature(value.clone()));
        value["ptName"] = serde_json::json!("Renamed");
        assert_ne!(before, inventory_signature(value));
    }

    #[test]
    fn retarget_interpolation_estimates_eta_from_observed_travel_rate() {
        // 0.25s per percent: a 40% move takes 10s, less the finish-early margin
        assert_eq!(plan_retarget_interpolation(0.25, 0, 40), Some(8.5));
        // Only the distance matters, not the direction
        assert_eq!(plan_retarget_interpolation(0.25, 40, 0), Some(8.5));
        // Already there: nothing to animate
        assert_eq!(plan_retarget_interpolation(0.25, 40, 40), None);
        // Short hops keep a floor so the task isn't degenerate
        assert_eq!(plan_retarget_interpolation(0.25, 40, 41), Some(0.5));
        // No usable rate: don't invent one, leave it to the hub's next event
        assert_eq!(plan_retarget_interpolation(f64::NAN, 0, 50), None);
        assert_eq!(plan_retarget_interpolation(0.0, 0, 50), None);
    }

    #[test]
    fn position_command_plan_handles_mid_motion_retarget() {
        use PositionCommandPlan::*;
        // At rest: direction comes from the hub-reported position
        assert_eq!(
            plan_position_command(80, Some(20), false),
            Send(Some("opening"))
        );
        assert_eq!(
            plan_position_command(20, Some(80), false),
            Send(Some("closing"))
        );
        // At rest and already parked there: skipping saves a hub round trip
        assert_eq!(plan_position_command(50, Some(50), false), Skip);
        // Mid-motion the same command must NOT be skipped — the shade is
        // travelling away from that position, so dropping it strands the
        // shade heading for the superseded target. No direction label,
        // since it will simply stop about where it already is.
        assert_eq!(plan_position_command(50, Some(50), true), Send(None));
        // Mid-motion direction comes from the best-known position
        assert_eq!(
            plan_position_command(90, Some(50), true),
            Send(Some("opening"))
        );
        assert_eq!(
            plan_position_command(10, Some(50), true),
            Send(Some("closing"))
        );
        // Position unknown: always send, assume opening
        assert_eq!(
            plan_position_command(50, None, false),
            Send(Some("opening"))
        );
        assert_eq!(plan_position_command(50, None, true), Send(Some("opening")));
    }

    #[test]
    fn motion_label_is_per_rail_and_silent_for_stationary_rails() {
        // A rail that is actually travelling gets its own direction
        assert_eq!(motion_label(Some(20), Some(80)), Some("opening"));
        assert_eq!(motion_label(Some(80), Some(20)), Some("closing"));
        // A TDBU move names both rails in the event, but only one of them
        // is going anywhere. The stationary rail must not be labelled --
        // reporting it as "opening" is what made the bottom rail claim to
        // move whenever the top rail was driven.
        assert_eq!(motion_label(Some(40), Some(40)), None);
        assert_eq!(motion_label(Some(0), Some(0)), None);
        assert_eq!(motion_label(Some(100), Some(100)), None);
        // A rail the hub didn't report can't be labelled either
        assert_eq!(motion_label(None, Some(50)), None);
        assert_eq!(motion_label(Some(50), None), None);
        assert_eq!(motion_label(None, None), None);
    }

    #[test]
    fn rail_addressing_targets_the_commanded_rail() {
        assert_eq!(rail_entity_id(42, false), "42");
        assert_eq!(rail_entity_id(42, true), format!("42{SECONDARY_SUFFIX}"));

        // OPEN/CLOSE on the top rail must move the top rail, not the
        // bottom one, and must leave the other axis unset so the hub
        // holds it where it is.
        let top_open = rail_position(true, 100, None);
        assert_eq!(top_open.secondary, Some(1.0));
        assert_eq!(top_open.primary, None);

        let bottom_close = rail_position(false, 0, Some(0.5));
        assert_eq!(bottom_close.primary, Some(0.0));
        assert_eq!(bottom_close.secondary, None);
        assert_eq!(bottom_close.velocity, Some(0.5));
    }

    #[test]
    fn interpolate_pct_covers_endpoints_midpoints_and_clamping() {
        // Endpoints
        assert_eq!(interpolate_pct(0, 100, 0.0), 0);
        assert_eq!(interpolate_pct(0, 100, 1.0), 100);
        // Midpoint, both directions
        assert_eq!(interpolate_pct(0, 100, 0.5), 50);
        assert_eq!(interpolate_pct(80, 20, 0.5), 50);
        // t beyond the range clamps to the endpoints
        assert_eq!(interpolate_pct(10, 90, 1.5), 90);
        assert_eq!(interpolate_pct(10, 90, -0.5), 10);
        // No movement stays put
        assert_eq!(interpolate_pct(42, 42, 0.7), 42);
    }

    #[test]
    fn sanitize_eta_clamps_hostile_and_degenerate_values() {
        // Normal values pass through
        assert_eq!(sanitize_eta_secs(3.5), 3.5);
        // Values that would panic Duration::from_secs_f64
        assert_eq!(sanitize_eta_secs(-5.0), 0.25);
        assert_eq!(sanitize_eta_secs(f64::NAN), 0.25);
        // Degenerate / absurd values are clamped
        assert_eq!(sanitize_eta_secs(0.0), 0.25);
        assert_eq!(sanitize_eta_secs(f64::INFINITY), 600.0);
        assert_eq!(sanitize_eta_secs(1e12), 600.0);
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_restarts_sessions_with_backoff_and_reset() {
        const BASE: Duration = Duration::from_secs(1);
        const MAX: Duration = Duration::from_secs(8);
        const HEALTHY: Duration = Duration::from_secs(30);

        let starts: Arc<StdMutex<Vec<tokio::time::Instant>>> = Arc::new(StdMutex::new(Vec::new()));
        let attempt = Arc::new(AtomicUsize::new(0));

        {
            let starts = starts.clone();
            tokio::spawn(supervise_sessions("test", BASE, MAX, HEALTHY, move || {
                let starts = starts.clone();
                let attempt = attempt.clone();
                async move {
                    starts.lock().unwrap().push(tokio::time::Instant::now());
                    match attempt.fetch_add(1, Ordering::SeqCst) {
                        0..=2 => anyhow::bail!("session died immediately"),
                        3 => {
                            // Healthy session: outlives HEALTHY, then dies
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            anyhow::bail!("session died after healthy run")
                        }
                        _ => {
                            // Park forever so the timeline ends here
                            std::future::pending::<()>().await;
                            Ok(())
                        }
                    }
                }
            }));
        }

        // Paused-time runtime: this fast-forwards through the whole timeline
        tokio::time::sleep(Duration::from_secs(120)).await;

        let starts = starts.lock().unwrap();
        assert_eq!(starts.len(), 5, "expected 5 session attempts");
        let gaps: Vec<Duration> = starts.windows(2).map(|w| w[1] - w[0]).collect();
        assert_eq!(
            gaps[0],
            Duration::from_secs(1),
            "first restart after base delay"
        );
        assert_eq!(gaps[1], Duration::from_secs(2), "backoff doubles");
        assert_eq!(gaps[2], Duration::from_secs(4), "backoff doubles again");
        // Session 4 ran for 60s (> HEALTHY) so the backoff resets to base:
        // 60s session + 1s base delay
        assert_eq!(
            gaps[3],
            Duration::from_secs(61),
            "healthy session resets backoff"
        );
    }
}
