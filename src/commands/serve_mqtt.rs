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
use tokio::sync::mpsc::Receiver;

const SECONDARY_SUFFIX: &str = "_middle";
const MODEL: &str = "pv2mqtt";
const WEZ: &str = "Wez Furlong";
const HUNTER_DOUGLAS: &str = "Hunter Douglas";
const BATTERY_LABEL: &str = "Battery";
const RECHARGEABLE_LABEL: &str = "Rechargeable Battery";
const HARD_WIRED_LABEL: &str = "Hard Wired";

// <https://www.home-assistant.io/integrations/cover.mqtt/>

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

enum ServerEvent {
    MqttMessage {
        router: Arc<MqttRouter<Arc<Pv2MqttState>>>,
        msg: Message,
    },
    ShadeEvent(ShadeEvent),
    PeriodicStateUpdate,
    HubDiscovered(ResolvedHub),
}

#[derive(Debug)]
enum RegEntry {
    Delay(Duration),
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
        if self.deletes.is_empty() {
            self.deletes.push(RegEntry::Delay(Duration::from_secs(4)));
        }
        self.deletes.push(RegEntry::msg(topic, ""));
    }

    pub fn config<T: Into<String>, P: Into<String>>(&mut self, topic: T, payload: P) {
        self.configs.push(RegEntry::msg(topic, payload));
    }

    pub fn update<T: Into<String>, P: Into<String>>(&mut self, topic: T, payload: P) {
        self.updates.push(RegEntry::msg(topic, payload));
    }

    pub async fn apply_updates(mut self, state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
        let is_first_run = state.first_run.load(Ordering::SeqCst);

        if is_first_run {
            if !self.configs.is_empty() && !self.updates.is_empty() {
                // Delay between registering configs and advising hass
                // of the states, so that hass has had enough time
                // to subscribe to the correct topics
                let delay = self.configs.len() as u64 * 30;
                log::info!(
                    "there are {} configs, and {} updates. delay ms = {delay}",
                    self.configs.len(),
                    self.updates.len()
                );
                self.updates
                    .insert(0, RegEntry::Delay(Duration::from_millis(delay)));
            }
        } else {
            self.deletes.clear();
        }
        for queue in [self.deletes, self.configs, self.updates] {
            for entry in queue {
                match entry {
                    RegEntry::Delay(duration) => {
                        tokio::time::sleep(duration).await;
                    }
                    RegEntry::Msg { topic, payload } => {
                        state
                            .client
                            .publish(&topic, payload.as_bytes(), QoS::AtMostOnce, false)
                            .await?;
                    }
                }
            }
        }
        state.first_run.store(false, Ordering::SeqCst);
        Ok(())
    }
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
        let shade_display_name = match &area {
            Some(room) => format!("{room} {}", shade.pt_name),
            None => shade.pt_name.clone(),
        };
        let device = Device {
            suggested_area: area,
            identifiers: vec![device_id.clone()],
            via_device: Some(format!("{MODEL}-{serial}")),
            name: shade_display_name,
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

async fn advise_hass_of_unresponsive(state: &Arc<Pv2MqttState>) -> anyhow::Result<()> {
    log::info!("Marking hub status as unresponsive");
    state.responding.store(false, Ordering::SeqCst);
    state
        .client
        .publish(
            format!("{MODEL}/sensor/{}-responding/state", state.serial),
            "UNRESPONSIVE",
            QoS::AtMostOnce,
            false,
        )
        .await?;
    Ok(())
}

async fn advise_hass_of_state_label(
    state: &Arc<Pv2MqttState>,
    shade_id: &str,
    shade_state: &str,
) -> anyhow::Result<()> {
    state
        .client
        .publish(
            &format!(
                "{MODEL}/shade/{serial}/{shade_id}/state",
                serial = state.serial
            ),
            &shade_state.as_bytes(),
            QoS::AtMostOnce,
            false,
        )
        .await?;
    Ok(())
}

async fn advise_hass_of_position(
    state: &Arc<Pv2MqttState>,
    shade_id: &str,
    position: u8,
) -> anyhow::Result<()> {
    state
        .client
        .publish(
            &format!(
                "{MODEL}/shade/{serial}/{shade_id}/position",
                serial = state.serial
            ),
            &format!("{position}").as_bytes(),
            QoS::AtMostOnce,
            false,
        )
        .await?;

    Ok(())
}

/// Spawns a task that publishes interpolated shade position every 250ms
/// until `eta_secs` elapses. The returned `AbortHandle` cancels it early.
fn spawn_position_interpolation(
    state: Arc<Pv2MqttState>,
    shade_id: i32,
    start: ShadePosition,
    target: ShadePosition,
    eta_secs: f64,
) -> tokio::task::AbortHandle {
    let handle = tokio::spawn(async move {
        let start_time = tokio::time::Instant::now();
        let eta = Duration::from_secs_f64(eta_secs);
        let shade_id_str = format!("{shade_id}");
        let sec_id = format!("{shade_id}{SECONDARY_SUFFIX}");
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let elapsed = start_time.elapsed();
            let t = (elapsed.as_secs_f64() / eta_secs).min(1.0);
            if let (Some(s), Some(tgt)) = (start.pos1_percent(), target.pos1_percent()) {
                let pct = (s as f64 + (tgt as f64 - s as f64) * t).round() as u8;
                let _ = advise_hass_of_position(&state, &shade_id_str, pct).await;
            }
            if let (Some(s), Some(tgt)) = (start.pos2_percent(), target.pos2_percent()) {
                let pct = (s as f64 + (tgt as f64 - s as f64) * t).round() as u8;
                let _ = advise_hass_of_position(&state, &sec_id, pct).await;
            }
            if elapsed >= eta {
                break;
            }
        }
    });
    handle.abort_handle()
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
    Ok(())
}


async fn advise_hass_of_battery_level(
    state: &Arc<Pv2MqttState>,
    shade: &ShadeData,
) -> anyhow::Result<()> {
    let availability_topic = state.battery_availability_topic(shade);
    let state_topic = state.battery_state_topic(shade);

    if let Some(pct) = shade.battery_percent() {
        state
            .client
            .publish(state_topic, format!("{pct}"), QoS::AtMostOnce, false)
            .await?;
        state
            .client
            .publish(availability_topic, "online", QoS::AtMostOnce, false)
            .await?;
    } else {
        state
            .client
            .publish(availability_topic, "offline", QoS::AtMostOnce, false)
            .await?;
    }

    Ok(())
}

impl ServeMqttCommand {
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
            motion_tasks: std::sync::Mutex::new(HashMap::new()),
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
        let hub = state.hub.load();
        match event.evt {
            ShadeEventKind::MotionStopped => {
                // Cancel any in-progress interpolation for this shade
                if let Some(handle) = state.motion_tasks.lock().unwrap().remove(&event.id) {
                    handle.abort();
                }
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
                let shade_id_str = format!("{}", event.id);
                // Determine opening vs closing from target vs current positions
                let motion_state = match (&event.current_positions, &event.target_positions) {
                    (Some(cur), Some(tgt)) => {
                        let cur_pct = cur.pos1_percent().unwrap_or(0);
                        let tgt_pct = tgt.pos1_percent().unwrap_or(0);
                        if tgt_pct >= cur_pct { "opening" } else { "closing" }
                    }
                    _ => "opening",
                };
                advise_hass_of_state_label(state, &shade_id_str, motion_state).await?;
                // Cancel any previous interpolation task for this shade
                if let Some(handle) = state.motion_tasks.lock().unwrap().remove(&event.id) {
                    handle.abort();
                }
                // Spawn position interpolation if we have enough data
                if let (Some(current), Some(target)) =
                    (event.current_positions, event.target_positions)
                {
                    if let Some(eta) = target.eta_in_seconds {
                        let eta = (eta - 1.5).max(0.5);
                        let abort = spawn_position_interpolation(
                            Arc::clone(state),
                            event.id,
                            current,
                            target,
                            eta,
                        );
                        state.motion_tasks.lock().unwrap().insert(event.id, abort);
                    }
                }
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
                // Also mark secondary rail offline if present
                state
                    .client
                    .publish(
                        format!(
                            "{MODEL}/shade/{serial}/{}{SECONDARY_SUFFIX}/availability",
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
    let shade_id_str = if is_secondary {
        format!("{shade_id}{SECONDARY_SUFFIX}")
    } else {
        format!("{shade_id}")
    };
    let current = if is_secondary { shade.pos2_percent() } else { shade.pos1_percent() };
    let motion_state = match current {
        Some(cur) if position > cur => "opening",
        Some(cur) if position < cur => "closing",
        _ => "opening", // unknown current position; assume opening
    };
    advise_hass_of_state_label(&state, &shade_id_str, motion_state).await?;
    hub.hub.set_shade_position(shade_id, pos).await?;
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
    let shade_id_str = format!("{shade_id}");
    match command.as_ref() {
        "OPEN" => {
            advise_hass_of_state_label(&state, &shade_id_str, "opening").await?;
            hub.hub.move_shade(shade_id, ShadeUpdateMotion::Up).await?;
        }
        "CLOSE" => {
            advise_hass_of_state_label(&state, &shade_id_str, "closing").await?;
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

async fn mqtt_homeassitant_status(
    Payload(status): Payload<String>,
    State(state): State<Arc<Pv2MqttState>>,
) -> anyhow::Result<()> {
    log::info!("Home Assistant status changed: {status}",);
    // Make apply_updates be more thorough
    state.first_run.store(true, Ordering::SeqCst);
    register_with_hass(&state).await
}

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
    motion_tasks: std::sync::Mutex<HashMap<i32, tokio::task::AbortHandle>>,
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
