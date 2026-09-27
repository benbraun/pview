//! Per-hub settings and discovery manifest. Writes replace the file atomically.
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

pub fn effective_velocity(value: f64) -> anyhow::Result<Option<f64>> {
    anyhow::ensure!(
        value.is_finite() && (0.0..=100.0).contains(&value),
        "Velocity must be a finite number in 0..=100"
    );
    Ok(if value == 0.0 {
        None
    } else {
        Some(value.max(7.0) / 100.0)
    })
}

#[derive(Serialize, Deserialize, Default)]
pub struct StoredSettings {
    pub serial: String,
    #[serde(default)]
    pub velocities: HashMap<i32, f64>,
    #[serde(default)]
    pub discovery_topics: HashSet<String>,
}

impl StoredSettings {
    pub fn load(path: &Path, serial: &str) -> anyhow::Result<Self> {
        let data = match std::fs::read(path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    serial: serial.into(),
                    ..Default::default()
                })
            }
            Err(err) => return Err(err).with_context(|| format!("Reading {}", path.display())),
        };
        let state: Self = serde_json::from_slice(&data)
            .with_context(|| format!("Invalid settings in {}", path.display()))?;
        anyhow::ensure!(
            state.serial == serial,
            "Settings file belongs to a different hub"
        );
        for value in state.velocities.values() {
            anyhow::ensure!(
                value.is_finite() && (0.07..=1.0).contains(value),
                "Invalid saved velocity"
            );
        }
        Ok(state)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        use std::io::Write;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension(format!(
            "tmp-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let result = (|| -> anyhow::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&serde_json::to_vec_pretty(self)?)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result.with_context(|| format!("Saving {}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn velocity_validates_and_reports_the_effective_value() {
        assert_eq!(effective_velocity(0.0).unwrap(), None);
        assert_eq!(effective_velocity(1.0).unwrap(), Some(0.07));
        assert_eq!(effective_velocity(100.0).unwrap(), Some(1.0));
        for value in [-1.0, 101.0, f64::NAN, f64::INFINITY] {
            assert!(effective_velocity(value).is_err());
        }
    }
    #[test]
    fn settings_round_trip_and_reject_wrong_hub_or_corruption() {
        let dir = std::env::temp_dir().join(format!("pview-settings-{}", std::process::id()));
        let path = dir.join("state.json");
        let mut state = StoredSettings::load(&path, "hub").unwrap();
        state.velocities.insert(7, 0.07);
        state
            .discovery_topics
            .insert("homeassistant/cover/hub-7/config".into());
        state.save(&path).unwrap();
        assert_eq!(
            StoredSettings::load(&path, "hub").unwrap().velocities[&7],
            0.07
        );
        assert!(StoredSettings::load(&path, "other-hub").is_err());
        std::fs::write(&path, b"broken").unwrap();
        assert!(StoredSettings::load(&path, "hub").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
