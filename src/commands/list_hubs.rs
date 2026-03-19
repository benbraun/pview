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
