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
