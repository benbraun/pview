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
            println!(
                "Battery:       {:?} ({}%)",
                bat,
                shade.battery_percent().unwrap_or(0)
            );
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
        println!(
            "Firmware:      {}.{}.{}",
            fw.revision, fw.sub_revision, fw.build
        );
        println!("Positions:     {}", shade.positions.describe());
        Ok(())
    }
}
