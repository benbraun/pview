use crate::api_types::{ShadePosition, ShadeUpdateMotion};

#[derive(clap::Args, Debug)]
#[group(required = true)]
struct TargetPosition {
    #[arg(long, conflicts_with = "percent,secondary_percent")]
    motion: Option<ShadeUpdateMotion>,
    /// Set the primary rail position (0–100)
    #[arg(long, group = "position")]
    percent: Option<u8>,
    /// Set the secondary (middle) rail position (0–100)
    #[arg(long, group = "position")]
    secondary_percent: Option<u8>,
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
        } else if let Some(percent) = self.target_position.secondary_percent {
            let pos = ShadePosition {
                secondary: Some(ShadePosition::percent_to_pos(percent)),
                ..Default::default()
            };
            hub.set_shade_position(shade.id, pos).await?;
            println!("Set {} (id={}) secondary to {percent}%", shade.pt_name, shade.id);
        } else {
            anyhow::bail!("One of --motion, --percent, or --secondary-percent is required");
        }
        Ok(())
    }
}
