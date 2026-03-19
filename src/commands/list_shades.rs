use crate::api_types::ShadeCapabilityFlags;
use std::collections::BTreeMap;
use tabout::{Alignment, Column};

/// List shades and their current positions
#[derive(clap::Parser, Debug)]
pub struct ListShadesCommand {
    /// Only return shades in the specified room
    #[clap(long)]
    room: Option<String>,
}

impl ListShadesCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let hub = args.hub().await?;

        let opt_room_id = match &self.room {
            Some(name) => Some(hub.room_by_name(name).await?.id),
            None => None,
        };

        let rooms = hub.list_rooms().await?;
        let shades = hub.list_shades(opt_room_id).await?;

        let mut shades_by_room: BTreeMap<i32, Vec<_>> = BTreeMap::new();
        for shade in shades {
            shades_by_room.entry(shade.room_id).or_default().push(shade);
        }

        let columns = &[
            Column { name: "ROOM".to_string(), alignment: Alignment::Left },
            Column { name: "SHADE".to_string(), alignment: Alignment::Left },
            Column { name: "POSITION".to_string(), alignment: Alignment::Right },
        ];
        let mut rows = vec![];
        for room_data in &rooms {
            if let Some(shades) = shades_by_room.get(&room_data.id) {
                for shade in shades {
                    rows.push(vec![
                        room_data.pt_name.clone(),
                        shade.pt_name.clone(),
                        shade.positions.describe_pos1(),
                    ]);
                    if shade
                        .capabilities
                        .flags()
                        .contains(ShadeCapabilityFlags::SECONDARY_RAIL)
                    {
                        rows.push(vec![
                            room_data.pt_name.clone(),
                            format!("{} Middle Rail", shade.pt_name),
                            shade.positions.describe_pos2(),
                        ]);
                    }
                }
            }
        }
        println!("{}", tabout::tabulate_output_as_string(columns, &rows)?);
        Ok(())
    }
}
