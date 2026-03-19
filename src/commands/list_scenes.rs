use std::collections::HashMap;
use tabout::{Alignment, Column};

/// List scenes and their associated rooms
#[derive(clap::Parser, Debug)]
pub struct ListScenesCommand {
    /// Only return scenes in the specified room
    #[clap(long)]
    room: Option<String>,
}

impl ListScenesCommand {
    pub async fn run(&self, args: &crate::Args) -> anyhow::Result<()> {
        let hub = args.hub().await?;
        let mut scenes = hub.list_scenes().await?;

        if let Some(room) = &self.room {
            let room = hub.room_by_name(room).await?;
            scenes.retain(|scene| scene.room_ids.contains(&room.id));
        }

        let room_by_id: HashMap<i32, String> = hub
            .list_rooms()
            .await?
            .into_iter()
            .map(|r| (r.id, r.pt_name))
            .collect();

        let columns = &[
            Column { name: "SCENE".to_string(), alignment: Alignment::Left },
            Column { name: "ROOMS".to_string(), alignment: Alignment::Left },
        ];
        let mut rows = vec![];
        for scene in &scenes {
            let room_names: Vec<&str> = scene
                .room_ids
                .iter()
                .filter_map(|id| room_by_id.get(id).map(|s| s.as_str()))
                .collect();
            rows.push(vec![scene.pt_name.clone(), room_names.join(", ")]);
        }
        println!("{}", tabout::tabulate_output_as_string(columns, &rows)?);
        Ok(())
    }
}
