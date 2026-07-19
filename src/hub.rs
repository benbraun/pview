use crate::api_types::*;
use crate::discovery::resolve_hub;
use crate::http_helpers::{get_request_with_json_response, request_with_json_response};
use anyhow::Context;
use async_stream::try_stream;
use futures_util::{Stream, StreamExt};
use reqwest::Method;
use serde_json::json;
use std::net::IpAddr;
use std::time::Duration;

/// Ceiling for buffered, not-yet-delimited SSE data. A well-behaved hub
/// sends small events; hitting this means the peer is streaming garbage,
/// and we drop the buffer rather than grow without bound.
const MAX_SSE_BUFFER: usize = 256 * 1024;

/// Incremental parser for an SSE byte stream: push chunks as they arrive,
/// get back the payload of each complete `data:` line. Buffers bytes (not
/// text) so that a UTF-8 character split across chunks survives intact.
struct SseDataParser {
    buffer: Vec<u8>,
}

impl SseDataParser {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
            haystack.windows(needle.len()).position(|w| w == needle)
        }

        self.buffer.extend_from_slice(chunk);
        let mut payloads = vec![];
        loop {
            // SSE events are separated by blank lines (\r\n\r\n or \n\n);
            // take whichever delimiter comes first.
            let crlf = find(&self.buffer, b"\r\n\r\n").map(|pos| (pos, 4));
            let lf = find(&self.buffer, b"\n\n").map(|pos| (pos, 2));
            let delim = match (crlf, lf) {
                (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
                (a, b) => a.or(b),
            };
            let Some((pos, delim_len)) = delim else { break };
            let event_text = String::from_utf8_lossy(&self.buffer[..pos]).into_owned();
            self.buffer.drain(..pos + delim_len);

            for line in event_text.lines() {
                let line = line.trim_end_matches('\r');
                if let Some(data) = line.strip_prefix("data:") {
                    payloads.push(data.trim().to_string());
                }
            }
        }
        if self.buffer.len() > MAX_SSE_BUFFER {
            log::warn!(
                "SSE: discarding {} bytes of unterminated event data",
                self.buffer.len()
            );
            self.buffer.clear();
        }
        payloads
    }
}

#[derive(Debug, Clone)]
pub struct Hub {
    addr: IpAddr,
}

impl Hub {
    fn url(&self, extra: &str) -> String {
        format!("http://{}/{extra}", self.addr)
    }

    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    pub async fn list_rooms(&self) -> anyhow::Result<Vec<RoomData>> {
        let mut rooms: Vec<RoomData> =
            get_request_with_json_response(self.url("home/rooms")).await?;
        rooms.sort_by(|a, b| a.pt_name.cmp(&b.pt_name));
        Ok(rooms)
    }

    pub async fn list_scenes(&self) -> anyhow::Result<Vec<Scene>> {
        let mut scenes: Vec<Scene> =
            get_request_with_json_response(self.url("home/scenes")).await?;
        scenes.sort_by(|a, b| a.pt_name.cmp(&b.pt_name));
        Ok(scenes)
    }

    /// Fetches all shades; filters by room_id client-side if provided
    /// (v3 GET /home/shades has no server-side room parameter).
    pub async fn list_shades(&self, room_id: Option<i32>) -> anyhow::Result<Vec<ShadeData>> {
        let mut shades: Vec<ShadeData> =
            get_request_with_json_response(self.url("home/shades")).await?;
        if let Some(id) = room_id {
            shades.retain(|s| s.room_id == id);
        }
        shades.sort_by(|a, b| a.pt_name.cmp(&b.pt_name));
        Ok(shades)
    }

    pub fn with_addr(addr: IpAddr) -> Self {
        Self { addr }
    }

    pub async fn discover(timeout: Duration) -> anyhow::Result<Self> {
        let addr = resolve_hub(timeout).await.context(
            "Failed to discover the PowerView Hub. \
             Ensure that pview is running on the same network as the Hub!",
        )?;
        Ok(Self::with_addr(addr))
    }

    pub async fn room_by_name(&self, name: &str) -> anyhow::Result<RoomData> {
        let rooms = self.list_rooms().await?;
        for room in rooms {
            if room.pt_name.eq_ignore_ascii_case(name) || room.id.to_string() == name {
                return Ok(room);
            }
        }
        anyhow::bail!("No room with name or id matching '{name}' was found");
    }

    pub async fn scene_by_name(&self, name: &str) -> anyhow::Result<Scene> {
        let scenes = self.list_scenes().await?;
        for s in scenes {
            if s.pt_name.eq_ignore_ascii_case(name) || s.id.to_string() == name {
                return Ok(s);
            }
        }
        anyhow::bail!("No scene with name or id matching '{name}' was found");
    }

    pub async fn shade_by_id(&self, shade_id: i32) -> anyhow::Result<ShadeData> {
        get_request_with_json_response(self.url(&format!("home/shades/{shade_id}"))).await
    }

    /// Look up a shade by name or id.
    ///
    /// Accepts:
    /// - A numeric id
    /// - `"Shade Name"` — matches if exactly one shade has that name; errors with
    ///   suggestions if multiple shades share the name
    /// - `"Room Name/Shade Name"` — unambiguous room-qualified lookup
    pub async fn shade_by_name(&self, name: &str) -> anyhow::Result<ShadeData> {
        let (shades, rooms) = tokio::try_join!(self.list_shades(None), self.list_rooms())?;
        let room_name_by_id: std::collections::HashMap<i32, &str> =
            rooms.iter().map(|r| (r.id, r.pt_name.as_str())).collect();

        // Numeric id shortcut
        if let Ok(id) = name.parse::<i32>() {
            if let Some(shade) = shades.into_iter().find(|s| s.id == id) {
                return Ok(shade);
            }
            anyhow::bail!("No shade with id {id} was found");
        }

        // Room-qualified lookup: try every space split position to find a
        // "Room Name Shade Name" combination that matches a real room+shade pair.
        let words: Vec<&str> = name.split_whitespace().collect();
        if words.len() > 1 {
            let mut qualified_matches: Vec<ShadeData> = vec![];
            for split in 1..words.len() {
                let room_part = words[..split].join(" ");
                let shade_part = words[split..].join(" ");
                for shade in &shades {
                    if shade.pt_name.eq_ignore_ascii_case(&shade_part)
                        && room_name_by_id
                            .get(&shade.room_id)
                            .map(|r| r.eq_ignore_ascii_case(&room_part))
                            .unwrap_or(false)
                    {
                        qualified_matches.push(shade.clone());
                    }
                }
            }
            if !qualified_matches.is_empty() {
                return match qualified_matches.len() {
                    1 => Ok(qualified_matches.remove(0)),
                    _ => {
                        anyhow::bail!("Multiple shades match '{name}'; use the numeric id instead")
                    }
                };
            }
        }

        // Bare name lookup — error if ambiguous
        let matched: Vec<ShadeData> = shades
            .into_iter()
            .filter(|s| s.pt_name.eq_ignore_ascii_case(name))
            .collect();
        match matched.len() {
            1 => Ok(matched.into_iter().next().unwrap()),
            0 => anyhow::bail!("No shade with name or id matching '{name}' was found"),
            _ => {
                let suggestions: Vec<String> = matched
                    .iter()
                    .map(|s| {
                        let room = room_name_by_id
                            .get(&s.room_id)
                            .copied()
                            .unwrap_or("unknown");
                        format!("  {room} {name}")
                    })
                    .collect();
                anyhow::bail!(
                    "Multiple shades named '{name}' found. Use room-qualified name:\n{}",
                    suggestions.join("\n")
                )
            }
        }
    }

    /// Sends a motion command to the hub.
    /// Up/Down/LeftTilt/RightTilt map to `PUT home/shades/{id}/positions`.
    /// Jog maps to `PUT home/shades/{id}/motion {"motion":"jog"}`.
    /// Stop maps to `PUT home/shades/stop?ids={id}` with empty body.
    pub async fn move_shade(&self, shade_id: i32, motion: ShadeUpdateMotion) -> anyhow::Result<()> {
        match motion {
            ShadeUpdateMotion::Up => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition {
                        primary: Some(1.0),
                        ..Default::default()
                    },
                )
                .await
            }
            ShadeUpdateMotion::Down => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition {
                        primary: Some(0.0),
                        ..Default::default()
                    },
                )
                .await
            }
            ShadeUpdateMotion::LeftTilt => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition {
                        tilt: Some(0.0),
                        ..Default::default()
                    },
                )
                .await
            }
            ShadeUpdateMotion::RightTilt => {
                self.set_shade_position(
                    shade_id,
                    ShadePosition {
                        tilt: Some(1.0),
                        ..Default::default()
                    },
                )
                .await
            }
            ShadeUpdateMotion::Jog => {
                let url = self.url(&format!("home/shades/{shade_id}/motion"));
                let _: serde_json::Value =
                    request_with_json_response(Method::PUT, url, &json!({"motion": "jog"})).await?;
                Ok(())
            }
            ShadeUpdateMotion::Stop => {
                let url = self.url(&format!("home/shades/stop?ids={shade_id}"));
                let _: serde_json::Value =
                    request_with_json_response(Method::PUT, url, &json!({})).await?;
                Ok(())
            }
        }
    }

    /// Sets shade position via `PUT home/shades/{id}/positions`.
    /// Only `Some` fields are serialized; the hub preserves axes not included in the body.
    pub async fn set_shade_position(
        &self,
        shade_id: i32,
        position: ShadePosition,
    ) -> anyhow::Result<()> {
        let url = self.url(&format!("home/shades/{shade_id}/positions"));
        let _: serde_json::Value =
            request_with_json_response(Method::PUT, url, &json!({ "positions": position })).await?;
        Ok(())
    }

    /// Activates a scene. Returns the list of affected shade ids.
    pub async fn activate_scene(&self, scene_id: i32) -> anyhow::Result<Vec<i32>> {
        #[derive(serde::Deserialize)]
        struct ActivateResponse {
            #[serde(rename = "shadeIds")]
            shade_ids: Vec<i32>,
        }
        let url = self.url(&format!("home/scenes/{scene_id}/activate"));
        let resp: ActivateResponse =
            request_with_json_response(Method::PUT, url, &json!({})).await?;
        Ok(resp.shade_ids)
    }

    pub async fn get_gateway_data(&self) -> anyhow::Result<GatewayConfig> {
        let resp: GatewayResponse = get_request_with_json_response(self.url("gateway")).await?;
        Ok(resp.config)
    }

    /// Opens the SSE shade events stream at `GET home/shades/events?sse=true`.
    /// Uses a dedicated reqwest client with no overall timeout (long-lived
    /// connection); TCP keepalive detects dead peers instead.
    /// SSE events delimited by blank lines; only `data:` lines are parsed.
    /// `Unknown` events are logged at debug level and not yielded.
    pub async fn shade_events_stream(
        &self,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<ShadeEvent>>> {
        // No overall timeout: this is a long-lived streaming connection.
        // TCP keepalive is what detects a half-open connection (hub reboot
        // or network drop without a FIN): the probes fail and the stream
        // errors out, letting the caller reconnect. Without it, reading
        // from a dead peer blocks forever.
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_keepalive_interval(Duration::from_secs(15))
            .tcp_keepalive_retries(4)
            .build()?;
        let url = format!("http://{}/home/shades/events?sse=true", self.addr);
        let response = client.get(&url).send().await?;
        let byte_stream = response.bytes_stream();

        let stream = try_stream! {
            let mut parser = SseDataParser::new();
            tokio::pin!(byte_stream);
            while let Some(chunk) = byte_stream.next().await {
                let chunk = chunk?;
                for json_str in parser.push(&chunk) {
                    match serde_json::from_str::<ShadeEvent>(&json_str) {
                        Ok(event) if event.evt != ShadeEventKind::Unknown => {
                            yield event;
                        }
                        Ok(_) => {
                            log::debug!("SSE: unknown event kind in: {json_str}");
                        }
                        Err(e) => {
                            log::warn!("SSE: failed to parse event: {e:#} — {json_str}");
                        }
                    }
                }
            }
        };

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_parser_yields_data_and_preserves_utf8_split_across_chunks() {
        let mut parser = SseDataParser::new();
        // "café" with the 2-byte é (0xC3 0xA9) split across chunks
        assert_eq!(parser.push(b"data: {\"name\":\"caf"), Vec::<String>::new());
        assert_eq!(parser.push(&[0xC3]), Vec::<String>::new());
        assert_eq!(
            parser.push(&[0xA9, b'"', b'}', b'\n', b'\n']),
            vec![r#"{"name":"café"}"#.to_string()]
        );
    }

    #[test]
    fn sse_parser_handles_crlf_delimiters_and_multiple_events() {
        let mut parser = SseDataParser::new();
        let payloads = parser.push(b"data: one\r\n\r\ndata: two\n\ndata: partial");
        assert_eq!(payloads, vec!["one".to_string(), "two".to_string()]);
        // the partial event completes on a later push
        assert_eq!(parser.push(b"\n\n"), vec!["partial".to_string()]);
    }

    #[test]
    fn sse_parser_bounds_buffer_growth() {
        let mut parser = SseDataParser::new();
        // A hostile/broken peer streams data with no event delimiter
        for _ in 0..600 {
            let payloads = parser.push(&[b'x'; 1024]);
            assert_eq!(payloads, Vec::<String>::new());
            assert!(
                parser.buffer.len() <= MAX_SSE_BUFFER,
                "buffer grew to {} bytes",
                parser.buffer.len()
            );
        }
        // The next delimiter terminates the garbage "event" (which yields
        // nothing), and parsing resyncs: a well-formed event parses again.
        assert_eq!(parser.push(b"\n\n"), Vec::<String>::new());
        assert_eq!(parser.push(b"data: ok\n\n"), vec!["ok".to_string()]);
    }
}
