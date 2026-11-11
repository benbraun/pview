//! A thin MQTT layer over rumqttc.
//!
//! [`Client`] is a cloneable publishing handle that fails fast while the
//! broker connection is down, instead of queueing work behind an event loop
//! that is busy reconnecting. [`MqttRouter`] maps topic patterns such as
//! `pv2mqtt/shade/:serial/:shade_id/command` to handlers whose arguments are
//! extracted from the message ([`Topic`], [`Payload`], [`Params`], [`State`]).
//! The router follows the design of the one in Wez Furlong's mosquitto-rs
//! (MIT), which this module replaces.

use anyhow::Context;
use rumqttc::AsyncClient;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub use rumqttc::QoS;

/// How long a publish may wait for room in the event loop's request queue
/// while connected. Only a stalled event loop gets anywhere near this.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub topic: String,
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub struct Client {
    inner: AsyncClient,
    connected: Arc<AtomicBool>,
}

impl Client {
    /// Wraps the handle of a connected event loop.
    pub fn new(inner: AsyncClient) -> Self {
        Self {
            inner,
            connected: Arc::new(AtomicBool::new(true)),
        }
    }

    /// A client whose event loop is gone: every publish fails.
    #[cfg(test)]
    pub fn offline() -> Self {
        let (inner, _eventloop) =
            AsyncClient::new(rumqttc::MqttOptions::new("offline", "localhost", 1883), 1);
        Self {
            inner,
            connected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::SeqCst);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// Queues a publish. Fails immediately while disconnected, like
    /// libmosquitto's `MOSQ_ERR_NO_CONN`, so callers keep treating the topic
    /// as unpublished and send it again after the reconnect re-registration.
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        qos: QoS,
        retain: bool,
    ) -> anyhow::Result<()> {
        let topic = topic.into();
        anyhow::ensure!(
            self.is_connected(),
            "MQTT is not connected; not publishing {topic}"
        );
        tokio::time::timeout(
            PUBLISH_TIMEOUT,
            self.inner.publish(topic.clone(), qos, retain, payload),
        )
        .await
        .with_context(|| format!("timed out queueing MQTT publish to {topic}"))?
        .with_context(|| format!("publishing to {topic}"))
    }

    /// Queues a subscription without waiting, so the task that polls the
    /// event loop can call it without deadlocking on a full queue.
    pub fn try_subscribe(&self, topic: &str, qos: QoS) -> anyhow::Result<()> {
        self.inner
            .try_subscribe(topic, qos)
            .with_context(|| format!("subscribing to {topic}"))
    }
}

pub struct Request<S> {
    params: serde_json::Value,
    message: Message,
    state: S,
}

/// Extracts a handler argument from an incoming message.
pub trait FromRequest<S>: Sized {
    fn from_request(request: &Request<S>) -> anyhow::Result<Self>;
}

/// The message topic.
pub struct Topic(pub String);
impl<S> FromRequest<S> for Topic {
    fn from_request(request: &Request<S>) -> anyhow::Result<Self> {
        Ok(Self(request.message.topic.clone()))
    }
}

/// The UTF-8 payload, parsed with `FromStr`.
pub struct Payload<T>(pub T);
impl<S, T> FromRequest<S> for Payload<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    fn from_request(request: &Request<S>) -> anyhow::Result<Self> {
        let text = std::str::from_utf8(&request.message.payload)
            .context("payload is not UTF-8, cannot parse it")?;
        let value = text
            .parse()
            .map_err(|err| anyhow::anyhow!("failed to parse payload {text:?}: {err}"))?;
        Ok(Self(value))
    }
}

/// The `:name` segments of the matched route, deserialized into `T`.
pub struct Params<T>(pub T);
impl<S, T: DeserializeOwned> FromRequest<S> for Params<T> {
    fn from_request(request: &Request<S>) -> anyhow::Result<Self> {
        Ok(Self(
            serde_json::from_value(request.params.clone()).context("parsing topic parameters")?,
        ))
    }
}

/// The router's shared state.
pub struct State<S>(pub S);
impl<S: Clone> FromRequest<S> for State<S> {
    fn from_request(request: &Request<S>) -> anyhow::Result<Self> {
        Ok(Self(request.state.clone()))
    }
}

/// Deserializes a string through `FromStr`, for `#[serde(deserialize_with)]`
/// on [`Params`] fields.
pub fn parse_deser<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: FromStr,
    T::Err: std::fmt::Display,
{
    use serde::de::Error;
    use serde::Deserialize;
    let s = String::deserialize(d)?;
    s.parse::<T>()
        .map_err(|err| D::Error::custom(format!("parsing {s}: {err:#}")))
}

type HandlerFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type Handler<S> = Box<dyn Fn(Request<S>) -> HandlerFuture + Send + Sync>;

/// Adapts an async function whose arguments all implement [`FromRequest`]
/// into a stored handler.
pub trait MakeHandler<Args, S> {
    fn into_handler(self) -> Handler<S>;
}

macro_rules! impl_make_handler {
    ($($arg:ident),+) => {
        impl<F, Fut, S, $($arg,)+> MakeHandler<($($arg,)+), S> for F
        where
            F: Fn($($arg,)+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
            S: Send + 'static,
            $($arg: FromRequest<S> + Send + 'static,)+
        {
            #[allow(non_snake_case)]
            fn into_handler(self) -> Handler<S> {
                let func = Arc::new(self);
                Box::new(move |request: Request<S>| {
                    let func = Arc::clone(&func);
                    Box::pin(async move {
                        $(let $arg = $arg::from_request(&request)?;)+
                        func($($arg,)+).await
                    })
                })
            }
        }
    };
}

impl_make_handler!(A);
impl_make_handler!(A, B);
impl_make_handler!(A, B, C);
impl_make_handler!(A, B, C, D);
impl_make_handler!(A, B, C, D, E);

enum Segment {
    Literal(String),
    Param(String),
}

struct Route<S> {
    segments: Vec<Segment>,
    handler: Handler<S>,
}

pub struct MqttRouter<S> {
    routes: Vec<Route<S>>,
}

impl<S> Default for MqttRouter<S> {
    fn default() -> Self {
        Self { routes: vec![] }
    }
}

impl<S: Send + 'static> MqttRouter<S> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `path`, in which a `:name` segment matches one topic level
    /// and is available to the handler through [`Params`].
    pub fn route<Args>(&mut self, path: impl Into<String>, handler: impl MakeHandler<Args, S>) {
        let segments = path
            .into()
            .split('/')
            .map(|segment| match segment.strip_prefix(':') {
                Some(name) => Segment::Param(name.to_string()),
                None => Segment::Literal(segment.to_string()),
            })
            .collect();
        self.routes.push(Route {
            segments,
            handler: handler.into_handler(),
        });
    }

    /// The topic filters to subscribe to, with `+` for each parameter.
    pub fn topics(&self) -> Vec<String> {
        self.routes
            .iter()
            .map(|route| {
                route
                    .segments
                    .iter()
                    .map(|segment| match segment {
                        Segment::Literal(text) => text.as_str(),
                        Segment::Param(_) => "+",
                    })
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .collect()
    }

    /// Runs the first route matching the message's topic.
    pub async fn dispatch(&self, message: Message, state: S) -> anyhow::Result<()> {
        let levels: Vec<&str> = message.topic.split('/').collect();
        for route in &self.routes {
            if route.segments.len() != levels.len() {
                continue;
            }
            let mut params = serde_json::Map::new();
            let matched =
                route
                    .segments
                    .iter()
                    .zip(&levels)
                    .all(|(segment, level)| match segment {
                        Segment::Literal(text) => text == level,
                        Segment::Param(name) => {
                            params.insert(name.clone(), (*level).into());
                            true
                        }
                    });
            if matched {
                let params = if params.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::Object(params)
                };
                return (route.handler)(Request {
                    params,
                    message,
                    state,
                })
                .await;
            }
        }
        anyhow::bail!("no route for topic {}", message.topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::sync::Mutex;

    #[derive(Deserialize)]
    struct Shade {
        serial: String,
        #[serde(deserialize_with = "parse_deser")]
        id: i32,
    }

    type Seen = Arc<Mutex<Vec<String>>>;

    async fn set(
        Params(Shade { serial, id }): Params<Shade>,
        Topic(topic): Topic,
        State(seen): State<Seen>,
        Payload(value): Payload<u8>,
    ) -> anyhow::Result<()> {
        seen.lock()
            .unwrap()
            .push(format!("{topic} {serial} {id} {value}"));
        Ok(())
    }

    async fn status(
        Payload(text): Payload<String>,
        State(seen): State<Seen>,
    ) -> anyhow::Result<()> {
        seen.lock().unwrap().push(format!("status {text}"));
        Ok(())
    }

    fn message(topic: &str, payload: &str) -> Message {
        Message {
            topic: topic.into(),
            payload: payload.into(),
        }
    }

    fn router() -> MqttRouter<Seen> {
        let mut router = MqttRouter::new();
        router.route("homeassistant/status", status);
        router.route("pv/shade/:serial/:id/set", set);
        router
    }

    #[test]
    fn topics_replace_parameters_with_single_level_wildcards() {
        assert_eq!(
            router().topics(),
            vec!["homeassistant/status", "pv/shade/+/+/set"]
        );
    }

    #[tokio::test]
    async fn dispatch_extracts_params_topic_state_and_payload() {
        let seen = Seen::default();
        let router = router();
        router
            .dispatch(message("pv/shade/abc/7/set", "42"), seen.clone())
            .await
            .unwrap();
        router
            .dispatch(message("homeassistant/status", "online"), seen.clone())
            .await
            .unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["pv/shade/abc/7/set abc 7 42", "status online"]
        );
    }

    #[tokio::test]
    async fn dispatch_rejects_unrouted_topics_and_bad_values() {
        let seen = Seen::default();
        let router = router();
        for (topic, payload) in [
            ("pv/shade/abc/7/set/extra", "1"),
            ("pv/shade/abc/set", "1"),
            ("pv/shade/abc/x/set", "1"),
            ("pv/shade/abc/7/set", "300"),
            ("pv/shade/abc/7/set", "\u{0}\u{fffd}not a number"),
        ] {
            assert!(
                router
                    .dispatch(message(topic, payload), seen.clone())
                    .await
                    .is_err(),
                "{topic} {payload:?} should fail"
            );
        }
        let invalid_utf8 = Message {
            topic: "homeassistant/status".into(),
            payload: vec![0xff, 0xfe],
        };
        assert!(router.dispatch(invalid_utf8, seen.clone()).await.is_err());
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn offline_client_fails_fast() {
        let client = Client::offline();
        let err = client
            .publish("a/b", "x", QoS::AtLeastOnce, true)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("not connected"));
    }
}
