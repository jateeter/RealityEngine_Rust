//! A minimal MQTT v3.1.1 client, ingest only (C++ `mqtt_client.cpp`):
//! CONNECT, SUBSCRIBE to a fixed set of filters, receive PUBLISH, PINGREQ
//! when half the keepalive has passed without traffic, and reconnect after
//! `reconnect_delay` whenever the connection is lost. No PUBLISH of its own,
//! no QoS above 1, no TLS.
//!
//! One difference from C++, which reads a QoS 1 PUBLISH's packet id and
//! never acknowledges it: this client answers PUBACK, as v3.1.1 §4.3.2
//! requires. Nothing on the HTTP surface observes it.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::Instant;

const CONNECT: u8 = 0x10;
const CONNACK: u8 = 0x20;
const PUBLISH: u8 = 0x30;
const PUBACK: u8 = 0x40;
const SUBSCRIBE: u8 = 0x82;
const PINGREQ: u8 = 0xC0;

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) host: String,
    pub(crate) port: i64,
    pub(crate) client_id: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) keepalive_sec: i64,
    pub(crate) reconnect_delay: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".to_owned(),
            port: 1883,
            client_id: "reality-engine-pe".to_owned(),
            username: None,
            password: None,
            keepalive_sec: 60,
            reconnect_delay: Duration::from_millis(2000),
        }
    }
}

#[derive(Default, Clone, Copy)]
pub(crate) struct Stats {
    pub(crate) connect_attempts: u64,
    pub(crate) connect_successes: u64,
    pub(crate) messages_received: u64,
    pub(crate) bytes_received: u64,
    pub(crate) pings_sent: u64,
    pub(crate) reconnects: u64,
    pub(crate) last_message_at_ms: i64,
}

pub(crate) struct Client {
    pub(crate) config: Config,
    /// Each filter once, with its QoS clamped to 0..1.
    subscriptions: Vec<(String, u8)>,
    stats: Mutex<Stats>,
    connected: AtomicBool,
}

fn put_string(out: &mut Vec<u8>, text: &str) {
    let bytes = &text.as_bytes()[..text.len().min(usize::from(u16::MAX))];
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// A packet: the first byte, the remaining length (§2.2.3), the body.
fn packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    let mut length = body.len();
    loop {
        let mut byte = (length & 0x7F) as u8;
        length >>= 7;
        if length > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if length == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

async fn remaining_length(stream: &mut TcpStream) -> std::io::Result<usize> {
    let mut length = 0usize;
    let mut multiplier = 1usize;
    for _ in 0..4 {
        let byte = stream.read_u8().await?;
        length += usize::from(byte & 0x7F) * multiplier;
        if byte & 0x80 == 0 {
            return Ok(length);
        }
        multiplier *= 128;
    }
    Err(std::io::Error::other("malformed remaining length"))
}

impl Client {
    pub(crate) fn new(config: Config, subscriptions: Vec<(String, i64)>) -> Self {
        Self {
            config,
            subscriptions: subscriptions
                .into_iter()
                .map(|(filter, qos)| (filter, qos.clamp(0, 1) as u8))
                .collect(),
            stats: Mutex::default(),
            connected: AtomicBool::new(false),
        }
    }

    pub(crate) fn stats(&self) -> Stats {
        self.stats.lock().map(|s| *s).unwrap_or_default()
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    fn count(&self, update: impl FnOnce(&mut Stats)) {
        if let Ok(mut stats) = self.stats.lock() {
            update(&mut stats);
        }
    }

    /// TCP, CONNECT, and a CONNACK that accepts.
    async fn connect(&self) -> std::io::Result<TcpStream> {
        let port = u16::try_from(self.config.port).map_err(std::io::Error::other)?;
        let mut stream = TcpStream::connect((self.config.host.as_str(), port)).await?;
        let mut body = Vec::new();
        put_string(&mut body, "MQTT");
        body.push(0x04);
        let mut flags = 0x02;
        if self.config.username.is_some() {
            flags |= 0x80;
        }
        if self.config.password.is_some() {
            flags |= 0x40;
        }
        body.push(flags);
        body.extend_from_slice(&(self.config.keepalive_sec as u16).to_be_bytes());
        put_string(&mut body, &self.config.client_id);
        for field in [&self.config.username, &self.config.password]
            .into_iter()
            .flatten()
        {
            put_string(&mut body, field);
        }
        stream.write_all(&packet(CONNECT, &body)).await?;
        let first = stream.read_u8().await?;
        if first & 0xF0 != CONNACK || remaining_length(&mut stream).await? != 2 {
            return Err(std::io::Error::other("no CONNACK"));
        }
        let mut ack = [0u8; 2];
        stream.read_exact(&mut ack).await?;
        if ack[1] != 0 {
            return Err(std::io::Error::other("connection refused"));
        }
        Ok(stream)
    }

    async fn subscribe(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        if self.subscriptions.is_empty() {
            return Ok(());
        }
        let mut body = 1u16.to_be_bytes().to_vec();
        for (filter, qos) in &self.subscriptions {
            put_string(&mut body, filter);
            body.push(*qos);
        }
        stream.write_all(&packet(SUBSCRIBE, &body)).await
    }

    /// Read packets until the connection fails or `stop` is raised, pinging
    /// on schedule and handing each PUBLISH to `on_message`.
    async fn read_loop(
        &self,
        stream: &mut TcpStream,
        stop: &mut watch::Receiver<bool>,
        on_message: &(dyn Fn(String, Vec<u8>) + Send + Sync),
    ) {
        let mut last_traffic = Instant::now();
        let half_keepalive =
            Duration::from_millis((self.config.keepalive_sec.max(0) * 1000 / 2) as u64);
        loop {
            let first = tokio::select! {
                _ = stop.changed() => return,
                read = tokio::time::timeout(Duration::from_secs(1), stream.read_u8()) => read,
            };
            if self.config.keepalive_sec > 0 && last_traffic.elapsed() >= half_keepalive {
                if stream.write_all(&[PINGREQ, 0]).await.is_ok() {
                    self.count(|s| s.pings_sent += 1);
                }
                last_traffic = Instant::now();
            }
            let first = match first {
                Err(_) => continue,
                Ok(Err(_)) => return,
                Ok(Ok(first)) => first,
            };
            let Ok(length) = remaining_length(stream).await else {
                return;
            };
            let mut body = vec![0u8; length];
            if stream.read_exact(&mut body).await.is_err() {
                return;
            }
            last_traffic = Instant::now();
            if first & 0xF0 != PUBLISH || body.len() < 2 {
                continue;
            }
            let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
            if body.len() < 2 + topic_len {
                continue;
            }
            let topic = String::from_utf8_lossy(&body[2..2 + topic_len]).into_owned();
            let mut start = 2 + topic_len;
            if (first >> 1) & 0x03 > 0 && body.len() >= start + 2 {
                let _ = stream
                    .write_all(&packet(PUBACK, &body[start..start + 2]))
                    .await;
                start += 2;
            }
            let payload = body[start..].to_vec();
            self.count(|s| {
                s.messages_received += 1;
                s.bytes_received += payload.len() as u64;
                s.last_message_at_ms = super::now();
            });
            on_message(topic, payload);
        }
    }

    /// Connect, subscribe and read, reconnecting, until `stop` is raised.
    pub(crate) async fn run(
        &self,
        mut stop: watch::Receiver<bool>,
        on_message: Box<dyn Fn(String, Vec<u8>) + Send + Sync>,
    ) {
        while !*stop.borrow() {
            self.count(|s| s.connect_attempts += 1);
            let connected = tokio::select! {
                _ = stop.changed() => return,
                connected = self.connect() => connected,
            };
            let Ok(mut stream) = connected else {
                tokio::select! {
                    _ = stop.changed() => return,
                    () = tokio::time::sleep(self.config.reconnect_delay) => {}
                }
                continue;
            };
            self.count(|s| s.connect_successes += 1);
            self.connected.store(true, Ordering::SeqCst);
            if self.subscribe(&mut stream).await.is_ok() {
                self.read_loop(&mut stream, &mut stop, on_message.as_ref())
                    .await;
            }
            self.connected.store(false, Ordering::SeqCst);
            if *stop.borrow() {
                let _ = stream.write_all(&[0xE0, 0]).await;
                return;
            }
            self.count(|s| s.reconnects += 1);
        }
    }
}
