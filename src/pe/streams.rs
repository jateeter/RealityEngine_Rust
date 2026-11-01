//! The PE's event streams (#20 workstream 3): SSE `GET /api/events` and the
//! WebSocket `/ws`.
//!
//! C++ `hub_broadcast` is the concrete form this follows; SURFACE_SPEC.md,
//! "Streaming events", is the contract. Both streams carry the same messages:
//!
//! - `{type: "state-update", state}`, `state` as `GET /api/state` answers it,
//!   after every change to it: a configuration, a reset, a source registered,
//!   patched or deleted, a bootstrap, a sensor value, a push.
//! - `{type: "push-result", ...}`, the push's reply with `type` beside its
//!   keys, after every push that ran, failed ones included.
//!
//! SSE frames each as `data: <json>`, opens with a `: connected` comment and
//! keeps the line open with `: keepalive` every `SSE_HEARTBEAT_MS` (15 s). A
//! WebSocket client is sent the current state as it connects, and pinged on
//! the same interval. Messages a client sends are not read. A message is built
//! only when someone is listening.

use super::*;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use futures_util::{Stream, stream};
use std::convert::Infallible;
use tokio::sync::broadcast::error::RecvError;

/// Room for a slow client before it skips ahead.
const CAPACITY: usize = 256;

pub(crate) fn channel() -> tokio::sync::broadcast::Sender<String> {
    tokio::sync::broadcast::channel(CAPACITY).0
}

/// A message as the wire carries it: ECMAScript number formatting, as every
/// other answer of this engine.
fn text(message: &Value) -> String {
    ecmascript_json(message)
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|_| message.to_string())
}

impl PerceptionEngine {
    fn listened(&self) -> bool {
        self.events.receiver_count() > 0
    }

    fn state_update(&self) -> String {
        text(&json!({ "type": "state-update", "state": self.state() }))
    }

    /// C++ `broadcast_state`. No subscriber is not an error.
    pub(crate) fn broadcast_state(&self) {
        if self.listened() {
            let _ = self.events.send(self.state_update());
        }
    }

    /// C++ `broadcast_push_result`.
    pub(crate) fn broadcast_push_result(&self, result: &Value) {
        if !self.listened() {
            return;
        }
        let mut message = json!({ "type": "push-result" });
        for (key, value) in result.as_object().into_iter().flatten() {
            message[key] = value.clone();
        }
        let _ = self.events.send(text(&message));
    }
}

pub(crate) async fn events(
    State(pe): State<SharedPe>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, (StatusCode, Json<Value>)> {
    let receiver = read(&pe)?.events.subscribe();
    let connected = stream::once(async { Ok(SseEvent::default().comment("connected")) });
    let messages = stream::unfold(receiver, |mut receiver| async move {
        loop {
            match receiver.recv().await {
                Ok(text) => return Some((Ok(SseEvent::default().data(text)), receiver)),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Ok(
        Sse::new(futures_util::StreamExt::chain(connected, messages)).keep_alive(
            KeepAlive::new()
                .interval(simulation::heartbeat())
                .text("keepalive"),
        ),
    )
}

pub(crate) async fn websocket(State(pe): State<SharedPe>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| serve_socket(pe, socket))
}

async fn serve_socket(pe: SharedPe, mut socket: WebSocket) {
    let (mut receiver, first) = match pe.read() {
        Ok(state) => (state.events.subscribe(), state.state_update()),
        Err(_) => return,
    };
    if socket.send(Message::text(first)).await.is_err() {
        return;
    }
    let mut ping = tokio::time::interval(simulation::heartbeat());
    ping.tick().await;
    loop {
        tokio::select! {
            message = receiver.recv() => match message {
                Ok(text) => {
                    if socket.send(Message::text(text)).await.is_err() {
                        return;
                    }
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            },
            _ = ping.tick() => {
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    return;
                }
            }
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
        }
    }
}
