//! The participant clock on this process's HTTP boundaries (RealityEngine_CI#600;
//! SURFACE_SPEC.md "The instance clock"), shared by the RE and the PE:
//!
//! - Server: every request is a receive, applied before it is handled --
//!   `max(local, received) + 1` with an `RE-Clock` header, `+ 1` without -- and
//!   a malformed or repeated header is refused with 400 (still a receive).
//!   Every response, a 404 or failure included, is a send: the clock ticks,
//!   then stamps `RE-Clock`.
//! - Client ([`stamp`], [`receive_response`]): every outbound call to an
//!   internal peer is a send and its response a receive. An external party is
//!   never stamped: the clock crosses the local integration boundaries only,
//!   and an instance UUID never leaves the install.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use crate::instance_clock::{HeaderKind, InstanceClock, parse_header};

pub const HEADER: &str = "re-clock";

fn clock_failure(message: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": message })),
    )
        .into_response()
}

fn stamp_response(clock: &InstanceClock, mut response: Response) -> Response {
    match clock.send() {
        Ok(lamport) => {
            if let Ok(value) = HeaderValue::from_str(&clock.header(lamport)) {
                response.headers_mut().insert(HEADER, value);
            }
            response
        }
        Err(message) => clock_failure(message),
    }
}

/// The server half, around every route (`from_fn_with_state`).
pub async fn server(
    State(clock): State<Arc<InstanceClock>>,
    request: Request,
    next: Next,
) -> Response {
    let values: Vec<&HeaderValue> = request.headers().get_all(HEADER).iter().collect();
    let parsed = match values.as_slice() {
        [] => Some(None),
        [one] => one.to_str().ok().and_then(parse_header).map(Some),
        _ => None,
    };
    let Some(received) = parsed else {
        clock.count_header(HeaderKind::Rejected);
        if let Err(message) = clock.receive(None) {
            return clock_failure(message);
        }
        let refused = (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "malformed RE-Clock header" })),
        )
            .into_response();
        return stamp_response(&clock, refused);
    };
    clock.count_header(if received.is_some() {
        HeaderKind::Received
    } else {
        HeaderKind::Missing
    });
    if let Err(message) = clock.receive(received) {
        return clock_failure(message);
    }
    let response = next.run(request).await;
    stamp_response(&clock, response)
}

/// An internal peer, by the rule lane TLS uses ([`crate::tls::is_internal_host`]).
pub fn is_internal_host(host: &str) -> bool {
    crate::tls::is_internal_host(host)
}

/// The client half, before a call: the `RE-Clock` to send to `host`, or
/// `None` for an external party (which is neither stamped nor ticked).
pub fn stamp(clock: &InstanceClock, host: &str) -> Option<String> {
    if !is_internal_host(host) {
        return None;
    }
    clock.send().ok().map(|lamport| clock.header(lamport))
}

/// The client half, after a call to an internal peer: its response's header is
/// the responder's clock. One that is absent or unreadable still marks a
/// receive, as a local event.
pub fn receive_response(clock: &InstanceClock, headers: &HeaderMap) {
    let received = headers
        .get(HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_header);
    let _ = clock.receive(received);
}

/// `headers` on a clock route: the inbound `RE-Clock` headers since boot.
pub fn headers_json(clock: &InstanceClock) -> Value {
    let (received, missing, rejected) = clock.header_counts();
    json!({ "received": received, "missing": missing, "rejected": rejected })
}

/// `clock` on a message this participant stores or forwards: a tick, as
/// `{instance, lamport}` (SURFACE_SPEC "The instance clock", body fields).
pub fn event_json(clock: &InstanceClock) -> Value {
    match clock.event() {
        Ok((instance, lamport)) => json!({ "instance": instance, "lamport": lamport }),
        Err(_) => Value::Null,
    }
}

/// A completion's `causedBy`: the PE's own dispatch record's clock when it has
/// one, else the sender's well-formed echo (a canonical lower-case UUID and a
/// lamport in `[0, 2^53 - 1]`), else null.
pub fn caused_by(own: Option<&Value>, echo: &Value) -> Value {
    if let Some(own) = own.filter(|v| v.is_object()) {
        return own.clone();
    }
    let instance = echo["instance"].as_str().unwrap_or("");
    let lowercase_uuid = crate::instance_clock::canonical_uuid(instance)
        && !instance.bytes().any(|c| c.is_ascii_uppercase());
    match echo["lamport"].as_u64() {
        Some(lamport) if lowercase_uuid && lamport <= crate::instance_clock::MAX_LAMPORT => {
            json!({ "instance": instance, "lamport": lamport })
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_external_party_is_never_stamped() {
        let clock = InstanceClock::minted();
        assert!(stamp(&clock, "api.openai.com").is_none());
        assert_eq!(clock.lamport(), 0, "and the clock does not tick for it");
        assert!(stamp(&clock, "127.0.0.1").is_some());
        assert_eq!(clock.lamport(), 1);
    }

    #[test]
    fn caused_by_prefers_the_record_then_a_well_formed_echo() {
        let own = json!({ "instance": "0192f3a0-0000-7000-8000-0000000000c2", "lamport": 7 });
        let echo = json!({ "instance": "0192f3a0-0000-7000-8000-000000000009", "lamport": 42 });
        assert_eq!(caused_by(Some(&own), &echo), own);
        assert_eq!(caused_by(None, &echo), echo);
        for bad in [
            json!({ "instance": "NOPE", "lamport": 42 }),
            json!({ "instance": "0192F3A0-0000-7000-8000-000000000009", "lamport": 42 }),
            json!({ "instance": "0192f3a0-0000-7000-8000-000000000009", "lamport": -1 }),
            json!({ "instance": "0192f3a0-0000-7000-8000-000000000009", "lamport": 9_007_199_254_740_992u64 }),
            Value::Null,
        ] {
            assert_eq!(caused_by(None, &bad), Value::Null, "{bad}");
        }
    }
}
