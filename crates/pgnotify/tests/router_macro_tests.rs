#![cfg(feature = "sonic")]

use pgnotify::hook::NotifyPayload as _;
use pgnotify::hook::NotifyRouter as _;
use pgnotify::notify_ref::NotificationRef;
use pgnotify::types::PayloadBuf;
use pgnotify_macros::NotifyPayload;
use pgnotify_macros::NotifyRouter;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, NotifyPayload)]
struct Price {
    ticks: u64,
}

#[test]
fn integration_payload_json_round_trip() {
    let mut out = PayloadBuf::new();
    assert!(Price { ticks: 42 }.encode_payload(&mut out).is_ok());
    assert_eq!(out.as_bytes(), br#"{"ticks":42}"#);
    assert_eq!(
        Price::decode_payload(out.as_bytes()),
        Ok(Price { ticks: 42 })
    );
}

#[derive(Debug, PartialEq, NotifyRouter)]
enum Events {
    #[channel = "heartbeat"]
    Heartbeat,
    #[channel = "prices"]
    Price(Price),
    #[channel = "book_state"]
    Book { value: Price },
}

#[test]
fn integration_scenario_router_empty_payload_and_error_callbacks() {
    use pgnotify::hook::NotifyHook as _;
    let mut observed = Vec::new();
    {
        let mut hook = Events::router_hook(|event| observed.push(event));
        hook.on_notify(NotificationRef {
            backend_pid: 7,
            channel_id: None,
            channel: "heartbeat",
            payload: "",
        });
        hook.on_notify(NotificationRef {
            backend_pid: 7,
            channel_id: None,
            channel: "other",
            payload: "broken",
        });
        hook.on_notify(NotificationRef {
            backend_pid: 7,
            channel_id: None,
            channel: "prices",
            payload: "broken",
        });
        hook.on_error(pgnotify::error::PgNotifyError::CodecError { reason: "upstream" });
        drop(hook.into_inner());
    }
    assert_eq!(observed.len(), 3);
    assert_eq!(observed[0], Ok(Events::Heartbeat));
    assert!(matches!(
        observed[1],
        Err(pgnotify::error::PgNotifyError::CodecError { .. })
    ));
    assert_eq!(
        observed[2],
        Err(pgnotify::error::PgNotifyError::CodecError { reason: "upstream" })
    );
    let mut out = PayloadBuf::new();
    assert!(
        Events::Price(Price { ticks: 8 })
            .encode_event_payload(&mut out)
            .is_ok()
    );
    assert_eq!(Events::Price(Price { ticks: 8 }).channel_name(), "prices");
    assert!(Events::Heartbeat.encode_event_payload(&mut out).is_ok());
    assert!(out.is_empty());
    assert!(
        Events::decode_notification(NotificationRef {
            backend_pid: 0,
            channel_id: None,
            channel: "heartbeat",
            payload: "null"
        })
        .is_err()
    );
}

#[test]
fn integration_scenario_listener_fault_reports_error_once() {
    use pgnotify::error::PgNotifyError;
    use pgnotify::hook::NotifyHook as _;

    let error = PgNotifyError::CodecError { reason: "upstream" };
    let mut observed = Vec::new();
    {
        let mut hook = Events::router_hook(|event| observed.push(event));
        hook.on_error(error);
        hook.on_disconnect(1, error);
    }
    assert_eq!(observed, vec![Err(error)]);
}

#[derive(Debug, PartialEq, NotifyRouter)]
enum Collisions {
    #[channel = "abcdefgh.first"]
    First,
    #[channel = "abcdefgh.other"]
    Other,
    #[channel = "abcdefgh.oXher"]
    Third,
    #[channel = "quotes\";data"]
    Quoted,
    #[channel = "\u{901a}\u{9053}"]
    Unicode,
}

#[test]
fn integration_scenario_prefix_collisions_and_quoted_sql() {
    for (channel, expected) in [
        ("abcdefgh.first", Collisions::First),
        ("abcdefgh.other", Collisions::Other),
        ("abcdefgh.oXher", Collisions::Third),
        ("\u{901a}\u{9053}", Collisions::Unicode),
    ] {
        assert_eq!(
            Collisions::decode_notification(NotificationRef {
                backend_pid: 0,
                channel_id: None,
                channel,
                payload: ""
            }),
            Ok(Some(expected))
        );
    }
    assert_eq!(
        Collisions::decode_notification(NotificationRef {
            backend_pid: 0,
            channel_id: None,
            channel: "abcdefgh.oYher",
            payload: ""
        }),
        Ok(None)
    );
    assert!(Collisions::LISTEN_BATCH_SQL.contains("LISTEN \"quotes\"\";data\";"));
    assert_eq!(Collisions::CHANNELS.len(), 5);
}

#[derive(Debug, PartialEq, Serialize, Deserialize, NotifyPayload)]
#[serde(transparent)]
struct Text(String);

#[test]
fn integration_scenario_payload_capacity_and_malformed_json() {
    use pgnotify::error::PgNotifyError;
    let mut out = PayloadBuf::new();
    let exact = Text("a".repeat(PayloadBuf::CAPACITY - 2));
    assert!(exact.encode_payload(&mut out).is_ok());
    assert_eq!(out.len(), PayloadBuf::CAPACITY);
    assert_eq!(Text::decode_payload(out.as_bytes()), Ok(exact));
    let oversized = Text("a".repeat(PayloadBuf::CAPACITY));
    assert_eq!(
        oversized.encode_payload(&mut out),
        Err(PgNotifyError::PayloadTooLarge {
            len: 8000,
            max: 7999
        })
    );
    assert!(out.is_empty());
    assert!(Price { ticks: 1 }.encode_payload(&mut out).is_ok());
    assert!(matches!(
        Price::decode_payload(b"{"),
        Err(PgNotifyError::CodecError { .. })
    ));
    assert_eq!(
        Price::decode_payload(&vec![b' '; 8000]),
        Err(PgNotifyError::PayloadTooLarge {
            len: 8000,
            max: 7999
        })
    );
}

#[test]
fn integration_router_decodes_known_channels_and_ignores_unknown() {
    let notify = NotificationRef {
        backend_pid: 1,
        channel_id: None,
        channel: "prices",
        payload: r#"{"ticks":42}"#,
    };
    assert_eq!(
        Events::decode_notification(notify),
        Ok(Some(Events::Price(Price { ticks: 42 })))
    );
    assert_eq!(
        Events::decode_notification(NotificationRef {
            channel: "other",
            ..notify
        }),
        Ok(None)
    );
    assert_eq!(
        Events::LISTEN_BATCH_SQL,
        "LISTEN \"heartbeat\";LISTEN \"prices\";LISTEN \"book_state\";"
    );
}
