use pgnotify::{NotificationRef, NotifyHook, NotifyPayload as _, NotifyRouter as _, PayloadBuf};
use pgnotify_macros::{NotifyPayload, NotifyRouter};
use serde::{Deserialize, Serialize};

// Caller module names must not affect generated codec paths.
mod sonic_rs {}

#[derive(Serialize, Deserialize, NotifyPayload)]
struct Payload<T> { value: T }

#[derive(Serialize, Deserialize, NotifyPayload)]
struct Marker<'__pgnotify_de> { marker: std::marker::PhantomData<&'__pgnotify_de ()> }

#[derive(NotifyRouter)]
enum GenericNames<__PgNotifyCallback: pgnotify::NotifyPayload> {
    #[channel = "generic"]
    Value(__PgNotifyCallback),
}

#[derive(NotifyRouter)]
enum First {
    #[channel = "changed"]
    Changed(Payload<u64>),
    #[channel = "ready"]
    Ready,
}

#[derive(NotifyRouter)]
enum Second<T: pgnotify::NotifyPayload> {
    #[channel = "values"]
    Value { inner: T },
}

fn main() -> pgnotify::Result<()> {
    let mut out = PayloadBuf::new();
    Payload { value: 7u64 }.encode_payload(&mut out)?;
    let _: Payload<u64> = Payload::decode_payload(out.as_bytes())?;
    let event = Second::Value { inner: Payload { value: 2u32 } };
    event.encode_event_payload(&mut out)?;
    let mut hook: FirstRouterHook<First, _> = First::router_hook(|_: pgnotify::Result<First>| {});
    hook.on_notify(NotificationRef { backend_pid: 0, channel_id: None, channel: "ready", payload: "" });
    let _callback = hook.into_inner();
    Ok(())
}
