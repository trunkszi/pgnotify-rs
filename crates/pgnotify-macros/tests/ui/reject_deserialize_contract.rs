use serde::Serialize;

#[derive(Serialize, pgnotify_macros::NotifyPayload)]
struct WriteOnly { value: u64 }
fn main() {
    drop(<WriteOnly as pgnotify::NotifyPayload>::decode_payload(b"{}"));
}
