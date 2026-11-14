#[derive(pgnotify_macros::NotifyPayload)]
struct NotSerializable { value: u64 }
fn main() {}
