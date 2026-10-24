#[derive(pgnotify_macros::NotifyRouter)]
enum Events {
    #[channel = "bad\0name"]
    Invalid,
}
fn main() {}
