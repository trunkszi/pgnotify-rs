#[derive(pgnotify_macros::NotifyRouter)]
enum Events {
    #[channel = ""]
    Empty,
}
fn main() {}
