#[derive(pgnotify_macros::NotifyRouter)]
enum Events {
    #[channel("name")]
    Invalid,
}
fn main() {}
