#[derive(pgnotify_macros::NotifyRouter)]
enum Events {
    #[channel = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    Long,
}
fn main() {}
