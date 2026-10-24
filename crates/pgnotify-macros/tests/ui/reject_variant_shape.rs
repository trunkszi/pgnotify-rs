#[derive(pgnotify_macros::NotifyRouter)]
enum Events {
    #[channel = "pair"]
    Pair(u32, u32),
}
fn main() {}
