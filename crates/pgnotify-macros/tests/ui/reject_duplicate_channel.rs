#[derive(pgnotify_macros::NotifyRouter)]
enum Events {
    #[channel = "same"]
    First,
    #[channel = "same"]
    Second,
}
fn main() {}
