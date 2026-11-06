#![cfg(feature = "sonic")]
use std::{fs, path::PathBuf, process::Command};

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn renamed_dependency_compiles() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = Fixture(
        std::env::temp_dir().join(format!("pgnotify-renamed-contract-{}", std::process::id())),
    );
    fs::create_dir_all(fixture.0.join("src"))?;
    fs::copy(root.join("../../Cargo.lock"), fixture.0.join("Cargo.lock"))?;
    fs::write(
        fixture.0.join("Cargo.toml"),
        format!(
            "[package]\nname = \"renamed-notifications\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[workspace]\n[dependencies]\nnotifications = {{ package = \"pgnotify\", path = {:?}, default-features = false, features = [\"sonic\"] }}\npgnotify-macros = {{ path = {:?} }}\nserde = {{ version = \"1\", features = [\"derive\"] }}\n",
            root,
            root.join("../pgnotify-macros")
        ),
    )?;
    fs::write(
        fixture.0.join("src/main.rs"),
        r#"
use notifications::{NotifyPayload as _, NotifyRouter as _, NotificationRef, PayloadBuf};
use pgnotify_macros::NotifyPayload;
#[derive(serde::Serialize, serde::Deserialize, NotifyPayload)]
struct Payload { count: ::core::primitive::u64 }
mod caller {
    use super::Payload;
    #[allow(non_camel_case_types, dead_code)]
    struct u64;
    #[allow(non_camel_case_types, dead_code)]
    struct str;
    #[allow(dead_code)]
    struct FnMut;
    #[allow(dead_code)]
    struct Option;
    #[allow(dead_code)]
    struct Some;
    #[allow(dead_code)]
    struct None;
    #[allow(dead_code)]
    struct Ok;
    #[allow(dead_code)]
    struct Err;
    #[derive(pgnotify_macros::NotifyRouter)]
    pub(super) enum Route {
        #[channel = "orders"]
        Orders(Payload),
    }
}
use caller::Route;
fn main() -> notifications::Result<()> {
    let mut out = PayloadBuf::new();
    Payload { count: 1 }.encode_payload(&mut out)?;
    assert!(Route::decode_notification(NotificationRef {
        backend_pid: 1, channel_id: None, channel: "orders", payload: out.as_str()?
    })?.is_some());
    Ok(())
}
"#,
    )?;
    let output = Command::new(env!("CARGO"))
        .args(["check", "--offline", "--quiet"])
        .arg("--manifest-path")
        .arg(fixture.0.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", fixture.0.join("target"))
        .output()?;
    assert!(
        output.status.success(),
        "renamed dependency failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
