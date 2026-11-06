#[path = "../../crates/pgnotify/tests/support/postgres.rs"]
mod postgres;

use std::{env, process::Command};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::args().nth(1).as_deref() != Some("ci") {
        return Err("usage: cargo xtask ci".into());
    }
    let database_url = postgres::database_url()?;
    for args in [
        vec!["fmt", "--all", "--", "--check"],
        vec![
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
        vec![
            "clippy",
            "-p",
            "pgnotify",
            "--no-default-features",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
        vec![
            "clippy",
            "-p",
            "pgnotify",
            "--no-default-features",
            "--features",
            "sonic",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
        vec!["test", "--workspace", "--locked"],
        vec![
            "test",
            "-p",
            "pgnotify",
            "--no-default-features",
            "--locked",
        ],
        vec![
            "test",
            "-p",
            "pgnotify",
            "--no-default-features",
            "--features",
            "sonic",
            "--locked",
        ],
        vec!["doc", "--workspace", "--no-deps", "--locked"],
    ] {
        let mut command = Command::new("cargo");
        command.args(&args);
        command.env("PGNOTIFY_TEST_DATABASE_URL", &database_url);
        if args[0] == "doc" {
            let mut flags = env::var("RUSTDOCFLAGS").unwrap_or_default();
            flags.push_str(" -D warnings");
            command.env("RUSTDOCFLAGS", flags);
        }
        if !command.status()?.success() {
            return Err(format!("quality gate failed: cargo {}", args.join(" ")).into());
        }
    }
    Ok(())
}
