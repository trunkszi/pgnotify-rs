//! Cold-path PostgreSQL fixture discovery and local Docker readiness.

use std::{
    env,
    error::Error,
    net::{SocketAddr, TcpStream},
    process::{Command, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

const CONTAINER: &str = "pgnotify-pg-e2e";
const DEFAULT_URL: &str = "postgresql://postgres:postgres@127.0.0.1:55433/postgres?sslmode=disable";
static DATABASE: OnceLock<Result<String, String>> = OnceLock::new();

/// Resolve an explicit endpoint or prepare the local Docker fixture.
///
/// # Errors
/// Returns missing Docker, failed startup, or readiness timeout errors.
pub(crate) fn database_url() -> Result<String, Box<dyn Error>> {
    DATABASE.get_or_init(resolve).clone().map_err(Into::into)
}

fn resolve() -> Result<String, String> {
    match env::var("PGNOTIFY_TEST_DATABASE_URL") {
        Ok(value) if !value.is_empty() => return Ok(value),
        Ok(_) => return Err("PGNOTIFY_TEST_DATABASE_URL must not be empty".into()),
        Err(env::VarError::NotPresent) => (),
        Err(env::VarError::NotUnicode(_)) => {
            return Err("PGNOTIFY_TEST_DATABASE_URL must be UTF-8".into());
        }
    }
    let running = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Running}}", CONTAINER])
        .output()
        .map_err(|_| "Docker is required for the default PostgreSQL fixture")?;
    if !running.status.success() {
        let started = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--name",
                CONTAINER,
                "--env",
                "POSTGRES_PASSWORD=postgres",
                "--publish",
                "127.0.0.1:55433:5432",
                "postgres:18.6",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|_| "could not launch the PostgreSQL fixture")?;
        if !started.success() {
            // Another test process may have created this same named fixture.
            let exists = Command::new("docker")
                .args(["inspect", CONTAINER])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map_err(|_| "could not inspect the PostgreSQL fixture")?;
            if !exists.success() {
                return Err("could not create the PostgreSQL fixture".into());
            }
        }
    } else if running.stdout.as_slice() != b"true\n" {
        let started = Command::new("docker")
            .args(["start", CONTAINER])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|_| "could not start the PostgreSQL fixture")?;
        if !started.success() {
            return Err("could not start the PostgreSQL fixture".into());
        }
    }
    let address: SocketAddr = "127.0.0.1:55433"
        .parse()
        .map_err(|_| "invalid fixture address")?;
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let healthy = Command::new("docker")
            .args(["exec", CONTAINER, "pg_isready", "-U", "postgres"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|_| "could not check PostgreSQL readiness")?;
        if healthy.success()
            && TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok()
        {
            return Ok(DEFAULT_URL.into());
        }
        if Instant::now() >= deadline {
            return Err("PostgreSQL fixture readiness timed out".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}
