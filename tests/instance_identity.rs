//! RealityEngine_CI#296 across processes: an instance holding an allocated UUID
//! takes an exclusive lock for its life, so a second live process with the same
//! UUID refuses to boot. A POSIX record lock does not exclude its own process,
//! so this needs a second one: the built engine.

use reality_engine::instance_clock::InstanceClock;
use std::{process::Command, time::Duration};

const UUID: &str = "0192f3a4-5b6c-7d8e-9f01-23456789abce";

fn boot_engine(clock_dir: &std::path::Path, corpus: &std::path::Path) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_reality-engine"))
        .env("INSTANCE_UUID", UUID)
        .env("INSTANCE_CLOCK_DIR", clock_dir)
        .env("MACHINES_DIR", corpus)
        .env("REALITY_ENGINE_PORT", "0")
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

#[test]
fn a_second_process_with_the_same_uuid_refuses_to_boot() {
    let dir = std::env::temp_dir().join(format!("re-rust-identity-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (clocks, corpus) = (dir.join("clock"), dir.join("machines"));
    std::fs::create_dir_all(&corpus).unwrap();

    let held = InstanceClock::persisted(UUID, &clocks.join(format!("{UUID}.lamport"))).unwrap();
    let refused = boot_engine(&clocks, &corpus).wait_with_output().unwrap();
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains(&format!("Instance {UUID} is already live")),
        "{stderr}"
    );

    // Released with the holder: the same UUID now boots, and keeps running.
    drop(held);
    let mut booted = boot_engine(&clocks, &corpus);
    std::thread::sleep(Duration::from_millis(500));
    let still_running = booted.try_wait().unwrap().is_none();
    booted.kill().unwrap();
    let output = booted.wait_with_output().unwrap();
    assert!(still_running, "{}", String::from_utf8_lossy(&output.stderr));
    let _ = std::fs::remove_dir_all(&dir);
}
