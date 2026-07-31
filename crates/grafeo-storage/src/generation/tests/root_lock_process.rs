//! Fresh-process root-lock proof (W0 §9 / §17).
//!
//! Separate process-level proof surface: a second process is rejected while
//! the owner holds the lock, and an owner abort releases the lock for a
//! fresh process. These run with real kernel locks on a real filesystem.

use std::path::Path;
use std::process::{Command, Stdio};

use crate::generation::lock::{RootLock, RootLockError};
use tempfile::TempDir;

const HELPER_ENV: &str = "GRAFEOROOTPROC_HELPER";

fn child_main() {
    let mode = std::env::var("GRAFEOROOTPROC_MODE").unwrap_or_default();
    let root = std::env::var("GRAFEOROOTPROC_ROOT").expect("child root env");
    match mode.as_str() {
        "second" => match RootLock::try_acquire(Path::new(&root)) {
            Err(RootLockError::AlreadyLocked) => std::process::exit(0),
            Err(other) => {
                eprintln!("child: unexpected {other}");
                std::process::exit(2);
            }
            Ok(_) => {
                eprintln!("child: acquired a held lock");
                std::process::exit(3);
            }
        },
        "crash" => {
            let lock = RootLock::try_acquire(Path::new(&root)).expect("child acquire");
            std::hint::black_box(&lock);
            std::process::abort();
        }
        other => {
            eprintln!("child: unknown mode {other}");
            std::process::exit(4);
        }
    }
}

fn supported_tempdir() -> Option<TempDir> {
    // Ambient temp dir must live on an allowlisted filesystem for the real
    // mount validation to pass (GCP host: /tmp → ext4 bind mount).
    let text = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let entries = crate::generation::lock::parse_mountinfo(&text).unwrap_or_default();
    let tmp = std::fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
    let allowed = crate::generation::lock::filesystem_for_path(&tmp, &entries).is_ok_and(|f| {
        matches!(
            f.as_str(),
            "ext2" | "ext3" | "ext4" | "xfs" | "btrfs" | "zfs"
        )
    });
    if !allowed {
        eprintln!("SKIP: ambient temp filesystem not in lock allowlist");
        return None;
    }
    Some(tempfile::tempdir().expect("tempdir"))
}

#[test]
fn root_lock_second_process_rejected() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if crate::generation::tests::support::in_any_child() {
        return;
    }
    let Some(dir) = supported_tempdir() else {
        return;
    };
    let _lock = RootLock::try_acquire(dir.path()).expect("parent acquire");

    let status = Command::new(std::env::current_exe().expect("current exe"))
        .env(HELPER_ENV, "1")
        .env("GRAFEOROOTPROC_MODE", "second")
        .env("GRAFEOROOTPROC_ROOT", dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("spawn child");
    assert!(status.success(), "child must observe AlreadyLocked");
}

#[test]
fn root_lock_crash_releases_for_fresh_process() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if crate::generation::tests::support::in_any_child() {
        return;
    }
    let Some(dir) = supported_tempdir() else {
        return;
    };
    let status = Command::new(std::env::current_exe().expect("current exe"))
        .env(HELPER_ENV, "1")
        .env("GRAFEOROOTPROC_MODE", "crash")
        .env("GRAFEOROOTPROC_ROOT", dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("spawn child");
    assert!(!status.success(), "child must abort while holding the lock");

    RootLock::try_acquire(dir.path()).expect("fresh process acquires after crash");
}
