//! RootLock tests: acquisition, process rejection, crash release, alias
//! rejection, mount validation (synthetic + real), and CLOEXEC probe.

use std::path::Path;
use std::process::{Command, Stdio};

use super::{
    ALLOWED_FILESYSTEMS, MountEntry, RootLock, RootLockError, filesystem_for_path, parse_mountinfo,
};
use tempfile::TempDir;

/// Helper-mode env var used by the child re-exec pattern.
const HELPER_ENV: &str = "GRAFEOLOCK_HELPER";

/// True when the ambient temp dir lives on an allowlisted filesystem.
fn ambient_fs_supported() -> bool {
    let text = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let entries = parse_mountinfo(&text).unwrap_or_default();
    let tmp = std::fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
    filesystem_for_path(&tmp, &entries).is_ok_and(|f| ALLOWED_FILESYSTEMS.contains(&f.as_str()))
}

fn supported_tempdir() -> Option<TempDir> {
    if !ambient_fs_supported() {
        eprintln!("SKIP: ambient temp filesystem not in lock allowlist");
        return None;
    }
    Some(tempfile::tempdir().expect("tempdir must succeed"))
}

fn child_main() {
    let mode = std::env::var("GRAFEOLOCK_CHILD_MODE").unwrap_or_default();
    let root = std::env::var("GRAFEOLOCK_CHILD_ROOT").expect("child root env");
    match mode.as_str() {
        "second" => match RootLock::try_acquire(Path::new(&root)) {
            Err(RootLockError::AlreadyLocked) => std::process::exit(0),
            Err(other) => {
                eprintln!("child: unexpected error {other}");
                std::process::exit(2);
            }
            Ok(_) => {
                eprintln!("child: unexpectedly acquired a held lock");
                std::process::exit(3);
            }
        },
        "crash" => {
            let lock = match RootLock::try_acquire(Path::new(&root)) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("child: acquire failed: {e}");
                    std::process::exit(4);
                }
            };
            // Hold the lock file open until abort.
            std::hint::black_box(&lock);
            std::process::abort();
        }
        other => {
            eprintln!("child: unknown mode {other}");
            std::process::exit(5);
        }
    }
}

#[test]
fn lock_acquire_and_release() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if super::super::tests::support::in_any_child() {
        return;
    }
    let Some(dir) = supported_tempdir() else {
        return;
    };
    let root = dir.path().to_path_buf();

    let lock = RootLock::try_acquire(&root).expect("first acquire must succeed");
    assert_eq!(lock.canonical_root(), dir.path());
    assert!(lock.lock_path().starts_with(&root));
    assert!(lock.lock_path().ends_with("root.lock"));
    assert!(lock.lock_path().exists());

    drop(lock);
    RootLock::try_acquire(&root).expect("acquire after drop must succeed");
}

#[test]
fn lock_second_process_rejected() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if super::super::tests::support::in_any_child() {
        return;
    }
    let Some(dir) = supported_tempdir() else {
        return;
    };
    let root = dir.path().to_path_buf();

    let _lock = RootLock::try_acquire(&root).expect("parent acquire must succeed");

    let status = Command::new(std::env::current_exe().expect("current exe"))
        .env(HELPER_ENV, "1")
        .env("GRAFEOLOCK_CHILD_MODE", "second")
        .env("GRAFEOLOCK_CHILD_ROOT", &root)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("spawn child");

    assert!(
        status.success(),
        "child must observe AlreadyLocked while parent holds the lock"
    );
}

#[test]
fn lock_crash_release() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if super::super::tests::support::in_any_child() {
        return;
    }
    let Some(dir) = supported_tempdir() else {
        return;
    };
    let root = dir.path().to_path_buf();

    let status = Command::new(std::env::current_exe().expect("current exe"))
        .env(HELPER_ENV, "1")
        .env("GRAFEOLOCK_CHILD_MODE", "crash")
        .env("GRAFEOLOCK_CHILD_ROOT", &root)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("spawn child");

    // Child aborted (SIGABRT); on Unix the exit code is 134.
    assert!(!status.success(), "child must abort while holding the lock");

    // Kernel releases the flock when the child's handle closes on death.
    RootLock::try_acquire(&root).expect("acquire after child abort must succeed");
}

#[test]
fn lock_alias_rejected() {
    let Some(dir) = supported_tempdir() else {
        return;
    };
    let root = dir.path().to_path_buf();

    // Symlink alias.
    let link = dir.path().join("alias-link");
    std::os::unix::fs::symlink(&root, &link).expect("symlink");
    match RootLock::try_acquire(&link) {
        Err(RootLockError::AliasMismatch {
            canonical,
            requested,
        }) => {
            assert_eq!(canonical, root);
            assert_eq!(requested, link);
        }
        other => panic!("symlink path must be rejected, got {other:?}"),
    }

    // `..` alias.
    let dotdot = root.join("sub").join("..");
    std::fs::create_dir(root.join("sub")).expect("subdir");
    match RootLock::try_acquire(&dotdot) {
        Err(RootLockError::AliasMismatch { .. }) => {}
        other => panic!("dot-dot path must be rejected, got {other:?}"),
    }
}

#[test]
fn lock_mount_validation_allow_synthetic() {
    let text = "36 35 98:0 / /mnt/data rw,noatime - ext4 /dev/nvme0n1 rw\n\
                37 35 98:1 / /mnt/other rw - xfs /dev/nvme1n1 rw\n";
    let entries = parse_mountinfo(text).expect("parse");
    assert_eq!(
        entries,
        vec![
            MountEntry {
                mount_point: "/mnt/data".to_string(),
                fstype: "ext4".to_string(),
            },
            MountEntry {
                mount_point: "/mnt/other".to_string(),
                fstype: "xfs".to_string(),
            },
        ]
    );
    let fstype =
        filesystem_for_path(Path::new("/mnt/data/graphs/root"), &entries).expect("resolve");
    assert_eq!(fstype, "ext4");
    assert!(ALLOWED_FILESYSTEMS.contains(&fstype.as_str()));

    let fstype = filesystem_for_path(Path::new("/mnt/other/g"), &entries).expect("resolve");
    assert_eq!(fstype, "xfs");
}

#[test]
fn lock_mount_validation_reject_synthetic() {
    let text = "1 0 0:0 / / rw - tmpfs tmpfs rw\n\
                2 1 98:0 / /mnt/nfs rw - nfs4 server:/export rw\n\
                3 1 98:0 / /mnt/ovl rw - overlay overlay rw\n";
    let entries = parse_mountinfo(text).expect("parse");
    for mount in ["/", "/mnt/nfs", "/mnt/ovl"] {
        let fstype = filesystem_for_path(Path::new(mount), &entries).expect("resolve");
        assert!(
            !ALLOWED_FILESYSTEMS.contains(&fstype.as_str()),
            "{mount} fstype {fstype} must not be allowlisted"
        );
    }
}

#[test]
fn lock_mount_validation_real_allow() {
    let Some(dir) = supported_tempdir() else {
        return;
    };
    RootLock::try_acquire(dir.path()).expect("real allowlisted fs must acquire");
}

#[test]
fn lock_mount_parser_escapes() {
    // \040 = space, \011 = tab, \134 = backslash.
    let text = "1 0 0:0 / /mnt/with\\040space rw - ext4 /dev/sda1 rw\n";
    let entries = parse_mountinfo(text).expect("parse");
    assert_eq!(entries[0].mount_point, "/mnt/with space");
}

#[test]
fn lock_mount_parser_no_match_is_error() {
    let text = "1 0 0:0 / /mnt/other rw - ext4 /dev/sda1 rw\n";
    let entries = parse_mountinfo(text).expect("parse");
    assert!(filesystem_for_path(Path::new("/var/nowhere"), &entries).is_err());
}

#[test]
fn lock_cloexec_probe() {
    let Some(dir) = supported_tempdir() else {
        return;
    };
    let lock = RootLock::try_acquire(dir.path()).expect("acquire");
    let fd = probe_cloexec_for(&lock);
    assert!(
        fd,
        "root.lock handle must have O_CLOEXEC set (fdinfo flags & 0o2000000)"
    );
}

/// Reads every `/proc/self/fdinfo/<n>` entry and returns true when any open
/// handle points at `root.lock` and carries the O_CLOEXEC flag (0o2000000).
fn probe_cloexec_for(lock: &RootLock) -> bool {
    let target = lock.lock_path();
    let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
        return false;
    };
    for entry in entries.flatten() {
        let fd = entry.file_name().to_string_lossy().into_owned();
        let link = std::fs::read_link(entry.path()).unwrap_or_default();
        if link != target {
            continue;
        }
        let info = std::fs::read_to_string(format!("/proc/self/fdinfo/{fd}")).unwrap_or_default();
        let flags = info
            .lines()
            .find_map(|l| l.strip_prefix("flags:"))
            .map_or(0, |f| u64::from_str_radix(f.trim(), 8).unwrap_or(0));
        if flags & 0o2000000 != 0 {
            return true;
        }
    }
    false
}
