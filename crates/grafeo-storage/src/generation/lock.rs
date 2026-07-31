//! Root-bound exclusive process ownership (G-EM0.W0-B, Module 1).
//!
//! [`RootLock`] provides Option-S process ownership of a writable generation
//! root: exactly one OS process may hold the lock for one canonical root.
//! The lock is an exclusive kernel lock on `<root>/root.lock`, released by
//! handle close (drop or process exit). There is no PID file, no lease
//! timeout, and no timestamp fencing.
//!
//! Fail-closed guarantees:
//!
//! - the root path must be canonical (no symlinks, no `..`, no aliases);
//! - the filesystem type must be in the local-durable allowlist (Linux:
//!   ext2/ext3/ext4/xfs/btrfs/zfs), proven by parsing `/proc/self/mountinfo`
//!   with longest mount-point match;
//! - a second process attempting acquisition while the owner is alive gets
//!   [`RootLockError::AlreadyLocked`].

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;

/// Errors from root lock acquisition.
#[derive(Debug, thiserror::Error)]
pub enum RootLockError {
    /// Another process holds the exclusive lock on this root.
    #[error("root already locked by another process")]
    AlreadyLocked,
    /// The caller-supplied path is not canonical (symlink, `..`, alias).
    #[error("alias/symlink mismatch: canonical={canonical}, requested={requested}")]
    AliasMismatch {
        /// Canonical form of the root.
        canonical: PathBuf,
        /// Path the caller requested.
        requested: PathBuf,
    },
    /// The containing filesystem is not in the durable-local allowlist.
    #[error("unsupported filesystem: {0}")]
    UnsupportedFilesystem(String),
    /// The mount table could not be read or parsed.
    #[error("mount table unreadable or unparseable")]
    MountTableError,
    /// Underlying I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Filesystem types accepted for a durable generation root on Linux.
const ALLOWED_FILESYSTEMS: &[&str] = &["ext2", "ext3", "ext4", "xfs", "btrfs", "zfs"];

/// One parsed mount entry from `/proc/self/mountinfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountEntry {
    /// Decoded mount point (absolute path).
    pub mount_point: String,
    /// Filesystem type (e.g. `ext4`, `tmpfs`, `overlay`).
    pub fstype: String,
}

/// Exclusive process ownership of a writable generation root.
///
/// Acquires an exclusive kernel lock on `<root>/root.lock`.
/// The lock is released when this value is dropped (handle close).
/// No PID file, no lease timeout, no timestamp fencing.
#[derive(Debug)]
pub struct RootLock {
    canonical_root: PathBuf,
    lock_path: PathBuf,
    /// Held open for the lifetime of the lock; close = release.
    _file: File,
}

impl RootLock {
    /// Attempt exclusive acquisition. Fails closed on:
    ///
    /// - root path is not canonical (symlink/alias);
    /// - filesystem type not in the allowlist;
    /// - another process holds the lock.
    ///
    /// # Errors
    ///
    /// Returns [`RootLockError`] for every rejection branch.
    pub fn try_acquire(root: &Path) -> std::result::Result<Self, RootLockError> {
        let canonical = std::fs::canonicalize(root)?;
        if canonical != root {
            return Err(RootLockError::AliasMismatch {
                canonical,
                requested: root.to_path_buf(),
            });
        }

        // Reject unsupported filesystems before creating anything.
        validate_filesystem(&canonical)?;

        let lock_path = canonical.join("root.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        // fs2 0.4: try_lock_exclusive errors with WouldBlock on contention.
        match file.try_lock_exclusive() {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Err(RootLockError::AlreadyLocked);
            }
            Err(e) => return Err(RootLockError::Io(e)),
        }

        set_close_on_exec(&file)?;

        Ok(Self {
            canonical_root: canonical,
            lock_path,
            _file: file,
        })
    }

    /// The canonical root path this lock is bound to.
    #[must_use]
    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }

    /// The lock file path (`<root>/root.lock`).
    #[must_use]
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
}

/// Validate that the canonical root lives on a supported durable local
/// filesystem. Linux parses `/proc/self/mountinfo`; Windows and other
/// platforms fail closed (the Windows volume-query route is documented in
/// the W0 contract §9 and is exercised by the parent's Windows runner).
fn validate_filesystem(canonical: &Path) -> std::result::Result<(), RootLockError> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/self/mountinfo")
            .map_err(|_| RootLockError::MountTableError)?;
        let entries = parse_mountinfo(&text)?;
        let fstype = filesystem_for_path(canonical, &entries)?;
        if ALLOWED_FILESYSTEMS.contains(&fstype.as_str()) {
            Ok(())
        } else {
            Err(RootLockError::UnsupportedFilesystem(fstype))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Windows CI route (GetVolumeInformationW, NTFS/ReFS allowlist) is
        // parent-owned per W0 §17. Fail closed on every other platform.
        #[cfg(target_os = "windows")]
        {
            Err(RootLockError::UnsupportedFilesystem(
                "windows volume validation not wired in this implementation leaf".to_string(),
            ))
        }
        #[cfg(not(target_os = "windows"))]
        {
            Err(RootLockError::UnsupportedFilesystem(
                "filesystem validation only implemented on Linux".to_string(),
            ))
        }
    }
}

/// Parse `/proc/self/mountinfo` text into mount entries.
///
/// Line grammar: `mount_id parent_id major:minor root mount_point options
/// [optional...] - fstype source super_options`. The mount point is
/// space-separated and hex-escaped (`\040` space, `\011` tab, `\012` newline,
/// `\134` backslash).
///
/// # Errors
///
/// Returns [`RootLockError::MountTableError`] on any unparseable line or if
/// no separator is found.
pub fn parse_mountinfo(text: &str) -> std::result::Result<Vec<MountEntry>, RootLockError> {
    let mut entries = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (left, right) = line
            .split_once(" - ")
            .ok_or(RootLockError::MountTableError)?;
        let mut left_fields = left.split_whitespace();
        // Fields: 0=mount_id, 1=parent_id, 2=major:minor, 3=root, 4=mount_point.
        let mount_point_escaped = left_fields.nth(4).ok_or(RootLockError::MountTableError)?;
        let fstype = right
            .split_whitespace()
            .next()
            .ok_or(RootLockError::MountTableError)?;
        entries.push(MountEntry {
            mount_point: decode_mountinfo_escapes(mount_point_escaped),
            fstype: fstype.to_string(),
        });
    }
    Ok(entries)
}

/// Decode hex escapes in a mountinfo mount-point field.
fn decode_mountinfo_escapes(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            let hex = &s[i + 1..i + 4];
            if let Ok(code) = u8::from_str_radix(hex, 8) {
                out.push(code);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Resolve the filesystem type for a canonical path using longest
/// mount-point match over the parsed mount table.
///
/// # Errors
///
/// Returns [`RootLockError::MountTableError`] when no mount point matches.
pub fn filesystem_for_path(
    canonical: &Path,
    entries: &[MountEntry],
) -> std::result::Result<String, RootLockError> {
    let path_str = canonical.to_string_lossy();
    let mut best: Option<(&MountEntry, usize)> = None;
    for entry in entries {
        let mp = entry.mount_point.as_str();
        if mp == path_str
            || (path_str.starts_with(mp) && path_str.as_bytes().get(mp.len()) == Some(&b'/'))
        {
            let len = mp.len();
            if best.is_none_or(|(_, best_len)| len > best_len) {
                best = Some((entry, len));
            }
        }
    }
    best.map(|(entry, _)| entry.fstype.clone())
        .ok_or(RootLockError::MountTableError)
}

/// Set close-on-exec on the lock file handle.
///
/// On Unix this issues `fcntl(F_SETFD, FD_CLOEXEC)`; on other platforms the
/// standard library defaults are relied on (documented limitation).
#[cfg(unix)]
fn set_close_on_exec(file: &File) -> std::result::Result<(), RootLockError> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is a valid open file descriptor owned by `file`, which is
    // alive for the duration of this call. fcntl(F_SETFD) does not close it.
    #[allow(unsafe_code)]
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    if rc == -1 {
        return Err(RootLockError::Io(io::Error::last_os_error()));
    }
    Ok(())
}

/// Non-Unix: no explicit close-on-exec wiring (std handles are non-inheritable
/// on Windows by default; documented in the W0 contract §9).
#[cfg(not(unix))]
fn set_close_on_exec(_file: &File) -> std::result::Result<(), RootLockError> {
    Ok(())
}

#[cfg(test)]
#[path = "tests/lock_tests.rs"]
mod tests;
