//! Deterministic power-loss model (G-EM0.W0-B, Module 7).
//!
//! [`DeterministicFileOps`] wraps a REAL temp directory and tracks which
//! files/directories are durable (fsync'd) versus volatile. On
//! [`inject_power_loss`](DeterministicFileOps::inject_power_loss), volatile
//! state is discarded: unsynced files are deleted, dirtied files revert to
//! their last-synced content, and unsynced renames are undone. Recovery then
//! runs against the surviving real filesystem.
//!
//! Model contract (the real fsync contract):
//!
//! - file sync (`sync_all`/`sync_path`) makes that file's current bytes
//!   durable;
//! - directory sync (`sync_dir`) makes directory entries durable — it does
//!   NOT make file bytes durable;
//! - a rename is durable only after its parent directory is synced;
//! - a write to an already-durable file without a subsequent sync may be
//!   lost (the file reverts to its last-synced content).
//!
//! The same `publish_generation` runs against this implementation and
//! [`OsGenerationFileOps`]; the crash matrix in `tests/faults_tests.rs`
//! proves the publication ordering is exactly W0 §11.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use grafeo_common::utils::error::{Error, Result};

use crate::file::generation_writer::GenerationFileOps;

/// Deterministic file operations for fault injection.
///
/// Wraps a real temp directory. Tracks sync state per file and per
/// directory. On [`inject_power_loss`](Self::inject_power_loss), deletes
/// unsynced files, restores dirtied files to their last-synced content, and
/// reverts unsynced directory entries (renames, creates).
#[derive(Debug)]
pub struct DeterministicFileOps {
    /// Root temp directory for this test.
    root: PathBuf,
    /// Sync/durability tracking (interior-mutable: the trait takes `&self`
    /// while injection needs `&mut self`; the crash matrix is single-threaded).
    state: RefCell<State>,
}

/// Sync/durability tracking state.
#[derive(Debug, Default)]
struct State {
    /// Last-synced content per durable file (the durability snapshot).
    synced_content: HashMap<PathBuf, Vec<u8>>,
    /// Files created or dirtied since their last sync (volatile).
    volatile_files: HashSet<PathBuf>,
    /// Directories whose entries are durable.
    synced_dirs: HashSet<PathBuf>,
    /// Directories created but never synced (volatile).
    volatile_dirs: HashSet<PathBuf>,
    /// Renames whose parent directory is not yet synced, as (from, to).
    volatile_renames: Vec<(PathBuf, PathBuf)>,
}

/// Move a key between two paths in a map.
fn move_key<V>(map: &mut HashMap<PathBuf, V>, from: &Path, to: &Path) {
    if let Some(v) = map.remove(from) {
        map.insert(to.to_path_buf(), v);
    }
}

impl DeterministicFileOps {
    /// Create a new deterministic ops rooted at a fresh temp directory.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            state: RefCell::new(State::default()),
        }
    }

    /// The temp root this ops instance is bound to.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Simulate power loss:
    ///
    /// 1. Revert all volatile renames (file returns to its pre-rename path).
    /// 2. Delete volatile files with no synced content; restore dirtied
    ///    files to their last-synced content.
    /// 3. Remove volatile directories that ended up empty.
    ///
    /// After this, only synced files and synced directory entries remain.
    pub fn inject_power_loss(&mut self) {
        let mut state = self.state.borrow_mut();

        // 1. Revert unsynced renames.
        let renames = std::mem::take(&mut state.volatile_renames);
        for (from, to) in renames {
            if to.exists() {
                if from.exists() {
                    let _ = std::fs::remove_file(&to);
                } else {
                    let _ = std::fs::rename(&to, &from);
                }
            }
            move_key(&mut state.synced_content, &to, &from);
            if state.volatile_files.remove(&to) {
                state.volatile_files.insert(from);
            }
        }

        // 2. Discard volatile file state.
        let volatile = std::mem::take(&mut state.volatile_files);
        for path in volatile {
            match state.synced_content.get(&path) {
                Some(bytes) => {
                    // Dirtied file: revert to its last-synced content.
                    let _ = std::fs::write(&path, bytes);
                }
                None => {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }

        // 3. Remove volatile directories that ended up empty (deepest first).
        let volatile_dirs = std::mem::take(&mut state.volatile_dirs);
        let mut dirs: Vec<PathBuf> = volatile_dirs.into_iter().collect();
        dirs.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
        for dir in dirs {
            if dir.is_dir() && std::fs::read_dir(&dir).map_or(false, |mut e| e.next().is_none()) {
                let _ = std::fs::remove_dir(&dir);
            }
        }
    }

    /// Resolve the path behind an open file handle via `/proc/self/fd`
    /// (Linux). On non-Linux platforms this fails closed with an explicit
    /// error, per the dispatch's documented limitation.
    fn path_of_file(file: &File) -> std::io::Result<PathBuf> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = file;
            Err(std::io::Error::other(
                "DeterministicFileOps sync_all requires /proc (Linux)",
            ))
        }
    }

    fn mark_file_synced(&self, path: &Path) {
        let mut state = self.state.borrow_mut();
        if let Ok(bytes) = std::fs::read(path) {
            state.synced_content.insert(path.to_path_buf(), bytes);
            state.volatile_files.remove(path);
        }
    }

    fn mark_dir_synced(&self, path: &Path) {
        let mut state = self.state.borrow_mut();
        state.synced_dirs.insert(path.to_path_buf());
        state.volatile_dirs.remove(path);
        // Promote renames whose target parent is now durable.
        let mut remaining = Vec::new();
        let renames = std::mem::take(&mut state.volatile_renames);
        for (from, to) in renames {
            let parent_synced = to
                .parent()
                .is_some_and(|p| state.synced_dirs.contains(p) || p == path);
            if parent_synced {
                move_key(&mut state.synced_content, &from, &to);
                if state.volatile_files.remove(&from) {
                    state.volatile_files.insert(to);
                }
            } else {
                remaining.push((from, to));
            }
        }
        state.volatile_renames = remaining;
    }

    fn track_rename(&self, from: &Path, to: &Path) {
        let mut state = self.state.borrow_mut();
        move_key(&mut state.synced_content, from, to);
        if state.volatile_files.remove(from) {
            state.volatile_files.insert(to.to_path_buf());
        }
        state
            .volatile_renames
            .push((from.to_path_buf(), to.to_path_buf()));
    }
}

impl GenerationFileOps for DeterministicFileOps {
    fn create_new(&self, path: &Path) -> Result<File> {
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(Error::Io)?;
        let mut state = self.state.borrow_mut();
        state.volatile_files.insert(path.to_path_buf());
        state.synced_content.remove(path);
        Ok(file)
    }

    fn sync_all(&self, file: &File) -> Result<()> {
        let path = Self::path_of_file(file).map_err(Error::Io)?;
        file.sync_all().map_err(Error::Io)?;
        self.mark_file_synced(&path);
        Ok(())
    }

    fn sync_dir(&self, path: &Path) -> Result<()> {
        let dir = File::open(path).map_err(Error::Io)?;
        dir.sync_all().map_err(Error::Io)?;
        self.mark_dir_synced(path);
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        std::fs::rename(from, to).map_err(Error::Io)?;
        self.track_rename(from, to);
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path).map_err(Error::Io)?;
        let mut state = self.state.borrow_mut();
        state.volatile_files.remove(path);
        state.synced_content.remove(path);
        state
            .volatile_renames
            .retain(|(from, to)| from != path && to != path);
        Ok(())
    }

    fn path_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn open_existing(&self, path: &Path) -> Result<File> {
        let file = File::options()
            .read(true)
            .write(true)
            .open(path)
            .map_err(Error::Io)?;
        let mut state = self.state.borrow_mut();
        // Adopt-on-open: a pre-existing file's current bytes are its
        // last-synced content (it existed before this ops instance started
        // tracking). A write through this handle is not durable until the
        // next sync, so the file is marked dirty with that snapshot.
        if !state.synced_content.contains_key(path)
            && let Ok(bytes) = std::fs::read(path)
        {
            state.synced_content.insert(path.to_path_buf(), bytes);
        }
        state.volatile_files.insert(path.to_path_buf());
        Ok(file)
    }

    fn read_to_end(&self, path: &Path) -> Result<Vec<u8>> {
        std::fs::read(path).map_err(Error::Io)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path).map_err(Error::Io)?;
        let mut state = self.state.borrow_mut();
        if !state.synced_dirs.contains(path) {
            state.volatile_dirs.insert(path.to_path_buf());
        }
        Ok(())
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(path).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        Ok(names)
    }

    fn sha256(&self, path: &Path) -> Result<[u8; 32]> {
        use sha2::Digest;
        let mut file = File::open(path).map_err(Error::Io)?;
        let mut hasher = sha2::Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf).map_err(Error::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        Ok(out)
    }

    fn file_len(&self, path: &Path) -> Result<u64> {
        std::fs::metadata(path).map_err(Error::Io).map(|m| m.len())
    }

    fn sync_path(&self, path: &Path) -> Result<()> {
        let f = File::open(path).map_err(Error::Io)?;
        f.sync_all().map_err(Error::Io)?;
        self.mark_file_synced(path);
        Ok(())
    }

    fn copy_bounded(&self, src: &Path, dst: &Path, buf_size: usize) -> Result<u64> {
        let mut reader = File::open(src).map_err(Error::Io)?;
        let mut writer = self.create_new(dst)?;
        let mut buf = vec![0u8; buf_size.max(1)];
        let mut total: u64 = 0;
        loop {
            let n = reader.read(&mut buf).map_err(Error::Io)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n]).map_err(Error::Io)?;
            // reason: bounded by source file size, fits u64
            #[allow(clippy::cast_possible_truncation)]
            {
                total += n as u64;
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
#[path = "tests/faults_tests.rs"]
mod tests;
