//! Backup of a generation root that a live `GrafeoDB` is serving.
//!
//! The offline [`super::retirement::backup_generation_root`] takes the root's
//! [`RootOwnership`](super::RootOwnership), which holds the exclusive root
//! lock, so it cannot be used while a database has the root open. This
//! module adds [`GrafeoDB::backup_generation_root`], which runs against the
//! database's own ownership and retirement authority and never re-acquires
//! the lock.
//!
//! # Consistency point
//!
//! The backup is the selected generation, the manifest slot that selects it,
//! and the WAL files from the slot's replay boundary onward, cut at a single
//! point:
//!
//! 1. A handoff gate is taken. While it is held no epoch handoff can start
//!    (a freeze is refused), so the manifest cannot be rewritten and the WAL
//!    cannot be truncated or rotated by publication. A handoff that is
//!    already running makes the backup refuse with
//!    [`RetirementError::HandoffInProgress`] before anything is read.
//! 2. The selected slot is read and its generation pinned (GC cannot delete
//!    it) and re-validated against the slot's length and SHA-256.
//! 3. The live WAL is flushed and fsynced **under its own append lock**, which
//!    also reports the active file's sequence and length. This is **the
//!    cut**: every commit that returned before the cut is inside it, and the
//!    reported length is on an append-group boundary. Every lower sequence is
//!    already final (rotation swaps the file under the same lock). The cut is
//!    taken at or after the call, after the generation hash check.
//! 4. The `wal_*.log` files from the boundary through the active sequence are
//!    copied, the active one only up to the reported length. Files created by
//!    a rotation after the cut are ignored.
//! 5. The staged WAL is replay-validated from the manifest boundary before
//!    the backup is published, so a bad cut is refused at backup time.
//!
//! Writers are never blocked. A commit that is in flight during the cut may
//! or may not be included, and the last file can end inside a frame or an
//! unfinished transaction. That is the same state a crash leaves behind, and
//! replay on open already discards it (torn tail, uncommitted records), so a
//! restored root contains exactly the transactions whose commit marker is
//! inside the cut.

use std::path::Path;

use crate::database::GrafeoDB;

use super::retirement::{
    GenerationBackupReceipt, RetirementAuthority, RetirementError, backup_live_generation_root,
};

impl GrafeoDB {
    /// Back up the generation root this database has open, while it keeps
    /// serving reads and writes.
    ///
    /// `destination_dir` must be outside the root and `backup_name` a single
    /// safe path component; the backup is published atomically as
    /// `<destination_dir>/<backup_name>` and an existing backup is never
    /// overwritten. Restore it with
    /// [`restore_generation_root`](super::retirement::restore_generation_root).
    ///
    /// The consistency point is described in the module docs: all commits
    /// acknowledged before this call are in the backup, and nothing partial
    /// is. The call copies the whole generation file, so it takes time
    /// proportional to the database size, but it does not block writers.
    ///
    /// Epoch handoff (publication) and a backup exclude each other. If a
    /// handoff is running, this returns [`RetirementError::HandoffInProgress`]
    /// without copying anything; while a backup runs,
    /// [`GrafeoDB::freeze_epoch_for_handoff`] returns an error and can be
    /// retried afterwards. Both are clean refusals, never a torn backup or a
    /// torn publication. (The check briefly waits on the handoff slot lock
    /// while a freeze is capturing; the wait is bounded by the capture.)
    ///
    /// A poisoned WAL is refused with [`RetirementError::WalPoisoned`].
    ///
    /// `destination_dir` must not be shared between machines or PID
    /// namespaces: a backup removes staging directories whose owning process
    /// id is not alive locally, which could hit another host's live one.
    ///
    /// `wal_checkpoint` on a generation root only syncs the WAL (see #23), so
    /// it cannot shorten a backup's WAL. Without that change, a checkpoint
    /// that deleted old log files during a backup would make the backup fail
    /// (missing file) rather than publish a hole.
    ///
    /// Live-root GC must be run through
    /// [`GrafeoDB::retirement_authority`] so that it sees this backup's pin.
    ///
    /// # Errors
    ///
    /// [`RetirementError::NotGenerationRoot`] when the database is not an
    /// open generation root, [`RetirementError::HandoffInProgress`] as above,
    /// and the errors of
    /// [`backup_generation_root`](super::retirement::backup_generation_root)
    /// otherwise.
    pub fn backup_generation_root(
        &self,
        destination_dir: impl AsRef<Path>,
        backup_name: &str,
    ) -> Result<GenerationBackupReceipt, RetirementError> {
        let root = self
            .generation_root
            .as_ref()
            .ok_or(RetirementError::NotGenerationRoot)?;
        // Held through the backup, so `close()` waits for it; refused once
        // the database was closed (AMH #176).
        let _held = root.ownership().lock().hold()?;
        let _gate = self
            .epoch_handoff
            .begin_backup()
            .map_err(|phase| RetirementError::HandoffInProgress(phase.name()))?;

        let take_wal_cut = || -> Result<Option<(u64, u64)>, RetirementError> {
            let Some(wal) = self.wal.as_ref() else {
                return Ok(None);
            };
            if let Some(reason) = wal.poisoned_reason() {
                return Err(RetirementError::WalPoisoned(reason));
            }
            Ok(Some(wal.manager().flush_for_cut()?))
        };
        backup_live_generation_root(
            root.retirement(),
            root.ownership(),
            destination_dir.as_ref(),
            backup_name,
            &take_wal_cut,
        )
    }

    /// The retirement authority of the open generation root, or `None` when
    /// the database is not a generation root or was closed (its root lock is
    /// released, so it no longer owns the root; AMH #176). Run live-root GC
    /// through this so backups taken by the database keep their generation
    /// pinned.
    #[must_use]
    pub fn retirement_authority(&self) -> Option<&RetirementAuthority> {
        self.generation_root
            .as_ref()
            .filter(|r| r.ownership().lock().is_held())
            .map(|r| r.retirement())
    }
}
