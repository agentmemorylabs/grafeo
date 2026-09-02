//! In-process backup-pin registry (G-EM0.4b, packet requirement 4).
//!
//! A backup pins an exact selected manifest sequence plus all referenced
//! immutable generation/WAL bytes *before* copying. The pin is an
//! in-process, ref-counted guard: GC treats a pinned generation as
//! [`RetentionClass::BackupPinned`](super::RetentionClass) and never deletes
//! it until the last guard covering it drops.

use std::collections::BTreeMap;

use parking_lot::Mutex;

/// An active backup pin: why a generation is protected from GC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupPin {
    /// Root-relative path of the pinned generation.
    pub generation_path: String,
    /// The exact manifest publication sequence pinned for backup.
    pub publication_sequence: u64,
    /// Wall-clock milliseconds (UNIX epoch) when the pin was taken.
    pub started_at_ms: u64,
}

/// The ref-counted in-process pin registry.
///
/// A path stays pinned until the last [`BackupPinGuard`] covering it drops.
/// Two concurrent backups of the same generation each hold a guard and each
/// keep it pinned.
#[derive(Debug, Default)]
pub struct PinRegistry {
    /// Path → (pin, live guard count).
    pins: Mutex<BTreeMap<String, (BackupPin, usize)>>,
}

impl PinRegistry {
    /// Pin a generation, returning an RAII guard that releases one reference
    /// on drop.
    #[must_use]
    pub fn pin(&self, generation_path: String, publication_sequence: u64) -> BackupPinGuard<'_> {
        let mut pins = self.pins.lock();
        pins.entry(generation_path.clone())
            .and_modify(|(_, count)| *count += 1)
            .or_insert_with(|| {
                (
                    BackupPin {
                        generation_path: generation_path.clone(),
                        publication_sequence,
                        started_at_ms: super::super::now_ms(),
                    },
                    1,
                )
            });
        BackupPinGuard {
            pins: &self.pins,
            generation_path,
        }
    }

    /// The currently active pins (plain data snapshot).
    #[must_use]
    pub fn active(&self) -> Vec<BackupPin> {
        self.pins
            .lock()
            .values()
            .map(|(pin, _)| pin.clone())
            .collect()
    }
}

/// An RAII backup pin. Dropping the guard releases one reference on the
/// pin; the generation becomes GC-eligible (if otherwise unreferenced) only
/// after the last guard drops.
#[derive(Debug)]
pub struct BackupPinGuard<'a> {
    /// The registry's pin map.
    pins: &'a Mutex<BTreeMap<String, (BackupPin, usize)>>,
    /// The pinned root-relative generation path.
    generation_path: String,
}

impl BackupPinGuard<'_> {
    /// The pinned root-relative generation path.
    #[must_use]
    pub fn pinned_path(&self) -> &str {
        &self.generation_path
    }
}

impl Drop for BackupPinGuard<'_> {
    fn drop(&mut self) {
        let mut pins = self.pins.lock();
        if let Some((_, count)) = pins.get_mut(&self.generation_path) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.generation_path);
            }
        }
    }
}
