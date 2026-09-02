//! Dual-slot manifest tests: round trip, independence, torn-slot
//! preservation, typed errors, non-canonical rejection, selection order.

use std::fs::File;
use std::io::{Seek, Write as _};

use super::{
    MANIFEST_SIZE, ManifestError, ManifestSlot, create_manifest, inactive_slot_index,
    read_manifest, write_slot,
};
use tempfile::TempDir;

fn fixture_slot(seq: u64) -> ManifestSlot {
    ManifestSlot {
        publication_sequence: seq,
        parent_publication_sequence: seq.saturating_sub(1),
        overlay_epoch: 7,
        transaction_id: 42,
        wal_log_sequence: 3,
        wal_byte_offset: 0,
        generation_length: 12345,
        node_count: 100,
        edge_count: 250,
        outer_container_format_version: 2,
        compact_store_format_version: 5,
        generation_sha256: [0xAB; 32],
        generation_id: format!("g-{seq:020}"),
        parent_generation_id: if seq == 1 {
            String::new()
        } else {
            format!("g-{:020}", seq - 1)
        },
        generation_path: format!("generations/g-{seq:020}-abcdef1234567890.grafeo"),
    }
}

#[test]
fn manifest_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    create_manifest(&path).unwrap();

    let slot = fixture_slot(1);
    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    write_slot(&mut file, 0, &slot).unwrap();
    drop(file);

    let (index, decoded) = read_manifest(&path).unwrap();
    assert_eq!(index, 0);
    assert_eq!(decoded, slot);
}

#[test]
fn manifest_both_slots_independent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    create_manifest(&path).unwrap();

    let slot0 = fixture_slot(1);
    let mut slot1 = fixture_slot(2);
    slot1.generation_id = "g-second".to_string();
    slot1.generation_path = "generations/g-00000000000000000002-beef.grafeo".to_string();

    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    write_slot(&mut file, 0, &slot0).unwrap();
    write_slot(&mut file, 1, &slot1).unwrap();
    drop(file);

    let (index, decoded) = read_manifest(&path).unwrap();
    assert_eq!(index, 1, "highest sequence must be selected");
    assert_eq!(decoded, slot1);
    assert_ne!(decoded, slot0);
}

#[test]
fn manifest_torn_slot_preserves_other() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    create_manifest(&path).unwrap();

    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    write_slot(&mut file, 0, &fixture_slot(1)).unwrap();
    // Write slot 1 then corrupt its CRC byte.
    write_slot(&mut file, 1, &fixture_slot(2)).unwrap();
    file.seek(std::io::SeekFrom::Start(4096 + 4092)).unwrap();
    file.write_all(&[0x00]).unwrap();
    drop(file);

    let (index, decoded) = read_manifest(&path).unwrap();
    assert_eq!(index, 0, "torn slot 1 must not block slot 0");
    assert_eq!(decoded.publication_sequence, 1);
}

#[test]
fn manifest_both_invalid_typed_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    create_manifest(&path).unwrap();

    // Corrupt the magic of both slots.
    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    file.write_all_at(0, b"XXXX").unwrap();
    file.write_all_at(4096, b"YYYY").unwrap();
    drop(file);

    match read_manifest(&path) {
        Err(ManifestError::BothInvalid {
            slot0_cause,
            slot1_cause,
        }) => {
            assert!(slot0_cause.contains("bad magic"), "cause: {slot0_cause}");
            assert!(slot1_cause.contains("bad magic"), "cause: {slot1_cause}");
        }
        other => panic!("expected BothInvalid, got {other:?}"),
    }
}

#[test]
fn manifest_noncanonical_rejected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    create_manifest(&path).unwrap();

    // 1. Oversized generation_id length field (130 > 128).
    let mut bad = fixture_slot(1);
    bad.generation_id = "x".repeat(130);
    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    assert!(matches!(
        write_slot(&mut file, 0, &bad),
        Err(ManifestError::NonCanonical { .. })
    ));
    drop(file);

    // 2. Absolute generation path.
    let mut bad = fixture_slot(1);
    bad.generation_path = "/etc/passwd".to_string();
    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    assert!(matches!(
        write_slot(&mut file, 0, &bad),
        Err(ManifestError::NonCanonical { .. })
    ));
    drop(file);

    // 3. Traversal in generation path.
    let mut bad = fixture_slot(1);
    bad.generation_path = "generations/../root.grafeo".to_string();
    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    assert!(matches!(
        write_slot(&mut file, 0, &bad),
        Err(ManifestError::NonCanonical { .. })
    ));
    drop(file);
}

#[test]
fn manifest_selection_highest_sequence() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    create_manifest(&path).unwrap();

    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    write_slot(&mut file, 0, &fixture_slot(5)).unwrap();
    write_slot(&mut file, 1, &fixture_slot(6)).unwrap();
    drop(file);

    let (index, decoded) = read_manifest(&path).unwrap();
    assert_eq!(index, 1);
    assert_eq!(decoded.publication_sequence, 6);
}

#[test]
fn manifest_inactive_slot_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    create_manifest(&path).unwrap();

    // Fresh manifest: both invalid → genesis writes slot 0.
    assert_eq!(inactive_slot_index(&path).unwrap(), 0);

    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    write_slot(&mut file, 0, &fixture_slot(1)).unwrap();
    drop(file);
    // Only slot 0 valid → slot 1 is inactive.
    assert_eq!(inactive_slot_index(&path).unwrap(), 1);

    let mut file = File::options().read(true).write(true).open(&path).unwrap();
    write_slot(&mut file, 1, &fixture_slot(2)).unwrap();
    drop(file);
    // Both valid, slot 1 higher → slot 0 inactive.
    assert_eq!(inactive_slot_index(&path).unwrap(), 0);
}

#[test]
fn manifest_truncated_file_is_both_invalid() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.bin");
    std::fs::write(&path, [0u8; 100]).unwrap();
    match read_manifest(&path) {
        Err(ManifestError::BothInvalid { .. }) => {}
        other => panic!("expected BothInvalid, got {other:?}"),
    }
}

/// Small helper: write_all_at like std's unstable API, via seek+write.
trait WriteAllAt {
    fn write_all_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()>;
}

impl WriteAllAt for File {
    fn write_all_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.seek(std::io::SeekFrom::Start(offset))?;
        self.write_all(data)
    }
}

#[allow(dead_code)]
fn _assert_manifest_size() {
    // Guards against accidental layout drift.
    assert_eq!(MANIFEST_SIZE, 8192);
}
