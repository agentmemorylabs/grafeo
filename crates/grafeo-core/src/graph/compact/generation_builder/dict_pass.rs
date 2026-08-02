//! Dictionary-pass helpers extracted from the orchestrator (R4 LOC split).
//!
//! Contains the string-occurrence collection, remap-run consumption, and
//! per-column DictChunkStreamer — all bounded, disk-backed dictionary passes.

use crate::graph::compact::generation::emit::global_dict::{StringUseKind, occurrence_record};
use crate::graph::compact::generation::{
    CancelToken, GenerationBudget, GenerationError, GenerationMetrics, RunSetLease,
};
use crate::graph::compact::generation_builder::column_pass::ColumnGeometry;
use crate::graph::compact::generation_builder::edge_pass::RelTableKey;
use crate::graph::compact::generation_builder::node_pass::NodeSchema;
use crate::graph::compact::generation_builder::staging;
use crate::graph::compact::mapped::SegmentKind;
use grafeo_common::utils::hash::FxHashMap;
use std::path::PathBuf;

/// Collects string occurrences from the node schema, rel keys, occurrence run,
/// and membership runs (for multi-label nodes).
#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_string_occurrences(
    node_schema: &NodeSchema,
    rel_keys: &[RelTableKey],
    occ_lease: &RunSetLease,
    membership_runs: Option<&RunSetLease>,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    str_occ_sink: &mut dyn crate::graph::compact::generation::ExternalRunSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(), GenerationError> {
    // Node labels (physical labels from schema).
    for label in &node_schema.labels {
        str_occ_sink.push(occurrence_record(
            label.as_bytes(),
            StringUseKind::Label,
            &[],
        )?)?;
    }
    // Edge types.
    for k in rel_keys {
        str_occ_sink.push(occurrence_record(
            k.edge_type.as_bytes(),
            StringUseKind::EdgeType,
            &[],
        )?)?;
    }
    // Membership labels (logical labels from multi-label nodes).
    if let Some(membership_lease) = membership_runs {
        merger.merge_all(
            &membership_lease.handles,
            budget,
            metrics,
            cancel,
            &mut |rec| {
                let labels = staging::decode_labels(&rec.payload)?;
                for label in labels {
                    str_occ_sink.push(occurrence_record(
                        label.as_bytes(),
                        StringUseKind::Label,
                        &[],
                    )?)?;
                }
                Ok(())
            },
        )?;
    }
    // Prop keys + string values from the occurrence run.
    merger.merge_all(&occ_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (tid, prop, _off) = split_occ_key(&rec.key)?;
        str_occ_sink.push(occurrence_record(
            prop.as_bytes(),
            StringUseKind::PropertyKey,
            &[],
        )?)?;
        // String values. The DictValue owner_key carries the column
        // identity (`table_id u16 BE || prop_key`), so the dictionary
        // pass's remap stream groups each column's strings together for
        // the per-column chunk map (D0.8.3).
        if !rec.payload.is_empty() && rec.payload[0] == 3 {
            let b = rec
                .payload
                .get(1..)
                .ok_or_else(|| GenerationError::Codec("str occ".into()))?;
            if b.len() >= 4 {
                let len = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                if let Some(s) = b.get(4..4 + len) {
                    let prop_b = prop.as_bytes();
                    let prop_len = u16::try_from(prop_b.len()).map_err(|_| {
                        GenerationError::WireWidthOverflow {
                            what: "dict_value_owner_prop_len",
                            count: prop_b.len() as u64,
                            max: u64::from(u16::MAX),
                        }
                    })?;
                    let mut owner = Vec::with_capacity(4 + prop_b.len());
                    owner.extend_from_slice(&tid.to_be_bytes());
                    owner.extend_from_slice(&prop_len.to_be_bytes());
                    owner.extend_from_slice(prop_b);
                    str_occ_sink.push(occurrence_record(s, StringUseKind::DictValue, &owner)?)?;
                }
            }
        }
        Ok(())
    })?;
    Ok(())
}

/// Consumes the global-dictionary remap run (bounded, D0.8.3).
///
/// The dictionary pass re-emitted every string occurrence as a remap record:
/// key = `use_kind u8 || owner_key`, payload = `str_len u32 LE || string ||
/// code u32 LE`. One streaming merge produces:
///
/// 1. The **schema-scoped** string→code map — labels, property keys, edge
///    types (and zone strings). Bounded by schema (column count), never by
///    the number of dictionary values.
/// 2. A disk-backed **per-column `.dict` catalog** for `DictValue` records:
///    one seek/mmap chunk per column (in `(table_id, prop_key)` order). The
///    column-body and zone-map passes open one chunk at a time in lockstep
///    with the occurrence run — only the offset table is resident, never a
///    `HashMap<String, u32>`.
///
/// Catalog layout (LE, repeated until EOF):
/// `[tid u16][prop_len u16][prop][path_len u16][relative .dict path]`
///
/// # Errors
///
/// Codec, I/O, or budget failure.
#[allow(clippy::too_many_arguments)]
pub(crate) fn consume_remap_run(
    remap_lease: &RunSetLease,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
    temp_dir: &std::path::Path,
    catalog_path: &std::path::Path,
) -> Result<FxHashMap<String, u32>, GenerationError> {
    use crate::graph::compact::generation::emit::global_dict::StringUseKind;
    use std::io::Write;

    let mut schema: FxHashMap<String, u32> = FxHashMap::default();
    let file = std::fs::File::create(catalog_path).map_err(|e| {
        GenerationError::Io(format!(
            "create dict catalog {}: {e}",
            catalog_path.display()
        ))
    })?;
    let mut catalog = std::io::BufWriter::with_capacity(64 * 1024, file);

    let mut current: Option<DictChunkStreamer> = None;

    merger.merge_all(&remap_lease.handles, budget, metrics, cancel, &mut |rec| {
        let Some(&kind) = rec.key.first() else {
            return Err(GenerationError::Codec("empty remap key".into()));
        };
        match kind {
            k if k == StringUseKind::DictValue as u8 => {
                // key = use_kind || tid u16 BE || prop_len u16 BE || prop || string
                // payload = code u32 LE
                if rec.key.len() < 5 {
                    return Err(GenerationError::Codec(
                        "dict remap owner key too short".into(),
                    ));
                }
                if rec.payload.len() != 4 {
                    return Err(GenerationError::Codec(
                        "dict remap code payload width".into(),
                    ));
                }
                let tid = u16::from_be_bytes([rec.key[1], rec.key[2]]);
                let prop_len = u16::from_be_bytes([rec.key[3], rec.key[4]]) as usize;
                if rec.key.len() < 5 + prop_len {
                    return Err(GenerationError::Codec("dict remap prop truncated".into()));
                }
                let prop = &rec.key[5..5 + prop_len];
                let string = &rec.key[5 + prop_len..];
                let code = u32::from_le_bytes(rec.payload[0..4].try_into().unwrap());
                let is_new_col = current
                    .as_ref()
                    .is_none_or(|c| c.tid != tid || c.prop.as_slice() != prop);
                if is_new_col {
                    if let Some(prev) = current.take() {
                        prev.finish_into(&mut catalog, temp_dir)?;
                    }
                    current = Some(DictChunkStreamer::open(tid, prop, temp_dir)?);
                }
                current
                    .as_mut()
                    .expect("just opened")
                    .push_unique(string, code)?;
                Ok(())
            }
            k if k == StringUseKind::Label as u8
                || k == StringUseKind::PropertyKey as u8
                || k == StringUseKind::EdgeType as u8
                || k == StringUseKind::ZoneString as u8 =>
            {
                if rec.payload.len() < 8 {
                    return Err(GenerationError::Codec("remap payload too short".into()));
                }
                let slen = u32::from_le_bytes(rec.payload[0..4].try_into().unwrap()) as usize;
                let string = rec
                    .payload
                    .get(4..4 + slen)
                    .ok_or_else(|| GenerationError::Codec("remap string truncated".into()))?;
                let code_end = 4 + slen;
                if code_end + 4 != rec.payload.len() {
                    return Err(GenerationError::Codec(
                        "remap payload trailing bytes".into(),
                    ));
                }
                let code =
                    u32::from_le_bytes(rec.payload[code_end..code_end + 4].try_into().unwrap());
                let s = std::str::from_utf8(string)
                    .map_err(|_| GenerationError::Codec("remap string not UTF-8".into()))?;
                schema.insert(s.to_string(), code);
                Ok(())
            }
            other => Err(GenerationError::Codec(format!(
                "bad remap use_kind {other}"
            ))),
        }
    })?;
    if let Some(prev) = current.take() {
        prev.finish_into(&mut catalog, temp_dir)?;
    }
    catalog
        .flush()
        .map_err(|e| GenerationError::Io(format!("flush dict catalog: {e}")))?;
    Ok(schema)
}

/// Streams one Dict column's distinct (string, code) pairs to a body spool,
/// retaining only the last string for adjacent dedup.
struct DictChunkStreamer {
    tid: u16,
    prop: Vec<u8>,
    dict_name: String,
    body_path: PathBuf,
    offsets_path: PathBuf,
    body: Option<std::io::BufWriter<std::fs::File>>,
    offsets: Option<std::io::BufWriter<std::fs::File>>,
    body_bytes: u64,
    count: u32,
    last: Option<Vec<u8>>,
}

impl DictChunkStreamer {
    fn open(tid: u16, prop: &[u8], temp_dir: &std::path::Path) -> Result<Self, GenerationError> {
        let hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            prop.hash(&mut h);
            h.finish()
        };
        let dict_name = format!("dictchunk-{tid}-{hash}.dict");
        let body_path = temp_dir.join(format!("dictchunk-{tid}-{hash}.body"));
        let offsets_path = temp_dir.join(format!("dictchunk-{tid}-{hash}.off"));
        let body_file = std::fs::File::create(&body_path).map_err(|e| {
            GenerationError::Io(format!(
                "create dict chunk body {}: {e}",
                body_path.display()
            ))
        })?;
        let off_file = std::fs::File::create(&offsets_path).map_err(|e| {
            GenerationError::Io(format!(
                "create dict chunk offsets {}: {e}",
                offsets_path.display()
            ))
        })?;
        Ok(Self {
            tid,
            prop: prop.to_vec(),
            dict_name,
            body_path,
            offsets_path,
            body: Some(std::io::BufWriter::with_capacity(64 * 1024, body_file)),
            offsets: Some(std::io::BufWriter::with_capacity(64 * 1024, off_file)),
            body_bytes: 0,
            count: 0,
            last: None,
        })
    }

    fn push_unique(&mut self, string: &[u8], code: u32) -> Result<(), GenerationError> {
        use std::io::Write;
        if self.last.as_deref() == Some(string) {
            return Ok(());
        }
        let slen = u32::try_from(string.len()).map_err(|_| GenerationError::WireWidthOverflow {
            what: "dict_chunk_str_len",
            count: string.len() as u64,
            max: u64::from(u32::MAX),
        })?;
        {
            let offsets = self
                .offsets
                .as_mut()
                .ok_or_else(|| GenerationError::Io("dict chunk offsets closed".into()))?;
            offsets
                .write_all(&self.body_bytes.to_le_bytes())
                .map_err(|e| GenerationError::Io(format!("write dict chunk offset: {e}")))?;
        }
        let body = self
            .body
            .as_mut()
            .ok_or_else(|| GenerationError::Io("dict chunk body closed".into()))?;
        body.write_all(&slen.to_le_bytes())
            .and_then(|_| body.write_all(string))
            .and_then(|_| body.write_all(&code.to_le_bytes()))
            .map_err(|e| GenerationError::Io(format!("write dict chunk entry: {e}")))?;
        self.body_bytes = self.body_bytes.checked_add(4 + u64::from(slen) + 4).ok_or(
            GenerationError::WireWidthOverflow {
                what: "dict_chunk_body_bytes",
                count: u64::MAX,
                max: u64::MAX,
            },
        )?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or(GenerationError::WireWidthOverflow {
                what: "dict_chunk_entry_count",
                count: u64::from(u32::MAX) + 1,
                max: u64::from(u32::MAX),
            })?;
        self.last = Some(string.to_vec());
        Ok(())
    }

    fn finish_into(
        mut self,
        catalog: &mut std::io::BufWriter<std::fs::File>,
        temp_dir: &std::path::Path,
    ) -> Result<(), GenerationError> {
        use std::io::{Read, Write};
        if let Some(mut body) = self.body.take() {
            body.flush()
                .map_err(|e| GenerationError::Io(format!("flush dict chunk body: {e}")))?;
        }
        if let Some(mut offsets) = self.offsets.take() {
            offsets
                .flush()
                .map_err(|e| GenerationError::Io(format!("flush dict chunk offsets: {e}")))?;
        }
        let prop_len =
            u16::try_from(self.prop.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "dict_chunk_prop_len",
                count: self.prop.len() as u64,
                max: u64::from(u16::MAX),
            })?;
        let dict_path = temp_dir.join(&self.dict_name);
        let mut out = std::fs::File::create(&dict_path).map_err(|e| {
            GenerationError::Io(format!("create dict chunk {}: {e}", dict_path.display()))
        })?;
        out.write_all(&self.count.to_le_bytes())
            .map_err(|e| GenerationError::Io(format!("write dict count: {e}")))?;
        {
            let offsets_path = std::mem::take(&mut self.offsets_path);
            let mut offsets = std::fs::File::open(&offsets_path).map_err(|e| {
                GenerationError::Io(format!(
                    "reopen dict chunk offsets {}: {e}",
                    offsets_path.display()
                ))
            })?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = offsets
                    .read(&mut buf)
                    .map_err(|e| GenerationError::Io(format!("read dict chunk offsets: {e}")))?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n])
                    .map_err(|e| GenerationError::Io(format!("copy dict chunk offsets: {e}")))?;
            }
            let _ = std::fs::remove_file(&offsets_path);
        }
        {
            let body_path = std::mem::take(&mut self.body_path);
            let mut body = std::fs::File::open(&body_path).map_err(|e| {
                GenerationError::Io(format!(
                    "reopen dict chunk body {}: {e}",
                    body_path.display()
                ))
            })?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = body
                    .read(&mut buf)
                    .map_err(|e| GenerationError::Io(format!("read dict chunk body: {e}")))?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n])
                    .map_err(|e| GenerationError::Io(format!("copy dict chunk body: {e}")))?;
            }
            let _ = std::fs::remove_file(&body_path);
        }

        let rel = self.dict_name.as_bytes();
        let rlen = u16::try_from(rel.len()).map_err(|_| GenerationError::WireWidthOverflow {
            what: "dict_catalog_path_len",
            count: rel.len() as u64,
            max: u64::from(u16::MAX),
        })?;
        catalog
            .write_all(&self.tid.to_le_bytes())
            .and_then(|_| catalog.write_all(&prop_len.to_le_bytes()))
            .and_then(|_| catalog.write_all(&self.prop))
            .and_then(|_| catalog.write_all(&rlen.to_le_bytes()))
            .and_then(|_| catalog.write_all(rel))
            .map_err(|e| GenerationError::Io(format!("write dict catalog entry: {e}")))?;
        Ok(())
    }
}

impl Drop for DictChunkStreamer {
    fn drop(&mut self) {
        self.body.take();
        self.offsets.take();
        if !self.body_path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.body_path);
        }
        if !self.offsets_path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.offsets_path);
        }
    }
}

pub(crate) fn split_occ_key(key: &[u8]) -> Result<(u16, &str, u64), GenerationError> {
    if key.len() < 10 {
        return Err(GenerationError::Codec("occ key short".into()));
    }
    let tid = u16::from_be_bytes([key[0], key[1]]);
    let off_start = key.len() - 8;
    let prop = std::str::from_utf8(&key[2..off_start])
        .map_err(|_| GenerationError::Codec("occ key utf8".into()))?;
    let off = u64::from_be_bytes(key[off_start..].try_into().unwrap());
    Ok((tid, prop, off))
}

pub(crate) fn build_node_col_keys(
    geometries: &[ColumnGeometry],
    ntables: usize,
) -> Vec<Vec<String>> {
    let mut keys = vec![Vec::new(); ntables];
    for g in geometries {
        if (g.table_id as usize) < ntables {
            keys[g.table_id as usize].push(g.key.clone());
        }
    }
    for k in &mut keys {
        k.sort();
    }
    keys
}

pub(crate) fn build_rel_col_keys(geometries: &[ColumnGeometry], nrels: usize) -> Vec<Vec<String>> {
    let mut keys = vec![Vec::new(); nrels];
    for g in geometries {
        if g.table_id >= 0x8000 {
            let rid = (g.table_id - 0x8000) as usize;
            if rid < nrels {
                keys[rid].push(g.key.clone());
            }
        }
    }
    for k in &mut keys {
        k.sort();
    }
    keys
}

pub(crate) fn make_resident_desc(
    kind: SegmentKind,
    alignment: u16,
    element_width: u32,
    bytes: &[u8],
) -> crate::graph::compact::generation::emit::descriptor::SegmentDescriptor {
    crate::graph::compact::generation::emit::descriptor::SegmentDescriptor {
        kind,
        encoding_version: 1,
        flags: 0x0001,
        alignment,
        element_width,
        length: bytes.len() as u64,
        crc: crc32fast::hash(bytes),
        element_count: if element_width > 0 {
            (bytes.len() / element_width as usize) as u32
        } else {
            0
        },
        body: crate::graph::compact::generation::emit::descriptor::SegmentBody::Resident(
            bytes::Bytes::from(bytes.to_vec()),
        ),
    }
}
