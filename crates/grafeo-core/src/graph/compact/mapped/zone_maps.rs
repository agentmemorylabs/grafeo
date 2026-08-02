//! Mapped table/block zone-map segment codec (v5 kinds 18–19).
//!
//! Fixed 40-byte records. String min/max are dictionary codes into the
//! global `StringOffsets`/`StringBytes` segments (no owned string payload
//! in the zone-map segment itself). Numeric/bool min/max are inlined.

// The `as usize`/`as u32`/`as u16` casts in this module convert between wire
// field widths and in-memory indices for data already bounds-checked against
// the resident section length; they cannot truncate on the 64-bit targets
// this engine supports.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
use super::string_dict::MappedStringDictionary;
use crate::graph::compact::zone_map::ZoneMap;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_common::utils::hash::FxHashMap;

fn read_u16_at(data: &[u8], off: usize) -> Result<u16, String> {
    let end = off.checked_add(2).ok_or("u16 offset overflow")?;
    if end > data.len() {
        return Err("truncated u16".into());
    }
    Ok(u16::from_le_bytes([data[off], data[off + 1]]))
}
fn read_u32_at(data: &[u8], off: usize) -> Result<u32, String> {
    let end = off.checked_add(4).ok_or("u32 offset overflow")?;
    if end > data.len() {
        return Err("truncated u32".into());
    }
    Ok(u32::from_le_bytes([
        data[off],
        data[off + 1],
        data[off + 2],
        data[off + 3],
    ]))
}
fn read_u64_at(data: &[u8], off: usize) -> Result<u64, String> {
    let end = off.checked_add(8).ok_or("u64 offset overflow")?;
    if end > data.len() {
        return Err("truncated u64".into());
    }
    Ok(u64::from_le_bytes([
        data[off],
        data[off + 1],
        data[off + 2],
        data[off + 3],
        data[off + 4],
        data[off + 5],
        data[off + 6],
        data[off + 7],
    ]))
}

/// Wire size of one zone-map record.
pub const ZONE_MAP_RECORD_LEN: usize = 40;

/// `block_index` value meaning "table-level zone map" (kind 18).
pub const TABLE_ZONE_BLOCK_SENTINEL: u32 = u32::MAX;

const TAG_ABSENT: u8 = 0;
const TAG_INT64: u8 = 1;
const TAG_BOOL: u8 = 2;
const TAG_STRING_CODE: u8 = 3;
const TAG_FLOAT64: u8 = 4;

/// Writes one zone-map record into `buf`.
///
/// # Errors
///
/// Returns an error if the zone-map value encoding fails (e.g., unsupported
/// value type or string code out of dictionary range).
pub fn write_zone_map_record(
    buf: &mut Vec<u8>,
    table_id: u16,
    column_key_code: u32,
    block_index: u32,
    zm: &ZoneMap,
    string_index: &FxHashMap<String, u32>,
) -> Result<(), String> {
    buf.extend_from_slice(&table_id.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
    buf.extend_from_slice(&column_key_code.to_le_bytes());
    buf.extend_from_slice(&block_index.to_le_bytes());
    let null_count = u32::try_from(zm.null_count).map_err(|_| "zone map null_count overflow")?;
    let row_count = u32::try_from(zm.row_count).map_err(|_| "zone map row_count overflow")?;
    buf.extend_from_slice(&null_count.to_le_bytes());
    buf.extend_from_slice(&row_count.to_le_bytes());
    let (min_tag, min_payload) = encode_value(&zm.min, string_index)?;
    let (max_tag, max_payload) = encode_value(&zm.max, string_index)?;
    buf.push(min_tag);
    buf.push(max_tag);
    buf.extend_from_slice(&0u16.to_le_bytes()); // pad
    buf.extend_from_slice(&min_payload.to_le_bytes());
    buf.extend_from_slice(&max_payload.to_le_bytes());
    debug_assert_eq!(buf.len() % ZONE_MAP_RECORD_LEN, 0);
    Ok(())
}

fn encode_value(
    v: &Option<Value>,
    string_index: &FxHashMap<String, u32>,
) -> Result<(u8, u64), String> {
    match v {
        None => Ok((TAG_ABSENT, 0)),
        Some(Value::Int64(n)) => Ok((TAG_INT64, *n as u64)),
        Some(Value::Bool(b)) => Ok((TAG_BOOL, u64::from(*b))),
        Some(Value::String(s)) => {
            let code = *string_index
                .get(s.as_str())
                .ok_or_else(|| format!("zone map string not interned: {s}"))?;
            Ok((TAG_STRING_CODE, u64::from(code)))
        }
        Some(Value::Float64(f)) => Ok((TAG_FLOAT64, f.to_bits())),
        Some(_) => Ok((TAG_ABSENT, 0)), // unsupported: treat as absent
    }
}

fn decode_value(
    tag: u8,
    payload: u64,
    dict: &MappedStringDictionary,
) -> Result<Option<Value>, String> {
    match tag {
        TAG_ABSENT => Ok(None),
        TAG_INT64 => Ok(Some(Value::Int64(payload as i64))),
        TAG_BOOL => Ok(Some(Value::Bool(payload != 0))),
        TAG_STRING_CODE => {
            let code = u32::try_from(payload).map_err(|_| "zone map string code overflow")?;
            let s = dict
                .get(code)
                .ok_or_else(|| format!("zone map string code {code} missing from dictionary"))?;
            Ok(Some(Value::String(arcstr::ArcStr::from(s))))
        }
        TAG_FLOAT64 => Ok(Some(Value::Float64(f64::from_bits(payload)))),
        other => Err(format!("unknown zone map value tag {other}")),
    }
}

/// One decoded zone-map record (before grouping by table/column).
#[derive(Debug, Clone)]
struct RawZoneRecord {
    table_id: u16,
    column_key: PropertyKey,
    block_index: u32,
    zone_map: ZoneMap,
}

fn parse_records(data: &[u8], dict: &MappedStringDictionary) -> Result<Vec<RawZoneRecord>, String> {
    if !data.len().is_multiple_of(ZONE_MAP_RECORD_LEN) {
        return Err(format!(
            "zone map segment length {} not multiple of {ZONE_MAP_RECORD_LEN}",
            data.len()
        ));
    }
    let mut out = Vec::with_capacity(data.len() / ZONE_MAP_RECORD_LEN);
    let mut off = 0usize;
    while off < data.len() {
        let table_id = read_u16_at(data, off)?;
        let reserved = read_u16_at(data, off + 2)?;
        if reserved != 0 {
            return Err(format!("zone map reserved non-zero at offset {off}"));
        }
        let col_code = read_u32_at(data, off + 4)?;
        let block_index = read_u32_at(data, off + 8)?;
        let null_count = read_u32_at(data, off + 12)? as usize;
        let row_count = read_u32_at(data, off + 16)? as usize;
        let min_tag = data[off + 20];
        let max_tag = data[off + 21];
        let pad = read_u16_at(data, off + 22)?;
        if pad != 0 {
            return Err(format!("zone map pad non-zero at offset {off}"));
        }
        let min_payload = read_u64_at(data, off + 24)?;
        let max_payload = read_u64_at(data, off + 32)?;
        let key_str = dict
            .get(col_code)
            .ok_or_else(|| format!("zone map column code {col_code} missing"))?;
        let min = decode_value(min_tag, min_payload, dict)?;
        let max = decode_value(max_tag, max_payload, dict)?;
        out.push(RawZoneRecord {
            table_id,
            column_key: PropertyKey::new(key_str),
            block_index,
            zone_map: ZoneMap {
                min,
                max,
                null_count,
                row_count,
            },
        });
        off += ZONE_MAP_RECORD_LEN;
    }
    Ok(out)
}

/// Parses kind-18 table zone maps into per-node-table maps and per-rel-table maps.
///
/// Relationship-tagged records (`table_id & 0x8000 != 0`) are installed into
/// the returned `rel_tables` vector (indexed by `table_id & 0x7FFF`).
///
/// # Errors
///
/// Returns an error if the data length is not a multiple of the record size,
/// a table_id is out of range, or a zone-map value fails to decode.
pub fn parse_table_zone_maps(
    data: &[u8],
    dict: &MappedStringDictionary,
    table_count: usize,
    rel_count: usize,
) -> Result<
    (
        Vec<FxHashMap<PropertyKey, ZoneMap>>,
        Vec<FxHashMap<PropertyKey, ZoneMap>>,
    ),
    String,
> {
    let records = parse_records(data, dict)?;
    let mut tables: Vec<FxHashMap<PropertyKey, ZoneMap>> =
        (0..table_count).map(|_| FxHashMap::default()).collect();
    let mut rel_tables: Vec<FxHashMap<PropertyKey, ZoneMap>> =
        (0..rel_count).map(|_| FxHashMap::default()).collect();
    for rec in records {
        if rec.block_index != TABLE_ZONE_BLOCK_SENTINEL {
            return Err(format!(
                "TableZoneMaps record has non-sentinel block_index {}",
                rec.block_index
            ));
        }
        // Relationship-tagged table_ids (0x8000 | rel_id) are installed on
        // the corresponding rel table. Out-of-range rel ids fail closed.
        if rec.table_id & 0x8000 != 0 {
            let rid = (rec.table_id & 0x7FFF) as usize;
            if rid >= rel_count {
                return Err(format!(
                    "TableZoneMaps rel table_id {} out of range (rel_count {rel_count})",
                    rec.table_id
                ));
            }
            rel_tables[rid].insert(rec.column_key, rec.zone_map);
            continue;
        }
        let tid = rec.table_id as usize;
        if tid >= table_count {
            return Err(format!(
                "TableZoneMaps table_id {} out of range (count {table_count})",
                rec.table_id
            ));
        }
        tables[tid].insert(rec.column_key, rec.zone_map);
    }
    Ok((tables, rel_tables))
}

/// Parses kind-19 block zone maps into per-node-table and per-rel-table column → Vec maps.
///
/// Relationship-tagged records (`table_id & 0x8000 != 0`) are installed into
/// the returned `rel_tables` vector (indexed by `table_id & 0x7FFF`).
///
/// # Errors
///
/// Returns an error if the data is truncated, a table_id/column is out of
/// range, block indices are non-contiguous, or a value fails to decode.
pub fn parse_block_zone_maps(
    data: &[u8],
    dict: &MappedStringDictionary,
    table_count: usize,
    rel_count: usize,
) -> Result<
    (
        Vec<FxHashMap<PropertyKey, Vec<ZoneMap>>>,
        Vec<FxHashMap<PropertyKey, Vec<ZoneMap>>>,
    ),
    String,
> {
    let records = parse_records(data, dict)?;
    // Group: table → column → (block_index, ZoneMap)
    let mut staged: Vec<FxHashMap<PropertyKey, Vec<(u32, ZoneMap)>>> =
        (0..table_count).map(|_| FxHashMap::default()).collect();
    let mut rel_staged: Vec<FxHashMap<PropertyKey, Vec<(u32, ZoneMap)>>> =
        (0..rel_count).map(|_| FxHashMap::default()).collect();
    for rec in records {
        if rec.block_index == TABLE_ZONE_BLOCK_SENTINEL {
            return Err("BlockZoneMaps record has table-level sentinel block_index".into());
        }
        // Relationship-tagged table_ids (0x8000 | rel_id) are installed on
        // the corresponding rel table. Out-of-range rel ids fail closed.
        if rec.table_id & 0x8000 != 0 {
            let rid = (rec.table_id & 0x7FFF) as usize;
            if rid >= rel_count {
                return Err(format!(
                    "BlockZoneMaps rel table_id {} out of range (rel_count {rel_count})",
                    rec.table_id
                ));
            }
            rel_staged[rid]
                .entry(rec.column_key)
                .or_default()
                .push((rec.block_index, rec.zone_map));
            continue;
        }
        let tid = rec.table_id as usize;
        if tid >= table_count {
            return Err(format!(
                "BlockZoneMaps table_id {} out of range (count {table_count})",
                rec.table_id
            ));
        }
        staged[tid]
            .entry(rec.column_key)
            .or_default()
            .push((rec.block_index, rec.zone_map));
    }
    let tables = finalize_block_maps(staged)?;
    let rel_tables = finalize_block_maps(rel_staged)?;
    Ok((tables, rel_tables))
}

/// Validates contiguous block indices and converts staged pairs to final maps.
fn finalize_block_maps(
    staged: Vec<FxHashMap<PropertyKey, Vec<(u32, ZoneMap)>>>,
) -> Result<Vec<FxHashMap<PropertyKey, Vec<ZoneMap>>>, String> {
    let mut tables: Vec<FxHashMap<PropertyKey, Vec<ZoneMap>>> =
        (0..staged.len()).map(|_| FxHashMap::default()).collect();
    for (tid, cols) in staged.into_iter().enumerate() {
        for (key, mut pairs) in cols {
            pairs.sort_by_key(|(idx, _)| *idx);
            // Fail closed on gaps / duplicates.
            for (i, (idx, _)) in pairs.iter().enumerate() {
                if *idx as usize != i {
                    return Err(format!(
                        "BlockZoneMaps for table {tid} column {key:?}: expected block {i}, got {idx}"
                    ));
                }
            }
            tables[tid].insert(key, pairs.into_iter().map(|(_, zm)| zm).collect());
        }
    }
    Ok(tables)
}

/// Builds TableZoneMaps + BlockZoneMaps segment bodies for all node tables.
///
/// # Errors
///
/// Returns an error if a zone-map value cannot be encoded or a required
/// string code is missing from `string_index`.
pub fn build_zone_map_segments(
    store: &crate::graph::compact::CompactStore,
    string_index: &FxHashMap<String, u32>,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut table_seg = Vec::new();
    let mut block_seg = Vec::new();
    for (tid, nt) in store.node_tables_by_id.iter().enumerate() {
        let table_id = tid as u16;
        // Deterministic column order for zone maps: sort by key.
        let mut keys: Vec<_> = nt.zone_maps().keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let Some(zm) = nt.zone_maps().get(key) else {
                continue;
            };
            let code = *string_index
                .get(key.as_str())
                .ok_or_else(|| format!("zone map key not interned: {}", key.as_str()))?;
            write_zone_map_record(
                &mut table_seg,
                table_id,
                code,
                TABLE_ZONE_BLOCK_SENTINEL,
                zm,
                string_index,
            )?;
        }
        let mut bkeys: Vec<_> = nt.block_zone_maps().keys().cloned().collect();
        bkeys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &bkeys {
            let Some(zms) = nt.block_zone_maps().get(key) else {
                continue;
            };
            let code = *string_index
                .get(key.as_str())
                .ok_or_else(|| format!("block zone map key not interned: {}", key.as_str()))?;
            for (block_idx, zm) in zms.iter().enumerate() {
                let bi = u32::try_from(block_idx).map_err(|_| "block index overflow")?;
                write_zone_map_record(&mut block_seg, table_id, code, bi, zm, string_index)?;
            }
        }
    }
    Ok((table_seg, block_seg))
}
