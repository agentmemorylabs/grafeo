#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
//! Source-true identity resolution, CSR construction, and CompactStore emission.

use super::budget::{GenerationBudget, GenerationMetrics};
use super::columns::{encode_column, push_zone_strings};
use super::error::GenerationError;
use super::input::{
    EdgeRecordSource, GenerationEdge, GenerationNode, NodeRecordSource, OriginalEdgeId,
    RelSchemaDecl,
};
use super::strings::{GlobalStringDictionary, collect_and_assign_global_codes};
use crate::graph::compact::CompactStore;
use crate::graph::compact::column::ColumnCodec;
use crate::graph::compact::csr::CsrAdjacency;
use crate::graph::compact::id::MAX_TABLE_ID;
use crate::graph::compact::node_table::NodeTable;
use crate::graph::compact::rel_table::RelTable;
use crate::graph::compact::schema::{ColumnDef, EdgeSchema, TableSchema};
use crate::graph::compact::zone_map::{ZoneMap, compute_block_zone_maps};
use crate::statistics::{EdgeTypeStatistics, LabelStatistics, Statistics};
use arcstr::ArcStr;
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_common::utils::hash::{FxHashMap, FxHashSet};

/// Result of a source-true generation.
#[derive(Debug)]
pub struct GeneratedCompact {
    /// Heap CompactStore with preserve-IDs maps and reverse CSR.
    pub store: CompactStore,
    /// Global lexicographic string dictionary used for v5 emission.
    pub global_strings: GlobalStringDictionary,
    /// Metrics collected during generation.
    pub metrics: GenerationMetrics,
}

/// Generate a CompactStore from typed original-ID input using the locked algorithm.
///
/// # Errors
///
/// Fail-closed on duplicates, missing/wrong-table endpoints, and wire-width overflow.
pub fn generate_compact_store(
    nodes: &mut dyn NodeRecordSource,
    edges: &mut dyn EdgeRecordSource,
    rel_schemas: &[RelSchemaDecl],
    budget: &GenerationBudget,
) -> Result<GeneratedCompact, GenerationError> {
    budget.validate()?;
    let mut metrics = GenerationMetrics::default();
    let mut string_occ: Vec<String> = Vec::new();

    // Optional relationship schema declarations force endpoint table checks.
    let rel_decls = rel_schemas;

    // ── Partition nodes by label, sort by original ID, assign dense offsets ──
    let mut by_label: FxHashMap<String, Vec<GenerationNode>> = FxHashMap::default();
    let mut seen_nodes: FxHashSet<u64> = FxHashSet::default();
    while let Some(n) = nodes.next_node()? {
        if !seen_nodes.insert(n.id.as_u64()) {
            return Err(GenerationError::DuplicateNodeId(n.id.as_u64()));
        }
        by_label.entry(n.label.clone()).or_default().push(n);
    }

    let mut labels: Vec<String> = by_label.keys().cloned().collect();
    labels.sort();
    if labels.len() > usize::from(MAX_TABLE_ID) + 1 {
        return Err(GenerationError::WireWidthOverflow {
            what: "node_table_count",
            count: labels.len() as u64,
            max: u64::from(MAX_TABLE_ID),
        });
    }

    let mut label_to_table_id: FxHashMap<ArcStr, u16> = FxHashMap::default();
    let mut table_id_to_label: Vec<ArcStr> = Vec::new();
    let mut node_id_map: FxHashMap<NodeId, (u16, u64)> = FxHashMap::default();
    let mut node_offset_to_id: Vec<Vec<NodeId>> = Vec::new();
    let mut node_rows: Vec<Vec<GenerationNode>> = Vec::new();

    for (tid_usize, label) in labels.iter().enumerate() {
        let tid = u16::try_from(tid_usize).map_err(|_| GenerationError::WireWidthOverflow {
            what: "node_table_id",
            count: tid_usize as u64,
            max: u64::from(u16::MAX),
        })?;
        let label_arc = ArcStr::from(label.as_str());
        charge_schema(&mut metrics, budget, label)?;
        string_occ.push(label.clone());
        label_to_table_id.insert(label_arc.clone(), tid);
        table_id_to_label.push(label_arc);

        let mut rows = by_label.remove(label).unwrap_or_default();
        rows.sort_by_key(|n| n.id.as_u64());
        if rows.len() > u32::MAX as usize {
            return Err(GenerationError::WireWidthOverflow {
                what: "node_table_row_count",
                count: rows.len() as u64,
                max: u64::from(u32::MAX),
            });
        }
        let mut rev = Vec::with_capacity(rows.len());
        for (off, n) in rows.iter().enumerate() {
            node_id_map.insert(n.id.to_node_id(), (tid, off as u64));
            rev.push(n.id.to_node_id());
        }
        node_offset_to_id.push(rev);
        node_rows.push(rows);
    }

    // ── Build node tables ──────────────────────────────────────────────────
    let mut node_tables_by_id: Vec<NodeTable> = Vec::with_capacity(node_rows.len());
    for (tid, rows) in node_rows.iter().enumerate() {
        let label = table_id_to_label[tid].clone();
        let row_count = rows.len();
        let mut keys: FxHashSet<PropertyKey> = FxHashSet::default();
        for n in rows {
            keys.extend(n.properties.keys().cloned());
        }
        let mut key_list: Vec<PropertyKey> = keys.into_iter().collect();
        key_list.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        let mut columns: FxHashMap<PropertyKey, ColumnCodec> = FxHashMap::default();
        let mut zone_maps: FxHashMap<PropertyKey, ZoneMap> = FxHashMap::default();
        let mut col_defs: Vec<ColumnDef> = Vec::new();

        for key in &key_list {
            charge_schema(&mut metrics, budget, key.as_str())?;
            string_occ.push(key.as_str().to_string());
            let values: Vec<Option<&Value>> = rows.iter().map(|n| n.properties.get(key)).collect();
            let ctx = format!("node table {label} column {key}");
            let (codec, col_type, zm) = encode_column(&values, &ctx, &mut string_occ)?;
            col_defs.push(ColumnDef::new(key.as_str(), col_type));
            if let Some(z) = zm {
                push_zone_strings(&z, &mut string_occ);
                zone_maps.insert(key.clone(), z);
            }
            columns.insert(key.clone(), codec);
        }

        let block_zone_maps: FxHashMap<PropertyKey, Vec<ZoneMap>> = columns
            .iter()
            .map(|(k, c)| {
                let zms = compute_block_zone_maps(c);
                for zm in &zms {
                    push_zone_strings(zm, &mut string_occ);
                }
                (k.clone(), zms)
            })
            .collect();

        let tid_u16 = tid as u16;
        let schema = TableSchema::new(label.as_str(), tid_u16, col_defs);
        node_tables_by_id.push(NodeTable::from_columns_with_block_stats(
            schema,
            columns,
            zone_maps,
            block_zone_maps,
            row_count,
        ));
    }

    // ── Partition edges ────────────────────────────────────────────────────
    #[derive(Clone, Debug)]
    struct ResolvedEdge {
        original_id: OriginalEdgeId,
        src_off: u32,
        dst_off: u32,
        properties: FxHashMap<PropertyKey, Value>,
    }

    type RelKey = (String, u16, u16);
    let mut edge_groups: FxHashMap<RelKey, Vec<ResolvedEdge>> = FxHashMap::default();
    let mut seen_edges: FxHashSet<u64> = FxHashSet::default();

    // Map edge_type -> allowed (src_label, dst_label) when schemas are declared.
    let mut decl_by_type: FxHashMap<String, (String, String)> = FxHashMap::default();
    for d in rel_decls {
        decl_by_type.insert(
            d.edge_type.clone(),
            (d.src_label.clone(), d.dst_label.clone()),
        );
        string_occ.push(d.edge_type.clone());
    }

    while let Some(e) = edges.next_edge()? {
        if !seen_edges.insert(e.id.as_u64()) {
            return Err(GenerationError::DuplicateEdgeId(e.id.as_u64()));
        }
        let (src_tid, src_off) = resolve_endpoint(&node_id_map, &e, true)?;
        let (dst_tid, dst_off) = resolve_endpoint(&node_id_map, &e, false)?;

        if let Some((exp_src, exp_dst)) = decl_by_type.get(&e.edge_type) {
            let actual_src = table_id_to_label[src_tid as usize].as_str();
            let actual_dst = table_id_to_label[dst_tid as usize].as_str();
            if actual_src != exp_src.as_str() {
                let exp_tid = *label_to_table_id.get(exp_src.as_str()).ok_or_else(|| {
                    GenerationError::InvalidInput(format!("rel schema src label {exp_src} unknown"))
                })?;
                return Err(GenerationError::WrongTableEndpoint {
                    edge_id: e.id.as_u64(),
                    node_id: e.src.as_u64(),
                    expected_table: exp_tid,
                    actual_table: src_tid,
                    is_source: true,
                });
            }
            if actual_dst != exp_dst.as_str() {
                let exp_tid = *label_to_table_id.get(exp_dst.as_str()).ok_or_else(|| {
                    GenerationError::InvalidInput(format!("rel schema dst label {exp_dst} unknown"))
                })?;
                return Err(GenerationError::WrongTableEndpoint {
                    edge_id: e.id.as_u64(),
                    node_id: e.dst.as_u64(),
                    expected_table: exp_tid,
                    actual_table: dst_tid,
                    is_source: false,
                });
            }
        }

        string_occ.push(e.edge_type.clone());
        let key = (e.edge_type.clone(), src_tid, dst_tid);
        let src_off_u32 =
            u32::try_from(src_off).map_err(|_| GenerationError::WireWidthOverflow {
                what: "src_dense_offset",
                count: src_off,
                max: u64::from(u32::MAX),
            })?;
        let dst_off_u32 =
            u32::try_from(dst_off).map_err(|_| GenerationError::WireWidthOverflow {
                what: "dst_dense_offset",
                count: dst_off,
                max: u64::from(u32::MAX),
            })?;
        edge_groups.entry(key).or_default().push(ResolvedEdge {
            original_id: e.id,
            src_off: src_off_u32,
            dst_off: dst_off_u32,
            properties: e.properties,
        });
    }

    let mut rel_keys: Vec<RelKey> = edge_groups.keys().cloned().collect();
    rel_keys.sort();
    if rel_keys.len() > usize::from(MAX_TABLE_ID) + 1 {
        return Err(GenerationError::WireWidthOverflow {
            what: "rel_table_count",
            count: rel_keys.len() as u64,
            max: u64::from(MAX_TABLE_ID),
        });
    }

    let mut rel_tables_by_id: Vec<RelTable> = Vec::with_capacity(rel_keys.len());
    let mut edge_type_to_rel_id: FxHashMap<ArcStr, Vec<u16>> = FxHashMap::default();
    let mut rel_table_id_to_type: Vec<ArcStr> = Vec::new();
    let mut edge_id_map: FxHashMap<EdgeId, (u16, u64)> = FxHashMap::default();
    let mut edge_offset_to_id: Vec<Vec<EdgeId>> = Vec::new();

    for (rid_usize, key) in rel_keys.iter().enumerate() {
        let rel_table_id =
            u16::try_from(rid_usize).map_err(|_| GenerationError::WireWidthOverflow {
                what: "rel_table_id",
                count: rid_usize as u64,
                max: u64::from(u16::MAX),
            })?;
        let (edge_type, src_tid, dst_tid) = key;
        charge_schema(&mut metrics, budget, edge_type)?;
        let mut edges = edge_groups.remove(key).unwrap_or_default();

        // Forward order: (src_off, dst_off, original_edge_id).
        edges.sort_by(|a, b| {
            (a.src_off, a.dst_off, a.original_id.as_u64()).cmp(&(
                b.src_off,
                b.dst_off,
                b.original_id.as_u64(),
            ))
        });

        if edges.len() > u32::MAX as usize {
            return Err(GenerationError::WireWidthOverflow {
                what: "rel_table_edge_count",
                count: edges.len() as u64,
                max: u64::from(u32::MAX),
            });
        }

        let src_row_count = node_tables_by_id
            .get(*src_tid as usize)
            .map_or(0, NodeTable::len);
        let dst_row_count = node_tables_by_id
            .get(*dst_tid as usize)
            .map_or(0, NodeTable::len);

        let fwd_pairs: Vec<(u32, u32)> = edges.iter().map(|e| (e.src_off, e.dst_off)).collect();
        let fwd = CsrAdjacency::from_sorted_edges(src_row_count, &fwd_pairs);

        // Reverse: (dst, src, forward_position).
        let mut rev_records: Vec<(u32, u32, u32)> = edges
            .iter()
            .enumerate()
            .map(|(pos, e)| (e.dst_off, e.src_off, pos as u32))
            .collect();
        rev_records.sort_by_key(|&(d, s, fp)| (d, s, fp));
        let rev_pairs: Vec<(u32, u32)> = rev_records.iter().map(|&(d, s, _)| (d, s)).collect();
        let mut bwd = CsrAdjacency::from_sorted_edges(dst_row_count, &rev_pairs);
        let forward_positions: Vec<u32> = rev_records.iter().map(|&(_, _, fp)| fp).collect();
        bwd.set_edge_data(forward_positions);

        let mut rev_ids = Vec::with_capacity(edges.len());
        for (pos, e) in edges.iter().enumerate() {
            edge_id_map.insert(e.original_id.to_edge_id(), (rel_table_id, pos as u64));
            rev_ids.push(e.original_id.to_edge_id());
        }
        edge_offset_to_id.push(rev_ids);

        let mut keys: FxHashSet<PropertyKey> = FxHashSet::default();
        for e in &edges {
            keys.extend(e.properties.keys().cloned());
        }
        let mut key_list: Vec<PropertyKey> = keys.into_iter().collect();
        key_list.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        let mut properties: FxHashMap<PropertyKey, ColumnCodec> = FxHashMap::default();
        let mut col_defs: Vec<ColumnDef> = Vec::new();
        for key in &key_list {
            charge_schema(&mut metrics, budget, key.as_str())?;
            string_occ.push(key.as_str().to_string());
            let values: Vec<Option<&Value>> = edges.iter().map(|e| e.properties.get(key)).collect();
            let ctx = format!("rel table {edge_type} column {key}");
            let (codec, col_type, _) = encode_column(&values, &ctx, &mut string_occ)?;
            col_defs.push(ColumnDef::new(key.as_str(), col_type));
            properties.insert(key.clone(), codec);
        }

        let src_label = table_id_to_label[*src_tid as usize].as_str();
        let dst_label = table_id_to_label[*dst_tid as usize].as_str();
        let schema = EdgeSchema::new(
            edge_type.as_str(),
            rel_table_id,
            src_label,
            dst_label,
            col_defs,
        );
        let et = ArcStr::from(edge_type.as_str());
        rel_table_id_to_type.push(et.clone());
        edge_type_to_rel_id
            .entry(et)
            .or_default()
            .push(rel_table_id);
        rel_tables_by_id.push(RelTable::new(
            schema,
            fwd,
            Some(bwd),
            properties,
            *src_tid,
            *dst_tid,
        ));
    }

    // ── Statistics ─────────────────────────────────────────────────────────
    let mut stats = Statistics::new();
    let mut total_nodes: u64 = 0;
    let mut total_edges: u64 = 0;
    for (idx, nt) in node_tables_by_id.iter().enumerate() {
        let count = nt.len() as u64;
        total_nodes += count;
        stats.update_label(table_id_to_label[idx].as_str(), LabelStatistics::new(count));
    }
    let mut edge_type_counts: FxHashMap<&str, u64> = FxHashMap::default();
    for (idx, rt) in rel_tables_by_id.iter().enumerate() {
        let count = rt.num_edges() as u64;
        total_edges += count;
        *edge_type_counts
            .entry(rel_table_id_to_type[idx].as_str())
            .or_default() += count;
    }
    for (et, count) in edge_type_counts {
        stats.update_edge_type(et, EdgeTypeStatistics::new(count, 0.0, 0.0));
    }
    stats.total_nodes = total_nodes;
    stats.total_edges = total_edges;

    let mut store = CompactStore::new(
        node_tables_by_id,
        label_to_table_id,
        rel_tables_by_id,
        edge_type_to_rel_id,
        table_id_to_label,
        rel_table_id_to_type,
        stats,
    );
    store.set_id_maps(
        node_id_map,
        edge_id_map,
        node_offset_to_id,
        edge_offset_to_id,
    );

    let global_strings = collect_and_assign_global_codes(string_occ)?;
    metrics.global_string_count = global_strings.len() as u64;

    Ok(GeneratedCompact {
        store,
        global_strings,
        metrics,
    })
}

/// Emit a production v5 payload with lexicographic global string codes (test helper).
///
/// # Errors
///
/// Codec or generation failures.
#[cfg(test)]
pub fn generate_v5_payload(
    nodes: &mut dyn NodeRecordSource,
    edges: &mut dyn EdgeRecordSource,
    rel_schemas: &[RelSchemaDecl],
    budget: &GenerationBudget,
) -> Result<(Vec<u8>, GeneratedCompact), GenerationError> {
    let mut generated = generate_compact_store(nodes, edges, rel_schemas, budget)?;
    let mut source = super::segment_source::CompactV5SegmentSource::new(
        &generated.store,
        &generated.global_strings,
    )?;
    let bytes = super::segment_source::assemble_v5_payload_from_source(
        &mut source,
        generated.store.total_nodes(),
        generated.store.total_edges(),
        generated.store.preserves_ids(),
    )?;

    generated
        .metrics
        .reserve_anon(bytes.len() as u64, budget.max_anon_bytes)?;
    Ok((bytes, generated))
}

fn charge_schema(
    metrics: &mut GenerationMetrics,
    budget: &GenerationBudget,
    s: &str,
) -> Result<(), GenerationError> {
    let bytes = (s.len() as u64).saturating_add(32);
    metrics.reserve_schema(bytes, budget.max_schema_bytes)
}

fn resolve_endpoint(
    node_id_map: &FxHashMap<NodeId, (u16, u64)>,
    edge: &GenerationEdge,
    is_source: bool,
) -> Result<(u16, u64), GenerationError> {
    let nid = if is_source { edge.src } else { edge.dst };
    node_id_map
        .get(&nid.to_node_id())
        .copied()
        .ok_or(GenerationError::MissingEndpoint {
            edge_id: edge.id.as_u64(),
            node_id: nid.as_u64(),
            is_source,
        })
}
