//! A later `MATCH` must join on a variable that an earlier clause already
//! bound, wherever that variable sits in the new pattern.
//!
//! Regression for the agent-memory-hosted evidence wipe:
//!
//! ```cypher
//! MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids
//! MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o)
//! DETACH DELETE ev
//! ```
//!
//! Before the fix the second `MATCH` scanned `ev` as a cross join over the
//! earlier rows and added `o` as a fresh column with no equality check, so
//! every evidence node matched and the delete wiped all of them.
//!
//! ```bash
//! cargo test -p grafeo-engine --features cypher --test cypher_rebound_variable_join
//! ```

#![cfg(feature = "cypher")]

use std::collections::HashMap;

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

/// Fixture:
///
/// ```text
/// Documents:    dA, dB
/// Occurrences:  o1 (doc A, x=1), o2 (doc A, x=2), o3 (doc B, x=1), o4 (doc B, no evidence)
/// Evidence:     ev1->o1, ev2->o2, ev3->o3, ev4->o3   (EVIDENCE_FOR_OCCURRENCE)
/// IN_DOC:       o1->dA, o2->dA, o3->dB, o4->dB
/// ```
fn fixture() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    db.execute_cypher(
        "CREATE (dA:Document {name: 'dA'}), (dB:Document {name: 'dB'}), \
         (o1:SymbolOccurrence {name: 'o1', document_id: 'A', x: 1}), \
         (o2:SymbolOccurrence {name: 'o2', document_id: 'A', x: 2}), \
         (o3:SymbolOccurrence {name: 'o3', document_id: 'B', x: 1}), \
         (o4:SymbolOccurrence {name: 'o4', document_id: 'B', x: 3}), \
         (ev1:RelationshipEvidence {name: 'ev1'}), \
         (ev2:RelationshipEvidence {name: 'ev2'}), \
         (ev3:RelationshipEvidence {name: 'ev3'}), \
         (ev4:RelationshipEvidence {name: 'ev4'}), \
         (ev1)-[:EVIDENCE_FOR_OCCURRENCE]->(o1), \
         (ev2)-[:EVIDENCE_FOR_OCCURRENCE]->(o2), \
         (ev3)-[:EVIDENCE_FOR_OCCURRENCE]->(o3), \
         (ev4)-[:EVIDENCE_FOR_OCCURRENCE]->(o3), \
         (o1)-[:IN_DOC]->(dA), (o2)-[:IN_DOC]->(dA), \
         (o3)-[:IN_DOC]->(dB), (o4)-[:IN_DOC]->(dB)",
    )
    .expect("create fixture");
    db
}

fn ids_a() -> HashMap<String, Value> {
    let mut params = HashMap::new();
    params.insert(
        "ids".to_string(),
        Value::List(vec![Value::String("A".into())].into()),
    );
    params
}

fn s(v: &str) -> Value {
    Value::String(v.into())
}

/// Runs a query and returns its rows sorted (string-rendered) so tests are
/// independent of output order.
fn rows(db: &GrafeoDB, query: &str, params: Option<HashMap<String, Value>>) -> Vec<Vec<Value>> {
    let result = match params {
        Some(p) => db.execute_cypher_with_params(query, p),
        None => db.execute_cypher(query),
    }
    .unwrap_or_else(|e| panic!("query failed: {query}\n{e}"));
    let mut rows = result.rows().to_vec();
    rows.sort_by_key(|r| format!("{r:?}"));
    rows
}

fn count(db: &GrafeoDB, query: &str, params: Option<HashMap<String, Value>>) -> i64 {
    let r = rows(db, query, params);
    assert_eq!(r.len(), 1, "count query must return one row: {query}");
    r[0][0].as_int64().expect("count is an integer")
}

// ============================================================================
// Bound variable at the END of a one-hop pattern (the AMH shape)
// ============================================================================

#[test]
fn end_bound_one_hop_return() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN ev.name, o.name",
        Some(ids_a()),
    );
    assert_eq!(got, vec![vec![s("ev1"), s("o1")], vec![s("ev2"), s("o2")]]);
}

#[test]
fn end_bound_one_hop_count() {
    let db = fixture();
    let n = count(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN count(*)",
        Some(ids_a()),
    );
    assert_eq!(n, 2);
}

#[test]
fn end_bound_one_hop_detach_delete_only_matching_evidence() {
    let db = fixture();
    db.execute_cypher_with_params(
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         DETACH DELETE ev",
        ids_a(),
    )
    .expect("detach delete");

    let survivors = rows(&db, "MATCH (ev:RelationshipEvidence) RETURN ev.name", None);
    assert_eq!(survivors, vec![vec![s("ev3")], vec![s("ev4")]]);
    // The occurrences themselves are untouched.
    assert_eq!(
        count(&db, "MATCH (o:SymbolOccurrence) RETURN count(o)", None),
        4
    );
    // Remaining evidence edges still point at o3.
    let edges = rows(
        &db,
        "MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) RETURN ev.name, o.name",
        None,
    );
    assert_eq!(
        edges,
        vec![vec![s("ev3"), s("o3")], vec![s("ev4"), s("o3")]]
    );
}

#[test]
fn end_bound_after_with() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids WITH o \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN ev.name",
        Some(ids_a()),
    );
    assert_eq!(got, vec![vec![s("ev1")], vec![s("ev2")]]);
}

#[test]
fn end_bound_with_edge_variable_and_unlabeled_start() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence {name: 'o3'}) \
         MATCH (ev)-[r:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN ev.name, type(r)",
        None,
    );
    assert_eq!(
        got,
        vec![
            vec![s("ev3"), s("EVIDENCE_FOR_OCCURRENCE")],
            vec![s("ev4"), s("EVIDENCE_FOR_OCCURRENCE")],
        ]
    );
}

// ============================================================================
// Bound variable in the MIDDLE of a two-hop pattern
// ============================================================================

#[test]
fn middle_bound_two_hop() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence {name: 'o1'}) \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o)-[:IN_DOC]->(d:Document) \
         RETURN ev.name, o.name, d.name",
        None,
    );
    assert_eq!(got, vec![vec![s("ev1"), s("o1"), s("dA")]]);
}

#[test]
fn middle_bound_two_hop_multiple_bound_rows() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o)-[:IN_DOC]->(d:Document) \
         RETURN ev.name, o.name, d.name",
        Some(ids_a()),
    );
    assert_eq!(
        got,
        vec![
            vec![s("ev1"), s("o1"), s("dA")],
            vec![s("ev2"), s("o2"), s("dA")],
        ]
    );
}

// ============================================================================
// BOTH ends bound
// ============================================================================

#[test]
fn both_ends_bound_by_separate_matches() {
    let db = fixture();
    let n = count(
        &db,
        "MATCH (ev:RelationshipEvidence) \
         MATCH (o:SymbolOccurrence) \
         MATCH (ev)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN count(*)",
        None,
    );
    assert_eq!(n, 4);
}

#[test]
fn both_ends_bound_pairs() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (ev:RelationshipEvidence), (o:SymbolOccurrence) WHERE o.document_id = 'B' \
         MATCH (ev)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN ev.name, o.name",
        None,
    );
    assert_eq!(got, vec![vec![s("ev3"), s("o3")], vec![s("ev4"), s("o3")]]);
}

#[test]
fn both_ends_bound_end_written_first() {
    let db = fixture();
    let n = count(
        &db,
        "MATCH (ev:RelationshipEvidence) \
         MATCH (o:SymbolOccurrence) \
         MATCH (o)<-[:EVIDENCE_FOR_OCCURRENCE]-(ev) \
         RETURN count(*)",
        None,
    );
    assert_eq!(n, 4);
}

// ============================================================================
// Direction: incoming, outgoing (wrong way), undirected
// ============================================================================

#[test]
fn end_bound_incoming_arrow() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (ev:RelationshipEvidence {name: 'ev1'}) \
         MATCH (o:SymbolOccurrence)<-[:EVIDENCE_FOR_OCCURRENCE]-(ev) \
         RETURN o.name",
        None,
    );
    assert_eq!(got, vec![vec![s("o1")]]);
}

#[test]
fn end_bound_wrong_direction_matches_nothing() {
    let db = fixture();
    let n = count(
        &db,
        "MATCH (ev:RelationshipEvidence {name: 'ev1'}) \
         MATCH (o:SymbolOccurrence)-[:EVIDENCE_FOR_OCCURRENCE]->(ev) \
         RETURN count(*)",
        None,
    );
    assert_eq!(n, 0);
}

#[test]
fn end_bound_undirected() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence {name: 'o3'}) \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]-(o) \
         RETURN ev.name",
        None,
    );
    assert_eq!(got, vec![vec![s("ev3")], vec![s("ev4")]]);
}

// ============================================================================
// Bound variable carrying a label / property map must join AND filter
// ============================================================================

#[test]
fn end_bound_with_property_map_joins_and_filters() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o:SymbolOccurrence {x: 1}) \
         RETURN ev.name, o.name",
        Some(ids_a()),
    );
    assert_eq!(got, vec![vec![s("ev1"), s("o1")]]);
}

#[test]
fn end_bound_with_mismatched_label_matches_nothing() {
    let db = fixture();
    let n = count(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o:Document) \
         RETURN count(*)",
        Some(ids_a()),
    );
    assert_eq!(n, 0);
}

#[test]
fn middle_bound_with_property_map_joins_and_filters() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence) \
         MATCH (ev)-[:EVIDENCE_FOR_OCCURRENCE]->(o {x: 1})-[:IN_DOC]->(d) \
         RETURN ev.name, d.name",
        None,
    );
    assert_eq!(
        got,
        vec![
            vec![s("ev1"), s("dA")],
            vec![s("ev3"), s("dB")],
            vec![s("ev4"), s("dB")],
        ]
    );
}

// ============================================================================
// Named path and variable-length with an end-bound variable
// ============================================================================

#[test]
fn end_bound_named_path() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence {name: 'o1'}) \
         MATCH p = (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN ev.name, length(p)",
        None,
    );
    assert_eq!(got, vec![vec![s("ev1"), Value::Int64(1)]]);
}

#[test]
fn end_bound_variable_length() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (d:Document {name: 'dA'}) \
         MATCH (ev:RelationshipEvidence)-[*2..2]->(d) \
         RETURN ev.name",
        None,
    );
    assert_eq!(got, vec![vec![s("ev1")], vec![s("ev2")]]);
}

// ============================================================================
// Regression guards (already correct before the fix)
// ============================================================================

#[test]
fn guard_start_bound_pattern() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (o)<-[:EVIDENCE_FOR_OCCURRENCE]-(ev:RelationshipEvidence) \
         RETURN ev.name, o.name",
        Some(ids_a()),
    );
    assert_eq!(got, vec![vec![s("ev1"), s("o1")], vec![s("ev2"), s("o2")]]);
}

#[test]
fn guard_property_join_across_matches() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (a:SymbolOccurrence {name: 'o1'}) \
         MATCH (b:SymbolOccurrence {document_id: a.document_id}) \
         RETURN b.name",
        None,
    );
    assert_eq!(got, vec![vec![s("o1")], vec![s("o2")]]);
}

#[test]
fn guard_unrelated_later_match_is_cross_product() {
    let db = fixture();
    let n = count(
        &db,
        "MATCH (o:SymbolOccurrence) WHERE o.document_id IN $ids \
         MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(x) \
         RETURN count(*)",
        Some(ids_a()),
    );
    // 2 occurrences x 4 evidence edges: no shared variable, so a cross product.
    assert_eq!(n, 8);
}

#[test]
fn guard_comma_pattern_in_single_match() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence {name: 'o3'}), \
         (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN ev.name",
        None,
    );
    assert_eq!(got, vec![vec![s("ev3")], vec![s("ev4")]]);
}

#[test]
fn guard_optional_match_end_bound() {
    let db = fixture();
    let got = rows(
        &db,
        "MATCH (o:SymbolOccurrence) \
         OPTIONAL MATCH (ev:RelationshipEvidence)-[:EVIDENCE_FOR_OCCURRENCE]->(o) \
         RETURN o.name, ev.name",
        None,
    );
    assert_eq!(
        got,
        vec![
            vec![s("o1"), s("ev1")],
            vec![s("o2"), s("ev2")],
            vec![s("o3"), s("ev3")],
            vec![s("o3"), s("ev4")],
            vec![s("o4"), Value::Null],
        ]
    );
}
