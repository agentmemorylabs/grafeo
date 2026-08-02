//! CSR three-stage streaming tests (G-EM0.5b D0.8.5).

#![cfg(feature = "generation-streaming")]

use crate::graph::compact::generation::emit::descriptor::SegmentBody;
use crate::graph::compact::generation::emit::sink::{MemorySegmentSink, SegmentSink};
use crate::graph::compact::generation::{
    GenerationBudget, GenerationMetrics, InMemoryRunStore, RunStore, SortRecord,
};
use crate::graph::compact::generation_builder::csr_pass::{
    RelTableGeometry, stream_forward_csr, stream_reverse_csr,
};
use crate::graph::compact::mapped::SegmentKind;

fn budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

fn body_bytes(b: &SegmentBody) -> Vec<u8> {
    match b {
        SegmentBody::Resident(bytes) => bytes.to_vec(),
        SegmentBody::Spilled(p) => std::fs::read(p).unwrap(),
    }
}

fn read_u32s(bytes: &[u8]) -> Vec<u32> {
    bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

/// One rel table, 3 source rows, 3 dst rows, edges exercising duplicate
/// endpoint pairs and a self-loop.
#[test]
fn csr_streams_forward_reverse_and_positions() {
    let b = budget();
    let mut store = InMemoryRunStore::new();
    let mut metrics = GenerationMetrics::default();

    // Forward records: rel 0, edges (src,dst): (0,0) self, (0,1), (0,1) dup,
    // (2,1). Sorted by (rel,src,dst,eid).
    let edges = [(0u64, 0u64, 0u64), (0, 1, 1), (0, 1, 2), (2, 1, 3)];
    let mut fwd_sink = store.sink("fwd", &b).unwrap();
    for (src, dst, eid) in edges {
        let mut key = Vec::new();
        key.extend_from_slice(&0u16.to_be_bytes());
        key.extend_from_slice(&src.to_be_bytes());
        key.extend_from_slice(&dst.to_be_bytes());
        key.extend_from_slice(&eid.to_be_bytes());
        fwd_sink.push(SortRecord::new(key, Vec::new())).unwrap();
    }
    let fwd_lease = fwd_sink.finish().unwrap();

    let geo = |_rel: u16| RelTableGeometry {
        rel_id: 0,
        src_rows: 3,
        dst_rows: 3,
    };

    let mut fwd_off_sink = Box::new(MemorySegmentSink::new(
        SegmentKind::ForwardCsrOffsets,
        1,
        1,
        4,
        4,
    ));
    let mut fwd_tgt_sink = Box::new(MemorySegmentSink::new(
        SegmentKind::ForwardCsrTargets,
        1,
        1,
        4,
        4,
    ));
    let mut rev_sink = store.sink("rev", &b).unwrap();
    let mut fwd_merger = store.merger("fwd").unwrap();

    stream_forward_csr(
        &fwd_lease,
        fwd_merger.as_mut(),
        &geo,
        fwd_off_sink.as_mut(),
        fwd_tgt_sink.as_mut(),
        rev_sink.as_mut(),
        &b,
        &mut metrics,
        None,
    )
    .unwrap();

    let fwd_off = read_u32s(&body_bytes(&fwd_off_sink.finish().unwrap().body));
    let fwd_tgt = read_u32s(&body_bytes(&fwd_tgt_sink.finish().unwrap().body));
    // 3 src rows + sentinel. Row0: edges 0,1,2 (positions 0-2). Row1: empty.
    // Row2: edge 3. Sentinel: 4.
    assert_eq!(fwd_off, vec![0, 3, 3, 4], "forward offsets");
    assert_eq!(fwd_tgt, vec![0, 1, 1, 1], "forward targets (dst offsets)");

    // Reverse pass.
    let rev_lease = rev_sink.finish().unwrap();
    let mut rev_off_sink = Box::new(MemorySegmentSink::new(
        SegmentKind::ReverseCsrOffsets,
        1,
        1,
        4,
        4,
    ));
    let mut rev_tgt_sink = Box::new(MemorySegmentSink::new(
        SegmentKind::ReverseCsrTargets,
        1,
        1,
        4,
        4,
    ));
    let mut pos_sink = Box::new(MemorySegmentSink::new(
        SegmentKind::ForwardPositions,
        1,
        1,
        4,
        4,
    ));
    let mut rev_merger = store.merger("rev").unwrap();
    stream_reverse_csr(
        &rev_lease,
        rev_merger.as_mut(),
        &geo,
        rev_off_sink.as_mut(),
        rev_tgt_sink.as_mut(),
        pos_sink.as_mut(),
        &b,
        &mut metrics,
        None,
    )
    .unwrap();

    let rev_off = read_u32s(&body_bytes(&rev_off_sink.finish().unwrap().body));
    let rev_tgt = read_u32s(&body_bytes(&rev_tgt_sink.finish().unwrap().body));
    let fwd_pos = read_u32s(&body_bytes(&pos_sink.finish().unwrap().body));

    // Reverse: dst0 has self-loop (fwd pos 0). dst1 has edges fwd pos 1,2,3.
    // dst2 empty. dst rows + sentinel.
    // Reverse records sorted by (dst,src,eid): (0,0,e0)→pos0, (1,0,e1)→pos1,
    // (1,0,e2)→pos2, (1,2,e3)→pos3.
    assert_eq!(rev_off, vec![0, 1, 4, 4], "reverse offsets");
    assert_eq!(rev_tgt, vec![0, 0, 0, 2], "reverse targets (src offsets)");
    assert_eq!(fwd_pos, vec![0, 1, 2, 3], "real forward positions");
}
