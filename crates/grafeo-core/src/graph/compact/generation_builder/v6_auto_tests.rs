//! G4 `Auto` version-selection geometry (round-1 review M1).

#![cfg(feature = "generation-streaming")]

use crate::graph::compact::mapped::{
    PayloadVersion, PayloadVersionPolicy, read_block_index_record,
};

/// Round-1 review M1: v5's limits are per field (body offset ≤ u32::MAX,
/// body length ≤ u32::MAX), not on the end. 300k rows × two 2048-wide f32
/// properties is a valid v5 payload at trunk: each body is 2,457,600,007
/// bytes, the second starts at 2,457,600,007, and the end (4,915,200,014)
/// passes u32::MAX. `Auto` must keep it v5. A third such column starts
/// past u32::MAX and must move the payload to v6.
#[test]
fn auto_keeps_v5_when_only_the_end_passes_u32() {
    use crate::graph::compact::generation_builder::emit_columns::{
        ColumnEmissionResult, EmittedColumn, write_directory_segments,
    };
    use crate::graph::compact::generation_builder::emit_meta::CodecKind;

    let body: u64 = 1 + 2 + 4 + 300_000 * 2048 * 4;
    assert_eq!(body, 2_457_600_007);
    let column = |i: u32| EmittedColumn {
        column_index: i,
        table_id: 0,
        key: format!("embedding_{i}"),
        kind: CodecKind::Float32Vector,
        body_len: body,
        body_start: u64::from(i) * body,
        codec_len: 300_000,
        requires_v6: false,
        block_zone_maps: Vec::new(),
    };
    let write_v5 = |result: &ColumnEmissionResult| {
        let indices: Vec<u32> = result.columns.iter().map(|c| c.column_index).collect();
        let (mut nd, mut rd, mut cd, mut bi) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        write_directory_segments(
            PayloadVersion::V5,
            &result.columns,
            &[(0, 300_000, indices)],
            &[],
            &mut nd,
            &mut rd,
            &mut cd,
            &mut bi,
        )
        .map(|()| bi)
    };

    let two = ColumnEmissionResult {
        columns: vec![column(0), column(1)],
        ..ColumnEmissionResult::default()
    };
    assert_eq!(two.columns[1].body_start + body, 4_915_200_014);
    assert!(!two.requires_v6(), "every v5 field fits: Auto must stay v5");
    assert_eq!(
        PayloadVersionPolicy::Auto.resolve(two.requires_v6()),
        PayloadVersion::V5
    );
    let block_index = write_v5(&two).expect("trunk's v5 writer accepts this geometry");
    let second = read_block_index_record(&block_index, PayloadVersion::V5, 1).unwrap();
    assert_eq!((second.body_offset, second.body_len), (body, body));
    // 614,400,000 components per column: the narrow vector header fits too.
    assert!(!PayloadVersionPolicy::Auto.wide_vector_header(300_000 * 2048));

    let three = ColumnEmissionResult {
        columns: vec![column(0), column(1), column(2)],
        ..ColumnEmissionResult::default()
    };
    assert!(three.requires_v6(), "the third column starts past u32::MAX");
    assert!(write_v5(&three).is_err());
}
