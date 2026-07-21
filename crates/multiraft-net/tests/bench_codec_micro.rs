//! Microbench: JSON string vs bincode for Raft-like payloads.
//!
//! ```bash
//! cargo test -p multiraft-net --test bench_codec_micro -- --nocapture
//! ```

use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Serialize, Deserialize)]
struct FakeAppend {
    vote_term: u64,
    prev_index: u64,
    entries: Vec<FakeEntry>,
}

#[derive(Clone, Serialize, Deserialize)]
struct FakeEntry {
    index: u64,
    term: u64,
    data: Vec<u8>,
}

#[test]
fn json_vs_bincode_append_batch() {
    let entries: Vec<_> = (1..=64)
        .map(|i| FakeEntry {
            index: i,
            term: 1,
            data: vec![7u8; 64],
        })
        .collect();
    let msg = FakeAppend {
        vote_term: 3,
        prev_index: 0,
        entries,
    };

    let rounds = 20_000u64;

    let t0 = Instant::now();
    let mut json_bytes = 0usize;
    for _ in 0..rounds {
        let s = serde_json::to_string(&msg).unwrap();
        json_bytes = s.len();
        let _: FakeAppend = serde_json::from_str(&s).unwrap();
    }
    let json_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t1 = Instant::now();
    let mut bin_bytes = 0usize;
    for _ in 0..rounds {
        let b = bincode::serialize(&msg).unwrap();
        bin_bytes = b.len();
        let _: FakeAppend = bincode::deserialize(&b).unwrap();
    }
    let bin_ms = t1.elapsed().as_secs_f64() * 1000.0;

    println!(
        "{}",
        serde_json::json!({
            "rounds": rounds,
            "entries_per_msg": 64,
            "json_payload_bytes": json_bytes,
            "bincode_payload_bytes": bin_bytes,
            "json_roundtrip_ms": json_ms,
            "bincode_roundtrip_ms": bin_ms,
            "size_ratio_json_over_bin": (json_bytes as f64) / (bin_bytes as f64),
            "speedup_x": json_ms / bin_ms.max(0.001),
        })
    );

    assert!(bin_bytes < json_bytes);
    assert!(bin_ms < json_ms);
}
