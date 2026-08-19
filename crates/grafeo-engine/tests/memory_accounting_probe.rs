//! ENGINE-MEMORY-ACCOUNTING.1 — production-scale RO accounting probe.
//!
//! Opens a COPY of the real account graph read-only and compares the
//! measured anonymous RSS delta across open against
//! `GrafeoDB::memory_usage().total_bytes`, dumping the full hierarchical
//! breakdown so the under-count is attributed to concrete components.
//! Env-gated: the path comes from `AM_MEMORY_ACCOUNTING_PROBE_PATH`; the
//! test is skipped (not failed) when unset, so normal CI never touches
//! production artifacts.
//!
//! Probe a COPY, never the live file: the server holds the RW flock on the
//! standalone `.grafeo`; RO open takes a shared lock which would contend.
//!
//! ```bash
//! cp /data/grafeo/am-personal.grafeo /data/tmp/probe-am-personal.grafeo
//! AM_MEMORY_ACCOUNTING_PROBE_PATH=/data/tmp/probe-am-personal.grafeo \
//! cargo test -p grafeo-engine --test memory_accounting_probe \
//!   --features "lpg,vector-index,mmap,compact-store,generation,generation-streaming,grafeo-file" -- --nocapture
//! ```

#![cfg(all(
    feature = "lpg",
    feature = "mmap",
    feature = "grafeo-file",
    not(feature = "temporal")
))]

use grafeo_engine::GrafeoDB;

fn rss_anon_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("RssAnon:") {
            return rest.trim().split_whitespace().next()?.parse().ok();
        }
    }
    None
}

/// Bucket anonymous mappings by size class (from /proc/self/maps) to
/// attribute retained anon residency to concrete allocations.
fn anon_map_buckets() -> Vec<(String, usize, u64)> {
    let mut buckets: Vec<(String, usize, u64)> = Vec::new();
    if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
        for line in maps.lines() {
            let mut parts = line.split_whitespace();
            let range = parts.next().unwrap_or("");
            let perms = parts.next().unwrap_or("");
            let offset = parts.next().unwrap_or("0");
            let _dev = parts.next().unwrap_or("");
            let _inode = parts.next().unwrap_or("");
            let pathname = parts.collect::<Vec<_>>().join(" ");
            let offset_zero = u64::from_str_radix(offset, 16).unwrap_or(1) == 0;
            let anon = pathname.trim().is_empty() && offset_zero;
            if !anon {
                continue;
            }
            let (start, end) = range.split_once('-').unwrap_or(("0", "0"));
            let size = u64::from_str_radix(end, 16).unwrap_or(0)
                - u64::from_str_radix(start, 16).unwrap_or(0);
            let class = if size >= 64 * 1024 * 1024 {
                ">=64MiB"
            } else if size >= 8 * 1024 * 1024 {
                "8-64MiB"
            } else if size >= 1 * 1024 * 1024 {
                "1-8MiB"
            } else if size >= 64 * 1024 {
                "64KiB-1MiB"
            } else {
                "<64KiB"
            };
            if let Some(b) = buckets.iter_mut().find(|(c, _, _)| c == class) {
                b.1 += 1;
                b.2 += size;
            } else {
                buckets.push((class.to_string(), 1, size));
            }
            let _ = perms;
        }
    }
    buckets.sort_by(|a, b| b.2.cmp(&a.2));
    buckets
}

fn rss_file_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("RssFile:") {
            return rest.trim().split_whitespace().next()?.parse().ok();
        }
    }
    None
}

#[test]
fn production_accounting_probe() {
    let Some(path) = std::env::var_os("AM_MEMORY_ACCOUNTING_PROBE_PATH") else {
        eprintln!("AM_MEMORY_ACCOUNTING_PROBE_PATH unset — skipping accounting probe");
        return;
    };

    let before_anon = rss_anon_kb().expect("RssAnon readable");
    let before_file = rss_file_kb().expect("RssFile readable");

    let db = GrafeoDB::open_read_only(&path)
        .unwrap_or_else(|e| panic!("RO open of probe copy {}: {e}", path.to_string_lossy()));

    let after_anon = rss_anon_kb().expect("RssAnon readable");
    let after_file = rss_file_kb().expect("RssFile readable");

    let usage = db.memory_usage();
    let stats = db.detailed_stats();

    // Detect the backing shape (2026-08-19): a layered DB holds a decoded
    // CompactStore base that memory_usage() must charge; a plain eager
    // LpgStore has no base. Report which one we have so the accounting
    // target is unambiguous.
    #[cfg(all(feature = "compact-store", feature = "lpg"))]
    {
        let is_layered = db.layered_store().is_some();
        println!("  layered_store present: {is_layered}");
        #[cfg(feature = "grafeo-file")]
        {
            let backing = db.compact_backing().map(|b| format!("{b:?}"));
            println!(
                "  compact_backing: {}",
                backing.unwrap_or_else(|| "None".to_string())
            );
        }
        #[cfg(feature = "mmap")]
        {
            println!(
                "  compact_tiered present: {}",
                db.compact_tiered().is_some()
            );
        }
        if let Some(layered) = db.layered_store() {
            println!(
                "  layered base memory_bytes(): {} KiB ({:.2} GiB)",
                layered.base_store_arc().memory_bytes() / 1024,
                layered.base_store_arc().memory_bytes() as f64 / (1024.0 * 1048576.0)
            );
            println!(
                "  layered overlay_memory_bytes(): {} KiB",
                layered.overlay_memory_bytes() / 1024
            );
        }
    }
    #[cfg(not(all(feature = "compact-store", feature = "lpg")))]
    {
        println!("  (compact-store/lpg features off — layered detection unavailable)");
    }

    let anon_delta_kb = after_anon.saturating_sub(before_anon);
    let file_delta_kb = after_file.saturating_sub(before_file);
    let reported_kb = usage.total_bytes / 1024;

    // Full hierarchical breakdown (MemoryUsage derives Serialize).
    let breakdown = serde_json::to_string_pretty(&usage).expect("usage serializes");

    println!("ACCOUNTING PROBE");
    println!(
        "  graph: nodes={} edges={} labels={}",
        stats.node_count, stats.edge_count, stats.label_count
    );
    println!(
        "  measured RssAnon delta across open: {} KiB ({:.2} GiB)",
        anon_delta_kb,
        anon_delta_kb as f64 / 1048576.0
    );
    println!(
        "  measured RssFile delta across open: {} KiB ({:.2} GiB)",
        file_delta_kb,
        file_delta_kb as f64 / 1048576.0
    );
    println!(
        "  reported memory_usage().total_bytes: {} KiB ({:.2} GiB)",
        reported_kb,
        reported_kb as f64 / 1048576.0
    );
    if anon_delta_kb > 0 {
        let ratio = reported_kb as f64 / anon_delta_kb as f64;
        println!("  reported / measured-anon ratio: {ratio:.2} (packet target >= 0.80)");
    }
    println!("  anon mapping buckets (virtual sizes, size-descending):");
    for (class, count, bytes) in anon_map_buckets() {
        println!(
            "    {:12} count={:<5} virtual={:.2} GiB",
            class,
            count,
            bytes as f64 / 1073741824.0
        );
    }
    println!("  breakdown:\n{breakdown}");

    assert!(
        anon_delta_kb > 0,
        "open must measurably allocate anon memory"
    );

    // Attribution discriminator (2026-08-19), two-phase, gdb-driven:
    //
    //   Phase A (DB open):   external `malloc_trim(0)` while db is resident,
    //                        then sample anon  -> anon_open_trimmed
    //   drop(db)
    //   Phase B (DB dropped): external `malloc_trim(0)` again, sample anon
    //                        -> anon_dropped_trimmed
    //
    //   retained_by_db ≈ anon_open_trimmed − anon_dropped_trimmed
    //
    // If `reported >= 0.80 * retained_by_db`, the accounting is honest about
    // what the DB actually retains and the open-path residual is allocator
    // slack (freed-but-not-returned / fragmentation) — fix = post-open trim,
    // NOT inflating memory_usage(). If `reported` falls far below
    // `retained_by_db`, real structures are missing from memory_usage().
    //
    // The workspace denies unsafe, so malloc_trim comes from an external gdb;
    // the probe publishes markers the driver waits for.
    let hold = std::env::var("AM_PROBE_HOLD_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if hold > 0 {
        println!(
            "TRIM_NOW_A pid={} anon_kb={}",
            std::process::id(),
            after_anon
        );
        std::thread::sleep(std::time::Duration::from_secs(hold));
        let anon_open_trimmed = rss_anon_kb().unwrap_or(0);
        println!("PHASE_A anon_open_trimmed_kb={anon_open_trimmed}");

        let pre_drop_anon = rss_anon_kb().unwrap_or(0);
        drop(usage);
        drop(stats);
        drop(db);
        std::thread::sleep(std::time::Duration::from_millis(500));

        println!("TRIM_NOW_B anon_kb={}", rss_anon_kb().unwrap_or(0));
        std::thread::sleep(std::time::Duration::from_secs(hold));
        let anon_dropped_trimmed = rss_anon_kb().unwrap_or(0);
        println!("PHASE_B anon_dropped_trimmed_kb={anon_dropped_trimmed}");

        println!(
            "  pre-drop anon: {} KiB ({:.2} GiB)",
            pre_drop_anon,
            pre_drop_anon as f64 / 1048576.0
        );
        let retained_by_db = anon_open_trimmed.saturating_sub(anon_dropped_trimmed);
        println!(
            "  retained_by_db (open_trimmed - dropped_trimmed): {} KiB ({:.2} GiB)",
            retained_by_db,
            retained_by_db as f64 / 1048576.0
        );
        if retained_by_db > 0 {
            let attribution = reported_kb as f64 / retained_by_db as f64;
            println!(
                "  reported / retained_by_db ratio: {attribution:.2} \
                 (>= 0.80 => accounting honest, residual is allocator slack)"
            );
        }
    } else {
        drop(usage);
        drop(stats);
        drop(db);
    }
}
