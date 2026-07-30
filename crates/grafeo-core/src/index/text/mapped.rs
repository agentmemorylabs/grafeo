//! Mapped TextIndex section payload v2 (G-E1.RO).
//!
//! ```text
//! [magic "GTXT"][version u8=2][flags u8][pad u16]
//! [index_count u32]
//! directory: index_count × {
//!   key_off u32, key_len u32,
//!   k1_bits u64, b_bits u64,
//!   term_count u32, terms_off u32,
//!   postings_off u32, doc_count u32, docs_off u32,
//!   total_length u64, pad u32
//! }  // 56 bytes each
//! keys UTF-8, terms (sorted), postings packs, doc_lengths (sorted)
//!
//! term record: term_len u16 | term utf8 | posting_start u32 | posting_count u32
//! posting record: node_id u64 | term_freq u32 | pad u32  (16 bytes)
//! doc record: node_id u64 | doc_len u32 | pad u32       (16 bytes)
//! ```

use std::collections::HashMap;

use bytes::Bytes;

use grafeo_common::types::NodeId;
use grafeo_common::utils::error::{Error, Result};

use super::{BM25Config, SimpleTokenizer, Tokenizer};

/// Magic for mapped TextIndex payload.
pub const TEXT_INDEX_MAGIC: &[u8; 4] = b"GTXT";
/// Mapped TextIndex payload version (v1 remains legacy bincode).
pub const TEXT_INDEX_MAPPED_VERSION: u8 = 2;
const HEADER_LEN: usize = 12;
const DIR_ENTRY_LEN: usize = 56;
const POSTING_REC: usize = 16;
const DOC_REC: usize = 16;

/// Accounting for mapped text indexes under RO open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TextIndexMemoryAccounting {
    /// Retained mapped section payload bytes.
    pub mapped_payload_bytes: u64,
    /// Proportional anonymous postings/terms (0 when fully mapped).
    pub anonymous_proportional_bytes: u64,
    /// Bounded owner/schema handles.
    pub anonymous_owner_bytes: u64,
    /// Number of text indexes.
    pub index_count: u32,
    /// Total posting entries across all indexes.
    pub posting_count: u64,
}

/// One mapped BM25 inverted index.
#[derive(Debug, Clone)]
pub struct MappedTextIndex {
    /// "label:property" key.
    pub key: String,
    config: BM25Config,
    data: Bytes,
    term_count: u32,
    terms_off: usize,
    postings_off: usize,
    doc_count: u32,
    docs_off: usize,
    total_length: u64,
}

impl MappedTextIndex {
    /// BM25 search over mapped postings (no proportional heap postings).
    pub fn search(&self, query: &str, k: usize) -> Vec<(NodeId, f64)> {
        let tokenizer = SimpleTokenizer::new();
        let query_tokens = tokenizer.tokenize(query);
        if query_tokens.is_empty() || self.doc_count == 0 {
            return Vec::new();
        }
        let n = f64::from(self.doc_count);
        let avg_dl = self.total_length as f64 / n;
        let mut scores: HashMap<NodeId, f64> = HashMap::new();

        for token in &query_tokens {
            let Some((df, start, count)) = self.find_term(token.as_str()) else {
                continue;
            };
            let df_f = f64::from(df);
            for i in 0..count {
                let (node_id, tf) = self.posting_at(start + i);
                let dl = f64::from(self.doc_len(node_id).unwrap_or(0));
                let tf_f = f64::from(tf);
                *scores.entry(node_id).or_insert(0.0) +=
                    self.bm25_term_score(df_f, tf_f, dl, n, avg_dl);
            }
        }

        let mut results: Vec<(NodeId, f64)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        results
    }

    /// Number of documents in this index.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.doc_count
    }

    /// Whether the index has no documents.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.doc_count == 0
    }

    fn bm25_term_score(&self, df: f64, tf: f64, dl: f64, n: f64, avg_dl: f64) -> f64 {
        let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
        let tf_component = (tf * (self.config.k1 + 1.0))
            / (tf + self.config.k1 * (1.0 - self.config.b + self.config.b * dl / avg_dl));
        idf * tf_component
    }

    fn find_term(&self, term: &str) -> Option<(u32, u32, u32)> {
        let mut lo = 0u32;
        let mut hi = self.term_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (t, _start, _count) = self.term_at(mid);
            if t < term {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= self.term_count {
            return None;
        }
        let (t, start, count) = self.term_at(lo);
        if t == term {
            Some((count, start, count))
        } else {
            None
        }
    }

    fn term_at(&self, index: u32) -> (&str, u32, u32) {
        // Walk term records from terms_off. Variable length; use offset table:
        // term_count × u32 offsets relative to terms body.
        let table = &self.data[self.terms_off..self.terms_off + self.term_count as usize * 4];
        let off_rel = u32::from_le_bytes(
            table[index as usize * 4..index as usize * 4 + 4]
                .try_into()
                .expect("4"),
        ) as usize;
        let base = self.terms_off + self.term_count as usize * 4 + off_rel;
        let term_len =
            u16::from_le_bytes(self.data[base..base + 2].try_into().expect("2")) as usize;
        let term = std::str::from_utf8(&self.data[base + 2..base + 2 + term_len]).unwrap_or("");
        let start = u32::from_le_bytes(
            self.data[base + 2 + term_len..base + 6 + term_len]
                .try_into()
                .expect("4"),
        );
        let count = u32::from_le_bytes(
            self.data[base + 6 + term_len..base + 10 + term_len]
                .try_into()
                .expect("4"),
        );
        (term, start, count)
    }

    fn posting_at(&self, index: u32) -> (NodeId, u32) {
        let base = self.postings_off + index as usize * POSTING_REC;
        let node_id = NodeId::new(u64::from_le_bytes(
            self.data[base..base + 8].try_into().expect("8"),
        ));
        let tf = u32::from_le_bytes(self.data[base + 8..base + 12].try_into().expect("4"));
        (node_id, tf)
    }

    fn doc_len(&self, id: NodeId) -> Option<u32> {
        let mut lo = 0u32;
        let mut hi = self.doc_count;
        let target = id.as_u64();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let base = self.docs_off + mid as usize * DOC_REC;
            let mid_id = u64::from_le_bytes(self.data[base..base + 8].try_into().expect("8"));
            if mid_id < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= self.doc_count {
            return None;
        }
        let base = self.docs_off + lo as usize * DOC_REC;
        let found = u64::from_le_bytes(self.data[base..base + 8].try_into().expect("8"));
        if found != target {
            return None;
        }
        Some(u32::from_le_bytes(
            self.data[base + 8..base + 12].try_into().expect("4"),
        ))
    }
}

/// Full mapped TextIndex section.
#[derive(Debug, Clone)]
pub struct MappedTextIndexSet {
    indexes: Vec<MappedTextIndex>,
    accounting: TextIndexMemoryAccounting,
    #[allow(dead_code)]
    data: Bytes,
}

impl MappedTextIndexSet {
    /// Indexes in the set.
    #[must_use]
    pub fn indexes(&self) -> &[MappedTextIndex] {
        &self.indexes
    }

    /// Accounting snapshot.
    #[must_use]
    pub fn accounting(&self) -> TextIndexMemoryAccounting {
        self.accounting
    }

    /// Lookup by "label:property" key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&MappedTextIndex> {
        self.indexes.iter().find(|i| i.key == key)
    }
}

/// Snapshot used when encoding from a live inverted index.
#[derive(Debug, Clone)]
pub struct TextIndexEncodeSnapshot {
    /// "label:property"
    pub key: String,
    /// BM25 k1 parameter.
    pub k1: f64,
    /// BM25 b parameter.
    pub b: f64,
    /// Sorted (term, postings).
    pub postings: Vec<(String, Vec<(NodeId, u32)>)>,
    /// Sorted (node_id, doc_len).
    pub doc_lengths: Vec<(NodeId, u32)>,
    /// Sum of document lengths for avgdl.
    pub total_length: u64,
}

/// Encode mapped TextIndex v2 payload.
pub fn encode_text_index_section(indexes: &[TextIndexEncodeSnapshot]) -> Result<Vec<u8>> {
    let mut keys = Vec::new();
    let mut key_spans: Vec<(u32, u32)> = Vec::new();
    let mut bodies: Vec<IndexBody> = Vec::new();

    for idx in indexes {
        let key_off = keys.len() as u32;
        keys.extend_from_slice(idx.key.as_bytes());
        key_spans.push((key_off, idx.key.len() as u32));

        let mut postings = idx.postings.clone();
        postings.sort_by(|(a, _), (b, _)| a.cmp(b));
        let mut docs = idx.doc_lengths.clone();
        docs.sort_by_key(|(id, _)| id.as_u64());

        // Build postings pack + term table
        let mut posting_blob = Vec::new();
        let mut term_offsets = Vec::new();
        let mut term_body = Vec::new();
        for (term, entries) in &postings {
            let start = (posting_blob.len() / POSTING_REC) as u32;
            for (nid, tf) in entries {
                posting_blob.extend_from_slice(&nid.as_u64().to_le_bytes());
                posting_blob.extend_from_slice(&tf.to_le_bytes());
                posting_blob.extend_from_slice(&0u32.to_le_bytes());
            }
            term_offsets.push(term_body.len() as u32);
            let tb = term.as_bytes();
            term_body.extend_from_slice(&(tb.len() as u16).to_le_bytes());
            term_body.extend_from_slice(tb);
            term_body.extend_from_slice(&start.to_le_bytes());
            term_body.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        }
        let mut terms_blob = Vec::with_capacity(term_offsets.len() * 4 + term_body.len());
        for off in &term_offsets {
            terms_blob.extend_from_slice(&off.to_le_bytes());
        }
        terms_blob.extend_from_slice(&term_body);

        let mut docs_blob = Vec::with_capacity(docs.len() * DOC_REC);
        for (nid, len) in &docs {
            docs_blob.extend_from_slice(&nid.as_u64().to_le_bytes());
            docs_blob.extend_from_slice(&len.to_le_bytes());
            docs_blob.extend_from_slice(&0u32.to_le_bytes());
        }

        bodies.push(IndexBody {
            k1: idx.k1,
            b: idx.b,
            term_count: postings.len() as u32,
            terms_blob,
            posting_count: (posting_blob.len() / POSTING_REC) as u32,
            posting_blob,
            doc_count: docs.len() as u32,
            docs_blob,
            total_length: idx.total_length,
        });
    }

    let dir_len = indexes.len() * DIR_ENTRY_LEN;
    let keys_off = HEADER_LEN + dir_len;
    // Lay out: keys | for each index: terms | postings | docs
    let mut payload = Vec::new();
    let mut dir = vec![0u8; dir_len];
    for (i, body) in bodies.iter().enumerate() {
        let (key_rel, key_len) = key_spans[i];
        let key_abs = (keys_off as u32).wrapping_add(key_rel);
        let base_off = keys_off + keys.len() + payload.len();
        let terms_off = base_off as u32;
        payload.extend_from_slice(&body.terms_blob);
        let postings_off = (keys_off + keys.len() + payload.len()) as u32;
        payload.extend_from_slice(&body.posting_blob);
        let docs_off = (keys_off + keys.len() + payload.len()) as u32;
        payload.extend_from_slice(&body.docs_blob);

        let d = i * DIR_ENTRY_LEN;
        dir[d..d + 4].copy_from_slice(&key_abs.to_le_bytes());
        dir[d + 4..d + 8].copy_from_slice(&key_len.to_le_bytes());
        dir[d + 8..d + 16].copy_from_slice(&body.k1.to_bits().to_le_bytes());
        dir[d + 16..d + 24].copy_from_slice(&body.b.to_bits().to_le_bytes());
        dir[d + 24..d + 28].copy_from_slice(&body.term_count.to_le_bytes());
        dir[d + 28..d + 32].copy_from_slice(&terms_off.to_le_bytes());
        dir[d + 32..d + 36].copy_from_slice(&postings_off.to_le_bytes());
        dir[d + 36..d + 40].copy_from_slice(&body.doc_count.to_le_bytes());
        dir[d + 40..d + 44].copy_from_slice(&docs_off.to_le_bytes());
        dir[d + 44..d + 52].copy_from_slice(&body.total_length.to_le_bytes());
        // pad u32 at 52..56 already zero
        let _ = body.posting_count;
    }

    let mut out = Vec::with_capacity(keys_off + keys.len() + payload.len());
    out.extend_from_slice(TEXT_INDEX_MAGIC);
    out.push(TEXT_INDEX_MAPPED_VERSION);
    out.push(0);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(indexes.len() as u32).to_le_bytes());
    out.extend_from_slice(&dir);
    out.extend_from_slice(&keys);
    out.extend_from_slice(&payload);
    Ok(out)
}

struct IndexBody {
    k1: f64,
    b: f64,
    term_count: u32,
    terms_blob: Vec<u8>,
    posting_count: u32,
    posting_blob: Vec<u8>,
    doc_count: u32,
    docs_blob: Vec<u8>,
    total_length: u64,
}

/// Parse mapped TextIndex v2 (or reject other versions).
pub fn parse_text_index_section(data: Bytes) -> Result<MappedTextIndexSet> {
    if data.len() < HEADER_LEN {
        return Err(Error::Serialization(
            "TextIndex section truncated header".into(),
        ));
    }
    if &data[0..4] != TEXT_INDEX_MAGIC {
        return Err(Error::Serialization(format!(
            "TextIndex bad magic (expected GTXT mapped v2): {:?}",
            &data[0..4]
        )));
    }
    let version = data[4];
    if version != TEXT_INDEX_MAPPED_VERSION {
        return Err(Error::Serialization(format!(
            "Unsupported TextIndex mapped version {version}; expected {TEXT_INDEX_MAPPED_VERSION}"
        )));
    }
    let index_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let dir_end = HEADER_LEN + index_count * DIR_ENTRY_LEN;
    if data.len() < dir_end {
        return Err(Error::Serialization(
            "TextIndex section truncated directory".into(),
        ));
    }

    let mut indexes = Vec::with_capacity(index_count);
    let mut posting_count = 0u64;
    for i in 0..index_count {
        let d = HEADER_LEN + i * DIR_ENTRY_LEN;
        let key_off = u32::from_le_bytes(data[d..d + 4].try_into().unwrap()) as usize;
        let key_len = u32::from_le_bytes(data[d + 4..d + 8].try_into().unwrap()) as usize;
        let k1 = f64::from_bits(u64::from_le_bytes(data[d + 8..d + 16].try_into().unwrap()));
        let b = f64::from_bits(u64::from_le_bytes(data[d + 16..d + 24].try_into().unwrap()));
        let term_count = u32::from_le_bytes(data[d + 24..d + 28].try_into().unwrap());
        let terms_off = u32::from_le_bytes(data[d + 28..d + 32].try_into().unwrap()) as usize;
        let postings_off = u32::from_le_bytes(data[d + 32..d + 36].try_into().unwrap()) as usize;
        let doc_count = u32::from_le_bytes(data[d + 36..d + 40].try_into().unwrap());
        let docs_off = u32::from_le_bytes(data[d + 40..d + 44].try_into().unwrap()) as usize;
        let total_length = u64::from_le_bytes(data[d + 44..d + 52].try_into().unwrap());

        let key_end = key_off
            .checked_add(key_len)
            .ok_or_else(|| Error::Serialization("TextIndex key overflow".into()))?;
        if key_end > data.len() {
            return Err(Error::Serialization("TextIndex key out of bounds".into()));
        }
        let key = std::str::from_utf8(&data[key_off..key_end])
            .map_err(|e| Error::Serialization(format!("TextIndex key UTF-8: {e}")))?
            .to_string();

        // Count postings from last term if any
        if term_count > 0 {
            // Approximate: measure postings region until docs_off
            if docs_off >= postings_off {
                posting_count += ((docs_off - postings_off) / POSTING_REC) as u64;
            }
        }

        indexes.push(MappedTextIndex {
            key,
            config: BM25Config { k1, b },
            data: data.clone(),
            term_count,
            terms_off,
            postings_off,
            doc_count,
            docs_off,
            total_length,
        });
    }

    let owner = std::mem::size_of::<MappedTextIndexSet>()
        + indexes.len() * std::mem::size_of::<MappedTextIndex>()
        + indexes.iter().map(|i| i.key.len()).sum::<usize>();

    Ok(MappedTextIndexSet {
        accounting: TextIndexMemoryAccounting {
            mapped_payload_bytes: data.len() as u64,
            anonymous_proportional_bytes: 0,
            anonymous_owner_bytes: owner as u64,
            index_count: index_count as u32,
            posting_count,
        },
        indexes,
        data,
    })
}

/// Detect whether `data` is a mapped GTXT v2 payload.
#[must_use]
pub fn is_mapped_text_payload(data: &[u8]) -> bool {
    data.len() >= 5 && &data[0..4] == TEXT_INDEX_MAGIC && data[4] == TEXT_INDEX_MAPPED_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::text::{BM25Config, InvertedIndex};

    #[test]
    fn mapped_text_search_parity_with_heap() {
        let mut heap = InvertedIndex::new(BM25Config::default());
        heap.insert(NodeId::new(1), "the quick brown fox jumps");
        heap.insert(NodeId::new(2), "a lazy brown dog sleeps");
        heap.insert(NodeId::new(3), "quick silver fox");
        let (postings, doc_lengths, total_length) = heap.snapshot();
        let cfg = heap.config().clone();
        let snap = TextIndexEncodeSnapshot {
            key: "Article:body".into(),
            k1: cfg.k1,
            b: cfg.b,
            postings,
            doc_lengths,
            total_length,
        };
        let bytes = encode_text_index_section(&[snap]).unwrap();
        let set = parse_text_index_section(Bytes::from(bytes)).unwrap();
        assert_eq!(set.accounting().anonymous_proportional_bytes, 0);
        let mapped = set.get("Article:body").unwrap();
        let heap_hits = heap.search("brown fox", 10);
        let mapped_hits = mapped.search("brown fox", 10);
        assert_eq!(heap_hits.len(), mapped_hits.len());
        // Same top document
        assert_eq!(heap_hits[0].0, mapped_hits[0].0);
    }

    #[test]
    fn mapped_text_bad_magic_fails() {
        assert!(parse_text_index_section(Bytes::from(vec![0u8; 20])).is_err());
    }
}
