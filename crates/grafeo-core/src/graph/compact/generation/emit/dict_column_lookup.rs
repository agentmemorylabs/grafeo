//! Per-column dictionary lookup without a resident string/offset map (G-EM0.5b).
//!
//! Each Dict column's distinct `(string, global_code)` pairs live in a sorted
//! on-disk chunk. Lookup binary-searches via on-demand offset reads and
//! compares strings via seek-read or a mapped `Bytes` view. Neither strings
//! nor the offset table are copied into anonymous `Vec`/`HashMap` state.
//!
//! On-disk layout (LE):
//! ```text
//! [count u32]
//! [count × u64 entry_offsets]   // relative to entries region start
//! [count × (str_len u32, string, code u32)] sorted by string bytes
//! ```

use crate::graph::compact::generation::error::GenerationError;
use bytes::Bytes;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Lookup trait for Dict column string→code resolution.
pub trait DictCodeLookup {
    /// Returns the global dictionary code for `s`, if interned in this column.
    fn code_of(&mut self, s: &[u8]) -> Option<u32>;
}

/// No-op lookup for columns without DictValue chunks.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyDictLookup;

impl DictCodeLookup for EmptyDictLookup {
    fn code_of(&mut self, _s: &[u8]) -> Option<u32> {
        None
    }
}

/// Seek- or mmap-backed lookup for one Dict column chunk.
///
/// Offset table entries are read from the mapped/seek view on demand — never
/// copied into a resident `Vec<u64>`.
pub struct DictColumnLookup {
    mapped: Option<Bytes>,
    file: Option<File>,
    count: u32,
    data_base: u64,
}

impl DictColumnLookup {
    /// Opens a finished `.dict` chunk file.
    ///
    /// # Errors
    ///
    /// I/O or malformed-chunk failure.
    pub fn open(path: &Path) -> Result<Self, GenerationError> {
        #[cfg(all(unix, feature = "mmap"))]
        {
            if let Ok(lookup) = Self::open_mapped(path) {
                return Ok(lookup);
            }
        }
        Self::open_seek(path)
    }

    #[cfg(all(unix, feature = "mmap"))]
    fn open_mapped(path: &Path) -> Result<Self, GenerationError> {
        use memmap2::Mmap;

        let file = File::open(path)
            .map_err(|e| GenerationError::Io(format!("open dict chunk {}: {e}", path.display())))?;
        let meta = file
            .metadata()
            .map_err(|e| GenerationError::Io(format!("stat dict chunk {}: {e}", path.display())))?;
        if meta.len() < 4 {
            return Err(GenerationError::Codec("dict chunk too short".into()));
        }
        #[allow(unsafe_code)]
        let mmap = unsafe { Mmap::map(&file) }
            .map_err(|e| GenerationError::Io(format!("mmap dict chunk {}: {e}", path.display())))?;
        struct MmapOwner(memmap2::Mmap);
        impl AsRef<[u8]> for MmapOwner {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }
        let bytes = Bytes::from_owner(MmapOwner(mmap));
        let count = read_u32_at(&bytes, 0)?;
        Ok(Self {
            mapped: Some(bytes),
            file: None,
            count,
            data_base: 4 + u64::from(count) * 8,
        })
    }

    fn open_seek(path: &Path) -> Result<Self, GenerationError> {
        let mut file = File::open(path)
            .map_err(|e| GenerationError::Io(format!("open dict chunk {}: {e}", path.display())))?;
        let count = read_u32(&mut file)?;
        Ok(Self {
            mapped: None,
            file: Some(file),
            count,
            data_base: 4 + u64::from(count) * 8,
        })
    }

    fn offset_at(&mut self, idx: usize) -> Result<u64, GenerationError> {
        if let Some(m) = &self.mapped {
            return read_u64_at(m, 4 + idx * 8);
        }
        let f = self
            .file
            .as_mut()
            .ok_or_else(|| GenerationError::Codec("dict chunk seek handle missing".into()))?;
        f.seek(SeekFrom::Start(4 + (idx as u64) * 8))
            .map_err(|e| GenerationError::Io(format!("seek dict offset: {e}")))?;
        read_u64(f)
    }

    fn compare_entry(&mut self, idx: usize, needle: &[u8]) -> Result<Ordering, GenerationError> {
        let rel = self.offset_at(idx)?;
        if let Some(m) = &self.mapped {
            let (s, _) = entry_at_mapped(m, self.data_base, rel)?;
            return Ok(s.cmp(needle));
        }
        let f = self
            .file
            .as_mut()
            .ok_or_else(|| GenerationError::Codec("dict chunk seek handle missing".into()))?;
        f.seek(SeekFrom::Start(self.data_base + rel))
            .map_err(|e| GenerationError::Io(format!("seek dict entry: {e}")))?;
        let slen = read_u32(f)? as usize;
        if slen != needle.len() {
            let mut buf = vec![0u8; slen.min(4096)];
            if slen <= buf.len() {
                f.read_exact(&mut buf[..slen])
                    .map_err(|e| GenerationError::Io(format!("read dict string: {e}")))?;
                return Ok(buf[..slen].cmp(needle));
            }
            // Long strings: stream-compare without retaining the whole string.
            let mut pos = 0usize;
            while pos < slen {
                let to_read = buf.len().min(slen - pos);
                f.read_exact(&mut buf[..to_read])
                    .map_err(|e| GenerationError::Io(format!("read dict string: {e}")))?;
                let needle_slice = if pos < needle.len() {
                    &needle[pos..needle.len().min(pos + to_read)]
                } else {
                    &[]
                };
                let left = &buf[..to_read];
                // Compare available prefix; length mismatch decides when one side ends.
                let cmp_len = left.len().min(needle_slice.len());
                let cmp = left[..cmp_len].cmp(&needle_slice[..cmp_len]);
                if cmp != Ordering::Equal {
                    return Ok(cmp);
                }
                if left.len() != needle_slice.len() {
                    return Ok(left.len().cmp(&needle_slice.len()));
                }
                pos += to_read;
            }
            return Ok(slen.cmp(&needle.len()));
        }
        let mut pos = 0usize;
        while pos < slen {
            let mut chunk = [0u8; 64];
            let to_read = chunk.len().min(slen - pos);
            f.read_exact(&mut chunk[..to_read])
                .map_err(|e| GenerationError::Io(format!("read dict string: {e}")))?;
            let cmp = chunk[..to_read].cmp(&needle[pos..pos + to_read]);
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
            pos += to_read;
        }
        Ok(Ordering::Equal)
    }

    fn code_at(&mut self, idx: usize) -> Result<u32, GenerationError> {
        let rel = self.offset_at(idx)?;
        if let Some(m) = &self.mapped {
            let (_, code) = entry_at_mapped(m, self.data_base, rel)?;
            return Ok(code);
        }
        let f = self
            .file
            .as_mut()
            .ok_or_else(|| GenerationError::Codec("dict chunk seek handle missing".into()))?;
        f.seek(SeekFrom::Start(self.data_base + rel))
            .map_err(|e| GenerationError::Io(format!("seek dict entry: {e}")))?;
        let slen = read_u32(f)? as usize;
        f.seek(SeekFrom::Current(slen as i64))
            .map_err(|e| GenerationError::Io(format!("skip dict string: {e}")))?;
        read_u32(f)
    }

    /// Byte length of the opened chunk (for mapped ledger charging).
    #[must_use]
    pub fn byte_len(&self) -> u64 {
        if let Some(m) = &self.mapped {
            return m.len() as u64;
        }
        self.data_base // lower bound when seek-backed
    }
}

impl DictCodeLookup for DictColumnLookup {
    fn code_of(&mut self, needle: &[u8]) -> Option<u32> {
        if self.count == 0 {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = self.count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.compare_entry(mid, needle) {
                Ok(Ordering::Less) => lo = mid + 1,
                Ok(Ordering::Greater) => hi = mid,
                Ok(Ordering::Equal) => return self.code_at(mid).ok(),
                Err(_) => return None,
            }
        }
        None
    }
}

fn entry_at_mapped(
    m: &Bytes,
    data_base: u64,
    rel: u64,
) -> Result<(&[u8], u32), GenerationError> {
    let base = data_base as usize + rel as usize;
    let slen = read_u32_at(m, base)? as usize;
    let str_start = base + 4;
    let str_end = str_start + slen;
    let code_off = str_end;
    if code_off + 4 > m.len() {
        return Err(GenerationError::Codec("dict chunk entry truncated".into()));
    }
    let s = &m[str_start..str_end];
    let code = u32::from_le_bytes(m[code_off..code_off + 4].try_into().unwrap());
    Ok((s, code))
}

/// Sequential catalog over per-column `.dict` files written during remap consume.
///
/// Catalog entry layout (LE, repeated until EOF):
/// `[tid u16][prop_len u16][prop][path_len u16][relative_path]`
pub struct DictChunkCatalog {
    reader: Box<dyn Read>,
    temp_dir: PathBuf,
    pending: Option<(u16, Vec<u8>, PathBuf)>,
}

impl DictChunkCatalog {
    /// Opens the catalog at `path`; chunk files resolve under `temp_dir`.
    ///
    /// # Errors
    ///
    /// I/O failure opening the catalog.
    pub fn open(path: &Path, temp_dir: &Path) -> Result<Self, GenerationError> {
        let file = File::open(path).map_err(|e| {
            GenerationError::Io(format!("open dict catalog {}: {e}", path.display()))
        })?;
        Ok(Self {
            reader: Box::new(file),
            temp_dir: temp_dir.to_path_buf(),
            pending: None,
        })
    }

    /// Loads the lookup for `(tid, prop)`, or `None` when the column has no chunk.
    ///
    /// # Errors
    ///
    /// Codec/order or I/O failure.
    pub fn lookup_for(
        &mut self,
        tid: u16,
        prop: &str,
    ) -> Result<Option<DictColumnLookup>, GenerationError> {
        let target: (u16, &[u8]) = (tid, prop.as_bytes());
        if self.pending.is_none() {
            match self.read_entry()? {
                None => return Ok(None),
                Some(entry) => self.pending = Some(entry),
            }
        }
        let (ptid, pprop, _) = self.pending.as_ref().expect("just set");
        match (*ptid, pprop.as_slice()).cmp(&target) {
            Ordering::Equal => {
                let (_, _, path) = self.pending.take().expect("checked");
                DictColumnLookup::open(&path).map(Some)
            }
            Ordering::Greater => Ok(None),
            Ordering::Less => Err(GenerationError::Codec(format!(
                "dict catalog order mismatch: entry for table {ptid} column {} before table {tid} column {prop}",
                String::from_utf8_lossy(pprop)
            ))),
        }
    }

    /// Fails closed unless every catalog entry has been consumed.
    ///
    /// # Errors
    ///
    /// Codec or I/O failure.
    pub fn verify_drained(&mut self) -> Result<(), GenerationError> {
        if self.pending.is_some() {
            return Err(GenerationError::Codec(
                "dict catalog entry left unconsumed".into(),
            ));
        }
        match self.read_entry()? {
            None => Ok(()),
            Some(_) => Err(GenerationError::Codec(
                "dict catalog entry left unconsumed".into(),
            )),
        }
    }

    fn read_entry(&mut self) -> Result<Option<(u16, Vec<u8>, PathBuf)>, GenerationError> {
        let mut hdr = [0u8; 4];
        if !read_full(self.reader.as_mut(), &mut hdr)? {
            return Ok(None);
        }
        let tid = u16::from_le_bytes([hdr[0], hdr[1]]);
        let prop_len = u16::from_le_bytes([hdr[2], hdr[3]]) as usize;
        let mut prop = vec![0u8; prop_len];
        if !read_full(self.reader.as_mut(), &mut prop)? {
            return Err(GenerationError::Codec("dict catalog prop truncated".into()));
        }
        let mut plen = [0u8; 2];
        if !read_full(self.reader.as_mut(), &mut plen)? {
            return Err(GenerationError::Codec("dict catalog path len truncated".into()));
        }
        let path_len = u16::from_le_bytes(plen) as usize;
        let mut rel = vec![0u8; path_len];
        if !read_full(self.reader.as_mut(), &mut rel)? {
            return Err(GenerationError::Codec("dict catalog path truncated".into()));
        }
        let rel = std::str::from_utf8(&rel)
            .map_err(|_| GenerationError::Codec("dict catalog path not UTF-8".into()))?;
        Ok(Some((tid, prop, self.temp_dir.join(rel))))
    }
}

fn read_full(r: &mut dyn Read, buf: &mut [u8]) -> Result<bool, GenerationError> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(GenerationError::Codec("dict catalog truncated".into()));
            }
            Ok(n) => filled += n,
            Err(e) => return Err(GenerationError::Io(e.to_string())),
        }
    }
    Ok(true)
}

fn read_u32(r: &mut File) -> Result<u32, GenerationError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)
        .map_err(|e| GenerationError::Io(format!("read u32: {e}")))?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64(r: &mut File) -> Result<u64, GenerationError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)
        .map_err(|e| GenerationError::Io(format!("read u64: {e}")))?;
    Ok(u64::from_le_bytes(b))
}

fn read_u32_at(buf: &[u8], off: usize) -> Result<u32, GenerationError> {
    buf.get(off..off + 4)
        .ok_or_else(|| GenerationError::Codec("dict chunk header truncated".into()))
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
}

fn read_u64_at(buf: &[u8], off: usize) -> Result<u64, GenerationError> {
    buf.get(off..off + 8)
        .ok_or_else(|| GenerationError::Codec("dict chunk offsets truncated".into()))
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_dict(path: &Path, entries: &[(&str, u32)]) {
        let mut body = Vec::new();
        let mut offsets = Vec::new();
        for (s, code) in entries {
            offsets.push(body.len() as u64);
            let slen = u32::try_from(s.len()).unwrap();
            body.extend_from_slice(&slen.to_le_bytes());
            body.extend_from_slice(s.as_bytes());
            body.extend_from_slice(&code.to_le_bytes());
        }
        let count = u32::try_from(entries.len()).unwrap();
        let mut file = File::create(path).unwrap();
        file.write_all(&count.to_le_bytes()).unwrap();
        for off in &offsets {
            file.write_all(&off.to_le_bytes()).unwrap();
        }
        file.write_all(&body).unwrap();
    }

    #[test]
    fn dict_column_lookup_binary_search() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("col.dict");
        write_dict(&path, &[("alice", 1), ("bob", 2), ("carol", 3)]);
        let mut lookup = DictColumnLookup::open(&path).expect("open");
        assert_eq!(lookup.code_of(b"alice"), Some(1));
        assert_eq!(lookup.code_of(b"bob"), Some(2));
        assert_eq!(lookup.code_of(b"zzz"), None);
    }

    #[test]
    fn dict_chunk_catalog_order() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.dict");
        let b = dir.path().join("b.dict");
        write_dict(&a, &[("x", 1)]);
        write_dict(&b, &[("y", 2)]);
        let cat_path = dir.path().join("catalog.cat");
        let mut cat = File::create(&cat_path).unwrap();
        for (tid, prop, rel) in [(0u16, "name", "a.dict"), (0u16, "title", "b.dict")] {
            cat.write_all(&tid.to_le_bytes()).unwrap();
            let prop_b = prop.as_bytes();
            cat.write_all(&(prop_b.len() as u16).to_le_bytes()).unwrap();
            cat.write_all(prop_b).unwrap();
            cat.write_all(&(rel.len() as u16).to_le_bytes()).unwrap();
            cat.write_all(rel.as_bytes()).unwrap();
        }
        drop(cat);
        let mut catalog = DictChunkCatalog::open(&cat_path, dir.path()).unwrap();
        assert!(catalog.lookup_for(0, "age").unwrap().is_none());
        let mut lk = catalog.lookup_for(0, "name").unwrap().expect("name chunk");
        assert_eq!(lk.code_of(b"x"), Some(1));
        let mut lk = catalog.lookup_for(0, "title").unwrap().expect("title chunk");
        assert_eq!(lk.code_of(b"y"), Some(2));
        catalog.verify_drained().expect("drained");
    }
}
