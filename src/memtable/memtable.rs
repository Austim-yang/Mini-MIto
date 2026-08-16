use std::{
    fs::{self},
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use serde::{Deserialize, Serialize};

use crate::{
    memtable::{
        SkipList, Wal,
        traits::{ImmutableMemtable, Memtable},
        wal::Operation,
    },
    schema::TableSchema,
    sstable::sstable::{SSTable, SstableIndex},
    types::{Key, Value},
};

#[derive(Serialize, Deserialize)]
pub struct ManifestEntry {
    id: usize,
    path: String,
    min_key: Key,
    max_key: Key,
    entry_count: usize,
}

pub struct MutableSkipListMemtable {
    inner: Arc<SkipList>,
    wal: Arc<Mutex<Wal>>,
    wal_path: PathBuf,
}

impl Memtable for MutableSkipListMemtable {
    fn write(&self, key: Key, seq: u64, value: Option<Value>) -> io::Result<Option<Value>> {
        let op = match &value {
            Some(v) => Operation::Insert {
                key: key.clone(),
                seq,
                value: v.clone(),
            },
            None => Operation::Delete {
                key: key.clone(),
                seq,
            },
        };
        self.wal.lock().unwrap().append(&op)?;
        Ok(self.inner.insert(key, seq, value))
    }

    fn write_batch(&self, entries: Vec<(Key, u64, Option<Value>)>) -> io::Result<()> {
        let mut wal_guard = self.wal.lock().unwrap();
        for (key, seq, value) in &entries {
            let op = match value {
                Some(v) => Operation::Insert {
                    key: key.clone(),
                    seq: *seq,
                    value: v.clone(),
                },
                None => Operation::Delete {
                    key: key.clone(),
                    seq: *seq,
                },
            };
            wal_guard.append(&op)?;
        }
        for (key, seq, value) in entries {
            self.inner.insert(key, seq, value);
        }
        Ok(())
    }

    fn replay(&self, op: &Operation) -> io::Result<()> {
        match op {
            Operation::Insert { key, seq, value } | Operation::Update { key, seq, value } => {
                self.inner.insert(key.clone(), *seq, Some(value.clone()));
            }
            Operation::Delete { key, seq } => {
                self.inner.insert(key.clone(), *seq, None);
            }
        }
        Ok(())
    }

    fn get(&self, key: &Key) -> io::Result<Option<(u64, Option<Value>)>> {
        Ok(self.inner.get(key))
    }

    fn max_seq(&self) -> u64 {
        self.inner.max_seq()
    }

    fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, u64, Option<Value>)>> {
        let mut results = Vec::new();
        for (k, seq, v) in self.inner.iter() {
            if &k >= start && &k <= end {
                results.push((k, seq, v));
            }
        }
        Ok(results)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Key, u64, Option<Value>)> + '_> {
        Box::new(self.inner.iter())
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn estimated_size(&self) -> usize {
        self.inner.len() * 64
    }

    fn freeze(&self) -> io::Result<Box<dyn ImmutableMemtable>> {
        self.wal.lock().unwrap().close()?;
        Ok(Box::new(ImmutableSkipListMemtable {
            inner: self.inner.clone(),
            wal_path: self.wal_path.clone(),
        }))
    }

    fn fork(&self) -> io::Result<Box<dyn Memtable>> {
        let parent = self.wal_path.parent().unwrap();
        let stem = self.wal_path.file_stem().unwrap().to_str().unwrap();
        let seq: usize = stem.trim_start_matches("wal_").parse().unwrap_or(0);
        let new_seq = seq + 1;
        let new_path = parent.join(format!("wal_{:03}.log", new_seq));
        Ok(Box::new(MutableSkipListMemtable::new(new_path)?))
    }

    fn close(&self) -> io::Result<()> {
        self.wal.lock().unwrap().close()
    }
}

impl MutableSkipListMemtable {
    fn new(wal_path: PathBuf) -> io::Result<Self> {
        let wal = Wal::new(&wal_path)?;
        let mem = Self {
            inner: Arc::new(SkipList::new()),
            wal: Arc::new(Mutex::new(wal)),
            wal_path,
        };
        {
            let wal = mem.wal.lock().unwrap();
            wal.recover(&mut |op: &Operation| {
                let _ = mem.replay(op);
            })?;
        }
        Ok(mem)
    }
}

use std::fmt;

impl fmt::Debug for MutableSkipListMemtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Memtable")
            .field("wal_path", &self.wal_path)
            .finish()
    }
}

pub struct ImmutableSkipListMemtable {
    inner: Arc<SkipList>,
    wal_path: PathBuf,
}

impl ImmutableMemtable for ImmutableSkipListMemtable {
    fn get(&self, key: &Key) -> io::Result<Option<(u64, Option<Value>)>> {
        Ok(self.inner.get(key))
    }

    fn max_seq(&self) -> u64 {
        self.inner.max_seq()
    }

    fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, u64, Option<Value>)>> {
        let mut results = Vec::new();
        for (k, seq, v) in self.inner.iter() {
            if &k >= start && &k <= end {
                results.push((k, seq, v));
            }
        }
        Ok(results)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Key, u64, Option<Value>)> + '_> {
        Box::new(self.inner.iter())
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn estimated_size(&self) -> usize {
        self.inner.len() * 64
    }

    fn to_sstable(&self, id: usize, path: &Path, schema: &TableSchema) -> io::Result<SSTable> {
        SSTable::create_from_skiplist(&self.inner, id, path, true, schema)
    }

    fn wal_path(&self) -> &Path {
        &self.wal_path
    }
}

pub struct MemtableManager {
    active: Arc<RwLock<Option<Box<dyn Memtable>>>>,
    immutables: Arc<RwLock<Vec<Box<dyn ImmutableMemtable>>>>,
    sst_id: AtomicUsize,
    seq: AtomicU64,
    base_dir: PathBuf,
    max_memory_bytes: usize,
    flush_threshold: usize,
    manifest_path: PathBuf,
    immutable_ssts: Arc<RwLock<Vec<SSTable>>>,
    schema: Arc<TableSchema>,
}

impl MemtableManager {
    pub fn new<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        Self::with_schema(wal_path, Arc::new(TableSchema::default_table()))
    }

    pub fn with_schema<P: AsRef<Path>>(wal_path: P, schema: Arc<TableSchema>) -> io::Result<Self> {
        let base_dir = wal_path
            .as_ref()
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf();
        let manifest_path = base_dir.join("manifest");
        let initial_wal = base_dir.join("wal_000.log");
        let active_mem = MutableSkipListMemtable::new(initial_wal)?;
        let mut mgr = Self {
            active: Arc::new(RwLock::new(Some(Box::new(active_mem)))),
            immutables: Arc::new(RwLock::new(Vec::new())),
            sst_id: AtomicUsize::new(0),
            seq: AtomicU64::new(0),
            base_dir: base_dir.clone(),
            max_memory_bytes: 1024 * 1024 * 10,
            flush_threshold: 1000,
            manifest_path: manifest_path.clone(),
            immutable_ssts: Arc::new(RwLock::new(Vec::new())),
            schema,
        };
        mgr.load_manifest()?;
        mgr.recover()?;
        mgr.reset_seq_watermark()?;
        Ok(mgr)
    }

    fn reset_seq_watermark(&self) -> io::Result<()> {
        let mut watermark = 0u64;
        if let Some(active) = self.active.read().unwrap().as_ref() {
            watermark = watermark.max(active.max_seq());
        }
        for imm in self.immutables.read().unwrap().iter() {
            watermark = watermark.max(imm.max_seq());
        }
        for sst in self.immutable_ssts.read().unwrap().iter() {
            watermark = watermark.max(sst.max_seq());
        }
        self.seq.store(watermark + 1, Ordering::SeqCst);
        Ok(())
    }

    pub fn schema(&self) -> Arc<TableSchema> {
        self.schema.clone()
    }

    fn recover(&mut self) -> io::Result<()> {
        let mut wal_files: Vec<PathBuf> = fs::read_dir(&self.base_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                name.to_str()
                    .map(|s| s.starts_with("wal_") && s.ends_with(".log"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
            .collect();
        wal_files.sort();

        if wal_files.is_empty() {
            let new_wal = self.base_dir.join("wal_000.log");
            let active = MutableSkipListMemtable::new(new_wal)?;
            *self.active.write().unwrap() = Some(Box::new(active));
            return Ok(());
        }

        let last_wal = wal_files.pop().unwrap();
        let found = self.scan_sst_files()?;

        let active = MutableSkipListMemtable::new(last_wal)?;
        *self.active.write().unwrap() = Some(Box::new(active));

        self.merge_orphan_wals(&wal_files)?;
        if !self.manifest_path.exists() || found > 0 {
            self.write_manifest()?;
        }
        Ok(())
    }

    fn merge_orphan_wals(&self, wal_files: &[PathBuf]) -> io::Result<()> {
        let active_guard = self.active.read().unwrap();
        let active = active_guard.as_ref().unwrap();
        for path in wal_files {
            let wal = Wal::new(path)?;
            let skiplist = SkipList::new();
            wal.recover(&mut |op: &Operation| match op {
                Operation::Insert { key, seq, value } | Operation::Update { key, seq, value } => {
                    skiplist.insert(key.clone(), *seq, Some(value.clone()));
                }
                Operation::Delete { key, seq } => {
                    skiplist.insert(key.clone(), *seq, None);
                }
            })?;
            if skiplist.is_empty() {
                let _ = fs::remove_file(path);
                continue;
            }
            for (key, seq, value) in skiplist.iter() {
                match active.get(&key)? {
                    Some((s, _)) if s >= seq => {}
                    _ => {
                        active.write(key, seq, value)?;
                    }
                }
            }
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

    fn scan_sst_files(&mut self) -> io::Result<usize> {
        let mut files: Vec<PathBuf> = fs::read_dir(&self.base_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.ends_with(".sst"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
            .collect();

        let mut ssts = self.immutable_ssts.write().unwrap();
        let existing: Vec<PathBuf> = ssts.iter().map(|s| s.path().clone()).collect();

        files.sort_by_key(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(usize::MAX)
        });

        let mut added = 0;
        for path in files {
            if existing.contains(&path) {
                continue;
            }
            let is_numeric = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.parse::<usize>().is_ok())
                .unwrap_or(false);
            if !is_numeric {
                continue;
            }
            let sst = SSTable::open_from_path(&path, &self.schema)?;
            let current = self.sst_id.load(Ordering::SeqCst);
            if sst.id() >= current {
                self.sst_id.store(sst.id() + 1, Ordering::SeqCst);
            }
            ssts.push(sst);
            added += 1;
        }
        ssts.sort_by_key(|s| s.id());
        Ok(added)
    }

    fn load_manifest(&mut self) -> io::Result<()> {
        if !self.manifest_path.exists() {
            return Ok(());
        }
        let file = fs::File::open(&self.manifest_path)?;
        let reader = io::BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let entry: super::memtable::ManifestEntry = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }
        entries.sort_by_key(|e| e.id);
        let mut ssts = self.immutable_ssts.write().unwrap();
        for entry in entries {
            let path = self.base_dir.join(&entry.path);
            if path.exists() {
                let index = SstableIndex::load_from_file(&path)?;
                let sst = SSTable::new(
                    entry.id,
                    path,
                    entry.min_key,
                    entry.max_key,
                    entry.entry_count,
                    self.schema.clone(),
                    index,
                );
                ssts.push(sst);
                let current = self.sst_id.load(Ordering::SeqCst);
                if entry.id >= current {
                    self.sst_id.store(entry.id + 1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    fn write_manifest(&self) -> io::Result<()> {
        let tmp_path = self.manifest_path.with_extension("tmp");
        let file = fs::File::create(&tmp_path)?;
        let mut writer = io::BufWriter::new(file);
        let ssts = self.immutable_ssts.read().unwrap();
        for sst in ssts.iter() {
            let entry = super::memtable::ManifestEntry {
                id: sst.id(),
                path: sst
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                min_key: sst.min_key().clone(),
                max_key: sst.max_key().clone(),
                entry_count: sst.entry_count(),
            };
            let line = serde_json::to_string(&entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        fs::rename(&tmp_path, &self.manifest_path)?;
        Ok(())
    }

    pub fn write(&self, key: Key, value: Value) -> io::Result<Option<Value>> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Key) -> io::Result<Option<Value>> {
        self.write_inner(key, None)
    }

    pub fn write_batch(&self, entries: Vec<(Key, Option<Value>)>) -> io::Result<()> {
        let n = entries.len() as u64;
        let start = self.seq.fetch_add(n, Ordering::SeqCst);
        let entries: Vec<(Key, u64, Option<Value>)> = entries
            .into_iter()
            .enumerate()
            .map(|(i, (key, value))| (key, start + i as u64, value))
            .collect();
        {
            let active_opt = self.active.read().unwrap();
            let active = active_opt.as_ref().unwrap();
            active.write_batch(entries)?;
        }
        self.maybe_flush()?;
        Ok(())
    }

    fn write_inner(&self, key: Key, value: Option<Value>) -> io::Result<Option<Value>> {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let result = {
            let active_opt = self.active.read().unwrap();
            let active = active_opt.as_ref().unwrap();
            active.write(key, seq, value)?
        };
        self.maybe_flush()?;
        Ok(result)
    }

    fn maybe_flush(&self) -> io::Result<()> {
        let active_len = self
            .active
            .read()
            .unwrap()
            .as_ref()
            .map(|a| a.len())
            .unwrap_or(0);
        let total = self.estimated_total_memory();
        if total > self.max_memory_bytes || active_len >= self.flush_threshold {
            self.schedule_flush()?;
        }
        Ok(())
    }

    pub fn set_flush_threshold(&mut self, threshold: usize) {
        self.flush_threshold = threshold;
    }

    pub fn sst_id(&self) -> usize {
        self.sst_id.load(Ordering::SeqCst)
    }

    pub fn get(&self, key: Key) -> io::Result<Option<Value>> {
        let mut best: Option<(u64, Option<Value>)> = None;

        if let Some(active) = self.active.read().unwrap().as_ref()
            && active.max_seq() > best.as_ref().map(|(s, _)| *s).unwrap_or(0)
            && let Some(e) = active.get(&key)?
        {
            best = Some(e);
        }

        for imm in self.immutables.read().unwrap().iter().rev() {
            if imm.max_seq() > best.as_ref().map(|(s, _)| *s).unwrap_or(0)
                && let Some(e) = imm.get(&key)?
            {
                best = Some(e);
            }
        }

        for sst in self.immutable_ssts.read().unwrap().iter().rev() {
            if sst.max_seq() > best.as_ref().map(|(s, _)| *s).unwrap_or(0)
                && let Some(e) = sst.get(&key)?
            {
                best = Some(e);
            }
        }

        Ok(best.map(|(_, v)| v).flatten())
    }

    pub fn flush(&self) -> io::Result<()> {
        self.schedule_flush()?;
        Ok(())
    }

    fn schedule_flush(&self) -> io::Result<()> {
        let (imm, wal_path) = {
            let mut active_lock = self.active.write().unwrap();
            let old_active = match active_lock.take() {
                Some(a) => a,
                None => return Ok(()),
            };
            if old_active.len() == 0 {
                *active_lock = Some(old_active);
                return Ok(());
            }
            let new_active = match old_active.fork() {
                Ok(a) => a,
                Err(e) => {
                    *active_lock = Some(old_active);
                    return Err(e);
                }
            };

            let imm = match old_active.freeze() {
                Ok(i) => i,
                Err(e) => {
                    *active_lock = Some(old_active);
                    return Err(e);
                }
            };
            *active_lock = Some(new_active);
            let wal_path = imm.wal_path().to_path_buf();
            (imm, wal_path)
        };

        self.immutables.write().unwrap().push(imm);
        self.flush_immutables()?;
        let _ = fs::remove_file(wal_path);
        Ok(())
    }

    fn flush_immutables(&self) -> io::Result<()> {
        let mut immutables = self.immutables.write().unwrap();
        let mut new_ssts = Vec::new();
        let mut ids_to_remove = Vec::new();
        for (i, imm) in immutables.iter().enumerate() {
            let id = self.sst_id.load(Ordering::SeqCst);
            let path = self.base_dir.join(format!("{:04}.sst", id));
            let sst = imm.to_sstable(id, &path, &self.schema)?;
            new_ssts.push(sst);
            ids_to_remove.push(i);
            self.sst_id.fetch_add(1, Ordering::SeqCst);
        }
        for i in ids_to_remove.into_iter().rev() {
            immutables.remove(i);
        }
        {
            let mut ssts = self.immutable_ssts.write().unwrap();
            ssts.extend(new_ssts);
        }
        self.write_manifest()?;
        Ok(())
    }

    pub fn compact(&self) -> io::Result<()> {
        let ssts = self.immutable_ssts.read().unwrap();
        if ssts.len() < 4 {
            return Ok(());
        }
        let old_paths: Vec<PathBuf> = ssts.iter().map(|s| s.path().clone()).collect();
        let merged_skiplist = SkipList::new();
        for sst in ssts.iter() {
            let rows = sst.scan(sst.min_key(), sst.max_key())?;
            for (k, seq, v) in rows {
                merged_skiplist.insert(k, seq, v);
            }
        }
        drop(ssts);
        let id = self.sst_id.load(Ordering::SeqCst);
        let path = self.base_dir.join(format!("{:04}.sst", id));
        let new_sst =
            SSTable::create_from_skiplist(&merged_skiplist, id, &path, true, &self.schema)?;
        {
            let mut ssts_w = self.immutable_ssts.write().unwrap();
            *ssts_w = vec![new_sst];
            self.sst_id.fetch_add(1, Ordering::SeqCst);
        }
        self.write_manifest()?;
        for p in old_paths {
            let _ = fs::remove_file(p);
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        let active_len = self
            .active
            .read()
            .unwrap()
            .as_ref()
            .map(|a| a.len())
            .unwrap_or(0);
        let imm_len: usize = self
            .immutables
            .read()
            .unwrap()
            .iter()
            .map(|i| i.len())
            .sum();
        let sst_len: usize = self
            .immutable_ssts
            .read()
            .unwrap()
            .iter()
            .map(|s| s.entry_count())
            .sum();
        active_len + imm_len + sst_len
    }

    pub fn estimated_total_memory(&self) -> usize {
        let active = self
            .active
            .read()
            .unwrap()
            .as_ref()
            .map(|a| a.estimated_size())
            .unwrap_or(0);
        let imm: usize = self
            .immutables
            .read()
            .unwrap()
            .iter()
            .map(|i| i.estimated_size())
            .sum();
        active + imm
    }

    pub fn get_immutable_ssts(&self) -> Vec<SSTable> {
        self.immutable_ssts.read().unwrap().clone()
    }

    pub fn iter_all_data(&self) -> io::Result<impl Iterator<Item = (Key, Option<Value>)> + '_> {
        use std::collections::BTreeMap;
        let mut map: BTreeMap<Key, (u64, Option<Value>)> = BTreeMap::new();
        for sst in self.immutable_ssts.read().unwrap().iter().rev() {
            for (k, seq, v) in sst.scan(sst.min_key(), sst.max_key())? {
                map.entry(k)
                    .and_modify(|(s, cur)| {
                        if seq > *s {
                            *s = seq;
                            *cur = v.clone();
                        }
                    })
                    .or_insert((seq, v));
            }
        }
        for imm in self.immutables.read().unwrap().iter().rev() {
            for (k, seq, v) in imm.iter() {
                map.entry(k)
                    .and_modify(|(s, cur)| {
                        if seq > *s {
                            *s = seq;
                            *cur = v.clone();
                        }
                    })
                    .or_insert((seq, v));
            }
        }
        if let Some(active) = self.active.read().unwrap().as_ref() {
            for (k, seq, v) in active.iter() {
                map.entry(k)
                    .and_modify(|(s, cur)| {
                        if seq > *s {
                            *s = seq;
                            *cur = v.clone();
                        }
                    })
                    .or_insert((seq, v));
            }
        }
        Ok(map.into_iter().map(|(k, (_, v))| (k, v)))
    }

    pub fn snapshot_sources(
        &self,
    ) -> io::Result<Vec<Box<dyn Iterator<Item = (Key, u64, Option<Value>)>>>> {
        let mut out: Vec<Box<dyn Iterator<Item = (Key, u64, Option<Value>)>>> = Vec::new();
        if let Some(active) = self.active.read().unwrap().as_ref()
            && active.len() > 0
        {
            out.push(Box::new(active.iter().collect::<Vec<_>>().into_iter()));
        }

        for imm in self.immutables.read().unwrap().iter().rev() {
            if imm.len() > 0 {
                out.push(Box::new(imm.iter().collect::<Vec<_>>().into_iter()));
            }
        }
        for sst in self.immutable_ssts.read().unwrap().iter().rev() {
            let iter = sst.scan_iter(sst.min_key(), sst.max_key())?;
            out.push(Box::new(iter));
        }
        Ok(out)
    }

    pub fn close(&self) -> io::Result<()> {
        if let Some(active) = self.active.read().unwrap().as_ref() {
            active.close()?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for MemtableManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemtableManager")
            .field("sst_id", &self.sst_id.load(Ordering::SeqCst))
            .field("seq", &self.seq.load(Ordering::SeqCst))
            .field("base_dir", &self.base_dir)
            .field("max_memory_bytes", &self.max_memory_bytes)
            .field("flush_threshold", &self.flush_threshold)
            .field(
                "immutable_ssts_count",
                &self.immutable_ssts.read().unwrap().len(),
            )
            .field("immutables_count", &self.immutables.read().unwrap().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::*;
    use crate::schema::{ColumnDef, SemanticType};
    use arrow_schema::DataType;
    use tempfile::tempdir;

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_memtable_insert_get_remove() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mgr = MemtableManager::new(&path).unwrap();
        assert_eq!(mgr.len(), 0);

        assert_eq!(mgr.write(k(1, 0), v("one")).unwrap(), None);
        assert_eq!(mgr.write(k(2, 0), v("two")).unwrap(), None);
        assert_eq!(mgr.len(), 2);

        assert_eq!(mgr.get(k(1, 0)).unwrap(), Some(v("one")));
        assert_eq!(mgr.get(k(3, 0)).unwrap(), None);

        assert_eq!(mgr.write(k(1, 0), v("uno")).unwrap(), Some(v("one")));
        assert_eq!(mgr.get(k(1, 0)).unwrap(), Some(v("uno")));

        assert_eq!(mgr.delete(k(2, 0)).unwrap(), Some(v("two")));
        assert_eq!(mgr.len(), 2);
        assert_eq!(mgr.get(k(2, 0)).unwrap(), None);
        assert_eq!(mgr.delete(k(3, 0)).unwrap(), None);

        mgr.close().unwrap();
    }

    #[test]
    fn test_memtable_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");

        {
            let mgr = MemtableManager::new(&path).unwrap();
            mgr.write(k(1, 0), v("one")).unwrap();
            mgr.write(k(2, 0), v("two")).unwrap();
            mgr.close().unwrap();
        }

        {
            let mgr = MemtableManager::new(&path).unwrap();
            assert_eq!(mgr.len(), 2);
            assert_eq!(mgr.get(k(1, 0)).unwrap(), Some(v("one")));
            assert_eq!(mgr.get(k(2, 0)).unwrap(), Some(v("two")));
        }
    }

    #[test]
    fn test_memtable_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");

        {
            let mgr = MemtableManager::new(&path).unwrap();
            assert_eq!(mgr.len(), 0);
            mgr.close().unwrap();
        }

        {
            let mgr = MemtableManager::new(&path).unwrap();
            assert_eq!(mgr.len(), 0);
        }
    }

    #[test]
    fn test_memtable_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mgr = MemtableManager::new(&path).unwrap();
        mgr.write(k(1, 0), v("one")).unwrap();
        mgr.flush().unwrap();
        mgr.close().unwrap();

        let mgr2 = MemtableManager::new(&path).unwrap();
        assert_eq!(mgr2.get(k(1, 0)).unwrap(), Some(v("one")));
    }

    #[test]
    fn test_memtable_flush_multiple() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut mgr = MemtableManager::new(&wal_path)?;
        mgr.set_flush_threshold(2);

        mgr.write(k(1, 0), v("a"))?;
        mgr.write(k(2, 0), v("b"))?;
        assert_eq!(mgr.get_immutable_ssts().len(), 1);

        mgr.write(k(3, 0), v("c"))?;
        mgr.write(k(4, 0), v("d"))?;
        assert_eq!(mgr.get_immutable_ssts().len(), 2);

        assert_eq!(mgr.get(k(1, 0))?, Some(v("a")));
        assert_eq!(mgr.get(k(4, 0))?, Some(v("d")));
        Ok(())
    }

    #[test]
    fn test_memtable_manifest_fallback_scan() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let manifest_path = dir.path().join("manifest");

        {
            let mut mgr = MemtableManager::new(&wal_path)?;
            mgr.set_flush_threshold(2);

            mgr.write(k(1, 0), v("a"))?;
            mgr.write(k(2, 0), v("b"))?;
            mgr.write(k(3, 0), v("c"))?;
            mgr.write(k(4, 0), v("d"))?;

            mgr.flush()?;
            assert!(mgr.get_immutable_ssts().len() >= 2);
            assert!(manifest_path.exists());
            mgr.close()?;
        }

        fs::remove_file(&manifest_path)?;
        assert!(!manifest_path.exists());

        {
            let mgr = MemtableManager::new(&wal_path)?;
            assert!(mgr.get_immutable_ssts().len() >= 2);
            assert_eq!(mgr.get(k(1, 0))?, Some(v("a")));
            assert_eq!(mgr.get(k(2, 0))?, Some(v("b")));
            assert_eq!(mgr.get(k(3, 0))?, Some(v("c")));
            assert_eq!(mgr.get(k(4, 0))?, Some(v("d")));
            assert!(manifest_path.exists());
        }

        {
            let mgr = MemtableManager::new(&wal_path)?;
            assert!(mgr.get_immutable_ssts().len() >= 2);
            assert_eq!(mgr.get(k(1, 0))?, Some(v("a")));
            assert_eq!(mgr.get(k(4, 0))?, Some(v("d")));
        }

        Ok(())
    }

    #[test]
    fn test_compaction() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut mgr = MemtableManager::new(&wal_path)?;
        mgr.set_flush_threshold(2);

        for i in 0..8 {
            let key = (vec![i as u8], i as i64);
            mgr.write(key, format!("v{}", i).into_bytes())?;
        }
        assert_eq!(mgr.get_immutable_ssts().len(), 4);

        for i in 0..8 {
            let key = (vec![i as u8], i as i64);
            assert_eq!(mgr.get(key)?, Some(format!("v{}", i).into_bytes()));
        }

        mgr.compact()?;
        assert_eq!(mgr.get_immutable_ssts().len(), 1);

        for i in 0..8 {
            let key = (vec![i as u8], i as i64);
            assert_eq!(mgr.get(key)?, Some(format!("v{}", i).into_bytes()));
        }

        mgr.delete(k(3, 0))?;
        mgr.delete(k(5, 0))?;
        mgr.write(k(8, 0), v("v8"))?;
        mgr.write(k(9, 0), v("v9"))?;
        mgr.write(k(10, 0), v("v10"))?;
        mgr.write(k(11, 0), v("v11"))?;
        assert_eq!(mgr.get_immutable_ssts().len(), 4);
        mgr.compact()?;
        assert_eq!(mgr.get_immutable_ssts().len(), 1);

        assert_eq!(mgr.get(k(3, 0))?, None);
        assert_eq!(mgr.get(k(5, 0))?, None);
        assert_eq!(mgr.get(k(0, 0))?, Some(v("v0")));
        assert_eq!(mgr.get(k(8, 0))?, Some(v("v8")));
        assert_eq!(mgr.get(k(9, 0))?, Some(v("v9")));
        assert_eq!(mgr.get(k(10, 0))?, Some(v("v10")));
        assert_eq!(mgr.get(k(11, 0))?, Some(v("v11")));

        Ok(())
    }

    fn schema3() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnDef {
                    name: "host".into(),
                    data_type: DataType::Utf8,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "cpu".into(),
                    data_type: DataType::Utf8,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "timestamp".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Timestamp,
                },
                ColumnDef {
                    name: "value".into(),
                    data_type: DataType::Float64,
                    semantic: SemanticType::Field,
                },
                ColumnDef {
                    name: "note".into(),
                    data_type: DataType::Utf8,
                    semantic: SemanticType::Field,
                },
            ],
            primary_key: vec![0, 1],
            time_index: 2,
        }
    }

    fn mkkey(s: &TableSchema, host: &str, cpu: &str, ts: i64) -> Key {
        s.key(&[host.as_bytes().to_vec(), cpu.as_bytes().to_vec()], ts)
    }
    fn mkval(s: &TableSchema, value: f64, note: &str) -> Value {
        s.value(&[value.to_le_bytes().to_vec(), note.as_bytes().to_vec()])
    }

    #[test]
    fn test_manager_multi_tag_flush_compact() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let schema = Arc::new(schema3());
        let mut mgr = MemtableManager::with_schema(&wal_path, schema.clone())?;
        mgr.set_flush_threshold(2);

        let rows = vec![
            (("h1", "c1", 10), 1.5, "a"),
            (("h1", "c2", 10), 2.5, "b"),
            (("h1", "c1", 20), 3.5, "c"),
            (("h2", "c1", 10), 4.5, "d"),
            (("h2", "c2", 10), 5.5, "e"),
            (("h2", "c2", 20), 6.5, "f"),
            (("h1", "c2", 30), 7.5, "g"),
            (("h2", "c1", 30), 8.5, "h"),
        ];
        for ((host, cpu, ts), value, note) in rows.clone() {
            mgr.write(mkkey(&schema, host, cpu, ts), mkval(&schema, value, note))?;
        }
        assert_eq!(mgr.get_immutable_ssts().len(), 4);

        mgr.compact()?;
        assert_eq!(mgr.get_immutable_ssts().len(), 1);

        for ((host, cpu, ts), value, note) in rows.clone() {
            let got = mgr.get(mkkey(&schema, host, cpu, ts))?;
            assert_eq!(
                got,
                Some(mkval(&schema, value, note)),
                "{} {} {}",
                host,
                cpu,
                ts
            );
        }
        assert_eq!(mgr.get(mkkey(&schema, "h1", "c1", 99))?, None);

        mgr.delete(mkkey(&schema, "h1", "c1", 20))?;
        mgr.flush()?;
        mgr.compact()?;
        assert_eq!(mgr.get(mkkey(&schema, "h1", "c1", 20))?, None);
        assert_eq!(
            mgr.get(mkkey(&schema, "h2", "c1", 10))?,
            Some(mkval(&schema, 4.5, "d"))
        );

        Ok(())
    }

    #[test]
    fn test_get_newest_write_wins_across_flush() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut mgr = MemtableManager::new(&wal_path)?;
        mgr.set_flush_threshold(2);
        mgr.write(k(1, 0), v("old"))?;
        mgr.write(k(2, 0), v("b"))?;
        mgr.write(k(1, 0), v("new"))?;

        assert_eq!(mgr.get(k(1, 0))?, Some(v("new")));
        mgr.flush()?;
        assert_eq!(mgr.get(k(1, 0))?, Some(v("new")));
        mgr.compact()?;
        assert_eq!(mgr.get(k(1, 0))?, Some(v("new")));
        assert_eq!(mgr.get(k(2, 0))?, Some(v("b")));
        Ok(())
    }

    #[test]
    fn test_seq_survives_restart_and_compact() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");

        {
            let mgr = MemtableManager::new(&wal_path)?;
            mgr.write(k(1, 0), v("a"))?;
            mgr.write(k(2, 0), v("b"))?;
            mgr.flush()?;
            let ssts = mgr.get_immutable_ssts();
            assert_eq!(ssts.len(), 1);
            assert_eq!(ssts[0].max_seq(), 2);
            mgr.close()?;
        }

        {
            let mgr = MemtableManager::new(&wal_path)?;
            mgr.write(k(3, 0), v("c"))?;
            mgr.flush()?;
            let ssts = mgr.get_immutable_ssts();
            assert!(ssts.iter().any(|s| s.max_seq() == 3));

            assert_eq!(mgr.get(k(1, 0))?, Some(v("a")));
            assert_eq!(mgr.get(k(2, 0))?, Some(v("b")));
            assert_eq!(mgr.get(k(3, 0))?, Some(v("c")));
            Ok(())
        }
    }
}
