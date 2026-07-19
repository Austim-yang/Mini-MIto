use std::{
    fs::{self},
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    memtable::{SkipList, Wal, wal::Operation},
    sstable::sstable::SSTable,
};

#[derive(Serialize, Deserialize)]
pub struct ManifestEntry<K> {
    id: usize,
    path: String,
    min_key: K,
    max_key: K,
    entry_count: usize,
}

pub struct Memtable<K, V> {
    skiplist: SkipList<K, V>,
    wal: Wal,
    wal_path: PathBuf,
    flush_threshold: usize,
    sst_id: usize,
    immutable_ssts: Vec<SSTable<K, V>>,
    manifest_path: PathBuf,
}

impl<K, V> Memtable<K, V>
where
    K: Ord + Clone + Default + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Clone + Default + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    pub fn new<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        let manifest_path = wal_path
            .as_ref()
            .parent()
            .unwrap_or(Path::new("."))
            .join("manifest");
        let mut mem = Memtable {
            skiplist: SkipList::new(),
            wal: Wal::new(&wal_path)?,
            wal_path: wal_path.as_ref().to_path_buf(),
            flush_threshold: 1000,
            sst_id: 0,
            immutable_ssts: Vec::new(),
            manifest_path,
        };
        mem.load_manafest()?;
        mem.wal.recover(&mut mem.skiplist)?;

        Ok(mem)
    }

    pub fn insert(&mut self, key: K, value: V) -> io::Result<Option<V>> {
        let op = Operation::Insert {
            key: key.clone(),
            value: value.clone(),
        };
        self.wal.append(&op)?;
        let old_value = self.skiplist.insert(key, Some(value));
        if self.skiplist.len() >= self.flush_threshold {
            self.flush()?;
        }
        if self.immutable_ssts.len() >= 4 {
            self.compact()?;
        }
        Ok(old_value)
    }

    pub fn get(&self, key: &K) -> io::Result<Option<V>> {
        if let Some(value) = self.skiplist.get(key) {
            return Ok(value);
        }
        for sst in self.immutable_ssts.iter().rev() {
            if let Some(value) = sst.get(key)? {
                return Ok(value);
            }
        }
        Ok(None)
    }

    pub fn remove(&mut self, key: K) -> io::Result<Option<V>> {
        let op = Operation::<K, V>::Delete { key: key.clone() };
        self.wal.append(&op)?;
        let old_value = self.skiplist.remove(key);
        if self.skiplist.len() >= self.flush_threshold {
            self.flush()?;
        }
        if self.immutable_ssts.len() >= 4 {
            self.compact()?;
        }
        Ok(old_value)
    }

    pub fn len(&self) -> usize {
        self.skiplist.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn flush_wal(&mut self) -> io::Result<()> {
        self.wal.flush()
    }

    fn write_manifest(&self) -> io::Result<()> {
        let tmp_path = self.manifest_path.with_extension("tmp");
        let file = fs::File::create(&tmp_path)?;
        let mut writer = io::BufWriter::new(file);

        for sst in &self.immutable_ssts {
            let entry = ManifestEntry {
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

    fn load_manafest(&mut self) -> io::Result<()> {
        if !self.manifest_path.exists() {
            self.scan_exist_ssts()?;
            self.write_manifest()?;
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
            let entry: ManifestEntry<K> = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }

        entries.sort_by_key(|e| e.id);

        for entry in entries {
            let sst_path = self.wal_path.parent().unwrap().join(&entry.path);
            let sst = SSTable::new(
                entry.id,
                sst_path,
                entry.min_key,
                entry.max_key,
                entry.entry_count,
            );

            self.immutable_ssts.push(sst);
            if entry.id >= self.sst_id {
                self.sst_id = entry.id + 1;
            }
        }

        Ok(())
    }

    fn scan_exist_ssts(&mut self) -> io::Result<()> {
        let dir = self.wal_path.parent().unwrap_or(Path::new("."));
        let mut entries = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sst") {
                let file_name = path.file_stem().unwrap().to_string_lossy();
                if let Ok(id) = file_name.parse::<usize>() {
                    let sst = SSTable::open_from_path(&path)?;
                    entries.push((id, sst));
                }
            }
        }
        entries.sort_by_key(|(id, _)| *id);
        for (id, sst) in entries {
            self.immutable_ssts.push(sst);
            if id >= self.sst_id {
                self.sst_id = id + 1;
            }
        }
        Ok(())
    }

    pub fn compact(&mut self) -> io::Result<()> {
        if self.immutable_ssts.len() < 4 {
            return Ok(());
        }

        let mut temp_skiplist = SkipList::new();

        for sst in &self.immutable_ssts {
            let pairs = sst.scan(sst.min_key(), sst.max_key())?;
            for (k, v) in pairs {
                temp_skiplist.insert(k, v);
            }
        }

        let new_id = self.sst_id;
        self.sst_id += 1;
        let new_path = self
            .wal_path
            .parent()
            .unwrap()
            .join(format!("{:04}.sst", new_id));
        let new_sst = SSTable::create_from_skiplist(&temp_skiplist, new_id, &new_path, false)?;
        let new_list = vec![new_sst];
        let old_ssts = std::mem::replace(&mut self.immutable_ssts, new_list);
        self.write_manifest()?;

        for sst in &old_ssts {
            let _ = std::fs::remove_file(sst.path());
        }

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.skiplist.len() == 0 {
            return Ok(());
        }
        let sst_filename = format!("{:04}.sst", self.sst_id);
        let sst_path = self
            .wal_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(sst_filename);
        let sst = SSTable::create_from_skiplist(&self.skiplist, self.sst_id, &sst_path, true)?;
        self.immutable_ssts.push(sst);
        self.sst_id += 1;

        if let Err(e) = self.write_manifest() {
            let _ = fs::remove_file(&sst_path);
            return Err(e);
        }

        self.skiplist = SkipList::new();
        self.wal.close()?;
        if self.wal_path.exists() {
            fs::remove_file(&self.wal_path)?;
        }
        self.wal = Wal::new(&self.wal_path)?;

        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.wal.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_memtable_insert_get_remove() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut mem = Memtable::<i32, String>::new(&path).unwrap();
        assert!(mem.is_empty());
        assert_eq!(mem.len(), 0);

        assert_eq!(mem.insert(1, "one".to_string()).unwrap(), None);
        assert_eq!(mem.insert(2, "two".to_string()).unwrap(), None);
        assert_eq!(mem.len(), 2);

        assert_eq!(mem.get(&1).unwrap(), Some("one".to_string()));
        assert_eq!(mem.get(&3).unwrap(), None);

        assert_eq!(
            mem.insert(1, "uno".to_string()).unwrap(),
            Some("one".to_string())
        );
        assert_eq!(mem.get(&1).unwrap(), Some("uno".to_string()));

        assert_eq!(mem.remove(2).unwrap(), Some("two".to_string()));
        assert_eq!(mem.len(), 2);
        assert_eq!(mem.get(&2).unwrap(), None);
        assert_eq!(mem.remove(3).unwrap(), None);

        mem.close().unwrap();
    }

    #[test]
    fn test_memtable_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");

        {
            let mut mem = Memtable::<i32, String>::new(&path).unwrap();
            mem.insert(1, "one".to_string()).unwrap();
            mem.insert(2, "two".to_string()).unwrap();
            mem.close().unwrap();
        }

        {
            let mem = Memtable::<i32, String>::new(&path).unwrap();
            assert_eq!(mem.len(), 2);
            assert_eq!(mem.get(&1).unwrap(), Some("one".to_string()));
            assert_eq!(mem.get(&2).unwrap(), Some("two".to_string()));
        }
    }

    #[test]
    fn test_memtable_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");

        {
            let mut mem = Memtable::<i32, String>::new(&path).unwrap();
            assert!(mem.is_empty());
            mem.close().unwrap();
        }

        {
            let mem = Memtable::<i32, String>::new(&path).unwrap();
            assert!(mem.is_empty());
        }
    }

    #[test]
    fn test_memtable_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut mem = Memtable::<i32, String>::new(&path).unwrap();
        mem.insert(1, "one".to_string()).unwrap();
        mem.flush_wal().unwrap();
        mem.close().unwrap();

        let mem2 = Memtable::<i32, String>::new(&path).unwrap();
        assert_eq!(mem2.get(&1).unwrap(), Some("one".to_string()));
    }

    #[test]
    fn test_memtable_flush_multiple() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut mem = Memtable::new(&wal_path)?;
        mem.flush_threshold = 2;

        mem.insert(1, "a".to_string())?;
        mem.insert(2, "b".to_string())?;
        assert_eq!(mem.immutable_ssts.len(), 1);

        mem.insert(3, "c".to_string())?;
        mem.insert(4, "d".to_string())?;
        assert_eq!(mem.immutable_ssts.len(), 2);

        assert_eq!(mem.get(&1).unwrap(), Some("a".to_string()));
        assert_eq!(mem.get(&4).unwrap(), Some("d".to_string()));
        Ok(())
    }

    #[test]
    fn test_memtable_manifest_fallback_scan() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let manifest_path = dir.path().join("manifest");

        {
            let mut mem = Memtable::new(&wal_path)?;
            mem.flush_threshold = 2;

            mem.insert(1, "a".to_string())?;
            mem.insert(2, "b".to_string())?;
            mem.insert(3, "c".to_string())?;
            mem.insert(4, "d".to_string())?;

            assert_eq!(mem.immutable_ssts.len(), 2);
            assert!(manifest_path.exists());
            mem.close()?;
        }

        fs::remove_file(&manifest_path)?;
        assert!(!manifest_path.exists());

        {
            let mem = Memtable::new(&wal_path)?;
            assert_eq!(mem.immutable_ssts.len(), 2);
            assert_eq!(mem.sst_id, 2);
            assert_eq!(mem.get(&1)?, Some("a".to_string()));
            assert_eq!(mem.get(&2)?, Some("b".to_string()));
            assert_eq!(mem.get(&3)?, Some("c".to_string()));
            assert_eq!(mem.get(&4)?, Some("d".to_string()));
            assert!(manifest_path.exists());
        }

        {
            let mem = Memtable::new(&wal_path)?;
            assert_eq!(mem.immutable_ssts.len(), 2);
            assert_eq!(mem.get(&1)?, Some("a".to_string()));
            assert_eq!(mem.get(&4)?, Some("d".to_string()));
        }

        Ok(())
    }

    #[test]
    fn test_compaction() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut mem = Memtable::new(&wal_path)?;
        mem.flush_threshold = 2;

        for i in 0..8 {
            mem.insert(i, format!("v{}", i))?;
        }

        assert_eq!(mem.immutable_ssts.len(), 1);
        assert_eq!(mem.immutable_ssts[0].entry_count(), 8);

        for i in 0..8 {
            assert_eq!(mem.get(&i)?, Some(format!("v{}", i)));
        }

        mem.remove(3)?;
        mem.remove(5)?;
        mem.insert(8, "v8".to_string())?;
        mem.insert(9, "v9".to_string())?;

        assert_eq!(mem.immutable_ssts.len(), 3);
        mem.compact()?;
        assert_eq!(mem.immutable_ssts.len(), 3);

        assert_eq!(mem.get(&3)?, None);
        assert_eq!(mem.get(&5)?, None);
        assert_eq!(mem.get(&0)?, Some("v0".to_string()));
        assert_eq!(mem.get(&8)?, Some("v8".to_string()));
        assert_eq!(mem.get(&9)?, Some("v9".to_string()));

        Ok(())
    }
}
