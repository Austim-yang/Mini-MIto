use std::{fs, io, path::{Path, PathBuf}};

use crate::{memtable::{SkipList, Wal, wal::Operation}, sstable::sstable::SSTable};

pub struct Memtable<K, V> {
    skiplist: SkipList<K, V>,
    wal: Wal,
    wal_path: PathBuf,
    flush_threshold: usize,
    sst_id: usize,
    immutable_ssts: Vec<SSTable<K, V>>,
}

impl<K, V> Memtable<K, V>
where
    K: Ord + Clone + Default + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Clone + Default + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    pub fn new<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        let mut skiplist = SkipList::new();
        let wal = Wal::new(&wal_path)?;
        wal.recover(&mut skiplist)?;

        Ok(Memtable { skiplist, wal, wal_path: wal_path.as_ref().to_path_buf(), flush_threshold: 1000, sst_id: 0, immutable_ssts: Vec::new() })
    }

    pub fn insert(&mut self, key: K, value: V) -> io::Result<Option<V>> {
        let op = Operation::Insert { key: key.clone(), value: value.clone(), };
        self.wal.append(&op)?;
        let old_value = self.skiplist.insert(key, value);
        if self.skiplist.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(old_value)
    }

    pub fn get(&self, key: &K) -> io::Result<Option<V>> {
        if let Some(value) = self.skiplist.get(key) {
            return Ok(Some(value))
        }
        for sst in &self.immutable_ssts {
            if let Some(value) = sst.get(key)? {
                return Ok(Some(value));
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

    pub fn flush(&mut self) -> io::Result<()> {
        if self.skiplist.len() == 0 {
            return  Ok(());
        }
        let sst_filename = format!("{:04}.sst", self.sst_id);
        let sst_path = self.wal_path.parent().unwrap_or(Path::new(".")).join(sst_filename);
        let sst = SSTable::create_from_skiplist(&self.skiplist, &sst_path)?;
        self.immutable_ssts.push(sst);
        self.sst_id += 1;
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

        assert_eq!(mem.insert(1, "uno".to_string()).unwrap(), Some("one".to_string()));
        assert_eq!(mem.get(&1).unwrap(), Some("uno".to_string()));

        assert_eq!(mem.remove(2).unwrap(), Some("two".to_string()));
        assert_eq!(mem.len(), 1);
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
}