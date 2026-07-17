use std::{io, path::Path};

use crate::memtable::{SkipList, Wal, wal::Operation};

pub struct Memtable<K, V> {
    skiplist: SkipList<K, V>,
    wal: Wal,
}

impl<K, V> Memtable<K, V>
where
    K: Ord + Clone + Default + for<'de> serde::Deserialize<'de> + serde::Serialize,
    V: Clone + Default + for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    pub fn new<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        let mut skiplist = SkipList::new();
        let wal = Wal::new(wal_path)?;
        wal.recover(&mut skiplist)?;

        Ok(Memtable { skiplist, wal })
    }

    pub fn insert(&mut self, key: K, value: V) -> io::Result<Option<V>> {
        let op = Operation::Insert { key: key.clone(), value: value.clone(), };
        self.wal.append(&op)?;
        Ok(self.skiplist.insert(key, value))
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.skiplist.get(key)
    }

    pub fn remove(&mut self, key: K) -> io::Result<Option<V>> {
        let op = Operation::<K, V>::Delete { key: key.clone() };
        self.wal.append(&op)?;
        Ok(self.skiplist.remove(key))
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

    pub fn close(self) -> io::Result<()> {
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

        assert_eq!(mem.get(&1), Some("one".to_string()));
        assert_eq!(mem.get(&3), None);

        assert_eq!(mem.insert(1, "uno".to_string()).unwrap(), Some("one".to_string()));
        assert_eq!(mem.get(&1), Some("uno".to_string()));

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
            assert_eq!(mem.get(&1), Some("one".to_string()));
            assert_eq!(mem.get(&2), Some("two".to_string()));
        }
    }

    #[test]
    fn test_memtable_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");

        {
            let mem = Memtable::<i32, String>::new(&path).unwrap();
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
        assert_eq!(mem2.get(&1), Some("one".to_string()));
    }
}