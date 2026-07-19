use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::memtable::SkipList;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation<K, V> {
    Insert { key: K, value: V },
    Update { key: K, value: V },
    Delete { key: K },
}

pub struct Wal {
    writer: BufWriter<File>,
    path: String,
}

impl Wal {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.as_ref())?;
        Ok(Wal {
            writer: BufWriter::new(file),
            path: path.as_ref().to_string_lossy().into_owned(),
        })
    }

    pub fn append<K, V>(&mut self, op: &Operation<K, V>) -> io::Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        let line =
            serde_json::to_string(op).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn recover<K, V>(&self, skiplist: &mut SkipList<K, V>) -> io::Result<()>
    where
        K: for<'de> Deserialize<'de> + Ord + Clone + Default,
        V: for<'de> Deserialize<'de> + Clone + Default,
    {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let op: Operation<K, V> = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            match op {
                Operation::Insert { key, value } | Operation::Update { key, value } => {
                    skiplist.insert(key, Some(value));
                }
                Operation::Delete { key } => {
                    skiplist.remove(key);
                }
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::SkipList;
    use tempfile::tempdir;

    #[test]
    fn test_wal_insert_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();

        wal.append(&Operation::Insert {
            key: 1,
            value: "one".to_string(),
        })
        .unwrap();
        wal.append(&Operation::Insert {
            key: 2,
            value: "two".to_string(),
        })
        .unwrap();
        wal.close().unwrap();

        let mut list: SkipList<i32, String> = SkipList::new();
        let wal_recover = Wal::new(&path).unwrap();
        wal_recover.recover(&mut list).unwrap();

        assert_eq!(list.get(&1), Some("one".to_string()));
        assert_eq!(list.get(&2), Some("two".to_string()));
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_wal_update_and_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();

        wal.append(&Operation::Insert {
            key: 10,
            value: "old".to_string(),
        })
        .unwrap();
        wal.append(&Operation::Update {
            key: 10,
            value: "new".to_string(),
        })
        .unwrap();
        wal.append(&Operation::<i32, String>::Delete { key: 10 })
            .unwrap();
        wal.close().unwrap();

        let mut list: SkipList<i32, String> = SkipList::new();
        let wal_recover = Wal::new(&path).unwrap();
        wal_recover.recover(&mut list).unwrap();

        assert_eq!(list.get(&10), None);
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_wal_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");
        Wal::new(&path).unwrap().close().unwrap();

        let mut list: SkipList<i32, String> = SkipList::new();
        let wal = Wal::new(&path).unwrap();
        wal.recover(&mut list).unwrap();
        assert_eq!(list.len(), 0);
    }
}