use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{
    memtable::SkipList,
    types::{Key, Value},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Insert { key: Key, value: Value },
    Update { key: Key, value: Value },
    Delete { key: Key },
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

    pub fn append(&mut self, op: &Operation) -> io::Result<()> {
        let line =
            serde_json::to_string(op).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn recover(&self, skiplist: &mut SkipList) -> io::Result<()> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let op: Operation = serde_json::from_str(&line)
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

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_wal_insert_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();

        wal.append(&Operation::Insert {
            key: k(1, 0),
            value: v("one"),
        })
        .unwrap();
        wal.append(&Operation::Insert {
            key: k(2, 0),
            value: v("two"),
        })
        .unwrap();
        wal.close().unwrap();

        let mut list = SkipList::new();
        let wal_recover = Wal::new(&path).unwrap();
        wal_recover.recover(&mut list).unwrap();

        assert_eq!(list.get(&k(1, 0)), Some(Some(v("one"))));
        assert_eq!(list.get(&k(2, 0)), Some(Some(v("two"))));
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_wal_update_and_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();

        wal.append(&Operation::Insert {
            key: k(10, 0),
            value: v("old"),
        })
        .unwrap();
        wal.append(&Operation::Update {
            key: k(10, 0),
            value: v("new"),
        })
        .unwrap();
        wal.append(&Operation::Delete { key: k(10, 0) }).unwrap();
        wal.close().unwrap();

        let mut list = SkipList::new();
        let wal_recover = Wal::new(&path).unwrap();
        wal_recover.recover(&mut list).unwrap();

        assert_eq!(list.get(&k(10, 0)), Some(None));
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_wal_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");
        Wal::new(&path).unwrap().close().unwrap();

        let mut list = SkipList::new();
        let wal = Wal::new(&path).unwrap();
        wal.recover(&mut list).unwrap();
        assert_eq!(list.len(), 0);
    }
}
