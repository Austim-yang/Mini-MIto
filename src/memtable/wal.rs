use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::types::{Key, Value};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Insert { key: Key, seq: u64, value: Value },
    Update { key: Key, seq: u64, value: Value },
    Delete { key: Key, seq: u64 },
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

    pub fn recover(&self, sink: &mut dyn FnMut(&Operation)) -> io::Result<()> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let op: Operation = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            sink(&op);
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
    use std::collections::BTreeMap;

    use super::*;

    use tempfile::tempdir;

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    #[derive(Default)]
    struct Rows(BTreeMap<Key, (u64, Option<Value>)>);

    impl Rows {
        fn insert(&mut self, key: Key, seq: u64, value: Option<Value>) {
            self.0.insert(key, (seq, value));
        }
        fn get(&self, key: &Key) -> Option<(u64, Option<Value>)> {
            self.0.get(key).cloned()
        }
    }

    fn replay_into(rows: &mut Rows) -> impl FnMut(&Operation) + '_ {
        move |op: &Operation| match op {
            Operation::Insert { key, seq, value } | Operation::Update { key, seq, value } => {
                rows.insert(key.clone(), *seq, Some(value.clone()));
            }
            Operation::Delete { key, seq } => {
                rows.insert(key.clone(), *seq, None);
            }
        }
    }

    #[test]
    fn test_wal_insert_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();

        wal.append(&Operation::Insert {
            key: k(1, 0),
            seq: 1,
            value: v("one"),
        })
        .unwrap();
        wal.append(&Operation::Insert {
            key: k(2, 0),
            seq: 2,
            value: v("two"),
        })
        .unwrap();
        wal.close().unwrap();

        let mut rows = Rows::default();
        let wal_recover = Wal::new(&path).unwrap();
        wal_recover.recover(&mut replay_into(&mut rows)).unwrap();

        assert_eq!(rows.get(&k(1, 0)), Some((1, Some(v("one")))));
        assert_eq!(rows.get(&k(2, 0)), Some((2, Some(v("two")))));
        assert_eq!(rows.0.len(), 2);
    }

    #[test]
    fn test_wal_update_and_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();

        wal.append(&Operation::Insert {
            key: k(10, 0),
            seq: 1,
            value: v("old"),
        })
        .unwrap();
        wal.append(&Operation::Update {
            key: k(10, 0),
            seq: 2,
            value: v("new"),
        })
        .unwrap();
        wal.append(&Operation::Delete {
            key: k(10, 0),
            seq: 3,
        })
        .unwrap();
        wal.close().unwrap();

        let mut rows = Rows::default();
        let wal_recover = Wal::new(&path).unwrap();
        wal_recover.recover(&mut replay_into(&mut rows)).unwrap();

        assert_eq!(rows.get(&k(10, 0)), Some((3, None)));
        assert_eq!(rows.0.len(), 1);
    }

    #[test]
    fn test_wal_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");
        Wal::new(&path).unwrap().close().unwrap();

        let mut rows = Rows::default();
        let wal = Wal::new(&path).unwrap();
        wal.recover(&mut replay_into(&mut rows)).unwrap();
        assert_eq!(rows.0.len(), 0);
    }

    #[test]
    fn test_wal_roundtrip_preserves_seq() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seq.log");
        let mut wal = Wal::new(&path).unwrap();
        wal.append(&Operation::Insert {
            key: k(1, 0),
            seq: 42,
            value: v("x"),
        })
        .unwrap();
        wal.append(&Operation::Delete {
            key: k(1, 0),
            seq: 43,
        })
        .unwrap();
        wal.close().unwrap();

        let mut rows = Rows::default();
        let wal_recover = Wal::new(&path).unwrap();
        wal_recover.recover(&mut replay_into(&mut rows)).unwrap();
        assert_eq!(rows.get(&k(1, 0)), Some((43, None)));
        let max_seq = rows.0.values().map(|(s, _)| *s).max();
        assert_eq!(max_seq, Some(43));
    }
}
