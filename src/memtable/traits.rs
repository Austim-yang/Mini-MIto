use std::{io, path::Path};

use crate::{Key, Value, memtable::wal::Operation, schema::TableSchema, sstable::sstable::SSTable};

pub trait Memtable: Send + Sync {
    fn write(&self, key: Key, seq: u64, value: Option<Value>) -> io::Result<Option<Value>>;
    fn write_batch(&self, entries: Vec<(Key, u64, Option<Value>)>) -> io::Result<()>;
    fn replay(&self, op: &Operation) -> io::Result<()>;
    fn get(&self, key: &Key) -> io::Result<Option<(u64, Option<Value>)>>;
    fn max_seq(&self) -> u64;
    fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, u64, Option<Value>)>>;
    fn iter(&self) -> Box<dyn Iterator<Item = (Key, u64, Option<Value>)> + '_>;
    fn len(&self) -> usize;
    fn estimated_size(&self) -> usize;
    fn freeze(&self) -> io::Result<Box<dyn ImmutableMemtable>>;
    fn fork(&self) -> io::Result<Box<dyn Memtable>>;
    fn close(&self) -> io::Result<()>;
}

pub trait ImmutableMemtable: Send + Sync {
    fn get(&self, key: &Key) -> io::Result<Option<(u64, Option<Value>)>>;
    fn max_seq(&self) -> u64;
    fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, u64, Option<Value>)>>;
    fn iter(&self) -> Box<dyn Iterator<Item = (Key, u64, Option<Value>)> + '_>;
    fn len(&self) -> usize;
    fn estimated_size(&self) -> usize;
    fn to_sstable(&self, id: usize, path: &Path, schema: &TableSchema) -> io::Result<SSTable>;
    fn wal_path(&self) -> &Path;
}
