use std::{io, path::Path};

use crate::{Key, Value, sstable::sstable::SSTable};

pub trait Memtable: Send + Sync {
    fn write(&self, key: Key, value: Option<Value>) -> io::Result<Option<Value>>;
    fn write_batch(&self, entries: Vec<(Key, Option<Value>)>) -> io::Result<()>;
    fn get(&self, key: &Key) -> io::Result<Option<Option<Value>>>;
    fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, Option<Value>)>>;
    fn iter(&self) -> Box<dyn Iterator<Item = (Key, Option<Value>)> + '_>;
    fn len(&self) -> usize;
    fn estimated_size(&self) -> usize;
    fn freeze(&self) -> io::Result<Box<dyn ImmutableMemtable>>;
    fn fork(&self) -> io::Result<Box<dyn Memtable>>;
    fn close(&self) -> io::Result<()>;
}

pub trait ImmutableMemtable: Send + Sync {
    fn get(&self, key: &Key) -> io::Result<Option<Option<Value>>>;
    fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, Option<Value>)>>;
    fn iter(&self) -> Box<dyn Iterator<Item = (Key, Option<Value>)> + '_>;
    fn len(&self) -> usize;
    fn estimated_size(&self) -> usize;
    fn to_sstable(&self, id: usize, path: &Path) -> io::Result<SSTable>;
    fn wal_path(&self) -> &Path;
}