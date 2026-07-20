use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    vec,
};

use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};

use crate::{
    memtable::SkipList,
    types::{Key, Value},
};

pub struct SSTable {
    id: usize,
    path: PathBuf,
    min_key: Key,
    max_key: Key,
    entry_count: usize,
}

impl SSTable {
    pub fn new(id: usize, path: PathBuf, min_key: Key, max_key: Key, entry_count: usize) -> Self {
        SSTable {
            id,
            path,
            min_key,
            max_key,
            entry_count,
        }
    }

    pub fn create_from_skiplist(
        skiplist: &SkipList,
        id: usize,
        path: impl AsRef<Path>,
        include_tombstones: bool,
    ) -> io::Result<Self> {
        let mut keys_bytes = Vec::new();
        let mut values_bytes = Vec::new();
        let mut min_key = None;
        let mut max_key = None;
        let mut count = 0;

        for (key, value) in skiplist.iter() {
            if !include_tombstones && value.is_none() {
                continue;
            }
            let key_json = serde_json::to_vec(&key)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value_json = serde_json::to_vec(&value)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            keys_bytes.push(key_json);
            values_bytes.push(value_json);

            if min_key.is_none() || key < *min_key.as_ref().unwrap() {
                min_key = Some(key.clone());
            }
            if max_key.is_none() || key > *max_key.as_ref().unwrap() {
                max_key = Some(key.clone());
            }
            count += 1;
        }

        let min_key = min_key.unwrap_or_default();
        let max_key = max_key.unwrap_or_default();

        let schema = Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ]);

        let key_array = BinaryArray::from_iter_values(keys_bytes.iter().map(|v| v.as_slice()));
        let value_array = BinaryArray::from_iter_values(values_bytes.iter().map(|v| v.as_slice()));

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(key_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
            ],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let file = File::create(path.as_ref())?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        writer
            .write(&batch)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writer
            .close()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(SSTable {
            id,
            path: path.as_ref().to_path_buf(),
            min_key,
            max_key,
            entry_count: count,
        })
    }

    pub fn open_from_path(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut reader = builder.build()?;
        let mut min_key = None;
        let mut max_key = None;
        let mut count = 0;
        while let Some(batch) = reader.next() {
            let batch = batch.unwrap();
            let key_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let key_bytes = key_col.value(i);
                let k: Key = serde_json::from_slice(key_bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if min_key.is_none() || k < *min_key.as_ref().unwrap() {
                    min_key = Some(k.clone());
                }
                if max_key.is_none() || k > *max_key.as_ref().unwrap() {
                    max_key = Some(k.clone());
                }
                count += 1;
            }
        }
        let min_key = min_key.unwrap_or_default();
        let max_key = max_key.unwrap_or_default();
        let id = path
            .as_ref()
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .parse::<usize>()
            .unwrap();

        Ok(SSTable::new(
            id,
            path.as_ref().to_path_buf(),
            min_key,
            max_key,
            count,
        ))
    }

    pub fn get(&self, key: &Key) -> io::Result<Option<Option<Value>>> {
        if self.entry_count == 0 || key < &self.min_key || key > &self.max_key {
            return Ok(None);
        }

        let file = File::open(&self.path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build()?;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result.unwrap();
            let key_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("key column must be BinaryArray");
            let value_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("value column must be BinaryArray");

            for i in 0..batch.num_rows() {
                let key_bytes = key_col.value(i);
                let k: Key = serde_json::from_slice(key_bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if &k == key {
                    let val_bytes = value_col.value(i);
                    let v: Option<Value> = serde_json::from_slice(val_bytes)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    return Ok(Some(v));
                }
            }
        }

        Ok(None)
    }

    pub fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, Option<Value>)>> {
        if self.entry_count == 0 || start > end || end < &self.min_key || start > &self.max_key {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build()?;

        let mut results = Vec::new();
        while let Some(batch_result) = reader.next() {
            let batch = batch_result.unwrap();
            let key_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("key column must be BinaryArray");
            let value_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("value column must be BinaryArray");

            for i in 0..batch.num_rows() {
                let key_bytes = key_col.value(i);
                let k: Key = serde_json::from_slice(key_bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if k >= *start && k <= *end {
                    let val_bytes = value_col.value(i);
                    let v: Option<Value> = serde_json::from_slice(val_bytes)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    results.push((k, v));
                }
            }
        }

        Ok(results)
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub fn min_key(&self) -> &Key {
        &self.min_key
    }

    pub fn max_key(&self) -> &Key {
        &self.max_key
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
    fn test_sstable_create_and_get() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut skiplist = SkipList::new();
        skiplist.insert(k(10, 0), Some(v("ten")));
        skiplist.insert(k(20, 0), Some(v("twenty")));
        skiplist.insert(k(30, 0), Some(v("thirty")));

        let sstable = SSTable::create_from_skiplist(&skiplist, 1, &path, true)?;

        assert_eq!(sstable.entry_count(), 3);
        assert_eq!(sstable.min_key(), &k(10, 0));
        assert_eq!(sstable.max_key(), &k(30, 0));

        assert_eq!(sstable.get(&k(10, 0))?.unwrap(), Some(v("ten")));
        assert_eq!(sstable.get(&k(20, 0))?.unwrap(), Some(v("twenty")));
        assert_eq!(sstable.get(&k(30, 0))?.unwrap(), Some(v("thirty")));

        assert_eq!(sstable.get(&k(5, 0))?, None);
        assert_eq!(sstable.get(&k(25, 0))?, None);
        assert_eq!(sstable.get(&k(40, 0))?, None);

        assert_eq!(sstable.get(&k(10, 0))?.unwrap(), Some(v("ten")));
        assert_eq!(sstable.get(&k(30, 0))?.unwrap(), Some(v("thirty")));

        Ok(())
    }

    #[test]
    fn test_sstable_scan() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_scan.sst");

        let mut skiplist = SkipList::new();
        skiplist.insert(k(10, 0), Some(v("ten")));
        skiplist.insert(k(20, 0), Some(v("twenty")));
        skiplist.insert(k(30, 0), Some(v("thirty")));
        skiplist.insert(k(40, 0), Some(v("forty")));
        skiplist.insert(k(50, 0), Some(v("fifty")));

        let sstable = SSTable::create_from_skiplist(&skiplist, 1, &path, true)?;

        let result = sstable.scan(&k(20, 0), &k(40, 0))?;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, k(20, 0));
        assert_eq!(result[1].0, k(30, 0));
        assert_eq!(result[2].0, k(40, 0));

        let result = sstable.scan(&k(10, 0), &k(10, 0))?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, k(10, 0));

        let result = sstable.scan(&k(1, 0), &k(5, 0))?;
        assert!(result.is_empty());

        let result = sstable.scan(&k(60, 0), &k(70, 0))?;
        assert!(result.is_empty());

        let result = sstable.scan(&k(30, 0), &k(20, 0))?;
        assert!(result.is_empty());

        Ok(())
    }
}
