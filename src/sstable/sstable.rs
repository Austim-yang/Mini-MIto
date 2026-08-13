use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    vec,
};

use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};

use crate::{
    memtable::SkipList,
    types::{Key, Value},
};

const TAG_COL: usize = 0;
const TS_COL: usize = 1;
const FIELDS_COL: usize = 2;

fn sst_schema() -> Schema {
    Schema::new(vec![
        Field::new("tags", DataType::Binary, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("fields", DataType::Binary, true),
    ])
}

#[derive(Clone, Debug)]
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
        let mut tag_buf = Vec::with_capacity(skiplist.len());
        let mut ts_buf = Vec::with_capacity(skiplist.len());
        let mut field_buf = Vec::with_capacity(skiplist.len());
        let mut min_key = None;
        let mut max_key = None;
        let mut count = 0;

        for (key, value) in skiplist.iter() {
            if !include_tombstones && value.is_none() {
                continue;
            }
            if min_key.is_none() || key < *min_key.as_ref().unwrap() {
                min_key = Some(key.clone());
            }
            if max_key.is_none() || key > *max_key.as_ref().unwrap() {
                max_key = Some(key.clone());
            }
            let (tags, ts) = key;
            tag_buf.push(tags);
            ts_buf.push(ts);
            field_buf.push(value);
            count += 1;
        }

        let batch = RecordBatch::try_new(
            Arc::new(sst_schema()),
            vec![
                Arc::new(BinaryArray::from_iter_values(tag_buf)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(ts_buf)) as ArrayRef,
                Arc::new(BinaryArray::from_iter(
                    field_buf.iter().map(|o| o.as_deref()),
                )) as ArrayRef,
            ],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let file = File::create(path.as_ref())?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(sst_schema()), Some(props))
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
            min_key: min_key.unwrap_or_default(),
            max_key: max_key.unwrap_or_default(),
            entry_count: count,
        })
    }

    pub fn open_from_path(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut reader = builder.build()?;
        let mut min_key: Option<Key> = None;
        let mut max_key: Option<Key> = None;
        let mut count = 0;
        while let Some(batch) = reader.next() {
            let batch = batch
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                .unwrap();
            let tags = batch
                .column(TAG_COL)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let ts = batch
                .column(TS_COL)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let k = (tags.value(i).to_vec(), ts.value(i));
                if min_key.is_none() || k < *min_key.as_ref().unwrap() {
                    min_key = Some(k.clone());
                }
                if max_key.is_none() || k > *max_key.as_ref().unwrap() {
                    max_key = Some(k.clone());
                }
                count += 1;
            }
        }
        let id = path
            .as_ref()
            .file_stem()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "sst file has no stem"))?
            .to_string_lossy()
            .parse::<usize>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(SSTable::new(
            id,
            path.as_ref().to_path_buf(),
            min_key.unwrap_or_default(),
            max_key.unwrap_or_default(),
            count,
        ))
    }

    pub fn get(&self, key: &Key) -> io::Result<Option<Option<Value>>> {
        if self.entry_count == 0 || key < &self.min_key || key > &self.max_key {
            return Ok(None);
        }

        let file = File::open(&self.path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            .unwrap();
        let mut reader = builder.build()?;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result.unwrap();
            let tags = batch
                .column(TAG_COL)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("key column must be BinaryArray");
            let ts = batch
                .column(TS_COL)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column must be Int64Array");
            let fields = batch
                .column(FIELDS_COL)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("value column must be BinaryArray");

            for i in 0..batch.num_rows() {
                let k = (tags.value(i).to_vec(), ts.value(i));
                if k > *key {
                    return Ok(None);
                }
                if k == *key {
                    let v = (!fields.is_null(i)).then(|| fields.value(i).to_vec());
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
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            .unwrap();
        let mut reader = builder.build()?;

        let mut results = Vec::new();
        while let Some(batch_result) = reader.next() {
            let batch = batch_result.unwrap();
            let tags = batch
                .column(TAG_COL)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("key column must be BinaryArray");
            let ts = batch
                .column(TS_COL)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column must be Int64Array");
            let fields = batch
                .column(FIELDS_COL)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("value column must be BinaryArray");

            for i in 0..batch.num_rows() {
                let k = (tags.value(i).to_vec(), ts.value(i));
                if k > *end {
                    return Ok(results);
                }
                if k >= *start {
                    let v = (!fields.is_null(i)).then(|| fields.value(i).to_vec());
                    results.push((k, v));
                }
            }
        }

        Ok(results)
    }

    pub fn scan_iter(&self, start: &Key, end: &Key) -> io::Result<SSTableIter> {
        if self.entry_count == 0 || start > end || end < &self.min_key || start > &self.max_key {
            return Ok(SSTableIter {
                inner: Box::new(std::iter::empty()),
                current: Vec::new(),
                pos: 0,
                start: start.clone(),
                end: end.clone(),
            });
        }
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder.build()?;
        Ok(SSTableIter {
            inner: Box::new(
                reader.map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))),
            ),
            current: Vec::new(),
            pos: 0,
            start: start.clone(),
            end: end.clone(),
        })
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

pub struct SSTableIter {
    inner: Box<dyn Iterator<Item = io::Result<RecordBatch>> + Send>,
    current: Vec<(Key, Option<Value>)>,
    pos: usize,
    start: Key,
    end: Key,
}

impl SSTableIter {
    fn fill(&mut self) -> io::Result<bool> {
        while self.pos >= self.current.len() {
            match self.inner.next() {
                None => return Ok(false),
                Some(batch) => {
                    let batch = batch?;
                    let tags = batch
                        .column(TAG_COL)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    let ts = batch
                        .column(TS_COL)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let fields = batch
                        .column(FIELDS_COL)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    self.current.clear();
                    self.pos = 0;
                    for i in 0..batch.num_rows() {
                        let k = (tags.value(i).to_vec(), ts.value(i));
                        if k > self.end {
                            break;
                        }
                        if k >= self.start {
                            let v = (!fields.is_null(i)).then(|| fields.value(i).to_vec());
                            self.current.push((k, v));
                        }
                    }
                }
            }
        }
        Ok(true)
    }
}

impl Iterator for SSTableIter {
    type Item = (Key, Option<Value>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.current.len() {
            if self.fill().ok()? == false {
                return None;
            }
        }
        let item = self.current[self.pos].clone();
        self.pos += 1;
        Some(item)
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

        let skiplist = SkipList::new();
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

        let skiplist = SkipList::new();
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

    #[test]
    fn test_sstable_arrow_native_schema() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("native.sst");
        let list = SkipList::new();
        list.insert((vec![1], 100), Some(v("a")));
        list.insert((vec![1], 200), None);
        list.insert((vec![2], 100), Some(v("b")));

        SSTable::create_from_skiplist(&list, 1, &path, true)?;

        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema().fields().len(), 3);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
        assert!(batch.column(2).is_null(1));

        Ok(())
    }

    #[test]
    fn test_sstable_tombstone_roundtrip() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let list = SkipList::new();
        list.insert((vec![1], 10), Some(v("a")));
        list.insert((vec![1], 20), None);
        let sst = SSTable::create_from_skiplist(&list, 1, &path, true)?;

        assert_eq!(sst.get(&(vec![1], 20))?.unwrap(), None);
        assert_eq!(sst.get(&(vec![1], 10))?.unwrap(), Some(v("a")));
        assert_eq!(sst.get(&(vec![9], 99))?, None);

        let reopened = SSTable::open_from_path(&path)?;
        assert_eq!(reopened.min_key(), &(vec![1], 10));
        assert_eq!(reopened.max_key(), &(vec![1], 20));
        assert_eq!(reopened.entry_count(), 2);
        Ok(())
    }

    #[test]
    fn test_sstable_scan_iter() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("si.sst");
        let list = SkipList::new();
        for i in 0..10 {
            list.insert((vec![i], i as i64), Some(v(&format!("v{}", i))));
        }
        let sst = SSTable::create_from_skiplist(&list, 1, &path, true)?;
        let got: Vec<_> = sst.scan_iter(&(vec![3], 3), &(vec![6], 6))?.collect();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].0, (vec![3], 3));
        assert_eq!(got[3].0, (vec![6], 6));
        Ok(())
    }
}
