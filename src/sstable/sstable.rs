use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    vec,
};

use arrow::array::{ArrayRef, RecordBatch};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};

use crate::{
    memtable::SkipList,
    schema::{SemanticType, TableSchema, cells_to_array, parse_column_cells},
    types::{Key, Value},
};

fn key_at(cols: &[Vec<Option<Vec<u8>>>], schema: &TableSchema, i: usize) -> Key {
    let row: Vec<Vec<u8>> = (0..schema.columns.len())
        .map(|c| cols[c][i].clone().unwrap_or_default())
        .collect();
    schema.cells_to_key(&row)
}

fn value_at(cols: &[Vec<Option<Vec<u8>>>], schema: &TableSchema, i: usize) -> Option<Value> {
    let tombstone = schema
        .columns
        .iter()
        .enumerate()
        .any(|(c, col)| col.semantic == SemanticType::Field && cols[c][i].is_none());
    if tombstone {
        return None;
    }
    let row: Vec<Vec<u8>> = (0..schema.columns.len())
        .map(|c| cols[c][i].clone().unwrap_or_default())
        .collect();
    Some(schema.encode_fields(&row))
}

fn parse_batch(batch: &RecordBatch, schema: &TableSchema) -> Vec<Vec<Option<Vec<u8>>>> {
    (0..schema.columns.len())
        .map(|c| {
            parse_column_cells(
                &schema.columns[c].data_type,
                batch.column(c),
                batch.num_rows(),
            )
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct SSTable {
    id: usize,
    path: PathBuf,
    min_key: Key,
    max_key: Key,
    entry_count: usize,
    schema: Arc<TableSchema>,
}

impl SSTable {
    pub fn new(
        id: usize,
        path: PathBuf,
        min_key: Key,
        max_key: Key,
        entry_count: usize,
        schema: Arc<TableSchema>,
    ) -> Self {
        SSTable {
            id,
            path,
            min_key,
            max_key,
            entry_count,
            schema,
        }
    }

    pub fn create_from_skiplist(
        skiplist: &SkipList,
        id: usize,
        path: impl AsRef<Path>,
        include_tombstones: bool,
        schema: &TableSchema,
    ) -> io::Result<Self> {
        let ncols = schema.columns.len();
        let mut cols: Vec<Vec<Option<Vec<u8>>>> = vec![Vec::with_capacity(skiplist.len()); ncols];
        let mut min_key = None;
        let mut max_key = None;
        let mut count = 0;

        for (key, value) in skiplist.iter() {
            if !include_tombstones && value.is_none() {
                continue;
            }
            let cells = schema.key_to_cells(&key);
            let decoded = value.as_ref().map(|blob| schema.decode_fields(blob));
            let mut field_index = 0;
            for (i, col) in schema.columns.iter().enumerate() {
                if col.semantic == SemanticType::Field {
                    cols[i].push(match (&value, &decoded) {
                        (Some(_), Some(fcells)) => Some(fcells[field_index].clone()),
                        _ => None,
                    });
                    field_index += 1;
                } else {
                    cols[i].push(Some(cells[i].clone()));
                }
            }
            if min_key.is_none() || key < *min_key.as_ref().unwrap() {
                min_key = Some(key.clone());
            }
            if max_key.is_none() || key > *max_key.as_ref().unwrap() {
                max_key = Some(key.clone());
            }
            count += 1;
        }

        let arrays: Vec<ArrayRef> = (0..ncols)
            .map(|i| cells_to_array(&schema.columns[i].data_type, &cols[i]))
            .collect();
        let batch = RecordBatch::try_new(Arc::new(schema.arrow_schema()), arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let file = File::create(path.as_ref())?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.arrow_schema()), Some(props))
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
            schema: Arc::new(schema.clone()),
        })
    }

    pub fn open_from_path(path: impl AsRef<Path>, schema: &TableSchema) -> io::Result<Self> {
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder.build()?;
        let mut min_key: Option<Key> = None;
        let mut max_key: Option<Key> = None;
        let mut count = 0;
        for batch in reader {
            let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let cols = parse_batch(&batch, schema);
            for i in 0..batch.num_rows() {
                let k = key_at(&cols, schema, i);
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
            Arc::new(schema.clone()),
        ))
    }

    pub fn get(&self, key: &Key) -> io::Result<Option<Option<Value>>> {
        if self.entry_count == 0 || key < &self.min_key || key > &self.max_key {
            return Ok(None);
        }

        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let cols = parse_batch(&batch, &self.schema);

            for i in 0..batch.num_rows() {
                let k = key_at(&cols, &self.schema, i);
                if k > *key {
                    return Ok(None);
                }
                if k == *key {
                    return Ok(Some(value_at(&cols, &self.schema, i)));
                }
            }
        }

        Ok(None)
    }

    pub fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, Option<Value>)>> {
        if self.entry_count == 0 || start > end || end < &self.min_key || start > &self.max_key {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder.build()?;

        let mut results = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let cols = parse_batch(&batch, &self.schema);

            for i in 0..batch.num_rows() {
                let k = key_at(&cols, &self.schema, i);
                if k > *end {
                    return Ok(results);
                }
                if k >= *start {
                    results.push((k, value_at(&cols, &self.schema, i)));
                }
            }
        }

        Ok(results)
    }

    pub fn scan_iter(&self, start: &Key, end: &Key) -> io::Result<SSTableIter> {
        if self.entry_count == 0 || start > end || end < &self.min_key || start > &self.max_key {
            return Ok(SSTableIter::empty(
                start.clone(),
                end.clone(),
                self.schema.clone(),
            ));
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
            schema: self.schema.clone(),
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
    schema: Arc<TableSchema>,
}

impl SSTableIter {
    fn empty(start: Key, end: Key, schema: Arc<TableSchema>) -> Self {
        Self {
            inner: Box::new(std::iter::empty()),
            current: Vec::new(),
            pos: 0,
            start,
            end,
            schema,
        }
    }

    fn fill(&mut self) -> io::Result<bool> {
        while self.pos >= self.current.len() {
            match self.inner.next() {
                None => return Ok(false),
                Some(batch) => {
                    let batch = batch?;
                    let cols = parse_batch(&batch, &self.schema);
                    self.current.clear();
                    self.pos = 0;
                    for i in 0..batch.num_rows() {
                        let k = key_at(&cols, &self.schema, i);
                        if k > self.end {
                            break;
                        }
                        if k >= self.start {
                            self.current.push((k, value_at(&cols, &self.schema, i)));
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
        if self.pos >= self.current.len() && !(self.fill().ok()?) {
            return None;
        }

        let item = self.current[self.pos].clone();
        self.pos += 1;
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memtable::SkipList, schema::ColumnDef};
    use tempfile::tempdir;
    use arrow_schema::DataType;

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

        let sstable = SSTable::create_from_skiplist(
            &skiplist,
            1,
            &path,
            true,
            &TableSchema::default_table(),
        )?;

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

        let sstable = SSTable::create_from_skiplist(
            &skiplist,
            1,
            &path,
            true,
            &TableSchema::default_table(),
        )?;

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

        SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;

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
        let sst =
            SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;

        assert_eq!(sst.get(&(vec![1], 20))?.unwrap(), None);
        assert_eq!(sst.get(&(vec![1], 10))?.unwrap(), Some(v("a")));
        assert_eq!(sst.get(&(vec![9], 99))?, None);

        let reopened = SSTable::open_from_path(&path, &TableSchema::default_table())?;
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
        let sst =
            SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;
        let got: Vec<_> = sst.scan_iter(&(vec![3], 3), &(vec![6], 6))?.collect();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].0, (vec![3], 3));
        assert_eq!(got[3].0, (vec![6], 6));
        Ok(())
    }

    fn schema3() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnDef {
                    name: "host".into(),
                    data_type: DataType::Binary,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "region".into(),
                    data_type: DataType::Binary,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "ts".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Timestamp,
                },
                ColumnDef {
                    name: "cpu".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Field,
                },
                ColumnDef {
                    name: "mem".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Field,
                },
            ],
            primary_key: vec![0, 1],
            time_index: 2,
        }
    }

    fn cells(host: &[u8], region: &[u8], ts: i64, cpu: i64, mem: i64) -> Vec<Vec<u8>> {
        vec![
            host.to_vec(),
            region.to_vec(),
            ts.to_le_bytes().to_vec(),
            cpu.to_le_bytes().to_vec(),
            mem.to_le_bytes().to_vec(),
        ]
    }

    #[test]
    fn test_sstable_multi_tag_roundtrip() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let schema = schema3();
        let list = SkipList::new();
        let rows = vec![
            cells(b"h1", b"cn", 100, 1, 2),
            cells(b"h1", b"cn", 200, 3, 4),
            cells(b"h2", b"us", 100, 5, 6),
        ];
        for c in &rows {
            list.insert(schema.cells_to_key(c), Some(schema.encode_fields(c)));
        }
        let sst = SSTable::create_from_skiplist(&list, 1, &path, true, &schema)?;
        let k = schema.cells_to_key(&rows[1]);
        assert_eq!(sst.get(&k)?.unwrap(), Some(schema.encode_fields(&rows[1])));

        let reopened = SSTable::open_from_path(&path, &schema)?;
        assert_eq!(reopened.entry_count(), 3);
        assert_eq!(reopened.min_key(), sst.min_key());
        let got = sst.scan(sst.min_key(), sst.max_key())?;
        assert_eq!(got.len(), 3);
        Ok(())
    }
}
