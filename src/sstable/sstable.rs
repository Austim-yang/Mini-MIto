use std::{
    collections::HashMap,
    fmt::Write,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    vec,
};

use arrow::array::{ArrayRef, Int8Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};

use crate::{
    memtable::SkipList,
    schema::{BatchView, SemanticType, TableSchema, cells_to_array},
    sstable::bloom::BloomFilter,
    types::{Key, Value},
};

const CHUNK_ROWS: usize = 8192;
const INDEX_META_KEY: &str = "sstable.index";
const INDEX_VERSION: u32 = 2;
const SEQ_COL: &str = "__seq";
const OP_COL: &str = "__op_type";
const OP_PUT: i8 = 0;
const OP_DELETE: i8 = 1;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowGroupMeta {
    pub(crate) num_rows: usize,
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
}

#[derive(Clone, Debug)]
pub(crate) struct SstableIndex {
    pub(crate) bloom: BloomFilter,
    pub(crate) row_groups: Vec<RowGroupMeta>,
    pub(crate) max_seq: u64,
}

impl SstableIndex {
    pub(crate) fn to_json(&self) -> io::Result<String> {
        let dto = IndexDto {
            version: INDEX_VERSION,
            bloom: BloomDto {
                m: self.bloom.m,
                k: self.bloom.k,
                seed: self.bloom.seed,
                bits: base64::engine::general_purpose::STANDARD.encode(&self.bloom.bits),
            },
            row_groups: self
                .row_groups
                .iter()
                .map(|rg| RowGroupDto {
                    num_rows: rg.num_rows,
                    min_key: hex_key(&rg.min_key),
                    max_key: hex_key(&rg.max_key),
                })
                .collect(),
            max_seq: self.max_seq,
        };
        serde_json::to_string(&dto).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub(crate) fn from_json(s: &str) -> io::Result<Arc<SstableIndex>> {
        let dto: IndexDto =
            serde_json::from_str(s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if dto.version != INDEX_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported sstable index version",
            ));
        }
        let bits = base64::engine::general_purpose::STANDARD
            .decode(dto.bloom.bits)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if dto.bloom.m < 1024 || dto.bloom.k == 0 || bits.len() != ((dto.bloom.m + 7) / 8) as usize
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupt bloom params",
            ));
        }
        let bloom = BloomFilter {
            m: dto.bloom.m,
            k: dto.bloom.k,
            seed: dto.bloom.seed,
            bits,
        };
        let row_groups = dto
            .row_groups
            .iter()
            .map(from_row_group_dto)
            .collect::<io::Result<_>>()?;
        Ok(Arc::new(SstableIndex {
            bloom,
            row_groups,
            max_seq: dto.max_seq,
        }))
    }

    pub(crate) fn load_from_file(path: &Path) -> io::Result<Arc<SstableIndex>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let json = builder
            .schema()
            .metadata()
            .get(INDEX_META_KEY)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "sst file lacks index metadata")
            })?;
        Self::from_json(json)
    }

    pub(crate) fn bounds(&self) -> (Key, Key, usize) {
        let min = self
            .row_groups
            .first()
            .map(|rg| rg.min_key.clone())
            .unwrap_or_default();
        let max = self
            .row_groups
            .last()
            .map(|rg| rg.max_key.clone())
            .unwrap_or_default();
        let count = self.row_groups.iter().map(|rg| rg.num_rows).sum();
        (min, max, count)
    }
}

#[derive(Serialize, Deserialize)]
struct IndexDto {
    version: u32,
    bloom: BloomDto,
    row_groups: Vec<RowGroupDto>,
    max_seq: u64,
}
#[derive(Serialize, Deserialize)]
struct BloomDto {
    m: u64,
    k: u32,
    seed: u64,
    bits: String,
}
#[derive(Serialize, Deserialize)]
struct RowGroupDto {
    num_rows: usize,
    min_key: HexKeyDto,
    max_key: HexKeyDto,
}
#[derive(Serialize, Deserialize)]
struct HexKeyDto {
    tag: String,
    ts: i64,
}

fn hex_key(k: &Key) -> HexKeyDto {
    HexKeyDto {
        tag: to_hex(&k.0),
        ts: k.1,
    }
}
fn from_row_group_dto(d: &RowGroupDto) -> io::Result<RowGroupMeta> {
    Ok(RowGroupMeta {
        num_rows: d.num_rows,
        min_key: (from_hex(&d.min_key.tag)?, d.min_key.ts),
        max_key: (from_hex(&d.max_key.tag)?, d.max_key.ts),
    })
}

fn to_hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(result, "{:02x}", b).unwrap();
    }
    result
}

fn from_hex(s: &str) -> io::Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "hex string has odd length",
        ));
    }

    let mut chars = s.chars();
    let mut result = Vec::with_capacity(s.len() / 2);

    while let Some(high) = chars.next() {
        let low = chars.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "hex string is incomplete")
        })?;
        let high_val = high
            .to_digit(16)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid hex character"))?;
        let low_val = low
            .to_digit(16)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid hex character"))?;
        result.push((high_val * 16 + low_val) as u8);
    }

    Ok(result)
}

fn key_at(view: &BatchView, schema: &TableSchema, i: usize) -> Key {
    let tags = if schema.primary_key.len() == 1 {
        view.cell(schema.primary_key[0], i).unwrap_or_default()
    } else {
        let mut cells = vec![Vec::new(); schema.columns.len()];
        for &idx in &schema.primary_key {
            cells[idx] = view.cell(idx, i).unwrap_or_default();
        }
        schema.encode_tags(&cells)
    };
    let ts = view.ts_value(schema.time_index, i);
    (tags, ts)
}

fn value_at(
    view: &BatchView,
    schema: &TableSchema,
    field_cols: &[usize],
    i: usize,
) -> Option<Value> {
    if view.op_type(i) == OP_DELETE {
        return None;
    }
    if field_cols.is_empty() {
        return Some(Vec::new());
    }
    if field_cols.iter().any(|&c| view.is_null(c, i)) {
        return None;
    }
    if field_cols.len() == 1 {
        return view.cell(field_cols[0], i);
    }
    let mut cells = vec![Vec::new(); schema.columns.len()];
    for &c in field_cols {
        cells[c] = view.cell(c, i).unwrap_or_default();
    }
    Some(schema.encode_fields(&cells))
}

fn sst_schema(schema: &TableSchema) -> Schema {
    let mut fields: Vec<Field> = schema
        .arrow_schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    fields.push(Field::new(SEQ_COL, DataType::Int64, false));
    fields.push(Field::new(OP_COL, DataType::Int8, false));
    Schema::new(fields)
}

fn build_batch(
    rows: &[(Key, u64, Option<Value>)],
    schema: &TableSchema,
) -> io::Result<RecordBatch> {
    let ncols = schema.columns.len();
    let mut cols: Vec<Vec<Option<Vec<u8>>>> = vec![Vec::with_capacity(rows.len()); ncols];

    let nfields = schema
        .columns
        .iter()
        .filter(|c| c.semantic == SemanticType::Field)
        .count();
    let single_field_col = (nfields == 1).then(|| {
        schema
            .columns
            .iter()
            .position(|c| c.semantic == SemanticType::Field)
            .unwrap()
    });

    for (key, _seq, value) in rows {
        if schema.primary_key.len() == 1 {
            cols[schema.primary_key[0]].push(Some(key.0.clone()));
        } else {
            let tags = schema.decode_tags(&key.0);
            for (j, &idx) in schema.primary_key.iter().enumerate() {
                cols[idx].push(Some(tags[j].clone()));
            }
        }
        cols[schema.time_index].push(Some(key.1.to_le_bytes().to_vec()));
        match value {
            Some(blob) => match single_field_col {
                Some(idx) => cols[idx].push(Some(blob.clone())),
                None => {
                    let fcells = schema.decode_fields(blob);
                    let mut k = 0;
                    for (i, col) in schema.columns.iter().enumerate() {
                        if col.semantic == SemanticType::Field {
                            cols[i].push(Some(fcells[k].clone()));
                            k += 1;
                        }
                    }
                }
            },
            None => {
                for (i, col) in schema.columns.iter().enumerate() {
                    if col.semantic == SemanticType::Field {
                        cols[i].push(None);
                    }
                }
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = (0..ncols)
        .map(|i| cells_to_array(&schema.columns[i].data_type, &cols[i]))
        .collect();
    arrays.push(Arc::new(Int64Array::from_iter(
        rows.iter().map(|(_, seq, _)| Some(*seq as i64)),
    )));
    arrays.push(Arc::new(Int8Array::from_iter(rows.iter().map(
        |(_, _, value)| Some(if value.is_some() { OP_PUT } else { OP_DELETE }),
    ))));
    RecordBatch::try_new(Arc::new(sst_schema(schema)), arrays)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[derive(Clone, Debug)]
pub struct SSTable {
    id: usize,
    path: PathBuf,
    min_key: Key,
    max_key: Key,
    max_seq: u64,
    entry_count: usize,
    schema: Arc<TableSchema>,
    index: Arc<SstableIndex>,
}

impl SSTable {
    pub(crate) fn new(
        id: usize,
        path: PathBuf,
        min_key: Key,
        max_key: Key,
        entry_count: usize,
        schema: Arc<TableSchema>,
        index: Arc<SstableIndex>,
    ) -> Self {
        SSTable {
            id,
            path,
            min_key,
            max_key,
            entry_count,
            max_seq: index.max_seq,
            schema,
            index,
        }
    }

    pub fn create_from_skiplist(
        skiplist: &SkipList,
        id: usize,
        path: impl AsRef<Path>,
        include_tombstones: bool,
        schema: &TableSchema,
    ) -> io::Result<Self> {
        let seed = rand::random::<u64>();
        let mut bloom = BloomFilter::new(skiplist.len(), 0.01, seed);

        let mut row_groups: Vec<RowGroupMeta> = Vec::new();
        let mut max_seq: u64 = 0;
        let mut chunk_rows: usize = 0;
        let mut chunk_min: Option<Key> = None;
        let mut chunk_max: Option<Key> = None;

        for (key, seq, value) in skiplist.iter() {
            if !include_tombstones && value.is_none() {
                continue;
            }

            max_seq = max_seq.max(seq);
            bloom.insert(&key);
            if chunk_min.is_none() {
                chunk_min = Some(key.clone());
            }
            chunk_max = Some(key.clone());
            chunk_rows += 1;
            if chunk_rows == CHUNK_ROWS {
                row_groups.push(RowGroupMeta {
                    num_rows: chunk_rows,
                    min_key: chunk_min.take().unwrap(),
                    max_key: chunk_max.take().unwrap(),
                });
                chunk_rows = 0;
            }
        }

        if chunk_rows > 0 {
            row_groups.push(RowGroupMeta {
                num_rows: chunk_rows,
                min_key: chunk_min.unwrap(),
                max_key: chunk_max.unwrap(),
            });
        }

        let index = SstableIndex {
            bloom,
            row_groups,
            max_seq,
        };
        let meta = HashMap::from([(INDEX_META_KEY.to_string(), index.to_json()?)]);
        let arrow_schema = sst_schema(schema).with_metadata(meta);

        let final_path = path.as_ref().to_path_buf();
        let tmp_path = final_path.with_extension("sst.tmp");
        let file = File::create(&tmp_path)?;
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(CHUNK_ROWS))
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(arrow_schema), Some(props))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut current: Vec<(Key, u64, Option<Value>)> = Vec::with_capacity(CHUNK_ROWS);
        for (key, seq, value) in skiplist.iter() {
            if !include_tombstones && value.is_none() {
                continue;
            }
            current.push((key.clone(), seq, value.clone()));
            if current.len() == CHUNK_ROWS {
                writer
                    .write(&build_batch(&current, schema)?)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                current.clear();
            }
        }

        if !current.is_empty() {
            writer
                .write(&build_batch(&current, schema)?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
        writer
            .close()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::rename(&tmp_path, &final_path)?;

        let (min_key, max_key, entry_count) = index.bounds();
        Ok(SSTable {
            id,
            path: final_path,
            min_key,
            max_key,
            entry_count,
            max_seq,
            schema: Arc::new(schema.clone()),
            index: Arc::new(index),
        })
    }

    pub fn open_from_path(path: impl AsRef<Path>, schema: &TableSchema) -> io::Result<Self> {
        let index = SstableIndex::load_from_file(path.as_ref())?;
        let (min_key, max_key, entry_count) = index.bounds();
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
            min_key,
            max_key,
            entry_count,
            Arc::new(schema.clone()),
            index,
        ))
    }

    fn candidate_row_group(&self, key: &Key) -> Option<usize> {
        let rgs = &self.index.row_groups;
        let i = rgs.partition_point(|rg| rg.max_key < *key);
        let rg = rgs.get(i)?;
        (rg.min_key <= *key).then_some(i)
    }

    pub fn get(&self, key: &Key) -> io::Result<Option<(u64, Option<Value>)>> {
        if self.entry_count == 0 || key < &self.min_key || key > &self.max_key {
            return Ok(None);
        }

        if !self.index.bloom.contains(key) {
            return Ok(None);
        }

        let Some(rg) = self.candidate_row_group(key) else {
            return Ok(None);
        };

        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder.with_row_groups(vec![rg]).build()?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let view = BatchView::new(&batch, &self.schema);
            let field_cols: Vec<usize> = self
                .schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.semantic == SemanticType::Field)
                .map(|(c, _)| c)
                .collect();

            for i in 0..batch.num_rows() {
                let k = key_at(&view, &self.schema, i);
                if k > *key {
                    return Ok(None);
                }
                if k == *key {
                    let seq = view.seq_value(i) as u64;
                    return Ok(Some((seq, value_at(&view, &self.schema, &field_cols, i))));
                }
            }
        }

        Ok(None)
    }

    fn overlapping_row_groups(&self, start: &Key, end: &Key) -> Option<(usize, usize)> {
        let rgs = &self.index.row_groups;
        if rgs.is_empty() {
            return None;
        }
        let first = rgs.partition_point(|rg| rg.max_key < *start);
        let last = rgs
            .partition_point(|rg| rg.min_key <= *end)
            .saturating_sub(1);
        (first <= last).then_some((first, last))
    }

    pub fn scan(&self, start: &Key, end: &Key) -> io::Result<Vec<(Key, u64, Option<Value>)>> {
        if self.entry_count == 0 || start > end || end < &self.min_key || start > &self.max_key {
            return Ok(Vec::new());
        }

        let Some((first, last)) = self.overlapping_row_groups(start, end) else {
            return Ok(Vec::new());
        };

        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder.with_row_groups((first..=last).collect()).build()?;

        let mut results = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let view = BatchView::new(&batch, &self.schema);
            let field_cols: Vec<usize> = self
                .schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.semantic == SemanticType::Field)
                .map(|(c, _)| c)
                .collect();

            for i in 0..batch.num_rows() {
                let k = key_at(&view, &self.schema, i);
                if k > *end {
                    return Ok(results);
                }
                if k >= *start {
                    results.push((
                        k,
                        view.seq_value(i) as u64,
                        value_at(&view, &self.schema, &field_cols, i),
                    ));
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

        let Some((first, last)) = self.overlapping_row_groups(start, end) else {
            return Ok(SSTableIter::empty(
                start.clone(),
                end.clone(),
                self.schema.clone(),
            ));
        };
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder.with_row_groups((first..=last).collect()).build()?;
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

    pub fn max_seq(&self) -> u64 {
        self.max_seq
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
    current: Vec<(Key, u64, Option<Value>)>,
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
                    let view = BatchView::new(&batch, &self.schema);
                    let field_cols: Vec<usize> = self
                        .schema
                        .columns
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| c.semantic == SemanticType::Field)
                        .map(|(c, _)| c)
                        .collect();
                    self.current.clear();
                    self.pos = 0;
                    for i in 0..batch.num_rows() {
                        let k = key_at(&view, &self.schema, i);
                        if k > self.end {
                            break;
                        }
                        if k >= self.start {
                            self.current.push((
                                k,
                                view.seq_value(i) as u64,
                                value_at(&view, &self.schema, &field_cols, i),
                            ));
                        }
                    }
                }
            }
        }
        Ok(true)
    }
}

impl Iterator for SSTableIter {
    type Item = (Key, u64, Option<Value>);
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
    use arrow_schema::DataType;
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
        skiplist.insert(k(10, 0), 1, Some(v("ten")));
        skiplist.insert(k(20, 0), 2, Some(v("twenty")));
        skiplist.insert(k(30, 0), 3, Some(v("thirty")));

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
        assert_eq!(sstable.max_seq(), 3);

        assert_eq!(sstable.get(&k(10, 0))?.unwrap().0, 1);
        assert_eq!(sstable.get(&k(10, 0))?.unwrap().1, Some(v("ten")));
        assert_eq!(sstable.get(&k(20, 0))?.unwrap().0, 2);
        assert_eq!(sstable.get(&k(20, 0))?.unwrap().1, Some(v("twenty")));
        assert_eq!(sstable.get(&k(30, 0))?.unwrap().1, Some(v("thirty")));

        assert_eq!(sstable.get(&k(5, 0))?, None);
        assert_eq!(sstable.get(&k(25, 0))?, None);
        assert_eq!(sstable.get(&k(40, 0))?, None);

        Ok(())
    }

    #[test]
    fn test_sstable_scan() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_scan.sst");

        let skiplist = SkipList::new();
        skiplist.insert(k(10, 0), 1, Some(v("ten")));
        skiplist.insert(k(20, 0), 2, Some(v("twenty")));
        skiplist.insert(k(30, 0), 3, Some(v("thirty")));
        skiplist.insert(k(40, 0), 4, Some(v("forty")));
        skiplist.insert(k(50, 0), 5, Some(v("fifty")));

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
        list.insert((vec![1], 100), 1, Some(v("a")));
        list.insert((vec![1], 200), 2, None);
        list.insert((vec![2], 100), 3, Some(v("b")));

        SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;

        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema().fields().len(), 5);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
        assert!(batch.column(2).is_null(1));

        assert_eq!(batch.schema().field(3).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(4).data_type(), &DataType::Int8);
        let seq = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(seq.value(0), 1);
        assert_eq!(seq.value(1), 2);
        assert_eq!(seq.value(2), 3);
        let op = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(op.value(0), OP_PUT);
        assert_eq!(op.value(1), OP_DELETE);
        assert_eq!(op.value(2), OP_PUT);

        Ok(())
    }

    #[test]
    fn test_sstable_tombstone_roundtrip() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let list = SkipList::new();
        list.insert((vec![1], 10), 1, Some(v("a")));
        list.insert((vec![1], 20), 2, None);
        let sst =
            SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;

        assert_eq!(sst.get(&(vec![1], 20))?.unwrap().0, 2);
        assert_eq!(sst.get(&(vec![1], 20))?.unwrap().1, None);
        assert_eq!(sst.get(&(vec![1], 10))?.unwrap().1, Some(v("a")));
        assert_eq!(sst.get(&(vec![9], 99))?, None);
        assert_eq!(sst.get(&(vec![1], 20))?.unwrap().0, 2);
        assert_eq!(sst.get(&(vec![1], 20))?.unwrap().1, None);
        assert_eq!(sst.get(&(vec![1], 10))?.unwrap().1, Some(v("a")));
        assert_eq!(sst.get(&(vec![9], 99))?, None);

        let reopened = SSTable::open_from_path(&path, &TableSchema::default_table())?;
        assert_eq!(reopened.min_key(), &(vec![1], 10));
        assert_eq!(reopened.max_key(), &(vec![1], 20));
        assert_eq!(reopened.entry_count(), 2);
        assert_eq!(reopened.max_seq(), 2);
        Ok(())
    }

    #[test]
    fn test_sstable_scan_iter() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("si.sst");
        let list = SkipList::new();
        for i in 0..10u64 {
            list.insert(
                (vec![i as u8], i as i64),
                i + 1,
                Some(v(&format!("v{}", i))),
            );
        }
        let sst =
            SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;
        let got: Vec<_> = sst.scan_iter(&(vec![3], 3), &(vec![6], 6))?.collect();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].0, (vec![3], 3));
        assert_eq!(got[0].1, 4);
        assert_eq!(got[3].0, (vec![6], 6));
        assert_eq!(got[3].1, 7);
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
        for (i, c) in rows.iter().enumerate() {
            list.insert(
                schema.cells_to_key(c),
                i as u64 + 1,
                Some(schema.encode_fields(c)),
            );
        }
        let sst = SSTable::create_from_skiplist(&list, 1, &path, true, &schema)?;
        let k = schema.cells_to_key(&rows[1]);
        assert_eq!(
            sst.get(&k)?.unwrap().1,
            Some(schema.encode_fields(&rows[1]))
        );
        assert_eq!(sst.get(&k)?.unwrap().0, 2);

        let reopened = SSTable::open_from_path(&path, &schema)?;
        assert_eq!(reopened.entry_count(), 3);
        assert_eq!(reopened.min_key(), sst.min_key());
        let got = sst.scan(sst.min_key(), sst.max_key())?;
        assert_eq!(got.len(), 3);
        Ok(())
    }

    #[test]
    fn index_json_roundtrip() -> io::Result<()> {
        let mut bloom = BloomFilter::new(100, 0.01, 123);
        for i in 0..100 {
            bloom.insert(&k((i % 5) as u8, i));
        }
        let index = SstableIndex {
            bloom,
            row_groups: vec![
                RowGroupMeta {
                    num_rows: 50,
                    min_key: k(0, 0),
                    max_key: k(4, 49),
                },
                RowGroupMeta {
                    num_rows: 50,
                    min_key: k(4, 50),
                    max_key: k(9, 99),
                },
            ],
            max_seq: 99,
        };
        let json = index.to_json()?;
        let decoded = SstableIndex::from_json(&json)?;
        assert_eq!(decoded.bloom.m, index.bloom.m);
        assert_eq!(decoded.bloom.k, index.bloom.k);
        assert_eq!(decoded.bloom.seed, index.bloom.seed);
        assert_eq!(decoded.bloom.bits, index.bloom.bits);
        assert_eq!(decoded.row_groups, index.row_groups);
        assert_eq!(decoded.max_seq, 99);
        for i in 0..100 {
            assert!(decoded.bloom.contains(&k((i % 5) as u8, i)));
        }
        Ok(())
    }

    #[test]
    fn index_json_rejects_bad_version() {
        let err = SstableIndex::from_json(
            r#"{"version":999,"bloom":{"m":1024,"k":1,"seed":0,"bits":""},"row_groups":[],"max_seq":0}"#,
        );
        assert!(err.is_err());
    }

    #[test]
    fn index_json_rejects_bad_base64() {
        let err = SstableIndex::from_json(
            r#"{"version":2,"bloom":{"m":1024,"k":1,"seed":0,"bits":"!!!not-base64!!!"},"row_groups":[],"max_seq":0}"#,
        );
        assert!(err.is_err());
    }

    #[test]
    fn index_json_rejects_bad_hex() {
        let err = SstableIndex::from_json(
            r#"{"version":2,"bloom":{"m":1024,"k":1,"seed":0,"bits":"AA=="},
                "row_groups":[{"num_rows":1,"min_key":{"tag":"zz","ts":0},"max_key":{"tag":"aa","ts":1}}],
                "max_seq":0}"#,
        );
        assert!(err.is_err());
    }

    #[test]
    fn test_sstable_multi_chunk_roundtrip() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let list = SkipList::new();
        for i in 0..20_000i64 {
            list.insert((vec![0], i), i as u64 + 1, Some(v(&format!("v{}", i))));
        }
        let sst =
            SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;
        assert_eq!(sst.entry_count(), 20_000);
        assert_eq!(sst.min_key(), &k(0, 0));
        assert_eq!(sst.max_key(), &k(0, 19_999));
        assert_eq!(sst.max_seq(), 20_000);

        for &i in &[0i64, 8191, 8192, 8193, 19_999] {
            assert_eq!(sst.get(&k(0, i))?.unwrap().1, Some(v(&format!("v{}", i))));
        }

        let reopened = SSTable::open_from_path(&path, &TableSchema::default_table())?;
        assert_eq!(reopened.entry_count(), 20_000);
        assert_eq!(reopened.get(&k(0, 12_345))?.unwrap().1, Some(v("v12345")));
        Ok(())
    }

    #[test]
    fn test_sstable_empty_skiplist() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("2.sst");
        let list = SkipList::new();
        let sst =
            SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;
        assert_eq!(sst.entry_count(), 0);
        assert_eq!(sst.max_seq(), 0);

        let reopened = SSTable::open_from_path(&path, &TableSchema::default_table())?;
        assert_eq!(reopened.entry_count(), 0);
        assert_eq!(reopened.get(&k(5, 0))?, None);
        assert!(reopened.scan(&k(0, 0), &k(9, 9))?.is_empty());
        assert!(reopened.scan_iter(&k(0, 0), &k(9, 9))?.next().is_none());
        Ok(())
    }

    #[test]
    fn test_sstable_atomic_write_leaves_no_tmp() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("3.sst");
        let list = SkipList::new();
        list.insert(k(1, 0), 1, Some(v("a")));
        SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;
        assert!(path.exists());
        assert!(!dir.path().join("3.sst.tmp").exists());
        Ok(())
    }

    #[test]
    fn test_sstable_scan_across_chunks() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("4.sst");
        let list = SkipList::new();
        for i in 0..20_000i64 {
            list.insert((vec![0], i), i as u64 + 1, Some(v(&format!("v{}", i))));
        }
        let sst =
            SSTable::create_from_skiplist(&list, 1, &path, true, &TableSchema::default_table())?;

        let single = sst.scan(&k(0, 100), &k(0, 200))?;
        assert_eq!(single.len(), 101);
        assert_eq!(single[0].0, k(0, 100));
        assert_eq!(single[100].0, k(0, 200));

        let cross = sst.scan(&k(0, 8190), &k(0, 8193))?;
        assert_eq!(cross.len(), 4);
        assert_eq!(cross[0].0, k(0, 8190));
        assert_eq!(cross[3].0, k(0, 8193));

        let full = sst.scan(&k(0, 0), &k(0, 19_999))?;
        assert_eq!(full.len(), 20_000);
        let sub: Vec<Key> = full
            .iter()
            .map(|(k, _, _)| k.clone())
            .filter(|key| key.1 >= 8190 && key.1 <= 8193)
            .collect();
        assert_eq!(sub.len(), 4);
        assert_eq!(sub[0], k(0, 8190));
        assert_eq!(sub[3], k(0, 8193));
        Ok(())
    }

    #[test]
    fn test_sstable_missing_index_metadata_errors() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("5.sst");
        let schema = TableSchema::default_table();

        let file = File::create(&path)?;
        let writer = ArrowWriter::try_new(file, Arc::new(schema.arrow_schema()), None)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writer
            .close()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        assert!(SSTable::open_from_path(&path, &schema).is_err());
        Ok(())
    }
}
