use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    vec,
};

use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch},
        datatypes::SchemaRef,
    },
    error::DataFusionError,
    execution::RecordBatchStream,
};

use datafusion::error::Result as DataFusionResult;
use futures::Stream;

use crate::{
    memtable::memtable::Region,
    query::{merge::MergeIter, predicate::TimeRange},
    schema::{SemanticType, TableSchema, cells_to_array},
    types::{Key, Value},
};

const BATCH_SIZE: usize = 10_000;

pub struct LSMStream {
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    table_schema: Arc<TableSchema>,
    merge: MergeIter,
    batches: Vec<RecordBatch>,
    index: usize,
    emitted: usize,
}

impl Unpin for LSMStream {}
unsafe impl Send for LSMStream {}

impl RecordBatchStream for LSMStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for LSMStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.index < this.batches.len() {
            let batch = this.batches[this.index].clone();
            this.index += 1;
            return Poll::Ready(Some(Ok(batch)));
        }
        match this.refill() {
            Ok(true) => {
                let batch = this.batches[this.index].clone();
                this.index += 1;
                Poll::Ready(Some(Ok(batch)))
            }
            Ok(false) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl LSMStream {
    pub fn new(
        region: Arc<Region>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        time_range: TimeRange,
    ) -> io::Result<Self> {
        let table_schema = region.schema();
        let sources = match time_range.to_inclusive_bounds() {
            None => Vec::new(),
            Some(b) => region.snapshot_sources_with_range(b)?,
        };
        Ok(Self {
            schema,
            projection,
            limit,
            table_schema,
            merge: MergeIter::new(sources),
            batches: Vec::new(),
            index: 0,
            emitted: 0,
        })
    }

    fn build_record_batch(
        chunk: &[(Key, Value)],
        table_schema: &TableSchema,
        projection: Option<&[usize]>,
    ) -> DataFusionResult<RecordBatch> {
        let ncols = table_schema.columns.len();
        let mut cols: Vec<Vec<Option<Vec<u8>>>> = vec![Vec::with_capacity(chunk.len()); ncols];

        for (key, value) in chunk {
            let tags = table_schema.decode_tags(&key.0);
            let ts = key.1.to_le_bytes().to_vec();
            let fields = table_schema.decode_fields(value);
            let mut tag_i = 0;
            let mut field_i = 0;
            for (c, col) in table_schema.columns.iter().enumerate() {
                let cell = match col.semantic {
                    SemanticType::Tag => Some(tags[tag_i].clone()),
                    SemanticType::Timestamp => Some(ts.clone()),
                    SemanticType::Field => Some(fields[field_i].clone()),
                };
                cols[c].push(cell);
                match col.semantic {
                    SemanticType::Tag => tag_i += 1,
                    SemanticType::Field => field_i += 1,
                    SemanticType::Timestamp => {}
                }
            }
        }

        let arrays: Vec<ArrayRef> = (0..ncols)
            .map(|c| cells_to_array(&table_schema.columns[c].data_type, &cols[c]))
            .collect();
        let full = RecordBatch::try_new(Arc::new(table_schema.arrow_schema()), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        match projection {
            Some(indices) => full
                .project(indices)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
            None => Ok(full),
        }
    }

    fn refill(&mut self) -> DataFusionResult<bool> {
        if self.batches.len() > self.index {
            return Ok(true);
        }
        let mut rows: Vec<(Key, Value)> = Vec::new();
        while rows.len() < BATCH_SIZE {
            match self.merge.next() {
                Some((k, Some(v))) => {
                    rows.push((k, v));
                    self.emitted += 1;
                    if let Some(lim) = self.limit
                        && self.emitted >= lim
                    {
                        break;
                    }
                }
                Some((_, None)) => {}
                None => break,
            }
        }
        if rows.is_empty() {
            return Ok(false);
        }
        let batch =
            Self::build_record_batch(&rows, &self.table_schema, self.projection.as_deref())?;
        self.batches.push(batch);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Region;
    use futures::StreamExt;
    use tempfile::tempdir;

    fn key(tag: u8, ts: i64) -> (Vec<u8>, i64) {
        (vec![tag], ts)
    }
    fn val(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_lsm_stream_merges_layers() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut region = Region::new(&path).unwrap();
        region.set_flush_threshold(2);
        region.write(key(1, 10), val("a")).unwrap();
        region.write(key(2, 10), val("b")).unwrap();
        region.write(key(1, 10), val("a2")).unwrap();
        region.write(key(3, 10), val("c")).unwrap();

        let region = Arc::new(region);
        let schema = Arc::new(region.schema().arrow_schema());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stream = LSMStream::new(region, schema, None, None, TimeRange::unbounded()).unwrap();
        let batches: Vec<_> = rt.block_on(async { stream.collect::<Vec<_>>().await });
        let mut rows = Vec::new();
        for b in batches {
            let b = b.unwrap();
            for i in 0..b.num_rows() {
                rows.push((
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .unwrap()
                        .value(i)
                        .to_vec(),
                    b.column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                        .value(i),
                ));
            }
        }
        assert_eq!(rows, vec![(vec![1], 10), (vec![2], 10), (vec![3], 10)]);
    }
}
