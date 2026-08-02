use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryArray, Int64Array, RecordBatch},
        datatypes::SchemaRef,
    },
    error::DataFusionError,
    execution::RecordBatchStream,
};

use datafusion::error::Result as DataFusionResult;
use futures::Stream;

use crate::{
    memtable::memtable::MemtableManager,
    types::{Key, Value},
};

pub struct LSMStream {
    memtable_manager: Arc<MemtableManager>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    batches: Vec<RecordBatch>,
    index: usize,
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
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl LSMStream {
    pub fn new(
        memtable_manager: Arc<MemtableManager>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> io::Result<Self> {
        let merged = memtable_manager.iter_all_data()?.collect::<Vec<_>>();

        let mut rows = Vec::new();
        for (key, value) in merged {
            if let Some(v) = value {
                rows.push((key, v));
                if let Some(lim) = limit {
                    if rows.len() >= lim {
                        break;
                    }
                }
            }
        }

        let batch_size = 10000;
        let mut batches = Vec::new();

        for chunk in rows.chunks(batch_size) {
            let batch = Self::build_record_batch(chunk, &schema);
            if let Ok(b) = batch {
                batches.push(b);
            }
        }

        Ok(Self {
            memtable_manager,
            schema,
            projection,
            limit,
            batches,
            index: 0,
        })
    }

    fn build_record_batch(
        chunk: &[(Key, Value)],
        schema: &SchemaRef,
    ) -> DataFusionResult<RecordBatch> {
        let mut tags_vals = Vec::with_capacity(chunk.len());
        let mut ts_vals = Vec::with_capacity(chunk.len());
        let mut fields_vals = Vec::with_capacity(chunk.len());

        for (key, value) in chunk {
            let (tags, ts) = key;
            tags_vals.push(tags.as_slice());
            ts_vals.push(*ts);
            fields_vals.push(value.as_slice());
        }

        let tags_arr = Arc::new(BinaryArray::from_iter_values(tags_vals)) as ArrayRef;
        let ts_arr = Arc::new(Int64Array::from_iter_values(ts_vals)) as ArrayRef;
        let fields_arr = Arc::new(BinaryArray::from_iter_values(fields_vals)) as ArrayRef;

        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "tags" => tags_arr.clone(),
                "timestamp" => ts_arr.clone(),
                "fields" => fields_arr.clone(),
                _ => unreachable!(),
            })
            .collect();

        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}
