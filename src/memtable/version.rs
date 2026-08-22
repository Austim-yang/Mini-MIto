use std::{io, sync::Arc, vec::IntoIter};

use arrow::array::RecordBatch;

use crate::{
    memtable::{ImmutableMemtable, Memtable},
    sstable::sstable::{SSTable, SSTableBatchIter},
};

pub struct Version {
    pub active: Arc<dyn Memtable>,
    pub immutables: Vec<Arc<dyn ImmutableMemtable>>,
    pub ssts: Vec<SSTable>,
    pub seq: u64,
}

impl Version {
    pub fn new(active: Arc<dyn Memtable>, ssts: Vec<SSTable>, seq: u64) -> Version {
        Version {
            active,
            immutables: Vec::new(),
            ssts,
            seq,
        }
    }

    pub fn with_frozen(
        &self,
        new_active: Arc<dyn Memtable>,
        imm: Arc<dyn ImmutableMemtable>,
    ) -> Version {
        let mut immutables = self.immutables.clone();
        immutables.push(imm);
        Version {
            active: new_active,
            immutables,
            ssts: self.ssts.clone(),
            seq: self.seq,
        }
    }
}

pub enum Source {
    Sst(SSTableBatchIter),
    Memtable(IntoIter<Arc<RecordBatch>>),
}

impl Source {
    pub fn memtable(batches: Vec<Arc<RecordBatch>>) -> Self {
        Source::Memtable(batches.into_iter())
    }

    pub fn next_batch(&mut self) -> io::Result<Option<Arc<RecordBatch>>> {
        match self {
            Source::Sst(iter) => iter.next().transpose().map(|o| o.map(Arc::new)),
            Source::Memtable(iter) => Ok(iter.next()),
        }
    }
}
