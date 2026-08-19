use std::{io, sync::Arc};

use crate::{
    Key, Value,
    memtable::{ImmutableMemtable, Memtable},
    sstable::sstable::SSTable,
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

    pub fn sources(&self) -> io::Result<Vec<Box<dyn Iterator<Item = (Key, u64, Option<Value>)>>>> {
        let mut out: Vec<Box<dyn Iterator<Item = (Key, u64, Option<Value>)>>> = Vec::new();
        if self.active.len() > 0 {
            out.push(Box::new(self.active.iter().collect::<Vec<_>>().into_iter()));
        }
        for imm in self.immutables.iter().rev() {
            if imm.len() > 0 {
                out.push(Box::new(imm.iter().collect::<Vec<_>>().into_iter()));
            }
        }
        for sst in self.ssts.iter().rev() {
            out.push(Box::new(sst.scan_iter(sst.min_key(), sst.max_key())?));
        }
        Ok(out)
    }
}
