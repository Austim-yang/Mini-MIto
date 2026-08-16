use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use crate::{Key, Value};

struct HeapEntry {
    key: Key,
    seq: u64,
    src: usize,
    value: Option<Value>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq && self.src == other.src
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| other.seq.cmp(&self.seq))
            .then_with(|| self.src.cmp(&other.src))
    }
}

pub struct MergeIter {
    sources: Vec<Box<dyn Iterator<Item = (Key, u64, Option<Value>)>>>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
}

impl MergeIter {
    pub fn new(sources: Vec<Box<dyn Iterator<Item = (Key, u64, Option<Value>)>>>) -> Self {
        let mut it = MergeIter {
            sources,
            heap: BinaryHeap::new(),
        };
        for i in 0..it.sources.len() {
            it.push_source(i);
        }
        it
    }

    fn push_source(&mut self, src: usize) {
        if let Some((key, seq, value)) = self.sources[src].next() {
            self.heap.push(Reverse(HeapEntry {
                key,
                seq,
                src,
                value,
            }));
        }
    }
}

impl Iterator for MergeIter {
    type Item = (Key, Option<Value>);
    fn next(&mut self) -> Option<Self::Item> {
        let top = self.heap.pop()?.0;
        while let Some(Reverse(e)) = self.heap.peek() {
            if e.key == top.key {
                let dup = self.heap.pop().unwrap().0;
                self.push_source(dup.src);
            } else {
                break;
            }
        }
        self.push_source(top.src);
        Some((top.key, top.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Key, Value};

    fn vec_source(
        rows: Vec<(Key, u64, Option<Value>)>,
    ) -> Box<dyn Iterator<Item = (Key, u64, Option<Value>)>> {
        Box::new(rows.into_iter())
    }

    #[test]
    fn test_merge_dedup_newest_wins_and_tombstone() {
        let sources = vec![
            vec_source(vec![((vec![1], 50), 20, Some(b"new".to_vec()))]),
            vec_source(vec![
                ((vec![1], 50), 10, Some(b"old".to_vec())),
                ((vec![2], 10), 1, Some(b"x".to_vec())),
            ]),
        ];
        let mut m = MergeIter::new(sources);
        assert_eq!(m.next(), Some(((vec![1], 50), Some(b"new".to_vec()))));
        assert_eq!(m.next(), Some(((vec![2], 10), Some(b"x".to_vec()))));
        assert_eq!(m.next(), None);
    }

    #[test]
    fn test_merge_tombstone_suppresses_older() {
        let sources = vec![
            vec_source(vec![((vec![1], 50), 20, None)]),
            vec_source(vec![((vec![1], 50), 10, Some(b"old".to_vec()))]),
        ];
        let mut m = MergeIter::new(sources);
        assert_eq!(m.next(), Some(((vec![1], 50), None)));
        assert_eq!(m.next(), None);
    }

    #[test]
    fn test_merge_seq_overrides_layer_priority() {
        let sources = vec![
            vec_source(vec![((vec![1], 50), 3, Some(b"active".to_vec()))]),
            vec_source(vec![((vec![1], 50), 9, Some(b"sst".to_vec()))]),
        ];
        let mut m = MergeIter::new(sources);
        assert_eq!(m.next(), Some(((vec![1], 50), Some(b"sst".to_vec()))));
        assert_eq!(m.next(), None);
    }
}
