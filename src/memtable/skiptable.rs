use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_skiplist::SkipMap;

use crate::types::{Key, Value};

pub struct SkipList {
    inner: SkipMap<Key, (u64, Option<Value>)>,
    max_seq: AtomicU64,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            inner: SkipMap::new(),
            max_seq: AtomicU64::new(0),
        }
    }

    pub fn insert(&self, key: Key, seq: u64, value: Option<Value>) -> Option<Value> {
        let old = self.inner.get(&key).map(|entry| entry.value().clone());
        let replace = match &old {
            Some((old_seq, _)) => seq >= *old_seq,
            None => true,
        };
        if replace {
            self.inner.insert(key, (seq, value));
            self.max_seq.fetch_max(seq, Ordering::Relaxed);
        }
        old.map(|(_, v)| v).flatten()
    }

    pub fn get(&self, key: &Key) -> Option<(u64, Option<Value>)> {
        self.inner.get(key).map(|entry| entry.value().clone())
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Key, u64, Option<Value>)> + '_ {
        self.inner.iter().map(|entry| {
            let key = entry.key().clone();
            let (seq, value) = entry.value().clone();
            (key, seq, value)
        })
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    #[test]
    fn insert_multiple_reverse() {
        let list = SkipList::new();
        list.insert(k(3, 0), 1, Some(v("c")));
        list.insert(k(2, 0), 2, Some(v("b")));
        list.insert(k(1, 0), 3, Some(v("a")));
    }

    #[test]
    fn insert_update() {
        let list = SkipList::new();
        let old = list.insert(k(5, 0), 1, Some(v("old")));
        assert_eq!(old, None);
        let old = list.insert(k(5, 0), 2, Some(v("new")));
        assert_eq!(old, Some(v("old")));
    }

    #[test]
    fn insert_keeps_higher_seq() {
        let list = SkipList::new();
        list.insert(k(5, 0), 5, Some(v("five")));
        let old = list.insert(k(5, 0), 1, Some(v("one")));
        assert_eq!(old, Some(v("five")));
        assert_eq!(list.get(&k(5, 0)).unwrap().0, 5);
        assert_eq!(list.get(&k(5, 0)).unwrap().1, Some(v("five")));
    }

    #[test]
    fn test_get() {
        let list = SkipList::new();
        list.insert(k(5, 0), 1, Some(v("hello")));
        assert_eq!(list.get(&k(5, 0)), Some((1, Some(v("hello")))));
        assert_eq!(list.get(&k(6, 0)), None);
    }

    #[test]
    fn test_tombstone() {
        let list = SkipList::new();
        list.insert(k(5, 0), 1, Some(v("hello")));
        assert_eq!(list.insert(k(5, 0), 2, None), Some(v("hello")));
        assert_eq!(list.get(&k(5, 0)), Some((2, None)));
        assert_eq!(list.len(), 1);
    }
}
