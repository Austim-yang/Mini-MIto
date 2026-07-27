use crossbeam_skiplist::SkipMap;

use crate::types::{Key, Value};

pub struct SkipList {
    inner: SkipMap<Key, Option<Value>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            inner: SkipMap::new(),
        }
    }

    pub fn insert(&mut self, key: Key, value: Option<Value>) -> Option<Value> {
        let old_value = self.get(&key).unwrap_or(None);
        self.inner.insert(key, value);
        old_value
    }

    pub fn remove(&mut self, key: Key) -> Option<Value> {
        self.insert(key, None)
    }

    pub fn get(&self, key: &Key) -> Option<Option<Value>> {
        self.inner.get(key).map(|entry| entry.value().clone())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Key, Option<Value>)> + '_ {
        self.inner.iter().map(|entry| {
            let key = entry.key().clone();
            let value = entry.value().clone();
            (key, value)
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
        let mut list = SkipList::new();
        list.insert(k(3, 0), Some(v("c")));
        list.insert(k(2, 0), Some(v("b")));
        list.insert(k(1, 0), Some(v("a")));
    }

    #[test]
    fn insert_update() {
        let mut list = SkipList::new();
        let old = list.insert(k(5, 0), Some(v("old")));
        assert_eq!(old, None);
        let old = list.insert(k(5, 0), Some(v("new")));
        assert_eq!(old, Some(v("old")));
    }

    #[test]
    fn test_get() {
        let mut list = SkipList::new();
        list.insert(k(5, 0), Some(v("hello")));
        assert_eq!(list.get(&k(5, 0)), Some(Some(v("hello"))));
        assert_eq!(list.get(&k(6, 0)), None);
    }

    #[test]
    fn test_remove() {
        let mut list = SkipList::new();
        list.insert(k(5, 0), Some(v("hello")));
        list.insert(k(3, 0), Some(v("world")));
        assert_eq!(list.remove(k(5, 0)), Some(v("hello")));
        assert_eq!(list.len(), 2);
        assert_eq!(list.get(&k(5, 0)), Some(None));
        assert_eq!(list.remove(k(5, 0)), None);
        assert_eq!(list.get(&k(5, 0)), Some(None));
        assert_eq!(list.remove(k(3, 0)), Some(v("world")));
        assert_eq!(list.get(&k(3, 0)), Some(None));
        assert_eq!(list.len(), 2);
    }
}
