use std::{cell::RefCell, cmp::Ordering, rc::Rc};

use rand::RngExt;

use crate::types::{Key, Value};

const MAX_HEIGHT: usize = 32;
const SKIP_P: f32 = 0.5;

#[derive(Debug, PartialEq, Eq)]
pub struct Node {
    pub key: Key,
    pub value: Option<Value>,
    pub height: usize,
    pub next: [Option<Rc<RefCell<Node>>>; MAX_HEIGHT],
}

impl Node {
    pub fn new(key: Key, value: Option<Value>, height: usize) -> Self {
        assert!(height > 0 && height <= MAX_HEIGHT);
        Node {
            key,
            value,
            height,
            next: std::array::from_fn(|_| None),
        }
    }
}

pub struct SkipList {
    head: Rc<RefCell<Node>>,
    len: usize,
}

impl SkipList {
    pub fn new() -> Self {
        let head_node = Node::new(Key::default(), None, MAX_HEIGHT);
        SkipList {
            head: Rc::new(RefCell::new(head_node)),
            len: 0,
        }
    }

    pub fn insert(&mut self, key: Key, value: Option<Value>) -> Option<Value> {
        let mut pre = vec![None; MAX_HEIGHT];
        let mut cur = self.head.clone();
        let mut found = None;
        'search: for level in (0..MAX_HEIGHT).rev() {
            loop {
                let next_opt = cur.borrow().next[level].clone();
                if let Some(next_ref) = next_opt {
                    let cmp = next_ref.borrow().key.cmp(&key);
                    match cmp {
                        Ordering::Less => cur = next_ref,
                        Ordering::Equal => {
                            found = Some(next_ref);
                            break 'search;
                        }
                        Ordering::Greater => break,
                    }
                } else {
                    break;
                }
            }
            pre[level] = Some(cur.clone());
        }

        if let Some(node) = found {
            let mut node_borrow = node.borrow_mut();
            let old_value = std::mem::replace(&mut node_borrow.value, value);
            return old_value;
        }

        let new_height = random_level();
        let new_node = Rc::new(RefCell::new(Node::new(key, value, new_height)));

        for level in 0..new_height {
            {
                let mut new_borrow = new_node.borrow_mut();
                if let Some(pred) = &pre[level] {
                    let pred_borrow = pred.borrow();
                    new_borrow.next[level] = pred_borrow.next[level].clone();
                }
            }
            if let Some(pred) = &pre[level] {
                let mut pred_borrow = pred.borrow_mut();
                pred_borrow.next[level] = Some(new_node.clone());
            }
        }

        self.len += 1;

        None
    }

    pub fn remove(&mut self, key: Key) -> Option<Value> {
        self.insert(key, None)
    }

    pub fn get(&self, key: &Key) -> Option<Option<Value>> {
        let mut cur = self.head.clone();

        for level in (0..MAX_HEIGHT).rev() {
            loop {
                let next_opt = cur.borrow().next[level].clone();
                match next_opt {
                    Some(next_ref) if next_ref.borrow().key < *key => cur = next_ref,
                    Some(next_ref) if next_ref.borrow().key == *key => {
                        return Some(next_ref.borrow().value.clone());
                    }
                    _ => break,
                }
            }
        }

        None
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl SkipList {
    #[cfg(test)]
    fn print_all_levels(&self) {
        for level in (0..MAX_HEIGHT).rev() {
            print!("Level {}: ", level);
            let mut cur = self.head.clone();
            let mut first = true;
            loop {
                let next_opt = cur.borrow().next[level].clone();
                if let Some(next_ref) = next_opt {
                    if !first {
                        print!(" -> ");
                    }
                    first = false;
                    print!("{:?}={:?}", next_ref.borrow().key, next_ref.borrow().value);
                    cur = next_ref;
                    continue;
                }
                break;
            }
            println!();
        }
    }
}

impl SkipList {
    pub fn iter(&self) -> Iter {
        Iter {
            current: self.head.borrow().next[0].clone(),
        }
    }
}
pub struct Iter {
    current: Option<Rc<RefCell<Node>>>,
}

impl Iterator for Iter {
    type Item = (Key, Option<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.take().map(|node| {
            let node_ref = node.borrow();
            let key = node_ref.key.clone();
            let value = node_ref.value.clone();
            self.current = node_ref.next[0].clone();
            (key, value)
        })
    }
}

fn random_level() -> usize {
    let mut level = 1;
    let mut rng = rand::rng();
    while rng.random::<f32>() < SKIP_P && level < MAX_HEIGHT {
        level += 1;
    }
    level
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
    fn insert_single() {
        let mut list = SkipList::new();
        list.insert(k(10, 0), Some(v("hello")));
        list.print_all_levels();
    }

    #[test]
    fn insert_multiple_ordered() {
        let mut list = SkipList::new();
        list.insert(k(1, 0), Some(v("a")));
        list.insert(k(2, 0), Some(v("b")));
        list.insert(k(3, 0), Some(v("c")));
        list.print_all_levels();
        assert_eq!(3, list.len());
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
