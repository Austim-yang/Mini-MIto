use std::{cell::RefCell, cmp::Ordering, marker::PhantomData, rc::Rc};

use rand::RngExt;

const MAX_HEIGHT: usize = 32;
const SKIP_P: f32 = 0.5;

#[derive(Debug, PartialEq, Eq)]
pub struct Node<K, V> {
    pub key: K,
    pub value: V,
    pub height: usize,
    pub next: [Option<Rc<RefCell<Node<K, V>>>>; MAX_HEIGHT],
}

impl<K, V> Node<K, V> {
    pub fn new(key: K, value: V, height: usize) -> Self {
        assert!(height > 0 && height <= MAX_HEIGHT);
        Node {
            key,
            value,
            height,
            next: std::array::from_fn(|_| None),
        }
    }
}

pub struct SkipList<K, V> {
    head: Rc<RefCell<Node<K, V>>>,
    len: usize,
}

impl<K: Default + Ord + Clone, V: Default + Clone> SkipList<K, V> {
    pub fn new() -> Self {
        let head_node = Node::new(K::default(), V::default(), MAX_HEIGHT);
        SkipList {
            head: Rc::new(RefCell::new(head_node)),
            len: 0,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
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
            return Some(old_value);
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

    pub fn remove(&mut self, key: K) -> Option<V> {
        let mut pre = vec![None; MAX_HEIGHT];
        let mut cur = self.head.clone();
        let mut target: Option<Rc<RefCell<Node<K, V>>>> = None;
        for level in (0..MAX_HEIGHT).rev() {
            loop {
                let next_opt = cur.borrow().next[level].clone();
                if let Some(next_ref) = next_opt {
                    let cmp = next_ref.borrow().key.cmp(&key);
                    match cmp {
                        Ordering::Less => cur = next_ref,
                        Ordering::Equal => {
                            target = Some(next_ref);
                            break;
                        }
                        Ordering::Greater => break,
                    }
                } else {
                    break;
                }
            }
            pre[level] = Some(cur.clone());
        }

        let target_node = target?;
        let target_height = target_node.borrow().height;

        let old_value = target_node.borrow().value.clone();

        for level in 0..target_height {
            if let Some(pred) = &pre[level] {
                let target_next = {
                    let target_borrow = target_node.borrow();
                    target_borrow.next[level].clone()
                };
                let mut pred_borrow = pred.borrow_mut();
                pred_borrow.next[level] = target_next;
            }
        }

        self.len -= 1;

        Some(old_value)
    }

    pub fn get(&self, key: &K) -> Option<V> {
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

impl<K: std::fmt::Debug, V: std::fmt::Debug> SkipList<K, V> {
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

impl<K: Clone, V: Clone> SkipList<K, V> {
    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            current: self.head.borrow().next[0].clone(),
        }
    }
}
pub struct Iter<K, V> {
    current: Option<Rc<RefCell<Node<K, V>>>>,
}

impl<K: Clone, V: Clone> Iterator for Iter<K, V> {
    type Item = (K, V);

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

    #[test]
    fn insert_single() {
        let mut list = SkipList::new();
        list.insert(10, "hello");
        list.print_all_levels();
    }

    #[test]
    fn insert_multiple_ordered() {
        let mut list = SkipList::new();
        list.insert(1, "a");
        list.insert(2, "b");
        list.insert(3, "c");
        list.print_all_levels();
        assert_eq!(3, list.len());
    }

    #[test]
    fn insert_multiple_reverse() {
        let mut list = SkipList::new();
        list.insert(3, "c");
        list.insert(2, "b");
        list.insert(1, "a");
    }

    #[test]
    fn insert_update() {
        let mut list = SkipList::new();
        let old = list.insert(5, "old");
        assert_eq!(old, None);
        let old = list.insert(5, "new");
        assert_eq!(old, Some("old"));
    }

    #[test]
    fn test_get() {
        let mut list = SkipList::new();
        list.insert(5, "hello");
        assert_eq!(list.get(&5), Some("hello"));
        assert_eq!(list.get(&6), None);
    }

    #[test]
    fn test_remove() {
        let mut list = SkipList::new();
        list.insert(5, "hello");
        list.insert(3, "world");
        assert_eq!(list.remove(5), Some("hello"));
        assert_eq!(list.len(), 1);
        assert_eq!(list.remove(5), None);
        assert_eq!(list.remove(3), Some("world"));
        assert_eq!(list.len(), 0);
    }
}
